package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/haonox/toolane/internal/store/sqlite"
	"github.com/haonox/toolane/internal/tool"
)

const executionEndpoint = "POST /api/v1/executions"

var ErrDurableWorkflowPending = errors.New("durable workflows are not available yet")

type storedRecord struct {
	Snapshot  Snapshot          `json:"snapshot"`
	Arguments []byte            `json:"arguments"`
	Deadline  time.Time         `json:"queue_deadline"`
	Resource  *tool.ResourceRef `json:"resource,omitempty"`
	Limits    struct {
		QueueTimeout       time.Duration `json:"queue_timeout_ns"`
		ExecutionTimeout   time.Duration `json:"execution_timeout_ns"`
		OutputLimitBytes   int           `json:"output_limit_bytes"`
		ArgumentLimitBytes int           `json:"argument_limit_bytes"`
		ResultLimitBytes   int           `json:"result_limit_bytes"`
	} `json:"limits"`
}

// RecoveryResource ties a persisted resource to the execution that created it.
// The recovery callback must also scan ownership labels: a crash may happen
// after creation and before ResourceRef is committed.
type RecoveryResource struct {
	ExecutionID string
	Ref         tool.ResourceRef
}

type normalizedRequest struct {
	TenantID         string          `json:"tenant_id"`
	AgentID          string          `json:"agent_id"`
	Tool             string          `json:"tool"`
	Arguments        json.RawMessage `json:"arguments"`
	QueueTimeout     time.Duration   `json:"queue_timeout_ns"`
	ExecutionTimeout time.Duration   `json:"execution_timeout_ns"`
	OutputLimitBytes int             `json:"output_limit_bytes"`
}

// NewDurableManager coordinates previously accepted work before starting any
// workers. recoverResources receives persisted references and must confirm
// they are stopped or absent, while also scanning ownership labels for the
// create-before-record crash window. A failure prevents admission. The caller
// owns store and closes it after Manager.Close.
func NewDurableManager(ctx context.Context, config Config, registry *tool.Registry, store *sqlite.Store, recoverResources func(context.Context, []RecoveryResource) error) (*Manager, error) {
	if store == nil || recoverResources == nil {
		return nil, errors.New("durable manager requires a store and resource recovery")
	}
	m, err := newManager(config, registry, store)
	if err != nil {
		return nil, err
	}
	release, err := store.ClaimRuntime()
	if err != nil {
		return nil, err
	}
	claimed := true
	defer func() {
		if claimed {
			release()
		}
	}()
	rows, err := store.ListExecutions(ctx)
	if err != nil {
		return nil, fmt.Errorf("list executions before recovery: %w", err)
	}
	resources, err := recoveryResources(rows)
	if err != nil {
		return nil, fmt.Errorf("read owned resources: %w", err)
	}
	if err := recoverResources(ctx, resources); err != nil {
		return nil, fmt.Errorf("recover owned resources: %w", err)
	}
	if err := m.recoverExecutions(rows); err != nil {
		return nil, fmt.Errorf("recover executions: %w", err)
	}
	m.runtimeRelease = release
	claimed = false
	m.startWorkers()
	return m, nil
}

func encodeNormalizedRequest(p preparedRequest) ([]byte, error) {
	raw := p.arguments
	if len(raw) == 0 {
		raw = []byte("null")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("decode tool arguments for persistence: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("tool arguments must contain one JSON value")
	}
	arguments, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return json.Marshal(normalizedRequest{
		TenantID: p.request.TenantID, AgentID: p.request.AgentID, Tool: p.request.Tool,
		Arguments: arguments, QueueTimeout: p.queueTimeout, ExecutionTimeout: p.runTimeout,
		OutputLimitBytes: p.outputLimit,
	})
}

func encodeStoredExecution(r *record, next Snapshot) ([]byte, error) {
	value := storedRecord{Snapshot: next, Arguments: r.arguments, Deadline: r.deadline, Resource: r.resource}
	value.Limits.QueueTimeout = next.QueueTimeout
	value.Limits.ExecutionTimeout = next.ExecutionTimeout
	value.Limits.OutputLimitBytes = next.OutputLimitBytes
	value.Limits.ArgumentLimitBytes = next.ArgumentLimitBytes
	value.Limits.ResultLimitBytes = next.ResultLimitBytes
	return json.Marshal(value)
}

func decodeStoredExecution(row sqlite.Execution) (*record, error) {
	var value storedRecord
	if err := json.Unmarshal(row.Snapshot, &value); err != nil {
		return nil, fmt.Errorf("decode execution %s: %w", row.ID, err)
	}
	s := value.Snapshot
	if s.ID != row.ID || s.TenantID != row.TenantID || string(s.State) != row.State || !s.CreatedAt.Equal(row.CreatedAt) || value.Deadline.IsZero() || row.Version < 1 {
		return nil, fmt.Errorf("execution %s has inconsistent persisted fields", row.ID)
	}
	s.QueueTimeout = value.Limits.QueueTimeout
	s.ExecutionTimeout = value.Limits.ExecutionTimeout
	s.OutputLimitBytes = value.Limits.OutputLimitBytes
	s.ArgumentLimitBytes = value.Limits.ArgumentLimitBytes
	s.ResultLimitBytes = value.Limits.ResultLimitBytes
	if value.Resource != nil {
		if err := validateResourceRef(*value.Resource); err != nil {
			return nil, fmt.Errorf("execution %s has invalid resource: %w", row.ID, err)
		}
	}
	return &record{snapshot: s, arguments: value.Arguments, deadline: value.Deadline, version: row.Version, resource: value.Resource}, nil
}

func validateResourceRef(ref tool.ResourceRef) error {
	if ref.Kind != tool.ResourceDockerContainer || ref.ID == "" || len(ref.ID) > 128 ||
		ref.InstanceID == "" || len(ref.InstanceID) > 32 || strings.ContainsAny(ref.ID, " \t\r\n") {
		return errors.New("invalid Docker resource reference")
	}
	return nil
}

func recoveryResources(rows []sqlite.Execution) ([]RecoveryResource, error) {
	var resources []RecoveryResource
	for _, row := range rows {
		r, err := decodeStoredExecution(row)
		if err != nil {
			return nil, err
		}
		if r.resource != nil && r.snapshot.CleanupStatus != "succeeded" {
			resources = append(resources, RecoveryResource{ExecutionID: row.ID, Ref: *r.resource})
		}
	}
	return resources, nil
}

func stateEvent(snapshot Snapshot, at time.Time) (sqlite.Event, error) {
	payload, err := json.Marshal(struct {
		Status     State     `json:"status"`
		ErrorCode  string    `json:"error_code,omitempty"`
		StopReason string    `json:"stop_reason,omitempty"`
		At         time.Time `json:"at"`
	}{snapshot.State, snapshot.ErrorCode, snapshot.StopReason, at})
	if err != nil {
		return sqlite.Event{}, err
	}
	return sqlite.Event{Kind: "state", Payload: payload, OccurredAt: at}, nil
}

func (m *Manager) createStoredExecutionLocked(ctx context.Context, r *record, request []byte, key string) (string, bool, error) {
	encoded, err := encodeStoredExecution(r, r.snapshot)
	if err != nil {
		return "", false, err
	}
	event, err := stateEvent(r.snapshot, r.snapshot.CreatedAt)
	if err != nil {
		return "", false, err
	}
	row := sqlite.Execution{ID: r.snapshot.ID, TenantID: r.snapshot.TenantID, State: string(r.snapshot.State), Snapshot: encoded, Request: request, CreatedAt: r.snapshot.CreatedAt}
	if key == "" {
		err = m.store.CreateExecution(ctx, row, event)
	} else {
		var id string
		var reused bool
		id, reused, err = m.store.CreateIdempotentExecution(ctx, executionEndpoint, key, row, event)
		if err == nil && reused {
			return id, true, nil
		}
	}
	if err != nil {
		if !errors.Is(err, sqlite.ErrIdempotencyConflict) {
			m.haltStoreLocked(err)
		}
		return "", false, err
	}
	r.version = 1
	return r.snapshot.ID, false, nil
}

func (m *Manager) applySnapshotLocked(r *record, next Snapshot) error {
	return m.applySnapshotKindLocked(r, next, "state")
}

func (m *Manager) applySnapshotKindLocked(r *record, next Snapshot, kind string) error {
	if m.store != nil {
		encoded, err := encodeStoredExecution(r, next)
		if err != nil {
			m.haltStoreLocked(err)
			return err
		}
		event, err := stateEvent(next, time.Now().UTC())
		if err != nil {
			m.haltStoreLocked(err)
			return err
		}
		event.Kind = kind
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		version, err := m.store.Transition(ctx, next.ID, r.version, string(next.State), encoded, event)
		if err != nil {
			m.haltStoreLocked(err)
			return err
		}
		r.version = version
	}
	r.snapshot = next
	return nil
}

// resourceCreated records ownership before a newly created resource can start.
// The Docker executor must still remove it if this callback returns an error.
func (m *Manager) resourceCreated(r *record, ref tool.ResourceRef) error {
	if err := validateResourceRef(ref); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.storeError != nil {
		return m.storeError
	}
	if r.resource != nil || r.snapshot.State != Running && r.snapshot.State != Cancelling {
		return errors.New("execution cannot accept another created resource")
	}
	next := cloneSnapshot(r.snapshot)
	next.CleanupStatus = "pending"
	if m.store != nil {
		copyOf := *r
		copyOf.resource = &ref
		encoded, err := encodeStoredExecution(&copyOf, next)
		if err != nil {
			m.haltStoreLocked(err)
			return err
		}
		at := time.Now().UTC()
		payload, err := json.Marshal(struct {
			ResourceKind string    `json:"resource_kind"`
			At           time.Time `json:"at"`
		}{ref.Kind, at})
		if err != nil {
			m.haltStoreLocked(err)
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		version, err := m.store.Transition(ctx, next.ID, r.version, string(next.State), encoded,
			sqlite.Event{Kind: "resource_created", Payload: payload, OccurredAt: at})
		if err != nil {
			m.haltStoreLocked(err)
			return err
		}
		r.version = version
	}
	r.resource = &ref
	r.snapshot = next
	return nil
}

func (m *Manager) haltStoreLocked(err error) {
	if m.storeError == nil {
		m.storeError = err
	}
	m.halted = true
	m.cond.Broadcast()
}

func (m *Manager) recoverExecutions(rows []sqlite.Execution) error {
	for _, row := range rows {
		r, err := decodeStoredExecution(row)
		if err != nil {
			return err
		}
		if r.snapshot.WorkflowID != "" {
			return ErrDurableWorkflowPending
		}
		m.records[r.snapshot.ID] = r
		switch r.snapshot.State {
		case Queued:
			if _, ok := m.tools[r.snapshot.Tool]; !ok {
				return fmt.Errorf("recovered execution %s uses unregistered tool %q", r.snapshot.ID, r.snapshot.Tool)
			}
			if !time.Now().Before(r.deadline) {
				if err := m.finishLocked(r, TimedOut, "queue_timeout", "queue wait expired during restart"); err != nil {
					return err
				}
				continue
			}
			if m.pending >= m.config.QueueCapacity {
				return errors.New("recovered pending work exceeds queue capacity")
			}
			if tenant := m.tenants[r.snapshot.TenantID]; tenant != nil && tenant.pending >= m.config.TenantPendingLimit {
				return errors.New("recovered tenant work exceeds pending capacity")
			}
			m.queue = append(m.queue, r)
			m.admitPendingLocked(r)
		case Running, Cancelling:
			if err := m.finishLocked(r, Interrupted, "service_restarted", "execution result is unknown after restart"); err != nil {
				return err
			}
		case Succeeded, Failed, TimedOut, Cancelled, Skipped, Interrupted:
			m.retainTerminalLocked(terminalGroup{ids: []string{r.snapshot.ID}})
		default:
			return fmt.Errorf("recovered execution %s has unsupported state %q", r.snapshot.ID, r.snapshot.State)
		}
	}
	return nil
}
