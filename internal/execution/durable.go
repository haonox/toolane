package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/haonox/toolane/internal/store/sqlite"
	"github.com/haonox/toolane/internal/tool"
)

const executionEndpoint = "POST /api/v1/executions"
const workflowEndpoint = "POST /api/v1/workflows"

type storedRecord struct {
	Snapshot  Snapshot  `json:"snapshot"`
	Arguments []byte    `json:"arguments"`
	Deadline  time.Time `json:"queue_deadline"`
	Limits    struct {
		QueueTimeout       time.Duration `json:"queue_timeout_ns"`
		ExecutionTimeout   time.Duration `json:"execution_timeout_ns"`
		OutputLimitBytes   int           `json:"output_limit_bytes"`
		ArgumentLimitBytes int           `json:"argument_limit_bytes"`
		ResultLimitBytes   int           `json:"result_limit_bytes"`
	} `json:"limits"`
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

// NewManager reconciles previously accepted work before starting any
// workers. The caller owns store and closes it after Manager.Close. An optional
// observer runs synchronously under the Manager lock after each committed
// transition; it must stay fast and must not call back into Manager.
func NewManager(ctx context.Context, config Config, registry *tool.Registry, store *sqlite.Store, observers ...func(Snapshot, Snapshot)) (*Manager, error) {
	if store == nil {
		return nil, errors.New("manager requires a store")
	}
	if len(observers) > 1 {
		return nil, errors.New("at most one transition observer is allowed")
	}
	m, err := newManager(config, registry, store)
	if err != nil {
		return nil, err
	}
	if len(observers) == 1 {
		m.transitionObserver = observers[0]
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
	for _, row := range rows {
		if _, err := decodeStoredExecution(row); err != nil {
			return nil, fmt.Errorf("inspect execution before recovery: %w", err)
		}
	}
	_, err = store.Prune(ctx, sqlite.RetentionPolicy{MaxRecords: config.DurableRecordLimit, MaxAge: config.DurableRetention, Now: time.Now().UTC()})
	if err != nil && !errors.Is(err, sqlite.ErrRetentionCapacity) {
		return nil, fmt.Errorf("prune durable records before recovery: %w", err)
	}
	// Reconstruct from the rows that still exist after retention pruning.
	rows, err = store.ListExecutions(ctx)
	if err != nil {
		return nil, fmt.Errorf("list executions after pruning: %w", err)
	}
	workflows, err := store.ListWorkflows(ctx)
	if err != nil {
		return nil, fmt.Errorf("list workflows before recovery: %w", err)
	}
	workflowNodes, err := m.recoverWorkflows(workflows)
	if err != nil {
		return nil, fmt.Errorf("recover workflows: %w", err)
	}
	if err := m.recoverExecutions(rows, workflowNodes); err != nil {
		return nil, fmt.Errorf("recover executions: %w", err)
	}
	m.runtimeRelease = release
	claimed = false
	m.startWorkers()
	return m, nil
}

// pruneDurableLocked runs under the manager lock so an admission cannot race
// with pruning and cached terminal snapshots remain consistent with SQLite.
func (m *Manager) pruneDurableLocked(reserve int) error {
	if reserve > m.config.DurableRecordLimit {
		return sqlite.ErrRetentionCapacity
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if reserve > 0 {
		count, err := m.store.CountExecutions(ctx)
		if err != nil {
			m.haltStoreLocked(err)
			return err
		}
		if count <= m.config.DurableRecordLimit-reserve {
			return nil
		}
	}
	result, err := m.store.Prune(ctx, sqlite.RetentionPolicy{
		MaxRecords: m.config.DurableRecordLimit,
		MaxAge:     m.config.DurableRetention,
		Reserve:    reserve,
		Now:        time.Now().UTC(),
	})
	if len(result.ExecutionIDs) > 0 {
		deleted := make(map[string]bool, len(result.ExecutionIDs))
		for _, id := range result.ExecutionIDs {
			deleted[id] = true
			delete(m.records, id)
		}
		kept := m.terminal[:0]
		for _, group := range m.terminal {
			if len(group.ids) > 0 && deleted[group.ids[0]] {
				m.terminalCount -= len(group.ids)
				continue
			}
			kept = append(kept, group)
		}
		clear(m.terminal[len(kept):])
		m.terminal = kept
	}
	for _, id := range result.WorkflowIDs {
		delete(m.workflows, id)
	}
	if err != nil && !errors.Is(err, sqlite.ErrRetentionCapacity) {
		m.haltStoreLocked(err)
	}
	return err
}

func (m *Manager) prunePeriodically() {
	defer m.workers.Done()
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-ticker.C:
			m.mu.Lock()
			if !m.closing && !m.halted {
				_ = m.pruneDurableLocked(0)
			}
			m.mu.Unlock()
		}
	}
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

func encodeNormalizedWorkflowRequest(req WorkflowRequest, prepared []preparedRequest) ([]byte, error) {
	type normalizedNode struct {
		ID        string          `json:"node_id"`
		DependsOn []string        `json:"depends_on"`
		Execution json.RawMessage `json:"execution"`
	}
	value := struct {
		TenantID string           `json:"tenant_id"`
		Nodes    []normalizedNode `json:"nodes"`
	}{TenantID: prepared[0].request.TenantID, Nodes: make([]normalizedNode, len(req.Nodes))}
	for i, node := range req.Nodes {
		encoded, err := encodeNormalizedRequest(prepared[i])
		if err != nil {
			return nil, err
		}
		dependsOn := append([]string{}, node.DependsOn...)
		sort.Strings(dependsOn)
		value.Nodes[i] = normalizedNode{ID: node.ID, DependsOn: dependsOn, Execution: encoded}
	}
	return json.Marshal(value)
}

func encodeStoredExecution(r *record, next Snapshot) ([]byte, error) {
	value := storedRecord{Snapshot: next, Arguments: r.arguments, Deadline: r.deadline}
	value.Limits.QueueTimeout = next.QueueTimeout
	value.Limits.ExecutionTimeout = next.ExecutionTimeout
	value.Limits.OutputLimitBytes = next.OutputLimitBytes
	value.Limits.ArgumentLimitBytes = next.ArgumentLimitBytes
	value.Limits.ResultLimitBytes = next.ResultLimitBytes
	return json.Marshal(value)
}

func decodeStoredExecution(row sqlite.Execution) (*record, error) {
	var value storedRecord
	decoder := json.NewDecoder(bytes.NewReader(row.Snapshot))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("decode execution %s: %w", row.ID, err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("execution %s must contain one stored record", row.ID)
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
	return &record{snapshot: s, arguments: value.Arguments, deadline: value.Deadline, version: row.Version}, nil
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

func (m *Manager) createStoredWorkflowLocked(ctx context.Context, request WorkflowRequest, prepared []preparedRequest, w *workflow, key string, normalizedRequest []byte) (string, bool, error) {
	encodedRequest, err := json.Marshal(request)
	if err != nil {
		return "", false, err
	}
	stored := sqlite.Workflow{ID: w.id, TenantID: w.tenantID, State: string(w.state), Request: encodedRequest, CreatedAt: w.createdAt}
	stored.Nodes = make([]sqlite.WorkflowNode, len(w.nodes))
	for i, r := range w.nodes {
		encoded, err := encodeStoredExecution(r, r.snapshot)
		if err != nil {
			return "", false, err
		}
		normalized, err := encodeNormalizedRequest(prepared[i])
		if err != nil {
			return "", false, err
		}
		event, err := stateEvent(r.snapshot, w.createdAt)
		if err != nil {
			return "", false, err
		}
		stored.Nodes[i] = sqlite.WorkflowNode{
			NodeID: request.Nodes[i].ID, DependsOn: request.Nodes[i].DependsOn,
			Execution: sqlite.Execution{ID: r.snapshot.ID, TenantID: w.tenantID, State: string(r.snapshot.State), Snapshot: encoded, Request: normalized, CreatedAt: w.createdAt},
			Event:     event,
		}
	}
	var id string
	var reused bool
	if key == "" {
		err = m.store.CreateWorkflow(ctx, stored)
	} else {
		id, reused, err = m.store.CreateIdempotentWorkflow(ctx, workflowEndpoint, key, stored, normalizedRequest)
	}
	if err != nil {
		if !errors.Is(err, sqlite.ErrIdempotencyConflict) {
			m.haltStoreLocked(err)
		}
		return "", false, err
	}
	if reused {
		return id, true, nil
	}
	w.version = 1
	for _, r := range w.nodes {
		r.version = 1
	}
	return w.id, false, nil
}

func (m *Manager) transitionWorkflowLocked(w *workflow, state State, finishedAt *time.Time) error {
	if w.state == state && (w.finishedAt != nil) == (finishedAt != nil) {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	version, err := m.store.TransitionWorkflow(ctx, w.id, w.version, string(state), finishedAt)
	if err != nil {
		m.haltStoreLocked(err)
		return err
	}
	w.version = version
	w.state = state
	w.finishedAt = finishedAt
	return nil
}

func decodeStoredWorkflowSnapshot(stored sqlite.Workflow) (WorkflowSnapshot, error) {
	snapshot := WorkflowSnapshot{ID: stored.ID, TenantID: stored.TenantID, State: State(stored.State), CreatedAt: stored.CreatedAt, FinishedAt: stored.FinishedAt, Nodes: make([]Snapshot, len(stored.Nodes))}
	for i, node := range stored.Nodes {
		r, err := decodeStoredExecution(node.Execution)
		if err != nil {
			return WorkflowSnapshot{}, err
		}
		if r.snapshot.WorkflowID != stored.ID || r.snapshot.NodeID != node.NodeID || r.snapshot.TenantID != stored.TenantID {
			return WorkflowSnapshot{}, fmt.Errorf("workflow %s has inconsistent node %q", stored.ID, node.NodeID)
		}
		snapshot.Nodes[i] = cloneSnapshot(r.snapshot)
	}
	return snapshot, nil
}

func (m *Manager) applySnapshotLocked(r *record, next Snapshot) error {
	return m.applySnapshotKindLocked(r, next, "state")
}

func (m *Manager) applySnapshotKindLocked(r *record, next Snapshot, kind string) error {
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
	version, err := m.store.TransitionWithEvents(ctx, next.ID, r.version, string(next.State), encoded, r.outputEvents, event)
	if err != nil {
		m.haltStoreLocked(err)
		return err
	}
	r.version = version
	r.outputEvents = nil
	m.notifyEventsLocked(next.ID)
	before := r.snapshot
	r.snapshot = next
	if m.transitionObserver != nil {
		m.transitionObserver(before, next)
	}
	return nil
}

func (m *Manager) haltStoreLocked(err error) {
	if m.storeError == nil {
		m.storeError = err
	}
	m.halted = true
	m.cond.Broadcast()
}

func (m *Manager) recoverExecutions(rows []sqlite.Execution, workflowNodes map[string]bool) error {
	for _, row := range rows {
		r, err := decodeStoredExecution(row)
		if err != nil {
			return err
		}
		if r.snapshot.WorkflowID != "" {
			if !workflowNodes[row.ID] {
				return fmt.Errorf("execution %s references an unknown workflow", row.ID)
			}
			continue
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
