// Package execution owns the lifecycle of local tool executions.
package execution

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/haonox/toolane/internal/store/sqlite"
	"github.com/haonox/toolane/internal/tool"
)

var (
	// ErrNotFound indicates that an execution is not retained by the manager.
	ErrNotFound = errors.New("execution not found")
	// ErrQueueFull indicates that the bounded admission queue has no capacity.
	ErrQueueFull = errors.New("execution queue full")
	// ErrClosed indicates that the manager no longer accepts work.
	ErrClosed = errors.New("execution manager closed")
	// ErrAgentIDTooLarge indicates that an agent ID exceeds its byte limit.
	ErrAgentIDTooLarge = errors.New("agent ID exceeds limit")
	// ErrArgumentsTooLarge indicates that tool arguments exceed their byte limit.
	ErrArgumentsTooLarge = errors.New("tool arguments exceed limit")
	// ErrTenantIDTooLarge indicates that a tenant ID exceeds its byte limit.
	ErrTenantIDTooLarge = errors.New("tenant ID exceeds limit")
	// ErrTenantQueueFull indicates that a tenant's pending capacity is exhausted.
	ErrTenantQueueFull = errors.New("tenant pending queue full")
	// ErrBackendUnavailable indicates that new work is paused after an unconfirmed stop.
	ErrBackendUnavailable = errors.New("execution backend unavailable")
	// ErrStopUnconfirmed indicates that shutdown could not confirm all work stopped.
	ErrStopUnconfirmed = errors.New("execution stop could not be confirmed")
	// ErrInvalidRequest identifies caller-controlled input rejected before admission.
	ErrInvalidRequest = errors.New("invalid execution request")
)

// State describes the public execution lifecycle.
type State string

// SchedulerMode selects how ready tasks are claimed. RoundRobin is the V0.2
// default; FIFO remains available for controlled comparisons.
type SchedulerMode string

const (
	SchedulerRoundRobin SchedulerMode = "round_robin"
	SchedulerFIFO       SchedulerMode = "fifo"
)

const (
	Blocked     State = "blocked"
	Queued      State = "queued"
	Running     State = "running"
	Cancelling  State = "cancelling"
	Succeeded   State = "succeeded"
	Failed      State = "failed"
	TimedOut    State = "timed_out"
	Cancelled   State = "cancelled"
	Skipped     State = "skipped"
	Interrupted State = "interrupted"
)

// Config bounds active work and retained results. Use DefaultConfig as a
// starting point, then adjust fields before creating a Manager.
type Config struct {
	Workers              int
	QueueCapacity        int
	TenantRunningLimit   int
	TenantPendingLimit   int
	MaxTenantIDBytes     int
	Scheduler            SchedulerMode
	RetainedResults      int
	DurableRecordLimit   int
	DurableRetention     time.Duration
	MaxWorkflowNodes     int
	MaxWorkflowBytes     int
	DefaultQueueTimeout  time.Duration
	MaxQueueTimeout      time.Duration
	DefaultRunTimeout    time.Duration
	MaxRunTimeout        time.Duration
	MaxResultBytes       int
	MaxArgumentBytes     int
	MaxOutputBytes       int
	MaxAgentIDBytes      int
	MaxErrorMessageBytes int
}

// DefaultConfig returns the default bounded local execution configuration.
func DefaultConfig() Config {
	return Config{
		Workers:              4,
		QueueCapacity:        256,
		TenantRunningLimit:   2,
		TenantPendingLimit:   64,
		MaxTenantIDBytes:     256,
		Scheduler:            SchedulerRoundRobin,
		RetainedResults:      1000,
		DurableRecordLimit:   10000,
		DurableRetention:     7 * 24 * time.Hour,
		MaxWorkflowNodes:     64,
		MaxWorkflowBytes:     256 * 1024,
		DefaultQueueTimeout:  30 * time.Second,
		MaxQueueTimeout:      300 * time.Second,
		DefaultRunTimeout:    10 * time.Second,
		MaxRunTimeout:        60 * time.Second,
		MaxResultBytes:       64 * 1024,
		MaxArgumentBytes:     256 * 1024,
		MaxOutputBytes:       1024 * 1024,
		MaxAgentIDBytes:      256,
		MaxErrorMessageBytes: 4 * 1024,
	}
}

func (c Config) validate() error {
	if c.Workers <= 0 || c.QueueCapacity <= 0 || c.TenantRunningLimit <= 0 || c.TenantPendingLimit <= 0 || c.MaxTenantIDBytes <= 0 || c.RetainedResults <= 0 || c.DurableRecordLimit <= 0 || c.DurableRetention <= 0 || c.MaxWorkflowNodes <= 0 || c.MaxWorkflowBytes <= 0 || c.MaxResultBytes <= 0 || c.MaxArgumentBytes <= 0 || c.MaxOutputBytes <= 0 || c.MaxAgentIDBytes <= 0 || c.MaxErrorMessageBytes <= 0 {
		return errors.New("workers, global and tenant capacities, retained results and byte limits must be positive")
	}
	if c.MaxTenantIDBytes < len("local") {
		return errors.New("tenant ID byte limit must allow the local tenant")
	}
	if c.Scheduler != SchedulerRoundRobin && c.Scheduler != SchedulerFIFO {
		return errors.New("invalid scheduler mode")
	}
	if c.DefaultQueueTimeout <= 0 || c.MaxQueueTimeout < c.DefaultQueueTimeout ||
		c.DefaultRunTimeout <= 0 || c.MaxRunTimeout < c.DefaultRunTimeout {
		return errors.New("invalid queue or execution timeout range")
	}
	return nil
}

// Request describes one tool execution. Arguments are copied on admission. A
// cancelled Submit context prevents admission but does not own an accepted
// execution's lifetime.
type Request struct {
	TenantID         string
	AgentID          string
	Tool             string
	Arguments        json.RawMessage
	QueueTimeout     time.Duration
	ExecutionTimeout time.Duration
	OutputLimitBytes int
}

// Snapshot is an immutable view of an execution at one point in time.
type Snapshot struct {
	ID                 string          `json:"execution_id"`
	TenantID           string          `json:"tenant_id"`
	AgentID            string          `json:"agent_id,omitempty"`
	Tool               string          `json:"tool"`
	WorkflowID         string          `json:"workflow_id,omitempty"`
	NodeID             string          `json:"node_id,omitempty"`
	State              State           `json:"status"`
	ErrorCode          string          `json:"error_code,omitempty"`
	ErrorMessage       string          `json:"error_message,omitempty"`
	StopReason         string          `json:"stop_reason,omitempty"`
	CleanupStatus      string          `json:"cleanup_status"`
	CleanupError       string          `json:"cleanup_error,omitempty"`
	Result             json.RawMessage `json:"result,omitempty"`
	Stdout             string          `json:"stdout,omitempty"`
	Stderr             string          `json:"stderr,omitempty"`
	ExitCode           *int            `json:"exit_code,omitempty"`
	OutputTruncated    bool            `json:"output_truncated"`
	CreatedAt          time.Time       `json:"created_at"`
	ReadyAt            *time.Time      `json:"ready_at,omitempty"`
	RunningAt          *time.Time      `json:"running_at,omitempty"`
	FinishedAt         *time.Time      `json:"finished_at,omitempty"`
	ProcessStartedAt   *time.Time      `json:"process_started_at,omitempty"`
	QueueTimeout       time.Duration   `json:"-"`
	ExecutionTimeout   time.Duration   `json:"-"`
	OutputLimitBytes   int             `json:"-"`
	ArgumentLimitBytes int             `json:"-"`
	ResultLimitBytes   int             `json:"-"`
}

type record struct {
	snapshot        Snapshot
	arguments       json.RawMessage
	deadline        time.Time
	version         int64
	outputEvents    []sqlite.Event
	cancel          context.CancelFunc
	tenant          *tenantState
	workflow        *workflow
	dependsOn       []*record
	dependents      []*record
	terminalCounted bool
}

// Stats reports current capacity use. It is a snapshot, not a reservation.
type Stats struct {
	Queued  int
	Blocked int
	Pending int
	Running int
	Stored  int
	Tenants int
	Halted  bool
	Closing bool
}

// Manager owns state transitions, the ready queue, worker slots, and
// cancellation. A running tool occupies a worker until Execute returns; an
// executor must stop its work before returning after cancellation.
type Manager struct {
	mu                 sync.Mutex
	transitionObserver func(Snapshot, Snapshot)
	cond               *sync.Cond
	config             Config
	store              *sqlite.Store
	eventWatchers      map[string]map[chan struct{}]struct{}
	storeError         error
	runtimeRelease     func()
	tools              map[string]tool.Definition
	policies           map[string]effectivePolicy
	records            map[string]*record
	workflows          map[string]*workflow
	tenants            map[string]*tenantState
	tenantOrder        []string
	rrCursor           int
	queue              []*record
	terminal           []terminalGroup
	terminalCount      int
	pending            int
	running            int
	closing            bool
	halted             bool
	expiry             chan struct{}
	stop               chan struct{}
	done               chan struct{}
	workers            sync.WaitGroup
}

// NewManager freezes the registry and starts a fixed worker pool plus one queue
// deadline monitor. Call Close to stop them.
func NewManager(config Config, registry *tool.Registry) (*Manager, error) {
	m, err := newManager(config, registry, nil)
	if err != nil {
		return nil, err
	}
	m.startWorkers()
	return m, nil
}

func newManager(config Config, registry *tool.Registry, store *sqlite.Store) (*Manager, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if registry == nil {
		return nil, errors.New("nil tool registry")
	}
	definitions := registry.Snapshot()
	policies := make(map[string]effectivePolicy, len(definitions))
	for name, def := range definitions {
		policy, err := resolveToolPolicy(def.Policy, config)
		if err != nil {
			return nil, fmt.Errorf("tool %q policy: %w", name, err)
		}
		policies[name] = policy
	}
	m := &Manager{
		config:        config,
		store:         store,
		eventWatchers: make(map[string]map[chan struct{}]struct{}),
		tools:         definitions,
		policies:      policies,
		records:       make(map[string]*record),
		workflows:     make(map[string]*workflow),
		tenants:       make(map[string]*tenantState),
		expiry:        make(chan struct{}, 1),
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}
	m.cond = sync.NewCond(&m.mu)
	return m, nil
}

func (m *Manager) startWorkers() {
	config := m.config
	count := config.Workers + 1
	if m.store != nil {
		count++
	}
	m.workers.Add(count)
	for range config.Workers {
		go m.worker()
	}
	go m.expireQueued()
	if m.store != nil {
		go m.prunePeriodically()
	}
	go func() {
		m.workers.Wait()
		close(m.done)
	}()
}

// Submit validates a request before admission and returns its initial queued
// state. Queue limits count accepted work not yet claimed by a worker.
func (m *Manager) Submit(ctx context.Context, req Request) (Snapshot, error) {
	return m.submit(ctx, req, "")
}

// SubmitIdempotent is available only on a durable manager. The key is scoped
// to the authenticated tenant and single-execution submission endpoint.
func (m *Manager) SubmitIdempotent(ctx context.Context, req Request, key string) (Snapshot, error) {
	if m.store == nil || key == "" {
		return Snapshot{}, errors.New("idempotent submission requires a durable manager and nonempty key")
	}
	return m.submit(ctx, req, key)
}

func (m *Manager) submit(ctx context.Context, req Request, key string) (Snapshot, error) {
	prepared, err := m.prepareRequest(ctx, req)
	if err != nil {
		return Snapshot{}, err
	}
	id, err := newID()
	if err != nil {
		return Snapshot{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.admissionError(ctx); err != nil {
		return Snapshot{}, err
	}
	var normalizedRequest []byte
	if m.store != nil {
		normalizedRequest, err = encodeNormalizedRequest(prepared)
		if err != nil {
			return Snapshot{}, err
		}
		if key != "" {
			oldID, found, err := m.store.ResolveIdempotency(ctx, prepared.request.TenantID, executionEndpoint, key, normalizedRequest)
			if err != nil {
				if !errors.Is(err, sqlite.ErrIdempotencyConflict) && ctx.Err() == nil {
					m.haltStoreLocked(err)
				}
				return Snapshot{}, err
			}
			if found {
				return m.getLocked(oldID)
			}
		}
	}
	if m.pending >= m.config.QueueCapacity {
		return Snapshot{}, ErrQueueFull
	}
	if state := m.tenants[prepared.request.TenantID]; state != nil && state.pending >= m.config.TenantPendingLimit {
		return Snapshot{}, ErrTenantQueueFull
	}
	if m.store != nil {
		if err := m.pruneDurableLocked(1); err != nil {
			return Snapshot{}, err
		}
	}
	now := time.Now().UTC()
	r := prepared.makeRecord(id, "", "", now, true)
	if m.store != nil {
		// Once admission begins, its database transaction must finish even if
		// the submitting client disconnects. A retry can resolve the key.
		persistCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		oldID, reused, err := m.createStoredExecutionLocked(persistCtx, r, normalizedRequest, key)
		if err != nil {
			return Snapshot{}, err
		}
		if reused {
			return m.getLocked(oldID)
		}
	}
	m.records[id] = r
	m.queue = append(m.queue, r)
	m.admitPendingLocked(r)
	m.cond.Signal()
	m.wakeExpiry()
	return cloneSnapshot(r.snapshot), nil
}

type preparedRequest struct {
	request      Request
	arguments    json.RawMessage
	policy       effectivePolicy
	queueTimeout time.Duration
	runTimeout   time.Duration
	outputLimit  int
}

func (m *Manager) prepareRequest(ctx context.Context, req Request) (preparedRequest, error) {
	if err := ctx.Err(); err != nil {
		return preparedRequest{}, err
	}
	if len(req.TenantID) > m.config.MaxTenantIDBytes {
		return preparedRequest{}, ErrTenantIDTooLarge
	}
	if req.TenantID == "" {
		req.TenantID = "local"
	}
	if len(req.AgentID) > m.config.MaxAgentIDBytes {
		return preparedRequest{}, ErrAgentIDTooLarge
	}
	def, ok := m.tools[req.Tool]
	if !ok {
		return preparedRequest{}, fmt.Errorf("%w: unknown tool %q", ErrInvalidRequest, req.Tool)
	}
	policy := m.policies[req.Tool]
	queueTimeout, err := resolveTimeout(req.QueueTimeout, policy.defaultQueueTimeout, policy.maxQueueTimeout)
	if err != nil {
		return preparedRequest{}, fmt.Errorf("%w: queue timeout: %v", ErrInvalidRequest, err)
	}
	runTimeout, err := resolveTimeout(req.ExecutionTimeout, policy.defaultRunTimeout, policy.maxRunTimeout)
	if err != nil {
		return preparedRequest{}, fmt.Errorf("%w: execution timeout: %v", ErrInvalidRequest, err)
	}
	outputLimit := req.OutputLimitBytes
	if outputLimit < 0 || outputLimit > policy.maxOutputBytes {
		return preparedRequest{}, fmt.Errorf("%w: output limit outside allowed range", ErrInvalidRequest)
	}
	if outputLimit == 0 {
		outputLimit = policy.defaultOutputBytes
	}
	if len(req.Arguments) > policy.maxArgumentBytes {
		return preparedRequest{}, ErrArgumentsTooLarge
	}
	arguments := append(json.RawMessage(nil), req.Arguments...)
	if len(arguments) > 0 && !json.Valid(arguments) {
		return preparedRequest{}, fmt.Errorf("%w: tool arguments must be JSON", ErrInvalidRequest)
	}
	if err := def.Validate(arguments); err != nil {
		return preparedRequest{}, fmt.Errorf("%w: invalid tool arguments: %v", ErrInvalidRequest, err)
	}
	return preparedRequest{request: req, arguments: arguments, policy: policy, queueTimeout: queueTimeout, runTimeout: runTimeout, outputLimit: outputLimit}, nil
}

func (m *Manager) admissionError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.closing {
		return ErrClosed
	}
	if m.halted {
		if m.storeError != nil {
			return fmt.Errorf("%w: storage: %v", ErrBackendUnavailable, m.storeError)
		}
		return ErrBackendUnavailable
	}
	return nil
}

func (p preparedRequest) makeRecord(id, workflowID, nodeID string, now time.Time, ready bool) *record {
	state := Blocked
	var readyAt *time.Time
	if ready {
		state = Queued
		readyAt = &now
	}
	return &record{snapshot: Snapshot{
		ID:                 id,
		TenantID:           p.request.TenantID,
		AgentID:            p.request.AgentID,
		Tool:               p.request.Tool,
		WorkflowID:         workflowID,
		NodeID:             nodeID,
		State:              state,
		CleanupStatus:      "not_needed",
		CreatedAt:          now,
		ReadyAt:            readyAt,
		QueueTimeout:       p.queueTimeout,
		ExecutionTimeout:   p.runTimeout,
		OutputLimitBytes:   p.outputLimit,
		ArgumentLimitBytes: p.policy.maxArgumentBytes,
		ResultLimitBytes:   p.policy.maxResultBytes,
	}, arguments: p.arguments, deadline: now.Add(p.queueTimeout)}
}

func resolveTimeout(given, fallback, maximum time.Duration) (time.Duration, error) {
	if given < 0 || given > maximum {
		return 0, errors.New("outside allowed range")
	}
	if given == 0 {
		return fallback, nil
	}
	return given, nil
}

func newID() (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", fmt.Errorf("create execution ID: %w", err)
	}
	return hex.EncodeToString(data[:]), nil
}

// Get returns a copy of a retained execution record.
func (m *Manager) Get(id string) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.getLocked(id)
}

func (m *Manager) getLocked(id string) (Snapshot, error) {
	r, ok := m.records[id]
	if !ok {
		if m.store == nil {
			return Snapshot{}, ErrNotFound
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		row, err := m.store.GetExecution(ctx, id)
		if errors.Is(err, sqlite.ErrNotFound) {
			return Snapshot{}, ErrNotFound
		}
		if err != nil {
			return Snapshot{}, err
		}
		recovered, err := decodeStoredExecution(row)
		if err != nil {
			return Snapshot{}, err
		}
		return cloneSnapshot(recovered.snapshot), nil
	}
	return cloneSnapshot(r.snapshot), nil
}

// Cancel atomically accepts a stop request or returns the already committed
// terminal state. Repeated calls while stopping are idempotent.
func (m *Manager) Cancel(id string) (Snapshot, error) {
	m.mu.Lock()
	r, ok := m.records[id]
	if !ok {
		if m.store != nil {
			snapshot, err := m.getLocked(id)
			m.mu.Unlock()
			if err == nil && snapshot.FinishedAt != nil {
				return snapshot, nil
			}
			if err != nil {
				return Snapshot{}, err
			}
			return Snapshot{}, ErrNotFound
		}
		m.mu.Unlock()
		return Snapshot{}, ErrNotFound
	}
	var cancel context.CancelFunc
	switch r.snapshot.State {
	case Blocked:
		if err := m.finishLocked(r, Cancelled, "cancelled", "cancelled before execution"); err != nil {
			m.mu.Unlock()
			return Snapshot{}, err
		}
		m.releasePendingLocked(r)
		m.wakeExpiry()
	case Queued:
		if err := m.finishLocked(r, Cancelled, "cancelled", "cancelled before execution"); err != nil {
			m.mu.Unlock()
			return Snapshot{}, err
		}
		m.removeQueued(r)
		m.releasePendingLocked(r)
		m.wakeExpiry()
	case Running:
		next := cloneSnapshot(r.snapshot)
		next.State = Cancelling
		next.StopReason = "user_cancelled"
		if err := m.applySnapshotLocked(r, next); err != nil {
			cancel = r.cancel
			m.mu.Unlock()
			if cancel != nil {
				cancel()
			}
			return Snapshot{}, err
		}
		cancel = r.cancel
	}
	snapshot := cloneSnapshot(r.snapshot)
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return snapshot, nil
}

// Stats returns queue, running, and retained record counts.
func (m *Manager) Stats() Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Stats{Queued: len(m.queue), Blocked: m.pending - len(m.queue), Pending: m.pending, Running: m.running, Stored: len(m.records), Tenants: len(m.tenants), Halted: m.halted, Closing: m.closing}
}

// Close stops admission, cancels accepted work, and waits for all owned
// goroutines. A context deadline bounds the caller's wait while cleanup keeps
// running until cooperative executors return.
func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	var cancels []context.CancelFunc
	var closeErr error
	if !m.closing {
		m.closing = true
		for _, w := range m.workflows {
			if w.remaining > 0 {
				if err := m.transitionWorkflowLocked(w, Cancelling, nil); err != nil {
					closeErr = errors.Join(closeErr, err)
				}
				w.cancelled = true
			}
		}
		for len(m.queue) > 0 {
			r := m.queue[0]
			m.queue[0] = nil
			m.queue = m.queue[1:]
			if err := m.finishLocked(r, Cancelled, "cancelled", "manager closed before execution"); err != nil {
				closeErr = errors.Join(closeErr, err)
			}
			m.releasePendingLocked(r)
		}
		for _, r := range m.records {
			if r.snapshot.State == Blocked {
				if err := m.finishLocked(r, Cancelled, "cancelled", "manager closed before execution"); err != nil {
					closeErr = errors.Join(closeErr, err)
				}
				m.releasePendingLocked(r)
			}
			if r.snapshot.State == Running {
				next := cloneSnapshot(r.snapshot)
				next.State = Cancelling
				next.StopReason = "service_shutdown"
				if err := m.applySnapshotLocked(r, next); err != nil {
					closeErr = errors.Join(closeErr, err)
				}
				cancels = append(cancels, r.cancel)
			}
		}
		close(m.stop)
		m.cond.Broadcast()
	}
	m.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	select {
	case <-m.done:
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.running != 0 {
			return errors.Join(closeErr, m.storeError, ErrStopUnconfirmed)
		}
		if m.runtimeRelease != nil {
			m.runtimeRelease()
			m.runtimeRelease = nil
		}
		return errors.Join(closeErr, m.storeError)
	case <-ctx.Done():
		return errors.Join(closeErr, ctx.Err())
	}
}

func cloneSnapshot(s Snapshot) Snapshot {
	s.Result = append(json.RawMessage(nil), s.Result...)
	if s.ReadyAt != nil {
		value := *s.ReadyAt
		s.ReadyAt = &value
	}
	if s.RunningAt != nil {
		value := *s.RunningAt
		s.RunningAt = &value
	}
	if s.FinishedAt != nil {
		value := *s.FinishedAt
		s.FinishedAt = &value
	}
	if s.ProcessStartedAt != nil {
		value := *s.ProcessStartedAt
		s.ProcessStartedAt = &value
	}
	if s.ExitCode != nil {
		value := *s.ExitCode
		s.ExitCode = &value
	}
	return s
}

func (m *Manager) removeQueued(target *record) {
	for i, r := range m.queue {
		if r == target {
			copy(m.queue[i:], m.queue[i+1:])
			m.queue[len(m.queue)-1] = nil
			m.queue = m.queue[:len(m.queue)-1]
			return
		}
	}
}

func (m *Manager) finishLocked(r *record, state State, code, message string) error {
	return m.finishWithSnapshotLocked(r, r.snapshot, state, code, message)
}

func (m *Manager) finishWithSnapshotLocked(r *record, base Snapshot, state State, code, message string) error {
	if r.snapshot.FinishedAt != nil {
		return nil
	}
	next := cloneSnapshot(base)
	next.State = state
	next.ErrorCode = code
	next.ErrorMessage = m.limitErrorMessage(message)
	now := time.Now().UTC()
	next.FinishedAt = &now
	if err := m.applySnapshotKindLocked(r, next, "end"); err != nil {
		return err
	}
	r.arguments = nil
	if r.workflow != nil {
		if err := m.workflowNodeFinishedLocked(r); err != nil {
			return err
		}
	} else {
		m.retainTerminalLocked(terminalGroup{ids: []string{r.snapshot.ID}})
	}
	return nil
}

func (m *Manager) limitErrorMessage(message string) string {
	if len(message) > m.config.MaxErrorMessageBytes {
		message = message[:m.config.MaxErrorMessageBytes]
	}
	return strings.ToValidUTF8(message, "?")
}

func (m *Manager) wakeExpiry() {
	select {
	case m.expiry <- struct{}{}:
	default:
	}
}
