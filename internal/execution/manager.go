// Package execution owns the in-memory lifecycle of local tool executions.
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
	Blocked    State = "blocked"
	Queued     State = "queued"
	Running    State = "running"
	Cancelling State = "cancelling"
	Succeeded  State = "succeeded"
	Failed     State = "failed"
	TimedOut   State = "timed_out"
	Cancelled  State = "cancelled"
	Skipped    State = "skipped"
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
	if c.Workers <= 0 || c.QueueCapacity <= 0 || c.TenantRunningLimit <= 0 || c.TenantPendingLimit <= 0 || c.MaxTenantIDBytes <= 0 || c.RetainedResults <= 0 || c.MaxWorkflowNodes <= 0 || c.MaxWorkflowBytes <= 0 || c.MaxResultBytes <= 0 || c.MaxArgumentBytes <= 0 || c.MaxOutputBytes <= 0 || c.MaxAgentIDBytes <= 0 || c.MaxErrorMessageBytes <= 0 {
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
	snapshot   Snapshot
	arguments  json.RawMessage
	deadline   time.Time
	cancel     context.CancelFunc
	tenant     *tenantState
	workflow   *workflow
	dependsOn  []*record
	dependents []*record
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
}

// Manager owns state transitions, the ready queue, worker slots, and
// cancellation. A running tool occupies a worker until Execute returns; an
// executor must stop its work before returning after cancellation.
type Manager struct {
	mu            sync.Mutex
	cond          *sync.Cond
	config        Config
	tools         map[string]tool.Definition
	policies      map[string]effectivePolicy
	records       map[string]*record
	workflows     map[string]*workflow
	tenants       map[string]*tenantState
	tenantOrder   []string
	rrCursor      int
	queue         []*record
	terminal      []terminalGroup
	terminalCount int
	pending       int
	running       int
	closing       bool
	halted        bool
	expiry        chan struct{}
	stop          chan struct{}
	done          chan struct{}
	workers       sync.WaitGroup
}

// NewManager freezes the registry and starts a fixed worker pool plus one queue
// deadline monitor. Call Close to stop them.
func NewManager(config Config, registry *tool.Registry) (*Manager, error) {
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
		config:    config,
		tools:     definitions,
		policies:  policies,
		records:   make(map[string]*record),
		workflows: make(map[string]*workflow),
		tenants:   make(map[string]*tenantState),
		expiry:    make(chan struct{}, 1),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	m.cond = sync.NewCond(&m.mu)
	m.workers.Add(config.Workers + 1)
	for range config.Workers {
		go m.worker()
	}
	go m.expireQueued()
	go func() {
		m.workers.Wait()
		close(m.done)
	}()
	return m, nil
}

// Submit validates a request before admission and returns its initial queued
// state. Queue limits count accepted work not yet claimed by a worker.
func (m *Manager) Submit(ctx context.Context, req Request) (Snapshot, error) {
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
	if m.pending >= m.config.QueueCapacity {
		return Snapshot{}, ErrQueueFull
	}
	if state := m.tenants[prepared.request.TenantID]; state != nil && state.pending >= m.config.TenantPendingLimit {
		return Snapshot{}, ErrTenantQueueFull
	}
	now := time.Now().UTC()
	r := prepared.makeRecord(id, "", "", now, true)
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
		return preparedRequest{}, fmt.Errorf("unknown tool %q", req.Tool)
	}
	policy := m.policies[req.Tool]
	queueTimeout, err := resolveTimeout(req.QueueTimeout, policy.defaultQueueTimeout, policy.maxQueueTimeout)
	if err != nil {
		return preparedRequest{}, fmt.Errorf("queue timeout: %w", err)
	}
	runTimeout, err := resolveTimeout(req.ExecutionTimeout, policy.defaultRunTimeout, policy.maxRunTimeout)
	if err != nil {
		return preparedRequest{}, fmt.Errorf("execution timeout: %w", err)
	}
	outputLimit := req.OutputLimitBytes
	if outputLimit < 0 || outputLimit > policy.maxOutputBytes {
		return preparedRequest{}, errors.New("output limit outside allowed range")
	}
	if outputLimit == 0 {
		outputLimit = policy.defaultOutputBytes
	}
	if len(req.Arguments) > policy.maxArgumentBytes {
		return preparedRequest{}, ErrArgumentsTooLarge
	}
	arguments := append(json.RawMessage(nil), req.Arguments...)
	if err := def.Validate(arguments); err != nil {
		return preparedRequest{}, fmt.Errorf("invalid tool arguments: %w", err)
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
	r, ok := m.records[id]
	if !ok {
		return Snapshot{}, ErrNotFound
	}
	return cloneSnapshot(r.snapshot), nil
}

// Cancel atomically accepts a stop request or returns the already committed
// terminal state. Repeated calls while stopping are idempotent.
func (m *Manager) Cancel(id string) (Snapshot, error) {
	m.mu.Lock()
	r, ok := m.records[id]
	if !ok {
		m.mu.Unlock()
		return Snapshot{}, ErrNotFound
	}
	var cancel context.CancelFunc
	switch r.snapshot.State {
	case Blocked:
		m.releasePendingLocked(r)
		m.finishLocked(r, Cancelled, "cancelled", "cancelled before execution")
		m.wakeExpiry()
	case Queued:
		m.removeQueued(r)
		m.releasePendingLocked(r)
		m.finishLocked(r, Cancelled, "cancelled", "cancelled before execution")
		m.wakeExpiry()
	case Running:
		r.snapshot.State = Cancelling
		r.snapshot.StopReason = "user_cancelled"
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
	return Stats{Queued: len(m.queue), Blocked: m.pending - len(m.queue), Pending: m.pending, Running: m.running, Stored: len(m.records), Tenants: len(m.tenants), Halted: m.halted}
}

// Close stops admission, cancels accepted work, and waits for all owned
// goroutines. A context deadline bounds the caller's wait while cleanup keeps
// running until cooperative executors return.
func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	var cancels []context.CancelFunc
	if !m.closing {
		m.closing = true
		for _, w := range m.workflows {
			if w.remaining > 0 {
				w.cancelled = true
				w.state = Cancelling
			}
		}
		for len(m.queue) > 0 {
			r := m.queue[0]
			m.queue[0] = nil
			m.queue = m.queue[1:]
			m.releasePendingLocked(r)
			m.finishLocked(r, Cancelled, "cancelled", "manager closed before execution")
		}
		for _, r := range m.records {
			if r.snapshot.State == Blocked {
				m.releasePendingLocked(r)
				m.finishLocked(r, Cancelled, "cancelled", "manager closed before execution")
			}
			if r.snapshot.State == Running {
				r.snapshot.State = Cancelling
				r.snapshot.StopReason = "service_shutdown"
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
			return ErrStopUnconfirmed
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
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

func (m *Manager) finishLocked(r *record, state State, code, message string) {
	if r.snapshot.FinishedAt != nil {
		return
	}
	r.snapshot.State = state
	r.snapshot.ErrorCode = code
	r.snapshot.ErrorMessage = m.limitErrorMessage(message)
	now := time.Now().UTC()
	r.snapshot.FinishedAt = &now
	r.arguments = nil
	if r.workflow != nil {
		m.workflowNodeFinishedLocked(r)
	} else {
		m.retainTerminalLocked(terminalGroup{ids: []string{r.snapshot.ID}})
	}
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
