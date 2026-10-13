// Package execution owns the in-memory lifecycle of local tool executions.
package execution

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/haonox/toolane/internal/tool"
)

var (
	ErrNotFound           = errors.New("execution not found")
	ErrQueueFull          = errors.New("execution queue full")
	ErrClosed             = errors.New("execution manager closed")
	ErrArgumentsTooLarge  = errors.New("tool arguments exceed limit")
	ErrBackendUnavailable = errors.New("execution backend unavailable")
	ErrStopUnconfirmed    = errors.New("execution stop could not be confirmed")
)

// State describes the public execution lifecycle.
type State string

const (
	Queued     State = "queued"
	Running    State = "running"
	Cancelling State = "cancelling"
	Succeeded  State = "succeeded"
	Failed     State = "failed"
	TimedOut   State = "timed_out"
	Cancelled  State = "cancelled"
)

// Config bounds all active work and retained results. Use DefaultConfig as a
// starting point, then change fields before creating a Manager.
type Config struct {
	Workers             int
	QueueCapacity       int
	RetainedResults     int
	DefaultQueueTimeout time.Duration
	MaxQueueTimeout     time.Duration
	DefaultRunTimeout   time.Duration
	MaxRunTimeout       time.Duration
	MaxResultBytes      int
	MaxArgumentBytes    int
	MaxOutputBytes      int
}

func DefaultConfig() Config {
	return Config{
		Workers:             4,
		QueueCapacity:       256,
		RetainedResults:     1000,
		DefaultQueueTimeout: 30 * time.Second,
		MaxQueueTimeout:     300 * time.Second,
		DefaultRunTimeout:   10 * time.Second,
		MaxRunTimeout:       60 * time.Second,
		MaxResultBytes:      64 * 1024,
		MaxArgumentBytes:    256 * 1024,
		MaxOutputBytes:      1024 * 1024,
	}
}

func (c Config) validate() error {
	if c.Workers <= 0 || c.QueueCapacity <= 0 || c.RetainedResults <= 0 || c.MaxResultBytes <= 0 || c.MaxArgumentBytes <= 0 || c.MaxOutputBytes <= 0 {
		return errors.New("workers, queue capacity, retained results and byte limits must be positive")
	}
	if c.DefaultQueueTimeout <= 0 || c.MaxQueueTimeout < c.DefaultQueueTimeout ||
		c.DefaultRunTimeout <= 0 || c.MaxRunTimeout < c.DefaultRunTimeout {
		return errors.New("invalid queue or execution timeout range")
	}
	return nil
}

// Request is copied on admission. A cancelled Submit context prevents
// admission, but does not become the accepted execution's lifetime context.
type Request struct {
	AgentID          string
	Tool             string
	Arguments        json.RawMessage
	QueueTimeout     time.Duration
	ExecutionTimeout time.Duration
	OutputLimitBytes int
}

// Snapshot is an immutable view of an execution at one point in time.
type Snapshot struct {
	ID               string          `json:"execution_id"`
	TenantID         string          `json:"tenant_id"`
	AgentID          string          `json:"agent_id,omitempty"`
	Tool             string          `json:"tool"`
	State            State           `json:"status"`
	ErrorCode        string          `json:"error_code,omitempty"`
	ErrorMessage     string          `json:"error_message,omitempty"`
	StopReason       string          `json:"stop_reason,omitempty"`
	CleanupStatus    string          `json:"cleanup_status"`
	Result           json.RawMessage `json:"result,omitempty"`
	Stdout           string          `json:"stdout,omitempty"`
	Stderr           string          `json:"stderr,omitempty"`
	ExitCode         *int            `json:"exit_code,omitempty"`
	OutputTruncated  bool            `json:"output_truncated"`
	CreatedAt        time.Time       `json:"created_at"`
	ReadyAt          time.Time       `json:"ready_at"`
	RunningAt        *time.Time      `json:"running_at,omitempty"`
	FinishedAt       *time.Time      `json:"finished_at,omitempty"`
	ProcessStartedAt *time.Time      `json:"process_started_at,omitempty"`
	QueueTimeout     time.Duration   `json:"-"`
	ExecutionTimeout time.Duration   `json:"-"`
	OutputLimitBytes int             `json:"-"`
}

type record struct {
	snapshot  Snapshot
	arguments json.RawMessage
	deadline  time.Time
	cancel    context.CancelFunc
}

// Stats reports current capacity use. It is a snapshot, not a reservation.
type Stats struct {
	Queued  int
	Running int
	Stored  int
	Halted  bool
}

// Manager owns state changes, the FIFO queue, worker slots and cancellations.
// Each admitted running tool occupies one worker until Execute returns; the
// executor must stop its work before returning after cancellation.
type Manager struct {
	mu       sync.Mutex
	cond     *sync.Cond
	config   Config
	tools    map[string]tool.Definition
	records  map[string]*record
	queue    []*record
	terminal []string
	running  int
	closing  bool
	halted   bool
	expiry   chan struct{}
	stop     chan struct{}
	done     chan struct{}
	workers  sync.WaitGroup
}

// NewManager freezes the registry and starts a fixed worker pool and one queue
// deadline monitor. Call Close to stop them.
func NewManager(config Config, registry *tool.Registry) (*Manager, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if registry == nil {
		return nil, errors.New("nil tool registry")
	}
	m := &Manager{
		config:  config,
		tools:   registry.Snapshot(),
		records: make(map[string]*record),
		expiry:  make(chan struct{}, 1),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
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
// state. Queue limits count accepted work that has not been claimed by a worker.
func (m *Manager) Submit(ctx context.Context, req Request) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	def, ok := m.tools[req.Tool]
	if !ok {
		return Snapshot{}, fmt.Errorf("unknown tool %q", req.Tool)
	}
	queueTimeout, err := resolveTimeout(req.QueueTimeout, m.config.DefaultQueueTimeout, m.config.MaxQueueTimeout)
	if err != nil {
		return Snapshot{}, fmt.Errorf("queue timeout: %w", err)
	}
	runTimeout, err := resolveTimeout(req.ExecutionTimeout, m.config.DefaultRunTimeout, m.config.MaxRunTimeout)
	if err != nil {
		return Snapshot{}, fmt.Errorf("execution timeout: %w", err)
	}
	outputLimit := req.OutputLimitBytes
	if outputLimit < 0 || outputLimit > m.config.MaxOutputBytes {
		return Snapshot{}, errors.New("output limit outside allowed range")
	}
	if outputLimit == 0 {
		outputLimit = m.config.MaxOutputBytes
	}
	arguments := append(json.RawMessage(nil), req.Arguments...)
	if len(arguments) > m.config.MaxArgumentBytes {
		return Snapshot{}, ErrArgumentsTooLarge
	}
	if err := def.Validate(arguments); err != nil {
		return Snapshot{}, fmt.Errorf("invalid tool arguments: %w", err)
	}
	id, err := newID()
	if err != nil {
		return Snapshot{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if m.closing {
		return Snapshot{}, ErrClosed
	}
	if m.halted {
		return Snapshot{}, ErrBackendUnavailable
	}
	if len(m.queue) >= m.config.QueueCapacity {
		return Snapshot{}, ErrQueueFull
	}
	now := time.Now().UTC()
	r := &record{snapshot: Snapshot{
		ID:               id,
		TenantID:         "local",
		AgentID:          req.AgentID,
		Tool:             req.Tool,
		State:            Queued,
		CleanupStatus:    "not_needed",
		CreatedAt:        now,
		ReadyAt:          now,
		QueueTimeout:     queueTimeout,
		ExecutionTimeout: runTimeout,
		OutputLimitBytes: outputLimit,
	}, arguments: arguments, deadline: now.Add(queueTimeout)}
	m.records[id] = r
	m.queue = append(m.queue, r)
	m.cond.Signal()
	m.wakeExpiry()
	return cloneSnapshot(r.snapshot), nil
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
	case Queued:
		m.removeQueued(r)
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

// Stats returns queue, running and retained record counts.
func (m *Manager) Stats() Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Stats{Queued: len(m.queue), Running: m.running, Stored: len(m.records), Halted: m.halted}
}

// Close stops admission, cancels accepted work and waits for all owned
// goroutines. A context deadline bounds the caller's wait, while cleanup keeps
// running until cooperative executors actually return.
func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	var cancels []context.CancelFunc
	if !m.closing {
		m.closing = true
		for len(m.queue) > 0 {
			r := m.queue[0]
			m.queue = m.queue[1:]
			m.finishLocked(r, Cancelled, "cancelled", "manager closed before execution")
		}
		for _, r := range m.records {
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
	r.snapshot.State = state
	r.snapshot.ErrorCode = code
	r.snapshot.ErrorMessage = message
	now := time.Now().UTC()
	r.snapshot.FinishedAt = &now
	r.arguments = nil
	m.terminal = append(m.terminal, r.snapshot.ID)
	for len(m.terminal) > m.config.RetainedResults {
		id := m.terminal[0]
		m.terminal[0] = ""
		m.terminal = m.terminal[1:]
		delete(m.records, id)
	}
}

func (m *Manager) wakeExpiry() {
	select {
	case m.expiry <- struct{}{}:
	default:
	}
}
