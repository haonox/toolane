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

	"github.com/haonox/toolane/internal/storage/sqlite"
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
	ErrInvalidRequest    = errors.New("invalid execution request")
	// ErrTenantIDTooLarge indicates that a tenant ID exceeds its byte limit.
	ErrTenantIDTooLarge = errors.New("tenant ID exceeds limit")
	// ErrTenantQueueFull indicates that a tenant's pending capacity is exhausted.
	ErrTenantQueueFull = errors.New("tenant pending queue full")
	// ErrBackendUnavailable indicates that new work is paused after an unconfirmed stop.
	ErrBackendUnavailable  = errors.New("execution backend unavailable")
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different request")
	// ErrStopUnconfirmed indicates that shutdown could not confirm all work stopped.
	ErrStopUnconfirmed = errors.New("execution stop could not be confirmed")
)

// State describes the public execution lifecycle.
type State string

const (
	Queued      State = "queued"
	Running     State = "running"
	Cancelling  State = "cancelling"
	Succeeded   State = "succeeded"
	Failed      State = "failed"
	TimedOut    State = "timed_out"
	Cancelled   State = "cancelled"
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
	RetainedResults      int
	ResultRetention      time.Duration
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
		RetainedResults:      1000,
		ResultRetention:      24 * time.Hour,
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
	if c.Workers <= 0 || c.QueueCapacity <= 0 || c.TenantRunningLimit <= 0 || c.TenantPendingLimit <= 0 || c.MaxTenantIDBytes <= 0 || c.RetainedResults <= 0 || c.MaxResultBytes <= 0 || c.MaxArgumentBytes <= 0 || c.MaxOutputBytes <= 0 || c.MaxAgentIDBytes <= 0 || c.MaxErrorMessageBytes <= 0 {
		return errors.New("workers, global and tenant capacities, retained results and byte limits must be positive")
	}
	if c.MaxTenantIDBytes < len("local") {
		return errors.New("tenant ID byte limit must allow the local tenant")
	}
	if c.ResultRetention <= 0 || c.DefaultQueueTimeout <= 0 || c.MaxQueueTimeout < c.DefaultQueueTimeout ||
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
	IdempotencyKey   string
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
	snapshot       Snapshot
	arguments      json.RawMessage
	deadline       time.Time
	cancel         context.CancelFunc
	tenant         *tenantState
	storeSequence  int64
	storeVersion   int64
	idempotencyKey string
	requestHash    string
}

// Stats reports current capacity use. It is a snapshot, not a reservation.
type Stats struct {
	Queued  int
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
	tools              map[string]tool.Definition
	policies           map[string]effectivePolicy
	records            map[string]*record
	store              *sqlite.Store
	storageErr         error
	tenants            map[string]*tenantState
	tenantOrder        []string
	rrCursor           int
	queue              []*record
	terminal           []string
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
func NewManager(config Config, registry *tool.Registry, observers ...func(Snapshot, Snapshot)) (*Manager, error) {
	return newManager(config, registry, nil, observers...)
}

// NewPersistentManager opens a durable execution store and recovers accepted
// work before starting workers. Close releases the database lock.
func NewPersistentManager(config Config, registry *tool.Registry, databasePath string, observers ...func(Snapshot, Snapshot)) (*Manager, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if registry == nil {
		return nil, errors.New("nil tool registry")
	}
	store, err := sqlite.Open(databasePath)
	if err != nil {
		return nil, err
	}
	m, err := newManager(config, registry, store, observers...)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	return m, nil
}

func newManager(config Config, registry *tool.Registry, store *sqlite.Store, observers ...func(Snapshot, Snapshot)) (*Manager, error) {
	if len(observers) > 1 {
		return nil, errors.New("at most one observer allowed")
	}
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
		config:   config,
		store:    store,
		tools:    definitions,
		policies: policies,
		records:  make(map[string]*record),
		tenants:  make(map[string]*tenantState),
		expiry:   make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	if len(observers) == 1 {
		m.transitionObserver = observers[0]
	}
	m.cond = sync.NewCond(&m.mu)
	if store != nil {
		if err := m.recoverStored(); err != nil {
			return nil, err
		}
		if _, err := m.cleanupExpiredLocked(); err != nil {
			return nil, err
		}
	}
	m.workers.Add(config.Workers + 1)
	for range config.Workers {
		go m.worker()
	}
	go m.expireQueued()
	if store != nil {
		m.workers.Add(1)
		go m.expireTerminal()
	}
	go func() {
		m.workers.Wait()
		if m.store != nil {
			if err := m.store.Close(); err != nil {
				m.mu.Lock()
				m.storageErr = errors.Join(m.storageErr, err)
				m.mu.Unlock()
			}
		}
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
	var requestHash string
	if prepared.request.IdempotencyKey != "" {
		if m.store == nil {
			return Snapshot{}, fmt.Errorf("%w: idempotency requires persistent storage", ErrInvalidRequest)
		}
		requestHash, err = hashPrepared(prepared)
		if err != nil {
			return Snapshot{}, err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.admissionError(ctx); err != nil {
		return Snapshot{}, err
	}
	if prepared.request.IdempotencyKey != "" {
		stored, err := m.store.FindByIdempotency(context.Background(), prepared.request.TenantID, prepared.request.IdempotencyKey)
		switch {
		case err == nil:
			r, decodeErr := decodeStored(stored)
			if decodeErr != nil {
				m.haltStorageLocked(decodeErr)
				return Snapshot{}, ErrBackendUnavailable
			}
			if m.expiredSnapshot(r.snapshot, time.Now()) {
				deleted, deleteErr := m.store.DeleteExpiredID(context.Background(), stored.ID, time.Now().Add(-m.config.ResultRetention))
				if deleteErr != nil || !deleted {
					m.haltStorageLocked(errors.Join(deleteErr, errors.New("expired key could not be released")))
					return Snapshot{}, ErrBackendUnavailable
				}
				m.evictTerminalLocked(stored.ID)
			} else {
				if stored.RequestHash != requestHash {
					return Snapshot{}, ErrIdempotencyConflict
				}
				return cloneSnapshot(r.snapshot), nil
			}
		case errors.Is(err, sqlite.ErrNotFound):
		default:
			m.haltStorageLocked(err)
			return Snapshot{}, ErrBackendUnavailable
		}
	}
	if m.pending >= m.config.QueueCapacity {
		return Snapshot{}, ErrQueueFull
	}
	if state := m.tenants[prepared.request.TenantID]; state != nil && state.pending >= m.config.TenantPendingLimit {
		return Snapshot{}, ErrTenantQueueFull
	}
	id, err := newID()
	if err != nil {
		return Snapshot{}, err
	}
	now := time.Now().UTC()
	r := prepared.makeRecord(id, now)
	r.idempotencyKey, r.requestHash = prepared.request.IdempotencyKey, requestHash
	if m.store != nil {
		image, err := m.storedRecord(r, r.snapshot)
		if err != nil {
			return Snapshot{}, err
		}
		stored, err := m.store.Insert(context.Background(), image)
		if err != nil {
			m.haltStorageLocked(err)
			return Snapshot{}, ErrBackendUnavailable
		}
		r.storeSequence, r.storeVersion = stored.Sequence, stored.Version
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
	if req.IdempotencyKey != "" && !validIdempotencyKey(req.IdempotencyKey) {
		return preparedRequest{}, fmt.Errorf("%w: invalid idempotency key", ErrInvalidRequest)
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
	if len(req.Arguments) > 0 && !json.Valid(req.Arguments) {
		return preparedRequest{}, fmt.Errorf("%w: invalid JSON arguments", ErrInvalidRequest)
	}
	arguments := append(json.RawMessage(nil), req.Arguments...)
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
		return ErrBackendUnavailable
	}
	return nil
}

func (p preparedRequest) makeRecord(id string, now time.Time) *record {
	return &record{snapshot: Snapshot{ID: id, TenantID: p.request.TenantID, AgentID: p.request.AgentID,
		Tool: p.request.Tool, State: Queued, CleanupStatus: "not_needed", CreatedAt: now, ReadyAt: &now,
		QueueTimeout: p.queueTimeout, ExecutionTimeout: p.runTimeout, OutputLimitBytes: p.outputLimit,
		ArgumentLimitBytes: p.policy.maxArgumentBytes, ResultLimitBytes: p.policy.maxResultBytes},
		arguments: p.arguments, deadline: now.Add(p.queueTimeout)}
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

// Get returns a copy of an execution record, loading an evicted terminal
// snapshot from SQLite for persistent managers.
func (m *Manager) Get(id string) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.records[id]
	if !ok {
		if m.store == nil {
			return Snapshot{}, ErrNotFound
		}
		stored, err := m.store.Get(context.Background(), id)
		if errors.Is(err, sqlite.ErrNotFound) {
			return Snapshot{}, ErrNotFound
		}
		if err != nil {
			m.haltStorageLocked(err)
			return Snapshot{}, ErrBackendUnavailable
		}
		r, err = decodeStored(stored)
		if err != nil {
			m.haltStorageLocked(err)
			return Snapshot{}, ErrBackendUnavailable
		}
	}
	if m.expiredSnapshot(r.snapshot, time.Now()) {
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
		if m.store != nil {
			stored, err := m.store.Get(context.Background(), id)
			if err == nil {
				r, err = decodeStored(stored)
				if err == nil && r.snapshot.FinishedAt != nil && !m.expiredSnapshot(r.snapshot, time.Now()) {
					snapshot := cloneSnapshot(r.snapshot)
					m.mu.Unlock()
					return snapshot, nil
				}
			}
			if err != nil && !errors.Is(err, sqlite.ErrNotFound) {
				m.haltStorageLocked(err)
				m.mu.Unlock()
				return Snapshot{}, ErrBackendUnavailable
			}
		}
		m.mu.Unlock()
		return Snapshot{}, ErrNotFound
	}
	if m.expiredSnapshot(r.snapshot, time.Now()) {
		m.mu.Unlock()
		return Snapshot{}, ErrNotFound
	}
	var cancel context.CancelFunc
	switch r.snapshot.State {
	case Queued:
		if !m.finishLocked(r, Cancelled, "cancelled", "cancelled before execution") {
			m.mu.Unlock()
			return Snapshot{}, ErrBackendUnavailable
		}
		m.removeQueued(r)
		m.releasePendingLocked(r)
		m.wakeExpiry()
	case Running:
		next := cloneSnapshot(r.snapshot)
		next.State = Cancelling
		next.StopReason = "user_cancelled"
		if !m.applySnapshotLocked(r, next) {
			m.mu.Unlock()
			return Snapshot{}, ErrBackendUnavailable
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
	return Stats{Queued: len(m.queue), Pending: m.pending, Running: m.running, Stored: len(m.records), Tenants: len(m.tenants), Halted: m.halted, Closing: m.closing}
}

// Close stops admission and running work, then waits for owned goroutines.
// Persistent managers leave queued work on disk for the next startup;
// in-memory managers cancel it. A context deadline bounds the caller's wait.
func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	var cancels []context.CancelFunc
	if !m.closing {
		m.closing = true
		if m.store == nil {
			for len(m.queue) > 0 {
				r := m.queue[0]
				m.queue[0] = nil
				m.queue = m.queue[1:]
				m.releasePendingLocked(r)
				m.finishLocked(r, Cancelled, "cancelled", "manager closed before execution")
			}
		}
		for _, r := range m.records {
			if r.snapshot.State == Running {
				next := cloneSnapshot(r.snapshot)
				next.State = Cancelling
				next.StopReason = "service_shutdown"
				m.applySnapshotLocked(r, next)
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
			return errors.Join(ErrStopUnconfirmed, m.storageErr)
		}
		return m.storageErr
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

func (m *Manager) applySnapshotLocked(r *record, next Snapshot) bool {
	if err := m.persistSnapshotLocked(r, next); err != nil {
		m.haltStorageLocked(err)
		return false
	}
	before := r.snapshot
	r.snapshot = next
	if m.transitionObserver != nil {
		m.transitionObserver(before, next)
	}
	return true
}

func (m *Manager) finishLocked(r *record, state State, code, message string) bool {
	return m.finishWithSnapshotLocked(r, r.snapshot, state, code, message)
}

func (m *Manager) finishWithSnapshotLocked(r *record, base Snapshot, state State, code, message string) bool {
	if r.snapshot.FinishedAt != nil {
		return true
	}
	next := cloneSnapshot(base)
	next.State, next.ErrorCode = state, code
	next.ErrorMessage = m.limitErrorMessage(message)
	now := time.Now().UTC()
	next.FinishedAt = &now
	if !m.applySnapshotLocked(r, next) {
		return false
	}
	r.arguments = nil
	m.terminal = append(m.terminal, r.snapshot.ID)
	for len(m.terminal) > m.config.RetainedResults {
		delete(m.records, m.terminal[0])
		m.terminal[0] = ""
		m.terminal = m.terminal[1:]
	}
	return true
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
