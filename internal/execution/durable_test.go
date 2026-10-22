package execution

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/store/sqlite"
	"github.com/haonox/toolane/internal/tool"
)

func durableRegistry(t *testing.T, execute func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error)) *tool.Registry {
	t.Helper()
	registry := tool.NewRegistry()
	if err := registry.Register(tool.Definition{
		Name: "test", Validate: func(json.RawMessage) error { return nil }, Execute: execute,
	}); err != nil {
		t.Fatal(err)
	}
	return registry
}

func openDurableStore(t *testing.T, dir string) *sqlite.Store {
	t.Helper()
	s, err := sqlite.Open(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func closeDurableManager(t *testing.T, m *Manager) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDurableAdmissionClaimAndCompletion(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "state")
	s := openDurableStore(t, dir)
	var seenState State
	registry := durableRegistry(t, func(_ context.Context, _ json.RawMessage, options tool.RunOptions) (tool.Outcome, error) {
		row, err := s.GetExecution(ctx, options.ExecutionID)
		if err != nil {
			return tool.Outcome{}, err
		}
		seenState = State(row.State)
		return tool.Outcome{Result: json.RawMessage(`{"ok":true}`)}, nil
	})
	m, err := NewDurableManager(ctx, DefaultConfig(), registry, s)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := m.Submit(ctx, Request{Tool: "test", Arguments: json.RawMessage(`{"x":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	finished := awaitState(t, m, accepted.ID, Succeeded)
	if seenState != Running || string(finished.Result) != `{"ok":true}` {
		t.Fatalf("worker saw state %q; result=%s", seenState, finished.Result)
	}
	closeDurableManager(t, m)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openDurableStore(t, dir)
	defer s.Close()
	row, err := s.GetExecution(ctx, accepted.ID)
	if err != nil || row.State != string(Succeeded) {
		t.Fatalf("persisted result: %+v err=%v", row, err)
	}
	events, err := s.EventsAfter(ctx, accepted.ID, 0)
	if err != nil || len(events) != 3 || events[0].Sequence != 1 || events[1].Sequence != 2 || events[2].Sequence != 3 {
		t.Fatalf("persisted events: %+v err=%v", events, err)
	}
}

func TestDurableIdempotencyBeforeCapacityCheck(t *testing.T) {
	ctx := context.Background()
	s := openDurableStore(t, filepath.Join(t.TempDir(), "state"))
	defer s.Close()
	config := DefaultConfig()
	config.Workers = 1
	config.QueueCapacity = 1
	config.RetainedResults = 1
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	registry := durableRegistry(t, func(ctx context.Context, _ json.RawMessage, _ tool.RunOptions) (tool.Outcome, error) {
		if calls.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return tool.Outcome{}, ctx.Err()
			}
		}
		return tool.Outcome{}, nil
	})
	m, err := NewDurableManager(ctx, config, registry, s)
	if err != nil {
		t.Fatal(err)
	}
	first, err := m.SubmitIdempotent(ctx, Request{Tool: "test", Arguments: json.RawMessage(`{"b":2,"a":1}`)}, "key")
	if err != nil {
		t.Fatal(err)
	}
	<-started
	second, err := m.Submit(ctx, Request{Tool: "test"})
	if err != nil {
		t.Fatal(err)
	}
	row, err := s.GetExecution(ctx, second.ID)
	if err != nil || row.State != string(Queued) {
		t.Fatalf("accepted queued task was not persisted: state=%q err=%v", row.State, err)
	}
	replay, err := m.SubmitIdempotent(ctx, Request{Tool: "test", Arguments: json.RawMessage(`{ "a": 1, "b": 2 }`), QueueTimeout: config.DefaultQueueTimeout}, "key")
	if err != nil || replay.ID != first.ID {
		t.Fatalf("duplicate while queue full: %+v err=%v", replay, err)
	}
	if _, err := m.SubmitIdempotent(ctx, Request{Tool: "test", Arguments: json.RawMessage(`{"a":9}`)}, "key"); !errors.Is(err, sqlite.ErrIdempotencyConflict) {
		t.Fatalf("conflicting key: %v", err)
	}
	close(release)
	awaitState(t, m, first.ID, Succeeded)
	awaitState(t, m, second.ID, Succeeded)
	if calls.Load() != 2 {
		t.Fatalf("executor calls=%d, want 2", calls.Load())
	}
	if terminal, err := m.Cancel(first.ID); err != nil || terminal.State != Succeeded {
		t.Fatalf("cancel retained database result: %+v err=%v", terminal, err)
	}
	closeDurableManager(t, m)
}

func seedDurableExecution(t *testing.T, s *sqlite.Store, id string, createdAt time.Time, timeout time.Duration, running bool) {
	t.Helper()
	ctx := context.Background()
	r := &record{
		snapshot: Snapshot{
			ID: id, TenantID: "local", Tool: "test", State: Queued,
			CleanupStatus: "not_needed", CreatedAt: createdAt, ReadyAt: &createdAt,
			QueueTimeout: timeout, ExecutionTimeout: time.Second, OutputLimitBytes: 1024,
			ArgumentLimitBytes: 1024, ResultLimitBytes: 1024,
		},
		arguments: json.RawMessage(`{"x":1}`), deadline: createdAt.Add(timeout),
	}
	encoded, err := encodeStoredExecution(r, r.snapshot)
	if err != nil {
		t.Fatal(err)
	}
	event, err := stateEvent(r.snapshot, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateExecution(ctx, sqlite.Execution{ID: id, TenantID: "local", State: string(Queued), Snapshot: encoded, Request: []byte(`{"tool":"test"}`), CreatedAt: createdAt}, event); err != nil {
		t.Fatal(err)
	}
	if running {
		next := cloneSnapshot(r.snapshot)
		next.State = Running
		next.RunningAt = &createdAt
		encoded, err = encodeStoredExecution(r, next)
		if err != nil {
			t.Fatal(err)
		}
		event, err = stateEvent(next, createdAt)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Transition(ctx, id, 1, string(Running), encoded, event); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDurableRestartResumesQueuedAndInterruptsClaimed(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "state")
	s := openDurableStore(t, dir)
	seedDurableExecution(t, s, "queued", time.Now().UTC(), time.Minute, false)
	seedDurableExecution(t, s, "claimed", time.Now().UTC(), time.Minute, true)
	seedDurableExecution(t, s, "expired", time.Now().Add(-time.Minute).UTC(), time.Second, false)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openDurableStore(t, dir)
	defer s.Close()
	var calls atomic.Int32
	registry := durableRegistry(t, func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
		calls.Add(1)
		return tool.Outcome{}, nil
	})
	m, err := NewDurableManager(ctx, DefaultConfig(), registry, s)
	if err != nil {
		t.Fatal(err)
	}
	awaitState(t, m, "queued", Succeeded)
	claimed, err := m.Get("claimed")
	if err != nil || claimed.State != Interrupted || claimed.ErrorCode != "service_restarted" {
		t.Fatalf("claimed after restart: %+v err=%v", claimed, err)
	}
	expired, err := m.Get("expired")
	if err != nil || expired.State != TimedOut || expired.ErrorCode != "queue_timeout" {
		t.Fatalf("expired queue after restart: %+v err=%v", expired, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("executor calls=%d, want only queued task", calls.Load())
	}
	closeDurableManager(t, m)
}

func TestDurableRuntimeClaimPreventsSecondManager(t *testing.T) {
	ctx := context.Background()
	s := openDurableStore(t, filepath.Join(t.TempDir(), "state"))
	defer s.Close()
	seedDurableExecution(t, s, "queued", time.Now().UTC(), time.Minute, false)
	var calls atomic.Int32
	registry := durableRegistry(t, func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
		calls.Add(1)
		return tool.Outcome{}, nil
	})
	m, err := NewDurableManager(ctx, DefaultConfig(), registry, s)
	if err != nil {
		t.Fatalf("failed constructor retained runtime claim: %v", err)
	}
	if _, err := NewDurableManager(ctx, DefaultConfig(), registry, s); !errors.Is(err, sqlite.ErrRuntimeOwned) {
		t.Fatalf("second manager on one store: %v", err)
	}
	if err := s.Close(); !errors.Is(err, sqlite.ErrRuntimeOwned) {
		t.Fatalf("store closed while manager owns runtime: %v", err)
	}
	awaitState(t, m, "queued", Succeeded)
	if calls.Load() != 1 {
		t.Fatalf("executor calls=%d, want one", calls.Load())
	}
	closeDurableManager(t, m)
}

func TestDurableCancelAndTimeoutArePersisted(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "state")
	s := openDurableStore(t, dir)
	config := DefaultConfig()
	config.Workers = 1
	started := make(chan struct{})
	registry := durableRegistry(t, func(ctx context.Context, _ json.RawMessage, _ tool.RunOptions) (tool.Outcome, error) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-ctx.Done()
		return tool.Outcome{}, ctx.Err()
	})
	m, err := NewDurableManager(ctx, config, registry, s)
	if err != nil {
		t.Fatal(err)
	}
	running, err := m.Submit(ctx, Request{Tool: "test"})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	queued, err := m.Submit(ctx, Request{Tool: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := m.Cancel(queued.ID); err != nil || result.State != Cancelled {
		t.Fatalf("queued cancel: %+v err=%v", result, err)
	}
	if result, err := m.Cancel(running.ID); err != nil || result.State != Cancelling {
		t.Fatalf("running cancel: %+v err=%v", result, err)
	}
	awaitState(t, m, running.ID, Cancelled)
	timed, err := m.Submit(ctx, Request{Tool: "test", ExecutionTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	awaitState(t, m, timed.ID, TimedOut)
	closeDurableManager(t, m)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openDurableStore(t, dir)
	defer s.Close()
	for id, want := range map[string]State{queued.ID: Cancelled, running.ID: Cancelled, timed.ID: TimedOut} {
		row, err := s.GetExecution(ctx, id)
		if err != nil || State(row.State) != want {
			t.Fatalf("persisted %s: state=%q err=%v", id, row.State, err)
		}
	}
}

func TestDurableClaimFailureDoesNotRunTool(t *testing.T) {
	ctx := context.Background()
	s := openDurableStore(t, filepath.Join(t.TempDir(), "state"))
	defer s.Close()
	config := DefaultConfig()
	config.Workers = 1
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	registry := durableRegistry(t, func(ctx context.Context, _ json.RawMessage, _ tool.RunOptions) (tool.Outcome, error) {
		if calls.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return tool.Outcome{}, ctx.Err()
			}
		}
		return tool.Outcome{}, nil
	})
	m, err := NewDurableManager(ctx, config, registry, s)
	if err != nil {
		t.Fatal(err)
	}
	first, err := m.Submit(ctx, Request{Tool: "test"})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	second, err := m.Submit(ctx, Request{Tool: "test"})
	if err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.records[second.ID].version = 99 // Force the persisted claim comparison to fail.
	m.mu.Unlock()
	close(release)
	awaitState(t, m, first.ID, Succeeded)
	deadline := time.After(2 * time.Second)
	for !m.Stats().Halted {
		select {
		case <-deadline:
			t.Fatal("failed claim did not halt admission")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("executor called %d times after failed claim", calls.Load())
	}
	row, err := s.GetExecution(ctx, second.ID)
	if err != nil || row.State != string(Queued) {
		t.Fatalf("failed claim changed persisted state: %+v err=%v", row, err)
	}
	ctxClose, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := m.Close(ctxClose); err == nil {
		t.Fatal("close did not report storage failure")
	}
}

func TestDurableRejectsLegacyDockerResourceBeforeRecovery(t *testing.T) {
	ctx := context.Background()
	s := openDurableStore(t, filepath.Join(t.TempDir(), "state"))
	defer s.Close()
	seedDurableExecution(t, s, "claimed", time.Now().UTC(), time.Minute, true)
	row, err := s.GetExecution(ctx, "claimed")
	if err != nil {
		t.Fatal(err)
	}
	var stored storedRecord
	if err := json.Unmarshal(row.Snapshot, &stored); err != nil {
		t.Fatal(err)
	}
	stored.LegacyResource = json.RawMessage(`{"kind":"docker_container","id":"old-container","instance_id":"legacy"}`)
	encoded, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Transition(ctx, row.ID, row.Version, row.State, encoded,
		sqlite.Event{Kind: "progress", Payload: []byte(`{}`), OccurredAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	registry := durableRegistry(t, func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
		calls.Add(1)
		return tool.Outcome{}, nil
	})
	if _, err := NewDurableManager(ctx, DefaultConfig(), registry, s); err == nil || !strings.Contains(err.Error(), "legacy Docker resource") {
		t.Fatalf("legacy resource accepted: %v", err)
	}
	unchanged, err := s.GetExecution(ctx, "claimed")
	if err != nil || unchanged.State != string(Running) || calls.Load() != 0 {
		t.Fatalf("legacy recovery changed state or ran work: row=%+v calls=%d err=%v", unchanged, calls.Load(), err)
	}
}
