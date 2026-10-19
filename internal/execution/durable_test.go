//go:build linux

package execution

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
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
	m, err := NewDurableManager(ctx, DefaultConfig(), registry, s, func(context.Context, []RecoveryResource) error { return nil })
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
	m, err := NewDurableManager(ctx, config, registry, s, func(context.Context, []RecoveryResource) error { return nil })
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
	var recovered atomic.Bool
	registry := durableRegistry(t, func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
		if !recovered.Load() {
			return tool.Outcome{}, errors.New("worker started before recovery")
		}
		calls.Add(1)
		return tool.Outcome{}, nil
	})
	m, err := NewDurableManager(ctx, DefaultConfig(), registry, s, func(context.Context, []RecoveryResource) error {
		recovered.Store(true)
		return nil
	})
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

func TestDurableRecoveryFailureKeepsWorkersStopped(t *testing.T) {
	ctx := context.Background()
	s := openDurableStore(t, filepath.Join(t.TempDir(), "state"))
	defer s.Close()
	seedDurableExecution(t, s, "queued", time.Now().UTC(), time.Minute, false)
	var calls atomic.Int32
	registry := durableRegistry(t, func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
		calls.Add(1)
		return tool.Outcome{}, nil
	})
	if _, err := NewDurableManager(ctx, DefaultConfig(), registry, s, func(context.Context, []RecoveryResource) error {
		return errors.New("resource cleanup uncertain")
	}); err == nil {
		t.Fatal("recovery failure allowed manager startup")
	}
	if calls.Load() != 0 {
		t.Fatal("worker ran despite failed recovery")
	}
	m, err := NewDurableManager(ctx, DefaultConfig(), registry, s, func(context.Context, []RecoveryResource) error { return nil })
	if err != nil {
		t.Fatalf("failed constructor retained runtime claim: %v", err)
	}
	if _, err := NewDurableManager(ctx, DefaultConfig(), registry, s, func(context.Context, []RecoveryResource) error { return nil }); !errors.Is(err, sqlite.ErrRuntimeOwned) {
		t.Fatalf("second manager on one store: %v", err)
	}
	if err := s.Close(); !errors.Is(err, sqlite.ErrRuntimeOwned) {
		t.Fatalf("store closed while manager owns runtime: %v", err)
	}
	awaitState(t, m, "queued", Succeeded)
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
	m, err := NewDurableManager(ctx, config, registry, s, func(context.Context, []RecoveryResource) error { return nil })
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
	m, err := NewDurableManager(ctx, config, registry, s, func(context.Context, []RecoveryResource) error { return nil })
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

func TestDurableResourceOwnershipPersistsAndReachesRecovery(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "state")
	s := openDurableStore(t, dir)
	ref := tool.ResourceRef{Kind: tool.ResourceDockerContainer, ID: "container-123", InstanceID: "deployment"}
	var recorded atomic.Bool
	registry := durableRegistry(t, func(_ context.Context, _ json.RawMessage, options tool.RunOptions) (tool.Outcome, error) {
		if err := options.OnResourceCreated(ref); err != nil {
			return tool.Outcome{}, err
		}
		row, err := s.GetExecution(ctx, options.ExecutionID)
		if err != nil {
			return tool.Outcome{}, err
		}
		var stored storedRecord
		if err := json.Unmarshal(row.Snapshot, &stored); err != nil {
			return tool.Outcome{}, err
		}
		if stored.Resource == nil || *stored.Resource != ref || stored.Snapshot.CleanupStatus != "pending" || row.State != string(Running) {
			return tool.Outcome{}, errors.New("resource was not committed before tool continued")
		}
		recorded.Store(true)
		return tool.Outcome{CleanupStatus: "pending"}, nil
	})
	m, err := NewDurableManager(ctx, DefaultConfig(), registry, s, func(context.Context, []RecoveryResource) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	initial, err := m.Submit(ctx, Request{Tool: "test"})
	if err != nil {
		t.Fatal(err)
	}
	finished := awaitState(t, m, initial.ID, Succeeded)
	if !recorded.Load() || finished.CleanupStatus != "pending" {
		t.Fatalf("resource callback did not persist ownership: %+v", finished)
	}
	events, err := s.EventsAfter(ctx, initial.ID, 0)
	if err != nil || len(events) != 4 || events[2].Kind != "resource_created" {
		t.Fatalf("missing resource event: %+v err=%v", events, err)
	}
	closeDurableManager(t, m)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openDurableStore(t, dir)
	defer s.Close()
	recovered := false
	m, err = NewDurableManager(ctx, DefaultConfig(), registry, s, func(_ context.Context, resources []RecoveryResource) error {
		if len(resources) != 1 || resources[0].ExecutionID != initial.ID || resources[0].Ref != ref {
			return errors.New("recovery did not receive the persisted resource")
		}
		recovered = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !recovered {
		t.Fatal("resource recovery hook was not called")
	}
	closeDurableManager(t, m)
}

func TestDurableResourceWriteFailureDoesNotCommitOwnership(t *testing.T) {
	ctx := context.Background()
	s := openDurableStore(t, filepath.Join(t.TempDir(), "state"))
	defer s.Close()
	entered := make(chan struct{})
	proceed := make(chan struct{})
	callbackResult := make(chan error, 1)
	registry := durableRegistry(t, func(_ context.Context, _ json.RawMessage, options tool.RunOptions) (tool.Outcome, error) {
		close(entered)
		<-proceed
		err := options.OnResourceCreated(tool.ResourceRef{Kind: tool.ResourceDockerContainer, ID: "container-123", InstanceID: "deployment"})
		callbackResult <- err
		return tool.Outcome{CleanupStatus: "succeeded"}, err
	})
	config := DefaultConfig()
	config.Workers = 1
	m, err := NewDurableManager(ctx, config, registry, s, func(context.Context, []RecoveryResource) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	initial, err := m.Submit(ctx, Request{Tool: "test"})
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	m.mu.Lock()
	m.records[initial.ID].version = 99 // Inject a failed compare-and-swap on the resource write.
	m.mu.Unlock()
	close(proceed)
	if err := <-callbackResult; !errors.Is(err, sqlite.ErrVersionConflict) {
		t.Fatalf("resource callback accepted failed write: %v", err)
	}
	if !m.Stats().Halted {
		t.Fatal("resource write failure did not halt admission")
	}
	row, err := s.GetExecution(ctx, initial.ID)
	if err != nil {
		t.Fatal(err)
	}
	var stored storedRecord
	if err := json.Unmarshal(row.Snapshot, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Resource != nil || row.State != string(Running) {
		t.Fatalf("failed resource write changed persisted ownership: %+v", stored)
	}
	closeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := m.Close(closeCtx); err == nil {
		t.Fatal("close hid the storage failure")
	}
}

func TestDurableRecoveryFailureDoesNotInterruptOwnedResource(t *testing.T) {
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
	ref := tool.ResourceRef{Kind: tool.ResourceDockerContainer, ID: "container-123", InstanceID: "deployment"}
	stored.Resource = &ref
	stored.Snapshot.CleanupStatus = "pending"
	encoded, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Transition(ctx, row.ID, row.Version, string(Running), encoded,
		sqlite.Event{Kind: "resource_created", Payload: []byte(`{"resource_kind":"docker_container"}`), OccurredAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	registry := durableRegistry(t, func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
		calls.Add(1)
		return tool.Outcome{}, nil
	})
	if _, err := NewDurableManager(ctx, DefaultConfig(), registry, s, func(_ context.Context, resources []RecoveryResource) error {
		if len(resources) != 1 || resources[0].Ref != ref {
			return errors.New("lost persisted resource during recovery")
		}
		return errors.New("Docker stop not confirmed")
	}); err == nil {
		t.Fatal("manager started despite failed resource recovery")
	}
	row, err = s.GetExecution(ctx, "claimed")
	if err != nil || row.State != string(Running) || calls.Load() != 0 {
		t.Fatalf("failed recovery changed claimed task: row=%+v calls=%d err=%v", row, calls.Load(), err)
	}
	m, err := NewDurableManager(ctx, DefaultConfig(), registry, s, func(context.Context, []RecoveryResource) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := m.Get("claimed")
	if err != nil || snapshot.State != Interrupted || calls.Load() != 0 {
		t.Fatalf("confirmed recovery did not interrupt without rerun: %+v calls=%d err=%v", snapshot, calls.Load(), err)
	}
	closeDurableManager(t, m)
}
