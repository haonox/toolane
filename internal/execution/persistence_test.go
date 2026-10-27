package execution

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/storage/sqlite"
	"github.com/haonox/toolane/internal/tool"
)

func persistentPath(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "executions.sqlite")
}

func persistentRegistry(t *testing.T, execute func(context.Context, json.RawMessage) (tool.Outcome, error)) *tool.Registry {
	t.Helper()
	registry := tool.NewRegistry()
	if err := registry.Register(tool.Definition{Name: "test", Validate: func(json.RawMessage) error { return nil },
		Execute: func(ctx context.Context, args json.RawMessage, _ tool.RunOptions) (tool.Outcome, error) {
			return execute(ctx, args)
		}}); err != nil {
		t.Fatal(err)
	}
	return registry
}

func closePersistent(t *testing.T, m *Manager) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPersistentResultsSurviveRestartAndMemoryEviction(t *testing.T) {
	path := persistentPath(t)
	config := DefaultConfig()
	config.RetainedResults = 1
	registry := persistentRegistry(t, func(context.Context, json.RawMessage) (tool.Outcome, error) {
		return tool.Outcome{Result: json.RawMessage(`{"ok":true}`)}, nil
	})
	m, err := NewPersistentManager(config, registry, path)
	if err != nil {
		t.Fatal(err)
	}
	first := submit(t, m, Request{Tool: "test"})
	awaitState(t, m, first.ID, Succeeded)
	second := submit(t, m, Request{Tool: "test"})
	awaitState(t, m, second.ID, Succeeded)
	if got, err := m.Get(first.ID); err != nil || got.State != Succeeded || string(got.Result) != `{"ok":true}` {
		t.Fatalf("evicted result = %+v, %v", got, err)
	}
	closePersistent(t, m)
	reopened, err := NewPersistentManager(config, registry, path)
	if err != nil {
		t.Fatal(err)
	}
	defer closePersistent(t, reopened)
	for _, id := range []string{first.ID, second.ID} {
		got, err := reopened.Get(id)
		if err != nil || got.State != Succeeded || string(got.Result) != `{"ok":true}` || got.FinishedAt == nil {
			t.Fatalf("reopened result %s = %+v, %v", id, got, err)
		}
		if cancelled, err := reopened.Cancel(id); err != nil || cancelled.State != Succeeded {
			t.Fatalf("terminal cancel %s = %+v, %v", id, cancelled, err)
		}
	}
}

func TestPersistentQueuedWorkResumesAfterGracefulShutdown(t *testing.T) {
	path := persistentPath(t)
	config := DefaultConfig()
	config.Workers = 1
	started := make(chan struct{})
	var calls atomic.Int32
	registry := persistentRegistry(t, func(ctx context.Context, args json.RawMessage) (tool.Outcome, error) {
		if string(args) == `"anchor"` {
			close(started)
			<-ctx.Done()
			return tool.Outcome{}, ctx.Err()
		}
		calls.Add(1)
		return tool.Outcome{Result: json.RawMessage(`{"resumed":true}`)}, nil
	})
	m, err := NewPersistentManager(config, registry, path)
	if err != nil {
		t.Fatal(err)
	}
	anchor := submit(t, m, Request{Tool: "test", Arguments: json.RawMessage(`"anchor"`)})
	<-started
	queued := submit(t, m, Request{Tool: "test", Arguments: json.RawMessage(`"queued"`)})
	closePersistent(t, m)
	reopened, err := NewPersistentManager(config, registry, path)
	if err != nil {
		t.Fatal(err)
	}
	defer closePersistent(t, reopened)
	if got := awaitState(t, reopened, queued.ID, Succeeded); string(got.Result) != `{"resumed":true}` || calls.Load() != 1 {
		t.Fatalf("resumed execution = %+v, calls=%d", got, calls.Load())
	}
	if got, err := reopened.Get(anchor.ID); err != nil || got.State != Cancelled {
		t.Fatalf("stopped running execution = %+v, %v", got, err)
	}
}

func TestPersistentRecoveryMarksRunningOutcomeUnknown(t *testing.T) {
	path := persistentPath(t)
	store, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	snapshot := Snapshot{ID: "was-running", TenantID: "tenant-a", Tool: "test", State: Running,
		CleanupStatus: "not_needed", CreatedAt: now, RunningAt: &now,
		QueueTimeout: time.Minute, ExecutionTimeout: time.Minute,
		OutputLimitBytes: 1024, ArgumentLimitBytes: 1024, ResultLimitBytes: 1024}
	image := durableImage{Snapshot: snapshot, Arguments: json.RawMessage(`"input"`),
		QueueTimeoutNS: int64(time.Minute), ExecutionTimeoutNS: int64(time.Minute),
		OutputLimitBytes: 1024, ArgumentLimitBytes: 1024, ResultLimitBytes: 1024}
	payload, err := json.Marshal(image)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Insert(context.Background(), sqlite.Record{ID: snapshot.ID, TenantID: snapshot.TenantID,
		State: string(Running), QueueDeadline: now.Add(time.Minute), Payload: payload}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	registry := persistentRegistry(t, func(context.Context, json.RawMessage) (tool.Outcome, error) {
		calls.Add(1)
		return tool.Outcome{}, nil
	})
	m, err := NewPersistentManager(DefaultConfig(), registry, path)
	if err != nil {
		t.Fatal(err)
	}
	defer closePersistent(t, m)
	got, err := m.Get(snapshot.ID)
	if err != nil || got.State != Interrupted || got.ErrorCode != "outcome_unknown" || got.FinishedAt == nil || calls.Load() != 0 {
		t.Fatalf("recovered running execution = %+v, %v, calls=%d", got, err, calls.Load())
	}
}

func TestPersistentRecoveryExpiresQueuedWorkBeforeStartingWorkers(t *testing.T) {
	path := persistentPath(t)
	store, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	created := time.Now().UTC().Add(-2 * time.Minute)
	snapshot := Snapshot{ID: "expired-queued", TenantID: "tenant-a", Tool: "test", State: Queued,
		CleanupStatus: "not_needed", CreatedAt: created, ReadyAt: &created,
		QueueTimeout: time.Minute, ExecutionTimeout: time.Minute,
		OutputLimitBytes: 1024, ArgumentLimitBytes: 1024, ResultLimitBytes: 1024}
	image := durableImage{Snapshot: snapshot, Arguments: json.RawMessage(`"input"`),
		QueueTimeoutNS: int64(time.Minute), ExecutionTimeoutNS: int64(time.Minute),
		OutputLimitBytes: 1024, ArgumentLimitBytes: 1024, ResultLimitBytes: 1024}
	payload, err := json.Marshal(image)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Insert(context.Background(), sqlite.Record{ID: snapshot.ID, TenantID: snapshot.TenantID,
		State: string(Queued), QueueDeadline: created.Add(time.Minute), Payload: payload}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	registry := persistentRegistry(t, func(context.Context, json.RawMessage) (tool.Outcome, error) {
		calls.Add(1)
		return tool.Outcome{}, nil
	})
	m, err := NewPersistentManager(DefaultConfig(), registry, path)
	if err != nil {
		t.Fatal(err)
	}
	defer closePersistent(t, m)
	got, err := m.Get(snapshot.ID)
	if err != nil || got.State != TimedOut || got.ErrorCode != "queue_timeout" || calls.Load() != 0 {
		t.Fatalf("expired queued execution = %+v, %v, calls=%d", got, err, calls.Load())
	}
}

func TestPersistentManagerStopsAdmissionAfterStorageFailure(t *testing.T) {
	path := persistentPath(t)
	registry := persistentRegistry(t, func(context.Context, json.RawMessage) (tool.Outcome, error) {
		return tool.Outcome{}, nil
	})
	m, err := NewPersistentManager(DefaultConfig(), registry, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Submit(context.Background(), Request{Tool: "test"}); err != ErrBackendUnavailable || !m.Stats().Halted {
		t.Fatalf("submission after storage failure = %v, stats=%+v", err, m.Stats())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := m.Close(ctx); err == nil {
		t.Fatal("storage failure was not reported on close")
	}
}
