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

func TestDurableRetentionProtectsKeysAndRejectsAdmission(t *testing.T) {
	ctx := context.Background()
	s := openDurableStore(t, filepath.Join(t.TempDir(), "state"))
	defer s.Close()
	config := DefaultConfig()
	config.DurableRecordLimit = 1
	var calls atomic.Int32
	registry := durableRegistry(t, func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
		calls.Add(1)
		return tool.Outcome{}, nil
	})
	m, err := NewDurableManager(ctx, config, registry, s, func(context.Context, []RecoveryResource) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer closeDurableManager(t, m)
	first, err := m.SubmitIdempotent(ctx, Request{Tool: "test"}, "live-key")
	if err != nil {
		t.Fatal(err)
	}
	awaitState(t, m, first.ID, Succeeded)
	if _, err := m.Submit(ctx, Request{Tool: "test"}); !errors.Is(err, sqlite.ErrRetentionCapacity) {
		t.Fatalf("new single task at protected capacity: %v", err)
	}
	if _, err := m.SubmitWorkflow(ctx, WorkflowRequest{Nodes: []WorkflowNode{{ID: "node", Tool: "test"}}}); !errors.Is(err, sqlite.ErrRetentionCapacity) {
		t.Fatalf("new workflow at protected capacity: %v", err)
	}
	if _, err := m.SubmitWorkflow(ctx, WorkflowRequest{Nodes: []WorkflowNode{{ID: "a", Tool: "test"}, {ID: "b", Tool: "test"}}}); !errors.Is(err, sqlite.ErrRetentionCapacity) || m.Stats().Halted {
		t.Fatalf("oversized workflow should not halt manager: %v", err)
	}
	replay, err := m.SubmitIdempotent(ctx, Request{Tool: "test"}, "live-key")
	if err != nil || replay.ID != first.ID || calls.Load() != 1 {
		t.Fatalf("protected replay: %+v calls=%d err=%v", replay, calls.Load(), err)
	}
}

func TestDurableRetentionRemovesCachedTerminalOnAdmission(t *testing.T) {
	ctx := context.Background()
	s := openDurableStore(t, filepath.Join(t.TempDir(), "state"))
	defer s.Close()
	config := DefaultConfig()
	config.DurableRecordLimit = 1
	config.DurableRetention = 50 * time.Millisecond
	config.RetainedResults = 10
	registry := durableRegistry(t, func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
		return tool.Outcome{}, nil
	})
	m, err := NewDurableManager(ctx, config, registry, s, func(context.Context, []RecoveryResource) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer closeDurableManager(t, m)
	first, err := m.Submit(ctx, Request{Tool: "test"})
	if err != nil {
		t.Fatal(err)
	}
	awaitState(t, m, first.ID, Succeeded)
	time.Sleep(75 * time.Millisecond)
	second, err := m.Submit(ctx, Request{Tool: "test"})
	if err != nil {
		t.Fatal(err)
	}
	awaitState(t, m, second.ID, Succeeded)
	if _, err := m.Get(first.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cached expired execution remains: %v", err)
	}
	if _, err := s.GetExecution(ctx, first.ID); !errors.Is(err, sqlite.ErrNotFound) {
		t.Fatalf("database expired execution remains: %v", err)
	}
	if stats := m.Stats(); stats.Stored != 1 {
		t.Fatalf("cached record count after prune: %+v", stats)
	}
}

func TestDurableRetentionRestartsWithProtectedOverCapacity(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "state")
	s := openDurableStore(t, dir)
	registry := durableRegistry(t, func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
		return tool.Outcome{}, nil
	})
	config := DefaultConfig()
	config.DurableRecordLimit = 2
	m, err := NewDurableManager(ctx, config, registry, s, func(context.Context, []RecoveryResource) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "b"} {
		snapshot, err := m.SubmitIdempotent(ctx, Request{Tool: "test"}, key)
		if err != nil {
			t.Fatal(err)
		}
		awaitState(t, m, snapshot.ID, Succeeded)
	}
	closeDurableManager(t, m)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openDurableStore(t, dir)
	defer s.Close()
	config.DurableRecordLimit = 1
	m, err = NewDurableManager(ctx, config, registry, s, func(context.Context, []RecoveryResource) error { return nil })
	if err != nil {
		t.Fatalf("startup rejected protected history: %v", err)
	}
	defer closeDurableManager(t, m)
	if _, err := m.Submit(ctx, Request{Tool: "test"}); !errors.Is(err, sqlite.ErrRetentionCapacity) || m.Stats().Halted {
		t.Fatalf("admission after protected restart: %v", err)
	}
	if _, err := m.SubmitIdempotent(ctx, Request{Tool: "test"}, "a"); err != nil {
		t.Fatalf("protected key replay after restart: %v", err)
	}
}
