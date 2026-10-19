//go:build linux

package execution

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/tool"
)

func TestDurableOutputEventCapPreservesTerminal(t *testing.T) {
	ctx := context.Background()
	store := openDurableStore(t, filepath.Join(t.TempDir(), "state"))
	defer store.Close()
	output := strings.Repeat("x", outputChunkBytes*maxOutputEvents+1)
	registry := durableRegistry(t, func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
		return tool.Outcome{Stdout: output, Result: json.RawMessage(`{"ok":true}`)}, nil
	})
	config := DefaultConfig()
	config.MaxOutputBytes = len(output)
	m, err := NewDurableManager(ctx, config, registry, store, func(context.Context, []RecoveryResource) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer closeDurableManager(t, m)
	accepted, err := m.Submit(ctx, Request{Tool: "test", Arguments: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.After(5 * time.Second)
	var finished Snapshot
	for {
		finished, err = m.Get(accepted.ID)
		if err != nil {
			t.Fatal(err)
		}
		if finished.FinishedAt != nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("large output task did not finish")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if finished.State != Succeeded || finished.Stdout != output || finished.OutputTruncated {
		t.Fatalf("terminal result lost output: state=%s bytes=%d truncated=%v", finished.State, len(finished.Stdout), finished.OutputTruncated)
	}
	events, err := store.EventsAfter(ctx, accepted.ID, 0)
	if err != nil || len(events) != maxOutputEvents+3 {
		t.Fatalf("output event cap: events=%d err=%v", len(events), err)
	}
	if events[len(events)-1].Kind != "end" {
		t.Fatalf("terminal event dropped: %s", events[len(events)-1].Kind)
	}
	for i, event := range events {
		if event.Sequence != int64(i+1) {
			t.Fatalf("event sequence at %d: %d", i, event.Sequence)
		}
	}
}
