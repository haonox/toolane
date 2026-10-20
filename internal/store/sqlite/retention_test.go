package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func retentionExecution(id, state, cleanup string, finished *time.Time) (Execution, Event) {
	created := time.Now().Add(-9 * 24 * time.Hour).UTC()
	var snapshot struct {
		Snapshot struct {
			FinishedAt    *time.Time `json:"finished_at"`
			CleanupStatus string     `json:"cleanup_status"`
		} `json:"snapshot"`
	}
	snapshot.Snapshot.FinishedAt = finished
	snapshot.Snapshot.CleanupStatus = cleanup
	raw, _ := json.Marshal(snapshot)
	return Execution{ID: id, TenantID: "local", State: state, Snapshot: raw, Request: []byte(`{"tool":"test"}`), CreatedAt: created},
		Event{Kind: "state", Payload: raw, OccurredAt: created}
}

func retentionWorkflow(t *testing.T, s *Store, id string, finished *time.Time, protected bool) []string {
	t.Helper()
	ctx := context.Background()
	firstID, secondID := id+"-a", id+"-b"
	first, firstEvent := retentionExecution(firstID, "succeeded", "not_needed", finished)
	secondState := "queued"
	secondFinished := (*time.Time)(nil)
	if finished != nil {
		secondState, secondFinished = "succeeded", finished
	}
	second, secondEvent := retentionExecution(secondID, secondState, "not_needed", secondFinished)
	batch := Workflow{ID: id, TenantID: "local", State: "running", Request: []byte(`{"nodes":2}`), CreatedAt: first.CreatedAt,
		Nodes: []WorkflowNode{{NodeID: "a", Execution: first, Event: firstEvent}, {NodeID: "b", Execution: second, Event: secondEvent}}}
	var err error
	if protected {
		_, _, err = s.CreateIdempotentWorkflow(ctx, "POST /api/v1/workflows", id, batch, []byte(`{"nodes":2}`))
	} else {
		err = s.CreateWorkflow(ctx, batch)
	}
	if err != nil {
		t.Fatal(err)
	}
	if finished != nil {
		if _, err := s.TransitionWorkflow(ctx, id, 1, "succeeded", finished); err != nil {
			t.Fatal(err)
		}
	}
	return []string{firstID, secondID}
}

func TestRetentionPrunesAgeAndWholeWorkflow(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Now().UTC()
	old, recent := now.Add(-8*24*time.Hour), now.Add(-time.Hour)
	for _, item := range []struct {
		id       string
		finished time.Time
	}{{"old", old}, {"recent", recent}} {
		row, event := retentionExecution(item.id, "succeeded", "not_needed", &item.finished)
		if err := s.CreateExecution(ctx, row, event); err != nil {
			t.Fatal(err)
		}
	}
	removedNodes := retentionWorkflow(t, s, "old-workflow", &old, false)
	activeNodes := retentionWorkflow(t, s, "active-workflow", nil, false)
	protectedNodes := retentionWorkflow(t, s, "keyed-workflow", &old, true)
	result, err := s.Prune(ctx, RetentionPolicy{MaxRecords: 10, MaxAge: 7 * 24 * time.Hour, Now: now})
	if err != nil || result.Remaining != 5 || len(result.WorkflowIDs) != 1 || result.WorkflowIDs[0] != "old-workflow" {
		t.Fatalf("prune: %+v err=%v", result, err)
	}
	for _, id := range append([]string{"old"}, removedNodes...) {
		if _, err := s.GetExecution(ctx, id); !errors.Is(err, ErrNotFound) {
			t.Fatalf("execution %s retained: %v", id, err)
		}
		if events, err := s.EventsAfter(ctx, id, 0); err != nil || len(events) != 0 {
			t.Fatalf("events for %s retained: %+v err=%v", id, events, err)
		}
	}
	if _, err := s.GetWorkflow(ctx, "old-workflow"); !errors.Is(err, ErrWorkflowNotFound) {
		t.Fatalf("workflow retained: %v", err)
	}
	for _, id := range append(append([]string{"recent"}, activeNodes...), protectedNodes...) {
		if _, err := s.GetExecution(ctx, id); err != nil {
			t.Fatalf("protected execution %s lost: %v", id, err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE workflow_idempotency SET expires_at_ns = 0 WHERE workflow_id = 'keyed-workflow'`); err != nil {
		t.Fatal(err)
	}
	result, err = s.Prune(ctx, RetentionPolicy{MaxRecords: 10, MaxAge: 7 * 24 * time.Hour, Now: now})
	if err != nil || result.Remaining != 3 || len(result.WorkflowIDs) != 1 || result.WorkflowIDs[0] != "keyed-workflow" {
		t.Fatalf("expired workflow key: %+v err=%v", result, err)
	}
	for _, id := range protectedNodes {
		if _, err := s.GetExecution(ctx, id); !errors.Is(err, ErrNotFound) {
			t.Fatalf("expired workflow node %s retained: %v", id, err)
		}
	}
}

func TestRetentionCapacityKeepsProtectedRowsAndExpiresKeys(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Now().UTC()
	old, recent := now.Add(-8*24*time.Hour), now.Add(-time.Hour)
	for _, item := range []struct {
		id, state, cleanup string
		finished           *time.Time
	}{
		{"active", "running", "not_needed", nil},
		{"cleanup", "failed", "pending", &old},
		{"eligible", "succeeded", "not_needed", &recent},
	} {
		row, event := retentionExecution(item.id, item.state, item.cleanup, item.finished)
		if err := s.CreateExecution(ctx, row, event); err != nil {
			t.Fatal(err)
		}
	}
	keyed, keyedEvent := retentionExecution("keyed", "succeeded", "not_needed", &old)
	if _, _, err := s.CreateIdempotentExecution(ctx, "POST /api/v1/executions", "key", keyed, keyedEvent); err != nil {
		t.Fatal(err)
	}
	result, err := s.Prune(ctx, RetentionPolicy{MaxRecords: 3, MaxAge: 7 * 24 * time.Hour, Reserve: 1, Now: now})
	if !errors.Is(err, ErrRetentionCapacity) || result.Remaining != 3 || len(result.ExecutionIDs) != 1 || result.ExecutionIDs[0] != "eligible" {
		t.Fatalf("protected capacity: %+v err=%v", result, err)
	}
	for _, id := range []string{"active", "cleanup", "keyed"} {
		if _, err := s.GetExecution(ctx, id); err != nil {
			t.Fatalf("protected %s lost: %v", id, err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE idempotency SET expires_at_ns = 0 WHERE execution_id = 'keyed'`); err != nil {
		t.Fatal(err)
	}
	result, err = s.Prune(ctx, RetentionPolicy{MaxRecords: 3, MaxAge: 7 * 24 * time.Hour, Reserve: 1, Now: now})
	if err != nil || result.Remaining != 2 || len(result.ExecutionIDs) != 1 || result.ExecutionIDs[0] != "keyed" {
		t.Fatalf("expired key: %+v err=%v", result, err)
	}
}

func TestRetentionDeleteFailureRollsBack(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	old := time.Now().Add(-8 * 24 * time.Hour).UTC()
	row, event := retentionExecution("old", "succeeded", "not_needed", &old)
	if _, _, err := s.CreateIdempotentExecution(ctx, "POST /api/v1/executions", "expired-key", row, event); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE idempotency SET expires_at_ns = 0 WHERE execution_id = 'old'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE TRIGGER reject_retention BEFORE DELETE ON executions BEGIN SELECT RAISE(ABORT, 'retention failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Prune(ctx, RetentionPolicy{MaxRecords: 1, MaxAge: 7 * 24 * time.Hour, Now: time.Now().UTC()}); err == nil {
		t.Fatal("prune succeeded despite delete failure")
	}
	if _, err := s.GetExecution(ctx, "old"); err != nil {
		t.Fatalf("execution lost after rollback: %v", err)
	}
	var keys int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM idempotency WHERE execution_id = 'old'`).Scan(&keys); err != nil || keys != 1 {
		t.Fatalf("expired key deletion was not rolled back: count=%d err=%v", keys, err)
	}
}
