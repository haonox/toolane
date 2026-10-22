package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func workflowFixture() Workflow {
	first, firstEvent := fixture()
	second, secondEvent := fixture()
	first.ID = "node-execution-a"
	second.ID = "node-execution-b"
	second.State = "blocked"
	second.Snapshot = []byte(`{"status":"blocked"}`)
	secondEvent.Payload = second.Snapshot
	return Workflow{
		ID: "workflow-1", TenantID: first.TenantID, State: "running",
		Request: []byte(`{"nodes":["a","b"]}`), CreatedAt: first.CreatedAt,
		Nodes: []WorkflowNode{
			{NodeID: "a", Execution: first, Event: firstEvent},
			{NodeID: "b", DependsOn: []string{"a"}, Execution: second, Event: secondEvent},
		},
	}
}

func TestWorkflowAdmissionPersistsBatchAndOrder(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "state")
	s, err := Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	want := workflowFixture()
	if err := s.CreateWorkflow(ctx, want); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.GetWorkflow(ctx, want.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != want.ID || got.TenantID != want.TenantID || got.State != want.State || string(got.Request) != string(want.Request) || !got.CreatedAt.Equal(want.CreatedAt) || got.FinishedAt != nil || got.Version != 1 || len(got.Nodes) != 2 {
		t.Fatalf("reopened workflow: %+v", got)
	}
	for i, node := range got.Nodes {
		original := want.Nodes[i]
		if node.NodeID != original.NodeID || !reflect.DeepEqual(node.DependsOn, original.DependsOn) || node.Execution.ID != original.Execution.ID || node.Execution.State != original.Execution.State || node.Execution.Version != 1 || string(node.Execution.Snapshot) != string(original.Execution.Snapshot) || string(node.Execution.Request) != string(original.Execution.Request) || !node.Execution.CreatedAt.Equal(original.Execution.CreatedAt) || node.Event.ExecutionID != original.Execution.ID || node.Event.Sequence != 1 || node.Event.Kind != original.Event.Kind || string(node.Event.Payload) != string(original.Event.Payload) || !node.Event.OccurredAt.Equal(original.Event.OccurredAt) {
			t.Fatalf("reopened node %d: %+v", i, node)
		}
		events, err := s.EventsAfter(ctx, node.Execution.ID, 0)
		if err != nil || len(events) != 1 || events[0].Sequence != 1 {
			t.Fatalf("node %d events: %+v err=%v", i, events, err)
		}
	}
	if _, err := s.GetWorkflow(ctx, "absent"); !errors.Is(err, ErrWorkflowNotFound) {
		t.Fatalf("missing workflow: %v", err)
	}
}

func TestWorkflowAdmissionRollsBackWholeBatchOnLaterEventFailure(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.db.ExecContext(ctx, `CREATE TRIGGER reject_second_event BEFORE INSERT ON execution_events
		WHEN NEW.execution_id = 'node-execution-b'
		BEGIN SELECT RAISE(ABORT, 'test second event failure'); END`); err != nil {
		t.Fatal(err)
	}
	want := workflowFixture()
	if err := s.CreateWorkflow(ctx, want); err == nil {
		t.Fatal("batch succeeded despite second event failure")
	}
	if _, err := s.GetWorkflow(ctx, want.ID); !errors.Is(err, ErrWorkflowNotFound) {
		t.Fatalf("workflow survived rollback: %v", err)
	}
	for _, node := range want.Nodes {
		if _, err := s.GetExecution(ctx, node.Execution.ID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("execution %q survived rollback: %v", node.Execution.ID, err)
		}
		if events, err := s.EventsAfter(ctx, node.Execution.ID, 0); err != nil || len(events) != 0 {
			t.Fatalf("events for %q survived rollback: %+v err=%v", node.Execution.ID, events, err)
		}
	}
	for _, table := range []string{"workflows", "workflow_nodes", "executions", "execution_events"} {
		var count int
		if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("table %s count=%d err=%v", table, count, err)
		}
	}
}

func TestGetWorkflowRejectsIncompleteNodeData(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	record := workflowFixture()
	if err := s.CreateWorkflow(ctx, record); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM execution_events
		WHERE execution_id = ? AND sequence = 1`, record.Nodes[1].Execution.ID); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetWorkflow(ctx, record.ID); err == nil {
		t.Fatalf("incomplete batch read without error: %+v", got)
	}
}

func TestWorkflowSummaryTransitionAndList(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	record := workflowFixture()
	if err := s.CreateWorkflow(ctx, record); err != nil {
		t.Fatal(err)
	}
	version, err := s.TransitionWorkflow(ctx, record.ID, 1, "cancelling", nil)
	if err != nil || version != 2 {
		t.Fatalf("cancelling transition: version=%d err=%v", version, err)
	}
	if _, err := s.TransitionWorkflow(ctx, record.ID, 1, "failed", nil); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale workflow transition: %v", err)
	}
	finished := record.CreatedAt.Add(1)
	version, err = s.TransitionWorkflow(ctx, record.ID, 2, "cancelled", &finished)
	if err != nil || version != 3 {
		t.Fatalf("final transition: version=%d err=%v", version, err)
	}
	rows, err := s.ListWorkflows(ctx)
	if err != nil || len(rows) != 1 || rows[0].State != "cancelled" || rows[0].Version != 3 || rows[0].FinishedAt == nil || !rows[0].FinishedAt.Equal(finished) || len(rows[0].Nodes) != 2 {
		t.Fatalf("listed workflow: %+v err=%v", rows, err)
	}
}
