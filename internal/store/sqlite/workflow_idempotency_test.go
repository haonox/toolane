package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

const workflowEndpoint = "POST /api/v1/workflows"

func uniqueWorkflowFixture(n int) Workflow {
	record := workflowFixture()
	record.ID = fmt.Sprintf("workflow-%d", n)
	for i := range record.Nodes {
		record.Nodes[i].Execution.ID = fmt.Sprintf("workflow-%d-execution-%d", n, i)
	}
	return record
}

func TestConcurrentWorkflowIdempotencyAndScope(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const callers = 24
	start := make(chan struct{})
	var wg sync.WaitGroup
	ids := make([]string, callers)
	reused := make([]bool, callers)
	errorsByCaller := make([]error, callers)
	for i := range callers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ids[i], reused[i], errorsByCaller[i] = s.CreateIdempotentWorkflow(ctx, workflowEndpoint, "key", uniqueWorkflowFixture(i), []byte(`{"same":true}`))
		}(i)
	}
	close(start)
	wg.Wait()
	created := 0
	for i := range callers {
		if errorsByCaller[i] != nil || ids[i] != ids[0] {
			t.Fatalf("caller %d: id=%q reused=%t err=%v", i, ids[i], reused[i], errorsByCaller[i])
		}
		if !reused[i] {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("created %d workflows, want 1", created)
	}
	for table, want := range map[string]int{"workflows": 1, "workflow_nodes": 2, "executions": 2, "execution_events": 2, "workflow_idempotency": 1} {
		var count int
		if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != want {
			t.Fatalf("table %s count=%d want=%d err=%v", table, count, want, err)
		}
	}
	if id, found, err := s.ResolveWorkflowIdempotency(ctx, "tenant-a", workflowEndpoint, "key", []byte(`{"same":true}`)); err != nil || !found || id != ids[0] {
		t.Fatalf("resolve key: id=%q found=%t err=%v", id, found, err)
	}
	if _, _, err := s.CreateIdempotentWorkflow(ctx, workflowEndpoint, "key", uniqueWorkflowFixture(100), []byte(`{"same":false}`)); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting request: %v", err)
	}
	if _, _, err := s.ResolveWorkflowIdempotency(ctx, "tenant-a", workflowEndpoint, "key", []byte(`{"same":false}`)); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("resolve conflicting request: %v", err)
	}
	otherEndpoint := uniqueWorkflowFixture(101)
	if id, reused, err := s.CreateIdempotentWorkflow(ctx, "POST /api/v1/other-workflows", "key", otherEndpoint, []byte(`{"same":false}`)); err != nil || reused || id != otherEndpoint.ID {
		t.Fatalf("other endpoint: id=%q reused=%t err=%v", id, reused, err)
	}
	otherTenant := uniqueWorkflowFixture(102)
	otherTenant.TenantID = "tenant-b"
	for i := range otherTenant.Nodes {
		otherTenant.Nodes[i].Execution.TenantID = "tenant-b"
	}
	if id, reused, err := s.CreateIdempotentWorkflow(ctx, workflowEndpoint, "key", otherTenant, []byte(`{"same":false}`)); err != nil || reused || id != otherTenant.ID {
		t.Fatalf("other tenant: id=%q reused=%t err=%v", id, reused, err)
	}
}

func TestWorkflowIdempotencySurvivesRestartAndExpires(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "state")
	s, err := Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	first := uniqueWorkflowFixture(1)
	if _, _, err := s.CreateIdempotentWorkflow(ctx, workflowEndpoint, "key", first, []byte(`{"same":true}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, "DELETE FROM workflows WHERE id = ?", first.ID); err == nil {
		t.Fatal("live idempotency key did not protect workflow")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	second := uniqueWorkflowFixture(2)
	if id, reused, err := s.CreateIdempotentWorkflow(ctx, workflowEndpoint, "key", second, []byte(`{"same":true}`)); err != nil || !reused || id != first.ID {
		t.Fatalf("replay after restart: id=%q reused=%t err=%v", id, reused, err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE workflow_idempotency SET expires_at_ns = 0
		WHERE tenant_id = ? AND endpoint = ? AND key = ?`, first.TenantID, workflowEndpoint, "key"); err != nil {
		t.Fatal(err)
	}
	if id, reused, err := s.CreateIdempotentWorkflow(ctx, workflowEndpoint, "key", second, []byte(`{"same":true}`)); err != nil || reused || id != second.ID {
		t.Fatalf("create after expiry: id=%q reused=%t err=%v", id, reused, err)
	}
	if _, err := s.GetWorkflow(ctx, first.ID); err != nil {
		t.Fatalf("old workflow removed after expiry: %v", err)
	}
}

func TestWorkflowIdempotencyKeyFailureRollsBackBatch(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.db.ExecContext(ctx, `CREATE TRIGGER reject_workflow_key BEFORE INSERT ON workflow_idempotency
		BEGIN SELECT RAISE(ABORT, 'test key failure'); END`); err != nil {
		t.Fatal(err)
	}
	record := uniqueWorkflowFixture(1)
	if _, _, err := s.CreateIdempotentWorkflow(ctx, workflowEndpoint, "key", record, []byte(`{"same":true}`)); err == nil {
		t.Fatal("create succeeded despite key write failure")
	}
	if _, err := s.GetWorkflow(ctx, record.ID); !errors.Is(err, ErrWorkflowNotFound) {
		t.Fatalf("workflow survived rollback: %v", err)
	}
	for _, table := range []string{"workflows", "workflow_nodes", "executions", "execution_events", "workflow_idempotency"} {
		var count int
		if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("table %s count=%d err=%v", table, count, err)
		}
	}
}
