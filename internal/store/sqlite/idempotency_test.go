//go:build linux

package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

const executionEndpoint = "POST /api/v1/executions"

func TestConcurrentIdempotentAdmissionAndScope(t *testing.T) {
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
			record, event := fixture()
			record.ID = fmt.Sprintf("e-%d", i)
			ids[i], reused[i], errorsByCaller[i] = s.CreateIdempotentExecution(ctx, executionEndpoint, "same-key", record, event)
		}(i)
	}
	close(start)
	wg.Wait()
	created := 0
	for i := range callers {
		if errorsByCaller[i] != nil || ids[i] == "" {
			t.Fatalf("caller %d: id=%q err=%v", i, ids[i], errorsByCaller[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("caller %d got %q, want %q", i, ids[i], ids[0])
		}
		if !reused[i] {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("created %d executions, want 1", created)
	}
	var count int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM executions").Scan(&count); err != nil || count != 1 {
		t.Fatalf("execution count=%d err=%v", count, err)
	}
	record, event := fixture()
	record.ID = "different-request"
	record.Request = []byte(`{"tool":"other"}`)
	if _, _, err := s.CreateIdempotentExecution(ctx, executionEndpoint, "same-key", record, event); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("different request with same key: %v", err)
	}
	record.ID = "different-endpoint"
	if id, reused, err := s.CreateIdempotentExecution(ctx, "POST /api/v1/alternative", "same-key", record, event); err != nil || reused || id != record.ID {
		t.Fatalf("different endpoint: id=%q reused=%t err=%v", id, reused, err)
	}
	record.ID = "different-tenant"
	record.TenantID = "tenant-b"
	if id, reused, err := s.CreateIdempotentExecution(ctx, executionEndpoint, "same-key", record, event); err != nil || reused || id != record.ID {
		t.Fatalf("different tenant: id=%q reused=%t err=%v", id, reused, err)
	}
}

func TestIdempotencySurvivesRestartAndExpires(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "state")
	s, err := Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	record, event := fixture()
	id, reused, err := s.CreateIdempotentExecution(ctx, executionEndpoint, "key", record, event)
	if err != nil || reused || id != record.ID {
		t.Fatalf("first create: id=%q reused=%t err=%v", id, reused, err)
	}
	if _, err := s.db.ExecContext(ctx, "DELETE FROM executions WHERE id = ?", record.ID); err == nil {
		t.Fatal("active idempotency key did not protect its execution")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	record.ID = "second"
	id, reused, err = s.CreateIdempotentExecution(ctx, executionEndpoint, "key", record, event)
	if err != nil || !reused || id != "e1" {
		t.Fatalf("replay after restart: id=%q reused=%t err=%v", id, reused, err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE idempotency SET expires_at_ns = 0
		WHERE tenant_id = ? AND endpoint = ? AND key = ?`, record.TenantID, executionEndpoint, "key"); err != nil {
		t.Fatal(err)
	}
	id, reused, err = s.CreateIdempotentExecution(ctx, executionEndpoint, "key", record, event)
	if err != nil || reused || id != "second" {
		t.Fatalf("create after expiry: id=%q reused=%t err=%v", id, reused, err)
	}
	if _, err := s.GetExecution(ctx, "e1"); err != nil {
		t.Fatalf("old execution was removed: %v", err)
	}
}

func TestIdempotentAdmissionRollsBackOnEventFailure(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.db.ExecContext(ctx, `CREATE TRIGGER reject_events BEFORE INSERT ON execution_events
		BEGIN SELECT RAISE(ABORT, 'test event failure'); END`); err != nil {
		t.Fatal(err)
	}
	record, event := fixture()
	if _, _, err := s.CreateIdempotentExecution(ctx, executionEndpoint, "key", record, event); err == nil {
		t.Fatal("create succeeded despite event failure")
	}
	if _, err := s.GetExecution(ctx, record.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("record survived rollback: %v", err)
	}
	var count int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM idempotency").Scan(&count); err != nil || count != 0 {
		t.Fatalf("idempotency count=%d err=%v", count, err)
	}
}

func TestIdempotentAdmissionRollsBackOnKeyWriteFailure(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.db.ExecContext(ctx, `CREATE TRIGGER reject_key BEFORE INSERT ON idempotency
		BEGIN SELECT RAISE(ABORT, 'test key failure'); END`); err != nil {
		t.Fatal(err)
	}
	record, event := fixture()
	if _, _, err := s.CreateIdempotentExecution(ctx, executionEndpoint, "key", record, event); err == nil {
		t.Fatal("create succeeded despite idempotency write failure")
	}
	if _, err := s.GetExecution(ctx, record.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("record survived rollback: %v", err)
	}
	events, err := s.EventsAfter(ctx, record.ID, 0)
	if err != nil || len(events) != 0 {
		t.Fatalf("event survived rollback: %+v err=%v", events, err)
	}
}

func TestSchemaV1MigratesWithoutLosingExecutions(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "state")
	s, err := Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	record, event := fixture()
	if err := s.CreateExecution(ctx, record, event); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, "DROP TABLE idempotency"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, "PRAGMA user_version = 1"); err != nil {
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
	got, err := s.GetExecution(ctx, record.ID)
	if err != nil || got.ID != record.ID {
		t.Fatalf("v1 execution after migration: %+v err=%v", got, err)
	}
	events, err := s.EventsAfter(ctx, record.ID, 0)
	if err != nil || len(events) != 1 || events[0].Sequence != 1 {
		t.Fatalf("v1 events after migration: %+v err=%v", events, err)
	}
	var version int
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil || version != schemaVersion {
		t.Fatalf("migrated version=%d err=%v", version, err)
	}
}
