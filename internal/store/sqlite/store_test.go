//go:build linux

package sqlite

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func fixture() (Execution, Event) {
	now := time.Date(2026, 10, 19, 1, 2, 3, 4, time.UTC)
	return Execution{
		ID: "e1", TenantID: "tenant-a", State: "queued",
		Snapshot: []byte(`{"status":"queued"}`), Request: []byte(`{"tool":"demo"}`), CreatedAt: now,
	}, Event{Kind: "state", Payload: []byte(`{"status":"queued"}`), OccurredAt: now}
}

func TestPersistTransitionAndExclusiveOpen(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "state")
	s, err := Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, dir); !errors.Is(err, ErrLocked) {
		t.Fatalf("second opener: got %v, want ErrLocked", err)
	}
	record, event := fixture()
	if err := s.CreateExecution(ctx, record, event); err != nil {
		t.Fatal(err)
	}
	version, err := s.Transition(ctx, record.ID, 1, "running", []byte(`{"status":"running"}`), Event{
		Kind: "state", Payload: []byte(`{"status":"running"}`), OccurredAt: event.OccurredAt.Add(time.Second),
	})
	if err != nil || version != 2 {
		t.Fatalf("transition: version=%d err=%v", version, err)
	}
	if _, err := s.Transition(ctx, record.ID, 1, "failed", []byte(`{"status":"failed"}`), event); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale transition: %v", err)
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
	if err != nil || got.State != "running" || got.Version != 2 || string(got.Request) != string(record.Request) || !got.CreatedAt.Equal(record.CreatedAt) {
		t.Fatalf("reopened record: %+v, err=%v", got, err)
	}
	events, err := s.EventsAfter(ctx, record.ID, 0)
	if err != nil || len(events) != 2 || events[0].Sequence != 1 || events[1].Sequence != 2 {
		t.Fatalf("reopened events: %+v, err=%v", events, err)
	}
	replay, err := s.EventsAfter(ctx, record.ID, 1)
	if err != nil || len(replay) != 1 || replay[0].Sequence != 2 {
		t.Fatalf("events after 1: %+v, err=%v", replay, err)
	}
	if _, err := s.GetExecution(ctx, "absent"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing execution: %v", err)
	}
	if info, err := os.Stat(filepath.Join(dir, "toolane.sqlite")); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("database permissions: info=%v err=%v", info, err)
	}
}

func TestCreateAndTransitionRollBackWhenEventWriteFails(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	record, event := fixture()
	if _, err := s.db.ExecContext(ctx, `CREATE TRIGGER reject_events BEFORE INSERT ON execution_events
		BEGIN SELECT RAISE(ABORT, 'test event failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateExecution(ctx, record, event); err == nil {
		t.Fatal("create succeeded despite event write failure")
	}
	if _, err := s.GetExecution(ctx, record.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("record survived failed transaction: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, "DROP TRIGGER reject_events"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateExecution(ctx, record, event); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE TRIGGER reject_events BEFORE INSERT ON execution_events
		BEGIN SELECT RAISE(ABORT, 'test event failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Transition(ctx, record.ID, 1, "running", []byte(`{"status":"running"}`), event); err == nil {
		t.Fatal("transition succeeded despite event write failure")
	}
	got, err := s.GetExecution(ctx, record.ID)
	if err != nil || got.Version != 1 || got.State != "queued" {
		t.Fatalf("state changed despite failed event: %+v, err=%v", got, err)
	}
}

func TestRejectNewerSchemaWithoutChangingIt(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "state")
	s, err := Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, "PRAGMA user_version = 3"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, dir); !errors.Is(err, ErrNewerSchema) {
		t.Fatalf("opening future schema: %v", err)
	}
	// A rejected open must release the state lock.
	if _, err := Open(ctx, dir); !errors.Is(err, ErrNewerSchema) {
		t.Fatalf("second attempt after rejected open: %v", err)
	}
}

func TestSQLiteSettingsSurviveConnectionReplacement(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.db.SetConnMaxLifetime(time.Nanosecond)
	for _, check := range []struct {
		pragma string
		want   string
	}{
		{"foreign_keys", "1"},
		{"journal_mode", "wal"},
		{"synchronous", "2"},
		{"busy_timeout", "5000"},
	} {
		var got string
		if err := s.db.QueryRowContext(ctx, "PRAGMA "+check.pragma).Scan(&got); err != nil || got != check.want {
			t.Fatalf("PRAGMA %s = %q, err=%v; want %q", check.pragma, got, err, check.want)
		}
	}
}
