package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func openTestStore(t *testing.T, path string) *Store {
	t.Helper()
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func privateDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func queuedRecord(id string, deadline time.Time) Record {
	return Record{ID: id, TenantID: "tenant-a", State: "queued", QueueDeadline: deadline,
		Payload: []byte(`{"tool":"test.echo","arguments":{"value":"private"}}`)}
}

func TestSynchronousModeAndReopen(t *testing.T) {
	for _, test := range []struct {
		name string
		mode SynchronousMode
		want int
	}{
		{"default", "", 2},
		{"full", SynchronousFull, 2},
		{"normal", SynchronousNormal, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(privateDir(t), "executions.sqlite")
			store, err := OpenWithSynchronousMode(path, test.mode)
			if err != nil {
				t.Fatal(err)
			}
			var got int
			if err := store.db.QueryRow("PRAGMA synchronous").Scan(&got); err != nil || got != test.want {
				t.Fatalf("synchronous = %d, %v; want %d", got, err, test.want)
			}
			if _, err := store.Insert(context.Background(), queuedRecord("saved", time.Now().Add(time.Minute))); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenWithSynchronousMode(path, test.mode)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if _, err := reopened.Get(context.Background(), "saved"); err != nil {
				t.Fatalf("record lost after reopening store: %v", err)
			}
		})
	}
	if _, err := OpenWithSynchronousMode(filepath.Join(privateDir(t), "invalid.sqlite"), "off"); err == nil {
		t.Fatal("accepted unsupported synchronous mode")
	}
}

func TestOpenInsertRecoverAndCompareAndSwap(t *testing.T) {
	path := filepath.Join(privateDir(t), "executions.sqlite")
	store := openTestStore(t, path)
	ctx := context.Background()
	deadline := time.Now().UTC().Add(time.Minute).Truncate(time.Nanosecond)
	first, err := store.Insert(ctx, queuedRecord("one", deadline))
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Insert(ctx, queuedRecord("two", deadline))
	if err != nil {
		t.Fatal(err)
	}
	if first.Sequence != 1 || second.Sequence != 2 || first.Version != 1 {
		t.Fatalf("unexpected insertion order or version: %+v %+v", first, second)
	}
	if _, err := store.Insert(ctx, queuedRecord("one", deadline)); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate ID error = %v", err)
	}
	active, err := store.ListActive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 2 {
		t.Fatalf("active count = %d", len(active))
	}
	if got := []string{active[0].ID, active[1].ID}; !reflect.DeepEqual(got, []string{"one", "two"}) {
		t.Fatalf("active order = %v", got)
	}
	finished := time.Now().UTC()
	first.State, first.FinishedAt = "succeeded", &finished
	first.Payload = []byte(`{"status":"succeeded","result":{"value":"ok"}}`)
	updated, err := store.CompareAndSwap(ctx, first)
	if err != nil || updated.Version != 2 {
		t.Fatalf("update = %+v, %v", updated, err)
	}
	if _, err := store.CompareAndSwap(ctx, first); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale update error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openTestStore(t, path)
	got, err := reopened.Get(ctx, "one")
	if err != nil || got.State != "succeeded" || got.Version != 2 || !got.FinishedAt.Equal(finished) ||
		string(got.Payload) != string(updated.Payload) {
		t.Fatalf("recovered record = %+v, %v", got, err)
	}
	active, err = reopened.ListActive(ctx)
	if err != nil || len(active) != 1 || active[0].ID != "two" {
		t.Fatalf("recovered active = %+v, %v", active, err)
	}
	if _, err := reopened.Get(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing ID error = %v", err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("database permissions = %v, %v", info, err)
	}
}

func TestOpenRejectsConcurrentOwnerAndUnsafeDirectory(t *testing.T) {
	dir := privateDir(t)
	path := filepath.Join(dir, "executions.sqlite")
	store := openTestStore(t, path)
	if _, err := Open(path); !errors.Is(err, ErrLocked) {
		t.Fatalf("second owner error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	other := openTestStore(t, path)
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); err == nil {
		t.Fatal("unsafe database directory accepted")
	}
}

func TestOpenRejectsNewerSchema(t *testing.T) {
	path := filepath.Join(privateDir(t), "executions.sqlite")
	store := openTestStore(t, path)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA user_version = 99"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); err == nil {
		t.Fatal("newer schema accepted")
	}
}

func TestReadRejectsCorruptActiveRecord(t *testing.T) {
	path := filepath.Join(privateDir(t), "executions.sqlite")
	store := openTestStore(t, path)
	ctx := context.Background()
	if _, err := store.Insert(ctx, queuedRecord("one", time.Now().Add(time.Minute))); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE executions SET payload = ? WHERE id = ?`, []byte(`{bad`), "one"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ListActive(ctx); err == nil {
		t.Fatal("corrupt queued record was loaded for recovery")
	}
}

func TestMigrateV1AndTenantScopedIdempotency(t *testing.T) {
	path := filepath.Join(privateDir(t), "executions.sqlite")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`CREATE TABLE executions (seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
			tenant_id TEXT NOT NULL, state TEXT NOT NULL, queue_deadline_ns INTEGER NOT NULL,
			finished_at_ns INTEGER, payload BLOB NOT NULL, version INTEGER NOT NULL)`,
		`INSERT INTO executions (id, tenant_id, state, queue_deadline_ns, payload, version)
			VALUES ('legacy', 'tenant-a', 'queued', 2000000000000000000, '{}', 1)`,
		`PRAGMA user_version = 1`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	store := openTestStore(t, path)
	ctx := context.Background()
	if old, err := store.Get(ctx, "legacy"); err != nil || old.IdempotencyKey != "" || old.RequestHash != "" {
		t.Fatalf("migrated record = %+v, %v", old, err)
	}
	deadline := time.Now().Add(time.Minute)
	hash := strings.Repeat("a", 64)
	first := queuedRecord("first", deadline)
	first.IdempotencyKey, first.RequestHash = "retry-1", hash
	if _, err := store.Insert(ctx, first); err != nil {
		t.Fatal(err)
	}
	if got, err := store.FindByIdempotency(ctx, "tenant-a", "retry-1"); err != nil || got.ID != first.ID || got.RequestHash != hash {
		t.Fatalf("key lookup = %+v, %v", got, err)
	}
	duplicate := queuedRecord("duplicate", deadline)
	duplicate.IdempotencyKey, duplicate.RequestHash = "retry-1", hash
	if _, err := store.Insert(ctx, duplicate); !errors.Is(err, ErrConflict) {
		t.Fatalf("same-tenant duplicate key = %v", err)
	}
	duplicate.TenantID = "tenant-b"
	if _, err := store.Insert(ctx, duplicate); err != nil {
		t.Fatalf("same key in another tenant: %v", err)
	}
}

func TestDeleteExpiredResultsPreservesActiveAndRecent(t *testing.T) {
	store := openTestStore(t, filepath.Join(privateDir(t), "executions.sqlite"))
	ctx := context.Background()
	now := time.Now().UTC()
	old := queuedRecord("old", now)
	old.State, old.FinishedAt = "succeeded", ptrTime(now.Add(-2*time.Hour))
	old.IdempotencyKey, old.RequestHash = "old-key", strings.Repeat("b", 64)
	if _, err := store.Insert(ctx, old); err != nil {
		t.Fatal(err)
	}
	recent := queuedRecord("recent", now)
	recent.State, recent.FinishedAt = "failed", ptrTime(now)
	if _, err := store.Insert(ctx, recent); err != nil {
		t.Fatal(err)
	}
	active := queuedRecord("active", now.Add(time.Minute))
	if _, err := store.Insert(ctx, active); err != nil {
		t.Fatal(err)
	}
	ids, err := store.DeleteExpired(ctx, now.Add(-time.Hour), 1)
	if err != nil || !reflect.DeepEqual(ids, []string{"old"}) {
		t.Fatalf("deleted IDs = %v, %v", ids, err)
	}
	if _, err := store.Get(ctx, "old"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired result remains: %v", err)
	}
	if _, err := store.FindByIdempotency(ctx, "tenant-a", "old-key"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired key remains: %v", err)
	}
	if _, err := store.Get(ctx, "recent"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, "active"); err != nil {
		t.Fatal(err)
	}
	if deleted, err := store.DeleteExpiredID(ctx, "recent", now.Add(-time.Minute)); err != nil || deleted {
		t.Fatalf("recent result deleted early: %v, %v", deleted, err)
	}
}

func ptrTime(value time.Time) *time.Time { return &value }
