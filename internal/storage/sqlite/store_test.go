package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
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
