// Package sqlite provides durable execution records without owning scheduling.
// The execution manager remains responsible for state transitions and its
// in-memory queues; this package only commits and reads their durable image.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/mattn/go-sqlite3"
)

var (
	ErrNotFound = errors.New("execution record not found")
	ErrConflict = errors.New("execution record conflict")
	ErrLocked   = errors.New("execution database already open by another process")
)

// SynchronousMode controls when SQLite syncs the WAL to stable storage.
// Normal preserves committed transactions across application crashes but may
// lose the newest commits after an operating-system crash or power failure.
type SynchronousMode string

const (
	SynchronousFull   SynchronousMode = "full"
	SynchronousNormal SynchronousMode = "normal"
)

// Record is the durable envelope around a serialized execution. Payload is
// opaque to the store so execution-specific fields can evolve independently.
// Sequence and Version are assigned by the store.
type Record struct {
	Sequence       int64
	Version        int64
	ID             string
	TenantID       string
	State          string
	QueueDeadline  time.Time
	FinishedAt     *time.Time
	Payload        []byte
	IdempotencyKey string
	RequestHash    string
}

// Store owns one SQLite connection and an OS lock. A second Toolane process
// must not schedule the same database, even though SQLite allows many readers.
type Store struct {
	db        *sql.DB
	lock      *os.File
	closeOnce sync.Once
	closeErr  error
}

// Open creates or migrates a database in an existing private directory. The
// lock is held until Close; a stale lock file after a crash is harmless.
func Open(path string) (_ *Store, err error) {
	return OpenWithSynchronousMode(path, SynchronousFull)
}

// OpenWithSynchronousMode opens a store with an explicit WAL sync policy.
// An empty mode uses the durable default, FULL.
func OpenWithSynchronousMode(path string, mode SynchronousMode) (_ *Store, err error) {
	if mode == "" {
		mode = SynchronousFull
	}
	var synchronousPragma string
	switch mode {
	case SynchronousFull:
		synchronousPragma = "PRAGMA synchronous=FULL"
	case SynchronousNormal:
		synchronousPragma = "PRAGMA synchronous=NORMAL"
	default:
		return nil, fmt.Errorf("unsupported SQLite synchronous mode %q", mode)
	}
	if !filepath.IsAbs(path) || path == "" {
		return nil, errors.New("SQLite path must be absolute")
	}
	dir := filepath.Dir(path)
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, fmt.Errorf("stat SQLite directory: %w", err)
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("SQLite directory must be private (mode 0700)")
	}
	if info, statErr := os.Lstat(path); statErr == nil {
		if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			return nil, errors.New("SQLite database must be a private regular file")
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return nil, fmt.Errorf("stat SQLite database: %w", statErr)
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open SQLite lock: %w", err)
	}
	defer func() {
		if err != nil {
			_ = lock.Close()
		}
	}()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrLocked
		}
		return nil, fmt.Errorf("lock SQLite database: %w", err)
	}
	uri := (&url.URL{Scheme: "file", Path: path}).String()
	db, err := sql.Open("sqlite3", uri)
	if err != nil {
		return nil, fmt.Errorf("open SQLite database: %w", err)
	}
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()
	// The manager serializes transitions; one connection also makes PRAGMAs
	// connection-wide and prevents an in-process writer convoy.
	db.SetMaxOpenConns(1)
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL", synchronousPragma, "PRAGMA foreign_keys=ON", "PRAGMA busy_timeout=5000",
	} {
		if _, err = db.Exec(pragma); err != nil {
			return nil, fmt.Errorf("configure SQLite: %w", err)
		}
	}
	if err = os.Chmod(path, 0600); err != nil {
		return nil, fmt.Errorf("secure SQLite database: %w", err)
	}
	if err = migrate(db); err != nil {
		return nil, err
	}
	return &Store{db: db, lock: lock}, nil
}

// Close releases the database and its single-owner lock. It is safe to call
// more than once.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.db.Close()
		if err := s.lock.Close(); s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

func validRecord(r Record) error {
	if r.ID == "" || r.TenantID == "" || r.QueueDeadline.IsZero() || len(r.Payload) == 0 || !json.Valid(r.Payload) {
		return errors.New("execution record has missing or invalid fields")
	}
	if (r.IdempotencyKey == "") != (r.RequestHash == "") {
		return errors.New("idempotency key and request hash must be supplied together")
	}
	if r.RequestHash != "" {
		decoded, err := hex.DecodeString(r.RequestHash)
		if err != nil || len(decoded) != 32 {
			return errors.New("invalid request hash")
		}
	}
	switch r.State {
	case "queued", "running", "cancelling":
		if r.FinishedAt != nil {
			return errors.New("unfinished execution has a finish time")
		}
	case "succeeded", "failed", "timed_out", "cancelled", "interrupted":
		if r.FinishedAt == nil || r.FinishedAt.IsZero() {
			return errors.New("terminal execution needs a finish time")
		}
	default:
		return errors.New("invalid execution state")
	}
	return nil
}

// Insert commits one accepted execution before the caller exposes it to a
// worker or returns HTTP 202. Sequence preserves admission order on recovery.
func (s *Store) Insert(ctx context.Context, r Record) (Record, error) {
	if err := validRecord(r); err != nil {
		return Record{}, err
	}
	if r.Sequence != 0 || r.Version != 0 {
		return Record{}, errors.New("new execution must not have a sequence or version")
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO executions
		(id, tenant_id, state, queue_deadline_ns, finished_at_ns, payload, version, idempotency_key, request_hash)
		VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)`, r.ID, r.TenantID, r.State,
		r.QueueDeadline.UnixNano(), nullableTime(r.FinishedAt), r.Payload,
		nullableString(r.IdempotencyKey), nullableString(r.RequestHash))
	if err != nil {
		return Record{}, translateWriteError(err)
	}
	r.Sequence, err = result.LastInsertId()
	if err != nil {
		return Record{}, err
	}
	r.Version = 1
	r.Payload = append([]byte(nil), r.Payload...)
	return r, nil
}

// CompareAndSwap durably replaces an execution only if its version still
// matches. Callers should publish the new state in memory after this succeeds.
func (s *Store) CompareAndSwap(ctx context.Context, r Record) (Record, error) {
	if err := validRecord(r); err != nil {
		return Record{}, err
	}
	if r.Sequence <= 0 || r.Version <= 0 {
		return Record{}, errors.New("execution needs its stored sequence and version")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE executions SET state = ?, queue_deadline_ns = ?,
		finished_at_ns = ?, payload = ?, version = version + 1
		WHERE seq = ? AND id = ? AND version = ?`, r.State, r.QueueDeadline.UnixNano(),
		nullableTime(r.FinishedAt), r.Payload, r.Sequence, r.ID, r.Version)
	if err != nil {
		return Record{}, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return Record{}, err
	}
	if count == 0 {
		return Record{}, ErrConflict
	}
	r.Version++
	r.Payload = append([]byte(nil), r.Payload...)
	return r, nil
}

// Get loads a record, including its opaque payload.
func (s *Store) Get(ctx context.Context, id string) (Record, error) {
	r, err := scanRecord(s.db.QueryRowContext(ctx, `SELECT seq, version, id, tenant_id, state,
		queue_deadline_ns, finished_at_ns, payload, idempotency_key, request_hash FROM executions WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, ErrNotFound
	}
	return r, err
}

// FindByIdempotency returns the original execution for one tenant and key.
func (s *Store) FindByIdempotency(ctx context.Context, tenantID, key string) (Record, error) {
	r, err := scanRecord(s.db.QueryRowContext(ctx, `SELECT seq, version, id, tenant_id, state,
		queue_deadline_ns, finished_at_ns, payload, idempotency_key, request_hash
		FROM executions WHERE tenant_id = ? AND idempotency_key = ?`, tenantID, key))
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, ErrNotFound
	}
	return r, err
}

// ListActive returns only work that can need recovery. Completed records stay
// on disk and are fetched by ID instead of being loaded into scheduler memory.
func (s *Store) ListActive(ctx context.Context) ([]Record, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT seq, version, id, tenant_id, state,
		queue_deadline_ns, finished_at_ns, payload, idempotency_key, request_hash FROM executions
		WHERE state IN ('queued', 'running', 'cancelling') ORDER BY seq`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []Record
	for rows.Next() {
		r, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

// DeleteExpired removes at most limit terminal records whose finish time is
// at or before cutoff. It returns IDs so the caller can evict memory copies.
func (s *Store) DeleteExpired(ctx context.Context, cutoff time.Time, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, errors.New("cleanup limit must be positive")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id FROM executions WHERE finished_at_ns <= ?
		ORDER BY finished_at_ns, seq LIMIT ?`, cutoff.UnixNano(), limit)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, `DELETE FROM executions WHERE id = ? AND finished_at_ns <= ?`, id, cutoff.UnixNano()); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return ids, nil
}

// DeleteExpiredID releases one expired execution and its idempotency key.
// It is used when a retry reaches the key before periodic cleanup does.
func (s *Store) DeleteExpiredID(ctx context.Context, id string, cutoff time.Time) (bool, error) {
	result, err := s.db.ExecContext(ctx, `DELETE FROM executions WHERE id = ? AND finished_at_ns <= ?`, id, cutoff.UnixNano())
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	return count > 0, err
}

type scanner interface{ Scan(...any) error }

func scanRecord(row scanner) (Record, error) {
	var r Record
	var deadline int64
	var finished sql.NullInt64
	var key, hash sql.NullString
	err := row.Scan(&r.Sequence, &r.Version, &r.ID, &r.TenantID, &r.State, &deadline, &finished, &r.Payload, &key, &hash)
	if err != nil {
		return Record{}, err
	}
	r.QueueDeadline = time.Unix(0, deadline).UTC()
	if finished.Valid {
		at := time.Unix(0, finished.Int64).UTC()
		r.FinishedAt = &at
	}
	if key.Valid {
		r.IdempotencyKey = key.String
	}
	if hash.Valid {
		r.RequestHash = hash.String
	}
	if r.Sequence <= 0 || r.Version <= 0 {
		return Record{}, errors.New("stored execution has invalid sequence or version")
	}
	if err := validRecord(r); err != nil {
		return Record{}, fmt.Errorf("stored execution %q: %w", r.ID, err)
	}
	return r, nil
}

func nullableTime(at *time.Time) any {
	if at == nil {
		return nil
	}
	return at.UnixNano()
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func translateWriteError(err error) error {
	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) && sqliteErr.Code == sqlite3.ErrConstraint {
		return fmt.Errorf("%w: %v", ErrConflict, err)
	}
	return err
}
