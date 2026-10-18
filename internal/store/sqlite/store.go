// Package sqlite stores durable execution records and their ordered events.
// Lifecycle decisions remain the execution manager's responsibility.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

var (
	ErrLocked          = errors.New("state directory is already in use")
	ErrNotFound        = errors.New("execution not found")
	ErrVersionConflict = errors.New("execution version conflict")
	ErrNewerSchema     = errors.New("database schema is newer than this binary")
)

const schemaVersion = 2

// Execution contains the data required to reconstruct one execution. Snapshot
// and Request are JSON encoded by the manager; the store treats them as bytes.
type Execution struct {
	ID        string
	TenantID  string
	State     string
	Snapshot  []byte
	Request   []byte
	CreatedAt time.Time
	Version   int64
}

// Event is returned with its execution ID and sequence. On writes, callers
// leave those two fields empty; the store assigns them in the transaction.
type Event struct {
	ExecutionID string
	Sequence    int64
	Kind        string
	Payload     []byte
	OccurredAt  time.Time
}

// Store holds the state directory lock until Close. SQLite connections are
// limited to one because the first version has a single process writer.
type Store struct {
	db       *sql.DB
	lockFile *os.File
	mu       sync.Mutex
	closed   bool
}

// Open creates or migrates the database and exclusively claims stateDir.
// A kernel file lock releases automatically if the process crashes.
func Open(ctx context.Context, stateDir string) (_ *Store, err error) {
	if stateDir == "" {
		return nil, errors.New("empty state directory")
	}
	stateDir, err = filepath.Abs(stateDir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(stateDir, 0700); err != nil {
		return nil, fmt.Errorf("create state directory: %w", err)
	}
	stateDir, err = filepath.EvalSymlinks(stateDir)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(stateDir)
	if err != nil {
		return nil, err
	}
	if info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("state directory permissions must exclude group and others")
	}
	lockFile, err := os.OpenFile(filepath.Join(stateDir, ".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open state lock: %w", err)
	}
	defer func() {
		if err != nil {
			_ = lockFile.Close()
		}
	}()
	if err = lockStateDir(lockFile); err != nil {
		return nil, err
	}

	path := filepath.Join(stateDir, "toolane.sqlite")
	info, statErr := os.Lstat(path)
	if statErr == nil && !info.Mode().IsRegular() {
		return nil, errors.New("database path is not a regular file")
	}
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return nil, statErr
	}
	if statErr == nil && info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("database file permissions must exclude group and others")
	}
	// SQLite creates the database file with the process umask. Creating it here
	// fixes its permissions independently of the caller's umask.
	f, openErr := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if openErr != nil {
		return nil, fmt.Errorf("open database file: %w", openErr)
	}
	if closeErr := f.Close(); closeErr != nil {
		return nil, closeErr
	}
	uri := &url.URL{Scheme: "file", Path: path}
	params := url.Values{
		"_busy_timeout": []string{"5000"},
		"_journal_mode": []string{"WAL"},
		"_synchronous":  []string{"FULL"},
		"_foreign_keys": []string{"1"},
	}
	uri.RawQuery = params.Encode()
	dsn := uri.String()
	db, openErr := sql.Open("sqlite", dsn)
	if openErr != nil {
		return nil, openErr
	}
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()
	db.SetMaxOpenConns(1)
	if err = migrate(ctx, db); err != nil {
		return nil, err
	}
	return &Store{db: db, lockFile: lockFile}, nil
}

func migrate(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var version int
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version > schemaVersion {
		return fmt.Errorf("%w: found %d, supported %d", ErrNewerSchema, version, schemaVersion)
	}
	if version == 0 {
		for _, statement := range []string{
			`CREATE TABLE executions (
				id TEXT PRIMARY KEY,
				tenant_id TEXT NOT NULL,
				state TEXT NOT NULL,
				snapshot BLOB NOT NULL,
				request BLOB NOT NULL,
				created_at_ns INTEGER NOT NULL,
				version INTEGER NOT NULL CHECK (version > 0)
			)`,
			`CREATE INDEX executions_by_tenant ON executions (tenant_id, created_at_ns)`,
			`CREATE TABLE execution_events (
				execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
				sequence INTEGER NOT NULL CHECK (sequence > 0),
				kind TEXT NOT NULL CHECK (length(kind) > 0),
				payload BLOB NOT NULL,
				occurred_at_ns INTEGER NOT NULL,
				PRIMARY KEY (execution_id, sequence)
			)`,
			"PRAGMA user_version = 1",
		} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("migrate database: %w", err)
			}
		}
	}
	if version < 2 {
		for _, statement := range []string{
			`CREATE TABLE idempotency (
				tenant_id TEXT NOT NULL,
				endpoint TEXT NOT NULL,
				key TEXT NOT NULL,
				request_hash BLOB NOT NULL CHECK (length(request_hash) = 32),
				execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE RESTRICT,
				expires_at_ns INTEGER NOT NULL,
				PRIMARY KEY (tenant_id, endpoint, key)
			)`,
			"CREATE INDEX idempotency_by_execution ON idempotency (execution_id)",
			"PRAGMA user_version = 2",
		} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("migrate database: %w", err)
			}
		}
	}
	return tx.Commit()
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	dbErr := s.db.Close()
	lockErr := s.lockFile.Close()
	return errors.Join(dbErr, lockErr)
}

// CreateExecution commits the accepted record and its first event together.
// The caller must not acknowledge acceptance until this method succeeds.
func (s *Store) CreateExecution(ctx context.Context, record Execution, event Event) error {
	if err := validateCreate(record, event); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := insertExecution(ctx, tx, record, event); err != nil {
		return err
	}
	return tx.Commit()
}

func validateCreate(record Execution, event Event) error {
	if record.ID == "" || record.TenantID == "" || record.State == "" || len(record.Snapshot) == 0 || len(record.Request) == 0 || record.CreatedAt.IsZero() || event.Kind == "" || event.OccurredAt.IsZero() || event.ExecutionID != "" || event.Sequence != 0 {
		return errors.New("incomplete execution or event")
	}
	return nil
}

func insertExecution(ctx context.Context, tx *sql.Tx, record Execution, event Event) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO executions
		(id, tenant_id, state, snapshot, request, created_at_ns, version)
		VALUES (?, ?, ?, ?, ?, ?, 1)`, record.ID, record.TenantID, record.State, record.Snapshot, record.Request, record.CreatedAt.UnixNano()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO execution_events
		(execution_id, sequence, kind, payload, occurred_at_ns)
		VALUES (?, 1, ?, ?, ?)`, record.ID, event.Kind, event.Payload, event.OccurredAt.UnixNano()); err != nil {
		return err
	}
	return nil
}

// Transition atomically replaces the snapshot and appends its state event.
// expectedVersion prevents a stale caller from overwriting a newer decision.
func (s *Store) Transition(ctx context.Context, id string, expectedVersion int64, state string, snapshot []byte, event Event) (int64, error) {
	if id == "" || expectedVersion < 1 || state == "" || len(snapshot) == 0 || event.Kind == "" || event.OccurredAt.IsZero() || event.ExecutionID != "" || event.Sequence != 0 {
		return 0, errors.New("incomplete transition or event")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE executions SET state = ?, snapshot = ?, version = version + 1
		WHERE id = ? AND version = ?`, state, snapshot, id, expectedVersion)
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if affected == 0 {
		var exists bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM executions WHERE id = ?)", id).Scan(&exists); err != nil {
			return 0, err
		}
		if !exists {
			return 0, ErrNotFound
		}
		return 0, ErrVersionConflict
	}
	var sequence int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), 0) + 1 FROM execution_events WHERE execution_id = ?`, id).Scan(&sequence); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO execution_events
		(execution_id, sequence, kind, payload, occurred_at_ns)
		VALUES (?, ?, ?, ?, ?)`, id, sequence, event.Kind, event.Payload, event.OccurredAt.UnixNano()); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return expectedVersion + 1, nil
}

func (s *Store) GetExecution(ctx context.Context, id string) (Execution, error) {
	var record Execution
	var createdAtNS int64
	err := s.db.QueryRowContext(ctx, `SELECT id, tenant_id, state, snapshot, request, created_at_ns, version
		FROM executions WHERE id = ?`, id).Scan(&record.ID, &record.TenantID, &record.State, &record.Snapshot, &record.Request, &createdAtNS, &record.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return Execution{}, ErrNotFound
	}
	if err != nil {
		return Execution{}, err
	}
	record.CreatedAt = time.Unix(0, createdAtNS).UTC()
	return record, nil
}

// EventsAfter returns persisted events in sequence order for replay.
func (s *Store) EventsAfter(ctx context.Context, executionID string, after int64) ([]Event, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT sequence, kind, payload, occurred_at_ns FROM execution_events
		WHERE execution_id = ? AND sequence > ? ORDER BY sequence`, executionID, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []Event
	for rows.Next() {
		var event Event
		var occurredAtNS int64
		if err := rows.Scan(&event.Sequence, &event.Kind, &event.Payload, &occurredAtNS); err != nil {
			return nil, err
		}
		event.ExecutionID = executionID
		event.OccurredAt = time.Unix(0, occurredAtNS).UTC()
		events = append(events, event)
	}
	return events, rows.Err()
}
