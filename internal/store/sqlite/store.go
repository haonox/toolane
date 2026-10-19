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
	ErrRuntimeOwned    = errors.New("store already has a runtime owner")
)

const schemaVersion = 4

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
	db           *sql.DB
	lockFile     *os.File
	mu           sync.Mutex
	closed       bool
	runtimeOwned bool
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
	if version < 3 {
		for _, statement := range []string{
			`CREATE TABLE workflows (
				id TEXT PRIMARY KEY,
				tenant_id TEXT NOT NULL,
				state TEXT NOT NULL,
				request BLOB NOT NULL,
				created_at_ns INTEGER NOT NULL,
				finished_at_ns INTEGER,
				version INTEGER NOT NULL CHECK (version > 0)
			)`,
			`CREATE INDEX workflows_by_tenant ON workflows (tenant_id, created_at_ns)`,
			`CREATE TABLE workflow_nodes (
				workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
				node_id TEXT NOT NULL,
				position INTEGER NOT NULL CHECK (position >= 0),
				depends_on BLOB NOT NULL,
				execution_id TEXT NOT NULL UNIQUE REFERENCES executions(id) ON DELETE RESTRICT,
				PRIMARY KEY (workflow_id, node_id),
				UNIQUE (workflow_id, position)
			)`,
			"PRAGMA user_version = 3",
		} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("migrate database: %w", err)
			}
		}
	}
	if version < 4 {
		for _, statement := range []string{
			`CREATE TABLE workflow_idempotency (
				tenant_id TEXT NOT NULL,
				endpoint TEXT NOT NULL,
				key TEXT NOT NULL,
				request_hash BLOB NOT NULL CHECK (length(request_hash) = 32),
				workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE RESTRICT,
				expires_at_ns INTEGER NOT NULL,
				PRIMARY KEY (tenant_id, endpoint, key)
			)`,
			"CREATE INDEX workflow_idempotency_by_workflow ON workflow_idempotency (workflow_id)",
			"PRAGMA user_version = 4",
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
	if s.runtimeOwned {
		return ErrRuntimeOwned
	}
	s.closed = true
	dbErr := s.db.Close()
	lockErr := s.lockFile.Close()
	return errors.Join(dbErr, lockErr)
}

// ClaimRuntime prevents two managers in one process from scheduling the same
// persisted queue through one Store. Release the claim only after all workers
// have stopped; the state directory lock separately excludes other processes.
func (s *Store) ClaimRuntime() (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("store is closed")
	}
	if s.runtimeOwned {
		return nil, ErrRuntimeOwned
	}
	s.runtimeOwned = true
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			s.runtimeOwned = false
			s.mu.Unlock()
		})
	}, nil
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
	return s.TransitionWithEvents(ctx, id, expectedVersion, state, snapshot, nil, event)
}

// TransitionWithEvents commits output chunks and their resulting snapshot and
// state event together. A crash cannot expose output without its final record.
func (s *Store) TransitionWithEvents(ctx context.Context, id string, expectedVersion int64, state string, snapshot []byte, output []Event, event Event) (int64, error) {
	if id == "" || expectedVersion < 1 || state == "" || len(snapshot) == 0 || event.Kind == "" || event.OccurredAt.IsZero() || event.ExecutionID != "" || event.Sequence != 0 {
		return 0, errors.New("incomplete transition or event")
	}
	if len(output) > 1024 {
		return 0, errors.New("too many output events")
	}
	for _, item := range output {
		if item.Kind != "output" || item.OccurredAt.IsZero() || len(item.Payload) == 0 || item.ExecutionID != "" || item.Sequence != 0 {
			return 0, errors.New("invalid output event")
		}
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
	for _, item := range output {
		if _, err := tx.ExecContext(ctx, `INSERT INTO execution_events
			(execution_id, sequence, kind, payload, occurred_at_ns)
			VALUES (?, ?, ?, ?, ?)`, id, sequence, item.Kind, item.Payload, item.OccurredAt.UnixNano()); err != nil {
			return 0, err
		}
		sequence++
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

// ListExecutions returns records in admission order for startup coordination.
func (s *Store) ListExecutions(ctx context.Context) ([]Execution, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, tenant_id, state, snapshot, request, created_at_ns, version
		FROM executions ORDER BY created_at_ns, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []Execution
	for rows.Next() {
		var record Execution
		var createdAtNS int64
		if err := rows.Scan(&record.ID, &record.TenantID, &record.State, &record.Snapshot, &record.Request, &createdAtNS, &record.Version); err != nil {
			return nil, err
		}
		record.CreatedAt = time.Unix(0, createdAtNS).UTC()
		records = append(records, record)
	}
	return records, rows.Err()
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

// EventBounds reports the retained range and row count after a cursor. The
// count lets a stream reject missing history before writing HTTP headers.
func (s *Store) EventBounds(ctx context.Context, executionID string, after int64) (first, last, remaining int64, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT COALESCE(MIN(sequence), 0), COALESCE(MAX(sequence), 0),
		COUNT(*) FILTER (WHERE sequence > ?) FROM execution_events WHERE execution_id = ?`,
		after, executionID).Scan(&first, &last, &remaining)
	return
}

// EventsAfterLimit bounds a replay batch independently of the retained history.
func (s *Store) EventsAfterLimit(ctx context.Context, executionID string, after int64, limit int) ([]Event, error) {
	if after < 0 || limit < 1 || limit > 256 {
		return nil, errors.New("invalid event cursor or batch limit")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT sequence, kind, payload, occurred_at_ns FROM execution_events
		WHERE execution_id = ? AND sequence > ? ORDER BY sequence LIMIT ?`, executionID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := make([]Event, 0, limit)
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
