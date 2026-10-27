package sqlite

import (
	"database/sql"
	"fmt"
)

const schemaVersion = 1

// migrate runs before the store is exposed to the manager. SQLite commits the
// schema and user_version together, so a failed migration can be retried.
func migrate(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin SQLite migration: %w", err)
	}
	defer tx.Rollback()
	var version int
	if err := tx.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("read SQLite schema version: %w", err)
	}
	if version > schemaVersion {
		return fmt.Errorf("SQLite schema version %d is newer than supported version %d", version, schemaVersion)
	}
	if version == 0 {
		statements := []string{
			`CREATE TABLE executions (
				seq INTEGER PRIMARY KEY AUTOINCREMENT,
				id TEXT NOT NULL UNIQUE,
				tenant_id TEXT NOT NULL,
				state TEXT NOT NULL CHECK (state IN
					('queued', 'running', 'cancelling', 'succeeded', 'failed',
					 'timed_out', 'cancelled', 'interrupted')),
				queue_deadline_ns INTEGER NOT NULL,
				finished_at_ns INTEGER,
				payload BLOB NOT NULL,
				version INTEGER NOT NULL CHECK (version > 0)
			)`,
			`CREATE INDEX executions_active_seq ON executions(state, seq)`,
			`CREATE INDEX executions_queue_deadline ON executions(state, queue_deadline_ns)`,
			`CREATE INDEX executions_finished ON executions(finished_at_ns) WHERE finished_at_ns IS NOT NULL`,
			`PRAGMA user_version = 1`,
		}
		for _, statement := range statements {
			if _, err := tx.Exec(statement); err != nil {
				return fmt.Errorf("migrate SQLite schema: %w", err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit SQLite migration: %w", err)
	}
	return nil
}
