package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

var ErrRetentionCapacity = errors.New("durable record capacity is protected or exhausted")

// RetentionPolicy bounds persisted execution rows. Reserve makes room for a
// new batch before it is accepted. Now is explicit so callers and tests use
// the same cutoff for keys, age, and capacity.
type RetentionPolicy struct {
	MaxRecords int
	MaxAge     time.Duration
	Reserve    int
	Now        time.Time
}

type PruneResult struct {
	ExecutionIDs []string
	WorkflowIDs  []string
	Remaining    int
}

type retentionGroup struct {
	workflowID string
	ids        []string
	finishedAt time.Time
	safe       bool
	protected  bool
}

type retentionSnapshot struct {
	Snapshot struct {
		FinishedAt    *time.Time `json:"finished_at"`
		CleanupStatus string     `json:"cleanup_status"`
	} `json:"snapshot"`
	Resource json.RawMessage `json:"resource"`
}

// CountExecutions is the cheap admission check. The full retention scan runs
// only near capacity; the periodic sweep handles age while capacity is free.
func (s *Store) CountExecutions(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM executions`).Scan(&count)
	return count, err
}

// Prune removes expired keys, then complete and safe terminal records as
// whole groups. It removes records older than MaxAge and the oldest eligible
// groups needed to leave Reserve slots. Active work, live keys, and unfinished
// cleanup stay protected. A capacity error can follow a committed cleanup.
func (s *Store) Prune(ctx context.Context, policy RetentionPolicy) (PruneResult, error) {
	if policy.MaxRecords <= 0 || policy.MaxAge <= 0 || policy.Reserve < 0 || policy.Reserve > policy.MaxRecords || policy.Now.IsZero() {
		return PruneResult{}, errors.New("invalid retention policy")
	}
	nowNS := policy.Now.UnixNano()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PruneResult{}, err
	}
	defer tx.Rollback()
	for _, table := range []string{"idempotency", "workflow_idempotency"} {
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE expires_at_ns <= ?", nowNS); err != nil {
			return PruneResult{}, err
		}
	}
	protectedExecution, err := retainedIDs(ctx, tx, `SELECT execution_id FROM idempotency`)
	if err != nil {
		return PruneResult{}, err
	}
	protectedWorkflow, err := retainedIDs(ctx, tx, `SELECT workflow_id FROM workflow_idempotency`)
	if err != nil {
		return PruneResult{}, err
	}
	groups := make(map[string]*retentionGroup)
	rows, err := tx.QueryContext(ctx, `SELECT id, state, finished_at_ns FROM workflows`)
	if err != nil {
		return PruneResult{}, err
	}
	for rows.Next() {
		var id, state string
		var finishedNS sql.NullInt64
		if err := rows.Scan(&id, &state, &finishedNS); err != nil {
			rows.Close()
			return PruneResult{}, err
		}
		group := &retentionGroup{workflowID: id, safe: terminalRetentionState(state) && finishedNS.Valid, protected: protectedWorkflow[id]}
		if finishedNS.Valid {
			group.finishedAt = time.Unix(0, finishedNS.Int64).UTC()
		}
		groups[id] = group
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return PruneResult{}, err
	}
	rows.Close()
	owners := make(map[string]string)
	rows, err = tx.QueryContext(ctx, `SELECT execution_id, workflow_id FROM workflow_nodes`)
	if err != nil {
		return PruneResult{}, err
	}
	for rows.Next() {
		var executionID, workflowID string
		if err := rows.Scan(&executionID, &workflowID); err != nil {
			rows.Close()
			return PruneResult{}, err
		}
		owners[executionID] = workflowID
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return PruneResult{}, err
	}
	rows.Close()
	var candidates []*retentionGroup
	rows, err = tx.QueryContext(ctx, `SELECT id, state, snapshot FROM executions`)
	if err != nil {
		return PruneResult{}, err
	}
	total := 0
	for rows.Next() {
		var id, state string
		var snapshot []byte
		if err := rows.Scan(&id, &state, &snapshot); err != nil {
			rows.Close()
			return PruneResult{}, err
		}
		total++
		finished, safe, err := retentionEligibility(state, snapshot)
		if err != nil {
			rows.Close()
			return PruneResult{}, fmt.Errorf("execution %s retention snapshot: %w", id, err)
		}
		if workflowID := owners[id]; workflowID != "" {
			group := groups[workflowID]
			if group == nil {
				rows.Close()
				return PruneResult{}, fmt.Errorf("execution %s has missing workflow %s", id, workflowID)
			}
			group.ids = append(group.ids, id)
			group.safe = group.safe && safe
			if finished.After(group.finishedAt) {
				group.finishedAt = finished
			}
			group.protected = group.protected || protectedExecution[id]
			continue
		}
		if safe && !protectedExecution[id] {
			candidates = append(candidates, &retentionGroup{ids: []string{id}, finishedAt: finished, safe: true})
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return PruneResult{}, err
	}
	rows.Close()
	for _, group := range groups {
		if group.safe && !group.protected && len(group.ids) > 0 {
			sort.Strings(group.ids)
			candidates = append(candidates, group)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].finishedAt.Equal(candidates[j].finishedAt) {
			return candidates[i].ids[0] < candidates[j].ids[0]
		}
		return candidates[i].finishedAt.Before(candidates[j].finishedAt)
	})
	result := PruneResult{Remaining: total}
	cutoff := policy.Now.Add(-policy.MaxAge)
	for _, group := range candidates {
		if group.finishedAt.After(cutoff) && result.Remaining+policy.Reserve <= policy.MaxRecords {
			continue
		}
		if group.workflowID != "" {
			if _, err := tx.ExecContext(ctx, `DELETE FROM workflow_nodes WHERE workflow_id = ?`, group.workflowID); err != nil {
				return PruneResult{}, err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM workflows WHERE id = ?`, group.workflowID); err != nil {
				return PruneResult{}, err
			}
			result.WorkflowIDs = append(result.WorkflowIDs, group.workflowID)
		}
		for _, id := range group.ids {
			if _, err := tx.ExecContext(ctx, `DELETE FROM executions WHERE id = ?`, id); err != nil {
				return PruneResult{}, err
			}
			result.ExecutionIDs = append(result.ExecutionIDs, id)
		}
		result.Remaining -= len(group.ids)
	}
	if err := tx.Commit(); err != nil {
		return PruneResult{}, err
	}
	if result.Remaining+policy.Reserve > policy.MaxRecords {
		return result, ErrRetentionCapacity
	}
	return result, nil
}

func retainedIDs(ctx context.Context, tx *sql.Tx, query string) (map[string]bool, error) {
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make(map[string]bool)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids[id] = true
	}
	return ids, rows.Err()
}

func terminalRetentionState(state string) bool {
	switch state {
	case "succeeded", "failed", "timed_out", "cancelled", "skipped", "interrupted":
		return true
	default:
		return false
	}
}

func retentionEligibility(state string, raw []byte) (time.Time, bool, error) {
	if !terminalRetentionState(state) {
		return time.Time{}, false, nil
	}
	var snapshot retentionSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return time.Time{}, false, err
	}
	if snapshot.Snapshot.FinishedAt == nil || snapshot.Snapshot.CleanupStatus != "not_needed" && snapshot.Snapshot.CleanupStatus != "succeeded" {
		return time.Time{}, false, nil
	}
	if len(snapshot.Resource) > 0 && string(snapshot.Resource) != "null" && snapshot.Snapshot.CleanupStatus != "succeeded" {
		return time.Time{}, false, nil
	}
	return *snapshot.Snapshot.FinishedAt, true, nil
}
