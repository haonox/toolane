package sqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"time"
)

// ResolveWorkflowIdempotency checks a live workflow key before capacity
// admission. CreateIdempotentWorkflow still makes the final race decision.
func (s *Store) ResolveWorkflowIdempotency(ctx context.Context, tenantID, endpoint, key string, normalizedRequest []byte) (id string, found bool, err error) {
	var oldHash []byte
	var expiresAtNS int64
	err = s.db.QueryRowContext(ctx, `SELECT request_hash, workflow_id, expires_at_ns
		FROM workflow_idempotency WHERE tenant_id = ? AND endpoint = ? AND key = ?`, tenantID, endpoint, key).
		Scan(&oldHash, &id, &expiresAtNS)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if expiresAtNS <= time.Now().UnixNano() {
		return "", false, nil
	}
	hash := sha256.Sum256(normalizedRequest)
	if !bytes.Equal(oldHash, hash[:]) {
		return "", false, ErrIdempotencyConflict
	}
	return id, true, nil
}

// CreateIdempotentWorkflow atomically creates the complete batch and a scoped
// 24-hour key. normalizedRequest is the manager's validated canonical form;
// record.Request remains the original accepted workflow encoding.
func (s *Store) CreateIdempotentWorkflow(ctx context.Context, endpoint, key string, record Workflow, normalizedRequest []byte) (id string, reused bool, err error) {
	if endpoint == "" || key == "" || len(normalizedRequest) == 0 {
		return "", false, errors.New("workflow idempotency endpoint, key and request are required")
	}
	dependencies, err := validateWorkflowCreate(record)
	if err != nil {
		return "", false, err
	}
	hash := sha256.Sum256(normalizedRequest)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	var oldHash []byte
	var oldID string
	var expiresAtNS int64
	err = tx.QueryRowContext(ctx, `SELECT request_hash, workflow_id, expires_at_ns
		FROM workflow_idempotency WHERE tenant_id = ? AND endpoint = ? AND key = ?`, record.TenantID, endpoint, key).
		Scan(&oldHash, &oldID, &expiresAtNS)
	switch {
	case err == nil && expiresAtNS > now.UnixNano():
		if !bytes.Equal(oldHash, hash[:]) {
			return "", false, ErrIdempotencyConflict
		}
		return oldID, true, nil
	case err == nil:
		if _, err := tx.ExecContext(ctx, `DELETE FROM workflow_idempotency
			WHERE tenant_id = ? AND endpoint = ? AND key = ?`, record.TenantID, endpoint, key); err != nil {
			return "", false, err
		}
	case errors.Is(err, sql.ErrNoRows):
	default:
		return "", false, err
	}
	if err := insertWorkflow(ctx, tx, record, dependencies); err != nil {
		return "", false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO workflow_idempotency
		(tenant_id, endpoint, key, request_hash, workflow_id, expires_at_ns)
		VALUES (?, ?, ?, ?, ?, ?)`, record.TenantID, endpoint, key, hash[:], record.ID, now.Add(idempotencyLifetime).UnixNano()); err != nil {
		return "", false, err
	}
	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	return record.ID, false, nil
}
