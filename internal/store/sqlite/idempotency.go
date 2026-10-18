package sqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"time"
)

var ErrIdempotencyConflict = errors.New("idempotency key reused with different request")

const idempotencyLifetime = 24 * time.Hour

// ResolveIdempotency checks an unexpired key without creating a task. Admission
// must still use CreateIdempotentExecution so the database constraint decides
// any race between this check and the write.
func (s *Store) ResolveIdempotency(ctx context.Context, tenantID, endpoint, key string, normalizedRequest []byte) (id string, found bool, err error) {
	var oldHash []byte
	var expiresAtNS int64
	err = s.db.QueryRowContext(ctx, `SELECT request_hash, execution_id, expires_at_ns
		FROM idempotency WHERE tenant_id = ? AND endpoint = ? AND key = ?`, tenantID, endpoint, key).
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

// CreateIdempotentExecution atomically creates an execution, its first event,
// and a scoped idempotency record. record.Request must be the validated,
// normalized request encoding; the store hashes those exact bytes. A live key
// returns its original execution ID when the request is identical, or
// ErrIdempotencyConflict when it differs. A key may be reused after 24 hours.
//
// endpoint must identify the submission endpoint, not the individual resource.
// The caller must obtain tenantID from a trusted identity, not a request body.
func (s *Store) CreateIdempotentExecution(ctx context.Context, endpoint, key string, record Execution, event Event) (id string, reused bool, err error) {
	if endpoint == "" || key == "" {
		return "", false, errors.New("idempotency endpoint and key are required")
	}
	if err := validateCreate(record, event); err != nil {
		return "", false, err
	}
	hash := sha256.Sum256(record.Request)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	var oldHash []byte
	var oldID string
	var expiresAtNS int64
	err = tx.QueryRowContext(ctx, `SELECT request_hash, execution_id, expires_at_ns
		FROM idempotency WHERE tenant_id = ? AND endpoint = ? AND key = ?`, record.TenantID, endpoint, key).
		Scan(&oldHash, &oldID, &expiresAtNS)
	switch {
	case err == nil && expiresAtNS > now.UnixNano():
		if !bytes.Equal(oldHash, hash[:]) {
			return "", false, ErrIdempotencyConflict
		}
		return oldID, true, nil
	case err == nil:
		if _, err := tx.ExecContext(ctx, `DELETE FROM idempotency
			WHERE tenant_id = ? AND endpoint = ? AND key = ?`, record.TenantID, endpoint, key); err != nil {
			return "", false, err
		}
	case errors.Is(err, sql.ErrNoRows):
	default:
		return "", false, err
	}
	if err := insertExecution(ctx, tx, record, event); err != nil {
		return "", false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO idempotency
		(tenant_id, endpoint, key, request_hash, execution_id, expires_at_ns)
		VALUES (?, ?, ?, ?, ?, ?)`, record.TenantID, endpoint, key, hash[:], record.ID, now.Add(idempotencyLifetime).UnixNano()); err != nil {
		return "", false, err
	}
	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	return record.ID, false, nil
}
