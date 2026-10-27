package execution

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/haonox/toolane/internal/storage/sqlite"
)

// Snapshot intentionally omits policy limits from its public JSON shape, so
// durableImage stores them separately alongside the accepted arguments.
type durableImage struct {
	Snapshot           Snapshot        `json:"snapshot"`
	Arguments          json.RawMessage `json:"arguments,omitempty"`
	QueueTimeoutNS     int64           `json:"queue_timeout_ns"`
	ExecutionTimeoutNS int64           `json:"execution_timeout_ns"`
	OutputLimitBytes   int             `json:"output_limit_bytes"`
	ArgumentLimitBytes int             `json:"argument_limit_bytes"`
	ResultLimitBytes   int             `json:"result_limit_bytes"`
}

func (m *Manager) storedRecord(r *record, next Snapshot) (sqlite.Record, error) {
	image := durableImage{Snapshot: next, QueueTimeoutNS: int64(next.QueueTimeout),
		ExecutionTimeoutNS: int64(next.ExecutionTimeout), OutputLimitBytes: next.OutputLimitBytes,
		ArgumentLimitBytes: next.ArgumentLimitBytes, ResultLimitBytes: next.ResultLimitBytes}
	if next.FinishedAt == nil {
		image.Arguments = r.arguments
	}
	payload, err := json.Marshal(image)
	if err != nil {
		return sqlite.Record{}, fmt.Errorf("encode execution %s: %w", next.ID, err)
	}
	return sqlite.Record{Sequence: r.storeSequence, Version: r.storeVersion, ID: next.ID,
		TenantID: next.TenantID, State: string(next.State), QueueDeadline: r.deadline,
		FinishedAt: next.FinishedAt, Payload: payload, IdempotencyKey: r.idempotencyKey,
		RequestHash: r.requestHash}, nil
}

func decodeStored(stored sqlite.Record) (*record, error) {
	var image durableImage
	if err := json.Unmarshal(stored.Payload, &image); err != nil {
		return nil, fmt.Errorf("decode execution %s: %w", stored.ID, err)
	}
	s := image.Snapshot
	if s.ID != stored.ID || s.TenantID != stored.TenantID || string(s.State) != stored.State ||
		(s.FinishedAt == nil) != (stored.FinishedAt == nil) ||
		(s.FinishedAt != nil && !s.FinishedAt.Equal(*stored.FinishedAt)) ||
		image.QueueTimeoutNS <= 0 || image.ExecutionTimeoutNS <= 0 || image.OutputLimitBytes <= 0 ||
		image.ArgumentLimitBytes <= 0 || image.ResultLimitBytes <= 0 || s.Tool == "" {
		return nil, fmt.Errorf("stored execution %s has inconsistent fields", stored.ID)
	}
	s.QueueTimeout = time.Duration(image.QueueTimeoutNS)
	s.ExecutionTimeout = time.Duration(image.ExecutionTimeoutNS)
	s.OutputLimitBytes, s.ArgumentLimitBytes, s.ResultLimitBytes =
		image.OutputLimitBytes, image.ArgumentLimitBytes, image.ResultLimitBytes
	return &record{snapshot: s, arguments: append(json.RawMessage(nil), image.Arguments...),
		deadline: stored.QueueDeadline, storeSequence: stored.Sequence, storeVersion: stored.Version,
		idempotencyKey: stored.IdempotencyKey, requestHash: stored.RequestHash}, nil
}

func validIdempotencyKey(key string) bool {
	if len(key) == 0 || len(key) > 128 {
		return false
	}
	for i := range len(key) {
		c := key[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			c == '-' || c == '_' || c == '.' || c == '~' {
			continue
		}
		return false
	}
	return true
}

func hashPrepared(p preparedRequest) (string, error) {
	var arguments json.RawMessage
	if len(p.arguments) > 0 {
		decoder := json.NewDecoder(bytes.NewReader(p.arguments))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil {
			return "", fmt.Errorf("canonicalize execution arguments: %w", err)
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return "", fmt.Errorf("canonicalize execution arguments: %w", err)
		}
		arguments = encoded
	}
	identity := struct {
		AgentID            string          `json:"agent_id"`
		Tool               string          `json:"tool"`
		Arguments          json.RawMessage `json:"arguments"`
		QueueTimeoutNS     int64           `json:"queue_timeout_ns"`
		ExecutionTimeoutNS int64           `json:"execution_timeout_ns"`
		OutputLimitBytes   int             `json:"output_limit_bytes"`
	}{p.request.AgentID, p.request.Tool, arguments, int64(p.queueTimeout), int64(p.runTimeout), p.outputLimit}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return "", fmt.Errorf("hash execution request: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// recoverStored runs before workers or HTTP listeners exist. A queued record
// can resume; an execution that was already running has an unknown outcome and
// must never be replayed automatically.
func (m *Manager) recoverStored() error {
	active, err := m.store.ListActive(context.Background())
	if err != nil {
		return fmt.Errorf("load active executions: %w", err)
	}
	for _, stored := range active {
		r, err := decodeStored(stored)
		if err != nil {
			return err
		}
		if r.snapshot.State == Queued && time.Now().Before(r.deadline) {
			if _, ok := m.tools[r.snapshot.Tool]; ok {
				m.records[r.snapshot.ID] = r
				m.queue = append(m.queue, r)
				m.admitPendingLocked(r)
				continue
			}
		}
		before := r.snapshot
		next := cloneSnapshot(before)
		now := time.Now().UTC()
		next.FinishedAt = &now
		switch {
		case before.State == Queued && !time.Now().Before(r.deadline):
			next.State, next.ErrorCode, next.ErrorMessage = TimedOut, "queue_timeout", "queue wait expired during restart"
		case before.State == Queued:
			next.State, next.ErrorCode, next.ErrorMessage = Failed, "tool_unavailable", "configured tool is unavailable after restart"
		default:
			next.State, next.ErrorCode, next.StopReason = Interrupted, "outcome_unknown", "service_restart"
			next.ErrorMessage = "execution interrupted by restart; external side effects may have occurred"
		}
		if err := m.persistSnapshotLocked(r, next); err != nil {
			return fmt.Errorf("recover execution %s: %w", before.ID, err)
		}
		r.snapshot = next
		if m.transitionObserver != nil {
			m.transitionObserver(before, next)
		}
	}
	return nil
}

func (m *Manager) persistSnapshotLocked(r *record, next Snapshot) error {
	if m.store == nil {
		return nil
	}
	stored, err := m.storedRecord(r, next)
	if err != nil {
		return err
	}
	updated, err := m.store.CompareAndSwap(context.Background(), stored)
	if err != nil {
		return err
	}
	r.storeVersion = updated.Version
	return nil
}

func (m *Manager) haltStorageLocked(err error) {
	m.storageErr = errors.Join(m.storageErr, fmt.Errorf("execution storage: %w", err))
	m.halted = true
	for _, r := range m.records {
		if r.cancel != nil {
			r.cancel()
		}
	}
	m.cond.Broadcast()
}

func (m *Manager) expiredSnapshot(s Snapshot, now time.Time) bool {
	return m.store != nil && s.FinishedAt != nil && !s.FinishedAt.Add(m.config.ResultRetention).After(now)
}

const cleanupBatchSize = 1000

// cleanupExpiredLocked removes durable results in bounded transactions and
// evicts any remaining in-memory copies of the deleted IDs.
func (m *Manager) cleanupExpiredLocked() (int, error) {
	if m.store == nil {
		return 0, nil
	}
	ids, err := m.store.DeleteExpired(context.Background(), time.Now().Add(-m.config.ResultRetention), cleanupBatchSize)
	if err != nil {
		return 0, fmt.Errorf("delete expired results: %w", err)
	}
	if len(ids) > 0 {
		deleted := make(map[string]struct{}, len(ids))
		for _, id := range ids {
			deleted[id] = struct{}{}
			if r := m.records[id]; r != nil && r.snapshot.FinishedAt != nil {
				delete(m.records, id)
			}
		}
		kept := m.terminal[:0]
		for _, id := range m.terminal {
			if _, removed := deleted[id]; !removed {
				kept = append(kept, id)
			}
		}
		m.terminal = kept
	}
	return len(ids), nil
}

func (m *Manager) evictTerminalLocked(id string) {
	if r := m.records[id]; r != nil && r.snapshot.FinishedAt != nil {
		delete(m.records, id)
	}
	kept := m.terminal[:0]
	for _, candidate := range m.terminal {
		if candidate != id {
			kept = append(kept, candidate)
		}
	}
	m.terminal = kept
}

func (m *Manager) expireTerminal() {
	defer m.workers.Done()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-ticker.C:
		}
		for {
			m.mu.Lock()
			if m.closing || m.halted {
				m.mu.Unlock()
				break
			}
			count, err := m.cleanupExpiredLocked()
			if err != nil {
				m.haltStorageLocked(err)
			}
			m.mu.Unlock()
			if err != nil || count < cleanupBatchSize {
				break
			}
			select {
			case <-m.stop:
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
}
