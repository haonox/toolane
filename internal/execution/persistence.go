package execution

import (
	"context"
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
		FinishedAt: next.FinishedAt, Payload: payload}, nil
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
		deadline: stored.QueueDeadline, storeSequence: stored.Sequence, storeVersion: stored.Version}, nil
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
