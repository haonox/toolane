package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/haonox/toolane/internal/tool"
)

func (m *Manager) worker() {
	defer m.workers.Done()
	for {
		r, snapshot, ctx, ok := m.claim()
		if !ok {
			return
		}
		options := tool.RunOptions{
			MaxOutputBytes:   snapshot.OutputLimitBytes,
			OnOutputLimit:    func() { m.outputLimitReached(r) },
			OnProcessStarted: func(at time.Time) { m.processStarted(r, at) },
			OnCleanupUpdate:  func(status, message string) { m.cleanupUpdated(r, status, message) },
			SandboxResources: tool.Resources{CPUMilli: snapshot.SandboxCPUMilli, MemoryBytes: snapshot.SandboxMemoryBytes},
			ExecutionID:      snapshot.ID,
		}
		outcome, err := runTool(ctx, m.tools[snapshot.Tool].Execute, r.arguments, options)
		if !m.complete(r, ctx, outcome, err) {
			return
		}
	}
}

func (m *Manager) claim() (*record, Snapshot, context.Context, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for {
		if m.closing {
			return nil, Snapshot{}, nil, false
		}
		if m.halted {
			m.cond.Wait()
			continue
		}
		r := m.takeReadyLocked()
		if r == nil {
			m.cond.Wait()
			continue
		}
		m.wakeExpiry()
		if !time.Now().Before(r.deadline) {
			if err := m.finishLocked(r, TimedOut, "queue_timeout", "queue wait expired"); err != nil {
				m.queue = append([]*record{r}, m.queue...)
				continue
			}
			m.releasePendingLocked(r)
			continue
		}
		now := time.Now().UTC()
		next := cloneSnapshot(r.snapshot)
		next.State = Running
		next.RunningAt = &now
		if err := m.applySnapshotLocked(r, next); err != nil {
			m.queue = append([]*record{r}, m.queue...)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), r.snapshot.ExecutionTimeout)
		r.cancel = cancel
		m.claimSlotLocked(r)
		// Another idle worker can claim the next accepted task.
		if len(m.queue) > 0 {
			m.cond.Signal()
		}
		context.AfterFunc(ctx, func() {
			if ctx.Err() == context.DeadlineExceeded {
				m.timeoutRunning(r)
			}
		})
		return r, cloneSnapshot(r.snapshot), ctx, true
	}
}

func runTool(ctx context.Context, execute func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error), args json.RawMessage, options tool.RunOptions) (outcome tool.Outcome, err error) {
	defer func() {
		if value := recover(); value != nil {
			outcome = tool.Outcome{}
			err = fmt.Errorf("tool panic: %v", value)
		}
	}()
	return execute(ctx, args, options)
}

func (m *Manager) timeoutRunning(r *record) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r.snapshot.State == Running {
		next := cloneSnapshot(r.snapshot)
		next.State = Cancelling
		next.StopReason = "execution_timeout"
		_ = m.applySnapshotLocked(r, next)
	}
}

func (m *Manager) outputLimitReached(r *record) {
	m.mu.Lock()
	var cancel context.CancelFunc
	if r.snapshot.State == Running {
		next := cloneSnapshot(r.snapshot)
		next.State = Cancelling
		next.StopReason = "output_limit_exceeded"
		_ = m.applySnapshotLocked(r, next)
		cancel = r.cancel
	}
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (m *Manager) processStarted(r *record, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r.snapshot.State == Running || r.snapshot.State == Cancelling {
		at = at.UTC()
		next := cloneSnapshot(r.snapshot)
		next.ProcessStartedAt = &at
		_ = m.applySnapshotKindLocked(r, next, "progress")
	}
}

func (m *Manager) cleanupUpdated(r *record, status, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if status != "succeeded" && status != "failed" {
		return
	}
	next := cloneSnapshot(r.snapshot)
	next.CleanupStatus = status
	next.CleanupError = m.limitErrorMessage(message)
	_ = m.applySnapshotKindLocked(r, next, "progress")
}

func (m *Manager) complete(r *record, ctx context.Context, outcome tool.Outcome, err error) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer r.cancel()
	next := cloneSnapshot(r.snapshot)
	if outcome.ProcessStartedAt != nil {
		next.ProcessStartedAt = outcome.ProcessStartedAt
	}
	next.ExitCode = outcome.ExitCode
	next.OutputTruncated = outcome.OutputTruncated
	stdout, stderr := outcome.Stdout, outcome.Stderr
	limit := next.OutputLimitBytes
	if len(stdout) > limit {
		stdout = stdout[:limit]
		next.OutputTruncated = true
	}
	if len(stderr) > limit-len(stdout) {
		stderr = stderr[:limit-len(stdout)]
		next.OutputTruncated = true
	}
	next.Stdout = strings.ToValidUTF8(stdout, "?")
	next.Stderr = strings.ToValidUTF8(stderr, "?")
	cleanupFinalized := outcome.CleanupStatus == "pending" && (next.CleanupStatus == "succeeded" || next.CleanupStatus == "failed")
	if outcome.CleanupStatus != "" && !cleanupFinalized {
		next.CleanupStatus = outcome.CleanupStatus
	}
	if outcome.CleanupError != "" && !cleanupFinalized {
		next.CleanupError = m.limitErrorMessage(outcome.CleanupError)
	}
	if outcome.StopUnconfirmed {
		if next.State == Running {
			next.State = Cancelling
			next.StopReason = "stop_unconfirmed"
		}
		next.CleanupStatus = "failed"
		if err != nil {
			next.ErrorMessage = m.limitErrorMessage(err.Error())
		}
		if applyErr := m.applySnapshotLocked(r, next); applyErr != nil {
			return false
		}
		m.halted = true
		m.cond.Broadcast()
		return false
	}
	if next.State == Running && ctx.Err() == context.DeadlineExceeded {
		next.State = Cancelling
		next.StopReason = "execution_timeout"
	}
	if next.State == Running && next.OutputTruncated {
		next.State = Cancelling
		next.StopReason = "output_limit_exceeded"
	}
	var finishErr error
	if next.State == Cancelling {
		switch next.StopReason {
		case "execution_timeout":
			finishErr = m.finishWithSnapshotLocked(r, next, TimedOut, "execution_timeout", "execution deadline expired")
		case "output_limit_exceeded":
			finishErr = m.finishWithSnapshotLocked(r, next, Failed, "output_limit_exceeded", "combined stdout and stderr exceeded limit")
		default:
			finishErr = m.finishWithSnapshotLocked(r, next, Cancelled, "cancelled", "execution stopped")
		}
	} else if err != nil {
		code := "execution_failed"
		var runErr *tool.RunError
		if errors.As(err, &runErr) && runErr != nil && runErr.Code != "" {
			code = runErr.Code
		}
		finishErr = m.finishWithSnapshotLocked(r, next, Failed, code, err.Error())
	} else if len(outcome.Result) > next.ResultLimitBytes {
		finishErr = m.finishWithSnapshotLocked(r, next, Failed, "result_limit_exceeded", "structured result exceeds limit")
	} else if len(outcome.Result) > 0 && !json.Valid(outcome.Result) {
		finishErr = m.finishWithSnapshotLocked(r, next, Failed, "invalid_result", "executor returned invalid JSON")
	} else {
		next.Result = append(json.RawMessage(nil), outcome.Result...)
		finishErr = m.finishWithSnapshotLocked(r, next, Succeeded, "", "")
	}
	if finishErr != nil {
		return false
	}
	m.releaseRunningLocked(r)
	return true
}

// expireQueued uses one timer for all accepted queue deadlines. Expired work
// never occupies a worker or calls an executor.
func (m *Manager) expireQueued() {
	defer m.workers.Done()
	for {
		m.mu.Lock()
		if m.closing {
			m.mu.Unlock()
			return
		}
		if m.storeError != nil {
			m.mu.Unlock()
			<-m.stop
			return
		}
		now := time.Now()
		var next time.Time
		for _, r := range m.records {
			if r.snapshot.State != Queued && r.snapshot.State != Blocked {
				continue
			}
			if !now.Before(r.deadline) {
				wasQueued := r.snapshot.State == Queued
				if err := m.finishLocked(r, TimedOut, "queue_timeout", "queue wait expired"); err != nil {
					break
				}
				if wasQueued {
					m.removeQueued(r)
				}
				m.releasePendingLocked(r)
				continue
			}
			if next.IsZero() || r.deadline.Before(next) {
				next = r.deadline
			}
		}
		m.mu.Unlock()
		if next.IsZero() {
			select {
			case <-m.expiry:
			case <-m.stop:
				return
			}
			continue
		}
		timer := time.NewTimer(time.Until(next))
		select {
		case <-timer.C:
		case <-m.expiry:
		case <-m.stop:
			timer.Stop()
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
}
