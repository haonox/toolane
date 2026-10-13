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
		r, ctx, ok := m.claim()
		if !ok {
			return
		}
		options := tool.RunOptions{
			MaxOutputBytes:   r.snapshot.OutputLimitBytes,
			OnOutputLimit:    func() { m.outputLimitReached(r) },
			OnProcessStarted: func(at time.Time) { m.processStarted(r, at) },
		}
		outcome, err := runTool(ctx, m.tools[r.snapshot.Tool].Execute, r.arguments, options)
		if !m.complete(r, ctx, outcome, err) {
			return
		}
	}
}

func (m *Manager) claim() (*record, context.Context, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for {
		if m.closing {
			return nil, nil, false
		}
		if m.halted {
			m.cond.Wait()
			continue
		}
		if len(m.queue) == 0 {
			m.cond.Wait()
			continue
		}
		r := m.queue[0]
		m.queue[0] = nil
		m.queue = m.queue[1:]
		m.wakeExpiry()
		if !time.Now().Before(r.deadline) {
			m.finishLocked(r, TimedOut, "queue_timeout", "queue wait expired")
			continue
		}
		now := time.Now().UTC()
		r.snapshot.State = Running
		r.snapshot.RunningAt = &now
		ctx, cancel := context.WithTimeout(context.Background(), r.snapshot.ExecutionTimeout)
		r.cancel = cancel
		m.running++
		// Another idle worker can claim the next accepted task.
		if len(m.queue) > 0 {
			m.cond.Signal()
		}
		context.AfterFunc(ctx, func() {
			if ctx.Err() == context.DeadlineExceeded {
				m.timeoutRunning(r)
			}
		})
		return r, ctx, true
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
		r.snapshot.State = Cancelling
		r.snapshot.StopReason = "execution_timeout"
	}
}

func (m *Manager) outputLimitReached(r *record) {
	m.mu.Lock()
	var cancel context.CancelFunc
	if r.snapshot.State == Running {
		r.snapshot.State = Cancelling
		r.snapshot.StopReason = "output_limit_exceeded"
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
		r.snapshot.ProcessStartedAt = &at
	}
}

func (m *Manager) complete(r *record, ctx context.Context, outcome tool.Outcome, err error) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer r.cancel()
	if outcome.ProcessStartedAt != nil {
		r.snapshot.ProcessStartedAt = outcome.ProcessStartedAt
	}
	r.snapshot.ExitCode = outcome.ExitCode
	r.snapshot.OutputTruncated = outcome.OutputTruncated
	stdout, stderr := []byte(outcome.Stdout), []byte(outcome.Stderr)
	limit := r.snapshot.OutputLimitBytes
	if len(stdout) > limit {
		stdout = stdout[:limit]
		r.snapshot.OutputTruncated = true
	}
	if len(stderr) > limit-len(stdout) {
		stderr = stderr[:limit-len(stdout)]
		r.snapshot.OutputTruncated = true
	}
	r.snapshot.Stdout = strings.ToValidUTF8(string(stdout), "?")
	r.snapshot.Stderr = strings.ToValidUTF8(string(stderr), "?")
	if outcome.StopUnconfirmed {
		if r.snapshot.State == Running {
			r.snapshot.State = Cancelling
			r.snapshot.StopReason = "stop_unconfirmed"
		}
		r.snapshot.CleanupStatus = "failed"
		if err != nil {
			r.snapshot.ErrorMessage = err.Error()
		}
		m.halted = true
		m.cond.Broadcast()
		return false
	}
	if r.snapshot.State == Running && ctx.Err() == context.DeadlineExceeded {
		r.snapshot.State = Cancelling
		r.snapshot.StopReason = "execution_timeout"
	}
	if r.snapshot.State == Running && r.snapshot.OutputTruncated {
		r.snapshot.State = Cancelling
		r.snapshot.StopReason = "output_limit_exceeded"
	}
	if r.snapshot.State == Cancelling {
		switch r.snapshot.StopReason {
		case "execution_timeout":
			m.finishLocked(r, TimedOut, "execution_timeout", "execution deadline expired")
		case "output_limit_exceeded":
			m.finishLocked(r, Failed, "output_limit_exceeded", "combined stdout and stderr exceeded limit")
		default:
			m.finishLocked(r, Cancelled, "cancelled", "execution stopped")
		}
	} else if err != nil {
		code := "execution_failed"
		var runErr *tool.RunError
		if errors.As(err, &runErr) && runErr != nil && runErr.Code != "" {
			code = runErr.Code
		}
		m.finishLocked(r, Failed, code, err.Error())
	} else if len(outcome.Result) > r.snapshot.ResultLimitBytes {
		m.finishLocked(r, Failed, "result_limit_exceeded", "structured result exceeds limit")
	} else if len(outcome.Result) > 0 && !json.Valid(outcome.Result) {
		m.finishLocked(r, Failed, "invalid_result", "executor returned invalid JSON")
	} else {
		r.snapshot.Result = append(json.RawMessage(nil), outcome.Result...)
		m.finishLocked(r, Succeeded, "", "")
	}
	m.running--
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
		now := time.Now()
		var next time.Time
		for i := 0; i < len(m.queue); {
			r := m.queue[i]
			if !now.Before(r.deadline) {
				copy(m.queue[i:], m.queue[i+1:])
				m.queue[len(m.queue)-1] = nil
				m.queue = m.queue[:len(m.queue)-1]
				m.finishLocked(r, TimedOut, "queue_timeout", "queue wait expired")
				continue
			}
			if next.IsZero() || r.deadline.Before(next) {
				next = r.deadline
			}
			i++
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
