package execution

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

func (m *Manager) worker() {
	defer m.workers.Done()
	for {
		r, ctx, ok := m.claim()
		if !ok {
			return
		}
		result, err := runTool(ctx, m.tools[r.snapshot.Tool].Execute, r.arguments)
		m.complete(r, ctx, result, err)
	}
}

func (m *Manager) claim() (*record, context.Context, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for {
		if m.closing {
			return nil, nil, false
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

func runTool(ctx context.Context, execute func(context.Context, json.RawMessage) (json.RawMessage, error), args json.RawMessage) (result json.RawMessage, err error) {
	defer func() {
		if value := recover(); value != nil {
			result = nil
			err = fmt.Errorf("tool panic: %v", value)
		}
	}()
	return execute(ctx, args)
}

func (m *Manager) timeoutRunning(r *record) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r.snapshot.State == Running {
		r.snapshot.State = Cancelling
		r.snapshot.StopReason = "execution_timeout"
	}
}

func (m *Manager) complete(r *record, ctx context.Context, result json.RawMessage, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer r.cancel()
	if r.snapshot.State == Running && ctx.Err() == context.DeadlineExceeded {
		r.snapshot.State = Cancelling
		r.snapshot.StopReason = "execution_timeout"
	}
	if r.snapshot.State == Cancelling {
		switch r.snapshot.StopReason {
		case "execution_timeout":
			m.finishLocked(r, TimedOut, "execution_timeout", "execution deadline expired")
		default:
			m.finishLocked(r, Cancelled, "cancelled", "execution stopped")
		}
	} else if err != nil {
		m.finishLocked(r, Failed, "execution_failed", err.Error())
	} else if len(result) > m.config.MaxResultBytes {
		m.finishLocked(r, Failed, "result_limit_exceeded", "structured result exceeds limit")
	} else if len(result) > 0 && !json.Valid(result) {
		m.finishLocked(r, Failed, "invalid_result", "executor returned invalid JSON")
	} else {
		r.snapshot.Result = append(json.RawMessage(nil), result...)
		m.finishLocked(r, Succeeded, "", "")
	}
	m.running--
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
