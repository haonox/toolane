// Package observability records bounded, process-local service metrics.
package observability

import (
	"fmt"
	"io"
	"sync"
	"time"
)

var (
	admissionKinds   = [...]string{"execution", "workflow"}
	admissionResults = [...]string{"accepted", "invalid", "unauthorized", "conflict", "capacity", "unavailable", "other"}
	terminalStates   = [...]string{"succeeded", "failed", "timed_out", "cancelled", "skipped", "interrupted"}
	durationBuckets  = [...]float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}
)

type durationHistogram struct {
	buckets [len(durationBuckets)]uint64
	count   uint64
	sum     float64
}

func (h *durationHistogram) observe(duration time.Duration) {
	if duration < 0 {
		return
	}
	seconds := duration.Seconds()
	h.count++
	h.sum += seconds
	for i, bound := range durationBuckets {
		if seconds <= bound {
			h.buckets[i]++
		}
	}
}

// Change describes one committed execution snapshot transition. Its values
// must be supplied only after the durable state write has succeeded.
type Change struct {
	BeforeState, AfterState     string
	BeforeCleanup, AfterCleanup string
	CreatedAt                   time.Time
	RunningAt                   *time.Time
	ProcessStartedAt            *time.Time
	FinishedAt                  *time.Time
}

// Gauges are live values gathered immediately before rendering.
type Gauges struct {
	Queued, Blocked, Pending, Running, Stored, Residual int
	Goroutines                                          int
	ReservedCPUMilli                                    int
	ReservedMemoryBytes                                 int64
	Halted, ResidualBlocked                             bool
}

// Recorder stores a fixed number of counters and histogram buckets. It never
// retains tenant IDs, execution IDs, request arguments, or response content.
type Recorder struct {
	mu            sync.Mutex
	admissions    [len(admissionKinds)][len(admissionResults)]uint64
	completed     [len(terminalStates)]uint64
	queue         durationHistogram
	execution     durationHistogram
	cleanupFailed uint64
}

func index(values []string, value string) int {
	for i, candidate := range values {
		if candidate == value {
			return i
		}
	}
	return -1
}

// RecordAdmission classifies one HTTP submission response using finite labels.
func (r *Recorder) RecordAdmission(kind string, status int) {
	kindIndex := index(admissionKinds[:], kind)
	if kindIndex < 0 {
		return
	}
	result := "other"
	switch {
	case status >= 200 && status < 300:
		result = "accepted"
	case status == 400 || status == 413:
		result = "invalid"
	case status == 401 || status == 403:
		result = "unauthorized"
	case status == 409:
		result = "conflict"
	case status == 429:
		result = "capacity"
	case status == 503:
		result = "unavailable"
	}
	r.mu.Lock()
	r.admissions[kindIndex][index(admissionResults[:], result)]++
	r.mu.Unlock()
}

// RecordChange observes a committed transition without retaining its identity.
func (r *Recorder) RecordChange(change Change) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if change.BeforeState != "running" && change.AfterState == "running" && change.RunningAt != nil {
		r.queue.observe(change.RunningAt.Sub(change.CreatedAt))
	}
	if change.BeforeState != change.AfterState && change.FinishedAt != nil {
		if i := index(terminalStates[:], change.AfterState); i >= 0 {
			r.completed[i]++
			started := change.ProcessStartedAt
			if started == nil {
				started = change.RunningAt
			}
			if started != nil {
				r.execution.observe(change.FinishedAt.Sub(*started))
			}
		}
	}
	if change.BeforeCleanup != "failed" && change.AfterCleanup == "failed" {
		r.cleanupFailed++
	}
}

// WritePrometheus emits only fixed metric names and labels. Callers gather
// Gauges first so they never acquire Manager locks while holding Recorder.mu.
func (r *Recorder) WritePrometheus(w io.Writer, gauges Gauges) error {
	r.mu.Lock()
	admissions, completed := r.admissions, r.completed
	queue, execution := r.queue, r.execution
	cleanupFailed := r.cleanupFailed
	r.mu.Unlock()
	write := func(format string, args ...any) error {
		_, err := fmt.Fprintf(w, format, args...)
		return err
	}
	if err := write("# TYPE toolane_admissions_total counter\n"); err != nil {
		return err
	}
	for i, kind := range admissionKinds {
		for j, result := range admissionResults {
			if err := write("toolane_admissions_total{kind=%q,result=%q} %d\n", kind, result, admissions[i][j]); err != nil {
				return err
			}
		}
	}
	if err := write("# TYPE toolane_executions_completed_total counter\n"); err != nil {
		return err
	}
	for i, state := range terminalStates {
		if err := write("toolane_executions_completed_total{status=%q} %d\n", state, completed[i]); err != nil {
			return err
		}
	}
	for _, item := range []struct {
		name string
		data durationHistogram
	}{{"toolane_queue_duration_seconds", queue}, {"toolane_execution_duration_seconds", execution}} {
		if err := write("# TYPE %s histogram\n", item.name); err != nil {
			return err
		}
		for i, bound := range durationBuckets {
			if err := write("%s_bucket{le=%q} %d\n", item.name, fmt.Sprint(bound), item.data.buckets[i]); err != nil {
				return err
			}
		}
		if err := write("%s_bucket{le=\"+Inf\"} %d\n%s_sum %g\n%s_count %d\n", item.name, item.data.count, item.name, item.data.sum, item.name, item.data.count); err != nil {
			return err
		}
	}
	if err := write("# TYPE toolane_cleanup_failures_total counter\ntoolane_cleanup_failures_total %d\n", cleanupFailed); err != nil {
		return err
	}
	for _, item := range []struct {
		name  string
		value any
	}{{"queued", gauges.Queued}, {"blocked", gauges.Blocked}, {"pending", gauges.Pending}, {"running", gauges.Running}, {"stored", gauges.Stored},
		{"residual_containers", gauges.Residual}, {"goroutines", gauges.Goroutines}, {"reserved_cpu_millicores", gauges.ReservedCPUMilli},
		{"reserved_memory_bytes", gauges.ReservedMemoryBytes}, {"halted", boolInt(gauges.Halted)},
		{"residual_blocked", boolInt(gauges.ResidualBlocked)}} {
		if err := write("# TYPE toolane_%s gauge\ntoolane_%s %v\n", item.name, item.name, item.value); err != nil {
			return err
		}
	}
	return nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
