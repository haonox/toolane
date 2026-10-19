package observability

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestRecorderUsesFiniteLabelsAndCommittedTransitionDurations(t *testing.T) {
	recorder := &Recorder{}
	recorder.RecordAdmission("execution", 202)
	recorder.RecordAdmission("execution", 429)
	recorder.RecordAdmission("workflow", 403)
	recorder.RecordAdmission("tenant-selected-label", 202)
	created := time.Unix(100, 0)
	running := created.Add(250 * time.Millisecond)
	started := running.Add(50 * time.Millisecond)
	finished := started.Add(2 * time.Second)
	recorder.RecordChange(Change{BeforeState: "queued", AfterState: "running", CreatedAt: created, RunningAt: &running})
	recorder.RecordChange(Change{BeforeState: "running", AfterState: "succeeded", CreatedAt: created,
		RunningAt: &running, ProcessStartedAt: &started, FinishedAt: &finished,
		BeforeCleanup: "pending", AfterCleanup: "failed"})
	// A later progress update must not count the same failure again.
	recorder.RecordChange(Change{BeforeState: "succeeded", AfterState: "succeeded", CreatedAt: created,
		BeforeCleanup: "failed", AfterCleanup: "failed"})
	var output bytes.Buffer
	if err := recorder.WritePrometheus(&output, Gauges{Queued: 2, Running: 1, Residual: 3, Goroutines: 9}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`toolane_admissions_total{kind="execution",result="accepted"} 1`,
		`toolane_admissions_total{kind="execution",result="capacity"} 1`,
		`toolane_admissions_total{kind="workflow",result="unauthorized"} 1`,
		`toolane_executions_completed_total{status="succeeded"} 1`,
		`toolane_queue_duration_seconds_sum 0.25`,
		`toolane_execution_duration_seconds_sum 2`,
		`toolane_cleanup_failures_total 1`,
		`toolane_queued 2`,
		`toolane_residual_containers 3`,
		`toolane_goroutines 9`,
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("missing metric %q in:\n%s", want, output.String())
		}
	}
	if strings.Contains(output.String(), "tenant-selected-label") {
		t.Fatal("unbounded label appeared in metrics")
	}
}
