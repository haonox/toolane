//go:build linux

package process_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/execution"
	"github.com/haonox/toolane/internal/executor/process"
	"github.com/haonox/toolane/internal/tool"
)

func TestHelperProcess(t *testing.T) {
	if os.Getenv("TOOLANE_TEST_HELPER") != "1" {
		return
	}
	switch os.Getenv("TOOLANE_TEST_MODE") {
	case "streams":
		fmt.Fprint(os.Stdout, "stdout")
		fmt.Fprint(os.Stderr, "stderr")
	case "nonzero":
		os.Exit(7)
	case "flood":
		chunk := strings.Repeat("x", 512)
		for range 10000 {
			fmt.Fprint(os.Stdout, chunk)
			fmt.Fprint(os.Stderr, chunk)
		}
	case "sleep", "child_wait":
		if path := os.Getenv("TOOLANE_READY_FILE"); path != "" {
			_ = os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0600)
		}
		time.Sleep(30 * time.Second)
	case "child", "exit_early_child", "closed_pipes_child":
		child := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
		child.Env = []string{"TOOLANE_TEST_HELPER=1", "TOOLANE_TEST_MODE=child_wait"}
		if os.Getenv("TOOLANE_TEST_MODE") != "closed_pipes_child" {
			child.Stdout = os.Stdout
			child.Stderr = os.Stderr
		}
		if err := child.Start(); err != nil {
			fmt.Fprint(os.Stderr, err)
			os.Exit(2)
		}
		fmt.Fprintf(os.Stdout, "child_pid=%d\n", child.Process.Pid)
		if os.Getenv("TOOLANE_TEST_MODE") == "exit_early_child" || os.Getenv("TOOLANE_TEST_MODE") == "closed_pipes_child" {
			os.Exit(0)
		}
		time.Sleep(30 * time.Second)
	default:
		os.Exit(3)
	}
	os.Exit(0)
}

func TestProcessRegistrationRequiresExplicitTrustAndAbsolutePath(t *testing.T) {
	args := func(json.RawMessage) ([]string, error) { return nil, nil }
	for _, spec := range []process.Spec{
		{Executable: "/bin/true", Args: args},
		{Trusted: true, Executable: "relative-program", Args: args},
		{Trusted: true, Executable: "/bin/true"},
	} {
		if _, err := process.TrustedDefinition("test", spec); err == nil {
			t.Fatalf("unsafe process registration accepted: %+v", spec)
		}
	}
}

func processManager(t *testing.T, mode, readyFile string, config execution.Config) *execution.Manager {
	t.Helper()
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	def, err := process.TrustedDefinition("test.process", process.Spec{
		Trusted:        true,
		Executable:     path,
		Args:           func(json.RawMessage) ([]string, error) { return []string{"-test.run=^TestHelperProcess$"}, nil },
		Environment:    []string{"TOOLANE_TEST_HELPER=1", "TOOLANE_TEST_MODE=" + mode, "TOOLANE_READY_FILE=" + readyFile},
		GracePeriod:    100 * time.Millisecond,
		CleanupTimeout: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := tool.NewRegistry()
	if err := registry.Register(def); err != nil {
		t.Fatal(err)
	}
	m, err := execution.NewManager(config, registry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := m.Close(ctx); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return m
}

func submitProcess(t *testing.T, m *execution.Manager, timeout time.Duration) execution.Snapshot {
	t.Helper()
	s, err := m.Submit(context.Background(), execution.Request{Tool: "test.process", ExecutionTimeout: timeout})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func awaitProcess(t *testing.T, m *execution.Manager, id string) execution.Snapshot {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		s, err := m.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if s.FinishedAt != nil {
			return s
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("execution %s stuck in %s", id, s.State)
		}
	}
}

func awaitFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("process did not signal readiness")
		}
	}
}

func TestProcessStreamsAndExitCode(t *testing.T) {
	m := processManager(t, "streams", "", execution.DefaultConfig())
	s := awaitProcess(t, m, submitProcess(t, m, 0).ID)
	if s.State != execution.Succeeded || s.Stdout != "stdout" || s.Stderr != "stderr" || s.ExitCode == nil || *s.ExitCode != 0 || s.ProcessStartedAt == nil {
		t.Fatalf("process result: %+v", s)
	}
	if s.OutputTruncated || m.Stats().Running != 0 {
		t.Fatalf("process resources not released: %+v, %+v", s, m.Stats())
	}
	failed := processManager(t, "nonzero", "", execution.DefaultConfig())
	s = awaitProcess(t, failed, submitProcess(t, failed, 0).ID)
	if s.State != execution.Failed || s.ErrorCode != "process_exit_nonzero" || s.ExitCode == nil || *s.ExitCode != 7 {
		t.Fatalf("nonzero result: %+v", s)
	}
}

func TestSharedOutputLimitStopsProcess(t *testing.T) {
	config := execution.DefaultConfig()
	config.MaxOutputBytes = 64
	m := processManager(t, "flood", "", config)
	s := awaitProcess(t, m, submitProcess(t, m, 0).ID)
	if s.State != execution.Failed || s.ErrorCode != "output_limit_exceeded" || s.StopReason != "output_limit_exceeded" || !s.OutputTruncated {
		t.Fatalf("output limit result: %+v", s)
	}
	if len(s.Stdout)+len(s.Stderr) != 64 || m.Stats().Running != 0 {
		t.Fatalf("output budget or running slot: stdout=%d stderr=%d stats=%+v", len(s.Stdout), len(s.Stderr), m.Stats())
	}
}

func TestTimeoutStopsOrdinaryChild(t *testing.T) {
	m := processManager(t, "child", "", execution.DefaultConfig())
	s := awaitProcess(t, m, submitProcess(t, m, 500*time.Millisecond).ID)
	if s.State != execution.TimedOut || s.ErrorCode != "execution_timeout" || m.Stats().Running != 0 {
		t.Fatalf("process timeout: %+v, %+v", s, m.Stats())
	}
	var pid int
	if _, err := fmt.Sscanf(s.Stdout, "child_pid=%d", &pid); err != nil {
		t.Fatalf("child PID missing from output %q: %v", s.Stdout, err)
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for processStillAlive(pid) {
		select {
		case <-time.After(10 * time.Millisecond):
		case <-deadline.C:
			t.Fatalf("child process %d survived group termination", pid)
		}
	}
}

func TestInheritedPipeDoesNotHoldWorkerForever(t *testing.T) {
	m := processManager(t, "exit_early_child", "", execution.DefaultConfig())
	s := awaitProcess(t, m, submitProcess(t, m, 0).ID)
	if s.State != execution.Failed || s.ErrorCode != "process_wait_failed" || m.Stats().Running != 0 {
		t.Fatalf("inherited pipe result: %+v, %+v", s, m.Stats())
	}
	var pid int
	if _, err := fmt.Sscanf(s.Stdout, "child_pid=%d", &pid); err != nil {
		t.Fatalf("child PID missing from output %q: %v", s.Stdout, err)
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for processStillAlive(pid) {
		select {
		case <-time.After(10 * time.Millisecond):
		case <-deadline.C:
			t.Fatalf("child process %d survived inherited pipe cleanup", pid)
		}
	}
}

func TestParentExitCleansChildWithClosedPipes(t *testing.T) {
	m := processManager(t, "closed_pipes_child", "", execution.DefaultConfig())
	s := awaitProcess(t, m, submitProcess(t, m, 0).ID)
	if s.State != execution.Succeeded || m.Stats().Running != 0 {
		t.Fatalf("parent exit result: %+v, %+v", s, m.Stats())
	}
	var pid int
	if _, err := fmt.Sscanf(s.Stdout, "child_pid=%d", &pid); err != nil {
		t.Fatalf("child PID missing from output %q: %v", s.Stdout, err)
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for processStillAlive(pid) {
		select {
		case <-time.After(10 * time.Millisecond):
		case <-deadline.C:
			t.Fatalf("child process %d survived parent exit", pid)
		}
	}
}

func processStillAlive(pid int) bool {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	if err != nil {
		return true
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		if strings.HasPrefix(line, "State:") {
			return !strings.Contains(line, "Z (zombie)") && !strings.Contains(line, "X (dead)")
		}
	}
	return true
}

func TestCancelAndCloseRunningProcess(t *testing.T) {
	ready := t.TempDir() + "/ready"
	m := processManager(t, "sleep", ready, execution.DefaultConfig())
	s := submitProcess(t, m, 0)
	awaitFile(t, ready)
	if active, err := m.Get(s.ID); err != nil || active.ProcessStartedAt == nil {
		t.Fatalf("running process lacks start time: %+v, %v", active, err)
	}
	stopping, err := m.Cancel(s.ID)
	if err != nil || stopping.State != execution.Cancelling {
		t.Fatalf("cancel request: %+v, %v", stopping, err)
	}
	final := awaitProcess(t, m, s.ID)
	if final.State != execution.Cancelled || m.Stats().Running != 0 {
		t.Fatalf("cancelled process: %+v, %+v", final, m.Stats())
	}
	ready2 := t.TempDir() + "/ready"
	closing := processManager(t, "sleep", ready2, execution.DefaultConfig())
	second := submitProcess(t, closing, 0)
	awaitFile(t, ready2)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := closing.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if final, err := closing.Get(second.ID); err != nil || final.State != execution.Cancelled || closing.Stats().Running != 0 {
		t.Fatalf("closed process: %+v, %v", final, err)
	}
}

func TestStartFailureDoesNotConsumeSlot(t *testing.T) {
	def, err := process.TrustedDefinition("missing", process.Spec{
		Trusted: true, Executable: "/no/such/toolane-binary",
		Args: func(json.RawMessage) ([]string, error) { return nil, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := tool.NewRegistry()
	if err := registry.Register(def); err != nil {
		t.Fatal(err)
	}
	m, err := execution.NewManager(execution.DefaultConfig(), registry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(context.Background()); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	for range 20 {
		s, err := m.Submit(context.Background(), execution.Request{Tool: "missing"})
		if err != nil {
			t.Fatal(err)
		}
		final := awaitProcess(t, m, s.ID)
		if final.State != execution.Failed || final.ErrorCode != "process_start_failed" || m.Stats().Running != 0 {
			t.Fatalf("start failure: %+v, stats=%+v", final, m.Stats())
		}
	}
}
