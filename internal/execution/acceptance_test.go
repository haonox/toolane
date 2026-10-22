package execution

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/tool"
)

func startedExecution(t *testing.T, started <-chan int, want int) {
	t.Helper()
	select {
	case got := <-started:
		if got != want {
			t.Fatalf("started execution %d, want %d", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("execution %d did not start", want)
	}
}

// ACC-01: a claimed execution occupies one worker; ready work stays FIFO.
func TestV01WorkerLimitAndFIFO(t *testing.T) {
	config := DefaultConfig()
	config.Workers = 2
	config.QueueCapacity = 2
	var release [4]chan struct{}
	var calls [4]atomic.Int32
	for i := range release {
		release[i] = make(chan struct{})
	}
	started := make(chan int, len(release))
	var active, peak atomic.Int32
	m := testManager(t, config, func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
		var number int
		if err := json.Unmarshal(raw, &number); err != nil || number < 1 || number > len(release) {
			return nil, errors.New("invalid test execution number")
		}
		calls[number-1].Add(1)
		current := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); current > previous; previous = peak.Load() {
			if peak.CompareAndSwap(previous, current) {
				break
			}
		}
		started <- number
		select {
		case <-release[number-1]:
			return json.RawMessage(`{"ok":true}`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	var tasks [4]Snapshot
	for i := range tasks[:2] {
		tasks[i] = submit(t, m, Request{Arguments: json.RawMessage(strconv.Itoa(i + 1))})
		startedExecution(t, started, i+1)
	}
	for i := 2; i < len(tasks); i++ {
		tasks[i] = submit(t, m, Request{Arguments: json.RawMessage(strconv.Itoa(i + 1))})
	}
	if stats := m.Stats(); stats.Running != 2 || stats.Queued != 2 {
		t.Fatalf("worker or queue limit violated: %+v", stats)
	}
	close(release[0])
	startedExecution(t, started, 3)
	close(release[1])
	startedExecution(t, started, 4)
	close(release[2])
	close(release[3])
	for _, task := range tasks {
		awaitState(t, m, task.ID, Succeeded)
	}
	if peak.Load() != 2 || active.Load() != 0 {
		t.Fatalf("active executors: peak=%d remaining=%d", peak.Load(), active.Load())
	}
	for i := range calls {
		if calls[i].Load() != 1 {
			t.Fatalf("execution %d called %d times", i+1, calls[i].Load())
		}
	}
	if stats := m.Stats(); stats.Running != 0 || stats.Queued != 0 {
		t.Fatalf("slots not released: %+v", stats)
	}
	t.Logf("ACC-01: peak executors=%d, completed=%d, running=0, queued=0", peak.Load(), len(tasks))
}

// ACC-02: full-queue rejection does not retain records or start goroutines.
func TestV01QueueFullRejectionRemainsBounded(t *testing.T) {
	config := DefaultConfig()
	config.Workers = 1
	config.QueueCapacity = 1
	started := make(chan struct{})
	release := make(chan struct{})
	m := testManager(t, config, func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		close(started)
		select {
		case <-release:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	first := submit(t, m, Request{})
	<-started
	second := submit(t, m, Request{})
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	beforeGoroutines := runtime.NumGoroutine()
	for range 2000 {
		if _, err := m.Submit(context.Background(), Request{Tool: "test"}); !errors.Is(err, ErrQueueFull) {
			t.Fatalf("full queue returned %v", err)
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	if stats := m.Stats(); stats.Running != 1 || stats.Queued != 1 || stats.Stored != 2 {
		t.Fatalf("full-queue rejection retained work: %+v", stats)
	}
	afterGoroutines := runtime.NumGoroutine()
	if afterGoroutines > beforeGoroutines+8 {
		t.Fatalf("goroutines grew from %d to %d", beforeGoroutines, afterGoroutines)
	}
	if after.HeapAlloc > before.HeapAlloc+4*1024*1024 {
		t.Fatalf("heap grew from %d to %d bytes", before.HeapAlloc, after.HeapAlloc)
	}
	if s, err := m.Cancel(second.ID); err != nil || s.State != Cancelled {
		t.Fatalf("cancel queued execution: %+v, %v", s, err)
	}
	close(release)
	awaitState(t, m, first.ID, Succeeded)
	if stats := m.Stats(); stats.Running != 0 || stats.Queued != 0 {
		t.Fatalf("slots not released: %+v", stats)
	}
	t.Logf("ACC-02: rejected=2000, heap before=%d after=%d bytes, goroutines before=%d after=%d", before.HeapAlloc, after.HeapAlloc, beforeGoroutines, afterGoroutines)
}

// ACC-02: oversized input is rejected before copying it into a record.
func TestV01OversizedAdmissionDoesNotCopy(t *testing.T) {
	config := DefaultConfig()
	config.MaxAgentIDBytes = 8
	config.MaxArgumentBytes = 8
	m := testManager(t, config, func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return nil, nil
	})
	largeArguments := Request{Tool: "test", Arguments: make(json.RawMessage, 1024*1024)}
	if allocations := testing.AllocsPerRun(20, func() {
		if _, err := m.Submit(context.Background(), largeArguments); !errors.Is(err, ErrArgumentsTooLarge) {
			t.Fatalf("oversized arguments returned %v", err)
		}
	}); allocations != 0 {
		t.Fatalf("oversized argument rejection allocated %.1f times per call", allocations)
	}
	if _, err := m.Submit(context.Background(), Request{Tool: "test", AgentID: strings.Repeat("a", 9)}); !errors.Is(err, ErrAgentIDTooLarge) {
		t.Fatalf("oversized agent ID returned %v", err)
	}
	if stats := m.Stats(); stats.Stored != 0 {
		t.Fatalf("oversized requests created records: %+v", stats)
	}
}

// ACC-04: a committed timeout cannot be replaced by a late success or cancel.
func TestV01TimeoutWinsLateCompletionAndCancel(t *testing.T) {
	expired := make(chan struct{})
	release := make(chan struct{})
	m := testManager(t, DefaultConfig(), func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		<-ctx.Done()
		close(expired)
		<-release
		return json.RawMessage(`{"late":true}`), nil
	})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	s := submit(t, m, Request{ExecutionTimeout: 20 * time.Millisecond})
	select {
	case <-expired:
	case <-time.After(2 * time.Second):
		t.Fatal("execution deadline did not fire")
	}
	stopping := awaitState(t, m, s.ID, Cancelling)
	if stopping.StopReason != "execution_timeout" || m.Stats().Running != 1 {
		t.Fatalf("timeout did not retain reservation: %+v, %+v", stopping, m.Stats())
	}
	if again, err := m.Cancel(s.ID); err != nil || again.StopReason != "execution_timeout" {
		t.Fatalf("cancel overwrote timeout: %+v, %v", again, err)
	}
	close(release)
	final := awaitState(t, m, s.ID, TimedOut)
	if final.ErrorCode != "execution_timeout" || final.Result != nil || m.Stats().Running != 0 {
		t.Fatalf("late completion replaced timeout: %+v, %+v", final, m.Stats())
	}
}

// ACC-04: a committed success ignores a late deadline callback.
func TestV01CompletionWinsLateTimeout(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	m := testManager(t, DefaultConfig(), func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		close(started)
		select {
		case <-release:
			return json.RawMessage(`{"ok":true}`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	s := submit(t, m, Request{ExecutionTimeout: time.Second})
	<-started
	close(release)
	awaitState(t, m, s.ID, Succeeded)
	m.mu.Lock()
	r := m.records[s.ID]
	m.mu.Unlock()
	m.timeoutRunning(r)
	if final, err := m.Get(s.ID); err != nil || final.State != Succeeded || string(final.Result) != `{"ok":true}` || m.Stats().Running != 0 {
		t.Fatalf("late timeout changed success: %+v, %v", final, err)
	}
}

func TestV01ErrorMessageIsBounded(t *testing.T) {
	config := DefaultConfig()
	config.MaxErrorMessageBytes = 32
	m := testManager(t, config, func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New(strings.Repeat("x", 1024*1024))
	})
	s := submit(t, m, Request{})
	final := awaitState(t, m, s.ID, Failed)
	if len(final.ErrorMessage) != 32 || len(final.Result) != 0 {
		t.Fatalf("error message was retained without a bound: %+v", final)
	}
}

// ACC-06: the manager also bounds output from an executor that returns it at once.
func TestV01ReturnedOutputIsBounded(t *testing.T) {
	config := DefaultConfig()
	config.MaxOutputBytes = 32
	registry := tool.NewRegistry()
	if err := registry.Register(tool.Definition{
		Name:     "test",
		Validate: func(json.RawMessage) error { return nil },
		Execute: func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
			return tool.Outcome{Stdout: strings.Repeat("a", 1024*1024), Stderr: strings.Repeat("b", 1024*1024)}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	m := newTestManagerWithRegistry(t, config, registry)
	s := submit(t, m, Request{})
	final := awaitState(t, m, s.ID, Failed)
	if final.ErrorCode != "output_limit_exceeded" || !final.OutputTruncated || len(final.Stdout)+len(final.Stderr) != 32 || m.Stats().Running != 0 {
		t.Fatalf("returned output was not bounded: %+v, %+v", final, m.Stats())
	}
}
