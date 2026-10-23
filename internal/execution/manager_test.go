package execution

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/tool"
)

func testManager(t *testing.T, config Config, execute func(context.Context, json.RawMessage) (json.RawMessage, error)) *Manager {
	t.Helper()
	registry := tool.NewRegistry()
	if err := registry.Register(tool.Definition{
		Name:     "test",
		Validate: func(json.RawMessage) error { return nil },
		Execute: func(ctx context.Context, args json.RawMessage, _ tool.RunOptions) (tool.Outcome, error) {
			result, err := execute(ctx, args)
			return tool.Outcome{Result: result}, err
		},
	}); err != nil {
		t.Fatal(err)
	}
	return newTestManagerWithRegistry(t, config, registry)
}

func newTestManagerWithRegistry(t *testing.T, config Config, registry *tool.Registry) *Manager {
	t.Helper()
	m, err := NewManager(config, registry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.Close(ctx); err != nil {
			t.Errorf("close manager: %v", err)
		}
	})
	return m
}

func submit(t *testing.T, m *Manager, req Request) Snapshot {
	t.Helper()
	if req.Tool == "" {
		req.Tool = "test"
	}
	s, err := m.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func awaitState(t *testing.T, m *Manager, id string, want State) Snapshot {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		s, err := m.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if s.State == want {
			return s
		}
		if s.FinishedAt != nil {
			t.Fatalf("execution %s reached %s, wanted %s", id, s.State, want)
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("execution %s still %s, wanted %s", id, s.State, want)
		}
	}
}

func TestQueueCapacityTimeoutAndCancel(t *testing.T) {
	config := DefaultConfig()
	config.Workers = 1
	config.QueueCapacity = 1
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	m := testManager(t, config, func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		if calls.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return json.RawMessage(`{"ok":true}`), nil
	})
	first := submit(t, m, Request{})
	<-started
	second := submit(t, m, Request{QueueTimeout: 20 * time.Millisecond})
	if _, err := m.Submit(context.Background(), Request{Tool: "test"}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full queue error = %v", err)
	}
	timedOut := awaitState(t, m, second.ID, TimedOut)
	if timedOut.ErrorCode != "queue_timeout" || calls.Load() != 1 {
		t.Fatalf("queued timeout: %+v, calls=%d", timedOut, calls.Load())
	}
	third := submit(t, m, Request{})
	cancelled, err := m.Cancel(third.ID)
	if err != nil || cancelled.State != Cancelled {
		t.Fatalf("cancel queued: %+v, %v", cancelled, err)
	}
	if stats := m.Stats(); stats.Queued != 0 || stats.Running != 1 {
		t.Fatalf("capacity after queued cancel: %+v", stats)
	}
	close(release)
	awaitState(t, m, first.ID, Succeeded)
	if calls.Load() != 1 {
		t.Fatalf("queued work unexpectedly executed %d times", calls.Load())
	}
}

func TestRunningCancellationWinsAndReleasesSlot(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	m := testManager(t, DefaultConfig(), func(context.Context, json.RawMessage) (json.RawMessage, error) {
		close(started)
		<-release
		return json.RawMessage(`{"ok":true}`), nil
	})
	s := submit(t, m, Request{})
	<-started
	stopping, err := m.Cancel(s.ID)
	if err != nil || stopping.State != Cancelling || stopping.StopReason != "user_cancelled" {
		t.Fatalf("cancel running: %+v, %v", stopping, err)
	}
	if again, err := m.Cancel(s.ID); err != nil || again.State != Cancelling {
		t.Fatalf("repeat cancel: %+v, %v", again, err)
	}
	close(release)
	final := awaitState(t, m, s.ID, Cancelled)
	if final.Result != nil || final.FinishedAt == nil || m.Stats().Running != 0 {
		t.Fatalf("cancelled execution retained work: %+v, stats=%+v", final, m.Stats())
	}
	if again, err := m.Cancel(s.ID); err != nil || again.State != Cancelled {
		t.Fatalf("cancel terminal: %+v, %v", again, err)
	}
}

func TestCompletionWinsBeforeCancellation(t *testing.T) {
	m := testManager(t, DefaultConfig(), func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	})
	s := submit(t, m, Request{})
	awaitState(t, m, s.ID, Succeeded)
	final, err := m.Cancel(s.ID)
	if err != nil || final.State != Succeeded {
		t.Fatalf("cancel after completion: %+v, %v", final, err)
	}
	final.Result[0] = 'x'
	again, _ := m.Get(s.ID)
	if string(again.Result) != `{"ok":true}` {
		t.Fatalf("returned snapshot changed stored result: %s", again.Result)
	}
	*final.FinishedAt = time.Time{}
	again, _ = m.Get(s.ID)
	if again.FinishedAt.IsZero() {
		t.Fatal("returned snapshot changed stored finish time")
	}
}

func TestExecutionTimeout(t *testing.T) {
	m := testManager(t, DefaultConfig(), func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	s := submit(t, m, Request{ExecutionTimeout: 20 * time.Millisecond})
	final := awaitState(t, m, s.ID, TimedOut)
	if final.ErrorCode != "execution_timeout" || final.StopReason != "execution_timeout" || m.Stats().Running != 0 {
		t.Fatalf("execution timeout: %+v, stats=%+v", final, m.Stats())
	}
}

func TestSubmitContextDoesNotOwnAcceptedWork(t *testing.T) {
	m := testManager(t, DefaultConfig(), func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	s, err := m.Submit(ctx, Request{Tool: "test"})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	awaitState(t, m, s.ID, Succeeded)
}

func TestCloseCancelsAndDrains(t *testing.T) {
	config := DefaultConfig()
	config.Workers = 1
	started := make(chan struct{})
	m := testManager(t, config, func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	first := submit(t, m, Request{})
	<-started
	second := submit(t, m, Request{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{first.ID, second.ID} {
		if s, err := m.Get(id); err != nil || s.State != Cancelled {
			t.Fatalf("closed execution %s: %+v, %v", id, s, err)
		}
	}
	if stats := m.Stats(); stats.Running != 0 || stats.Queued != 0 {
		t.Fatalf("close left active work: %+v", stats)
	}
	if _, err := m.Submit(context.Background(), Request{Tool: "test"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("submit after close = %v", err)
	}
}

func TestAdmissionAndResultLimits(t *testing.T) {
	config := DefaultConfig()
	config.MaxArgumentBytes = 3
	config.MaxResultBytes = 5
	m := testManager(t, config, func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"large":true}`), nil
	})
	if _, err := m.Submit(context.Background(), Request{Tool: "test", Arguments: json.RawMessage(`"long"`)}); !errors.Is(err, ErrArgumentsTooLarge) {
		t.Fatalf("oversized arguments: %v", err)
	}
	if _, err := m.Submit(context.Background(), Request{Tool: "missing"}); err == nil {
		t.Fatal("unknown tool was accepted")
	}
	if _, err := m.Submit(context.Background(), Request{Tool: "test", ExecutionTimeout: config.MaxRunTimeout + time.Second}); err == nil {
		t.Fatal("excessive execution timeout was accepted")
	}
	if stats := m.Stats(); stats.Stored != 0 {
		t.Fatalf("invalid requests created records: %+v", stats)
	}
	s := submit(t, m, Request{})
	final := awaitState(t, m, s.ID, Failed)
	if final.ErrorCode != "result_limit_exceeded" || len(final.Result) != 0 {
		t.Fatalf("oversized result: %+v", final)
	}
}

func TestToolPolicyNarrowsAdmissionDefaultsAndResult(t *testing.T) {
	registry := tool.NewRegistry()
	observedOutput := make(chan int, 2)
	for _, def := range []tool.Definition{
		{
			Name: "strict",
			Policy: tool.Policy{
				DefaultQueueTimeout: 100 * time.Millisecond,
				MaxQueueTimeout:     250 * time.Millisecond,
				DefaultRunTimeout:   200 * time.Millisecond,
				MaxRunTimeout:       500 * time.Millisecond,
				DefaultOutputBytes:  4,
				MaxOutputBytes:      8,
				MaxArgumentBytes:    8,
				MaxResultBytes:      5,
			},
			Validate: func(json.RawMessage) error { return nil },
			Execute: func(_ context.Context, _ json.RawMessage, options tool.RunOptions) (tool.Outcome, error) {
				observedOutput <- options.MaxOutputBytes
				return tool.Outcome{Result: json.RawMessage(`{"v":1}`)}, nil
			},
		},
		{
			Name:     "inherited",
			Validate: func(json.RawMessage) error { return nil },
			Execute: func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
				return tool.Outcome{Result: json.RawMessage(`{"ok":true}`)}, nil
			},
		},
	} {
		if err := registry.Register(def); err != nil {
			t.Fatal(err)
		}
	}
	config := DefaultConfig()
	m := newTestManagerWithRegistry(t, config, registry)
	for _, req := range []Request{
		{Tool: "strict", QueueTimeout: 300 * time.Millisecond},
		{Tool: "strict", ExecutionTimeout: time.Second},
		{Tool: "strict", OutputLimitBytes: 9},
		{Tool: "strict", Arguments: json.RawMessage(`"too long"`)},
	} {
		if _, err := m.Submit(context.Background(), req); err == nil {
			t.Fatalf("tool limit bypassed by %+v", req)
		}
	}
	if stats := m.Stats(); stats.Stored != 0 {
		t.Fatalf("rejected requests created records: %+v", stats)
	}
	strict := submit(t, m, Request{Tool: "strict"})
	if strict.QueueTimeout != 100*time.Millisecond || strict.ExecutionTimeout != 200*time.Millisecond ||
		strict.OutputLimitBytes != 4 || strict.ArgumentLimitBytes != 8 || strict.ResultLimitBytes != 5 {
		t.Fatalf("wrong effective strict policy: %+v", strict)
	}
	if final := awaitState(t, m, strict.ID, Failed); final.ErrorCode != "result_limit_exceeded" {
		t.Fatalf("tool result limit not applied: %+v", final)
	}
	if got := <-observedOutput; got != 4 {
		t.Fatalf("executor output budget = %d, want 4", got)
	}
	overridden := submit(t, m, Request{Tool: "strict", QueueTimeout: 150 * time.Millisecond, ExecutionTimeout: 300 * time.Millisecond, OutputLimitBytes: 7})
	if overridden.QueueTimeout != 150*time.Millisecond || overridden.ExecutionTimeout != 300*time.Millisecond || overridden.OutputLimitBytes != 7 {
		t.Fatalf("allowed override not recorded: %+v", overridden)
	}
	awaitState(t, m, overridden.ID, Failed)
	if got := <-observedOutput; got != 7 {
		t.Fatalf("executor output budget = %d, want 7", got)
	}
	inherited := submit(t, m, Request{Tool: "inherited"})
	if inherited.QueueTimeout != config.DefaultQueueTimeout || inherited.ExecutionTimeout != config.DefaultRunTimeout ||
		inherited.OutputLimitBytes != config.MaxOutputBytes || inherited.ArgumentLimitBytes != config.MaxArgumentBytes || inherited.ResultLimitBytes != config.MaxResultBytes {
		t.Fatalf("global defaults not inherited: %+v", inherited)
	}
	awaitState(t, m, inherited.ID, Succeeded)
}

func TestToolPolicyClipsInheritedDefaultsAndRejectsGlobalOverrun(t *testing.T) {
	definition := func(policy tool.Policy) tool.Definition {
		return tool.Definition{
			Name:     "sample",
			Policy:   policy,
			Validate: func(json.RawMessage) error { return nil },
			Execute: func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
				return tool.Outcome{}, nil
			},
		}
	}
	config := DefaultConfig()
	for _, policy := range []tool.Policy{
		{MaxQueueTimeout: config.MaxQueueTimeout + time.Second},
		{DefaultRunTimeout: config.MaxRunTimeout + time.Second},
		{MaxOutputBytes: config.MaxOutputBytes + 1},
		{MaxArgumentBytes: config.MaxArgumentBytes + 1},
		{MaxResultBytes: config.MaxResultBytes + 1},
	} {
		registry := tool.NewRegistry()
		if err := registry.Register(definition(policy)); err != nil {
			t.Fatal(err)
		}
		if _, err := NewManager(config, registry); err == nil {
			t.Fatalf("policy above global limit accepted: %+v", policy)
		}
	}
	registry := tool.NewRegistry()
	if err := registry.Register(definition(tool.Policy{
		MaxQueueTimeout: time.Second,
		MaxRunTimeout:   time.Second,
		MaxOutputBytes:  16,
	})); err != nil {
		t.Fatal(err)
	}
	m := newTestManagerWithRegistry(t, config, registry)
	s := submit(t, m, Request{Tool: "sample"})
	if s.QueueTimeout != time.Second || s.ExecutionTimeout != time.Second || s.OutputLimitBytes != 16 {
		t.Fatalf("inherited defaults did not fit tool maximum: %+v", s)
	}
	awaitState(t, m, s.ID, Succeeded)
}

func TestTerminalRetention(t *testing.T) {
	config := DefaultConfig()
	config.RetainedResults = 1
	m := testManager(t, config, func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	})
	first := submit(t, m, Request{})
	awaitState(t, m, first.ID, Succeeded)
	second := submit(t, m, Request{})
	awaitState(t, m, second.ID, Succeeded)
	if _, err := m.Get(first.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("oldest terminal record still retained: %v", err)
	}
	if stats := m.Stats(); stats.Stored != 1 {
		t.Fatalf("terminal retention exceeded: %+v", stats)
	}
}

func TestExecutorSuccessWithoutResultAndFailure(t *testing.T) {
	var calls atomic.Int32
	m := testManager(t, DefaultConfig(), func(context.Context, json.RawMessage) (json.RawMessage, error) {
		if calls.Add(1) == 1 {
			return nil, nil
		}
		return nil, errors.New("mock failed")
	})
	first := submit(t, m, Request{})
	if final := awaitState(t, m, first.ID, Succeeded); final.Result != nil {
		t.Fatalf("unexpected result: %s", final.Result)
	}
	second := submit(t, m, Request{})
	if final := awaitState(t, m, second.ID, Failed); final.ErrorCode != "execution_failed" {
		t.Fatalf("wrong executor error: %+v", final)
	}
}

func TestOutputLimitAndUserCancelKeepFirstStopReason(t *testing.T) {
	for _, tc := range []struct {
		name       string
		limitFirst bool
		want       State
		wantReason string
	}{
		{name: "output first", limitFirst: true, want: Failed, wantReason: "output_limit_exceeded"},
		{name: "cancel first", limitFirst: false, want: Cancelled, wantReason: "user_cancelled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started := make(chan struct{})
			limitCommitted := make(chan struct{})
			registry := tool.NewRegistry()
			if err := registry.Register(tool.Definition{
				Name:     "output",
				Validate: func(json.RawMessage) error { return nil },
				Execute: func(ctx context.Context, _ json.RawMessage, opts tool.RunOptions) (tool.Outcome, error) {
					close(started)
					if tc.limitFirst {
						opts.OnOutputLimit()
						close(limitCommitted)
						<-ctx.Done()
					} else {
						<-ctx.Done()
						opts.OnOutputLimit()
						close(limitCommitted)
					}
					return tool.Outcome{Stdout: "1234", OutputTruncated: true}, ctx.Err()
				},
			}); err != nil {
				t.Fatal(err)
			}
			m := newTestManagerWithRegistry(t, DefaultConfig(), registry)
			s := submit(t, m, Request{Tool: "output"})
			<-started
			if tc.limitFirst {
				<-limitCommitted
			}
			_, err := m.Cancel(s.ID)
			if err != nil {
				t.Fatal(err)
			}
			<-limitCommitted
			final := awaitState(t, m, s.ID, tc.want)
			if final.StopReason != tc.wantReason || !final.OutputTruncated || m.Stats().Running != 0 {
				t.Fatalf("stop ordering: %+v, stats=%+v", final, m.Stats())
			}
		})
	}
}

func TestUnconfirmedStopKeepsReservationAndHaltsAdmission(t *testing.T) {
	registry := tool.NewRegistry()
	if err := registry.Register(tool.Definition{
		Name:     "uncertain",
		Validate: func(json.RawMessage) error { return nil },
		Execute: func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
			return tool.Outcome{StopUnconfirmed: true}, errors.New(strings.Repeat("x", 1024))
		},
	}); err != nil {
		t.Fatal(err)
	}
	config := DefaultConfig()
	config.Workers = 1
	config.MaxErrorMessageBytes = 32
	m, err := NewManager(config, registry)
	if err != nil {
		t.Fatal(err)
	}
	s := submit(t, m, Request{Tool: "uncertain"})
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !m.Stats().Halted {
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("manager did not halt after unconfirmed stop")
		}
	}
	active, err := m.Get(s.ID)
	if err != nil || active.State != Cancelling || active.FinishedAt != nil || active.CleanupStatus != "failed" || len(active.ErrorMessage) != 32 {
		t.Fatalf("unconfirmed record: %+v, %v", active, err)
	}
	if stats := m.Stats(); stats.Running != 1 || stats.Tenants != 1 || !stats.Halted {
		t.Fatalf("unconfirmed reservation was released: %+v", stats)
	}
	if _, err := m.Submit(context.Background(), Request{Tool: "uncertain"}); !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("new work admitted after backend halt: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := m.Close(ctx); !errors.Is(err, ErrStopUnconfirmed) {
		t.Fatalf("close with unresolved process = %v", err)
	}
}
