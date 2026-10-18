package execution

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/tool"
)

const mib = 1024 * 1024

func TestSandboxBudgetSkipsUnfitReadyWork(t *testing.T) {
	for _, mode := range []SchedulerMode{SchedulerFIFO, SchedulerRoundRobin} {
		t.Run(string(mode), func(t *testing.T) {
			config := DefaultConfig()
			config.Workers = 3
			config.Scheduler = mode
			config.SandboxCPUBudgetMilli = 2000
			config.SandboxMemoryBudgetBytes = 256 * mib
			started := make(chan string, 3)
			releaseA := make(chan struct{})
			releaseC := make(chan struct{})
			registry := tool.NewRegistry()
			for _, spec := range []struct {
				name      string
				resources tool.Resources
			}{
				{"heavy", tool.Resources{CPUMilli: 1000, MemoryBytes: 192 * mib}},
				{"light", tool.Resources{CPUMilli: 500, MemoryBytes: 64 * mib}},
			} {
				resources := spec.resources
				if err := registry.Register(tool.Definition{
					Name: spec.name, Resources: resources,
					Validate: func(json.RawMessage) error { return nil },
					Execute: func(ctx context.Context, raw json.RawMessage, options tool.RunOptions) (tool.Outcome, error) {
						if options.SandboxResources != resources {
							return tool.Outcome{}, errors.New("executor received wrong reservation")
						}
						name := string(raw)
						started <- name
						switch name {
						case `"A"`:
							select {
							case <-releaseA:
							case <-ctx.Done():
								return tool.Outcome{}, ctx.Err()
							}
						case `"C"`:
							select {
							case <-releaseC:
							case <-ctx.Done():
								return tool.Outcome{}, ctx.Err()
							}
						}
						return tool.Outcome{}, nil
					},
				}); err != nil {
					t.Fatal(err)
				}
			}
			m, err := NewManager(config, registry)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				for _, ch := range []chan struct{}{releaseA, releaseC} {
					select {
					case <-ch:
					default:
						close(ch)
					}
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := m.Close(ctx); err != nil {
					t.Errorf("close: %v", err)
				}
			})
			a := submit(t, m, Request{TenantID: "A", Tool: "heavy", Arguments: json.RawMessage(`"A"`)})
			mustStart(t, started, `"A"`)
			b := submit(t, m, Request{TenantID: "B", Tool: "heavy", Arguments: json.RawMessage(`"B"`)})
			c := submit(t, m, Request{TenantID: "C", Tool: "light", Arguments: json.RawMessage(`"C"`)})
			mustStart(t, started, `"C"`)
			if stats := m.Stats(); stats.Running != 2 || stats.Pending != 1 || stats.ReservedSandboxCPUMilli != 1500 || stats.ReservedSandboxMemoryBytes != 256*mib {
				t.Fatalf("resources not reserved atomically: %+v", stats)
			}
			if snap, err := m.Get(b.ID); err != nil || snap.State != Queued {
				t.Fatalf("unfit task was claimed: %+v, %v", snap, err)
			}
			close(releaseC)
			awaitState(t, m, c.ID, Succeeded)
			if stats := m.Stats(); stats.ReservedSandboxMemoryBytes != 192*mib {
				t.Fatalf("light task reservation not released: %+v", stats)
			}
			close(releaseA)
			mustStart(t, started, `"B"`)
			awaitState(t, m, a.ID, Succeeded)
			awaitState(t, m, b.ID, Succeeded)
			if stats := m.Stats(); stats.ReservedSandboxCPUMilli != 0 || stats.ReservedSandboxMemoryBytes != 0 || stats.Running != 0 {
				t.Fatalf("sandbox reservation leaked: %+v", stats)
			}
		})
	}
}

func mustStart(t *testing.T, started <-chan string, want string) {
	t.Helper()
	select {
	case got := <-started:
		if got != want {
			t.Fatalf("started %s, want %s", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("%s did not start", want)
	}
}

func TestSandboxOversizeRejectedWithoutAdmission(t *testing.T) {
	config := DefaultConfig()
	registry := tool.NewRegistry()
	if err := registry.Register(tool.Definition{
		Name:      "oversize",
		Resources: tool.Resources{CPUMilli: 1001, MemoryBytes: 256 * mib},
		Validate:  func(json.RawMessage) error { return nil },
		Execute: func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
			return tool.Outcome{}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(tool.Definition{
		Name:      "valid",
		Resources: tool.Resources{CPUMilli: 1000, MemoryBytes: 256 * mib},
		Validate:  func(json.RawMessage) error { return nil },
		Execute: func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
			return tool.Outcome{}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	m, err := NewManager(config, registry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close(context.Background()) })
	if _, err := m.Submit(context.Background(), Request{Tool: "oversize"}); !errors.Is(err, ErrSandboxBudgetExceeded) {
		t.Fatalf("oversize submit error = %v", err)
	}
	if _, err := m.SubmitWorkflow(context.Background(), WorkflowRequest{Nodes: []WorkflowNode{{ID: "one", Tool: "valid"}, {ID: "two", Tool: "oversize", DependsOn: []string{"one"}}}}); !errors.Is(err, ErrSandboxBudgetExceeded) {
		t.Fatalf("oversize workflow error = %v", err)
	}
	if stats := m.Stats(); stats.Pending != 0 || stats.Running != 0 || stats.Stored != 0 {
		t.Fatalf("oversize work was admitted: %+v", stats)
	}
}

func TestSandboxStopUnconfirmedKeepsReservation(t *testing.T) {
	config := DefaultConfig()
	registry := tool.NewRegistry()
	if err := registry.Register(tool.Definition{
		Name:      "uncertain",
		Resources: tool.Resources{CPUMilli: 1000, MemoryBytes: 256 * mib},
		Validate:  func(json.RawMessage) error { return nil },
		Execute: func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
			return tool.Outcome{StopUnconfirmed: true}, errors.New("runtime unavailable")
		},
	}); err != nil {
		t.Fatal(err)
	}
	m, err := NewManager(config, registry)
	if err != nil {
		t.Fatal(err)
	}
	s := submit(t, m, Request{Tool: "uncertain"})
	deadline := time.After(time.Second)
	for !m.Stats().Halted {
		select {
		case <-deadline:
			t.Fatal("manager did not halt")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if snap, err := m.Get(s.ID); err != nil || snap.State != Cancelling || snap.CleanupStatus != "failed" {
		t.Fatalf("unconfirmed stop snapshot: %+v, %v", snap, err)
	}
	if stats := m.Stats(); stats.Running != 1 || stats.ReservedSandboxCPUMilli != 1000 || stats.ReservedSandboxMemoryBytes != 256*mib {
		t.Fatalf("unconfirmed stop released resources: %+v", stats)
	}
	if err := m.Close(context.Background()); !errors.Is(err, ErrStopUnconfirmed) {
		t.Fatalf("close error = %v", err)
	}
}

func TestCleanupRetryUpdatesTerminalSnapshot(t *testing.T) {
	updates := make(chan func(string, string), 1)
	registry := tool.NewRegistry()
	if err := registry.Register(tool.Definition{
		Name: "cleanup", Resources: tool.Resources{CPUMilli: 1000, MemoryBytes: 256 * mib},
		Validate: func(json.RawMessage) error { return nil },
		Execute: func(_ context.Context, _ json.RawMessage, options tool.RunOptions) (tool.Outcome, error) {
			updates <- options.OnCleanupUpdate
			return tool.Outcome{CleanupStatus: "pending", CleanupError: "remove unavailable"}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	m, err := NewManager(DefaultConfig(), registry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close(context.Background()) })
	s := submit(t, m, Request{Tool: "cleanup"})
	awaitState(t, m, s.ID, Succeeded)
	if snap, err := m.Get(s.ID); err != nil || snap.CleanupStatus != "pending" || snap.CleanupError == "" {
		t.Fatalf("pending cleanup not exposed: %+v %v", snap, err)
	}
	(<-updates)("succeeded", "")
	if snap, err := m.Get(s.ID); err != nil || snap.CleanupStatus != "succeeded" || snap.CleanupError != "" {
		t.Fatalf("cleanup retry not reflected: %+v %v", snap, err)
	}
}

func TestCleanupRetryBeforeCompletionPreservesFinalStatus(t *testing.T) {
	registry := tool.NewRegistry()
	if err := registry.Register(tool.Definition{
		Name: "fast-cleanup", Resources: tool.Resources{CPUMilli: 1000, MemoryBytes: 256 * mib},
		Validate: func(json.RawMessage) error { return nil },
		Execute: func(_ context.Context, _ json.RawMessage, options tool.RunOptions) (tool.Outcome, error) {
			options.OnCleanupUpdate("succeeded", "")
			return tool.Outcome{CleanupStatus: "pending", CleanupError: "earlier removal failure"}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	m, err := NewManager(DefaultConfig(), registry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close(context.Background()) })
	s := submit(t, m, Request{Tool: "fast-cleanup"})
	awaitState(t, m, s.ID, Succeeded)
	if snap, err := m.Get(s.ID); err != nil || snap.CleanupStatus != "succeeded" || snap.CleanupError != "" {
		t.Fatalf("late execution result overwrote cleanup recovery: %+v %v", snap, err)
	}
}
