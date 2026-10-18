package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func tenantRequest(tenant, value string) Request {
	return Request{TenantID: tenant, Tool: "workflow.test", Arguments: json.RawMessage(fmt.Sprintf("%q", value))}
}

// ACC-10: with the same accepted backlog, round robin gives B a claim before
// A's second task; FIFO retains the original ready order.
func TestTenantRoundRobinAndFIFOOrder(t *testing.T) {
	for _, tc := range []struct {
		mode SchedulerMode
		want []string
	}{
		{SchedulerRoundRobin, []string{"A1", "B1", "A2", "B2", "A3"}},
		{SchedulerFIFO, []string{"A1", "A2", "A3", "B1", "B2"}},
	} {
		t.Run(string(tc.mode), func(t *testing.T) {
			config := DefaultConfig()
			config.Workers = 1
			config.Scheduler = tc.mode
			started := make(chan string, 6)
			release := make(chan struct{})
			m := workflowManager(t, config, func(ctx context.Context, value string) error {
				started <- value
				if value == "anchor" {
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			})
			t.Cleanup(func() {
				select {
				case <-release:
				default:
					close(release)
				}
			})
			anchor := submit(t, m, tenantRequest("anchor", "anchor"))
			if got := <-started; got != "anchor" {
				t.Fatalf("first start = %q", got)
			}
			var tasks []Snapshot
			for _, pair := range []struct{ tenant, value string }{
				{"A", "A1"}, {"A", "A2"}, {"A", "A3"}, {"B", "B1"}, {"B", "B2"},
			} {
				tasks = append(tasks, submit(t, m, tenantRequest(pair.tenant, pair.value)))
			}
			close(release)
			var order []string
			for range tasks {
				select {
				case value := <-started:
					order = append(order, value)
				case <-time.After(time.Second):
					t.Fatal("task did not start")
				}
			}
			if !reflect.DeepEqual(order, tc.want) {
				t.Fatalf("claim order = %v, want %v", order, tc.want)
			}
			awaitState(t, m, anchor.ID, Succeeded)
			for _, task := range tasks {
				awaitState(t, m, task.ID, Succeeded)
			}
			if stats := m.Stats(); stats.Tenants != 0 || stats.Pending != 0 || stats.Running != 0 {
				t.Fatalf("tenant state not released: %+v", stats)
			}
		})
	}
}

func TestTenantRunningLimitSkipsSaturatedTenant(t *testing.T) {
	config := DefaultConfig()
	config.Workers = 3
	config.TenantRunningLimit = 1
	started := make(chan string, 3)
	releaseA := make(chan struct{})
	releaseB := make(chan struct{})
	var activeA, peakA atomic.Int32
	m := workflowManager(t, config, func(ctx context.Context, value string) error {
		if value == "A1" || value == "A2" {
			current := activeA.Add(1)
			if current > peakA.Load() {
				peakA.Store(current)
			}
			defer activeA.Add(-1)
		}
		started <- value
		switch value {
		case "A1":
			select {
			case <-releaseA:
			case <-ctx.Done():
				return ctx.Err()
			}
		case "B1":
			select {
			case <-releaseB:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	t.Cleanup(func() {
		for _, ch := range []chan struct{}{releaseA, releaseB} {
			select {
			case <-ch:
			default:
				close(ch)
			}
		}
	})
	a1 := submit(t, m, tenantRequest("A", "A1"))
	if got := <-started; got != "A1" {
		t.Fatalf("first start = %q", got)
	}
	a2 := submit(t, m, tenantRequest("A", "A2"))
	b1 := submit(t, m, tenantRequest("B", "B1"))
	select {
	case got := <-started:
		if got != "B1" {
			t.Fatalf("quota-blocked A prevented B: started %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("eligible tenant B did not start")
	}
	if state, err := m.Get(a2.ID); err != nil || state.State != Queued {
		t.Fatalf("A2 should remain queued: %+v, %v", state, err)
	}
	if stats := m.Stats(); stats.Running != 2 || stats.Queued != 1 {
		t.Fatalf("unexpected reservations: %+v", stats)
	}
	close(releaseB)
	awaitState(t, m, b1.ID, Succeeded)
	close(releaseA)
	awaitState(t, m, a1.ID, Succeeded)
	awaitState(t, m, a2.ID, Succeeded)
	if got := <-started; got != "A2" || peakA.Load() != 1 {
		t.Fatalf("tenant A overlap or wrong last start: %q peak=%d", got, peakA.Load())
	}
}

func TestTenantPendingLimitRejectsWithoutPartialWorkflow(t *testing.T) {
	config := DefaultConfig()
	config.Workers = 1
	config.QueueCapacity = 5
	config.TenantPendingLimit = 2
	started := make(chan struct{})
	release := make(chan struct{})
	m := workflowManager(t, config, func(ctx context.Context, value string) error {
		if value == "hold" {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	hold := submit(t, m, tenantRequest("A", "hold"))
	<-started
	a1 := submit(t, m, tenantRequest("A", "A1"))
	a2 := submit(t, m, tenantRequest("A", "A2"))
	if _, err := m.Submit(context.Background(), tenantRequest("A", "A3")); !errors.Is(err, ErrTenantQueueFull) {
		t.Fatalf("tenant pending rejection = %v", err)
	}
	if _, err := m.SubmitWorkflow(context.Background(), WorkflowRequest{TenantID: "A", Nodes: []WorkflowNode{workflowNode("x"), workflowNode("y", "x")}}); !errors.Is(err, ErrTenantQueueFull) {
		t.Fatalf("tenant workflow rejection = %v", err)
	}
	b, err := m.SubmitWorkflow(context.Background(), WorkflowRequest{TenantID: "B", Nodes: []WorkflowNode{workflowNode("x"), workflowNode("y", "x")}})
	if err != nil {
		t.Fatal(err)
	}
	if b.TenantID != "B" || b.Nodes[0].TenantID != "B" || b.Nodes[1].TenantID != "B" {
		t.Fatalf("workflow tenant identity: %+v", b)
	}
	if stats := m.Stats(); stats.Pending != 4 || stats.Stored != 5 || stats.Tenants != 2 {
		t.Fatalf("rejected batch left partial records: %+v", stats)
	}
	if node, err := m.Cancel(a1.ID); err != nil || node.State != Cancelled {
		t.Fatalf("cancel pending A task: %+v, %v", node, err)
	}
	a3 := submit(t, m, tenantRequest("A", "A3"))
	close(release)
	for _, id := range []string{hold.ID, a2.ID, a3.ID} {
		awaitState(t, m, id, Succeeded)
	}
	awaitWorkflow(t, m, b.ID, Succeeded)
	if stats := m.Stats(); stats.Pending != 0 || stats.Running != 0 || stats.Tenants != 0 {
		t.Fatalf("tenant reservations not released: %+v", stats)
	}
}

func TestTenantIdentityAndSchedulerConfiguration(t *testing.T) {
	config := DefaultConfig()
	config.MaxTenantIDBytes = 5
	m := workflowManager(t, config, func(context.Context, string) error { return nil })
	if _, err := m.Submit(context.Background(), tenantRequest("too-long", "x")); !errors.Is(err, ErrTenantIDTooLarge) {
		t.Fatalf("oversized tenant ID = %v", err)
	}
	local := submit(t, m, tenantRequest("", "local"))
	if local.TenantID != "local" {
		t.Fatalf("empty tenant should default to local: %+v", local)
	}
	awaitState(t, m, local.ID, Succeeded)
	config.Scheduler = "unexpected"
	if _, err := NewManager(config, nil); err == nil {
		t.Fatal("invalid scheduler accepted")
	}
}

func TestTenantStateDoesNotGrowAcrossCompletedIDs(t *testing.T) {
	config := DefaultConfig()
	config.RetainedResults = 1
	m := workflowManager(t, config, func(context.Context, string) error { return nil })
	for i := range 100 {
		s := submit(t, m, tenantRequest(fmt.Sprintf("tenant-%d", i), "done"))
		awaitState(t, m, s.ID, Succeeded)
		if stats := m.Stats(); stats.Tenants != 0 || stats.Pending != 0 || stats.Running != 0 {
			t.Fatalf("tenant %d left active scheduler state: %+v", i, stats)
		}
	}
	if stats := m.Stats(); stats.Stored != 1 {
		t.Fatalf("terminal retention grew with tenant IDs: %+v", stats)
	}
}
