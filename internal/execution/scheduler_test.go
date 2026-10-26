package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/haonox/toolane/internal/tool"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func tenantRequest(tenant, value string) Request {
	return Request{TenantID: tenant, Tool: "scheduler.test", Arguments: json.RawMessage(fmt.Sprintf("%q", value))}
}

func schedulerManager(t *testing.T, config Config, execute func(context.Context, string) error) *Manager {
	t.Helper()
	registry := tool.NewRegistry()
	if err := registry.Register(tool.Definition{
		Name:     "scheduler.test",
		Validate: func(raw json.RawMessage) error { var value string; return json.Unmarshal(raw, &value) },
		Execute: func(ctx context.Context, raw json.RawMessage, _ tool.RunOptions) (tool.Outcome, error) {
			var value string
			if err := json.Unmarshal(raw, &value); err != nil {
				return tool.Outcome{}, err
			}
			return tool.Outcome{}, execute(ctx, value)
		},
	}); err != nil {
		t.Fatal(err)
	}
	return newTestManagerWithRegistry(t, config, registry)
}

// With the same accepted backlog, round robin gives B a claim before A's second task.
func TestTenantRoundRobinOrder(t *testing.T) {
	config := DefaultConfig()
	config.Workers = 1
	started := make(chan string, 6)
	release := make(chan struct{})
	m := schedulerManager(t, config, func(ctx context.Context, value string) error {
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
	want := []string{"A1", "B1", "A2", "B2", "A3"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("claim order = %v, want %v", order, want)
	}
	awaitState(t, m, anchor.ID, Succeeded)
	for _, task := range tasks {
		awaitState(t, m, task.ID, Succeeded)
	}
	if stats := m.Stats(); stats.Tenants != 0 || stats.Pending != 0 || stats.Running != 0 {
		t.Fatalf("tenant state not released: %+v", stats)
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
	m := schedulerManager(t, config, func(ctx context.Context, value string) error {
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

// ACC-10: a tenant that keeps submitting against a full queue cannot prevent
// another ready tenant from being claimed, and its pending quota stays bounded.
func TestTenantContinuousSubmissionDoesNotStarveOtherTenant(t *testing.T) {
	config := DefaultConfig()
	config.Workers = 1
	config.QueueCapacity = 128
	config.TenantRunningLimit = 1
	started := make(chan string, 2)
	releaseAnchor := make(chan struct{})
	releaseB := make(chan struct{})
	producerAccepted := make(chan struct{})
	allowAAfterB := make(chan struct{})
	var aStarted, accepted, rejected atomic.Int32
	m := schedulerManager(t, config, func(ctx context.Context, value string) error {
		switch value {
		case "anchor":
			started <- value
			select {
			case <-releaseAnchor:
			case <-ctx.Done():
				return ctx.Err()
			}
		case "A":
			if aStarted.Add(1) == 1 {
				select {
				case <-producerAccepted:
				case <-allowAAfterB:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		case "B":
			started <- value
			select {
			case <-releaseB:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	anchor := submit(t, m, tenantRequest("anchor", "anchor"))
	if got := <-started; got != "anchor" {
		t.Fatalf("first start = %q", got)
	}
	for range config.TenantPendingLimit {
		submit(t, m, tenantRequest("A", "A"))
	}
	b := submit(t, m, tenantRequest("B", "B"))
	producerCtx, stopProducer := context.WithCancel(context.Background())
	producerDone := make(chan struct{})
	producerError := make(chan error, 1)
	go func() {
		defer close(producerDone)
		for {
			if producerCtx.Err() != nil {
				return
			}
			_, err := m.Submit(producerCtx, tenantRequest("A", "A"))
			switch {
			case err == nil:
				if accepted.Add(1) == 1 {
					close(producerAccepted)
				}
			case errors.Is(err, ErrTenantQueueFull):
				rejected.Add(1)
			case producerCtx.Err() != nil:
				return
			default:
				producerError <- err
				return
			}
			select {
			case <-producerCtx.Done():
				return
			case <-time.After(100 * time.Microsecond):
			}
		}
	}()
	t.Cleanup(func() {
		stopProducer()
		<-producerDone
		for _, ch := range []chan struct{}{releaseAnchor, releaseB, allowAAfterB} {
			select {
			case <-ch:
			default:
				close(ch)
			}
		}
	})
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for rejected.Load() < 20 {
		select {
		case err := <-producerError:
			t.Fatalf("A producer failed: %v", err)
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("A producer did not keep submitting: rejected=%d", rejected.Load())
		}
	}
	close(releaseAnchor)
	select {
	case got := <-started:
		if got != "B" {
			t.Fatalf("other tenant start = %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("B was starved by continuous A submissions")
	}
	aBeforeB := aStarted.Load()
	if aBeforeB > 1 {
		t.Fatalf("A claimed %d tasks before B", aBeforeB)
	}
	if aBeforeB == 1 && accepted.Load() == 0 {
		t.Fatal("A producer did not refill the queue before B started")
	}
	if stats := m.Stats(); stats.Running != 1 || stats.Pending < config.TenantPendingLimit-1 || stats.Pending > config.TenantPendingLimit {
		t.Fatalf("B did not start while A backlog remained bounded: %+v", stats)
	}
	stopProducer()
	<-producerDone
	close(allowAAfterB)
	select {
	case err := <-producerError:
		t.Fatalf("A producer failed: %v", err)
	default:
	}
	close(releaseB)
	awaitState(t, m, anchor.ID, Succeeded)
	awaitState(t, m, b.ID, Succeeded)
	for {
		if stats := m.Stats(); stats.Pending == 0 && stats.Running == 0 && stats.Tenants == 0 {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("tenant reservations not released: %+v", m.Stats())
		}
	}
	t.Logf("ACC-10: A accepted=%d after initial %d, rejected=%d; B claimed after %d A tasks", accepted.Load(), config.TenantPendingLimit, rejected.Load(), aBeforeB)
}

func TestTenantIdentity(t *testing.T) {
	config := DefaultConfig()
	config.MaxTenantIDBytes = 5
	m := schedulerManager(t, config, func(context.Context, string) error { return nil })
	if _, err := m.Submit(context.Background(), tenantRequest("too-long", "x")); !errors.Is(err, ErrTenantIDTooLarge) {
		t.Fatalf("oversized tenant ID = %v", err)
	}
	local := submit(t, m, tenantRequest("", "local"))
	if local.TenantID != "local" {
		t.Fatalf("empty tenant should default to local: %+v", local)
	}
	awaitState(t, m, local.ID, Succeeded)
}

func TestTenantStateDoesNotGrowAcrossCompletedIDs(t *testing.T) {
	config := DefaultConfig()
	config.RetainedResults = 1
	m := schedulerManager(t, config, func(context.Context, string) error { return nil })
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
