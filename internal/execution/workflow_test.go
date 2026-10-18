package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/tool"
)

func workflowManager(t *testing.T, config Config, execute func(context.Context, string) error) *Manager {
	t.Helper()
	registry := tool.NewRegistry()
	if err := registry.Register(tool.Definition{
		Name: "workflow.test",
		Validate: func(raw json.RawMessage) error {
			var value string
			return json.Unmarshal(raw, &value)
		},
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
	m, err := NewManager(config, registry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := m.Close(ctx); err != nil {
			t.Errorf("close workflow manager: %v", err)
		}
	})
	return m
}

func workflowNode(id string, dependencies ...string) WorkflowNode {
	return WorkflowNode{ID: id, Tool: "workflow.test", Arguments: json.RawMessage(fmt.Sprintf("%q", id)), DependsOn: dependencies}
}

func awaitWorkflow(t *testing.T, m *Manager, id string, want State) WorkflowSnapshot {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		s, err := m.GetWorkflow(id)
		if err != nil {
			t.Fatal(err)
		}
		if s.State == want {
			return s
		}
		if s.FinishedAt != nil {
			t.Fatalf("workflow reached %s, wanted %s", s.State, want)
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("workflow still %s, wanted %s", s.State, want)
		}
	}
}

func TestWorkflowInvalidGraphsRejectWholeBatch(t *testing.T) {
	config := DefaultConfig()
	config.MaxWorkflowNodes = 4
	config.MaxWorkflowBytes = 512
	var calls atomic.Int32
	m := workflowManager(t, config, func(context.Context, string) error {
		calls.Add(1)
		return nil
	})
	for _, tc := range []struct {
		name  string
		nodes []WorkflowNode
	}{
		{name: "empty"},
		{name: "duplicate ID", nodes: []WorkflowNode{workflowNode("a"), workflowNode("a")}},
		{name: "missing dependency", nodes: []WorkflowNode{workflowNode("a", "missing")}},
		{name: "self dependency", nodes: []WorkflowNode{workflowNode("a", "a")}},
		{name: "cycle", nodes: []WorkflowNode{workflowNode("a", "b"), workflowNode("b", "a")}},
		{name: "duplicate dependency", nodes: []WorkflowNode{workflowNode("a"), workflowNode("b", "a", "a")}},
		{name: "too many nodes", nodes: []WorkflowNode{workflowNode("a"), workflowNode("b"), workflowNode("c"), workflowNode("d"), workflowNode("e")}},
		{name: "invalid tool arguments", nodes: []WorkflowNode{workflowNode("a"), {ID: "b", Tool: "workflow.test", Arguments: json.RawMessage(`123`)}}},
		{name: "oversized request", nodes: []WorkflowNode{{ID: "a", Tool: "workflow.test", Arguments: json.RawMessage(fmt.Sprintf("%q", strings.Repeat("x", 600)))}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := m.SubmitWorkflow(context.Background(), WorkflowRequest{Nodes: tc.nodes}); err == nil {
				t.Fatal("invalid workflow was accepted")
			}
			if stats := m.Stats(); stats.Stored != 0 || stats.Pending != 0 || stats.Running != 0 || calls.Load() != 0 {
				t.Fatalf("rejected workflow changed manager: stats=%+v calls=%d", stats, calls.Load())
			}
		})
	}
}

func TestWorkflowCapacityRejectsWholeBatch(t *testing.T) {
	config := DefaultConfig()
	config.Workers = 1
	config.QueueCapacity = 2
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
	first := submit(t, m, Request{Tool: "workflow.test", Arguments: json.RawMessage(`"hold"`)})
	<-started
	second := submit(t, m, Request{Tool: "workflow.test", Arguments: json.RawMessage(`"queued"`)})
	if _, err := m.SubmitWorkflow(context.Background(), WorkflowRequest{Nodes: []WorkflowNode{workflowNode("a"), workflowNode("b", "a")}}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("batch capacity error = %v", err)
	}
	if stats := m.Stats(); stats.Pending != 1 || stats.Queued != 1 || stats.Stored != 2 {
		t.Fatalf("partial batch admitted: %+v", stats)
	}
	close(release)
	awaitState(t, m, first.ID, Succeeded)
	awaitState(t, m, second.ID, Succeeded)
}

func TestWorkflowDependencySuccessFailureAndIndependentBranch(t *testing.T) {
	config := DefaultConfig()
	config.Workers = 1
	var calls atomic.Int32
	m := workflowManager(t, config, func(_ context.Context, value string) error {
		calls.Add(1)
		if value == "b" {
			return errors.New("b failed")
		}
		return nil
	})
	s, err := m.SubmitWorkflow(context.Background(), WorkflowRequest{Nodes: []WorkflowNode{
		workflowNode("d", "b"), workflowNode("c", "a"), workflowNode("b", "a"),
		workflowNode("a"), workflowNode("independent"), workflowNode("joined", "a", "independent"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if s.Nodes[0].State != Blocked || s.Nodes[0].ReadyAt != nil || s.Nodes[3].State != Queued || s.Nodes[3].ReadyAt == nil {
		t.Fatalf("initial dependencies: %+v", s.Nodes)
	}
	final := awaitWorkflow(t, m, s.ID, Failed)
	want := map[string]State{"a": Succeeded, "b": Failed, "c": Succeeded, "d": Skipped, "independent": Succeeded, "joined": Succeeded}
	for _, node := range final.Nodes {
		if node.State != want[node.NodeID] {
			t.Fatalf("node %s state = %s, want %s", node.NodeID, node.State, want[node.NodeID])
		}
		if node.WorkflowID != s.ID || node.FinishedAt == nil {
			t.Fatalf("node lacks workflow identity or final time: %+v", node)
		}
		if node.NodeID == "d" && (node.ErrorCode != "dependency_failed" || node.ReadyAt != nil) {
			t.Fatalf("skipped node was readied or has wrong reason: %+v", node)
		}
	}
	if calls.Load() != 5 || m.Stats().Pending != 0 || m.Stats().Running != 0 {
		t.Fatalf("unexpected execution count or reservations: calls=%d stats=%+v", calls.Load(), m.Stats())
	}
}

func TestWorkflowCancelMarksWholeBatchBeforeStoppingRunner(t *testing.T) {
	config := DefaultConfig()
	config.Workers = 1
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	m := workflowManager(t, config, func(ctx context.Context, value string) error {
		calls.Add(1)
		if value == "root" {
			close(started)
			<-release
			return ctx.Err()
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
	s, err := m.SubmitWorkflow(context.Background(), WorkflowRequest{Nodes: []WorkflowNode{
		workflowNode("root"), workflowNode("blocked", "root"), workflowNode("queued"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	stopping, err := m.CancelWorkflow(s.ID)
	if err != nil || stopping.State != Cancelling {
		t.Fatalf("cancel workflow: %+v, %v", stopping, err)
	}
	if stopping.Nodes[0].State != Cancelling || stopping.Nodes[1].State != Cancelled || stopping.Nodes[2].State != Cancelled || m.Stats().Pending != 0 {
		t.Fatalf("batch cancellation not atomic: %+v stats=%+v", stopping, m.Stats())
	}
	close(release)
	final := awaitWorkflow(t, m, s.ID, Cancelled)
	if calls.Load() != 1 || final.FinishedAt == nil || m.Stats().Running != 0 {
		t.Fatalf("cancelled workflow left work: %+v calls=%d stats=%+v", final, calls.Load(), m.Stats())
	}
}

func TestWorkflowBlockedNodeTimeoutSkipsDescendant(t *testing.T) {
	config := DefaultConfig()
	config.Workers = 1
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	m := workflowManager(t, config, func(ctx context.Context, value string) error {
		calls.Add(1)
		if value == "root" {
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
	child := workflowNode("child", "root")
	child.QueueTimeout = 20 * time.Millisecond
	s, err := m.SubmitWorkflow(context.Background(), WorkflowRequest{Nodes: []WorkflowNode{workflowNode("root"), child, workflowNode("grandchild", "child")}})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	awaitState(t, m, s.Nodes[1].ID, TimedOut)
	if grandchild := awaitState(t, m, s.Nodes[2].ID, Skipped); grandchild.ErrorCode != "dependency_failed" {
		t.Fatalf("grandchild state: %+v", grandchild)
	}
	if stats := m.Stats(); stats.Pending != 0 || stats.Running != 1 {
		t.Fatalf("blocked timeout did not release pending capacity: %+v", stats)
	}
	close(release)
	final := awaitWorkflow(t, m, s.ID, Failed)
	if final.Nodes[1].ErrorCode != "queue_timeout" || calls.Load() != 1 {
		t.Fatalf("blocked node ran or lost timeout: %+v calls=%d", final.Nodes[1], calls.Load())
	}
}

func TestWorkflowRetentionEvictsWholeCompletedWorkflow(t *testing.T) {
	config := DefaultConfig()
	config.RetainedResults = 3
	m := workflowManager(t, config, func(context.Context, string) error { return nil })
	s, err := m.SubmitWorkflow(context.Background(), WorkflowRequest{Nodes: []WorkflowNode{workflowNode("a"), workflowNode("b", "a")}})
	if err != nil {
		t.Fatal(err)
	}
	awaitWorkflow(t, m, s.ID, Succeeded)
	first := submit(t, m, Request{Tool: "workflow.test", Arguments: json.RawMessage(`"first"`)})
	awaitState(t, m, first.ID, Succeeded)
	second := submit(t, m, Request{Tool: "workflow.test", Arguments: json.RawMessage(`"second"`)})
	awaitState(t, m, second.ID, Succeeded)
	if _, err := m.GetWorkflow(s.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("workflow retained after node eviction: %v", err)
	}
	for _, node := range s.Nodes {
		if _, err := m.Get(node.ID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("workflow node %s retained after group eviction: %v", node.ID, err)
		}
	}
	if stats := m.Stats(); stats.Stored != 2 {
		t.Fatalf("unexpected retained count: %+v", stats)
	}
}

func TestWorkflowDependencyCannotStartBeforeAllParents(t *testing.T) {
	config := DefaultConfig()
	config.Workers = 2
	started := make(chan string, 3)
	release := make(chan struct{})
	m := workflowManager(t, config, func(ctx context.Context, value string) error {
		started <- value
		if value == "slow" {
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
	s, err := m.SubmitWorkflow(context.Background(), WorkflowRequest{Nodes: []WorkflowNode{
		workflowNode("joined", "fast", "slow"), workflowNode("fast"), workflowNode("slow"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case got := <-started:
			if got == "joined" {
				t.Fatal("dependent started before its parents")
			}
		case <-time.After(time.Second):
			t.Fatal("root node did not start")
		}
	}
	blocked, err := m.Get(s.Nodes[0].ID)
	if err != nil || blocked.State != Blocked || blocked.ReadyAt != nil {
		t.Fatalf("dependent released before slow parent: %+v, %v", blocked, err)
	}
	close(release)
	select {
	case got := <-started:
		if got != "joined" {
			t.Fatalf("started %q after parents, want joined", got)
		}
	case <-time.After(time.Second):
		t.Fatal("dependent did not start")
	}
	awaitWorkflow(t, m, s.ID, Succeeded)
}

func TestWorkflowIndividualNodeCancelSkipsOnlyDependentBranch(t *testing.T) {
	config := DefaultConfig()
	config.Workers = 1
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
	s, err := m.SubmitWorkflow(context.Background(), WorkflowRequest{Nodes: []WorkflowNode{
		workflowNode("hold"), workflowNode("cancelled"), workflowNode("child", "cancelled"), workflowNode("independent"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if node, err := m.Cancel(s.Nodes[1].ID); err != nil || node.State != Cancelled {
		t.Fatalf("cancel workflow node: %+v, %v", node, err)
	}
	if child, err := m.Get(s.Nodes[2].ID); err != nil || child.State != Skipped || child.ErrorCode != "dependency_failed" {
		t.Fatalf("dependent after cancellation: %+v, %v", child, err)
	}
	close(release)
	final := awaitWorkflow(t, m, s.ID, Failed)
	if final.Nodes[3].State != Succeeded || m.Stats().Pending != 0 {
		t.Fatalf("independent branch did not finish: %+v stats=%+v", final, m.Stats())
	}
}

func TestWorkflowCloseCancelsBlockedAndQueued(t *testing.T) {
	config := DefaultConfig()
	config.Workers = 1
	started := make(chan struct{})
	m := workflowManager(t, config, func(ctx context.Context, value string) error {
		if value == "running" {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	s, err := m.SubmitWorkflow(context.Background(), WorkflowRequest{Nodes: []WorkflowNode{
		workflowNode("running"), workflowNode("blocked", "running"), workflowNode("queued"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if err := m.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	final, err := m.GetWorkflow(s.ID)
	if err != nil || final.State != Cancelled || final.FinishedAt == nil {
		t.Fatalf("closed workflow: %+v, %v", final, err)
	}
	for _, node := range final.Nodes {
		if node.State != Cancelled {
			t.Fatalf("node %s ended as %s", node.NodeID, node.State)
		}
	}
	if stats := m.Stats(); stats.Pending != 0 || stats.Running != 0 {
		t.Fatalf("closed manager retained capacity: %+v", stats)
	}
}
