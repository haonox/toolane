package execution

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/store/sqlite"
	"github.com/haonox/toolane/internal/tool"
)

func TestDurableWorkflowRunsAndSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "state")
	s := openDurableStore(t, dir)
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	registry := durableRegistry(t, func(_ context.Context, _ json.RawMessage, options tool.RunOptions) (tool.Outcome, error) {
		row, err := s.GetExecution(ctx, options.ExecutionID)
		if err != nil || row.State != string(Running) {
			return tool.Outcome{}, errors.New("workflow node was not claimed durably")
		}
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return tool.Outcome{Result: json.RawMessage(`{"ok":true}`)}, nil
	})
	config := DefaultConfig()
	config.Workers = 1
	config.RetainedResults = 1 // Completed batch must remain readable from SQLite.
	m, err := NewDurableManager(ctx, config, registry, s)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := m.SubmitWorkflow(ctx, WorkflowRequest{Nodes: []WorkflowNode{
		{ID: "root", Tool: "test"}, {ID: "child", Tool: "test", DependsOn: []string{"root"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	stored, err := s.GetWorkflow(ctx, accepted.ID)
	if err != nil || len(stored.Nodes) != 2 || stored.Nodes[0].Execution.State != string(Running) || stored.Nodes[1].Execution.State != string(Blocked) {
		t.Fatalf("accepted workflow not stored before execution: %+v err=%v", stored, err)
	}
	close(release)
	finished := awaitWorkflow(t, m, accepted.ID, Succeeded)
	if calls.Load() != 2 || finished.Nodes[0].State != Succeeded || finished.Nodes[1].State != Succeeded {
		t.Fatalf("workflow execution: calls=%d snapshot=%+v", calls.Load(), finished)
	}
	if terminal, err := m.CancelWorkflow(accepted.ID); err != nil || terminal.State != Succeeded {
		t.Fatalf("cancel evicted terminal workflow: %+v err=%v", terminal, err)
	}
	stored, err = s.GetWorkflow(ctx, accepted.ID)
	if err != nil || stored.State != string(Succeeded) || stored.FinishedAt == nil || stored.Version != 2 {
		t.Fatalf("workflow summary not committed: %+v err=%v", stored, err)
	}
	for i, node := range stored.Nodes {
		events, err := s.EventsAfter(ctx, node.Execution.ID, 0)
		wantEvents := 3
		if i == 1 {
			wantEvents = 4 // blocked -> queued is a persisted transition.
		}
		if err != nil || len(events) != wantEvents || node.Execution.State != string(Succeeded) {
			t.Fatalf("node %q state=%q event count=%d, want=%d err=%v", node.NodeID, node.Execution.State, len(events), wantEvents, err)
		}
	}
	closeDurableManager(t, m)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openDurableStore(t, dir)
	defer s.Close()
	m, err = NewDurableManager(ctx, config, registry, s)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDurableManager(t, m)
	got, err := m.GetWorkflow(accepted.ID)
	if err != nil || got.State != Succeeded || len(got.Nodes) != 2 || calls.Load() != 2 {
		t.Fatalf("completed workflow after restart: %+v calls=%d err=%v", got, calls.Load(), err)
	}
}

func seedWorkflowForRecovery(t *testing.T, s *sqlite.Store, states []State, dependencies [][]string) string {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	workflowID := "recovery-workflow"
	batch := sqlite.Workflow{ID: workflowID, TenantID: "local", State: string(Running), Request: []byte(`{"nodes":[]}`), CreatedAt: now}
	for i, state := range states {
		id := "node-" + string(rune('a'+i))
		var readyAt *time.Time
		if state == Queued {
			readyAt = &now
		}
		r := &record{snapshot: Snapshot{
			ID: id, TenantID: "local", Tool: "test", WorkflowID: workflowID, NodeID: id,
			State: state, CleanupStatus: "not_needed", CreatedAt: now, ReadyAt: readyAt,
			QueueTimeout: time.Minute, ExecutionTimeout: time.Second, OutputLimitBytes: 1024,
			ArgumentLimitBytes: 1024, ResultLimitBytes: 1024,
		}, arguments: json.RawMessage(`{"x":1}`), deadline: now.Add(time.Minute)}
		encoded, err := encodeStoredExecution(r, r.snapshot)
		if err != nil {
			t.Fatal(err)
		}
		event, err := stateEvent(r.snapshot, now)
		if err != nil {
			t.Fatal(err)
		}
		batch.Nodes = append(batch.Nodes, sqlite.WorkflowNode{
			NodeID: id, DependsOn: dependencies[i],
			Execution: sqlite.Execution{ID: id, TenantID: "local", State: string(state), Snapshot: encoded, Request: []byte(`{"tool":"test"}`), CreatedAt: now},
			Event:     event,
		})
	}
	if err := s.CreateWorkflow(ctx, batch); err != nil {
		t.Fatal(err)
	}
	return workflowID
}

func transitionSeedNode(t *testing.T, s *sqlite.Store, id string, state State) {
	t.Helper()
	ctx := context.Background()
	row, err := s.GetExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	r, err := decodeStoredExecution(row)
	if err != nil {
		t.Fatal(err)
	}
	next := cloneSnapshot(r.snapshot)
	next.State = state
	now := time.Now().UTC()
	if state == Running {
		next.RunningAt = &now
	} else {
		next.FinishedAt = &now
	}
	encoded, err := encodeStoredExecution(r, next)
	if err != nil {
		t.Fatal(err)
	}
	event, err := stateEvent(next, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Transition(ctx, id, row.Version, string(state), encoded, event); err != nil {
		t.Fatal(err)
	}
}

func TestDurableWorkflowRecoveryInterruptsClaimAndKeepsIndependentBranch(t *testing.T) {
	ctx := context.Background()
	s := openDurableStore(t, filepath.Join(t.TempDir(), "state"))
	defer s.Close()
	id := seedWorkflowForRecovery(t, s, []State{Queued, Blocked, Queued}, [][]string{nil, {"node-a"}, nil})
	transitionSeedNode(t, s, "node-a", Running)
	var calls atomic.Int32
	registry := durableRegistry(t, func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
		calls.Add(1)
		return tool.Outcome{}, nil
	})
	m, err := NewDurableManager(ctx, DefaultConfig(), registry, s)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDurableManager(t, m)
	got := awaitWorkflow(t, m, id, Failed)
	if got.Nodes[0].State != Interrupted || got.Nodes[1].State != Skipped || got.Nodes[2].State != Succeeded || calls.Load() != 1 {
		t.Fatalf("recovered workflow: %+v calls=%d", got, calls.Load())
	}
}

func TestDurableWorkflowRecoveryRepairsReadyChild(t *testing.T) {
	ctx := context.Background()
	s := openDurableStore(t, filepath.Join(t.TempDir(), "state"))
	defer s.Close()
	id := seedWorkflowForRecovery(t, s, []State{Queued, Blocked}, [][]string{nil, {"node-a"}})
	transitionSeedNode(t, s, "node-a", Succeeded) // Crash before child queued transition.
	var calls atomic.Int32
	registry := durableRegistry(t, func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
		calls.Add(1)
		return tool.Outcome{}, nil
	})
	m, err := NewDurableManager(ctx, DefaultConfig(), registry, s)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDurableManager(t, m)
	got := awaitWorkflow(t, m, id, Succeeded)
	if got.Nodes[1].State != Succeeded || calls.Load() != 1 {
		t.Fatalf("recovered ready child: %+v calls=%d", got, calls.Load())
	}
}

func TestDurableWorkflowCancelPersistsAllNodes(t *testing.T) {
	ctx := context.Background()
	s := openDurableStore(t, filepath.Join(t.TempDir(), "state"))
	defer s.Close()
	started := make(chan struct{})
	registry := durableRegistry(t, func(ctx context.Context, _ json.RawMessage, _ tool.RunOptions) (tool.Outcome, error) {
		close(started)
		<-ctx.Done()
		return tool.Outcome{}, ctx.Err()
	})
	config := DefaultConfig()
	config.Workers = 1
	m, err := NewDurableManager(ctx, config, registry, s)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDurableManager(t, m)
	accepted, err := m.SubmitWorkflow(ctx, WorkflowRequest{Nodes: []WorkflowNode{
		{ID: "running", Tool: "test"},
		{ID: "blocked", Tool: "test", DependsOn: []string{"running"}},
		{ID: "queued", Tool: "test"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	cancelling, err := m.CancelWorkflow(accepted.ID)
	if err != nil || cancelling.State != Cancelling {
		t.Fatalf("cancel workflow: %+v err=%v", cancelling, err)
	}
	finished := awaitWorkflow(t, m, accepted.ID, Cancelled)
	for _, node := range finished.Nodes {
		if node.State != Cancelled {
			t.Fatalf("node %q after cancel: %s", node.NodeID, node.State)
		}
	}
	stored, err := s.GetWorkflow(ctx, accepted.ID)
	if err != nil || stored.State != string(Cancelled) || stored.FinishedAt == nil {
		t.Fatalf("persisted cancellation: %+v err=%v", stored, err)
	}
}

func TestDurableWorkflowRecoveryRepairsSummaryAfterLastNode(t *testing.T) {
	ctx := context.Background()
	s := openDurableStore(t, filepath.Join(t.TempDir(), "state"))
	defer s.Close()
	id := seedWorkflowForRecovery(t, s, []State{Queued, Queued}, [][]string{nil, nil})
	transitionSeedNode(t, s, "node-a", Succeeded)
	transitionSeedNode(t, s, "node-b", Succeeded) // Crash before Workflow summary update.
	var calls atomic.Int32
	registry := durableRegistry(t, func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
		calls.Add(1)
		return tool.Outcome{}, nil
	})
	m, err := NewDurableManager(ctx, DefaultConfig(), registry, s)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDurableManager(t, m)
	got, err := m.GetWorkflow(id)
	if err != nil || got.State != Succeeded || got.FinishedAt == nil || calls.Load() != 0 {
		t.Fatalf("summary recovery: %+v calls=%d err=%v", got, calls.Load(), err)
	}
	stored, err := s.GetWorkflow(ctx, id)
	if err != nil || stored.State != string(Succeeded) || stored.FinishedAt == nil {
		t.Fatalf("persisted summary recovery: %+v err=%v", stored, err)
	}
}

func TestDurableWorkflowRecoveryFinishesAcceptedCancellation(t *testing.T) {
	ctx := context.Background()
	s := openDurableStore(t, filepath.Join(t.TempDir(), "state"))
	defer s.Close()
	id := seedWorkflowForRecovery(t, s, []State{Queued, Blocked}, [][]string{nil, {"node-a"}})
	if _, err := s.TransitionWorkflow(ctx, id, 1, string(Cancelling), nil); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	registry := durableRegistry(t, func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
		calls.Add(1)
		return tool.Outcome{}, nil
	})
	m, err := NewDurableManager(ctx, DefaultConfig(), registry, s)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDurableManager(t, m)
	got, err := m.GetWorkflow(id)
	if err != nil || got.State != Cancelled || got.Nodes[0].State != Cancelled || got.Nodes[1].State != Cancelled || calls.Load() != 0 {
		t.Fatalf("cancel recovery: %+v calls=%d err=%v", got, calls.Load(), err)
	}
}

func TestDurableWorkflowIdempotencyBeforeCapacityAndAfterRestart(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "state")
	s := openDurableStore(t, dir)
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	registry := durableRegistry(t, func(ctx context.Context, _ json.RawMessage, _ tool.RunOptions) (tool.Outcome, error) {
		if calls.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return tool.Outcome{}, ctx.Err()
			}
		}
		return tool.Outcome{}, nil
	})
	config := DefaultConfig()
	config.Workers = 1
	config.QueueCapacity = 2
	m, err := NewDurableManager(ctx, config, registry, s)
	if err != nil {
		t.Fatal(err)
	}
	request := WorkflowRequest{Nodes: []WorkflowNode{
		{ID: "root", Tool: "test", Arguments: json.RawMessage(`{"b":2,"a":1}`)},
		{ID: "child", Tool: "test", DependsOn: []string{"root"}},
	}}
	first, err := m.SubmitWorkflowIdempotent(ctx, request, "key")
	if err != nil {
		t.Fatal(err)
	}
	<-started
	queued, err := m.SubmitIdempotent(ctx, Request{Tool: "test"}, "key") // Same key, different endpoint.
	if err != nil {
		t.Fatal(err)
	}
	request.Nodes[0].Arguments = json.RawMessage(`{ "a": 1, "b": 2 }`)
	request.Nodes[0].QueueTimeout = config.DefaultQueueTimeout
	request.Nodes[0].ExecutionTimeout = config.DefaultRunTimeout
	replay, err := m.SubmitWorkflowIdempotent(ctx, request, "key")
	if err != nil || replay.ID != first.ID {
		t.Fatalf("replay while queue full: id=%q want=%q err=%v", replay.ID, first.ID, err)
	}
	request.Nodes[0].Arguments = json.RawMessage(`{"a":9}`)
	if _, err := m.SubmitWorkflowIdempotent(ctx, request, "key"); !errors.Is(err, sqlite.ErrIdempotencyConflict) {
		t.Fatalf("different workflow under same key: %v", err)
	}
	close(release)
	awaitWorkflow(t, m, first.ID, Succeeded)
	awaitState(t, m, queued.ID, Succeeded)
	if calls.Load() != 3 {
		t.Fatalf("executor calls=%d, want 3", calls.Load())
	}
	closeDurableManager(t, m)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openDurableStore(t, dir)
	defer s.Close()
	m, err = NewDurableManager(ctx, config, registry, s)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDurableManager(t, m)
	request.Nodes[0].Arguments = json.RawMessage(`{"a":1,"b":2}`)
	replay, err = m.SubmitWorkflowIdempotent(ctx, request, "key")
	if err != nil || replay.ID != first.ID || calls.Load() != 3 {
		t.Fatalf("replay after restart: id=%q calls=%d err=%v", replay.ID, calls.Load(), err)
	}
}

func TestNormalizedWorkflowDependencyOrder(t *testing.T) {
	config := DefaultConfig()
	registry := durableRegistry(t, func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
		return tool.Outcome{}, nil
	})
	m, err := newManager(config, registry, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := WorkflowRequest{Nodes: []WorkflowNode{
		{ID: "a", Tool: "test"}, {ID: "b", Tool: "test"},
		{ID: "join", Tool: "test", DependsOn: []string{"a", "b"}},
	}}
	prepare := func() []preparedRequest {
		result := make([]preparedRequest, len(request.Nodes))
		for i, node := range request.Nodes {
			var err error
			result[i], err = m.prepareRequest(context.Background(), Request{Tool: node.Tool})
			if err != nil {
				t.Fatal(err)
			}
		}
		return result
	}
	first, err := encodeNormalizedWorkflowRequest(request, prepare())
	if err != nil {
		t.Fatal(err)
	}
	request.Nodes[2].DependsOn = []string{"b", "a"}
	second, err := encodeNormalizedWorkflowRequest(request, prepare())
	if err != nil || string(first) != string(second) {
		t.Fatalf("dependency order changed normalized request: first=%s second=%s err=%v", first, second, err)
	}
}
