package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// WorkflowNode describes one statically parameterized execution and its direct
// dependencies. Node IDs are unique within a workflow.
type WorkflowNode struct {
	ID               string          `json:"node_id"`
	DependsOn        []string        `json:"depends_on,omitempty"`
	AgentID          string          `json:"agent_id,omitempty"`
	Tool             string          `json:"tool"`
	Arguments        json.RawMessage `json:"arguments"`
	QueueTimeout     time.Duration   `json:"queue_timeout,omitempty"`
	ExecutionTimeout time.Duration   `json:"execution_timeout,omitempty"`
	OutputLimitBytes int             `json:"output_limit_bytes,omitempty"`
}

// WorkflowRequest submits a complete dependency graph. Arguments are fixed at
// submission and never read from upstream results.
type WorkflowRequest struct {
	TenantID string         `json:"tenant_id,omitempty"`
	Nodes    []WorkflowNode `json:"nodes"`
}

// WorkflowSnapshot reports the workflow summary and nodes in submission order.
type WorkflowSnapshot struct {
	ID         string     `json:"workflow_id"`
	TenantID   string     `json:"tenant_id"`
	State      State      `json:"status"`
	Nodes      []Snapshot `json:"nodes"`
	CreatedAt  time.Time  `json:"created_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

type workflow struct {
	id         string
	tenantID   string
	state      State
	nodes      []*record
	createdAt  time.Time
	finishedAt *time.Time
	remaining  int
	cancelled  bool
}

type terminalGroup struct {
	ids        []string
	workflowID string
}

// SubmitWorkflow validates all nodes before atomically accepting the batch.
// Blocked nodes count toward the same pending capacity as ready nodes.
func (m *Manager) SubmitWorkflow(ctx context.Context, req WorkflowRequest) (WorkflowSnapshot, error) {
	dependencies, err := m.validateWorkflow(req)
	if err != nil {
		return WorkflowSnapshot{}, err
	}
	prepared := make([]preparedRequest, len(req.Nodes))
	ids := make([]string, len(req.Nodes))
	for i, node := range req.Nodes {
		prepared[i], err = m.prepareRequest(ctx, Request{
			TenantID: req.TenantID, AgentID: node.AgentID, Tool: node.Tool, Arguments: node.Arguments,
			QueueTimeout: node.QueueTimeout, ExecutionTimeout: node.ExecutionTimeout,
			OutputLimitBytes: node.OutputLimitBytes,
		})
		if err != nil {
			return WorkflowSnapshot{}, fmt.Errorf("node %q: %w", node.ID, err)
		}
		ids[i], err = newID()
		if err != nil {
			return WorkflowSnapshot{}, err
		}
	}
	workflowID, err := newID()
	if err != nil {
		return WorkflowSnapshot{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.admissionError(ctx); err != nil {
		return WorkflowSnapshot{}, err
	}
	if len(req.Nodes) > m.config.QueueCapacity-m.pending {
		return WorkflowSnapshot{}, ErrQueueFull
	}
	tenantID := prepared[0].request.TenantID
	if state := m.tenants[tenantID]; state != nil {
		if len(req.Nodes) > m.config.TenantPendingLimit-state.pending {
			return WorkflowSnapshot{}, ErrTenantQueueFull
		}
	} else if len(req.Nodes) > m.config.TenantPendingLimit {
		return WorkflowSnapshot{}, ErrTenantQueueFull
	}
	now := time.Now().UTC()
	w := &workflow{id: workflowID, tenantID: tenantID, state: Running, createdAt: now, remaining: len(req.Nodes)}
	w.nodes = make([]*record, len(req.Nodes))
	for i, node := range req.Nodes {
		r := prepared[i].makeRecord(ids[i], workflowID, node.ID, now, len(dependencies[i]) == 0)
		r.workflow = w
		w.nodes[i] = r
	}
	for i, deps := range dependencies {
		for _, parent := range deps {
			w.nodes[i].dependsOn = append(w.nodes[i].dependsOn, w.nodes[parent])
			w.nodes[parent].dependents = append(w.nodes[parent].dependents, w.nodes[i])
		}
	}
	m.workflows[workflowID] = w
	for _, r := range w.nodes {
		m.records[r.snapshot.ID] = r
		m.admitPendingLocked(r)
		if r.snapshot.State == Queued {
			m.queue = append(m.queue, r)
		}
	}
	m.cond.Broadcast()
	m.wakeExpiry()
	return snapshotWorkflow(w), nil
}

func (m *Manager) validateWorkflow(req WorkflowRequest) ([][]int, error) {
	if len(req.Nodes) == 0 || len(req.Nodes) > m.config.MaxWorkflowNodes {
		return nil, fmt.Errorf("workflow must contain 1 to %d nodes", m.config.MaxWorkflowNodes)
	}
	// Bound raw input before JSON encoding can expand escaped strings.
	size := len(req.TenantID)
	if size > m.config.MaxTenantIDBytes {
		return nil, ErrTenantIDTooLarge
	}
	if size > m.config.MaxWorkflowBytes {
		return nil, errors.New("workflow request exceeds size limit")
	}
	addSize := func(length int) error {
		if length > m.config.MaxWorkflowBytes-size {
			return errors.New("workflow request exceeds size limit")
		}
		size += length
		return nil
	}
	for _, node := range req.Nodes {
		if len(node.DependsOn) >= len(req.Nodes) {
			return nil, fmt.Errorf("node %q has too many dependencies", node.ID)
		}
		for _, length := range []int{len(node.ID), len(node.AgentID), len(node.Tool), len(node.Arguments)} {
			if err := addSize(length); err != nil {
				return nil, err
			}
		}
		for _, name := range node.DependsOn {
			if err := addSize(len(name)); err != nil {
				return nil, err
			}
		}
	}
	encoded, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode workflow request: %w", err)
	}
	if len(encoded) > m.config.MaxWorkflowBytes {
		return nil, errors.New("workflow request exceeds size limit")
	}
	index := make(map[string]int, len(req.Nodes))
	for i, node := range req.Nodes {
		if node.ID == "" {
			return nil, errors.New("workflow node ID is required")
		}
		if _, exists := index[node.ID]; exists {
			return nil, fmt.Errorf("duplicate workflow node ID %q", node.ID)
		}
		index[node.ID] = i
	}
	dependencies := make([][]int, len(req.Nodes))
	for i, node := range req.Nodes {
		seen := make(map[int]bool, len(node.DependsOn))
		for _, name := range node.DependsOn {
			parent, exists := index[name]
			if !exists {
				return nil, fmt.Errorf("node %q has missing dependency %q", node.ID, name)
			}
			if parent == i {
				return nil, fmt.Errorf("node %q depends on itself", node.ID)
			}
			if seen[parent] {
				return nil, fmt.Errorf("node %q repeats dependency %q", node.ID, name)
			}
			seen[parent] = true
			dependencies[i] = append(dependencies[i], parent)
		}
	}
	colors := make([]uint8, len(req.Nodes))
	var visit func(int) bool
	visit = func(i int) bool {
		if colors[i] == 1 {
			return false
		}
		if colors[i] == 2 {
			return true
		}
		colors[i] = 1
		for _, parent := range dependencies[i] {
			if !visit(parent) {
				return false
			}
		}
		colors[i] = 2
		return true
	}
	for i := range req.Nodes {
		if !visit(i) {
			return nil, errors.New("workflow contains a dependency cycle")
		}
	}
	return dependencies, nil
}

// GetWorkflow returns a copied view of a retained workflow.
func (m *Manager) GetWorkflow(id string) (WorkflowSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w, ok := m.workflows[id]
	if !ok {
		return WorkflowSnapshot{}, ErrNotFound
	}
	return snapshotWorkflow(w), nil
}

// CancelWorkflow atomically stops all unstarted nodes and requests that running
// nodes stop. The returned cancelling state does not confirm process shutdown.
func (m *Manager) CancelWorkflow(id string) (WorkflowSnapshot, error) {
	m.mu.Lock()
	w, ok := m.workflows[id]
	if !ok {
		m.mu.Unlock()
		return WorkflowSnapshot{}, ErrNotFound
	}
	var cancels []context.CancelFunc
	if w.remaining > 0 && !w.cancelled {
		w.cancelled = true
		w.state = Cancelling
		for _, r := range w.nodes {
			switch r.snapshot.State {
			case Blocked:
				m.releasePendingLocked(r)
				m.finishLocked(r, Cancelled, "cancelled", "workflow cancelled before execution")
			case Queued:
				m.removeQueued(r)
				m.releasePendingLocked(r)
				m.finishLocked(r, Cancelled, "cancelled", "workflow cancelled before execution")
			case Running:
				r.snapshot.State = Cancelling
				r.snapshot.StopReason = "user_cancelled"
				cancels = append(cancels, r.cancel)
			}
		}
		m.wakeExpiry()
	}
	snapshot := snapshotWorkflow(w)
	m.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	return snapshot, nil
}

func snapshotWorkflow(w *workflow) WorkflowSnapshot {
	s := WorkflowSnapshot{ID: w.id, TenantID: w.tenantID, State: w.state, CreatedAt: w.createdAt, Nodes: make([]Snapshot, len(w.nodes))}
	if w.finishedAt != nil {
		at := *w.finishedAt
		s.FinishedAt = &at
	}
	for i, r := range w.nodes {
		s.Nodes[i] = cloneSnapshot(r.snapshot)
	}
	return s
}

func (m *Manager) workflowNodeFinishedLocked(r *record) {
	w := r.workflow
	w.remaining--
	if !w.cancelled {
		for _, dependent := range r.dependents {
			if dependent.snapshot.State != Blocked {
				continue
			}
			if r.snapshot.State != Succeeded {
				m.releasePendingLocked(dependent)
				m.finishLocked(dependent, Skipped, "dependency_failed", "dependency did not succeed")
				continue
			}
			ready := true
			for _, parent := range dependent.dependsOn {
				if parent.snapshot.State != Succeeded {
					ready = false
					break
				}
			}
			if ready {
				now := time.Now().UTC()
				dependent.snapshot.State = Queued
				dependent.snapshot.ReadyAt = &now
				m.queue = append(m.queue, dependent)
				m.cond.Signal()
				m.wakeExpiry()
			}
		}
	}
	if w.remaining != 0 {
		return
	}
	if w.cancelled {
		w.state = Cancelled
	} else {
		w.state = Succeeded
		for _, node := range w.nodes {
			if node.snapshot.State != Succeeded {
				w.state = Failed
				break
			}
		}
	}
	now := time.Now().UTC()
	w.finishedAt = &now
	group := terminalGroup{workflowID: w.id, ids: make([]string, len(w.nodes))}
	for i, node := range w.nodes {
		group.ids[i] = node.snapshot.ID
	}
	m.retainTerminalLocked(group)
}

func (m *Manager) retainTerminalLocked(group terminalGroup) {
	m.terminal = append(m.terminal, group)
	m.terminalCount += len(group.ids)
	for m.terminalCount > m.config.RetainedResults {
		oldest := m.terminal[0]
		m.terminal[0] = terminalGroup{}
		m.terminal = m.terminal[1:]
		m.terminalCount -= len(oldest.ids)
		for _, id := range oldest.ids {
			delete(m.records, id)
		}
		if oldest.workflowID != "" {
			delete(m.workflows, oldest.workflowID)
		}
	}
}
