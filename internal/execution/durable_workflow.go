package execution

import (
	"fmt"
	"time"

	"github.com/haonox/toolane/internal/store/sqlite"
)

// recoverWorkflows reconstructs the graph before any worker can claim work.
// A tool that was running at the time of a crash still has an unknown result.
func (m *Manager) recoverWorkflows(stored []sqlite.Workflow) (map[string]bool, error) {
	claimed := make(map[string]bool)
	for _, row := range stored {
		w := &workflow{
			id: row.ID, tenantID: row.TenantID, state: State(row.State),
			createdAt: row.CreatedAt, finishedAt: row.FinishedAt, remaining: len(row.Nodes),
			cancelled: row.State == string(Cancelling) || row.State == string(Cancelled), version: row.Version,
			nodes: make([]*record, len(row.Nodes)),
		}
		if w.state != Running && w.state != Cancelling && w.state != Succeeded && w.state != Failed && w.state != Cancelled {
			return nil, fmt.Errorf("workflow %s has unsupported state %q", w.id, w.state)
		}
		index := make(map[string]*record, len(row.Nodes))
		for i, node := range row.Nodes {
			r, err := decodeStoredExecution(node.Execution)
			if err != nil {
				return nil, err
			}
			if r.snapshot.WorkflowID != w.id || r.snapshot.NodeID != node.NodeID || r.snapshot.TenantID != w.tenantID || !r.snapshot.CreatedAt.Equal(w.createdAt) || claimed[r.snapshot.ID] || index[node.NodeID] != nil {
				return nil, fmt.Errorf("workflow %s has inconsistent node %q", w.id, node.NodeID)
			}
			if !validRecoveredNodeState(r.snapshot) {
				return nil, fmt.Errorf("workflow %s node %q has inconsistent state %q", w.id, node.NodeID, r.snapshot.State)
			}
			if r.snapshot.FinishedAt == nil {
				if _, ok := m.tools[r.snapshot.Tool]; !ok {
					return nil, fmt.Errorf("recovered workflow node %s uses unregistered tool %q", r.snapshot.ID, r.snapshot.Tool)
				}
			}
			r.workflow = w
			w.nodes[i] = r
			index[node.NodeID] = r
			claimed[r.snapshot.ID] = true
		}
		for i, node := range row.Nodes {
			seen := make(map[string]bool, len(node.DependsOn))
			for _, name := range node.DependsOn {
				parent := index[name]
				if parent == nil || parent == w.nodes[i] || seen[name] {
					return nil, fmt.Errorf("workflow %s has invalid dependency %q for node %q", w.id, name, node.NodeID)
				}
				seen[name] = true
				w.nodes[i].dependsOn = append(w.nodes[i].dependsOn, parent)
				parent.dependents = append(parent.dependents, w.nodes[i])
			}
		}
		colors := make(map[*record]uint8, len(w.nodes))
		var visit func(*record) bool
		visit = func(r *record) bool {
			if colors[r] == 1 {
				return false
			}
			if colors[r] == 2 {
				return true
			}
			colors[r] = 1
			for _, parent := range r.dependsOn {
				if !visit(parent) {
					return false
				}
			}
			colors[r] = 2
			return true
		}
		for _, r := range w.nodes {
			if !visit(r) {
				return nil, fmt.Errorf("workflow %s has a dependency cycle", w.id)
			}
		}
		m.workflows[w.id] = w
		for _, r := range w.nodes {
			m.records[r.snapshot.ID] = r
			if r.snapshot.State == Queued || r.snapshot.State == Blocked {
				if m.pending >= m.config.QueueCapacity {
					return nil, fmt.Errorf("recovered workflow %s exceeds queue capacity", w.id)
				}
				if tenant := m.tenants[w.tenantID]; tenant != nil && tenant.pending >= m.config.TenantPendingLimit {
					return nil, fmt.Errorf("recovered workflow %s exceeds tenant capacity", w.id)
				}
				m.admitPendingLocked(r)
				if r.snapshot.State == Queued {
					m.queue = append(m.queue, r)
				}
			}
		}
		// A crash can land after a parent state commit and before its child
		// becomes ready or skipped. Replaying terminal parents repairs that gap.
		for _, r := range w.nodes {
			if r.snapshot.FinishedAt != nil {
				if err := m.workflowNodeFinishedLocked(r); err != nil {
					return nil, err
				}
			}
		}
		for _, r := range w.nodes {
			switch r.snapshot.State {
			case Blocked, Queued:
				if w.cancelled || !time.Now().Before(r.deadline) {
					wasQueued := r.snapshot.State == Queued
					state, code, message := TimedOut, "queue_timeout", "queue wait expired during restart"
					if w.cancelled {
						state, code, message = Cancelled, "cancelled", "workflow cancelled during restart"
					}
					if err := m.finishLocked(r, state, code, message); err != nil {
						return nil, err
					}
					if wasQueued {
						m.removeQueued(r)
					}
					m.releasePendingLocked(r)
				}
			case Running, Cancelling:
				if err := m.finishLocked(r, Interrupted, "service_restarted", "execution result is unknown after restart"); err != nil {
					return nil, err
				}
			}
		}
	}
	return claimed, nil
}

func validRecoveredNodeState(snapshot Snapshot) bool {
	switch snapshot.State {
	case Blocked, Queued, Running, Cancelling:
		return snapshot.FinishedAt == nil
	case Succeeded, Failed, TimedOut, Cancelled, Skipped, Interrupted:
		return snapshot.FinishedAt != nil
	default:
		return false
	}
}
