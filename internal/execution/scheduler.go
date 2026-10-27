package execution

import (
	"container/heap"
	"container/list"
)

// tenantState and every scheduler index are owned by Manager.mu. Only tenants
// with pending or running work remain in the map.
type tenantState struct {
	id        string
	pending   int
	running   int
	queue     list.List
	readyElem *list.Element
}

// deadlineHeap contains queued records only. The index permits O(log n)
// removal when a task is claimed or cancelled before its deadline.
type deadlineHeap []*record

func (h deadlineHeap) Len() int { return len(h) }
func (h deadlineHeap) Less(i, j int) bool {
	return h[i].deadline.Before(h[j].deadline)
}
func (h deadlineHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].deadlineIndex, h[j].deadlineIndex = i, j
}
func (h *deadlineHeap) Push(value any) {
	r := value.(*record)
	r.deadlineIndex = len(*h)
	*h = append(*h, r)
}
func (h *deadlineHeap) Pop() any {
	old := *h
	r := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	r.deadlineIndex = -1
	return r
}

func (m *Manager) tenantLocked(id string) *tenantState {
	if state := m.tenants[id]; state != nil {
		return state
	}
	state := &tenantState{id: id}
	m.tenants[id] = state
	return state
}

func (m *Manager) syncReadyLocked(state *tenantState) {
	eligible := state.queue.Len() > 0 && state.running < m.config.TenantRunningLimit
	if eligible && state.readyElem == nil {
		state.readyElem = m.readyTenants.PushBack(state)
	} else if !eligible && state.readyElem != nil {
		m.readyTenants.Remove(state.readyElem)
		state.readyElem = nil
	}
}

func (m *Manager) admitPendingLocked(r *record) {
	r.tenant = m.tenantLocked(r.snapshot.TenantID)
	r.tenant.pending++
	m.pending++
	r.queueElem = r.tenant.queue.PushBack(r)
	heap.Push(&m.deadlines, r)
	m.syncReadyLocked(r.tenant)
}

func (m *Manager) requeueFrontLocked(r *record) {
	r.queueElem = r.tenant.queue.PushFront(r)
	heap.Push(&m.deadlines, r)
	m.syncReadyLocked(r.tenant)
}

func (m *Manager) removeQueued(r *record) {
	if r.queueElem == nil {
		return
	}
	r.tenant.queue.Remove(r.queueElem)
	r.queueElem = nil
	heap.Remove(&m.deadlines, r.deadlineIndex)
	m.syncReadyLocked(r.tenant)
}

func (m *Manager) releasePendingLocked(r *record) {
	r.tenant.pending--
	m.pending--
	m.pruneTenantLocked(r.tenant)
}

func (m *Manager) claimSlotLocked(r *record) {
	r.tenant.pending--
	m.pending--
	r.tenant.running++
	m.running++
	m.syncReadyLocked(r.tenant)
}

func (m *Manager) releaseRunningLocked(r *record) {
	r.tenant.running--
	m.running--
	m.syncReadyLocked(r.tenant)
	m.pruneTenantLocked(r.tenant)
	// A ready tenant that was at its running limit can now be selected.
	m.cond.Broadcast()
}

func (m *Manager) pruneTenantLocked(state *tenantState) {
	if state.pending != 0 || state.running != 0 {
		return
	}
	if state.readyElem != nil {
		m.readyTenants.Remove(state.readyElem)
		state.readyElem = nil
	}
	delete(m.tenants, state.id)
}

// takeReadyLocked claims the oldest task of the next eligible tenant, then
// moves that tenant behind its peers if it still has runnable work.
func (m *Manager) takeReadyLocked() *record {
	front := m.readyTenants.Front()
	if front == nil {
		return nil
	}
	state := front.Value.(*tenantState)
	m.readyTenants.Remove(front)
	state.readyElem = nil
	r := state.queue.Front().Value.(*record)
	m.removeQueued(r)
	return r
}
