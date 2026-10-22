package execution

// tenantState is owned by Manager.mu. Only tenants with pending or running work
// are retained, so a stream of distinct tenant IDs cannot grow this map forever.
type tenantState struct {
	id      string
	pending int
	running int
}

func (m *Manager) tenantLocked(id string) *tenantState {
	if state := m.tenants[id]; state != nil {
		return state
	}
	state := &tenantState{id: id}
	m.tenants[id] = state
	m.tenantOrder = append(m.tenantOrder, id)
	return state
}

func (m *Manager) admitPendingLocked(r *record) {
	r.tenant = m.tenantLocked(r.snapshot.TenantID)
	r.tenant.pending++
	m.pending++
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
}

func (m *Manager) releaseRunningLocked(r *record) {
	r.tenant.running--
	m.running--
	m.pruneTenantLocked(r.tenant)
	// A ready tenant that was at its running limit can now be selected.
	m.cond.Broadcast()
}

func (m *Manager) pruneTenantLocked(state *tenantState) {
	if state.pending != 0 || state.running != 0 {
		return
	}
	delete(m.tenants, state.id)
	for i, id := range m.tenantOrder {
		if id != state.id {
			continue
		}
		copy(m.tenantOrder[i:], m.tenantOrder[i+1:])
		m.tenantOrder[len(m.tenantOrder)-1] = ""
		m.tenantOrder = m.tenantOrder[:len(m.tenantOrder)-1]
		if i < m.rrCursor {
			m.rrCursor--
		}
		if m.rrCursor >= len(m.tenantOrder) {
			m.rrCursor = 0
		}
		return
	}
}

// takeReadyLocked returns the oldest eligible task under FIFO or the oldest
// task for the next eligible tenant under round robin. It leaves blocked nodes,
// tenants at their running limit untouched.
func (m *Manager) takeReadyLocked() *record {
	if m.config.Scheduler == SchedulerFIFO {
		for i, r := range m.queue {
			if m.canClaimLocked(r) {
				return m.takeQueuedAtLocked(i)
			}
		}
		return nil
	}
	for offset := 0; offset < len(m.tenantOrder); offset++ {
		index := (m.rrCursor + offset) % len(m.tenantOrder)
		state := m.tenants[m.tenantOrder[index]]
		if state.running >= m.config.TenantRunningLimit {
			continue
		}
		for i, r := range m.queue {
			if r.tenant == state && m.canClaimLocked(r) {
				m.rrCursor = (index + 1) % len(m.tenantOrder)
				return m.takeQueuedAtLocked(i)
			}
		}
	}
	return nil
}

func (m *Manager) canClaimLocked(r *record) bool {
	return r.tenant.running < m.config.TenantRunningLimit
}

func (m *Manager) takeQueuedAtLocked(i int) *record {
	r := m.queue[i]
	copy(m.queue[i:], m.queue[i+1:])
	m.queue[len(m.queue)-1] = nil
	m.queue = m.queue[:len(m.queue)-1]
	return r
}
