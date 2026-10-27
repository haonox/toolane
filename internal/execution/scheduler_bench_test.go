package execution

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// BenchmarkTenantClaims measures scheduler work with many tenants sharing a
// backlog. Setup is outside the timed section so it reports claim cost.
func BenchmarkTenantClaims(b *testing.B) {
	const tenants = 1000
	const batch = 10000
	for done := 0; done < b.N; {
		b.StopTimer()
		count := min(batch, b.N-done)
		m := &Manager{config: DefaultConfig(), tenants: make(map[string]*tenantState)}
		m.cond = sync.NewCond(&m.mu)
		m.config.TenantRunningLimit = 1
		for i := range count {
			r := &record{snapshot: Snapshot{TenantID: fmt.Sprintf("tenant-%d", i/10%tenants)},
				deadline: time.Now().Add(time.Minute)}
			m.admitPendingLocked(r)
		}
		b.StartTimer()
		for range count {
			r := m.takeReadyLocked()
			if r == nil {
				b.Fatal("scheduler lost queued work")
			}
			m.claimSlotLocked(r)
			m.releaseRunningLocked(r)
		}
		done += count
	}
}
