package execution

import (
	"context"
	"encoding/json"
	"time"
	"unicode/utf8"

	"github.com/haonox/toolane/internal/store/sqlite"
)

const (
	maxOutputEvents  = 1024
	outputChunkBytes = 4096
)

// SubscribeEvents registers a one-slot, coalescing notification before the
// caller replays SQLite history. Notifications never carry event data: a slow
// reader always catches up from the durable sequence instead.
func (m *Manager) SubscribeEvents(id string) (<-chan struct{}, func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := make(chan struct{}, 1)
	if m.eventWatchers[id] == nil {
		m.eventWatchers[id] = make(map[chan struct{}]struct{})
	}
	m.eventWatchers[id][ch] = struct{}{}
	return ch, func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		delete(m.eventWatchers[id], ch)
		if len(m.eventWatchers[id]) == 0 {
			delete(m.eventWatchers, id)
		}
	}
}

func (m *Manager) notifyEventsLocked(id string) {
	for ch := range m.eventWatchers[id] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// EventBounds and EventsAfterLimit expose only persisted events. HTTP checks
// tenant ownership before calling either method.
func (m *Manager) EventBounds(ctx context.Context, id string, after int64) (int64, int64, int64, error) {
	return m.store.EventBounds(ctx, id, after)
}

func (m *Manager) EventsAfterLimit(ctx context.Context, id string, after int64, limit int) ([]sqlite.Event, error) {
	return m.store.EventsAfterLimit(ctx, id, after, limit)
}

// outputEventsLocked prepares at most 1024 UTF-8-safe chunks. The next state
// transition writes them atomically with the resulting snapshot.
func (m *Manager) outputEventsLocked(stdout, stderr string) ([]sqlite.Event, error) {
	events := make([]sqlite.Event, 0)
	for _, stream := range []struct{ name, data string }{{"stdout", stdout}, {"stderr", stderr}} {
		for len(stream.data) > 0 && len(events) < maxOutputEvents {
			end := min(len(stream.data), outputChunkBytes)
			for end < len(stream.data) && !utf8.RuneStart(stream.data[end]) {
				end--
			}
			at := time.Now().UTC()
			payload, err := json.Marshal(struct {
				Stream string    `json:"stream"`
				Chunk  string    `json:"chunk"`
				At     time.Time `json:"at"`
			}{stream.name, stream.data[:end], at})
			if err != nil {
				return nil, err
			}
			events = append(events, sqlite.Event{Kind: "output", Payload: payload, OccurredAt: at})
			stream.data = stream.data[end:]
		}
	}
	return events, nil
}
