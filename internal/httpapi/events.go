package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/haonox/toolane/internal/store/sqlite"
)

const eventReplayBatch = 128

// SSEOptions bounds live streams. Zero is invalid so server configuration can
// apply explicit defaults before constructing the API.
type SSEOptions struct {
	TenantLimit       int
	GlobalLimit       int
	WriteTimeout      time.Duration
	HeartbeatInterval time.Duration
}

func DefaultSSEOptions() SSEOptions {
	return SSEOptions{TenantLimit: 8, GlobalLimit: 64, WriteTimeout: 5 * time.Second, HeartbeatInterval: 15 * time.Second}
}

func (o SSEOptions) validate() error {
	if o.TenantLimit < 1 || o.GlobalLimit < o.TenantLimit || o.WriteTimeout <= 0 || o.HeartbeatInterval <= 0 {
		return errors.New("invalid SSE options")
	}
	return nil
}

func (a *API) reserveSSE(tenant string) (func(), bool) {
	a.sseMu.Lock()
	defer a.sseMu.Unlock()
	if a.sseTotal >= a.sseOptions.GlobalLimit || a.sseTenants[tenant] >= a.sseOptions.TenantLimit {
		return nil, false
	}
	a.sseTotal++
	a.sseTenants[tenant]++
	return func() {
		a.sseMu.Lock()
		defer a.sseMu.Unlock()
		a.sseTotal--
		a.sseTenants[tenant]--
		if a.sseTenants[tenant] == 0 {
			delete(a.sseTenants, tenant)
		}
	}, true
}

func lastEventID(r *http.Request) (int64, error) {
	values := r.Header.Values("Last-Event-ID")
	if len(values) == 0 {
		return 0, nil
	}
	if len(values) != 1 || len(values[0]) == 0 || len(values[0]) > 19 {
		return 0, errors.New("invalid Last-Event-ID")
	}
	for _, c := range values[0] {
		if c < '0' || c > '9' {
			return 0, errors.New("invalid Last-Event-ID")
		}
	}
	return strconv.ParseInt(values[0], 10, 64)
}

func (a *API) streamExecution(w http.ResponseWriter, r *http.Request, p principal) {
	id := r.PathValue("id")
	snapshot, err := a.manager.Get(id)
	if err != nil {
		a.managerError(w, r, err)
		return
	}
	if snapshot.TenantID != p.tenantID {
		writeError(w, r, http.StatusNotFound, "not_found", "execution not found")
		return
	}
	if trace := traceOf(r); trace != nil {
		trace.executionID = snapshot.ID
	}
	after, err := lastEventID(r)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_event_id", "invalid Last-Event-ID")
		return
	}
	release, ok := a.reserveSSE(p.tenantID)
	if !ok {
		writeError(w, r, http.StatusTooManyRequests, "sse_limit_exceeded", "too many event connections")
		return
	}
	defer release()
	// Subscribe first. A transition committed before this point is in the
	// replay; one committed after it wakes the coalescing notification.
	wake, unsubscribe, err := a.manager.SubscribeEvents(id)
	if err != nil {
		a.managerError(w, r, err)
		return
	}
	defer unsubscribe()
	first, last, remaining, err := a.manager.EventBounds(r.Context(), id, after)
	if err != nil {
		a.managerError(w, r, err)
		return
	}
	if first == 0 || after < first-1 {
		writeError(w, r, http.StatusGone, "event_history_expired", "event history is no longer available; query the execution")
		return
	}
	if after > last {
		writeError(w, r, http.StatusBadRequest, "invalid_event_id", "Last-Event-ID is ahead of this execution")
		return
	}
	if remaining != last-after {
		writeError(w, r, http.StatusGone, "event_history_expired", "event history is no longer available; query the execution")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	controller := http.NewResponseController(w)
	if err := flushSSE(controller, a.sseOptions.WriteTimeout); err != nil {
		return
	}
	ticker := time.NewTicker(a.sseOptions.HeartbeatInterval)
	defer ticker.Stop()
	for {
		events, err := a.manager.EventsAfterLimit(r.Context(), id, after, eventReplayBatch)
		if err != nil {
			return
		}
		for _, event := range events {
			if event.Sequence != after+1 || writeSSE(w, controller, event, a.sseOptions.WriteTimeout) != nil {
				return
			}
			after = event.Sequence
			if event.Kind == "end" {
				return
			}
		}
		if len(events) == eventReplayBatch {
			continue
		}
		// Older records may finish with a state event instead of "end".
		latest, err := a.manager.Get(id)
		if err != nil {
			return
		}
		if latest.FinishedAt != nil {
			_, last, _, err := a.manager.EventBounds(r.Context(), id, after)
			if err != nil {
				return
			}
			if last > after {
				continue
			}
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-wake:
		case <-ticker.C:
			if err := writeHeartbeat(w, controller, a.sseOptions.WriteTimeout); err != nil {
				return
			}
		}
	}
}

func flushSSE(controller *http.ResponseController, timeout time.Duration) error {
	if err := controller.SetWriteDeadline(time.Now().Add(timeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return controller.Flush()
}

func writeSSE(w http.ResponseWriter, controller *http.ResponseController, event sqlite.Event, timeout time.Duration) error {
	var compact bytes.Buffer
	if err := json.Compact(&compact, event.Payload); err != nil {
		return err
	}
	if err := controller.SetWriteDeadline(time.Now().Add(timeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", event.Sequence, event.Kind, compact.Bytes()); err != nil {
		return err
	}
	return controller.Flush()
}

func writeHeartbeat(w http.ResponseWriter, controller *http.ResponseController, timeout time.Duration) error {
	if err := controller.SetWriteDeadline(time.Now().Add(timeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	if _, err := io.WriteString(w, ": keepalive\n\n"); err != nil {
		return err
	}
	return controller.Flush()
}
