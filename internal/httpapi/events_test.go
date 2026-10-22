package httpapi

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/execution"
	"github.com/haonox/toolane/internal/executor/mock"
	"github.com/haonox/toolane/internal/store/sqlite"
	"github.com/haonox/toolane/internal/tool"
)

func eventsFixture(t *testing.T) (*API, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "state")
	store, err := sqlite.Open(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	registry := tool.NewRegistry()
	if err := registry.Register(mock.Definition()); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(tool.Definition{Name: "test.emit", Validate: func(json.RawMessage) error { return nil },
		Execute: func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
			return tool.Outcome{Stdout: strings.Repeat("世", 2000), Stderr: "done", Result: json.RawMessage(`{"ok":true}`)}, nil
		}}); err != nil {
		t.Fatal(err)
	}
	manager, err := execution.NewDurableManager(context.Background(), execution.DefaultConfig(), registry, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := manager.Close(ctx); err != nil {
			t.Error(err)
		}
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	api, err := New(manager, []Token{{Value: firstToken, TenantID: "first", Tools: []string{mock.Name, "test.emit"}},
		{Value: secondToken, TenantID: "second", Tools: []string{mock.Name}}})
	if err != nil {
		t.Fatal(err)
	}
	return api, dir
}

func submitEventTask(t *testing.T, api *API, body string) string {
	t.Helper()
	got := request(t, api, "POST", "/api/v1/executions", firstToken, body, "")
	if got.Code != http.StatusAccepted {
		t.Fatalf("submit: %d %s", got.Code, got.Body.String())
	}
	var snapshot struct {
		ExecutionID string `json:"execution_id"`
	}
	if err := json.Unmarshal(got.Body.Bytes(), &snapshot); err != nil || snapshot.ExecutionID == "" {
		t.Fatalf("submitted ID: %+v %v", snapshot, err)
	}
	return snapshot.ExecutionID
}

func waitFinished(t *testing.T, api *API, id string) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		snapshot, err := api.manager.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.FinishedAt != nil {
			return
		}
		select {
		case <-deadline:
			t.Fatal("execution did not finish")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func eventRequest(api *API, id, token, cursor string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", "/api/v1/executions/"+id+"/events", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	if cursor != "" {
		r.Header.Set("Last-Event-ID", cursor)
	}
	w := httptest.NewRecorder()
	api.ServeHTTP(w, r)
	return w
}

type wireEvent struct {
	id   int64
	kind string
	data map[string]any
}

func parseEvents(t *testing.T, body string) []wireEvent {
	t.Helper()
	var events []wireEvent
	for _, frame := range strings.Split(body, "\n\n") {
		if !strings.HasPrefix(frame, "id: ") {
			continue
		}
		lines := strings.Split(frame, "\n")
		if len(lines) != 3 {
			t.Fatalf("malformed SSE frame %q", frame)
		}
		id, err := strconv.ParseInt(strings.TrimPrefix(lines[0], "id: "), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		event := wireEvent{id: id, kind: strings.TrimPrefix(lines[1], "event: ")}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[2], "data: ")), &event.data); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	return events
}

func TestSSEReplayOutputAndTenantIsolation(t *testing.T) {
	api, _ := eventsFixture(t)
	id := submitEventTask(t, api, `{"tool":"test.emit","arguments":{}}`)
	waitFinished(t, api, id)
	if got := eventRequest(api, id, secondToken, ""); got.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant stream: %d %s", got.Code, got.Body.String())
	}
	if got := eventRequest(api, id, firstToken, "bad"); got.Code != http.StatusBadRequest {
		t.Fatalf("invalid cursor: %d %s", got.Code, got.Body.String())
	}
	if got := eventRequest(api, id, firstToken, "999999"); got.Code != http.StatusBadRequest {
		t.Fatalf("future cursor: %d %s", got.Code, got.Body.String())
	}
	w := eventRequest(api, id, firstToken, "")
	if w.Code != http.StatusOK || w.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("stream: %d %s", w.Code, w.Body.String())
	}
	events := parseEvents(t, w.Body.String())
	if len(events) < 5 || events[0].kind != "state" || events[len(events)-1].kind != "end" {
		t.Fatalf("unexpected events: %+v", events)
	}
	var stdout, stderr string
	for i, event := range events {
		if event.id != int64(i+1) {
			t.Fatalf("sequence at %d: %d", i, event.id)
		}
		if event.kind == "output" {
			stream, _ := event.data["stream"].(string)
			chunk, _ := event.data["chunk"].(string)
			if len(chunk) > 4096+4 {
				t.Fatal("output chunk too large")
			}
			if stream == "stdout" {
				stdout += chunk
			} else if stream == "stderr" {
				stderr += chunk
			}
		}
	}
	if stdout != strings.Repeat("世", 2000) || stderr != "done" {
		t.Fatalf("output replay incomplete: stdout=%d stderr=%q", len(stdout), stderr)
	}
	replayed := parseEvents(t, eventRequest(api, id, firstToken, "2").Body.String())
	if len(replayed) != len(events)-2 || replayed[0].id != 3 || replayed[len(replayed)-1].kind != "end" {
		t.Fatalf("Last-Event-ID replay: %+v", replayed)
	}
}

type streamWriter struct {
	header       http.Header
	status       int
	body         bytes.Buffer
	ready        chan struct{}
	writeStarted chan struct{}
	release      chan struct{}
	readyOnce    sync.Once
	writeOnce    sync.Once
}

func newStreamWriter(block bool) *streamWriter {
	w := &streamWriter{header: make(http.Header), ready: make(chan struct{})}
	if block {
		w.writeStarted = make(chan struct{})
		w.release = make(chan struct{})
	}
	return w
}

func (w *streamWriter) Header() http.Header    { return w.header }
func (w *streamWriter) WriteHeader(status int) { w.status = status }
func (w *streamWriter) Flush()                 { w.readyOnce.Do(func() { close(w.ready) }) }
func (w *streamWriter) Write(p []byte) (int, error) {
	if w.release != nil {
		w.writeOnce.Do(func() { close(w.writeStarted) })
		<-w.release
	}
	return w.body.Write(p)
}

func TestSSELiveBoundaryAndConnectionLimit(t *testing.T) {
	api, _ := eventsFixture(t)
	api.sseOptions.TenantLimit = 1
	api.sseOptions.GlobalLimit = 1
	id := submitEventTask(t, api, `{"tool":"mock.delay","arguments":{"delay_ms":300}}`)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r := httptest.NewRequest("GET", "/api/v1/executions/"+id+"/events", nil).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer "+firstToken)
	w := newStreamWriter(false)
	done := make(chan struct{})
	go func() { defer close(done); api.ServeHTTP(w, r) }()
	select {
	case <-w.ready:
	case <-time.After(time.Second):
		t.Fatal("stream did not start")
	}
	if got := eventRequest(api, id, firstToken, ""); got.Code != http.StatusTooManyRequests {
		t.Fatalf("connection limit: %d %s", got.Code, got.Body.String())
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("live stream did not end")
	}
	events := parseEvents(t, w.body.String())
	if len(events) < 3 || events[len(events)-1].kind != "end" {
		t.Fatalf("live events: %+v", events)
	}
	for i, event := range events {
		if event.id != int64(i+1) {
			t.Fatalf("gap or duplicate at %d: %d", i, event.id)
		}
	}
	if got := eventRequest(api, id, firstToken, ""); got.Code != http.StatusOK {
		t.Fatalf("slot was not released: %d", got.Code)
	}
}

func TestSSESlowWriterDoesNotBlockWorker(t *testing.T) {
	api, _ := eventsFixture(t)
	id := submitEventTask(t, api, `{"tool":"mock.delay","arguments":{"delay_ms":100}}`)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r := httptest.NewRequest("GET", "/api/v1/executions/"+id+"/events", nil).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer "+firstToken)
	w := newStreamWriter(true)
	defer func() {
		select {
		case <-w.release:
		default:
			close(w.release)
		}
	}()
	done := make(chan struct{})
	go func() { defer close(done); api.ServeHTTP(w, r) }()
	select {
	case <-w.writeStarted:
	case <-time.After(time.Second):
		t.Fatal("writer did not block")
	}
	waitFinished(t, api, id)
	close(w.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stream did not drain")
	}
	if got := parseEvents(t, w.body.String()); len(got) < 3 || got[len(got)-1].kind != "end" {
		t.Fatalf("slow writer recovery: %+v", got)
	}
}

func TestSSEExpiredHistoryHandshake(t *testing.T) {
	api, dir := eventsFixture(t)
	id := submitEventTask(t, api, `{"tool":"mock.delay","arguments":{"delay_ms":1}}`)
	waitFinished(t, api, id)
	db, err := sql.Open("sqlite", filepath.Join(dir, "toolane.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`DELETE FROM execution_events WHERE execution_id = ? AND sequence = 1`, id); err != nil {
		t.Fatal(err)
	}
	got := eventRequest(api, id, firstToken, "")
	if got.Code != http.StatusGone || !strings.Contains(got.Body.String(), `"event_history_expired"`) {
		t.Fatalf("expired history: %d %s", got.Code, got.Body.String())
	}
	if got := eventRequest(api, id, firstToken, "1"); got.Code != http.StatusOK {
		t.Fatalf("resumable cursor: %d %s", got.Code, got.Body.String())
	}
	if _, err := db.Exec(`DELETE FROM execution_events WHERE execution_id = ? AND sequence = 2`, id); err != nil {
		t.Fatal(err)
	}
	if got := eventRequest(api, id, firstToken, "1"); got.Code != http.StatusGone {
		t.Fatalf("middle history gap: %d %s", got.Code, got.Body.String())
	}
}
