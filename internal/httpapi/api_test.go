package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/execution"
	"github.com/haonox/toolane/internal/executor/mock"
	"github.com/haonox/toolane/internal/store/sqlite"
	"github.com/haonox/toolane/internal/tool"
)

const (
	firstToken  = "first-tenant-secret-123456"
	secondToken = "second-tenant-secret-12345"
)

func apiFixture(t *testing.T) *API {
	t.Helper()
	ctx := context.Background()
	store, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	registry := tool.NewRegistry()
	if err := registry.Register(mock.Definition()); err != nil {
		t.Fatal(err)
	}
	manager, err := execution.NewDurableManager(ctx, execution.DefaultConfig(), registry, store, func(context.Context, []execution.RecoveryResource) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := manager.Close(closeCtx); err != nil {
			t.Error(err)
		}
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	api, err := New(manager, []Token{
		{Value: firstToken, TenantID: "first", Tools: []string{mock.Name}},
		{Value: secondToken, TenantID: "second", Tools: []string{mock.Name}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return api
}

func request(t *testing.T, api *API, method, path, token, body, key string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	w := httptest.NewRecorder()
	api.ServeHTTP(w, r)
	if w.Header().Get("X-Request-ID") == "" {
		t.Fatal("missing request ID")
	}
	return w
}

func TestHTTPAuthorizationAndTenantIsolation(t *testing.T) {
	api := apiFixture(t)
	if got := request(t, api, "GET", "/healthz", "", "", ""); got.Code != http.StatusOK {
		t.Fatalf("health: %d %s", got.Code, got.Body.String())
	}
	if got := request(t, api, "GET", "/readyz", "", "", ""); got.Code != http.StatusOK {
		t.Fatalf("ready: %d %s", got.Code, got.Body.String())
	}
	for _, token := range []string{"", "wrong-secret-123456"} {
		got := request(t, api, "POST", "/api/v1/executions", token, `{"tool":"mock.delay"}`, "")
		if got.Code != http.StatusUnauthorized || !strings.Contains(got.Body.String(), `"request_id"`) {
			t.Fatalf("unauthorized: %d %s", got.Code, got.Body.String())
		}
	}
	spoof := request(t, api, "POST", "/api/v1/executions", firstToken, `{"tenant_id":"second","tool":"mock.delay","arguments":{"delay_ms":1}}`, "")
	if spoof.Code != http.StatusBadRequest {
		t.Fatalf("tenant spoof: %d %s", spoof.Code, spoof.Body.String())
	}
	forbidden := request(t, api, "POST", "/api/v1/executions", firstToken, `{"tool":"process.printf","arguments":{}}`, "")
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("tool allowlist: %d %s", forbidden.Code, forbidden.Body.String())
	}
	accepted := request(t, api, "POST", "/api/v1/executions", firstToken, `{"agent_id":"demo","tool":"mock.delay","arguments":{"delay_ms":500}}`, "")
	if accepted.Code != http.StatusAccepted {
		t.Fatalf("submit: %d %s", accepted.Code, accepted.Body.String())
	}
	var result struct {
		ExecutionID string `json:"execution_id"`
		TenantID    string `json:"tenant_id"`
		Limits      struct {
			QueueTimeoutMS int64 `json:"queue_timeout_ms"`
		} `json:"limits"`
	}
	if err := json.Unmarshal(accepted.Body.Bytes(), &result); err != nil || result.ExecutionID == "" || result.TenantID != "first" || result.Limits.QueueTimeoutMS != 30000 {
		t.Fatalf("accepted response: %+v err=%v", result, err)
	}
	path := "/api/v1/executions/" + result.ExecutionID
	for _, methodPath := range []struct{ method, path string }{{"GET", path}, {"POST", path + "/cancel"}} {
		got := request(t, api, methodPath.method, methodPath.path, secondToken, "", "")
		if got.Code != http.StatusNotFound {
			t.Fatalf("cross-tenant execution %s %s: %d %s", methodPath.method, methodPath.path, got.Code, got.Body.String())
		}
	}
	if got := request(t, api, "GET", path, firstToken, "", ""); got.Code != http.StatusOK {
		t.Fatalf("same-tenant get: %d %s", got.Code, got.Body.String())
	}
	if got := request(t, api, "POST", path+"/cancel", firstToken, "", ""); got.Code != http.StatusOK && got.Code != http.StatusAccepted {
		t.Fatalf("same-tenant cancel: %d %s", got.Code, got.Body.String())
	}
}

func TestHTTPWorkflowIsolationAndIdempotency(t *testing.T) {
	api := apiFixture(t)
	body := `{"nodes":[{"node_id":"a","tool":"mock.delay","arguments":{"delay_ms":1}},{"node_id":"b","depends_on":["a"],"tool":"mock.delay","arguments":{"delay_ms":1}}]}`
	first := request(t, api, "POST", "/api/v1/workflows", firstToken, body, "workflow-key")
	if first.Code != http.StatusAccepted {
		t.Fatalf("workflow submit: %d %s", first.Code, first.Body.String())
	}
	var result struct {
		WorkflowID string `json:"workflow_id"`
		Nodes      []struct {
			Limits struct {
				QueueTimeoutMS int64 `json:"queue_timeout_ms"`
			} `json:"limits"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &result); err != nil || result.WorkflowID == "" || len(result.Nodes) != 2 || result.Nodes[0].Limits.QueueTimeoutMS != 30000 {
		t.Fatalf("workflow response: %+v err=%v", result, err)
	}
	replay := request(t, api, "POST", "/api/v1/workflows", firstToken, body, "workflow-key")
	var replayed struct {
		WorkflowID string `json:"workflow_id"`
	}
	if err := json.Unmarshal(replay.Body.Bytes(), &replayed); err != nil || replay.Code != http.StatusAccepted || replayed.WorkflowID != result.WorkflowID {
		t.Fatalf("workflow replay: %d %+v err=%v", replay.Code, replayed, err)
	}
	conflict := request(t, api, "POST", "/api/v1/workflows", firstToken, strings.Replace(body, `"delay_ms":1`, `"delay_ms":2`, 1), "workflow-key")
	if conflict.Code != http.StatusConflict {
		t.Fatalf("workflow key conflict: %d %s", conflict.Code, conflict.Body.String())
	}
	path := "/api/v1/workflows/" + result.WorkflowID
	for _, methodPath := range []struct{ method, path string }{{"GET", path}, {"POST", path + "/cancel"}} {
		got := request(t, api, methodPath.method, methodPath.path, secondToken, "", "")
		if got.Code != http.StatusNotFound {
			t.Fatalf("cross-tenant workflow %s %s: %d %s", methodPath.method, methodPath.path, got.Code, got.Body.String())
		}
	}
	if got := request(t, api, "GET", path, firstToken, "", ""); got.Code != http.StatusOK {
		t.Fatalf("same-tenant workflow get: %d %s", got.Code, got.Body.String())
	}
	if got := request(t, api, "POST", path+"/cancel", firstToken, "", ""); got.Code != http.StatusOK && got.Code != http.StatusAccepted {
		t.Fatalf("same-tenant workflow cancel: %d %s", got.Code, got.Body.String())
	}
}

func TestHTTPJSONLimitAndValidation(t *testing.T) {
	api := apiFixture(t)
	tooLarge := `{"tool":"mock.delay","arguments":{"value":"` + strings.Repeat("a", maxJSONBytes) + `"}}`
	if got := request(t, api, "POST", "/api/v1/executions", firstToken, tooLarge, ""); got.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("large JSON: %d %s", got.Code, got.Body.String())
	}
	for _, body := range []string{`null`, `{"tool":"mock.delay","arguments":{"delay_ms":1}} {}`, `{"tool":"mock.delay","arguments":{"delay_ms":1},"queue_timeout_ms":9223372036855}`} {
		if got := request(t, api, "POST", "/api/v1/executions", firstToken, body, ""); got.Code != http.StatusBadRequest {
			t.Fatalf("invalid JSON %q: %d %s", body, got.Code, got.Body.String())
		}
	}
	valid := request(t, api, "POST", "/api/v1/executions", firstToken, `{"tool":"mock.delay","arguments":{"delay_ms":1}}`, "single-key")
	if valid.Code != http.StatusAccepted {
		t.Fatalf("single submit: %d %s", valid.Code, valid.Body.String())
	}
	replay := request(t, api, "POST", "/api/v1/executions", firstToken, `{"tool":"mock.delay","arguments":{"delay_ms":1}}`, "single-key")
	var a, b struct {
		ExecutionID string `json:"execution_id"`
	}
	_ = json.Unmarshal(valid.Body.Bytes(), &a)
	_ = json.Unmarshal(replay.Body.Bytes(), &b)
	if replay.Code != http.StatusAccepted || a.ExecutionID == "" || a.ExecutionID != b.ExecutionID {
		t.Fatalf("single replay: %d original=%+v replay=%+v", replay.Code, a, b)
	}
	conflict := request(t, api, "POST", "/api/v1/executions", firstToken, `{"tool":"mock.delay","arguments":{"delay_ms":2}}`, "single-key")
	if conflict.Code != http.StatusConflict {
		t.Fatalf("single key conflict: %d %s", conflict.Code, conflict.Body.String())
	}
	if bytes.Contains(conflict.Body.Bytes(), []byte("single-key")) {
		t.Fatal("error response leaked idempotency key")
	}
}

func TestHTTPRoutingErrorsHaveRequestID(t *testing.T) {
	api := apiFixture(t)
	for _, route := range []struct {
		method, path string
		status       int
	}{{"GET", "/does-not-exist", http.StatusNotFound}, {"PUT", "/api/v1/executions", http.StatusMethodNotAllowed}} {
		got := request(t, api, route.method, route.path, firstToken, "", "")
		if got.Code != route.status || !strings.Contains(got.Body.String(), `"request_id"`) {
			t.Fatalf("routing error %s %s: %d %s", route.method, route.path, got.Code, got.Body.String())
		}
	}
}

func TestHTTPRejectsInvalidTokenConfiguration(t *testing.T) {
	api := apiFixture(t)
	for _, tokens := range [][]Token{
		{{Value: "short", TenantID: "first", Tools: []string{mock.Name}}},
		{{Value: firstToken, TenantID: " bad", Tools: []string{mock.Name}}},
		{{Value: firstToken, TenantID: "first", Tools: []string{mock.Name, mock.Name}}},
		{{Value: firstToken, TenantID: "first", Tools: []string{mock.Name}}, {Value: firstToken, TenantID: "second", Tools: []string{mock.Name}}},
	} {
		if _, err := New(api.manager, tokens); err == nil {
			t.Fatalf("accepted invalid token configuration: %+v", tokens)
		}
	}
}
