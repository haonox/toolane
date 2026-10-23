package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/execution"
	"github.com/haonox/toolane/internal/executor/mock"
	"github.com/haonox/toolane/internal/tool"
)

const (
	firstToken  = "first-tenant-secret-123456"
	secondToken = "second-tenant-secret-12345"
)

func apiFixture(t *testing.T) *API {
	t.Helper()
	registry := tool.NewRegistry()
	if err := registry.Register(mock.Definition()); err != nil {
		t.Fatal(err)
	}
	manager, err := execution.NewManager(execution.DefaultConfig(), registry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := manager.Close(closeCtx); err != nil {
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

func request(t *testing.T, api *API, method, path, token, body string, _ string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}

	w := httptest.NewRecorder()
	api.ServeHTTP(w, r)
	if w.Header().Get("X-Request-ID") == "" {
		t.Fatal("missing request ID")
	}
	return w
}

func TestStructuredRequestLogsKeepIdentityAndExcludePayload(t *testing.T) {
	api := apiFixture(t)
	var output bytes.Buffer
	api.EnableLogger(slog.New(slog.NewJSONHandler(&output, nil)))
	secret := "private-payload-sentinel"
	if got := request(t, api, "POST", "/api/v1/executions", "wrong-secret-123456", `{"tool":"mock.delay"}`, ""); got.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status: %d", got.Code)
	}
	accepted := request(t, api, "POST", "/api/v1/executions", firstToken,
		`{"tool":"mock.delay","arguments":{"delay_ms":0,"value":"`+secret+`"}}`, "")
	if accepted.Code != http.StatusAccepted {
		t.Fatalf("admission status: %d %s", accepted.Code, accepted.Body.String())
	}
	var created executionView
	if err := json.Unmarshal(accepted.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	path := "/api/v1/executions/" + created.ID
	if got := request(t, api, "GET", path, secondToken, "", ""); got.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant query status: %d", got.Code)
	}
	if got := request(t, api, "GET", path, firstToken, "", ""); got.Code != http.StatusOK {
		t.Fatalf("owner query status: %d", got.Code)
	}
	deadline := time.After(2 * time.Second)
	for {
		snapshot, err := api.manager.Get(created.ID)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.FinishedAt != nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("mock execution did not finish")
		case <-time.After(time.Millisecond):
		}
	}
	text := output.String()
	for _, forbidden := range []string{firstToken, secondToken, secret, "wrong-secret-123456"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("request log leaked %q: %s", forbidden, text)
		}
	}
	var entries []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("invalid JSON log: %v", err)
		}
		entries = append(entries, entry)
	}
	if len(entries) != 4 || entries[0]["error_code"] != "unauthorized" || entries[0]["tenant_id"] != nil ||
		entries[1]["tenant_id"] != "first" || entries[1]["execution_id"] != created.ID ||
		entries[2]["tenant_id"] != "second" || entries[2]["execution_id"] != nil ||
		entries[3]["route"] != "GET /api/v1/executions/{id}" {
		t.Fatalf("unexpected request log fields: %+v", entries)
	}
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
