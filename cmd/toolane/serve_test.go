package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/httpapi"
)

func TestLoadServerConfigRequiresPrivateLoopbackConfiguration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "server.json")
	stateDir := filepath.Join(dir, "state")
	valid := `{"listen":"127.0.0.1:0","state_dir":"` + stateDir + `","tokens":[{"token":"a-long-private-token-123","tenant_id":"local","tools":["mock.delay"]}]}`
	if err := os.WriteFile(path, []byte(valid), 0600); err != nil {
		t.Fatal(err)
	}
	config, err := loadServerConfig(path)
	if err != nil || config.Listen != "127.0.0.1:0" || config.StateDir != stateDir || len(config.Tokens) != 1 || config.SSETenantLimit != 8 || config.SSEGlobalLimit != 64 {
		t.Fatalf("valid config: %+v err=%v", config, err)
	}
	for _, raw := range []string{
		strings.Replace(valid, "127.0.0.1:0", "0.0.0.0:8080", 1),
		strings.Replace(valid, `"tokens":`, `"metrics_listen":"0.0.0.0:9090","tokens":`, 1),
		strings.Replace(valid, "mock.delay", "process.printf", 1),
		strings.Replace(valid, `"tokens":`, `"sse_tenant_limit":9,"sse_global_limit":8,"tokens":`, 1),
		valid + `{}`,
		strings.Replace(valid, `"tokens":`, `"unsupported_option":true,"tokens":`, 1),
	} {
		if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadServerConfig(path); err == nil {
			t.Fatalf("accepted invalid server config: %s", raw)
		}
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadServerConfig(path); err == nil {
		t.Fatal("accepted world-readable token configuration")
	}
}

func TestRunServerStartsAndStopsLocalDurableAPI(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if errors.Is(err, syscall.EPERM) {
			t.Skip("local socket creation unavailable in this environment")
		}
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	managementListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	managementAddress := managementListener.Addr().String()
	_ = managementListener.Close()
	dir := t.TempDir()
	path := filepath.Join(dir, "server.json")
	raw, err := json.Marshal(serverConfig{Listen: address, MetricsListen: managementAddress, StateDir: filepath.Join(dir, "state"),
		Tokens: []httpapi.Token{{Value: "local-test-secret-123456", TenantID: "local", Tools: []string{"mock.delay"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	var logs bytes.Buffer
	go func() { done <- runServerWithLogger(ctx, path, slog.New(slog.NewJSONHandler(&logs, nil))) }()
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: time.Second}
	base := "http://" + address
	deadline := time.Now().Add(3 * time.Second)
	for {
		response, err := client.Get(base + "/readyz")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("service did not become ready: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	request, err := http.NewRequest(http.MethodPost, base+"/api/v1/executions", strings.NewReader(`{"tool":"mock.delay","arguments":{"delay_ms":1}}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer local-test-secret-123456")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var accepted struct {
		ExecutionID string `json:"execution_id"`
	}
	decodeErr := json.NewDecoder(response.Body).Decode(&accepted)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusAccepted || decodeErr != nil || accepted.ExecutionID == "" {
		t.Fatalf("HTTP submission status=%d id=%q err=%v", response.StatusCode, accepted.ExecutionID, decodeErr)
	}
	invalid, err := http.NewRequest(http.MethodPost, base+"/api/v1/executions", strings.NewReader(`{"tool":`))
	if err != nil {
		t.Fatal(err)
	}
	invalid.Header.Set("Authorization", "Bearer local-test-secret-123456")
	invalidResponse, err := client.Do(invalid)
	if err != nil {
		t.Fatal(err)
	}
	_ = invalidResponse.Body.Close()
	if invalidResponse.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed admission status=%d", invalidResponse.StatusCode)
	}
	eventsRequest, err := http.NewRequest(http.MethodGet, base+"/api/v1/executions/"+accepted.ExecutionID+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	eventsRequest.Header.Set("Authorization", "Bearer local-test-secret-123456")
	eventsResponse, err := client.Do(eventsRequest)
	if err != nil {
		t.Fatal(err)
	}
	events, readErr := io.ReadAll(eventsResponse.Body)
	_ = eventsResponse.Body.Close()
	if eventsResponse.StatusCode != http.StatusOK || readErr != nil || !strings.Contains(string(events), "event: end\n") {
		t.Fatalf("HTTP event stream status=%d body=%q err=%v", eventsResponse.StatusCode, events, readErr)
	}
	metricsResponse, err := client.Get("http://" + managementAddress + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	metricsBody, metricsErr := io.ReadAll(metricsResponse.Body)
	_ = metricsResponse.Body.Close()
	if metricsErr != nil || metricsResponse.StatusCode != http.StatusOK ||
		!strings.Contains(string(metricsBody), "toolane_goroutines ") ||
		!strings.Contains(string(metricsBody), `toolane_admissions_total{kind="execution",result="accepted"} 1`) ||
		!strings.Contains(string(metricsBody), `toolane_admissions_total{kind="execution",result="invalid"} 1`) ||
		!strings.Contains(string(metricsBody), `toolane_executions_completed_total{status="succeeded"} 1`) {
		t.Fatalf("management metrics status=%d body=%q err=%v", metricsResponse.StatusCode, metricsBody, metricsErr)
	}
	businessMetrics, err := client.Get(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	_ = businessMetrics.Body.Close()
	if businessMetrics.StatusCode != http.StatusNotFound {
		t.Fatalf("business listener exposed metrics: %d", businessMetrics.StatusCode)
	}
	managementAPI, err := client.Get("http://" + managementAddress + "/api/v1/executions")
	if err != nil {
		t.Fatal(err)
	}
	_ = managementAPI.Body.Close()
	if managementAPI.StatusCode != http.StatusNotFound {
		t.Fatalf("management listener exposed business API: %d", managementAPI.StatusCode)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("server shutdown: %v", err)
		}
	case <-time.After(12 * time.Second):
		t.Fatal("server did not stop")
	}
	logText := logs.String()
	if strings.Contains(logText, "local-test-secret-123456") {
		t.Fatal("service logs contain a credential")
	}
	seen := make(map[string]bool)
	for _, line := range strings.Split(strings.TrimSpace(logText), "\n") {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("invalid service JSON log: %v", err)
		}
		seen[entry["msg"].(string)] = true
		if entry["msg"] == "http_request" && entry["execution_id"] == accepted.ExecutionID {
			if entry["tenant_id"] != "local" || entry["request_id"] == "" {
				t.Fatalf("submission log is not correlated: %+v", entry)
			}
		}
		if entry["msg"] == "execution_transition" && entry["execution_id"] == accepted.ExecutionID {
			if entry["tenant_id"] != "local" || entry["to_state"] == "" {
				t.Fatalf("transition log is not correlated: %+v", entry)
			}
		}
	}
	for _, message := range []string{"recovery_complete", "service_ready", "metrics_ready", "http_request", "execution_transition"} {
		if !seen[message] {
			t.Fatalf("missing %s log: %s", message, logText)
		}
	}
}
