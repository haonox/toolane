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
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/execution"
	"github.com/haonox/toolane/internal/httpapi"
)

func TestLoadServerConfigRequiresPrivateLoopbackConfiguration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "server.json")
	valid := `{"business_listen":"127.0.0.1:0","tokens":[{"token":"a-long-private-token-123","tenant_id":"local","tools":["mock.delay"]}]}`
	if err := os.WriteFile(path, []byte(valid), 0600); err != nil {
		t.Fatal(err)
	}
	config, err := loadServerConfig(path)
	if err != nil || config.BusinessListen != "127.0.0.1:0" || config.MetricsListen != "127.0.0.1:9090" || len(config.Tokens) != 1 ||
		config.DatabasePath != filepath.Join(dir, "toolane-state", "executions.sqlite") {
		t.Fatalf("valid config: %+v err=%v", config, err)
	}
	for _, raw := range []string{
		strings.Replace(valid, "127.0.0.1:0", "0.0.0.0:8080", 1),
		strings.Replace(valid, `"tokens":`, `"metrics_listen":"0.0.0.0:9090","tokens":`, 1),
		strings.Replace(valid, "mock.delay", "process.printf", 1),
		strings.Replace(valid, "business_listen", "listen", 1),
		valid + `{}`,
		strings.Replace(valid, `"tokens":`, `"unsupported_option":true,"tokens":`, 1),
		strings.Replace(valid, `"tokens":`, `"database_path":"relative.sqlite","tokens":`, 1),
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

func TestLoadServerConfigSupportsManyTenantsWithinSizeLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.json")
	tokens := make([]httpapi.Token, 1000)
	for i := range tokens {
		tokens[i] = httpapi.Token{Value: "private-token-for-tenant-" + strconv.Itoa(i),
			TenantID: "tenant-" + strconv.Itoa(i), Tools: []string{"mock.delay"}}
	}
	raw, err := json.Marshal(serverConfig{Tokens: tokens})
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) <= 64*1024 || len(raw) >= maxServerConfigBytes {
		t.Fatalf("test config size %d does not exercise expanded limit", len(raw))
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	config, err := loadServerConfig(path)
	if err != nil || len(config.Tokens) != len(tokens) {
		t.Fatalf("load 1000 tenants: count=%d err=%v", len(config.Tokens), err)
	}
	if err := os.WriteFile(path, bytes.Repeat([]byte(" "), maxServerConfigBytes+1), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadServerConfig(path); err == nil {
		t.Fatal("accepted config over 2 MiB")
	}
}

func TestServerExecutionConfigOverridesAndRejectsInvalidCapacity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.json")
	base := `"tokens":[{"token":"a-long-private-token-123","tenant_id":"local","tools":["mock.delay"]}]`
	write := func(raw string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(`{` + base + `}`)
	config, err := loadServerConfig(path)
	if err != nil || config.executionConfig() != execution.DefaultConfig() {
		t.Fatalf("omitted execution config: %+v err=%v", config.executionConfig(), err)
	}
	write(`{` + base + `,"execution":{"workers":12}}`)
	config, err = loadServerConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	partial := execution.DefaultConfig()
	partial.Workers = 12
	if config.executionConfig() != partial {
		t.Fatalf("partial execution config: %+v", config.executionConfig())
	}
	write(`{` + base + `,"execution":{"workers":32,"queue_capacity":4096,` +
		`"tenant_running_limit":8,"tenant_pending_limit":512,"retained_results":10000}}`)
	config, err = loadServerConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	capacity := config.executionConfig()
	if capacity.Workers != 32 || capacity.QueueCapacity != 4096 || capacity.TenantRunningLimit != 8 ||
		capacity.TenantPendingLimit != 512 || capacity.RetainedResults != 10000 ||
		capacity.MaxArgumentBytes != execution.DefaultConfig().MaxArgumentBytes {
		t.Fatalf("wrong execution capacity: %+v", capacity)
	}
	write(`{` + base + `,"execution":{"workers":0}}`)
	if err := runServerWithLogger(context.Background(), path, slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("explicit zero workers accepted at startup")
	}
	write(`{` + base + `,"execution":{"unknown_capacity":1}}`)
	if _, err := loadServerConfig(path); err == nil {
		t.Fatal("unknown execution setting accepted")
	}
}

func TestRunServerStartsAndStopsLocalAPI(t *testing.T) {
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
	raw, err := json.Marshal(serverConfig{BusinessListen: address, MetricsListen: managementAddress, Tokens: []httpapi.Token{{Value: "local-test-secret-123456", TenantID: "local", Tools: []string{"mock.delay"}}}})
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
	resultDeadline := time.Now().Add(3 * time.Second)
	for {
		query, err := http.NewRequest(http.MethodGet, base+"/api/v1/executions/"+accepted.ExecutionID, nil)
		if err != nil {
			t.Fatal(err)
		}
		query.Header.Set("Authorization", "Bearer local-test-secret-123456")
		current, err := client.Do(query)
		if err != nil {
			t.Fatal(err)
		}
		var snapshot struct {
			Status string `json:"status"`
		}
		decodeErr := json.NewDecoder(current.Body).Decode(&snapshot)
		_ = current.Body.Close()
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if snapshot.Status == "succeeded" {
			break
		}
		if time.Now().After(resultDeadline) {
			t.Fatalf("execution did not succeed: %s", snapshot.Status)
		}
		time.Sleep(10 * time.Millisecond)
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
	for _, message := range []string{"service_ready", "metrics_ready", "http_request", "execution_transition"} {
		if !seen[message] {
			t.Fatalf("missing %s log: %s", message, logText)
		}
	}
	if info, err := os.Stat(filepath.Join(dir, "toolane-state", "executions.sqlite")); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("durable database permissions: %v, %v", info, err)
	}
	// The same execution ID and result remain available after a service restart.
	restartCtx, stopRestart := context.WithCancel(context.Background())
	restarted := make(chan error, 1)
	go func() {
		restarted <- runServerWithLogger(restartCtx, path, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	}()
	deadline = time.Now().Add(3 * time.Second)
	for {
		response, err := client.Get(base + "/readyz")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			stopRestart()
			<-restarted
			t.Fatalf("restarted service did not become ready: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	query, err := http.NewRequest(http.MethodGet, base+"/api/v1/executions/"+accepted.ExecutionID, nil)
	if err != nil {
		t.Fatal(err)
	}
	query.Header.Set("Authorization", "Bearer local-test-secret-123456")
	response, err = client.Do(query)
	if err != nil {
		t.Fatal(err)
	}
	var recovered execution.Snapshot
	decodeErr = json.NewDecoder(response.Body).Decode(&recovered)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || decodeErr != nil || recovered.State != execution.Succeeded || recovered.FinishedAt == nil {
		t.Fatalf("recovered HTTP result: status=%d snapshot=%+v err=%v", response.StatusCode, recovered, decodeErr)
	}
	stopRestart()
	if err := <-restarted; err != nil {
		t.Fatalf("restarted service shutdown: %v", err)
	}
}
