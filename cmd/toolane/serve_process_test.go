package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/execution"
	"github.com/haonox/toolane/internal/httpapi"
)

func TestProcessHTTPHelper(t *testing.T) {
	if os.Getenv("TOOLANE_PROCESS_HTTP_HELPER") != "1" {
		return
	}
	args := flag.Args()
	if len(args) != 3 {
		fmt.Fprintln(os.Stderr, "expected mode, value and execution ID")
		os.Exit(2)
	}
	mode, value, id := args[0], args[1], args[2]
	if mode == "slow" {
		if readyFile := os.Getenv("TOOLANE_PROCESS_READY_FILE"); readyFile != "" {
			_ = os.WriteFile(readyFile, []byte(fmt.Sprint(os.Getpid())), 0600)
		}
		time.Sleep(1200 * time.Millisecond)
		file, err := os.OpenFile(os.Getenv("TOOLANE_PROCESS_LEDGER"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			os.Exit(3)
		}
		_, _ = fmt.Fprintln(file, id)
		_ = file.Close()
	}
	if value == "fail" {
		os.Exit(7)
	}
	if value == "invalid" {
		fmt.Print("not JSON")
		os.Exit(0)
	}
	result, _ := json.Marshal(map[string]string{"value": value, "execution_id": id})
	fmt.Print(string(result))
	os.Exit(0)
}

func processTestConfig(t *testing.T, dir, address string) string {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	schema := json.RawMessage(`{"type":"object","properties":{"value":{"type":"string","minLength":1,"maxLength":20}},"required":["value"],"additionalProperties":false}`)
	literal := func(value string) processArgumentConfig { return processArgumentConfig{Literal: &value} }
	toolConfig := func(name, mode string) serverProcessToolConfig {
		return serverProcessToolConfig{Name: name, Description: "Return a fixed test result", Parameters: schema,
			Executable: executable, Directory: dir, Environment: map[string]string{"TOOLANE_PROCESS_HTTP_HELPER": "1", "TOOLANE_PROCESS_LEDGER": filepath.Join(dir, "ledger"), "TOOLANE_PROCESS_READY_FILE": filepath.Join(dir, "process-ready")},
			Args:         []processArgumentConfig{literal("-test.run=^TestProcessHTTPHelper$"), literal("--"), literal(mode), {Parameter: "value"}, {ExecutionID: true}},
			RunTimeoutMS: 3000}
	}
	config := serverConfig{Listen: address, StateDir: filepath.Join(dir, "state"),
		ProcessTools: []serverProcessToolConfig{toolConfig("test.echo", "echo"), toolConfig("test.slow", "slow")},
		Tokens: []httpapi.Token{{Value: "tenant-a-secret-123456", TenantID: "tenantA", Tools: []string{"test.echo"}},
			{Value: "tenant-b-secret-123456", TenantID: "tenantB", Tools: []string{"test.echo", "test.slow"}}}}
	path := filepath.Join(dir, "server.json")
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func processTestAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if errors.Is(err, syscall.EPERM) {
			t.Skip("local socket unavailable")
		}
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}

func startProcessTestServer(t *testing.T, configPath, address string) (string, *http.Client, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runServerWithLogger(ctx, configPath, slog.New(slog.NewJSONHandler(io.Discard, nil))) }()
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 3 * time.Second}
	base := "http://" + address
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, err := client.Get(base + "/readyz")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case serviceErr := <-done:
			cancel()
			t.Fatalf("service failed before readiness: %v", serviceErr)
		default:
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("service not ready: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop := func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("service shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("service shutdown timed out")
		}
	}
	return base, client, stop
}

func processRequest(t *testing.T, client *http.Client, method, url, token, body, key string) (int, []byte) {
	t.Helper()
	request, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, raw
}

func processSubmit(t *testing.T, client *http.Client, base, token, body, key string) execution.Snapshot {
	t.Helper()
	status, raw := processRequest(t, client, http.MethodPost, base+"/api/v1/executions", token, body, key)
	if status != http.StatusAccepted {
		t.Fatalf("submit status=%d body=%s", status, raw)
	}
	var snapshot execution.Snapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil || snapshot.ID == "" {
		t.Fatalf("submit response=%s err=%v", raw, err)
	}
	return snapshot
}

func processSnapshot(t *testing.T, client *http.Client, base, token, id string) execution.Snapshot {
	t.Helper()
	status, raw := processRequest(t, client, http.MethodGet, base+"/api/v1/executions/"+id, token, "", "")
	if status != http.StatusOK {
		t.Fatalf("get status=%d body=%s", status, raw)
	}
	var snapshot execution.Snapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func awaitProcessSnapshot(t *testing.T, client *http.Client, base, token, id string, condition func(execution.Snapshot) bool) execution.Snapshot {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		snapshot := processSnapshot(t, client, base, token, id)
		if condition(snapshot) {
			return snapshot
		}
		if time.Now().After(deadline) {
			t.Fatalf("execution did not reach expected state: %+v", snapshot)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestConfiguredProcessToolsThroughHTTP(t *testing.T) {
	dir := t.TempDir()
	address := processTestAddress(t)
	configPath := processTestConfig(t, dir, address)
	base, client, stop := startProcessTestServer(t, configPath, address)
	t.Cleanup(stop)
	status, catalog := processRequest(t, client, http.MethodGet, base+"/api/v1/tools", "tenant-a-secret-123456", "", "")
	if status != http.StatusOK || !strings.Contains(string(catalog), `"name":"test.echo"`) || strings.Contains(string(catalog), "test.slow") || strings.Contains(string(catalog), "executable") {
		t.Fatalf("filtered catalog status=%d body=%s", status, catalog)
	}
	for _, input := range []string{`{"tool":"test.echo","arguments":{}}`, `{"tool":"test.echo","arguments":{"value":"ok","extra":1}}`} {
		status, raw := processRequest(t, client, http.MethodPost, base+"/api/v1/executions", "tenant-a-secret-123456", input, "")
		if status != http.StatusBadRequest {
			t.Fatalf("invalid input status=%d body=%s", status, raw)
		}
	}
	status, raw := processRequest(t, client, http.MethodPost, base+"/api/v1/executions", "tenant-a-secret-123456", `{"tool":"test.slow","arguments":{"value":"no"}}`, "")
	if status != http.StatusForbidden {
		t.Fatalf("unauthorized tool status=%d body=%s", status, raw)
	}
	for _, token := range []string{"tenant-a-secret-123456", "tenant-b-secret-123456"} {
		accepted := processSubmit(t, client, base, token, `{"tool":"test.echo","arguments":{"value":"hello"}}`, "same-key")
		duplicate := processSubmit(t, client, base, token, `{"tool":"test.echo","arguments":{"value":"hello"}}`, "same-key")
		if duplicate.ID != accepted.ID {
			t.Fatalf("idempotency changed execution: %s %s", accepted.ID, duplicate.ID)
		}
		final := awaitProcessSnapshot(t, client, base, token, accepted.ID, func(s execution.Snapshot) bool { return s.FinishedAt != nil })
		var result map[string]string
		if final.State != execution.Succeeded || json.Unmarshal(final.Result, &result) != nil || result["value"] != "hello" || result["execution_id"] != accepted.ID {
			t.Fatalf("wrong real tool result: %+v", final)
		}
		other := "tenant-b-secret-123456"
		if token == other {
			other = "tenant-a-secret-123456"
		}
		status, _ := processRequest(t, client, http.MethodGet, base+"/api/v1/executions/"+accepted.ID, other, "", "")
		if status != http.StatusNotFound {
			t.Fatalf("cross-tenant query status=%d", status)
		}
	}
	for _, value := range []string{"fail", "invalid"} {
		accepted := processSubmit(t, client, base, "tenant-a-secret-123456", `{"tool":"test.echo","arguments":{"value":"`+value+`"}}`, "")
		final := awaitProcessSnapshot(t, client, base, "tenant-a-secret-123456", accepted.ID, func(s execution.Snapshot) bool { return s.FinishedAt != nil })
		if final.State != execution.Failed {
			t.Fatalf("tool error not surfaced: %+v", final)
		}
	}
	cancelled := processSubmit(t, client, base, "tenant-b-secret-123456", `{"tool":"test.slow","arguments":{"value":"cancel"}}`, "")
	awaitProcessSnapshot(t, client, base, "tenant-b-secret-123456", cancelled.ID, func(s execution.Snapshot) bool { return s.ProcessStartedAt != nil })
	status, raw = processRequest(t, client, http.MethodPost, base+"/api/v1/executions/"+cancelled.ID+"/cancel", "tenant-b-secret-123456", "", "")
	if status != http.StatusOK && status != http.StatusAccepted {
		t.Fatalf("cancel status=%d body=%s", status, raw)
	}
	final := awaitProcessSnapshot(t, client, base, "tenant-b-secret-123456", cancelled.ID, func(s execution.Snapshot) bool { return s.FinishedAt != nil })
	if final.State != execution.Cancelled {
		t.Fatalf("cancel state: %+v", final)
	}
	timed := processSubmit(t, client, base, "tenant-b-secret-123456", `{"tool":"test.slow","arguments":{"value":"timeout"},"execution_timeout_ms":100}`, "")
	final = awaitProcessSnapshot(t, client, base, "tenant-b-secret-123456", timed.ID, func(s execution.Snapshot) bool { return s.FinishedAt != nil })
	if final.State != execution.TimedOut {
		t.Fatalf("timeout state: %+v", final)
	}
	time.Sleep(1300 * time.Millisecond)
	if _, err := os.Stat(filepath.Join(dir, "ledger")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cancelled/timed out process wrote ledger: %v", err)
	}
}

func TestConfiguredProcessToolFailsClosedAtStartup(t *testing.T) {
	dir := t.TempDir()
	path := processTestConfig(t, dir, "127.0.0.1:0")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var config serverConfig
	if err := json.Unmarshal(raw, &config); err != nil {
		t.Fatal(err)
	}
	for name, modify := range map[string]func(*serverConfig){
		"unsupported schema": func(c *serverConfig) {
			c.ProcessTools[0].Parameters = json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false,"pattern":"x"}`)
		},
		"unknown binding":    func(c *serverConfig) { c.ProcessTools[0].Args[3].Parameter = "missing" },
		"missing executable": func(c *serverConfig) { c.ProcessTools[0].Executable = "/nonexistent/toolane-fixture" },
		"duplicate name":     func(c *serverConfig) { c.ProcessTools[1].Name = c.ProcessTools[0].Name },
	} {
		var candidate serverConfig
		if err := json.Unmarshal(raw, &candidate); err != nil {
			t.Fatal(err)
		}
		modify(&candidate)
		encoded, err := json.Marshal(candidate)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, encoded, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadServerConfig(path); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestProcessServerCrashChild(t *testing.T) {
	if os.Getenv("TOOLANE_PROCESS_SERVER_CHILD") != "1" {
		return
	}
	if err := runServer(context.Background(), os.Getenv("TOOLANE_PROCESS_SERVER_CONFIG")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

// A killed service cannot infer that its child stopped or that its side effect
// did not happen. The fixture uses a separate ledger to demonstrate the gap.
func TestConfiguredProcessCrashMarksResultUnknown(t *testing.T) {
	dir := t.TempDir()
	address := processTestAddress(t)
	configPath := processTestConfig(t, dir, address)
	child := exec.Command(os.Args[0], "-test.run=^TestProcessServerCrashChild$")
	child.Env = append(os.Environ(), "TOOLANE_PROCESS_SERVER_CHILD=1", "TOOLANE_PROCESS_SERVER_CONFIG="+configPath)
	child.Stdout, child.Stderr = io.Discard, io.Discard
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	childDone := make(chan error, 1)
	go func() { childDone <- child.Wait() }()
	t.Cleanup(func() { _ = child.Process.Kill() })
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 3 * time.Second}
	base := "http://" + address
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, err := client.Get(base + "/readyz")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case serviceErr := <-childDone:
			t.Fatalf("child service failed: %v", serviceErr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("child service not ready: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	accepted := processSubmit(t, client, base, "tenant-b-secret-123456", `{"tool":"test.slow","arguments":{"value":"after-crash"}}`, "")
	readyPath := filepath.Join(dir, "process-ready")
	var pid int
	deadline = time.Now().Add(3 * time.Second)
	for {
		raw, err := os.ReadFile(readyPath)
		if err == nil {
			pid, err = strconv.Atoi(string(raw))
			if err == nil && pid > 0 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("tool did not start: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Cleanup(func() { _ = syscall.Kill(-pid, syscall.SIGKILL) })
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-childDone:
	case <-time.After(3 * time.Second):
		t.Fatal("killed service did not exit")
	}
	restartedBase, restartedClient, stop := startProcessTestServer(t, configPath, address)
	t.Cleanup(stop)
	recovered := processSnapshot(t, restartedClient, restartedBase, "tenant-b-secret-123456", accepted.ID)
	if recovered.State != execution.Interrupted || recovered.ErrorCode != "service_restarted" || recovered.Result != nil {
		t.Fatalf("crash was treated as a known execution result: %+v", recovered)
	}
	deadline = time.Now().Add(3 * time.Second)
	for {
		ledger, err := os.ReadFile(filepath.Join(dir, "ledger"))
		if err == nil && strings.Contains(string(ledger), accepted.ID) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("child side effect was not observed after service crash: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
