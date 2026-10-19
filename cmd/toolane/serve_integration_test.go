//go:build integration && linux

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/execution"
	"github.com/haonox/toolane/internal/httpapi"
	"github.com/haonox/toolane/internal/store/sqlite"
	"github.com/haonox/toolane/internal/tool"
)

const (
	serviceImageTag    = "alpine:3.24.2"
	serviceImageDigest = "sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6"
)

func TestDockerHTTPServiceRecoversAndRunsSandbox(t *testing.T) {
	dockerBinary, err := exec.LookPath("docker")
	if err != nil {
		t.Fatal(err)
	}
	instance := fmt.Sprintf("it%x", time.Now().UnixNano())
	trackedExecution := strings.Repeat("d", 32)
	gapExecution := strings.Repeat("e", 32)
	create := func(executionID string) string {
		t.Helper()
		name := "toolane-" + instance + "-" + executionID
		output, err := exec.Command(dockerBinary, "container", "create", "--pull=never", "--name="+name,
			"--label=toolane.project=toolane", "--label=toolane.instance="+instance,
			"--label=toolane.execution="+executionID, serviceImageTag+"@"+serviceImageDigest, "true").CombinedOutput()
		if err != nil {
			t.Fatalf("create recovery fixture: %v: %s", err, output)
		}
		id := strings.TrimSpace(string(output))
		t.Cleanup(func() {
			output, err := exec.Command(dockerBinary, "container", "rm", "--force", "--volumes", id).CombinedOutput()
			if err != nil && !strings.Contains(strings.ToLower(string(output)), "no such container") {
				t.Errorf("remove recovery fixture %s: %v: %s", id, err, output)
			}
		})
		return id
	}
	trackedID := create(trackedExecution)
	gapID := create(gapExecution)
	dir := t.TempDir()
	stateDir := filepath.Join(dir, "state")
	store, err := sqlite.Open(context.Background(), stateDir)
	if err != nil {
		t.Fatal(err)
	}
	seedServiceRecoveryCandidate(t, store, trackedExecution, &tool.ResourceRef{
		Kind: tool.ResourceDockerContainer, ID: trackedID, InstanceID: instance,
	})
	seedServiceRecoveryCandidate(t, store, gapExecution, nil)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "server.json")
	config := serverConfig{Listen: address, StateDir: stateDir,
		Tokens: []httpapi.Token{
			{Value: "docker-http-local-token-12345", TenantID: "local", Tools: []string{sandboxToolName}},
			{Value: "docker-http-other-token-12345", TenantID: "other", Tools: []string{sandboxToolName}},
		},
		Docker: &serverDockerConfig{ImageTag: serviceImageTag, ImageDigest: serviceImageDigest,
			InstanceID: instance, DockerBinary: dockerBinary}}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runServer(ctx, configPath)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("server shutdown: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("server did not stop")
		}
	})
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 20 * time.Second}
	base := "http://" + address
	deadline := time.After(10 * time.Second)
	for {
		response, err := client.Get(base + "/readyz")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case err := <-done:
			t.Fatalf("Docker service failed before readiness: %v", err)
		case <-deadline:
			t.Fatalf("Docker service did not become ready: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
	}
	for _, id := range []string{trackedID, gapID} {
		if err := exec.Command(dockerBinary, "container", "inspect", id).Run(); err == nil {
			t.Fatalf("startup left owned container %s", id)
		}
	}
	if bound, err := boundDockerInstance(stateDir); err != nil || bound != instance {
		t.Fatalf("startup did not persist Docker instance: bound=%q err=%v", bound, err)
	}
	tracked := getServiceExecution(t, client, base, trackedExecution, "docker-http-local-token-12345")
	if tracked.State != execution.Interrupted || tracked.ErrorCode != "service_restarted" {
		t.Fatalf("tracked recovery: %+v", tracked)
	}
	request, err := http.NewRequest(http.MethodPost, base+"/api/v1/executions",
		strings.NewReader(`{"tool":"sandbox.shell","arguments":{"script":"printf 'uid=%s\\n' \"$(id -u)\"; printf 'hello\\n'"}}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer docker-http-local-token-12345")
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
		t.Fatalf("sandbox admission: status=%d id=%q err=%v", response.StatusCode, accepted.ExecutionID, decodeErr)
	}
	foreign := getServiceExecutionStatus(t, client, base, accepted.ExecutionID, "docker-http-other-token-12345")
	if foreign != http.StatusNotFound {
		t.Fatalf("cross-tenant query status=%d", foreign)
	}
	eventsRequest, err := http.NewRequest(http.MethodGet, base+"/api/v1/executions/"+accepted.ExecutionID+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	eventsRequest.Header.Set("Authorization", "Bearer docker-http-local-token-12345")
	eventsResponse, err := client.Do(eventsRequest)
	if err != nil {
		t.Fatal(err)
	}
	events, readErr := io.ReadAll(eventsResponse.Body)
	_ = eventsResponse.Body.Close()
	if eventsResponse.StatusCode != http.StatusOK || readErr != nil || !strings.Contains(string(events), "event: output\n") || !strings.Contains(string(events), "event: end\n") {
		t.Fatalf("sandbox SSE: status=%d events=%q err=%v", eventsResponse.StatusCode, events, readErr)
	}
	finished := getServiceExecution(t, client, base, accepted.ExecutionID, "docker-http-local-token-12345")
	if finished.State != execution.Succeeded || finished.CleanupStatus != "succeeded" || !strings.Contains(finished.Stdout, "uid=65532") || !strings.Contains(finished.Stdout, "hello") {
		t.Fatalf("sandbox result: %+v", finished)
	}
	remaining, err := exec.Command(dockerBinary, "container", "ls", "--all", "--quiet", "--filter=label=toolane.instance="+instance).Output()
	if err != nil || strings.TrimSpace(string(remaining)) != "" {
		t.Fatalf("sandbox containers remained: %q err=%v", remaining, err)
	}
}

func seedServiceRecoveryCandidate(t *testing.T, store *sqlite.Store, id string, resource *tool.ResourceRef) {
	t.Helper()
	at := time.Now().UTC()
	cleanup := "not_needed"
	if resource != nil {
		cleanup = "pending"
	}
	snapshot := execution.Snapshot{ID: id, TenantID: "local", Tool: sandboxToolName, State: execution.Running,
		CleanupStatus: cleanup, CreatedAt: at, ReadyAt: &at, RunningAt: &at}
	encoded, err := json.Marshal(map[string]any{
		"snapshot": snapshot, "arguments": []byte(`{"script":"true"}`), "queue_deadline": at.Add(time.Minute), "resource": resource,
		"limits": map[string]any{"queue_timeout_ns": int64(time.Minute), "execution_timeout_ns": int64(10 * time.Second),
			"output_limit_bytes": 1024 * 1024, "argument_limit_bytes": 256 * 1024, "result_limit_bytes": 64 * 1024},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateExecution(context.Background(), sqlite.Execution{ID: id, TenantID: "local", State: string(execution.Running),
		Snapshot: encoded, Request: []byte(`{"tool":"sandbox.shell"}`), CreatedAt: at},
		sqlite.Event{Kind: "state", Payload: []byte(`{"status":"running"}`), OccurredAt: at}); err != nil {
		t.Fatal(err)
	}
}

func getServiceExecutionStatus(t *testing.T, client *http.Client, base, id, token string) int {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, base+"/api/v1/executions/"+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	return response.StatusCode
}

func getServiceExecution(t *testing.T, client *http.Client, base, id, token string) execution.Snapshot {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, base+"/api/v1/executions/"+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var snapshot execution.Snapshot
	if err := json.NewDecoder(response.Body).Decode(&snapshot); err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("query execution: status=%d snapshot=%+v err=%v", response.StatusCode, snapshot, err)
	}
	return snapshot
}
