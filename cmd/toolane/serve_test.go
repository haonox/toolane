package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/execution"
	"github.com/haonox/toolane/internal/executor/docker"
	"github.com/haonox/toolane/internal/httpapi"
	"github.com/haonox/toolane/internal/tool"
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
		strings.Replace(valid, "mock.delay", "process.printf", 1),
		strings.Replace(valid, `"tokens":`, `"sse_tenant_limit":9,"sse_global_limit":8,"tokens":`, 1),
		valid + `{}`,
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

func TestDockerServiceConfigurationAndFailClosedRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "server.json")
	config := serverConfig{Listen: "127.0.0.1:0", StateDir: filepath.Join(dir, "state"),
		Tokens: []httpapi.Token{{Value: "docker-service-token-12345", TenantID: "local", Tools: []string{sandboxToolName}}},
		Docker: &serverDockerConfig{ImageTag: "alpine:3.24.2", ImageDigest: "sha256:" + strings.Repeat("a", 64),
			InstanceID: "service-test", DockerBinary: "/bin/false"}}
	write := func() {
		t.Helper()
		raw, err := json.Marshal(config)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write()
	parsed, err := loadServerConfig(path)
	if err != nil || parsed.Docker == nil || parsed.Docker.InstanceID != config.Docker.InstanceID {
		t.Fatalf("valid Docker config: %+v err=%v", parsed, err)
	}
	if err := runServer(context.Background(), path); err == nil || !strings.Contains(err.Error(), "recover Docker containers") {
		t.Fatalf("unavailable daemon opened service: %v", err)
	}
	backend, err := docker.NewBackend(docker.Spec{ImageTag: config.Docker.ImageTag, ImageDigest: config.Docker.ImageDigest,
		InstanceID: config.Docker.InstanceID, DockerBinary: config.Docker.DockerBinary})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	resources := []execution.RecoveryResource{{ExecutionID: strings.Repeat("a", 32), Ref: tool.ResourceRef{
		Kind: tool.ResourceDockerContainer, ID: strings.Repeat("b", 64), InstanceID: "old-instance"}}}
	if err := recoverServerResources(context.Background(), backend, config.Docker, resources); err == nil || !strings.Contains(err.Error(), "another backend") {
		t.Fatalf("foreign instance resource accepted: %v", err)
	}
	config.Docker = nil
	write()
	if _, err := loadServerConfig(path); err == nil {
		t.Fatal("sandbox token accepted without Docker configuration")
	}
	if err := recoverServerResources(context.Background(), nil, nil, resources); err == nil {
		t.Fatal("persisted Docker resource accepted without backend")
	}
	config.Docker = &serverDockerConfig{ImageTag: "alpine:3.24.2", InstanceID: "service-test"}
	write()
	if _, err := loadServerConfig(path); err == nil {
		t.Fatal("Docker configuration without digest accepted")
	}
}

func TestDockerInstanceBindingRequiresStableConfiguration(t *testing.T) {
	stateDir := t.TempDir()
	config := &serverDockerConfig{InstanceID: "stable-instance"}
	if err := checkDockerInstanceBinding(stateDir, config); err != nil {
		t.Fatalf("new state directory rejected: %v", err)
	}
	if err := bindDockerInstance(stateDir, config.InstanceID); err != nil {
		t.Fatalf("bind instance: %v", err)
	}
	if err := bindDockerInstance(stateDir, config.InstanceID); err != nil {
		t.Fatalf("same instance rejected: %v", err)
	}
	if err := checkDockerInstanceBinding(stateDir, config); err != nil {
		t.Fatalf("stable configuration rejected: %v", err)
	}
	if err := checkDockerInstanceBinding(stateDir, nil); err == nil {
		t.Fatal("Docker removal accepted after instance binding")
	}
	if err := checkDockerInstanceBinding(stateDir, &serverDockerConfig{InstanceID: "replacement"}); err == nil {
		t.Fatal("instance ID change accepted")
	}
	if err := bindDockerInstance(stateDir, "replacement"); err == nil {
		t.Fatal("instance binding overwritten")
	}
	path := filepath.Join(stateDir, dockerInstanceFile)
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("binding is not private: info=%v err=%v", info, err)
	}
	if err := os.WriteFile(path, []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := checkDockerInstanceBinding(stateDir, config); err == nil {
		t.Fatal("incomplete instance binding accepted")
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
	dir := t.TempDir()
	path := filepath.Join(dir, "server.json")
	raw, err := json.Marshal(serverConfig{Listen: address, StateDir: filepath.Join(dir, "state"),
		Tokens: []httpapi.Token{{Value: "local-test-secret-123456", TenantID: "local", Tools: []string{"mock.delay"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runServer(ctx, path) }()
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
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("server shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not stop")
	}
}
