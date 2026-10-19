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
	if err != nil || config.Listen != "127.0.0.1:0" || config.StateDir != stateDir || len(config.Tokens) != 1 {
		t.Fatalf("valid config: %+v err=%v", config, err)
	}
	for _, raw := range []string{
		strings.Replace(valid, "127.0.0.1:0", "0.0.0.0:8080", 1),
		strings.Replace(valid, "mock.delay", "process.printf", 1),
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
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("HTTP submission status=%d", response.StatusCode)
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
