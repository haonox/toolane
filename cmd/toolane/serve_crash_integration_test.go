//go:build integration

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
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/execution"
	"github.com/haonox/toolane/internal/httpapi"
)

const crashTestToken = "docker-crash-test-token-123456"

// TestDockerHTTPServiceCrashRecovery kills the service while its real Docker
// task is running, then restarts from the same SQLite state directory.
func TestDockerHTTPServiceCrashRecovery(t *testing.T) {
	if os.Getenv("TOOLANE_CRASH_TEST_CHILD") == "1" {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := runServer(ctx, os.Getenv("TOOLANE_CRASH_TEST_CONFIG")); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		return
	}
	dockerBinary, err := exec.LookPath("docker")
	if err != nil {
		t.Fatal(err)
	}
	instance := fmt.Sprintf("it%x", time.Now().UnixNano())
	dir := t.TempDir()
	address := crashTestAddress(t)
	configPath := filepath.Join(dir, "server.json")
	config := serverConfig{Listen: address, StateDir: filepath.Join(dir, "state"),
		Tokens: []httpapi.Token{{Value: crashTestToken, TenantID: "local", Tools: []string{sandboxToolName}}},
		Docker: &serverDockerConfig{ImageTag: serviceImageTag, ImageDigest: serviceImageDigest,
			InstanceID: instance, DockerBinary: dockerBinary}}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ids, err := crashTestContainers(dockerBinary, instance, true)
		if err != nil {
			t.Errorf("list crash test containers: %v", err)
			return
		}
		for _, id := range ids {
			output, err := exec.Command(dockerBinary, "container", "rm", "--force", "--volumes", id).CombinedOutput()
			if err != nil {
				t.Errorf("remove crash test container %s: %v: %s", id, err, output)
			}
		}
	})
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 5 * time.Second}
	base := "http://" + address
	first := startCrashTestService(t, configPath, dir)
	waitCrashTestReady(t, client, base, first)
	request, err := http.NewRequest(http.MethodPost, base+"/api/v1/executions",
		strings.NewReader(`{"tool":"sandbox.shell","arguments":{"script":"sleep 30"},"execution_timeout_ms":45000}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+crashTestToken)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var accepted struct {
		ExecutionID string `json:"execution_id"`
	}
	decodeErr := json.NewDecoder(response.Body).Decode(&accepted)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusAccepted || decodeErr != nil || len(accepted.ExecutionID) != 32 {
		t.Fatalf("submit running Docker task: status=%d id=%q err=%v", response.StatusCode, accepted.ExecutionID, decodeErr)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		snapshot := getServiceExecution(t, client, base, accepted.ExecutionID, crashTestToken)
		ids, err := crashTestContainers(dockerBinary, instance, false)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.State == execution.Running && len(ids) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Docker task did not start: snapshot=%+v containers=%v", snapshot, ids)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := first.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-first.done:
	case <-time.After(10 * time.Second):
		t.Fatal("killed service did not exit")
	}
	restartAt := time.Now()
	second := startCrashTestService(t, configPath, dir)
	waitCrashTestReady(t, client, base, second)
	recoveryTime := time.Since(restartAt)
	deadline = time.Now().Add(10 * time.Second)
	var recovered execution.Snapshot
	var ids []string
	for {
		recovered = getServiceExecution(t, client, base, accepted.ExecutionID, crashTestToken)
		ids, err = crashTestContainers(dockerBinary, instance, true)
		if err != nil {
			t.Fatal(err)
		}
		if recovered.State == execution.Interrupted && recovered.ErrorCode == "service_restarted" &&
			recovered.CleanupStatus == "succeeded" && len(ids) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("running task after restart: snapshot=%+v containers=%v", recovered, ids)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("actual service crash recovery: execution=%s restart_to_ready=%s containers_remaining=0", accepted.ExecutionID, recoveryTime)
}

type crashTestService struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
	log  string
}

func startCrashTestService(t *testing.T, configPath, dir string) *crashTestService {
	t.Helper()
	logFile, err := os.CreateTemp(dir, "service-*.log")
	if err != nil {
		t.Fatal(err)
	}
	service := &crashTestService{cmd: exec.Command(os.Args[0], "-test.run=^TestDockerHTTPServiceCrashRecovery$"),
		done: make(chan struct{}), log: logFile.Name()}
	service.cmd.Env = append(os.Environ(), "TOOLANE_CRASH_TEST_CHILD=1", "TOOLANE_CRASH_TEST_CONFIG="+configPath)
	service.cmd.Stdout, service.cmd.Stderr = logFile, logFile
	if err := service.cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	_ = logFile.Close()
	go func() {
		service.err = service.cmd.Wait()
		close(service.done)
	}()
	t.Cleanup(func() {
		select {
		case <-service.done:
			return
		default:
		}
		_ = service.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-service.done:
			if service.err != nil {
				t.Errorf("service shutdown: %v; log: %s", service.err, crashTestLog(service.log))
			}
		case <-time.After(15 * time.Second):
			_ = service.cmd.Process.Kill()
			<-service.done
			t.Errorf("service did not stop on SIGTERM; log: %s", crashTestLog(service.log))
		}
	})
	return service
}

func waitCrashTestReady(t *testing.T, client *http.Client, base string, service *crashTestService) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-service.done:
			t.Fatalf("service exited before readiness: %v; log: %s", service.err, crashTestLog(service.log))
		default:
		}
		response, err := client.Get(base + "/readyz")
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("service did not become ready; log: %s", crashTestLog(service.log))
}

func crashTestAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func crashTestContainers(dockerBinary, instance string, all bool) ([]string, error) {
	args := []string{"container", "ls", "--quiet"}
	if all {
		args = append(args, "--all")
	}
	args = append(args, "--filter=label=toolane.project=toolane", "--filter=label=toolane.instance="+instance)
	output, err := exec.Command(dockerBinary, args...).Output()
	if err != nil {
		return nil, err
	}
	return strings.Fields(string(output)), nil
}

func crashTestLog(path string) string {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err.Error()
	}
	lines := strings.Split(string(raw), "\n")
	if len(lines) > 12 {
		lines = lines[len(lines)-12:]
	}
	return strings.Join(lines, "\n")
}
