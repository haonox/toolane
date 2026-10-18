//go:build integration && linux

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/execution"
	"github.com/haonox/toolane/internal/executor/docker"
	"github.com/haonox/toolane/internal/tool"
)

const (
	imageTag    = "alpine:3.24.2"
	imageDigest = "sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6"
)

func sandboxManager(t *testing.T) *execution.Manager {
	t.Helper()
	instance := fmt.Sprintf("it%x", time.Now().UnixNano())
	def, err := docker.ScriptDefinition("sandbox.shell", docker.Spec{
		ImageTag: imageTag, ImageDigest: imageDigest, InstanceID: instance,
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := tool.NewRegistry()
	if err := registry.Register(def); err != nil {
		t.Fatal(err)
	}
	m, err := execution.NewManager(execution.DefaultConfig(), registry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		if err := m.Close(ctx); err != nil {
			t.Errorf("manager close: %v", err)
		}
		ids, err := exec.Command("docker", "container", "ls", "--all", "--quiet", "--filter", "label=toolane.instance="+instance).Output()
		if err != nil {
			t.Errorf("check sandbox leftovers: %v", err)
			return
		}
		if strings.TrimSpace(string(ids)) != "" {
			t.Errorf("sandbox containers remained after execution: %s", strings.TrimSpace(string(ids)))
			for _, id := range strings.Fields(string(ids)) {
				_ = exec.Command("docker", "container", "rm", "--force", "--volumes", id).Run()
			}
		}
	})
	return m
}

func runScript(t *testing.T, m *execution.Manager, script string, timeout time.Duration, outputLimit int) execution.Snapshot {
	t.Helper()
	args, err := json.Marshal(map[string]string{"script": script})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := m.Submit(context.Background(), execution.Request{
		Tool: "sandbox.shell", Arguments: args, ExecutionTimeout: timeout, OutputLimitBytes: outputLimit,
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, err := m.Get(initial.ID)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.FinishedAt != nil {
			if stats := m.Stats(); stats.ReservedSandboxCPUMilli != 0 || stats.ReservedSandboxMemoryBytes != 0 {
				t.Fatalf("sandbox budget leaked after result: %+v", stats)
			}
			return snapshot
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("sandbox did not finish: %+v", snapshot)
		}
	}
}

func TestDockerSandboxSmokeAndIsolation(t *testing.T) {
	m := sandboxManager(t)
	s := runScript(t, m, `
printf 'uid=%s\n' "$(id -u)"
printf x > /workspace/probe || exit 21
if (printf x > /etc/toolane-probe) 2>/dev/null; then exit 22; fi
if wget -q -T 2 -O /dev/null http://1.1.1.1 2>/dev/null; then exit 23; fi
printf 'isolated\n'
`, 8*time.Second, 0)
	if s.State != execution.Succeeded || s.CleanupStatus != "succeeded" || !strings.Contains(s.Stdout, "uid=65532") || !strings.Contains(s.Stdout, "isolated") {
		t.Fatalf("sandbox smoke/isolation failed: %+v", s)
	}
}

func TestDockerSandboxNonzeroExit(t *testing.T) {
	m := sandboxManager(t)
	s := runScript(t, m, "exit 7", 8*time.Second, 0)
	if s.State != execution.Failed || s.ErrorCode != "container_exit_nonzero" || s.ExitCode == nil || *s.ExitCode != 7 || s.CleanupStatus != "succeeded" {
		t.Fatalf("nonzero exit not recorded: %+v", s)
	}
}

func TestDockerSandboxOutputFlood(t *testing.T) {
	m := sandboxManager(t)
	s := runScript(t, m, "yes flood", 8*time.Second, 4096)
	if s.State != execution.Failed || s.ErrorCode != "output_limit_exceeded" || !s.OutputTruncated || len(s.Stdout)+len(s.Stderr) > 4096 || s.CleanupStatus != "succeeded" {
		t.Fatalf("output flood not bounded: %+v", s)
	}
}

func TestDockerSandboxTimeout(t *testing.T) {
	m := sandboxManager(t)
	s := runScript(t, m, "sleep 30", 300*time.Millisecond, 0)
	if s.State != execution.TimedOut || s.ErrorCode != "execution_timeout" || s.CleanupStatus != "succeeded" {
		t.Fatalf("sandbox timeout not enforced: %+v", s)
	}
}
