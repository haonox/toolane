//go:build integration && linux

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/execution"
	"github.com/haonox/toolane/internal/executor/docker"
	"github.com/haonox/toolane/internal/store/sqlite"
	"github.com/haonox/toolane/internal/tool"
)

// TestDockerDurableRecovery exercises both crash windows: an ID already in
// SQLite and a labeled container created just before that write.
func TestDockerDurableRecovery(t *testing.T) {
	instance := fmt.Sprintf("it%x", time.Now().UnixNano())
	stateDir := filepath.Join(t.TempDir(), "state")
	ctx := context.Background()
	store, err := sqlite.Open(ctx, stateDir)
	if err != nil {
		t.Fatal(err)
	}
	trackedExecution := strings.Repeat("d", 32)
	gapExecution := strings.Repeat("e", 32)
	create := func(executionID string) string {
		t.Helper()
		name := "toolane-" + instance + "-" + executionID
		output, err := exec.Command("docker", "container", "create", "--pull=never", "--name="+name,
			"--label=toolane.project=toolane", "--label=toolane.instance="+instance,
			"--label=toolane.execution="+executionID, imageTag+"@"+imageDigest, "true").CombinedOutput()
		if err != nil {
			t.Fatalf("create crash fixture: %v: %s", err, output)
		}
		id := strings.TrimSpace(string(output))
		t.Cleanup(func() {
			output, err := exec.Command("docker", "container", "rm", "--force", "--volumes", id).CombinedOutput()
			if err != nil && !strings.Contains(strings.ToLower(string(output)), "no such container") {
				t.Errorf("remove crash fixture %s: %v: %s", id, err, output)
			}
		})
		return id
	}
	trackedID := create(trackedExecution)
	gapID := create(gapExecution)
	seedInterruptedCandidate(t, store, trackedExecution, &tool.ResourceRef{
		Kind: tool.ResourceDockerContainer, ID: trackedID, InstanceID: instance,
	})
	seedInterruptedCandidate(t, store, gapExecution, nil)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open(ctx, stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	backend, err := docker.NewBackend(docker.Spec{ImageTag: imageTag, ImageDigest: imageDigest, InstanceID: instance})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		defer cancel()
		if err := backend.Close(closeCtx); err != nil {
			t.Errorf("backend close: %v", err)
		}
	}()
	def, err := backend.Definition("sandbox.shell")
	if err != nil {
		t.Fatal(err)
	}
	var executions atomic.Int32
	def.Execute = func(context.Context, json.RawMessage, tool.RunOptions) (tool.Outcome, error) {
		executions.Add(1)
		return tool.Outcome{}, errors.New("recovered execution was run again")
	}
	registry := tool.NewRegistry()
	if err := registry.Register(def); err != nil {
		t.Fatal(err)
	}
	recovered := false
	manager, err := execution.NewDurableManager(ctx, execution.DefaultConfig(), registry, store,
		func(ctx context.Context, resources []execution.RecoveryResource) error {
			if len(resources) != 1 || resources[0].ExecutionID != trackedExecution || resources[0].Ref.ID != trackedID || resources[0].Ref.InstanceID != instance {
				return fmt.Errorf("unexpected persisted resources: %+v", resources)
			}
			recovered = true
			return backend.Recover(ctx)
		})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		defer cancel()
		if err := manager.Close(closeCtx); err != nil {
			t.Errorf("manager close: %v", err)
		}
	}()
	if !recovered || executions.Load() != 0 {
		t.Fatalf("recovery skipped or reran tool: recovered=%t calls=%d", recovered, executions.Load())
	}
	for _, executionID := range []string{trackedExecution, gapExecution} {
		snapshot, err := manager.Get(executionID)
		if err != nil || snapshot.State != execution.Interrupted || snapshot.ErrorCode != "service_restarted" {
			t.Fatalf("execution %s after recovery: %+v err=%v", executionID, snapshot, err)
		}
	}
	for _, id := range []string{trackedID, gapID} {
		if err := exec.Command("docker", "container", "inspect", id).Run(); err == nil {
			t.Fatalf("owned container %s remained after recovery", id)
		}
	}
}

func seedInterruptedCandidate(t *testing.T, store *sqlite.Store, id string, resource *tool.ResourceRef) {
	t.Helper()
	at := time.Now().UTC()
	cleanupStatus := "not_needed"
	if resource != nil {
		cleanupStatus = "pending"
	}
	snapshot := execution.Snapshot{
		ID: id, TenantID: "local", Tool: "sandbox.shell", State: execution.Running,
		CleanupStatus: cleanupStatus, CreatedAt: at, ReadyAt: &at, RunningAt: &at,
	}
	encoded, err := json.Marshal(map[string]any{
		"snapshot":       snapshot,
		"arguments":      []byte(`{"script":"true"}`),
		"queue_deadline": at.Add(time.Minute),
		"resource":       resource,
		"limits": map[string]any{
			"queue_timeout_ns":     int64(time.Minute),
			"execution_timeout_ns": int64(10 * time.Second),
			"output_limit_bytes":   1024 * 1024,
			"argument_limit_bytes": 256 * 1024,
			"result_limit_bytes":   64 * 1024,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateExecution(context.Background(), sqlite.Execution{
		ID: id, TenantID: "local", State: string(execution.Running), Snapshot: encoded,
		Request: []byte(`{"tool":"sandbox.shell"}`), CreatedAt: at,
	}, sqlite.Event{Kind: "state", Payload: []byte(`{"status":"running"}`), OccurredAt: at}); err != nil {
		t.Fatal(err)
	}
}
