package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/tool"
)

type recoveryCLI struct {
	mu               sync.Mutex
	containers       map[string]containerInfo
	listIDs          []string
	listErr          error
	removeErr        error
	stopErr          error
	inspectAfterStop error
	stopped          bool
	removed          []string
}

func (f *recoveryCLI) run(_ context.Context, args ...string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch strings.Join(args[:min(2, len(args))], " ") {
	case "container ls":
		return strings.Join(f.listIDs, "\n"), f.listErr
	case "container inspect":
		if f.stopped && f.inspectAfterStop != nil {
			return "", f.inspectAfterStop
		}
		for _, info := range f.containers {
			if info.ID == args[len(args)-1] || info.Name == "/"+args[len(args)-1] {
				encoded, _ := json.Marshal(info)
				return string(encoded), nil
			}
		}
		return "", errors.New("No such container")
	case "container stop":
		f.stopped = true
		if f.stopErr != nil {
			return "", f.stopErr
		}
		info := f.containers[args[len(args)-1]]
		info.State.Running = false
		info.State.Status = "exited"
		f.containers[info.ID] = info
		return info.ID, nil
	case "container rm":
		if f.removeErr != nil {
			return "", f.removeErr
		}
		id := args[len(args)-1]
		delete(f.containers, id)
		f.removed = append(f.removed, id)
		return id, nil
	}
	return "", errors.New("unexpected Docker command")
}

func (*recoveryCLI) attach(string, io.Writer, io.Writer) (<-chan error, func(), error) {
	return nil, nil, errors.New("unexpected attach")
}

func ownedInfo(s Spec, id, executionID string, running bool) containerInfo {
	info := secureInfo(s)
	info.ID = id
	info.Name = "/toolane-" + s.InstanceID + "-" + executionID
	info.Config.Labels["toolane.execution"] = executionID
	if running {
		info.State.Status = "running"
		info.State.Running = true
	} else {
		info.State.Status = "exited"
	}
	return info
}

func TestRecoverSkipsActiveAndDeletesOwnedLeftover(t *testing.T) {
	s := testSpec(t)
	oldID := strings.Repeat("b", 32)
	activeID := strings.Repeat("c", 32)
	old := ownedInfo(s, "old", oldID, false)
	active := ownedInfo(s, "active", activeID, true)
	f := &recoveryCLI{containers: map[string]containerInfo{old.ID: old, active.ID: active}, listIDs: []string{old.ID, active.ID}}
	b := newBackend(s, f)
	b.active[strings.TrimPrefix(active.Name, "/")] = true
	if err := b.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.removed) != 1 || f.removed[0] != old.ID || !f.containers[active.ID].State.Running {
		t.Fatalf("recovery touched active or foreign work: removed=%v remaining=%v", f.removed, f.containers)
	}
}

func TestRecoverRejectsUnconfirmedStop(t *testing.T) {
	s := testSpec(t)
	info := ownedInfo(s, "running", testExecutionID, true)
	f := &recoveryCLI{containers: map[string]containerInfo{info.ID: info}, listIDs: []string{info.ID}, stopErr: errors.New("daemon unreachable"), inspectAfterStop: errors.New("daemon unreachable")}
	b := newBackend(s, f)
	if err := b.Recover(context.Background()); err == nil || !strings.Contains(err.Error(), "cannot confirm") {
		t.Fatalf("unconfirmed stop accepted: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.removed) != 0 {
		t.Fatalf("unconfirmed container removed: %v", f.removed)
	}
}

func TestConfirmRecoveredResourceChecksPersistedID(t *testing.T) {
	s := testSpec(t)
	id := strings.Repeat("d", 64)
	info := ownedInfo(s, id, testExecutionID, false)
	f := &recoveryCLI{containers: map[string]containerInfo{id: info}}
	b := newBackend(s, f)
	ref := tool.ResourceRef{Kind: tool.ResourceDockerContainer, ID: id, InstanceID: s.InstanceID}
	if err := b.ConfirmRecoveredResource(context.Background(), testExecutionID, ref); err != nil {
		t.Fatalf("stopped owned container rejected: %v", err)
	}
	info.Config.Labels["toolane.instance"] = "other"
	f.containers[id] = info
	if err := b.ConfirmRecoveredResource(context.Background(), testExecutionID, ref); err == nil {
		t.Fatal("persisted ID with changed ownership labels accepted")
	}
	info.Config.Labels["toolane.instance"] = s.InstanceID
	info.State.Running = true
	f.containers[id] = info
	if err := b.ConfirmRecoveredResource(context.Background(), testExecutionID, ref); err == nil {
		t.Fatal("running persisted container accepted")
	}
	delete(f.containers, id)
	if err := b.ConfirmRecoveredResource(context.Background(), testExecutionID, ref); err != nil {
		t.Fatalf("absent persisted container rejected: %v", err)
	}
	ref.InstanceID = "other"
	if err := b.ConfirmRecoveredResource(context.Background(), testExecutionID, ref); err == nil {
		t.Fatal("foreign instance reference accepted")
	}
}

func TestCheckRequiresRecoveryAndImage(t *testing.T) {
	s := testSpec(t)
	f := &fakeCLI{info: secureInfo(s)}
	b := newBackend(s, f)
	if err := b.Check(context.Background()); err == nil {
		t.Fatal("preflight accepted before recovery")
	}
	if err := b.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := b.Check(context.Background()); err != nil {
		t.Fatalf("valid daemon policy rejected: %v", err)
	}
	f.info.Image = ""
	if err := b.Check(context.Background()); err == nil {
		t.Fatal("missing image accepted")
	}
}

func TestUnavailableRecoveryRejectsExecutionBeforeCreate(t *testing.T) {
	s := testSpec(t)
	f := &recoveryCLI{listErr: errors.New("daemon unreachable")}
	b := newBackend(s, f)
	def, err := b.Definition("shell")
	if err != nil {
		t.Fatal(err)
	}
	out, err := def.Execute(context.Background(), json.RawMessage(`{"script":"echo hello"}`), testOptions(s))
	var runErr *tool.RunError
	if !errors.As(err, &runErr) || runErr.Code != "sandbox_recovery_unavailable" || out.CleanupStatus != "" {
		t.Fatalf("unavailable recovery admitted sandbox: outcome=%+v err=%v", out, err)
	}
}

func TestRecoveryRetriesDeletionWithinBound(t *testing.T) {
	s := testSpec(t)
	info := ownedInfo(s, "stopped", testExecutionID, false)
	f := &recoveryCLI{containers: map[string]containerInfo{info.ID: info}, listIDs: []string{info.ID}, removeErr: errors.New("remove unavailable")}
	b := newBackend(s, f)
	b.retryInterval = time.Millisecond
	b.maxRetries = 3
	if err := b.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if pending, _, _ := b.RecoveryStatus(); pending != 1 {
		t.Fatalf("failed deletion not tracked: %d", pending)
	}
	f.mu.Lock()
	f.removeErr = nil
	f.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if pending, blocked, _ := b.RecoveryStatus(); pending != 0 || blocked {
		t.Fatalf("successful retry remained pending: pending=%d blocked=%v", pending, blocked)
	}
}

func TestRecoveryLimitBlocksAdmission(t *testing.T) {
	s := testSpec(t)
	f := &recoveryCLI{containers: make(map[string]containerInfo), removeErr: errors.New("remove unavailable")}
	for i := 0; i < maxResidualContainers; i++ {
		id := fmt.Sprintf("%032x", i+1)
		info := ownedInfo(s, id, id, false)
		f.containers[id] = info
		f.listIDs = append(f.listIDs, id)
	}
	b := newBackend(s, f)
	b.retryInterval = time.Millisecond
	b.maxRetries = 1
	if err := b.Recover(context.Background()); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("residual limit accepted: %v", err)
	}
	if pending, blocked, _ := b.RecoveryStatus(); pending != maxResidualContainers || !blocked {
		t.Fatalf("residual cap not tracked: pending=%d blocked=%v", pending, blocked)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Close(ctx); err == nil {
		t.Fatal("closed with residual containers without error")
	}
}

func TestOverflowStaysBlockedUntilUntrackedResidualIsScanned(t *testing.T) {
	s := testSpec(t)
	f := &recoveryCLI{containers: make(map[string]containerInfo), removeErr: errors.New("remove unavailable")}
	for i := 0; i < maxResidualContainers+1; i++ {
		id := fmt.Sprintf("%032x", i+1)
		info := ownedInfo(s, id, id, false)
		f.containers[id] = info
		f.listIDs = append(f.listIDs, id)
	}
	b := newBackend(s, f)
	b.retryInterval = 50 * time.Millisecond
	b.maxRetries = 1
	if err := b.Recover(context.Background()); err == nil {
		t.Fatal("seventeenth residual was admitted")
	}
	f.mu.Lock()
	f.removeErr = nil
	f.mu.Unlock()
	b.retries.Wait()
	if pending, blocked, _ := b.RecoveryStatus(); pending != 0 || !blocked {
		t.Fatalf("untracked residual lost its admission block: pending=%d blocked=%v", pending, blocked)
	}
	if err := b.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if pending, blocked, _ := b.RecoveryStatus(); pending != 0 || blocked {
		t.Fatalf("rescan did not clear overflow: pending=%d blocked=%v", pending, blocked)
	}
}
