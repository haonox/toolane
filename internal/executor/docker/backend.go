package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/haonox/toolane/internal/tool"
)

const (
	maxResidualContainers = 16
	maxCleanupRetries     = 3
	cleanupRetryInterval  = time.Second
)

type residual struct {
	name        string
	executionID string
	onUpdate    func(string, string)
}

// Backend owns one image policy and deployment instance. Reuse it for tools
// sharing that policy; a fresh Backend on restart reclaims labeled leftovers.
type Backend struct {
	spec          Spec
	cli           dockerCLI
	recoveryMu    sync.Mutex
	mu            sync.Mutex
	active        map[string]bool
	pending       map[string]residual
	initialized   bool
	overflow      bool
	closed        bool
	closeDone     chan struct{}
	lastError     string
	retries       sync.WaitGroup
	retryInterval time.Duration
	maxRetries    int
}

// NewBackend creates a Docker backend without contacting the daemon.
func NewBackend(spec Spec) (*Backend, error) {
	s, err := spec.normalized()
	if err != nil {
		return nil, err
	}
	return newBackend(s, commandCLI{binary: s.DockerBinary}), nil
}

func newBackend(spec Spec, cli dockerCLI) *Backend {
	return &Backend{spec: spec, cli: cli, active: make(map[string]bool), pending: make(map[string]residual), retryInterval: cleanupRetryInterval, maxRetries: maxCleanupRetries}
}

// Definition registers a fixed-image Shell tool using this backend.
func (b *Backend) Definition(name string) (tool.Definition, error) {
	def, err := scriptDefinition(name, b.spec, b.cli)
	if err != nil {
		return tool.Definition{}, err
	}
	execute := def.Execute
	def.Execute = func(ctx context.Context, raw json.RawMessage, options tool.RunOptions) (tool.Outcome, error) {
		if !executionIDPattern.MatchString(options.ExecutionID) {
			return tool.Outcome{}, &tool.RunError{Code: "invalid_arguments", Err: errors.New("invalid execution ID")}
		}
		if err := b.ensureRecovered(ctx); err != nil {
			return tool.Outcome{}, &tool.RunError{Code: "sandbox_recovery_unavailable", Err: err}
		}
		name := "toolane-" + b.spec.InstanceID + "-" + options.ExecutionID
		b.mu.Lock()
		if b.closed || b.overflow || len(b.pending) >= maxResidualContainers || b.active[name] {
			b.mu.Unlock()
			return tool.Outcome{}, &tool.RunError{Code: "sandbox_recovery_limit", Err: errors.New("Docker backend closed or residual container limit reached")}
		}
		b.active[name] = true
		b.mu.Unlock()
		defer func() {
			b.mu.Lock()
			delete(b.active, name)
			b.mu.Unlock()
		}()
		out, runErr := execute(ctx, raw, options)
		if out.CleanupStatus == "failed" && !out.StopUnconfirmed {
			if b.schedule(residual{name: name, executionID: options.ExecutionID, onUpdate: options.OnCleanupUpdate}) {
				out.CleanupStatus = "pending"
			}
		}
		return out, runErr
	}
	return def, nil
}

func (b *Backend) ensureRecovered(ctx context.Context) error {
	b.mu.Lock()
	ready := b.initialized && !b.overflow && len(b.pending) < maxResidualContainers
	b.mu.Unlock()
	if ready {
		return nil
	}
	return b.Recover(ctx)
}

// Recover scans only this deployment's labeled containers. Call it during
// startup before accepting work; active tasks on this Backend are skipped.
func (b *Backend) Recover(ctx context.Context) error {
	b.recoveryMu.Lock()
	defer b.recoveryMu.Unlock()
	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	if closed {
		return errors.New("Docker backend is closed")
	}
	ids, err := b.cli.run(ctx, "container", "ls", "--all", "--quiet",
		"--filter=label=toolane.project=toolane", "--filter=label=toolane.instance="+b.spec.InstanceID)
	if err != nil {
		b.recordError(err)
		return fmt.Errorf("list owned sandbox containers: %w", err)
	}
	fields := strings.Fields(ids)
	r := &runner{spec: b.spec, cli: b.cli}
	for _, id := range fields {
		info, err := r.inspect(ctx, id)
		if isNotFound(err) {
			continue
		}
		if err != nil {
			b.recordError(err)
			return fmt.Errorf("inspect owned sandbox: %w", err)
		}
		name := strings.TrimPrefix(info.Name, "/")
		prefix := "toolane-" + b.spec.InstanceID + "-"
		executionID := strings.TrimPrefix(name, prefix)
		if !strings.HasPrefix(name, prefix) || !executionIDPattern.MatchString(executionID) {
			return fmt.Errorf("unexpected owned sandbox name %q", name)
		}
		if err := r.verifyOwnership(info, name, executionID); err != nil {
			return err
		}
		b.mu.Lock()
		active := b.active[name]
		b.mu.Unlock()
		if active {
			continue
		}
		cleanupCtx, cancel := context.WithTimeout(ctx, b.spec.CleanupTimeout)
		_, _, status, unconfirmed, cleanupErr := r.cleanup(cleanupCtx, name, executionID, true)
		cancel()
		if unconfirmed {
			b.recordError(cleanupErr)
			return fmt.Errorf("cannot confirm residual sandbox stopped: %w", cleanupErr)
		}
		if status == "failed" {
			b.schedule(residual{name: name, executionID: executionID})
		} else {
			b.mu.Lock()
			delete(b.pending, name)
			b.mu.Unlock()
		}
	}
	b.mu.Lock()
	b.initialized = true
	b.overflow = len(b.pending) >= maxResidualContainers
	blocked := b.overflow
	b.mu.Unlock()
	if blocked {
		return errors.New("residual sandbox container limit reached")
	}
	return nil
}

// Check verifies the configured image and daemon isolation features before a
// service opens sandbox admission. Recover must run first.
func (b *Backend) Check(ctx context.Context) error {
	b.mu.Lock()
	ready := b.initialized && !b.closed && !b.overflow
	b.mu.Unlock()
	if !ready {
		return errors.New("Docker backend must recover before preflight")
	}
	_, err := (&runner{spec: b.spec, cli: b.cli}).preflight(ctx)
	return err
}

// ConfirmRecoveredResource checks a persisted container ID independently of
// the label scan. The bool reports that the container is absent, which is the
// only condition that permits a durable cleanup record to be finalized. An
// existing container must still belong to this execution and be stopped.
func (b *Backend) ConfirmRecoveredResource(ctx context.Context, executionID string, ref tool.ResourceRef) (bool, error) {
	if !executionIDPattern.MatchString(executionID) || ref.Kind != tool.ResourceDockerContainer ||
		ref.InstanceID != b.spec.InstanceID || !containerIDPattern.MatchString(ref.ID) {
		return false, errors.New("invalid persisted Docker resource ownership")
	}
	r := &runner{spec: b.spec, cli: b.cli}
	info, err := r.inspect(ctx, ref.ID)
	if isNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect persisted sandbox: %w", err)
	}
	name := "toolane-" + b.spec.InstanceID + "-" + executionID
	if info.ID != ref.ID {
		return false, errors.New("persisted sandbox ID does not match inspection")
	}
	if err := r.verifyOwnership(info, name, executionID); err != nil {
		return false, fmt.Errorf("persisted sandbox ownership: %w", err)
	}
	if info.State.Running {
		return false, errors.New("persisted sandbox is still running after recovery")
	}
	return false, nil
}

func (b *Backend) schedule(item residual) bool {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return false
	}
	if b.pending[item.name].name != "" {
		b.mu.Unlock()
		return true
	}
	if len(b.pending) >= maxResidualContainers {
		b.overflow = true
		b.mu.Unlock()
		return false
	}
	b.pending[item.name] = item
	if len(b.pending) >= maxResidualContainers {
		b.overflow = true
	}
	b.retries.Add(1)
	b.mu.Unlock()
	go func() {
		defer b.retries.Done()
		r := &runner{spec: b.spec, cli: b.cli}
		for attempt := 0; attempt < b.maxRetries; attempt++ {
			timer := time.NewTimer(b.retryInterval)
			<-timer.C
			ctx, cancel := context.WithTimeout(context.Background(), b.spec.CleanupTimeout)
			_, _, status, _, err := r.cleanup(ctx, item.name, item.executionID, true)
			cancel()
			if status == "succeeded" {
				b.mu.Lock()
				delete(b.pending, item.name)
				b.mu.Unlock()
				if item.onUpdate != nil {
					item.onUpdate("succeeded", "")
				}
				return
			}
			b.recordError(err)
		}
		if item.onUpdate != nil {
			item.onUpdate("failed", b.recoveryError())
		}
	}()
	return true
}

func (b *Backend) recoveryError() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastError
}

func (b *Backend) recordError(err error) {
	if err == nil {
		return
	}
	b.mu.Lock()
	b.lastError = err.Error()
	b.mu.Unlock()
}

// RecoveryStatus exposes bounded residual work and its latest error.
func (b *Backend) RecoveryStatus() (pending int, blocked bool, lastError string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pending), b.overflow, b.lastError
}

// Close waits for bounded background retries. Call Manager.Close first.
func (b *Backend) Close(ctx context.Context) error {
	b.mu.Lock()
	first := !b.closed
	if first {
		b.closed = true
		b.closeDone = make(chan struct{})
	}
	done := b.closeDone
	b.mu.Unlock()
	if first {
		go func() { b.retries.Wait(); close(done) }()
	}
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.pending) > 0 || b.overflow {
		return fmt.Errorf("sandbox recovery incomplete (tracked=%d, overflow=%v): %s", len(b.pending), b.overflow, b.lastError)
	}
	return nil
}
