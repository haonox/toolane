package docker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/haonox/toolane/internal/tool"
)

func (r *runner) run(ctx context.Context, script string, options tool.RunOptions) (out tool.Outcome, runErr error) {
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if !executionIDPattern.MatchString(options.ExecutionID) || options.SandboxResources != r.spec.Resources || options.MaxOutputBytes <= 0 || options.OnOutputLimit == nil {
		return out, errors.New("sandbox requires execution ID, matching reservation and output budget")
	}
	imageID, err := r.preflight(ctx)
	if err != nil {
		return out, err
	}
	name := "toolane-" + r.spec.InstanceID + "-" + options.ExecutionID
	budget := newOutputBudget(options.MaxOutputBytes, options.OnOutputLimit)
	var attached <-chan error
	var terminate func()
	attachWaited := false
	var attachErr error
	created := false
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), r.spec.CleanupTimeout)
		info, found, cleanupStatus, stopUnconfirmed, cleanupErr := r.cleanup(cleanupCtx, name, options.ExecutionID, created)
		cancel()
		if attached != nil && !attachWaited {
			select {
			case attachErr = <-attached:
			case <-time.After(time.Second):
				terminate()
				select {
				case attachErr = <-attached:
				case <-time.After(time.Second):
					attachErr = errors.New("Docker attach did not exit after termination")
				}
			}
		}
		captured := budget.outcome()
		out.Stdout, out.Stderr, out.OutputTruncated = captured.Stdout, captured.Stderr, captured.OutputTruncated
		out.CleanupStatus = cleanupStatus
		if cleanupErr != nil {
			out.CleanupError = cleanupErr.Error()
		}
		if found {
			out.ProcessStartedAt = info.startedAt()
			if out.ProcessStartedAt != nil && options.OnProcessStarted != nil {
				options.OnProcessStarted(*out.ProcessStartedAt)
			}
			if info.State.Status != "created" {
				code := info.State.ExitCode
				out.ExitCode = &code
			}
		}
		if stopUnconfirmed {
			out.StopUnconfirmed = true
			runErr = &tool.RunError{Code: "sandbox_stop_unconfirmed", Err: errors.Join(runErr, cleanupErr)}
			return
		}
		if runErr != nil || !created {
			return
		}
		if !found {
			runErr = &tool.RunError{Code: "sandbox_inspect_failed", Err: errors.New("container disappeared before result inspection")}
			return
		}
		if err := ctx.Err(); err != nil {
			runErr = err
		} else if info.State.OOMKilled {
			runErr = &tool.RunError{Code: "resource_exhausted", Err: errors.New("container was OOM killed")}
		} else if info.State.ExitCode != 0 {
			runErr = &tool.RunError{Code: "container_exit_nonzero", Err: fmt.Errorf("container exited with code %d", info.State.ExitCode)}
		} else if attachErr != nil {
			runErr = &tool.RunError{Code: "sandbox_attach_failed", Err: attachErr}
		}
	}()
	id, err := r.cli.run(ctx, r.createArgs(name, options.ExecutionID, script)...)
	if err != nil {
		return out, &tool.RunError{Code: "sandbox_create_failed", Err: err}
	}
	created = true
	info, err := r.inspect(ctx, id)
	if err != nil {
		return out, &tool.RunError{Code: "sandbox_policy_unavailable", Err: err}
	}
	if info.ID != id {
		return out, &tool.RunError{Code: "sandbox_policy_unavailable", Err: errors.New("created container ID does not match inspection")}
	}
	if err := r.verifyPolicy(info, name, options.ExecutionID, imageID); err != nil {
		return out, &tool.RunError{Code: "sandbox_policy_unavailable", Err: err}
	}
	attached, terminate, err = r.cli.attach(id, streamWriter{budget: budget}, streamWriter{budget: budget, stderr: true})
	if err != nil {
		return out, &tool.RunError{Code: "sandbox_start_failed", Err: err}
	}
	select {
	case attachErr = <-attached:
		attachWaited = true
	case <-ctx.Done():
		return out, ctx.Err()
	}
	return out, nil
}

// cleanup uses a fresh bounded context and never acts on a container unless
// its deterministic name and all ownership labels match this execution.
func (r *runner) cleanup(ctx context.Context, name, executionID string, created bool) (containerInfo, bool, string, bool, error) {
	info, err := r.inspect(ctx, name)
	if isNotFound(err) {
		if created {
			return containerInfo{}, false, "succeeded", false, nil
		}
		return containerInfo{}, false, "not_needed", false, nil
	}
	if err != nil {
		return containerInfo{}, false, "failed", true, fmt.Errorf("inspect sandbox during cleanup: %w", err)
	}
	if err := r.verifyOwnership(info, name, executionID); err != nil {
		return info, true, "failed", true, err
	}
	if info.State.Running {
		_, stopErr := r.cli.run(ctx, "container", "stop", "--time="+fmt.Sprint(int(r.spec.GracePeriod/time.Second)), info.ID)
		info, err = r.inspect(ctx, info.ID)
		if err != nil {
			return containerInfo{}, false, "failed", true, errors.Join(stopErr, err)
		}
		if err := r.verifyOwnership(info, name, executionID); err != nil {
			return info, true, "failed", true, err
		}
		if info.State.Running {
			_, killErr := r.cli.run(ctx, "container", "kill", info.ID)
			info, err = r.inspect(ctx, info.ID)
			if err != nil {
				return containerInfo{}, false, "failed", true, errors.Join(stopErr, killErr, err)
			}
			if err := r.verifyOwnership(info, name, executionID); err != nil {
				return info, true, "failed", true, err
			}
			if info.State.Running {
				return info, true, "failed", true, errors.Join(stopErr, killErr, errors.New("container still running"))
			}
		}
	}
	if _, err := r.cli.run(ctx, "container", "rm", "--volumes", info.ID); err != nil && !isNotFound(err) {
		return info, true, "failed", false, fmt.Errorf("remove stopped sandbox: %w", err)
	}
	return info, true, "succeeded", false, nil
}

type outputBudget struct {
	mu        sync.Mutex
	remaining int
	stdout    []byte
	stderr    []byte
	truncated bool
	once      sync.Once
	onLimit   func()
}

func newOutputBudget(max int, onLimit func()) *outputBudget {
	return &outputBudget{remaining: max, onLimit: onLimit}
}

type streamWriter struct {
	budget *outputBudget
	stderr bool
}

func (w streamWriter) Write(p []byte) (int, error) {
	b := w.budget
	b.mu.Lock()
	n := min(len(p), b.remaining)
	if w.stderr {
		b.stderr = append(b.stderr, p[:n]...)
	} else {
		b.stdout = append(b.stdout, p[:n]...)
	}
	b.remaining -= n
	reached := b.remaining == 0
	if reached {
		b.truncated = true
	}
	b.mu.Unlock()
	if reached {
		b.once.Do(b.onLimit)
	}
	return len(p), nil
}

func (b *outputBudget) outcome() tool.Outcome {
	b.mu.Lock()
	defer b.mu.Unlock()
	return tool.Outcome{Stdout: strings.ToValidUTF8(string(b.stdout), "?"), Stderr: strings.ToValidUTF8(string(b.stderr), "?"), OutputTruncated: b.truncated}
}

var _ io.Writer = streamWriter{}
