//go:build linux

package process

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"syscall"
	"time"

	"github.com/haonox/toolane/internal/tool"
)

func run(ctx context.Context, raw json.RawMessage, options tool.RunOptions, spec Spec) (tool.Outcome, error) {
	if err := ctx.Err(); err != nil {
		return tool.Outcome{}, err
	}
	args, err := spec.Args(raw)
	if err != nil {
		return tool.Outcome{}, &tool.RunError{Code: "invalid_arguments", Err: err}
	}
	if options.MaxOutputBytes <= 0 || options.OnOutputLimit == nil {
		return tool.Outcome{}, errors.New("process executor requires an output budget and stop callback")
	}
	cmd := exec.Command(spec.Executable, args...)
	cmd.Env = append([]string{}, spec.Environment...)
	cmd.Dir = spec.Directory
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = spec.CleanupTimeout - spec.GracePeriod
	budget := newOutputBudget(options)
	cmd.Stdout = streamWriter{budget: budget}
	cmd.Stderr = streamWriter{budget: budget, stderr: true}
	if err := ctx.Err(); err != nil {
		return tool.Outcome{}, err
	}
	if err := cmd.Start(); err != nil {
		return tool.Outcome{}, &tool.RunError{Code: "process_start_failed", Err: err}
	}
	started := time.Now().UTC()
	if options.OnProcessStarted != nil {
		options.OnProcessStarted(started)
	}
	pgid := cmd.Process.Pid
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	var waitErr error
	var stopErr error
	signal := func(value syscall.Signal) {
		if err := signalGroup(pgid, value); err != nil {
			stopErr = errors.Join(stopErr, err)
			// The direct child must still be stopped even when its group cannot
			// be signalled. Descendants remain unconfirmed in this case.
			_ = cmd.Process.Kill()
		}
	}
	select {
	case waitErr = <-waited:
	case <-ctx.Done():
		// The process group includes ordinary descendants that inherited pipes.
		signal(syscall.SIGTERM)
		timer := time.NewTimer(spec.GracePeriod)
		select {
		case waitErr = <-waited:
			timer.Stop()
		case <-timer.C:
			signal(syscall.SIGKILL)
			waitErr = <-waited
		}
	}
	// A trusted parent can exit while an ordinary child remains in its process
	// group, even if that child closed stdout and stderr. Reap the group on all
	// paths after Wait has observed the parent exit.
	signal(syscall.SIGKILL)
	outcome := budget.outcome()
	outcome.ProcessStartedAt = &started
	if cmd.ProcessState != nil {
		if code := cmd.ProcessState.ExitCode(); code >= 0 {
			outcome.ExitCode = &code
		}
	}
	if stopErr != nil {
		outcome.StopUnconfirmed = true
		return outcome, &tool.RunError{Code: "process_stop_unconfirmed", Err: stopErr}
	}
	if err := ctx.Err(); err != nil {
		return outcome, err
	}
	if waitErr == nil {
		return outcome, nil
	}
	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		return outcome, &tool.RunError{Code: "process_exit_nonzero", Err: waitErr}
	}
	return outcome, &tool.RunError{Code: "process_wait_failed", Err: waitErr}
}

func signalGroup(pgid int, signal syscall.Signal) error {
	err := syscall.Kill(-pgid, signal)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("signal process group %d: %w", pgid, err)
	}
	return nil
}
