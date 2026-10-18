package docker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
)

// dockerCLI is deliberately small so lifecycle tests can simulate daemon
// failures without access to a Docker socket.
type dockerCLI interface {
	run(context.Context, ...string) (string, error)
	attach(string, io.Writer, io.Writer) (<-chan error, func(), error)
}

type commandCLI struct{ binary string }

func (c commandCLI) run(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, c.binary, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		message := strings.TrimSpace(stderr.String())
		if message != "" {
			return "", fmt.Errorf("docker %s: %w: %s", strings.Join(args[:min(2, len(args))], " "), err, message)
		}
		return "", fmt.Errorf("docker %s: %w", strings.Join(args[:min(2, len(args))], " "), err)
	}
	return strings.TrimSpace(stdout.String()), nil
}

func (c commandCLI) attach(id string, stdout, stderr io.Writer) (<-chan error, func(), error) {
	cmd := exec.Command(c.binary, "container", "start", "--attach", id)
	cmd.Stdin = nil
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		return nil, nil, err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	terminate := func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	}
	return done, terminate, nil
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "no such object") || strings.Contains(message, "no such container")
}
