package docker

import (
	"bytes"
	"context"
	"errors"
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

const maxDockerControlOutputBytes = 1024 * 1024

type boundedBuffer struct {
	bytes.Buffer
	overflow bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	remaining := maxDockerControlOutputBytes - b.Len()
	if len(p) > remaining {
		b.overflow = true
		b.Buffer.Write(p[:remaining])
		return len(p), nil
	}
	return b.Buffer.Write(p)
}

func (c commandCLI) run(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, c.binary, args...)
	var stdout, stderr boundedBuffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if stdout.overflow || stderr.overflow {
		return "", errors.New("Docker control output exceeded 1 MiB limit")
	}
	if err != nil {
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
