// Package process runs explicitly registered, trusted local executables.
// It is not a sandbox for untrusted code or shell commands.
package process

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"time"

	"github.com/haonox/toolane/internal/tool"
)

// Spec is server-owned policy. Args must produce an argument array, never a
// shell command. A request cannot select the executable, directory or env.
type Spec struct {
	Trusted         bool
	Executable      string
	Args            func(json.RawMessage) ([]string, error)
	ArgsWithOptions func(json.RawMessage, tool.RunOptions) ([]string, error)
	JSONStdout      bool
	Directory       string
	Environment     []string
	GracePeriod     time.Duration
	CleanupTimeout  time.Duration
}

// TrustedDefinition builds a Process tool after checking its fixed policy.
// The binary is checked at execution time so start failures remain observable.
func TrustedDefinition(name string, spec Spec) (tool.Definition, error) {
	if name == "" || !spec.Trusted || !filepath.IsAbs(spec.Executable) || (spec.Args == nil && spec.ArgsWithOptions == nil) || (spec.Args != nil && spec.ArgsWithOptions != nil) {
		return tool.Definition{}, errors.New("process tool requires name, trust flag, absolute executable and argument builder")
	}
	if spec.Directory != "" && !filepath.IsAbs(spec.Directory) {
		return tool.Definition{}, errors.New("process directory must be absolute")
	}
	if spec.GracePeriod < 0 || spec.CleanupTimeout < 0 {
		return tool.Definition{}, errors.New("process cleanup durations cannot be negative")
	}
	if spec.GracePeriod == 0 {
		spec.GracePeriod = 2 * time.Second
	}
	if spec.CleanupTimeout == 0 {
		spec.CleanupTimeout = 5 * time.Second
	}
	if spec.CleanupTimeout <= spec.GracePeriod {
		return tool.Definition{}, errors.New("cleanup timeout must exceed termination grace period")
	}
	// Copy configuration so later changes to a caller's slice do not change a
	// registered tool. The argument builder itself is server-owned code.
	spec.Environment = append([]string{}, spec.Environment...)
	return tool.Definition{
		Name: name,
		Validate: func(raw json.RawMessage) error {
			_, err := buildArgs(spec, raw, tool.RunOptions{ExecutionID: "00000000000000000000000000000000"})
			return err
		},
		Execute: func(ctx context.Context, raw json.RawMessage, options tool.RunOptions) (tool.Outcome, error) {
			return run(ctx, raw, options, spec)
		},
	}, nil
}

func buildArgs(spec Spec, raw json.RawMessage, options tool.RunOptions) ([]string, error) {
	if spec.ArgsWithOptions != nil {
		return spec.ArgsWithOptions(raw, options)
	}
	return spec.Args(raw)
}
