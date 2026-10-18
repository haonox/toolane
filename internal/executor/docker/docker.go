// Package docker runs server-registered shell scripts in constrained Docker
// containers. It trusts the host, daemon, and administrator-owned image.
package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/haonox/toolane/internal/tool"
)

const (
	defaultMemoryBytes = 256 * 1024 * 1024
	defaultTmpfsBytes  = 64 * 1024 * 1024
	containerUser      = "65532:65532"
	containerWorkdir   = "/workspace"
	containerPIDs      = 64
	cpuPeriod          = 100000
)

var (
	instancePattern    = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,31}$`)
	digestPattern      = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
	executionIDPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)
)

// Spec is entirely administrator-owned. InstanceID must remain stable across
// restarts of one deployment so later recovery can find its containers.
type Spec struct {
	ImageTag       string
	ImageDigest    string
	InstanceID     string
	DockerBinary   string
	Resources      tool.Resources
	TmpfsBytes     int64
	GracePeriod    time.Duration
	CleanupTimeout time.Duration
}

func (s Spec) normalized() (Spec, error) {
	if !instancePattern.MatchString(s.InstanceID) {
		return Spec{}, errors.New("sandbox instance ID must be 1-32 lowercase letters, digits, underscores or hyphens")
	}
	colon := strings.LastIndex(s.ImageTag, ":")
	if colon <= 0 || colon <= strings.LastIndex(s.ImageTag, "/") || colon == len(s.ImageTag)-1 || strings.ContainsAny(s.ImageTag, "@ \t\n") || strings.HasSuffix(s.ImageTag, ":latest") || !digestPattern.MatchString(s.ImageDigest) {
		return Spec{}, errors.New("sandbox image requires a fixed tag and sha256 digest")
	}
	if s.Resources == (tool.Resources{}) {
		s.Resources = tool.Resources{CPUMilli: 1000, MemoryBytes: defaultMemoryBytes}
	}
	if err := s.Resources.Validate(); err != nil {
		return Spec{}, err
	}
	if s.Resources.CPUMilli == 0 || s.Resources.CPUMilli > 1000 || s.Resources.MemoryBytes > defaultMemoryBytes {
		return Spec{}, errors.New("sandbox resources must fit within 1000 millicores and 256 MiB")
	}
	if s.TmpfsBytes == 0 {
		s.TmpfsBytes = defaultTmpfsBytes
	}
	if s.TmpfsBytes < 0 || s.TmpfsBytes > s.Resources.MemoryBytes {
		return Spec{}, errors.New("sandbox tmpfs must fit within memory reservation")
	}
	if s.GracePeriod == 0 {
		s.GracePeriod = 2 * time.Second
	}
	if s.CleanupTimeout == 0 {
		s.CleanupTimeout = 5 * time.Second
	}
	if s.GracePeriod < time.Second || s.GracePeriod%time.Second != 0 || s.CleanupTimeout <= s.GracePeriod {
		return Spec{}, errors.New("sandbox cleanup timeout must exceed whole-second grace period")
	}
	if s.DockerBinary == "" {
		path, err := exec.LookPath("docker")
		if err != nil {
			return Spec{}, fmt.Errorf("find Docker CLI: %w", err)
		}
		s.DockerBinary = path
	}
	if !filepath.IsAbs(s.DockerBinary) {
		return Spec{}, errors.New("Docker binary must be an absolute path")
	}
	return s, nil
}

func (s Spec) imageRef() string { return s.ImageTag + "@" + s.ImageDigest }

// ScriptDefinition registers one fixed image and policy. The request contains
// only a script; it cannot select image, flags, mounts, environment or limits.
func ScriptDefinition(name string, spec Spec) (tool.Definition, error) {
	backend, err := NewBackend(spec)
	if err != nil {
		return tool.Definition{}, err
	}
	return backend.Definition(name)
}

func scriptDefinition(name string, spec Spec, cli dockerCLI) (tool.Definition, error) {
	if name == "" || cli == nil {
		return tool.Definition{}, errors.New("sandbox requires a name and Docker client")
	}
	executor := &runner{spec: spec, cli: cli}
	return tool.Definition{
		Name:      name,
		Resources: spec.Resources,
		Validate: func(raw json.RawMessage) error {
			_, err := parseScript(raw)
			return err
		},
		Execute: func(ctx context.Context, raw json.RawMessage, options tool.RunOptions) (tool.Outcome, error) {
			script, err := parseScript(raw)
			if err != nil {
				return tool.Outcome{}, &tool.RunError{Code: "invalid_arguments", Err: err}
			}
			return executor.run(ctx, script, options)
		},
	}, nil
}

func parseScript(raw json.RawMessage) (string, error) {
	var value struct {
		Script string `json:"script"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return "", err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return "", errors.New("sandbox expects exactly one JSON object")
	}
	if strings.TrimSpace(value.Script) == "" || strings.ContainsRune(value.Script, 0) {
		return "", errors.New("sandbox script must be nonempty and contain no NUL byte")
	}
	return value.Script, nil
}

type runner struct {
	spec Spec
	cli  dockerCLI
}

func (r *runner) preflight(ctx context.Context) (string, error) {
	version, err := r.cli.run(ctx, "info", "--format={{.CgroupVersion}}")
	if err != nil {
		return "", &tool.RunError{Code: "sandbox_backend_unavailable", Err: err}
	}
	if version != "2" {
		return "", &tool.RunError{Code: "sandbox_policy_unavailable", Err: fmt.Errorf("Docker cgroup v2 required, got %q", version)}
	}
	security, err := r.cli.run(ctx, "info", "--format={{json .SecurityOptions}}")
	if err != nil {
		return "", &tool.RunError{Code: "sandbox_backend_unavailable", Err: err}
	}
	var options []string
	if err := json.Unmarshal([]byte(security), &options); err != nil {
		return "", &tool.RunError{Code: "sandbox_policy_unavailable", Err: err}
	}
	seccomp := false
	for _, option := range options {
		if strings.HasPrefix(option, "name=seccomp") {
			seccomp = true
		}
	}
	if !seccomp {
		return "", &tool.RunError{Code: "sandbox_policy_unavailable", Err: errors.New("Docker daemon lacks seccomp support")}
	}
	imageID, err := r.cli.run(ctx, "image", "inspect", "--format={{.Id}}", r.spec.imageRef())
	if err != nil {
		return "", &tool.RunError{Code: "sandbox_image_missing", Err: err}
	}
	if !strings.HasPrefix(imageID, "sha256:") {
		return "", &tool.RunError{Code: "sandbox_policy_unavailable", Err: errors.New("Docker returned invalid image ID")}
	}
	return imageID, nil
}

func (r *runner) createArgs(name, executionID, script string) []string {
	resources := r.spec.Resources
	return []string{
		"container", "create", "--pull=never", "--name=" + name,
		"--label=toolane.project=toolane",
		"--label=toolane.instance=" + r.spec.InstanceID,
		"--label=toolane.execution=" + executionID,
		"--network=none", "--ipc=none", "--cgroupns=private",
		"--read-only", "--user=" + containerUser, "--cap-drop=ALL",
		"--attach=stdout", "--attach=stderr",
		"--security-opt=no-new-privileges", "--security-opt=seccomp=builtin",
		"--pids-limit=" + strconv.Itoa(containerPIDs),
		"--cpu-period=" + strconv.Itoa(cpuPeriod),
		"--cpu-quota=" + strconv.Itoa(resources.CPUMilli*cpuPeriod/1000),
		"--memory=" + strconv.FormatInt(resources.MemoryBytes, 10),
		"--memory-swap=" + strconv.FormatInt(resources.MemoryBytes, 10),
		"--tmpfs=" + containerWorkdir + ":rw,nosuid,nodev,noexec,size=" + strconv.FormatInt(r.spec.TmpfsBytes, 10) + ",uid=65532,gid=65532",
		"--workdir=" + containerWorkdir,
		"--log-driver=none", "--no-healthcheck", "--stop-timeout=" + strconv.Itoa(int(r.spec.GracePeriod/time.Second)),
		"--entrypoint=/bin/sh", r.spec.imageRef(), "-c", script,
	}
}
