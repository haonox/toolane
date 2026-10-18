package docker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/tool"
)

const testExecutionID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func testSpec(t *testing.T) Spec {
	t.Helper()
	s, err := (Spec{
		ImageTag: "alpine:3.24.2", ImageDigest: "sha256:" + strings.Repeat("c", 64),
		InstanceID: "unit", DockerBinary: "/usr/bin/docker",
	}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestScriptDefinitionRejectsUntrustedOptions(t *testing.T) {
	s := testSpec(t)
	def, err := scriptDefinition("shell", s, &fakeCLI{})
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		`{"script":""}`, `{"script":"echo ok","image":"other"}`,
		`{"script":"echo ok","privileged":true}`, `{"script":"echo ok"} {"script":"again"}`,
	} {
		if err := def.Validate(json.RawMessage(raw)); err == nil {
			t.Fatalf("unsafe request accepted: %s", raw)
		}
	}
	if err := def.Validate(json.RawMessage(`{"script":"echo ok"}`)); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []Spec{
		{ImageTag: "alpine:latest", ImageDigest: s.ImageDigest, InstanceID: "unit", DockerBinary: s.DockerBinary},
		{ImageTag: "alpine:", ImageDigest: s.ImageDigest, InstanceID: "unit", DockerBinary: s.DockerBinary},
		{ImageTag: ":3.24.2", ImageDigest: s.ImageDigest, InstanceID: "unit", DockerBinary: s.DockerBinary},
		{ImageTag: s.ImageTag, ImageDigest: "sha256:bad", InstanceID: "unit", DockerBinary: s.DockerBinary},
		{ImageTag: s.ImageTag, ImageDigest: s.ImageDigest, InstanceID: "../host", DockerBinary: s.DockerBinary},
	} {
		if _, err := bad.normalized(); err == nil {
			t.Fatalf("invalid server policy accepted: %+v", bad)
		}
	}
}

type fakeCLI struct {
	info       containerInfo
	createArgs []string
	attached   bool
	removed    bool
	removeErr  error
}

func (f *fakeCLI) run(_ context.Context, args ...string) (string, error) {
	switch strings.Join(args[:min(2, len(args))], " ") {
	case "info --format={{.CgroupVersion}}":
		return "2", nil
	case "info --format={{json .SecurityOptions}}":
		return `["name=seccomp,profile=builtin"]`, nil
	case "image inspect":
		return f.info.Image, nil
	case "container create":
		f.createArgs = append([]string(nil), args...)
		return f.info.ID, nil
	case "container inspect":
		encoded, _ := json.Marshal(f.info)
		return string(encoded), nil
	case "container rm":
		f.removed = true
		return "", f.removeErr
	default:
		return "", errors.New("unexpected Docker command: " + strings.Join(args, " "))
	}
}

func (f *fakeCLI) attach(_ string, stdout, _ io.Writer) (<-chan error, func(), error) {
	f.attached = true
	_, _ = io.WriteString(stdout, "hello")
	f.info.State.Status = "exited"
	f.info.State.StartedAt = time.Now().UTC().Format(time.RFC3339Nano)
	done := make(chan error, 1)
	done <- nil
	return done, func() {}, nil
}

func secureInfo(s Spec) containerInfo {
	var info containerInfo
	info.ID = "container-id"
	info.Name = "/toolane-" + s.InstanceID + "-" + testExecutionID
	info.Image = "sha256:" + strings.Repeat("b", 64)
	info.Config.User = containerUser
	info.Config.WorkingDir = containerWorkdir
	info.Config.Entrypoint = []string{"/bin/sh"}
	info.Config.Labels = map[string]string{"toolane.project": "toolane", "toolane.instance": s.InstanceID, "toolane.execution": testExecutionID}
	h := &info.HostConfig
	h.ReadonlyRootfs = true
	h.NetworkMode, h.PidMode, h.IpcMode, h.CgroupnsMode = "none", "", "none", "private"
	h.CapDrop = []string{"ALL"}
	h.SecurityOpt = []string{"no-new-privileges", "seccomp=builtin"}
	pids := int64(containerPIDs)
	h.PidsLimit = &pids
	h.CpuPeriod = cpuPeriod
	h.CpuQuota = int64(s.Resources.CPUMilli * cpuPeriod / 1000)
	h.Memory, h.MemorySwap = s.Resources.MemoryBytes, s.Resources.MemoryBytes
	h.Tmpfs = map[string]string{containerWorkdir: "rw,nosuid,nodev,noexec,size=67108864,uid=65532,gid=65532"}
	h.LogConfig.Type = "none"
	h.RestartPolicy.Name = "no"
	info.State.Status = "created"
	return info
}

func testOptions(s Spec) tool.RunOptions {
	return tool.RunOptions{ExecutionID: testExecutionID, SandboxResources: s.Resources, MaxOutputBytes: 1024, OnOutputLimit: func() {}}
}

func TestPolicyCheckedBeforeStartAndContainerRemoved(t *testing.T) {
	s := testSpec(t)
	f := &fakeCLI{info: secureInfo(s)}
	f.info.HostConfig.ReadonlyRootfs = false
	def, err := scriptDefinition("shell", s, f)
	if err != nil {
		t.Fatal(err)
	}
	out, err := def.Execute(context.Background(), json.RawMessage(`{"script":"echo hello"}`), testOptions(s))
	var runErr *tool.RunError
	if !errors.As(err, &runErr) || runErr.Code != "sandbox_policy_unavailable" {
		t.Fatalf("policy mismatch error = %v", err)
	}
	if f.attached || !f.removed || out.CleanupStatus != "succeeded" {
		t.Fatalf("unsafe container started or leaked: attached=%v removed=%v outcome=%+v", f.attached, f.removed, out)
	}
	for _, flag := range []string{"--pull=never", "--network=none", "--read-only", "--security-opt=seccomp=builtin", "--log-driver=none"} {
		if !strings.Contains(strings.Join(f.createArgs, " "), flag) {
			t.Fatalf("create command missing %s", flag)
		}
	}
}

func TestRemovalFailureIsSeparateFromExecutionResult(t *testing.T) {
	s := testSpec(t)
	f := &fakeCLI{info: secureInfo(s), removeErr: errors.New("remove denied")}
	def, err := scriptDefinition("shell", s, f)
	if err != nil {
		t.Fatal(err)
	}
	out, err := def.Execute(context.Background(), json.RawMessage(`{"script":"echo hello"}`), testOptions(s))
	if err != nil || out.Stdout != "hello" || out.CleanupStatus != "failed" || out.CleanupError == "" || out.StopUnconfirmed {
		t.Fatalf("execution and cleanup conflated: outcome=%+v err=%v", out, err)
	}
}
