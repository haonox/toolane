package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

type containerInfo struct {
	ID     string `json:"Id"`
	Name   string `json:"Name"`
	Image  string `json:"Image"`
	Config struct {
		User       string            `json:"User"`
		WorkingDir string            `json:"WorkingDir"`
		Entrypoint []string          `json:"Entrypoint"`
		Labels     map[string]string `json:"Labels"`
	} `json:"Config"`
	HostConfig struct {
		Privileged     bool              `json:"Privileged"`
		ReadonlyRootfs bool              `json:"ReadonlyRootfs"`
		NetworkMode    string            `json:"NetworkMode"`
		PidMode        string            `json:"PidMode"`
		IpcMode        string            `json:"IpcMode"`
		CgroupnsMode   string            `json:"CgroupnsMode"`
		CapAdd         []string          `json:"CapAdd"`
		CapDrop        []string          `json:"CapDrop"`
		SecurityOpt    []string          `json:"SecurityOpt"`
		PidsLimit      *int64            `json:"PidsLimit"`
		CpuPeriod      int64             `json:"CpuPeriod"`
		CpuQuota       int64             `json:"CpuQuota"`
		Memory         int64             `json:"Memory"`
		MemorySwap     int64             `json:"MemorySwap"`
		Tmpfs          map[string]string `json:"Tmpfs"`
		Binds          []string          `json:"Binds"`
		Mounts         []json.RawMessage `json:"Mounts"`
		Devices        []json.RawMessage `json:"Devices"`
		PortBindings   map[string]any    `json:"PortBindings"`
		AutoRemove     bool              `json:"AutoRemove"`
		LogConfig      struct {
			Type string `json:"Type"`
		} `json:"LogConfig"`
		RestartPolicy struct {
			Name string `json:"Name"`
		} `json:"RestartPolicy"`
	} `json:"HostConfig"`
	Mounts []json.RawMessage `json:"Mounts"`
	State  struct {
		Status    string `json:"Status"`
		Running   bool   `json:"Running"`
		ExitCode  int    `json:"ExitCode"`
		OOMKilled bool   `json:"OOMKilled"`
		StartedAt string `json:"StartedAt"`
		Error     string `json:"Error"`
	} `json:"State"`
}

func (r *runner) inspect(ctx context.Context, id string) (containerInfo, error) {
	output, err := r.cli.run(ctx, "container", "inspect", "--format={{json .}}", id)
	if err != nil {
		return containerInfo{}, err
	}
	var info containerInfo
	if err := json.Unmarshal([]byte(output), &info); err != nil {
		return containerInfo{}, fmt.Errorf("decode container inspection: %w", err)
	}
	if info.ID == "" {
		return containerInfo{}, errors.New("Docker inspection omitted container ID")
	}
	return info, nil
}

func (r *runner) verifyOwnership(info containerInfo, name, executionID string) error {
	if info.Name != "/"+name || info.Config.Labels["toolane.project"] != "toolane" ||
		info.Config.Labels["toolane.instance"] != r.spec.InstanceID || info.Config.Labels["toolane.execution"] != executionID {
		return errors.New("container identity or ownership labels do not match")
	}
	return nil
}

func (r *runner) verifyPolicy(info containerInfo, name, executionID, imageID string) error {
	if err := r.verifyOwnership(info, name, executionID); err != nil {
		return err
	}
	h := info.HostConfig
	if info.Image != imageID || info.Config.User != containerUser || info.Config.WorkingDir != containerWorkdir ||
		!slices.Equal(info.Config.Entrypoint, []string{"/bin/sh"}) {
		return errors.New("container image, user, workdir or entrypoint does not match policy")
	}
	if h.Privileged || !h.ReadonlyRootfs || h.NetworkMode != "none" || h.PidMode != "" || h.IpcMode != "none" || h.CgroupnsMode != "private" ||
		len(h.CapAdd) != 0 || !containsFold(h.CapDrop, "ALL") || !slices.Contains(h.SecurityOpt, "no-new-privileges") || !slices.Contains(h.SecurityOpt, "seccomp=builtin") {
		return errors.New("container privilege or namespace policy does not match")
	}
	resources := r.spec.Resources
	if h.PidsLimit == nil || *h.PidsLimit != containerPIDs || h.CpuPeriod != cpuPeriod || h.CpuQuota != int64(resources.CPUMilli*cpuPeriod/1000) ||
		h.Memory != resources.MemoryBytes || h.MemorySwap != resources.MemoryBytes {
		return errors.New("container CPU, memory, swap or PID limit does not match")
	}
	tmpfs := h.Tmpfs[containerWorkdir]
	for _, option := range []string{"rw", "nosuid", "nodev", "noexec", fmt.Sprintf("size=%d", r.spec.TmpfsBytes), "uid=65532", "gid=65532"} {
		if !hasOption(tmpfs, option) {
			return fmt.Errorf("container workspace tmpfs lacks %s", option)
		}
	}
	if len(h.Tmpfs) != 1 || len(h.Binds) != 0 || len(h.Mounts) != 0 || len(info.Mounts) != 0 || len(h.Devices) != 0 || len(h.PortBindings) != 0 ||
		h.AutoRemove || h.LogConfig.Type != "none" || (h.RestartPolicy.Name != "" && h.RestartPolicy.Name != "no") {
		return errors.New("container mount, device, port, restart or log policy does not match")
	}
	if info.State.Running || info.State.Status != "created" {
		return errors.New("container started before policy verification")
	}
	return nil
}

func containsFold(values []string, want string) bool {
	for _, value := range values {
		if strings.EqualFold(value, want) {
			return true
		}
	}
	return false
}

func hasOption(list, want string) bool {
	return slices.Contains(strings.Split(list, ","), want)
}

func (info containerInfo) startedAt() *time.Time {
	started, err := time.Parse(time.RFC3339Nano, info.State.StartedAt)
	if err != nil || started.IsZero() || started.Year() <= 1 {
		return nil
	}
	started = started.UTC()
	return &started
}
