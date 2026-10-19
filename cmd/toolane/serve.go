package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/haonox/toolane/internal/execution"
	"github.com/haonox/toolane/internal/executor/docker"
	"github.com/haonox/toolane/internal/executor/mock"
	"github.com/haonox/toolane/internal/httpapi"
	"github.com/haonox/toolane/internal/observability"
	"github.com/haonox/toolane/internal/store/sqlite"
	"github.com/haonox/toolane/internal/tool"
)

type serverConfig struct {
	Listen         string              `json:"listen"`
	StateDir       string              `json:"state_dir"`
	Tokens         []httpapi.Token     `json:"tokens"`
	SSETenantLimit int                 `json:"sse_tenant_limit"`
	SSEGlobalLimit int                 `json:"sse_global_limit"`
	MetricsListen  string              `json:"metrics_listen,omitempty"`
	Docker         *serverDockerConfig `json:"docker,omitempty"`
}

type serverDockerConfig struct {
	ImageTag     string `json:"image_tag"`
	ImageDigest  string `json:"image_digest"`
	InstanceID   string `json:"instance_id"`
	DockerBinary string `json:"docker_binary,omitempty"`
}

const sandboxToolName = "sandbox.shell"

type transitionLog struct {
	executionID   string
	workflowID    string
	tenantID      string
	fromState     execution.State
	toState       execution.State
	cleanupStatus string
	errorCode     string
}

func loadServerConfig(path string) (serverConfig, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return serverConfig{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 64*1024 {
		return serverConfig{}, errors.New("server config must be a private regular file of at most 64 KiB")
	}
	file, err := os.Open(path)
	if err != nil {
		return serverConfig{}, err
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil {
		return serverConfig{}, err
	}
	if !os.SameFile(info, openedInfo) || !openedInfo.Mode().IsRegular() || openedInfo.Mode().Perm()&0077 != 0 {
		return serverConfig{}, errors.New("server config changed while opening")
	}
	raw, err := io.ReadAll(io.LimitReader(file, 64*1024+1))
	if err != nil {
		return serverConfig{}, err
	}
	if len(raw) > 64*1024 {
		return serverConfig{}, errors.New("server config exceeds 64 KiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var config serverConfig
	if err := decoder.Decode(&config); err != nil {
		return serverConfig{}, fmt.Errorf("decode server config: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return serverConfig{}, errors.New("server config must contain one JSON object")
	}
	if config.Listen == "" {
		config.Listen = "127.0.0.1:8080"
	}
	host, port, err := net.SplitHostPort(config.Listen)
	if err != nil || host != "localhost" && (net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback()) {
		return serverConfig{}, errors.New("listen address must use localhost or a loopback IP and a port")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 0 || portNumber > 65535 {
		return serverConfig{}, errors.New("invalid listen port")
	}
	if !filepath.IsAbs(config.StateDir) || len(config.Tokens) == 0 {
		return serverConfig{}, errors.New("absolute state_dir and tokens are required")
	}
	defaults := httpapi.DefaultSSEOptions()
	if config.SSETenantLimit == 0 {
		config.SSETenantLimit = defaults.TenantLimit
	}
	if config.SSEGlobalLimit == 0 {
		config.SSEGlobalLimit = defaults.GlobalLimit
	}
	if config.SSETenantLimit < 1 || config.SSEGlobalLimit < config.SSETenantLimit {
		return serverConfig{}, errors.New("invalid SSE connection limits")
	}
	if config.MetricsListen != "" {
		metricsHost, metricsPort, err := net.SplitHostPort(config.MetricsListen)
		if err != nil || metricsHost != "localhost" && (net.ParseIP(metricsHost) == nil || !net.ParseIP(metricsHost).IsLoopback()) {
			return serverConfig{}, errors.New("metrics listen address must use localhost or a loopback IP and a port")
		}
		metricsPortNumber, err := strconv.Atoi(metricsPort)
		if err != nil || metricsPortNumber < 0 || metricsPortNumber > 65535 {
			return serverConfig{}, errors.New("invalid metrics listen port")
		}
	}
	if config.Docker != nil && (config.Docker.ImageTag == "" || config.Docker.ImageDigest == "" || config.Docker.InstanceID == "") {
		return serverConfig{}, errors.New("Docker image tag, digest and stable instance ID are required")
	}
	for _, token := range config.Tokens {
		for _, name := range token.Tools {
			if name != mock.Name && (name != sandboxToolName || config.Docker == nil) {
				return serverConfig{}, errors.New("token names a tool that is not configured")
			}
		}
	}
	return config, nil
}

// runServer assembles the local durable service. Docker is optional and its
// recovery and policy preflight must complete before HTTP admission opens.
func runServer(ctx context.Context, configPath string) (retErr error) {
	return runServerWithLogger(ctx, configPath, slog.New(slog.NewJSONHandler(os.Stderr, nil)))
}

func runServerWithLogger(ctx context.Context, configPath string, logger *slog.Logger) (retErr error) {
	config, err := loadServerConfig(configPath)
	if err != nil {
		return err
	}
	registry := tool.NewRegistry()
	if err := registry.Register(mock.Definition()); err != nil {
		return err
	}
	var dockerBackend *docker.Backend
	if config.Docker != nil {
		dockerBackend, err = docker.NewBackend(docker.Spec{
			ImageTag: config.Docker.ImageTag, ImageDigest: config.Docker.ImageDigest,
			InstanceID: config.Docker.InstanceID, DockerBinary: config.Docker.DockerBinary,
		})
		if err != nil {
			return fmt.Errorf("configure Docker backend: %w", err)
		}
		defer func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			retErr = errors.Join(retErr, dockerBackend.Close(closeCtx))
		}()
		definition, err := dockerBackend.Definition(sandboxToolName)
		if err != nil {
			return err
		}
		if err := registry.Register(definition); err != nil {
			return err
		}
	}
	store, err := sqlite.Open(ctx, config.StateDir)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, store.Close()) }()
	if err := checkDockerInstanceBinding(config.StateDir, config.Docker); err != nil {
		return err
	}
	metrics := &observability.Recorder{}
	transitionEvents := make(chan transitionLog, 1024)
	transitionDone := make(chan struct{})
	var droppedTransitions atomic.Uint64
	go func() {
		defer close(transitionDone)
		for event := range transitionEvents {
			logger.Info("execution_transition", "execution_id", event.executionID, "workflow_id", event.workflowID,
				"tenant_id", event.tenantID, "from_state", event.fromState, "to_state", event.toState,
				"cleanup_status", event.cleanupStatus, "error_code", event.errorCode)
		}
	}()
	defer func() {
		close(transitionEvents)
		<-transitionDone
		if count := droppedTransitions.Load(); count > 0 {
			logger.Warn("transition_logs_dropped", "count", count)
		}
	}()
	var pendingRecovery []execution.RecoveryResource
	manager, err := execution.NewDurableManager(ctx, execution.DefaultConfig(), registry, store,
		func(ctx context.Context, resources []execution.RecoveryResource) error {
			if err := recoverServerResources(ctx, dockerBackend, config.Docker, resources); err != nil {
				return err
			}
			if config.Docker != nil {
				if err := bindDockerInstance(config.StateDir, config.Docker.InstanceID); err != nil {
					return err
				}
				for _, resource := range resources {
					if !resource.AbsentConfirmed {
						pendingRecovery = append(pendingRecovery, resource)
					}
				}
			}
			logger.Info("recovery_complete", "resources_checked", len(resources), "resources_pending", len(pendingRecovery))
			return nil
		}, func(before, after execution.Snapshot) {
			metrics.RecordChange(observability.Change{BeforeState: string(before.State), AfterState: string(after.State),
				BeforeCleanup: before.CleanupStatus, AfterCleanup: after.CleanupStatus, CreatedAt: after.CreatedAt,
				RunningAt: after.RunningAt, ProcessStartedAt: after.ProcessStartedAt, FinishedAt: after.FinishedAt})
			if before.State != after.State || before.CleanupStatus != after.CleanupStatus {
				event := transitionLog{executionID: after.ID, workflowID: after.WorkflowID, tenantID: after.TenantID,
					fromState: before.State, toState: after.State, cleanupStatus: after.CleanupStatus, errorCode: after.ErrorCode}
				select {
				case transitionEvents <- event:
				default:
					droppedTransitions.Add(1)
				}
			}
		})
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		retErr = errors.Join(retErr, manager.Close(closeCtx))
	}()
	if len(pendingRecovery) > 0 {
		watchCtx, watchCancel := context.WithCancel(context.Background())
		watchDone := make(chan struct{})
		go func() {
			defer close(watchDone)
			watchRecoveredCleanup(watchCtx, dockerBackend, manager, pendingRecovery, logger)
		}()
		defer func() {
			watchCancel()
			<-watchDone
		}()
	}
	sseOptions := httpapi.DefaultSSEOptions()
	sseOptions.TenantLimit, sseOptions.GlobalLimit = config.SSETenantLimit, config.SSEGlobalLimit
	api, err := httpapi.New(manager, config.Tokens, sseOptions)
	if err != nil {
		return err
	}
	api.EnableMetrics(metrics)
	api.EnableLogger(logger)
	listener, err := net.Listen("tcp", config.Listen)
	if err != nil {
		return err
	}
	var metricsListener net.Listener
	var metricsServer *http.Server
	if config.MetricsListen != "" {
		metricsListener, err = net.Listen("tcp", config.MetricsListen)
		if err != nil {
			_ = listener.Close()
			return fmt.Errorf("listen for management metrics: %w", err)
		}
		metricsMux := http.NewServeMux()
		metricsMux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
			stats := manager.Stats()
			gauges := observability.Gauges{Queued: stats.Queued, Blocked: stats.Blocked, Pending: stats.Pending, Running: stats.Running,
				Stored: stats.Stored, Goroutines: runtime.NumGoroutine(), ReservedCPUMilli: stats.ReservedSandboxCPUMilli,
				ReservedMemoryBytes: stats.ReservedSandboxMemoryBytes, Halted: stats.Halted}
			if dockerBackend != nil {
				gauges.Residual, gauges.ResidualBlocked, _ = dockerBackend.RecoveryStatus()
			}
			var output bytes.Buffer
			if err := metrics.WritePrometheus(&output, gauges); err != nil {
				http.Error(w, "metrics unavailable", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
			_, _ = w.Write(output.Bytes())
		})
		metricsServer = &http.Server{Handler: metricsMux, ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 8 * 1024}
	}
	server := &http.Server{Handler: api, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		IdleTimeout: 60 * time.Second, MaxHeaderBytes: 8 * 1024}
	served := make(chan error, 2)
	go func() { served <- server.Serve(listener) }()
	if metricsServer != nil {
		go func() { served <- metricsServer.Serve(metricsListener) }()
	}
	logger.Info("service_ready", "http_listen", listener.Addr().String())
	if metricsListener != nil {
		logger.Info("metrics_ready", "metrics_listen", metricsListener.Addr().String())
	}
	select {
	case <-ctx.Done():
	case err := <-served:
		if !errors.Is(err, http.ErrServerClosed) {
			retErr = errors.Join(retErr, err)
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	retErr = errors.Join(retErr, server.Shutdown(shutdownCtx))
	if metricsServer != nil {
		retErr = errors.Join(retErr, metricsServer.Shutdown(shutdownCtx))
	}
	return retErr
}

func recoverServerResources(ctx context.Context, backend *docker.Backend, config *serverDockerConfig, resources []execution.RecoveryResource) error {
	if backend == nil {
		if len(resources) > 0 {
			return errors.New("Docker resources require a configured recovery backend")
		}
		return nil
	}
	if config == nil {
		return errors.New("Docker backend configuration is missing")
	}
	for _, resource := range resources {
		if resource.Ref.Kind != tool.ResourceDockerContainer || resource.Ref.InstanceID != config.InstanceID {
			return fmt.Errorf("persisted Docker resource for execution %s belongs to another backend", resource.ExecutionID)
		}
	}
	if err := backend.Recover(ctx); err != nil {
		return fmt.Errorf("recover Docker containers: %w", err)
	}
	for i := range resources {
		absent, err := backend.ConfirmRecoveredResource(ctx, resources[i].ExecutionID, resources[i].Ref)
		if err != nil {
			return fmt.Errorf("confirm recovered execution %s: %w", resources[i].ExecutionID, err)
		}
		resources[i].AbsentConfirmed = absent
	}
	if err := backend.Check(ctx); err != nil {
		return fmt.Errorf("check Docker sandbox policy: %w", err)
	}
	return nil
}

// watchRecoveredCleanup follows bounded Docker retry work without retaining a
// running execution slot. A later restart also rechecks any still-pending ID.
func watchRecoveredCleanup(ctx context.Context, backend *docker.Backend, manager *execution.Manager, resources []execution.RecoveryResource, logger *slog.Logger) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for len(resources) > 0 {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			remaining := resources[:0]
			for _, resource := range resources {
				absent, err := backend.ConfirmRecoveredResource(ctx, resource.ExecutionID, resource.Ref)
				if err != nil || !absent {
					remaining = append(remaining, resource)
					continue
				}
				if err := manager.ConfirmRecoveredCleanup(resource.ExecutionID, resource.Ref); err != nil {
					remaining = append(remaining, resource)
				} else {
					logger.Info("recovered_cleanup_confirmed", "execution_id", resource.ExecutionID)
				}
			}
			resources = remaining
		}
	}
}

const dockerInstanceFile = ".docker-instance-id"

// boundDockerInstance reads the stable owner stored in the private, locked
// state directory. An incomplete file fails closed after a write interruption.
func boundDockerInstance(stateDir string) (string, error) {
	path := filepath.Join(stateDir, dockerInstanceFile)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() < 2 || info.Size() > 33 {
		return "", errors.New("Docker instance binding is not a private regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return "", errors.New("Docker instance binding changed while opening")
	}
	raw, err := io.ReadAll(io.LimitReader(file, 34))
	if err != nil {
		return "", err
	}
	if len(raw) < 2 || raw[len(raw)-1] != '\n' || strings.ContainsRune(string(raw[:len(raw)-1]), '\n') {
		return "", errors.New("Docker instance binding is incomplete")
	}
	return string(raw[:len(raw)-1]), nil
}

func checkDockerInstanceBinding(stateDir string, config *serverDockerConfig) error {
	bound, err := boundDockerInstance(stateDir)
	if err != nil || bound == "" {
		return err
	}
	if config == nil || config.InstanceID != bound {
		return errors.New("Docker instance ID changed or Docker backend was removed from this state directory")
	}
	return nil
}

// bindDockerInstance runs after successful recovery and before workers start.
// Syncing the file and directory makes the identity durable before admission.
func bindDockerInstance(stateDir, instanceID string) error {
	bound, err := boundDockerInstance(stateDir)
	if err != nil {
		return err
	}
	if bound != "" {
		if bound != instanceID {
			return errors.New("Docker instance ID differs from the state directory binding")
		}
		return nil
	}
	path := filepath.Join(stateDir, dockerInstanceFile)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		return checkDockerInstanceBinding(stateDir, &serverDockerConfig{InstanceID: instanceID})
	}
	if err != nil {
		return err
	}
	if _, err := io.WriteString(file, instanceID+"\n"); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	directory, err := os.Open(stateDir)
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return err
	}
	return checkDockerInstanceBinding(stateDir, &serverDockerConfig{InstanceID: instanceID})
}
