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
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/haonox/toolane/internal/execution"
	"github.com/haonox/toolane/internal/executor/mock"
	"github.com/haonox/toolane/internal/httpapi"
	"github.com/haonox/toolane/internal/observability"
	"github.com/haonox/toolane/internal/tool"
)

type serverConfig struct {
	Listen        string                    `json:"listen"`
	Tokens        []httpapi.Token           `json:"tokens"`
	MetricsListen string                    `json:"metrics_listen,omitempty"`
	ProcessTools  []serverProcessToolConfig `json:"process_tools,omitempty"`
}

type transitionLog struct {
	executionID   string
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
	if len(config.Tokens) == 0 {
		return serverConfig{}, errors.New("tokens are required")
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
	configured := map[string]bool{mock.Name: true}
	for _, processConfig := range config.ProcessTools {
		if configured[processConfig.Name] {
			return serverConfig{}, fmt.Errorf("duplicate tool name %q", processConfig.Name)
		}
		if _, err := processToolDefinition(processConfig); err != nil {
			return serverConfig{}, err
		}
		configured[processConfig.Name] = true
	}
	for _, token := range config.Tokens {
		for _, name := range token.Tools {
			if !configured[name] {
				return serverConfig{}, errors.New("token names a tool that is not configured")
			}
		}
	}
	return config, nil
}

// runServer assembles the local service.
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
	for _, processConfig := range config.ProcessTools {
		definition, err := processToolDefinition(processConfig)
		if err != nil {
			return err
		}
		if err := registry.Register(definition); err != nil {
			return err
		}
	}
	metrics := &observability.Recorder{}
	transitionEvents := make(chan transitionLog, 1024)
	transitionDone := make(chan struct{})
	var droppedTransitions atomic.Uint64
	go func() {
		defer close(transitionDone)
		for event := range transitionEvents {
			logger.Info("execution_transition", "execution_id", event.executionID,
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
	manager, err := execution.NewManager(execution.DefaultConfig(), registry,
		func(before, after execution.Snapshot) {
			metrics.RecordChange(observability.Change{BeforeState: string(before.State), AfterState: string(after.State),
				BeforeCleanup: before.CleanupStatus, AfterCleanup: after.CleanupStatus, CreatedAt: after.CreatedAt,
				RunningAt: after.RunningAt, ProcessStartedAt: after.ProcessStartedAt, FinishedAt: after.FinishedAt})
			if before.State != after.State || before.CleanupStatus != after.CleanupStatus {
				event := transitionLog{executionID: after.ID, tenantID: after.TenantID,
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
	api, err := httpapi.New(manager, config.Tokens)
	if err != nil {
		return err
	}
	api.EnableMetrics(metrics)
	api.EnableLogger(logger)
	api.EnableToolCatalog(registry.PublicDefinitions())
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
			gauges := observability.Gauges{Queued: stats.Queued, Pending: stats.Pending, Running: stats.Running,
				Stored: stats.Stored, Goroutines: runtime.NumGoroutine(), Halted: stats.Halted}
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
