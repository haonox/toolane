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
	"sync/atomic"
	"time"

	"github.com/haonox/toolane/internal/execution"
	"github.com/haonox/toolane/internal/executor/mock"
	"github.com/haonox/toolane/internal/httpapi"
	"github.com/haonox/toolane/internal/observability"
	"github.com/haonox/toolane/internal/tool"
)

type serverConfig struct {
	BusinessListen string                    `json:"business_listen"`
	DatabasePath   string                    `json:"database_path,omitempty"`
	Tokens         []httpapi.Token           `json:"tokens"`
	MetricsListen  string                    `json:"metrics_listen,omitempty"`
	ProcessTools   []serverProcessToolConfig `json:"process_tools,omitempty"`
	Execution      *serverExecutionConfig    `json:"execution,omitempty"`
}

// Pointers distinguish omitted settings from explicit zero values, which the
// execution manager rejects as invalid capacities.
type serverExecutionConfig struct {
	Workers            *int `json:"workers,omitempty"`
	QueueCapacity      *int `json:"queue_capacity,omitempty"`
	TenantRunningLimit *int `json:"tenant_running_limit,omitempty"`
	TenantPendingLimit *int `json:"tenant_pending_limit,omitempty"`
	RetainedResults    *int `json:"retained_results,omitempty"`
}

func (c serverConfig) executionConfig() execution.Config {
	config := execution.DefaultConfig()
	if c.Execution == nil {
		return config
	}
	if value := c.Execution.Workers; value != nil {
		config.Workers = *value
	}
	if value := c.Execution.QueueCapacity; value != nil {
		config.QueueCapacity = *value
	}
	if value := c.Execution.TenantRunningLimit; value != nil {
		config.TenantRunningLimit = *value
	}
	if value := c.Execution.TenantPendingLimit; value != nil {
		config.TenantPendingLimit = *value
	}
	if value := c.Execution.RetainedResults; value != nil {
		config.RetainedResults = *value
	}
	return config
}

type transitionLog struct {
	executionID   string
	tenantID      string
	fromState     execution.State
	toState       execution.State
	cleanupStatus string
	errorCode     string
}

const maxServerConfigBytes = 2 * 1024 * 1024

func loadServerConfig(path string) (serverConfig, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return serverConfig{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > maxServerConfigBytes {
		return serverConfig{}, errors.New("server config must be a private regular file of at most 2 MiB")
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
	raw, err := io.ReadAll(io.LimitReader(file, maxServerConfigBytes+1))
	if err != nil {
		return serverConfig{}, err
	}
	if len(raw) > maxServerConfigBytes {
		return serverConfig{}, errors.New("server config exceeds 2 MiB")
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
	if config.BusinessListen == "" {
		config.BusinessListen = "127.0.0.1:8080"
	}
	if config.DatabasePath == "" {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return serverConfig{}, fmt.Errorf("resolve server config path: %w", err)
		}
		config.DatabasePath = filepath.Join(filepath.Dir(absolute), "toolane-state", "executions.sqlite")
	}
	if !filepath.IsAbs(config.DatabasePath) {
		return serverConfig{}, errors.New("database_path must be absolute")
	}
	host, port, err := net.SplitHostPort(config.BusinessListen)
	if err != nil || host != "localhost" && (net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback()) {
		return serverConfig{}, errors.New("business listen address must use localhost or a loopback IP and a port")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 0 || portNumber > 65535 {
		return serverConfig{}, errors.New("invalid business listen port")
	}
	if len(config.Tokens) == 0 {
		return serverConfig{}, errors.New("tokens are required")
	}
	if config.MetricsListen == "" {
		config.MetricsListen = "127.0.0.1:9090"
	}
	metricsHost, metricsPort, err := net.SplitHostPort(config.MetricsListen)
	if err != nil || metricsHost != "localhost" && (net.ParseIP(metricsHost) == nil || !net.ParseIP(metricsHost).IsLoopback()) {
		return serverConfig{}, errors.New("metrics listen address must use localhost or a loopback IP and a port")
	}
	metricsPortNumber, err := strconv.Atoi(metricsPort)
	if err != nil || metricsPortNumber < 0 || metricsPortNumber > 65535 {
		return serverConfig{}, errors.New("invalid metrics listen port")
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

// runServerWithLogger 组装并运行服务；传入 logger 便于测试时检查日志。
func runServerWithLogger(ctx context.Context, configPath string, logger *slog.Logger) (retErr error) {
	// 启动前读取并校验私有配置；失败时不创建 Worker 或监听端口。
	config, err := loadServerConfig(configPath)
	if err != nil {
		return err
	}

	// 注册内置 Mock 工具和管理员配置的 Process 工具。Manager 创建后会固定这份工具集合。
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

	// 状态转换先写入有界通道，再由独立 goroutine 输出日志。
	// 通道满时只丢弃日志事件，避免慢日志写入阻塞任务状态转换。
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

	// Manager 按配置启动 Worker 和排队超时监视器；回调把状态变化记录为指标与日志。
	if err := os.MkdirAll(filepath.Dir(config.DatabasePath), 0700); err != nil {
		return fmt.Errorf("create SQLite directory: %w", err)
	}
	manager, err := execution.NewPersistentManager(config.executionConfig(), registry, config.DatabasePath,
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
	// defer 按后进先出执行：先调用 Manager.Close，再关闭上面的日志通道。
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		retErr = errors.Join(retErr, manager.Close(closeCtx))
	}()

	// HTTP API 将 Token 映射到租户，并通过 Manager 提交、查询和取消任务。
	api, err := httpapi.New(manager, config.Tokens)
	if err != nil {
		return err
	}
	api.EnableMetrics(metrics)
	api.EnableLogger(logger)
	api.EnableToolCatalog(registry.PublicDefinitions())

	// 两个端口都绑定成功后才开始服务；指标端口失败时关闭已绑定的业务端口。
	businessListener, err := net.Listen("tcp", config.BusinessListen)
	if err != nil {
		return err
	}
	metricsListener, err := net.Listen("tcp", config.MetricsListen)
	if err != nil {
		_ = businessListener.Close()
		return fmt.Errorf("listen for management metrics: %w", err)
	}
	// 指标接口使用独立路由，只在 metricsListener 上提供 /metrics。
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
	// 两个 HTTP Server 分别设置请求头、读取和空闲连接的超时。
	metricsServer := &http.Server{Handler: metricsMux, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 8 * 1024}
	server := &http.Server{Handler: api, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		IdleTimeout: 60 * time.Second, MaxHeaderBytes: 8 * 1024}
	// 两个监听循环并发运行；任一服务退出都将错误送回主流程。
	served := make(chan error, 2)
	go func() { served <- server.Serve(businessListener) }()
	go func() { served <- metricsServer.Serve(metricsListener) }()
	logger.Info("service_ready", "business_listen", businessListener.Addr().String())
	logger.Info("metrics_ready", "metrics_listen", metricsListener.Addr().String())
	// 等待外部停止信号，或任一 HTTP 服务意外退出。
	select {
	case <-ctx.Done():
	case err := <-served:
		if !errors.Is(err, http.ErrServerClosed) {
			retErr = errors.Join(retErr, err)
		}
	}
	// 先停止 HTTP 服务；函数返回时再由 defer 关闭 Manager 和日志通道。
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	retErr = errors.Join(retErr, server.Shutdown(shutdownCtx))
	retErr = errors.Join(retErr, metricsServer.Shutdown(shutdownCtx))
	return retErr
}
