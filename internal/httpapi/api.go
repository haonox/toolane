// Package httpapi exposes a tenant-scoped JSON API over a durable Manager.
package httpapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/haonox/toolane/internal/execution"
	"github.com/haonox/toolane/internal/observability"
	"github.com/haonox/toolane/internal/store/sqlite"
	"github.com/haonox/toolane/internal/tool"
)

const maxJSONBytes = 256 * 1024

// Token is an administrator-owned credential and its exact tool allowlist.
// TenantID is never read from a client's JSON request.
type Token struct {
	Value    string   `json:"token"`
	TenantID string   `json:"tenant_id"`
	Tools    []string `json:"tools"`
}

type principal struct {
	tenantID string
	tools    map[string]bool
}

type requestIDKey struct{}
type requestTraceKey struct{}

type requestTrace struct {
	tenantID    string
	executionID string
	workflowID  string
	errorCode   string
}

func traceOf(r *http.Request) *requestTrace {
	trace, _ := r.Context().Value(requestTraceKey{}).(*requestTrace)
	return trace
}

// API owns only HTTP transport state; callers own and close the Manager.
type API struct {
	manager     *execution.Manager
	metrics     *observability.Recorder
	logger      *slog.Logger
	tokens      map[[32]byte]principal
	mux         *http.ServeMux
	sseOptions  SSEOptions
	sseMu       sync.Mutex
	sseTotal    int
	sseTenants  map[string]int
	toolCatalog []tool.PublicDefinition
}

func New(manager *execution.Manager, tokens []Token, options ...SSEOptions) (*API, error) {
	if manager == nil || len(tokens) == 0 {
		return nil, errors.New("HTTP API requires a manager and at least one token")
	}
	if len(options) > 1 {
		return nil, errors.New("at most one SSE options value is allowed")
	}
	sseOptions := DefaultSSEOptions()
	if len(options) == 1 {
		sseOptions = options[0]
	}
	if err := sseOptions.validate(); err != nil {
		return nil, err
	}
	a := &API{manager: manager, tokens: make(map[[32]byte]principal, len(tokens)), mux: http.NewServeMux(),
		sseOptions: sseOptions, sseTenants: make(map[string]int)}
	for _, token := range tokens {
		if len(token.Value) < 16 || len(token.Value) > 4096 || !printableToken(token.Value) || !validTenantID(token.TenantID) || len(token.Tools) == 0 {
			return nil, errors.New("invalid HTTP token configuration")
		}
		hash := sha256.Sum256([]byte(token.Value))
		if _, exists := a.tokens[hash]; exists {
			return nil, errors.New("duplicate HTTP token")
		}
		tools := make(map[string]bool, len(token.Tools))
		for _, name := range token.Tools {
			if name == "" || tools[name] {
				return nil, errors.New("invalid or duplicate token tool")
			}
			tools[name] = true
		}
		a.tokens[hash] = principal{tenantID: token.TenantID, tools: tools}
	}
	a.mux.HandleFunc("GET /healthz", a.health)
	a.mux.HandleFunc("GET /readyz", a.ready)
	a.mux.HandleFunc("POST /api/v1/executions", a.observeAdmission("execution", a.withPrincipal(a.submitExecution)))
	a.mux.HandleFunc("GET /api/v1/tools", a.withPrincipal(a.listTools))
	a.mux.HandleFunc("GET /api/v1/executions/{id}", a.withPrincipal(a.getExecution))
	a.mux.HandleFunc("GET /api/v1/executions/{id}/events", a.withPrincipal(a.streamExecution))
	a.mux.HandleFunc("POST /api/v1/executions/{id}/cancel", a.withPrincipal(a.cancelExecution))
	a.mux.HandleFunc("POST /api/v1/workflows", a.observeAdmission("workflow", a.withPrincipal(a.submitWorkflow)))
	a.mux.HandleFunc("GET /api/v1/workflows/{id}", a.withPrincipal(a.getWorkflow))
	a.mux.HandleFunc("POST /api/v1/workflows/{id}/cancel", a.withPrincipal(a.cancelWorkflow))
	for pattern, allowed := range map[string]string{
		"/healthz": "GET", "/readyz": "GET",
		"/api/v1/executions": "POST", "/api/v1/tools": "GET", "/api/v1/executions/{id}": "GET",
		"/api/v1/executions/{id}/events": "GET",
		"/api/v1/executions/{id}/cancel": "POST",
		"/api/v1/workflows":              "POST", "/api/v1/workflows/{id}": "GET",
		"/api/v1/workflows/{id}/cancel": "POST",
	} {
		a.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Allow", allowed)
			writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		})
	}
	a.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, http.StatusNotFound, "not_found", "route not found")
	})
	return a, nil
}

// EnableToolCatalog installs public descriptions before the API starts serving.
func (a *API) EnableToolCatalog(definitions []tool.PublicDefinition) {
	a.toolCatalog = make([]tool.PublicDefinition, len(definitions))
	for i, definition := range definitions {
		a.toolCatalog[i] = definition
		a.toolCatalog[i].Parameters = append(json.RawMessage(nil), definition.Parameters...)
	}
}

func (a *API) listTools(w http.ResponseWriter, _ *http.Request, p principal) {
	visible := make([]tool.PublicDefinition, 0, len(a.toolCatalog))
	for _, definition := range a.toolCatalog {
		if p.tools[definition.Name] {
			visible = append(visible, definition)
		}
	}
	writeJSON(w, http.StatusOK, struct {
		Tools []tool.PublicDefinition `json:"tools"`
	}{Tools: visible})
}

// EnableMetrics attaches a process-local recorder before ServeHTTP is used.
func (a *API) EnableMetrics(recorder *observability.Recorder) {
	a.metrics = recorder
}

// EnableLogger attaches a structured request logger before ServeHTTP is used.
// Request bodies, raw paths, credentials and tool output are never logged.
func (a *API) EnableLogger(logger *slog.Logger) {
	a.logger = logger
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *statusWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(data)
}

func (a *API) observeAdmission(kind string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.metrics == nil {
			next(w, r)
			return
		}
		wrapped := &statusWriter{ResponseWriter: w}
		next(wrapped, r)
		status := wrapped.status
		if status == 0 {
			status = http.StatusOK
		}
		a.metrics.RecordAdmission(kind, status)
	}
}

func printableToken(token string) bool {
	for i := range len(token) {
		if token[i] < '!' || token[i] > '~' {
			return false
		}
	}
	return true
}

func validTenantID(id string) bool {
	if len(id) == 0 || len(id) > 256 {
		return false
	}
	for i := range len(id) {
		c := id[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' {
			continue
		}
		if i > 0 && (c == '-' || c == '_') {
			continue
		}
		return false
	}
	return true
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(15 * time.Second))
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		http.Error(w, "request ID unavailable", http.StatusInternalServerError)
		return
	}
	id := hex.EncodeToString(raw[:])
	w.Header().Set("X-Request-ID", id)
	trace := &requestTrace{}
	request := r.WithContext(context.WithValue(context.WithValue(r.Context(), requestIDKey{}, id), requestTraceKey{}, trace))
	if a.logger == nil {
		a.mux.ServeHTTP(w, request)
		return
	}
	start := time.Now()
	response := &statusWriter{ResponseWriter: w}
	a.mux.ServeHTTP(response, request)
	status := response.status
	if status == 0 {
		status = http.StatusOK
	}
	attrs := []any{"request_id", id, "method", r.Method, "route", request.Pattern,
		"http_status", status, "duration_ms", time.Since(start).Milliseconds()}
	if trace.tenantID != "" {
		attrs = append(attrs, "tenant_id", trace.tenantID)
	}
	if trace.executionID != "" {
		attrs = append(attrs, "execution_id", trace.executionID)
	}
	if trace.workflowID != "" {
		attrs = append(attrs, "workflow_id", trace.workflowID)
	}
	if trace.errorCode != "" {
		attrs = append(attrs, "error_code", trace.errorCode)
	}
	a.logger.Info("http_request", attrs...)
}

func (a *API) withPrincipal(next func(http.ResponseWriter, *http.Request, principal)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		values := r.Header.Values("Authorization")
		if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
			writeError(w, r, http.StatusUnauthorized, "unauthorized", "valid Bearer token required")
			return
		}
		value := strings.TrimPrefix(values[0], "Bearer ")
		if len(value) > 4096 || !printableToken(value) {
			writeError(w, r, http.StatusUnauthorized, "unauthorized", "valid Bearer token required")
			return
		}
		p, ok := a.tokens[sha256.Sum256([]byte(value))]
		if !ok {
			writeError(w, r, http.StatusUnauthorized, "unauthorized", "valid Bearer token required")
			return
		}
		if trace := traceOf(r); trace != nil {
			trace.tenantID = p.tenantID
		}
		next(w, r, p)
	}
}

type executionInput struct {
	AgentID            string          `json:"agent_id"`
	Tool               string          `json:"tool"`
	Arguments          json.RawMessage `json:"arguments"`
	QueueTimeoutMS     int64           `json:"queue_timeout_ms"`
	ExecutionTimeoutMS int64           `json:"execution_timeout_ms"`
	OutputLimitBytes   int             `json:"output_limit_bytes"`
}

type workflowInput struct {
	Nodes []struct {
		ID                 string          `json:"node_id"`
		DependsOn          []string        `json:"depends_on"`
		AgentID            string          `json:"agent_id"`
		Tool               string          `json:"tool"`
		Arguments          json.RawMessage `json:"arguments"`
		QueueTimeoutMS     int64           `json:"queue_timeout_ms"`
		ExecutionTimeoutMS int64           `json:"execution_timeout_ms"`
		OutputLimitBytes   int             `json:"output_limit_bytes"`
	} `json:"nodes"`
}

func decodeBody(w http.ResponseWriter, r *http.Request, value any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxJSONBytes))
	var raw json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		return err
	}
	if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || trimmed[0] != '{' {
		return errors.New("request body must be a JSON object")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		if err == nil {
			return errors.New("request body must contain one JSON value")
		}
		return err
	}
	strict := json.NewDecoder(bytes.NewReader(raw))
	strict.DisallowUnknownFields()
	return strict.Decode(value)
}

func timeout(ms int64) (time.Duration, error) {
	if ms < 0 || ms > math.MaxInt64/int64(time.Millisecond) {
		return 0, errors.New("timeout is outside allowed range")
	}
	return time.Duration(ms) * time.Millisecond, nil
}

func idempotencyKey(r *http.Request) (string, error) {
	values := r.Header.Values("Idempotency-Key")
	if len(values) == 0 {
		return "", nil
	}
	if len(values) != 1 || len(values[0]) == 0 || len(values[0]) > 128 || !printableToken(values[0]) || strings.Contains(values[0], ",") {
		return "", errors.New("invalid Idempotency-Key")
	}
	return values[0], nil
}

func (a *API) submitExecution(w http.ResponseWriter, r *http.Request, p principal) {
	var input executionInput
	if err := decodeBody(w, r, &input); err != nil {
		writeInputError(w, r, err)
		return
	}
	if !p.tools[input.Tool] {
		writeError(w, r, http.StatusForbidden, "tool_forbidden", "tool is not enabled for this tenant")
		return
	}
	queueTimeout, err := timeout(input.QueueTimeoutMS)
	if err != nil {
		writeInputError(w, r, err)
		return
	}
	runTimeout, err := timeout(input.ExecutionTimeoutMS)
	if err != nil {
		writeInputError(w, r, err)
		return
	}
	key, err := idempotencyKey(r)
	if err != nil {
		writeInputError(w, r, err)
		return
	}
	req := execution.Request{TenantID: p.tenantID, AgentID: input.AgentID, Tool: input.Tool, Arguments: input.Arguments,
		QueueTimeout: queueTimeout, ExecutionTimeout: runTimeout, OutputLimitBytes: input.OutputLimitBytes}
	var snapshot execution.Snapshot
	if key == "" {
		snapshot, err = a.manager.Submit(r.Context(), req)
	} else {
		snapshot, err = a.manager.SubmitIdempotent(r.Context(), req, key)
	}
	if err != nil {
		a.managerError(w, r, err)
		return
	}
	if trace := traceOf(r); trace != nil {
		trace.executionID = snapshot.ID
	}
	writeJSON(w, http.StatusAccepted, viewExecution(snapshot))
}

type executionView struct {
	execution.Snapshot
	Limits executionLimits `json:"limits"`
}

func viewExecution(snapshot execution.Snapshot) executionView {
	return executionView{snapshot, limitsOf(snapshot)}
}

type workflowView struct {
	execution.WorkflowSnapshot
	Nodes []executionView `json:"nodes"`
}

func viewWorkflow(snapshot execution.WorkflowSnapshot) workflowView {
	view := workflowView{WorkflowSnapshot: snapshot, Nodes: make([]executionView, len(snapshot.Nodes))}
	for i, node := range snapshot.Nodes {
		view.Nodes[i] = viewExecution(node)
	}
	return view
}

type executionLimits struct {
	QueueTimeoutMS     int64 `json:"queue_timeout_ms"`
	ExecutionTimeoutMS int64 `json:"execution_timeout_ms"`
	OutputLimitBytes   int   `json:"output_limit_bytes"`
	ArgumentLimitBytes int   `json:"argument_limit_bytes"`
	ResultLimitBytes   int   `json:"result_limit_bytes"`
}

func limitsOf(s execution.Snapshot) executionLimits {
	return executionLimits{int64(s.QueueTimeout / time.Millisecond), int64(s.ExecutionTimeout / time.Millisecond),
		s.OutputLimitBytes, s.ArgumentLimitBytes, s.ResultLimitBytes}
}

func (a *API) submitWorkflow(w http.ResponseWriter, r *http.Request, p principal) {
	var input workflowInput
	if err := decodeBody(w, r, &input); err != nil {
		writeInputError(w, r, err)
		return
	}
	key, err := idempotencyKey(r)
	if err != nil {
		writeInputError(w, r, err)
		return
	}
	req := execution.WorkflowRequest{TenantID: p.tenantID, Nodes: make([]execution.WorkflowNode, len(input.Nodes))}
	for i, node := range input.Nodes {
		if !p.tools[node.Tool] {
			writeError(w, r, http.StatusForbidden, "tool_forbidden", "tool is not enabled for this tenant")
			return
		}
		queueTimeout, err := timeout(node.QueueTimeoutMS)
		if err != nil {
			writeInputError(w, r, fmt.Errorf("node %q: %w", node.ID, err))
			return
		}
		runTimeout, err := timeout(node.ExecutionTimeoutMS)
		if err != nil {
			writeInputError(w, r, fmt.Errorf("node %q: %w", node.ID, err))
			return
		}
		req.Nodes[i] = execution.WorkflowNode{ID: node.ID, DependsOn: node.DependsOn, AgentID: node.AgentID,
			Tool: node.Tool, Arguments: node.Arguments, QueueTimeout: queueTimeout, ExecutionTimeout: runTimeout,
			OutputLimitBytes: node.OutputLimitBytes}
	}
	var snapshot execution.WorkflowSnapshot
	if key == "" {
		snapshot, err = a.manager.SubmitWorkflow(r.Context(), req)
	} else {
		snapshot, err = a.manager.SubmitWorkflowIdempotent(r.Context(), req, key)
	}
	if err != nil {
		a.managerError(w, r, err)
		return
	}
	if trace := traceOf(r); trace != nil {
		trace.workflowID = snapshot.ID
	}
	writeJSON(w, http.StatusAccepted, viewWorkflow(snapshot))
}

func (a *API) getExecution(w http.ResponseWriter, r *http.Request, p principal) {
	snapshot, err := a.manager.Get(r.PathValue("id"))
	if err != nil {
		a.managerError(w, r, err)
		return
	}
	if snapshot.TenantID != p.tenantID {
		writeError(w, r, http.StatusNotFound, "not_found", "execution not found")
		return
	}
	if trace := traceOf(r); trace != nil {
		trace.executionID = snapshot.ID
	}
	writeJSON(w, http.StatusOK, viewExecution(snapshot))
}

func (a *API) cancelExecution(w http.ResponseWriter, r *http.Request, p principal) {
	id := r.PathValue("id")
	before, err := a.manager.Get(id)
	if err != nil {
		a.managerError(w, r, err)
		return
	}
	if before.TenantID != p.tenantID {
		writeError(w, r, http.StatusNotFound, "not_found", "execution not found")
		return
	}
	if trace := traceOf(r); trace != nil {
		trace.executionID = before.ID
	}
	snapshot, err := a.manager.Cancel(id)
	if err != nil {
		a.managerError(w, r, err)
		return
	}
	status := http.StatusOK
	if snapshot.State == execution.Cancelling {
		status = http.StatusAccepted
	}
	writeJSON(w, status, viewExecution(snapshot))
}

func (a *API) getWorkflow(w http.ResponseWriter, r *http.Request, p principal) {
	snapshot, err := a.manager.GetWorkflow(r.PathValue("id"))
	if err != nil {
		a.managerError(w, r, err)
		return
	}
	if snapshot.TenantID != p.tenantID {
		writeError(w, r, http.StatusNotFound, "not_found", "workflow not found")
		return
	}
	if trace := traceOf(r); trace != nil {
		trace.workflowID = snapshot.ID
	}
	writeJSON(w, http.StatusOK, viewWorkflow(snapshot))
}

func (a *API) cancelWorkflow(w http.ResponseWriter, r *http.Request, p principal) {
	id := r.PathValue("id")
	before, err := a.manager.GetWorkflow(id)
	if err != nil {
		a.managerError(w, r, err)
		return
	}
	if before.TenantID != p.tenantID {
		writeError(w, r, http.StatusNotFound, "not_found", "workflow not found")
		return
	}
	if trace := traceOf(r); trace != nil {
		trace.workflowID = before.ID
	}
	snapshot, err := a.manager.CancelWorkflow(id)
	if err != nil {
		a.managerError(w, r, err)
		return
	}
	status := http.StatusOK
	if snapshot.State == execution.Cancelling {
		status = http.StatusAccepted
	}
	writeJSON(w, status, viewWorkflow(snapshot))
}

func (a *API) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) ready(w http.ResponseWriter, r *http.Request) {
	stats := a.manager.Stats()
	if stats.Halted || stats.Closing {
		writeError(w, r, http.StatusServiceUnavailable, "not_ready", "execution manager unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (a *API) managerError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, execution.ErrNotFound):
		writeError(w, r, http.StatusNotFound, "not_found", "resource not found")
	case errors.Is(err, sqlite.ErrIdempotencyConflict):
		writeError(w, r, http.StatusConflict, "idempotency_conflict", "idempotency key conflicts with original request")
	case errors.Is(err, execution.ErrQueueFull), errors.Is(err, execution.ErrTenantQueueFull), errors.Is(err, sqlite.ErrRetentionCapacity):
		w.Header().Set("Retry-After", "1")
		writeError(w, r, http.StatusTooManyRequests, "capacity_exhausted", "admission capacity exhausted")
	case errors.Is(err, execution.ErrBackendUnavailable), errors.Is(err, execution.ErrClosed), a.manager.Stats().Halted:
		writeError(w, r, http.StatusServiceUnavailable, "backend_unavailable", "execution backend unavailable")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeError(w, r, http.StatusRequestTimeout, "request_cancelled", "request ended before admission")
	case errors.Is(err, execution.ErrArgumentsTooLarge):
		writeError(w, r, http.StatusRequestEntityTooLarge, "arguments_too_large", "tool arguments exceed limit")
	case errors.Is(err, execution.ErrInvalidRequest), errors.Is(err, execution.ErrAgentIDTooLarge), errors.Is(err, execution.ErrTenantIDTooLarge), errors.Is(err, execution.ErrSandboxBudgetExceeded):
		writeError(w, r, http.StatusBadRequest, "invalid_request", "request validation failed")
	default:
		writeError(w, r, http.StatusInternalServerError, "internal_error", "internal service error")
	}
}

func writeInputError(w http.ResponseWriter, r *http.Request, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, r, http.StatusRequestEntityTooLarge, "request_too_large", "JSON request exceeds 256 KiB")
		return
	}
	writeError(w, r, http.StatusBadRequest, "invalid_request", err.Error())
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	if trace := traceOf(r); trace != nil {
		trace.errorCode = code
	}
	id, _ := r.Context().Value(requestIDKey{}).(string)
	writeJSON(w, status, struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
	}{code, message, id})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	encoded, err := json.Marshal(value)
	if err != nil {
		status = http.StatusInternalServerError
		encoded, _ = json.Marshal(struct {
			Code      string `json:"code"`
			Message   string `json:"message"`
			RequestID string `json:"request_id"`
		}{"internal_error", "response encoding failed", w.Header().Get("X-Request-ID")})
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(encoded, '\n'))
}
