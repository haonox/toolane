// Command http generates complete tool calls over Toolane's HTTP API.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
)

type tenant struct {
	ID    string   `json:"tenant_id"`
	Token string   `json:"token"`
	Tools []string `json:"tools"`
}

type tokenFile struct {
	Tokens []tenant `json:"tokens"`
}

type scenario struct {
	Name              string          `json:"name"`
	Tool              string          `json:"tool"`
	Weight            int             `json:"weight"`
	Arguments         json.RawMessage `json:"arguments"`
	ExpectedStatus    string          `json:"expected_status,omitempty"`
	ExpectedErrorCode string          `json:"expected_error_code,omitempty"`
	ExpectedResult    json.RawMessage `json:"expected_result,omitempty"`
	ResultValidation  string          `json:"result_validation,omitempty"`
}

type workload struct {
	Scenarios []scenario `json:"scenarios"`
}

type execution struct {
	ID         string          `json:"execution_id"`
	Status     string          `json:"status"`
	ErrorCode  string          `json:"error_code"`
	Result     json.RawMessage `json:"result"`
	RunningAt  *time.Time      `json:"running_at"`
	FinishedAt *time.Time      `json:"finished_at"`
	CreatedAt  time.Time       `json:"created_at"`
}

type sample struct {
	Tenant      string
	TenantGroup string
	Scenario    string
	Tool        string
	Dropped     bool
	ArrivalLag  time.Duration
	Admission   int
	Status      string
	Error       string
	Submit      time.Duration
	EndToEnd    time.Duration
	Queue       time.Duration
	Execution   time.Duration
	Polls       int
	HasResult   bool
	ResultBytes int
}

type percentile struct {
	P50 float64 `json:"p50_ms"`
	P95 float64 `json:"p95_ms"`
	P99 float64 `json:"p99_ms"`
}

type sizePercentile struct {
	P50 int `json:"p50_bytes"`
	P95 int `json:"p95_bytes"`
	P99 int `json:"p99_bytes"`
}

type tenantCounts struct {
	Submitted     int `json:"submitted"`
	Accepted      int `json:"accepted"`
	Succeeded     int `json:"succeeded"`
	Rejected      int `json:"rejected"`
	Failed        int `json:"failed"`
	ClientDropped int `json:"client_dropped"`
}

type breakdownSummary struct {
	Counts          tenantCounts   `json:"counts"`
	EndToEndLatency percentile     `json:"end_to_end_latency"`
	QueueLatency    percentile     `json:"queue_latency"`
	ExecutionTime   percentile     `json:"execution_time"`
	ResultSize      sizePercentile `json:"result_size"`
}

type scenarioTimings struct {
	endToEnd   []time.Duration
	queue      []time.Duration
	execution  []time.Duration
	resultSize []int
}

type summary struct {
	StartedAt        time.Time                   `json:"started_at"`
	DurationSeconds  float64                     `json:"duration_seconds"`
	BaseURL          string                      `json:"base_url"`
	Tool             string                      `json:"tool"`
	Requests         int                         `json:"requests"`
	Concurrency      int                         `json:"concurrency"`
	ArrivalRate      float64                     `json:"arrival_rate_per_second"`
	HotTenantPct     int                         `json:"hot_tenant_percent"`
	HotCallPct       int                         `json:"hot_call_percent"`
	ClientDropped    int                         `json:"client_capacity_drops"`
	AttemptedPerSec  float64                     `json:"attempted_per_second"`
	SucceededPerSec  float64                     `json:"succeeded_per_second"`
	Admission        map[string]int              `json:"admission_http_status"`
	FinalStatus      map[string]int              `json:"final_status"`
	ResultsPresent   int                         `json:"results_present"`
	ExpectedOutcomes int                         `json:"expected_outcomes"`
	Errors           map[string]int              `json:"errors"`
	PerTenant        map[string]tenantCounts     `json:"per_tenant"`
	PerTenantGroup   map[string]breakdownSummary `json:"per_tenant_group"`
	PerTool          map[string]tenantCounts     `json:"per_tool"`
	PerScenario      map[string]breakdownSummary `json:"per_scenario"`
	SubmitLatency    percentile                  `json:"submit_latency"`
	EndToEndLatency  percentile                  `json:"end_to_end_latency"`
	QueueLatency     percentile                  `json:"queue_latency"`
	ExecutionTime    percentile                  `json:"execution_time"`
	ResultSize       sizePercentile              `json:"result_size"`
	ArrivalLag       percentile                  `json:"arrival_lag"`
	TotalPolls       int                         `json:"total_polls"`
}

type options struct {
	baseURL      string
	pollInterval time.Duration
	waitTimeout  time.Duration
	client       *http.Client
}

func main() {
	var baseURL, tokensPath, workloadPath, outputPath string
	var requests, concurrency, hotTenantPct, hotCallPct int
	var arrivalRate float64
	var pollInterval, waitTimeout, httpTimeout time.Duration
	flag.StringVar(&baseURL, "base-url", "http://127.0.0.1:8080", "Toolane business HTTP origin")
	flag.StringVar(&tokensPath, "tokens-file", "benchmarks/generated/server.json", "private JSON file with a tokens array")
	flag.StringVar(&workloadPath, "workload-file", "benchmarks/config/workload.json", "weighted tool-call workload JSON")
	flag.IntVar(&requests, "requests", 100, "number of complete tool calls")
	flag.IntVar(&concurrency, "concurrency", 16, "maximum concurrent end-to-end tool calls")
	flag.IntVar(&hotTenantPct, "hot-tenant-percent", 0, "percentage of tenants in the hot group; 0 uses uniform tenant rotation")
	flag.IntVar(&hotCallPct, "hot-call-percent", 0, "percentage of calls assigned to the hot tenant group")
	flag.Float64Var(&arrivalRate, "arrival-rate", 0, "scheduled calls per second; 0 uses closed-loop concurrency")
	flag.DurationVar(&pollInterval, "poll-interval", 50*time.Millisecond, "delay between result polls")
	flag.DurationVar(&waitTimeout, "wait-timeout", 30*time.Second, "maximum wait for each accepted execution")
	flag.DurationVar(&httpTimeout, "http-timeout", 10*time.Second, "timeout for one HTTP request")
	flag.StringVar(&outputPath, "output", "", "optional new JSON summary file")
	flag.Parse()
	if err := validate(baseURL, tokensPath, workloadPath, requests, concurrency, hotTenantPct, hotCallPct, arrivalRate, pollInterval, waitTimeout, httpTimeout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	work, err := readWorkload(workloadPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	tenants, err := readTokens(tokensPath, work)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if hotTenantPct > 0 && len(tenants) < 2 {
		fmt.Fprintln(os.Stderr, "hot tenant selection requires at least two tenants")
		os.Exit(2)
	}
	transport := &http.Transport{Proxy: nil, MaxIdleConns: concurrency * 2,
		MaxIdleConnsPerHost: concurrency, MaxConnsPerHost: concurrency * 2,
		DialContext: (&net.Dialer{Timeout: httpTimeout}).DialContext, IdleConnTimeout: 30 * time.Second}
	defer transport.CloseIdleConnections()
	opt := options{baseURL: strings.TrimRight(baseURL, "/"), pollInterval: pollInterval, waitTimeout: waitTimeout,
		client: &http.Client{Transport: transport, Timeout: httpTimeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := checkReady(ctx, opt); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	started := time.Now()
	results := startLoad(ctx, opt, tenants, work, requests, concurrency, hotTenantPct, hotCallPct, arrivalRate, started)
	stats := newSummary(started, opt.baseURL, "weighted_mix", requests, concurrency)
	stats.ArrivalRate, stats.HotTenantPct, stats.HotCallPct = arrivalRate, hotTenantPct, hotCallPct
	var submitTimes, endToEndTimes, queueTimes, executionTimes, arrivalLags []time.Duration
	var resultSizes []int
	scenarioTimes := make(map[string]*scenarioTimings)
	groupTimes := make(map[string]*scenarioTimings)
	for result := range results {
		stats.add(result)
		timings := scenarioTimes[result.Scenario]
		if timings == nil {
			timings = &scenarioTimings{}
			scenarioTimes[result.Scenario] = timings
		}
		groupTiming := groupTimes[result.TenantGroup]
		if groupTiming == nil {
			groupTiming = &scenarioTimings{}
			groupTimes[result.TenantGroup] = groupTiming
		}
		if result.Submit > 0 {
			submitTimes = append(submitTimes, result.Submit)
		}
		if result.EndToEnd > 0 {
			endToEndTimes = append(endToEndTimes, result.EndToEnd)
			timings.endToEnd = append(timings.endToEnd, result.EndToEnd)
			groupTiming.endToEnd = append(groupTiming.endToEnd, result.EndToEnd)
		}
		if result.Queue > 0 {
			queueTimes = append(queueTimes, result.Queue)
			timings.queue = append(timings.queue, result.Queue)
			groupTiming.queue = append(groupTiming.queue, result.Queue)
		}
		if result.Execution > 0 {
			executionTimes = append(executionTimes, result.Execution)
			timings.execution = append(timings.execution, result.Execution)
			groupTiming.execution = append(groupTiming.execution, result.Execution)
		}
		if result.HasResult {
			resultSizes = append(resultSizes, result.ResultBytes)
			timings.resultSize = append(timings.resultSize, result.ResultBytes)
		}
		if arrivalRate > 0 {
			arrivalLags = append(arrivalLags, result.ArrivalLag)
		}
	}
	stats.DurationSeconds = time.Since(started).Seconds()
	if stats.DurationSeconds > 0 {
		stats.AttemptedPerSec = float64(stats.count()) / stats.DurationSeconds
		stats.SucceededPerSec = float64(stats.FinalStatus["succeeded"]) / stats.DurationSeconds
	}
	stats.SubmitLatency = percentiles(submitTimes)
	stats.EndToEndLatency = percentiles(endToEndTimes)
	stats.QueueLatency = percentiles(queueTimes)
	stats.ExecutionTime = percentiles(executionTimes)
	stats.ResultSize = sizePercentiles(resultSizes)
	stats.ArrivalLag = percentiles(arrivalLags)
	for name, timings := range scenarioTimes {
		item := stats.PerScenario[name]
		item.EndToEndLatency = percentiles(timings.endToEnd)
		item.QueueLatency = percentiles(timings.queue)
		item.ExecutionTime = percentiles(timings.execution)
		item.ResultSize = sizePercentiles(timings.resultSize)
		stats.PerScenario[name] = item
	}
	for name, timings := range groupTimes {
		item := stats.PerTenantGroup[name]
		item.EndToEndLatency = percentiles(timings.endToEnd)
		item.QueueLatency = percentiles(timings.queue)
		item.ExecutionTime = percentiles(timings.execution)
		stats.PerTenantGroup[name] = item
	}
	encoded, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(encoded))
	if outputPath != "" {
		if err := save(outputPath, append(encoded, '\n')); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

func startLoad(ctx context.Context, opt options, tenants []tenant, work workload, requests, concurrency, hotTenantPct, hotCallPct int, arrivalRate float64, started time.Time) <-chan sample {
	results := make(chan sample, concurrency)
	if arrivalRate == 0 {
		jobs := make(chan int)
		var clients sync.WaitGroup
		for range concurrency {
			clients.Add(1)
			go func() {
				defer clients.Done()
				for index := range jobs {
					selectedTenant, group := pickTenant(index, tenants, hotTenantPct, hotCallPct)
					result := run(ctx, opt, selectedTenant, work.pick(index))
					result.TenantGroup = group
					results <- result
				}
			}()
		}
		go func() {
			defer close(jobs)
			for index := range requests {
				select {
				case jobs <- index:
				case <-ctx.Done():
					return
				}
			}
		}()
		go func() {
			clients.Wait()
			close(results)
		}()
		return results
	}
	go func() {
		var clients sync.WaitGroup
		defer func() {
			clients.Wait()
			close(results)
		}()
		slots := make(chan struct{}, concurrency)
		for index := range requests {
			planned := started.Add(time.Duration(float64(index) / arrivalRate * float64(time.Second)))
			if wait := time.Until(planned); wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return
				}
			}
			if ctx.Err() != nil {
				return
			}
			lag := max(time.Duration(0), time.Since(planned))
			selectedTenant, group := pickTenant(index, tenants, hotTenantPct, hotCallPct)
			selectedScenario := work.pick(index)
			select {
			case slots <- struct{}{}:
				clients.Add(1)
				go func() {
					defer clients.Done()
					defer func() { <-slots }()
					result := run(ctx, opt, selectedTenant, selectedScenario)
					result.TenantGroup = group
					result.ArrivalLag = lag
					results <- result
				}()
			default:
				results <- sample{Tenant: selectedTenant.ID, TenantGroup: group, Scenario: selectedScenario.Name, Tool: selectedScenario.Tool,
					Dropped: true, ArrivalLag: lag, Error: "client_capacity"}
			}
		}
	}()
	return results
}

func spread(index int) uint64 {
	x := uint64(index) + 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func pickTenant(index int, tenants []tenant, hotTenantPct, hotCallPct int) (tenant, string) {
	if hotTenantPct == 0 {
		return tenants[index%len(tenants)], "uniform"
	}
	hotCount := min(len(tenants)-1, max(1, (len(tenants)*hotTenantPct+99)/100))
	choice := spread(index)
	if int(choice%100) < hotCallPct {
		position := int(spread(index+0x5f3759df) % uint64(hotCount))
		return tenants[position], "hot"
	}
	position := int(spread(index+0x5f3759df) % uint64(len(tenants)-hotCount))
	return tenants[hotCount+position], "cold"
}

func validate(baseURL, tokensPath, workloadPath string, requests, concurrency, hotTenantPct, hotCallPct int, arrivalRate float64, pollInterval, waitTimeout, httpTimeout time.Duration) error {
	parsed, err := url.Parse(baseURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" ||
		parsed.User != nil || parsed.Path != "" && parsed.Path != "/" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("base-url must be an HTTP(S) origin without credentials or path")
	}
	if tokensPath == "" || workloadPath == "" || requests <= 0 || concurrency <= 0 || pollInterval <= 0 || waitTimeout <= 0 || httpTimeout <= 0 {
		return errors.New("tokens-file and workload-file are required; counts and durations must be positive")
	}
	if hotTenantPct != 0 || hotCallPct != 0 {
		if hotTenantPct < 1 || hotTenantPct > 99 || hotCallPct < 1 || hotCallPct > 99 {
			return errors.New("hot tenant and hot call percentages must both be in 1..99")
		}
	}
	if math.IsNaN(arrivalRate) || math.IsInf(arrivalRate, 0) || arrivalRate < 0 || arrivalRate > 1_000_000 {
		return errors.New("arrival rate must be in 0..1000000 calls per second")
	}
	return nil
}

func readWorkload(path string) (workload, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return workload{}, err
	}
	var work workload
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&work); err != nil {
		return workload{}, err
	}
	if len(work.Scenarios) == 0 || len(work.Scenarios) > 100 {
		return workload{}, errors.New("workload needs 1..100 scenarios")
	}
	total := 0
	names := make(map[string]bool, len(work.Scenarios))
	for _, s := range work.Scenarios {
		var args map[string]json.RawMessage
		if strings.TrimSpace(s.Name) == "" || names[s.Name] || s.Tool == "" || s.Weight <= 0 || s.Weight > 10000 || json.Unmarshal(s.Arguments, &args) != nil || args == nil {
			return workload{}, errors.New("each scenario needs a unique name, tool, positive weight and JSON object arguments")
		}
		status := s.ExpectedStatus
		if status == "" {
			status = "succeeded"
		}
		if status != "succeeded" && status != "failed" && status != "timed_out" ||
			(status == "succeeded") != (s.ExpectedErrorCode == "") ||
			(status != "succeeded" && len(s.ExpectedResult) > 0) {
			return workload{}, errors.New("scenario expected status, error code and result are inconsistent")
		}
		if len(s.ExpectedResult) > 0 {
			var expected map[string]json.RawMessage
			if json.Unmarshal(s.ExpectedResult, &expected) != nil || expected == nil {
				return workload{}, errors.New("scenario expected_result must be a JSON object")
			}
		}
		if (s.ResultValidation != "" && s.ResultValidation != "identity" && s.ResultValidation != "expected_fields") ||
			(s.ResultValidation == "expected_fields" && len(s.ExpectedResult) == 0) {
			return workload{}, errors.New("result_validation must be identity or expected_fields; expected_fields requires expected_result")
		}
		names[s.Name] = true
		total += s.Weight
	}
	if total > 10000 {
		return workload{}, errors.New("workload total weight exceeds 10000")
	}
	return work, nil
}

func (w workload) pick(index int) scenario {
	total := 0
	for _, s := range w.Scenarios {
		total += s.Weight
	}
	// SplitMix64 gives a repeatable spread without tying tenant rotation to tool order.
	target := int(spread(index) % uint64(total))
	for _, s := range w.Scenarios {
		if target < s.Weight {
			return s
		}
		target -= s.Weight
	}
	panic("unreachable workload weight")
}

func readTokens(path string, work workload) ([]tenant, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("tokens-file must be a private regular file (chmod 600)")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var config tokenFile
	if err := json.Unmarshal(raw, &config); err != nil {
		return nil, err
	}
	var tenants []tenant
	for _, t := range config.Tokens {
		if t.ID == "" || t.Token == "" {
			return nil, errors.New("each token needs tenant_id and token")
		}
		for _, s := range work.Scenarios {
			if len(t.Tools) > 0 && !slices.Contains(t.Tools, s.Tool) {
				return nil, fmt.Errorf("tenant %s cannot call %s", t.ID, s.Tool)
			}
		}
		tenants = append(tenants, t)
	}
	if len(tenants) == 0 {
		return nil, errors.New("no tenant tokens configured")
	}
	return tenants, nil
}

func checkReady(ctx context.Context, opt options) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, opt.baseURL+"/readyz", nil)
	if err != nil {
		return err
	}
	response, err := opt.client.Do(request)
	if err != nil {
		return fmt.Errorf("readiness check failed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("readiness check returned HTTP %d", response.StatusCode)
	}
	return nil
}

func run(ctx context.Context, opt options, tenant tenant, scenario scenario) sample {
	result := sample{Tenant: tenant.ID, Scenario: scenario.Name, Tool: scenario.Tool}
	start := time.Now()
	requestBody, _ := json.Marshal(struct {
		Tool      string          `json:"tool"`
		Arguments json.RawMessage `json:"arguments"`
	}{Tool: scenario.Tool, Arguments: scenario.Arguments})
	status, body, err := call(ctx, opt.client, http.MethodPost, opt.baseURL+"/api/v1/executions", tenant.Token, requestBody)
	result.Submit = time.Since(start)
	result.Admission = status
	if err != nil {
		result.Error = "submit_transport"
		return result
	}
	if status != http.StatusAccepted {
		result.Error = fmt.Sprintf("admission_http_%d", status)
		return result
	}
	var accepted execution
	if err := json.Unmarshal(body, &accepted); err != nil || accepted.ID == "" {
		result.Error = "invalid_admission_response"
		return result
	}
	deadline := time.NewTimer(opt.waitTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(opt.pollInterval)
	defer ticker.Stop()
	for {
		result.Polls++
		status, body, err = call(ctx, opt.client, http.MethodGet, opt.baseURL+"/api/v1/executions/"+accepted.ID, tenant.Token, nil)
		if err != nil {
			result.Error = "poll_transport"
			return result
		}
		if status != http.StatusOK {
			result.Error = fmt.Sprintf("poll_http_%d", status)
			return result
		}
		var snapshot execution
		if err := json.Unmarshal(body, &snapshot); err != nil || snapshot.ID != accepted.ID {
			result.Error = "invalid_poll_response"
			return result
		}
		switch snapshot.Status {
		case "succeeded", "failed", "timed_out", "cancelled":
			result.Status = snapshot.Status
			result.HasResult = len(snapshot.Result) > 0
			result.ResultBytes = len(snapshot.Result)
			result.EndToEnd = time.Since(start)
			if snapshot.RunningAt != nil {
				result.Queue = snapshot.RunningAt.Sub(snapshot.CreatedAt)
			}
			if snapshot.RunningAt != nil && snapshot.FinishedAt != nil {
				result.Execution = snapshot.FinishedAt.Sub(*snapshot.RunningAt)
			}
			result.Error = validateTerminal(snapshot, scenario, accepted.ID)
			return result
		case "queued", "running", "cancelling":
		default:
			result.Error = "unknown_status"
			return result
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			result.Error = "client_wait_timeout"
			return result
		case <-ctx.Done():
			result.Error = "interrupted"
			return result
		}
	}
}

func validateTerminal(snapshot execution, scenario scenario, executionID string) string {
	wantStatus := scenario.ExpectedStatus
	if wantStatus == "" {
		wantStatus = "succeeded"
	}
	if snapshot.Status != wantStatus {
		return "unexpected_final_status"
	}
	if wantStatus != "succeeded" {
		if snapshot.ErrorCode != scenario.ExpectedErrorCode {
			return "unexpected_error_code"
		}
		return ""
	}
	var actual map[string]any
	if json.Unmarshal(snapshot.Result, &actual) != nil || actual == nil ||
		(scenario.ResultValidation != "expected_fields" && (actual["kind"] != scenario.Tool || actual["execution_id"] != executionID)) {
		return "invalid_tool_result"
	}
	if len(scenario.ExpectedResult) > 0 {
		var expected map[string]any
		if json.Unmarshal(scenario.ExpectedResult, &expected) != nil {
			return "invalid_expected_result"
		}
		for key, value := range expected {
			if !reflect.DeepEqual(actual[key], value) {
				return "unexpected_tool_result"
			}
		}
	}
	return ""
}

func call(ctx context.Context, client *http.Client, method, endpoint, token string, body []byte) (int, []byte, error) {
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 2*1024*1024+1))
	if err != nil || len(raw) > 2*1024*1024 {
		return response.StatusCode, nil, errors.New("response read failed or exceeds 2 MiB")
	}
	return response.StatusCode, raw, nil
}

func newSummary(start time.Time, baseURL, toolName string, requests, concurrency int) summary {
	return summary{StartedAt: start.UTC(), BaseURL: baseURL, Tool: toolName, Requests: requests,
		Concurrency: concurrency, Admission: map[string]int{}, FinalStatus: map[string]int{},
		Errors: map[string]int{}, PerTenant: map[string]tenantCounts{}, PerTool: map[string]tenantCounts{},
		PerScenario: map[string]breakdownSummary{}, PerTenantGroup: map[string]breakdownSummary{}}
}

func (s *summary) add(result sample) {
	counts := s.PerTenant[result.Tenant]
	toolCounts := s.PerTool[result.Tool]
	scenarioStats := s.PerScenario[result.Scenario]
	scenarioCounts := scenarioStats.Counts
	groupStats := s.PerTenantGroup[result.TenantGroup]
	groupCounts := groupStats.Counts
	if result.Dropped {
		s.ClientDropped++
		counts.ClientDropped++
		toolCounts.ClientDropped++
		scenarioCounts.ClientDropped++
		groupCounts.ClientDropped++
		s.Errors[result.Error]++
		s.PerTenant[result.Tenant] = counts
		s.PerTool[result.Tool] = toolCounts
		scenarioStats.Counts = scenarioCounts
		s.PerScenario[result.Scenario] = scenarioStats
		groupStats.Counts = groupCounts
		s.PerTenantGroup[result.TenantGroup] = groupStats
		return
	}
	counts.Submitted++
	toolCounts.Submitted++
	scenarioCounts.Submitted++
	groupCounts.Submitted++
	if result.Admission > 0 {
		s.Admission[fmt.Sprint(result.Admission)]++
	}
	if result.Admission == http.StatusAccepted {
		counts.Accepted++
		toolCounts.Accepted++
		scenarioCounts.Accepted++
		groupCounts.Accepted++
	} else if result.Admission != 0 {
		counts.Rejected++
		toolCounts.Rejected++
		scenarioCounts.Rejected++
		groupCounts.Rejected++
	}
	if result.Status != "" {
		s.FinalStatus[result.Status]++
		if result.Status == "succeeded" {
			counts.Succeeded++
			toolCounts.Succeeded++
			scenarioCounts.Succeeded++
			groupCounts.Succeeded++
		} else {
			counts.Failed++
			toolCounts.Failed++
			scenarioCounts.Failed++
			groupCounts.Failed++
		}
	}
	if result.HasResult {
		s.ResultsPresent++
	}
	if result.Status != "" && result.Error == "" {
		s.ExpectedOutcomes++
	}
	if result.Error != "" {
		s.Errors[result.Error]++
	}
	s.TotalPolls += result.Polls
	s.PerTenant[result.Tenant] = counts
	s.PerTool[result.Tool] = toolCounts
	scenarioStats.Counts = scenarioCounts
	s.PerScenario[result.Scenario] = scenarioStats
	groupStats.Counts = groupCounts
	s.PerTenantGroup[result.TenantGroup] = groupStats
}

func (s summary) count() int {
	count := 0
	for _, tenant := range s.PerTenant {
		count += tenant.Submitted
	}
	return count
}

func percentiles(values []time.Duration) percentile {
	if len(values) == 0 {
		return percentile{}
	}
	slices.Sort(values)
	value := func(p float64) float64 {
		index := int(math.Ceil(p*float64(len(values)))) - 1
		return float64(values[index]) / float64(time.Millisecond)
	}
	return percentile{P50: value(.50), P95: value(.95), P99: value(.99)}
}

func sizePercentiles(values []int) sizePercentile {
	if len(values) == 0 {
		return sizePercentile{}
	}
	slices.Sort(values)
	at := func(p float64) int { return values[int(math.Ceil(p*float64(len(values))))-1] }
	return sizePercentile{P50: at(.50), P95: at(.95), P99: at(.99)}
}

func save(path string, body []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(body)
	return err
}
