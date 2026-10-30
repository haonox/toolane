package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestDiagnosticWorkloadsDeclareAndCheckExpectedOutcomes(t *testing.T) {
	for _, name := range []string{"noop", "io", "fault"} {
		work, err := readWorkload(filepath.Join("..", "config", "workload-"+name+".json"))
		if err != nil {
			t.Fatalf("read %s workload: %v", name, err)
		}
		for _, scenario := range work.Scenarios {
			status := scenario.ExpectedStatus
			if status == "" {
				status = "succeeded"
			}
			snapshot := execution{Status: status, ErrorCode: scenario.ExpectedErrorCode,
				Result: json.RawMessage(`{"kind":"` + scenario.Tool + `","execution_id":"exec-1"}`)}
			if len(scenario.ExpectedResult) > 0 {
				var result map[string]any
				if err := json.Unmarshal(snapshot.Result, &result); err != nil {
					t.Fatal(err)
				}
				var expected map[string]any
				if err := json.Unmarshal(scenario.ExpectedResult, &expected); err != nil {
					t.Fatal(err)
				}
				for key, value := range expected {
					result[key] = value
				}
				snapshot.Result, err = json.Marshal(result)
				if err != nil {
					t.Fatal(err)
				}
			}
			if got := validateTerminal(snapshot, scenario, "exec-1"); got != "" {
				t.Errorf("%s expected outcome rejected: %s", scenario.Name, got)
			}
			if status != "succeeded" {
				snapshot.ErrorCode = "wrong_error"
				if got := validateTerminal(snapshot, scenario, "exec-1"); got != "unexpected_error_code" {
					t.Errorf("%s wrong error code: %s", scenario.Name, got)
				}
			} else {
				if len(scenario.ExpectedResult) > 0 {
					var result map[string]any
					if err := json.Unmarshal(snapshot.Result, &result); err != nil {
						t.Fatal(err)
					}
					for key := range result {
						if key != "kind" && key != "execution_id" {
							result[key] = "incorrect"
							break
						}
					}
					snapshot.Result, err = json.Marshal(result)
					if err != nil {
						t.Fatal(err)
					}
					if got := validateTerminal(snapshot, scenario, "exec-1"); got != "unexpected_tool_result" {
						t.Errorf("%s wrong expected field: %s", scenario.Name, got)
					}
				}
				snapshot.Result = json.RawMessage(`{"kind":"wrong","execution_id":"exec-1"}`)
				if got := validateTerminal(snapshot, scenario, "exec-1"); got != "invalid_tool_result" {
					t.Errorf("%s wrong result: %s", scenario.Name, got)
				}
			}
		}
	}
}

func TestExpectedFieldsValidationForBuiltInTool(t *testing.T) {
	work, err := readWorkload(filepath.Join("..", "config", "workload-mock-zero.json"))
	if err != nil {
		t.Fatal(err)
	}
	scenario := work.Scenarios[0]
	if got := validateTerminal(execution{Status: "succeeded", Result: json.RawMessage(`{"value":"ok"}`)}, scenario, "exec-1"); got != "" {
		t.Fatalf("valid built-in result: %s", got)
	}
	if got := validateTerminal(execution{Status: "succeeded", Result: json.RawMessage(`{"value":"wrong"}`)}, scenario, "exec-1"); got != "unexpected_tool_result" {
		t.Fatalf("wrong built-in result: %s", got)
	}
	if got := validateTerminal(execution{Status: "succeeded", Result: json.RawMessage(`[]`)}, scenario, "exec-1"); got != "invalid_tool_result" {
		t.Fatalf("non-object result: %s", got)
	}
}

func TestTenantSelectionKeepsUniformAndSkewedLoadsDistinct(t *testing.T) {
	tenants := make([]tenant, 100)
	for i := range tenants {
		tenants[i].ID = fmt.Sprintf("tenant-%03d", i)
	}
	for i := range 10_000 {
		selected, group := pickTenant(i, tenants, 0, 0)
		if selected.ID != tenants[i%len(tenants)].ID || group != "uniform" {
			t.Fatalf("uniform selection changed at %d: %q, %q", i, selected.ID, group)
		}
	}
	hotCalls := 0
	for i := range 10_000 {
		_, group := pickTenant(i, tenants, 10, 80)
		if group == "hot" {
			hotCalls++
		}
	}
	if hotCalls < 7_700 || hotCalls > 8_300 {
		t.Fatalf("hot tenant calls = %d, want about 8000", hotCalls)
	}
}

func TestOpenArrivalReportsClientCapacitySeparately(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		status, body := http.StatusOK, `{"execution_id":"accepted","status":"succeeded","result":{"kind":"bench.test","execution_id":"accepted"}}`
		switch request.Method {
		case http.MethodPost:
			<-release
			status, body = http.StatusAccepted, `{"execution_id":"accepted"}`
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	defer unblock()
	opt := options{baseURL: "http://example.invalid", client: client, pollInterval: time.Millisecond, waitTimeout: time.Second}
	work := workload{Scenarios: []scenario{{Name: "single", Tool: "bench.test", Weight: 1, Arguments: json.RawMessage(`{}`)}}}
	results := startLoad(context.Background(), opt, []tenant{{ID: "tenant", Token: "token"}}, work, 5, 1, 0, 0, 10_000, time.Now())
	stats := newSummary(time.Now(), opt.baseURL, "weighted_mix", 5, 1)
	for range 4 {
		select {
		case result := <-results:
			if !result.Dropped {
				t.Fatalf("expected a client capacity drop, got %+v", result)
			}
			stats.add(result)
		case <-time.After(2 * time.Second):
			t.Fatal("open arrival did not report client capacity")
		}
	}
	unblock()
	for result := range results {
		stats.add(result)
	}
	if stats.ClientDropped != 4 || stats.count() != 1 || stats.FinalStatus["succeeded"] != 1 {
		t.Fatalf("client and server outcomes were mixed: %+v", stats)
	}
	if stats.PerTenantGroup["uniform"].Counts.ClientDropped != 4 || stats.PerScenario["single"].Counts.Succeeded != 1 {
		t.Fatalf("breakdown lost client or server outcomes: %+v", stats)
	}
}
