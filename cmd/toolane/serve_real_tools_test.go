package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/haonox/toolane/internal/execution"
	"github.com/haonox/toolane/internal/httpapi"
)

const fixtureTokenA = "fixture-tenant-a-secret-123456"
const fixtureTokenB = "fixture-tenant-b-secret-123456"

func TestRealFixtureGeneratorProducesValidServerConfig(t *testing.T) {
	python, err := exec.LookPath("python3")
	if errors.Is(err, exec.ErrNotFound) {
		t.Skip("python3 is needed for the real Process fixtures")
	}
	if err != nil {
		t.Fatal(err)
	}
	generator, err := filepath.Abs(filepath.Join("..", "..", "tests", "fixtures", "make_config.py"))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	configPath := filepath.Join(dir, "server.json")
	command := exec.Command(python, generator, "--config", configPath, "--state-dir", filepath.Join(dir, "state"), "--output-dir", filepath.Join(dir, "outputs"))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("fixture config generator: %v: %s", err, output)
	}
	config, err := loadServerConfig(configPath)
	if err != nil {
		t.Fatalf("generated config is not accepted by Toolane: %v", err)
	}
	if len(config.ProcessTools) != 3 || len(config.Tokens) != 2 || config.Tokens[0].Value == config.Tokens[1].Value {
		t.Fatalf("wrong generated fixture set: %+v", config)
	}
	info, err := os.Stat(configPath)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("generated config is not private: %v %v", info, err)
	}
}

func realFixtureConfig(t *testing.T, dir, address, queryOrigin string) (string, string, string) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if errors.Is(err, exec.ErrNotFound) {
		t.Skip("python3 is needed for the real Process fixtures")
	}
	if err != nil {
		t.Fatal(err)
	}
	python, err = filepath.Abs(python)
	if err != nil {
		t.Fatal(err)
	}
	script, err := filepath.Abs(filepath.Join("..", "..", "tests", "fixtures", "process_tools.py"))
	if err != nil {
		t.Fatal(err)
	}
	outputDir := filepath.Join(dir, "outputs")
	if err := os.Mkdir(outputDir, 0700); err != nil {
		t.Fatal(err)
	}
	literal := func(value string) processArgumentConfig { return processArgumentConfig{Literal: &value} }
	makeTool := func(name, mode string, schema string, bindings ...processArgumentConfig) serverProcessToolConfig {
		return serverProcessToolConfig{
			Name: name, Description: "Reproducible " + mode + " test fixture", Parameters: json.RawMessage(schema),
			Executable: python, Directory: filepath.Dir(script),
			Environment:  map[string]string{"PYTHONDONTWRITEBYTECODE": "1", "TOOLANE_FIXTURE_OUTPUT_DIR": outputDir, "TOOLANE_FIXTURE_QUERY_ORIGIN": queryOrigin},
			Args:         append([]processArgumentConfig{literal(script), literal(mode)}, bindings...),
			RunTimeoutMS: 5000, MaxOutputBytes: 8192, MaxResultBytes: 8192,
		}
	}
	config := serverConfig{
		Listen: address, StateDir: filepath.Join(dir, "state"),
		ProcessTools: []serverProcessToolConfig{
			makeTool("data.summarize", "summarize", `{"type":"object","properties":{"repetitions":{"type":"integer","minimum":1,"maximum":200000}},"required":["repetitions"],"additionalProperties":false}`, processArgumentConfig{Parameter: "repetitions"}, processArgumentConfig{ExecutionID: true}),
			makeTool("service.query", "query", `{"type":"object","properties":{"delay_ms":{"type":"integer","minimum":0,"maximum":1000},"status":{"type":"integer","minimum":200,"maximum":599}},"required":["delay_ms","status"],"additionalProperties":false}`, processArgumentConfig{Parameter: "delay_ms"}, processArgumentConfig{Parameter: "status"}, processArgumentConfig{ExecutionID: true}),
			makeTool("report.generate", "report", `{"type":"object","properties":{"duration_ms":{"type":"integer","minimum":1000,"maximum":3000},"label":{"type":"string","minLength":1,"maxLength":80}},"required":["duration_ms","label"],"additionalProperties":false}`, processArgumentConfig{Parameter: "duration_ms"}, processArgumentConfig{Parameter: "label"}, processArgumentConfig{ExecutionID: true}),
		},
		Tokens: []httpapi.Token{
			{Value: fixtureTokenA, TenantID: "fixtureA", Tools: []string{"data.summarize", "service.query", "report.generate"}},
			{Value: fixtureTokenB, TenantID: "fixtureB", Tools: []string{"data.summarize", "service.query"}},
		},
	}
	configPath := filepath.Join(dir, "server.json")
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return configPath, outputDir, script
}

func fixtureResult(t *testing.T, snapshot execution.Snapshot) map[string]any {
	t.Helper()
	if snapshot.State != execution.Succeeded {
		t.Fatalf("%s state=%s code=%s stderr=%q", snapshot.ID, snapshot.State, snapshot.ErrorCode, snapshot.Stderr)
	}
	var result map[string]any
	if err := json.Unmarshal(snapshot.Result, &result); err != nil {
		t.Fatalf("%s result=%s: %v", snapshot.ID, snapshot.Result, err)
	}
	if result["execution_id"] != snapshot.ID {
		t.Fatalf("result does not match execution: %+v", result)
	}
	return result
}

func fixtureLedger(t *testing.T, outputDir string) []map[string]string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(outputDir, "ledger.jsonl"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var records []map[string]string
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var record map[string]string
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("invalid independent ledger line %q: %v", line, err)
		}
		records = append(records, record)
	}
	return records
}

func awaitFixtureFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture did not create %s", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRealProcessFixturesThroughHTTP(t *testing.T) {
	dir := t.TempDir()
	address := processTestAddress(t)
	queryService := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/query" {
			http.NotFound(w, r)
			return
		}
		delay, err := strconv.Atoi(r.URL.Query().Get("delay_ms"))
		if err != nil || delay < 0 || delay > 1000 {
			http.Error(w, "bad delay", http.StatusBadRequest)
			return
		}
		status, err := strconv.Atoi(r.URL.Query().Get("status"))
		if err != nil || status < 200 || status > 599 {
			http.Error(w, "bad status", http.StatusBadRequest)
			return
		}
		time.Sleep(time.Duration(delay) * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = fmt.Fprintf(w, `{"delay_ms":%d,"status":%d}`, delay, status)
	}))
	t.Cleanup(queryService.Close)
	configPath, outputDir, script := realFixtureConfig(t, dir, address, queryService.URL)
	base, client, stop := startProcessTestServer(t, configPath, address)
	t.Cleanup(stop)

	status, catalog := processRequest(t, client, http.MethodGet, base+"/api/v1/tools", fixtureTokenB, "", "")
	if status != http.StatusOK || !strings.Contains(string(catalog), `"name":"data.summarize"`) || !strings.Contains(string(catalog), `"name":"service.query"`) || strings.Contains(string(catalog), "report.generate") || strings.Contains(string(catalog), script) {
		t.Fatalf("incorrect public catalog status=%d body=%s", status, catalog)
	}
	status, raw := processRequest(t, client, http.MethodPost, base+"/api/v1/executions", fixtureTokenA, `{"tool":"data.summarize","arguments":{"repetitions":0}}`, "")
	if status != http.StatusBadRequest {
		t.Fatalf("invalid parameters accepted: %d %s", status, raw)
	}

	for _, token := range []string{fixtureTokenA, fixtureTokenB} {
		accepted := processSubmit(t, client, base, token, `{"tool":"data.summarize","arguments":{"repetitions":2}}`, "same-summary-key")
		final := awaitProcessSnapshot(t, client, base, token, accepted.ID, func(s execution.Snapshot) bool { return s.FinishedAt != nil })
		result := fixtureResult(t, final)
		data, err := os.ReadFile(filepath.Join(filepath.Dir(script), "orders.csv"))
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(data)
		if result["rows"] != float64(12) || result["total"] != float64(144) || result["dataset_sha256"] != hex.EncodeToString(digest[:]) {
			t.Fatalf("wrong dataset computation: %+v", result)
		}
		if result["totals"].(map[string]any)["A"] != float64(48) || result["totals"].(map[string]any)["B"] != float64(48) || result["totals"].(map[string]any)["C"] != float64(48) {
			t.Fatalf("wrong per-tenant totals: %+v", result)
		}
		status, events := processRequest(t, client, http.MethodGet, base+"/api/v1/executions/"+accepted.ID+"/events", token, "", "")
		if status != http.StatusOK || !strings.Contains(string(events), "event: end\n") || !strings.Contains(string(events), `"status":"succeeded"`) {
			t.Fatalf("real tool terminal SSE missing: status=%d events=%s", status, events)
		}
		other := fixtureTokenB
		if token == fixtureTokenB {
			other = fixtureTokenA
		}
		for _, request := range []struct{ method, suffix string }{{http.MethodGet, ""}, {http.MethodPost, "/cancel"}, {http.MethodGet, "/events"}} {
			status, raw := processRequest(t, client, request.method, base+"/api/v1/executions/"+accepted.ID+request.suffix, other, "", "")
			if status != http.StatusNotFound {
				t.Fatalf("cross-tenant %s %s: %d %s", request.method, request.suffix, status, raw)
			}
		}
	}

	query := processSubmit(t, client, base, fixtureTokenB, `{"tool":"service.query","arguments":{"delay_ms":50,"status":200}}`, "")
	queryResult := fixtureResult(t, awaitProcessSnapshot(t, client, base, fixtureTokenB, query.ID, func(s execution.Snapshot) bool { return s.FinishedAt != nil }))
	response := queryResult["response"].(map[string]any)
	if response["delay_ms"] != float64(50) || response["status"] != float64(200) {
		t.Fatalf("wrong HTTP fixture result: %+v", queryResult)
	}
	failed := processSubmit(t, client, base, fixtureTokenB, `{"tool":"service.query","arguments":{"delay_ms":0,"status":503}}`, "")
	failedFinal := awaitProcessSnapshot(t, client, base, fixtureTokenB, failed.ID, func(s execution.Snapshot) bool { return s.FinishedAt != nil })
	if failedFinal.State != execution.Failed || failedFinal.ErrorCode != "process_exit_nonzero" {
		t.Fatalf("HTTP failure not propagated: %+v", failedFinal)
	}
	timed := processSubmit(t, client, base, fixtureTokenB, `{"tool":"service.query","arguments":{"delay_ms":500,"status":200},"execution_timeout_ms":100}`, "")
	timedFinal := awaitProcessSnapshot(t, client, base, fixtureTokenB, timed.ID, func(s execution.Snapshot) bool { return s.FinishedAt != nil })
	if timedFinal.State != execution.TimedOut {
		t.Fatalf("HTTP timeout not propagated: %+v", timedFinal)
	}
	limited := processSubmit(t, client, base, fixtureTokenB, `{"tool":"data.summarize","arguments":{"repetitions":1},"output_limit_bytes":16}`, "")
	limitedFinal := awaitProcessSnapshot(t, client, base, fixtureTokenB, limited.ID, func(s execution.Snapshot) bool { return s.FinishedAt != nil })
	if limitedFinal.State != execution.Failed || limitedFinal.ErrorCode != "output_limit_exceeded" || !limitedFinal.OutputTruncated {
		t.Fatalf("real tool output limit not enforced: %+v", limitedFinal)
	}
	blockers := make([]execution.Snapshot, 2)
	for i := range blockers {
		blockers[i] = processSubmit(t, client, base, fixtureTokenB, `{"tool":"service.query","arguments":{"delay_ms":1000,"status":200}}`, "")
	}
	for _, blocker := range blockers {
		awaitProcessSnapshot(t, client, base, fixtureTokenB, blocker.ID, func(s execution.Snapshot) bool { return s.ProcessStartedAt != nil })
	}
	queued := processSubmit(t, client, base, fixtureTokenB, `{"tool":"data.summarize","arguments":{"repetitions":1},"queue_timeout_ms":100}`, "")
	queuedFinal := awaitProcessSnapshot(t, client, base, fixtureTokenB, queued.ID, func(s execution.Snapshot) bool { return s.FinishedAt != nil })
	if queuedFinal.State != execution.TimedOut || queuedFinal.ErrorCode != "queue_timeout" || queuedFinal.ProcessStartedAt != nil {
		t.Fatalf("queued real tool did not time out before launch: %+v", queuedFinal)
	}
	for _, blocker := range blockers {
		status, raw = processRequest(t, client, http.MethodPost, base+"/api/v1/executions/"+blocker.ID+"/cancel", fixtureTokenB, "", "")
		if status != http.StatusOK && status != http.StatusAccepted {
			t.Fatalf("cancel query blocker: %d %s", status, raw)
		}
		awaitProcessSnapshot(t, client, base, fixtureTokenB, blocker.ID, func(s execution.Snapshot) bool { return s.FinishedAt != nil })
	}

	status, raw = processRequest(t, client, http.MethodPost, base+"/api/v1/executions", fixtureTokenB, `{"tool":"report.generate","arguments":{"duration_ms":1000,"label":"denied"}}`, "")
	if status != http.StatusForbidden {
		t.Fatalf("unauthorized report accepted: %d %s", status, raw)
	}
	first := processSubmit(t, client, base, fixtureTokenA, `{"tool":"report.generate","arguments":{"duration_ms":1000,"label":"first"}}`, "report-key")
	replay := processSubmit(t, client, base, fixtureTokenA, `{"tool":"report.generate","arguments":{"duration_ms":1000,"label":"first"}}`, "report-key")
	if replay.ID != first.ID {
		t.Fatalf("idempotent report produced a second execution: %s %s", first.ID, replay.ID)
	}
	firstFinal := awaitProcessSnapshot(t, client, base, fixtureTokenA, first.ID, func(s execution.Snapshot) bool { return s.FinishedAt != nil })
	firstResult := fixtureResult(t, firstFinal)
	reportBytes, err := os.ReadFile(filepath.Join(outputDir, first.ID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var report map[string]string
	if err := json.Unmarshal(reportBytes, &report); err != nil || report["execution_id"] != first.ID || report["label"] != "first" {
		t.Fatalf("wrong independent report: %+v err=%v", report, err)
	}
	records := fixtureLedger(t, outputDir)
	digest := sha256.Sum256(reportBytes)
	if len(records) != 1 || records[0]["execution_id"] != first.ID || records[0]["report_sha256"] != hex.EncodeToString(digest[:]) || firstResult["report_sha256"] != hex.EncodeToString(digest[:]) {
		t.Fatalf("result, report and ledger disagree: result=%+v ledger=%+v", firstResult, records)
	}

	cancelled := processSubmit(t, client, base, fixtureTokenA, `{"tool":"report.generate","arguments":{"duration_ms":3000,"label":"cancelled"}}`, "")
	awaitFixtureFile(t, filepath.Join(outputDir, cancelled.ID+".started"))
	status, raw = processRequest(t, client, http.MethodPost, base+"/api/v1/executions/"+cancelled.ID+"/cancel", fixtureTokenA, "", "")
	if status != http.StatusOK && status != http.StatusAccepted {
		t.Fatalf("report cancel failed: %d %s", status, raw)
	}
	cancelledFinal := awaitProcessSnapshot(t, client, base, fixtureTokenA, cancelled.ID, func(s execution.Snapshot) bool { return s.FinishedAt != nil })
	if cancelledFinal.State != execution.Cancelled {
		t.Fatalf("report cancellation state: %+v", cancelledFinal)
	}
	if _, err := os.Stat(filepath.Join(outputDir, cancelled.ID+".json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cancelled report has a side effect: %v", err)
	}
	if len(fixtureLedger(t, outputDir)) != 1 {
		t.Fatal("cancelled report wrote the independent ledger")
	}

	workflowBody := `{"nodes":[` +
		`{"node_id":"root-ok","tool":"data.summarize","arguments":{"repetitions":1}},` +
		`{"node_id":"root-fail","tool":"service.query","arguments":{"delay_ms":0,"status":503}},` +
		`{"node_id":"child-ok","depends_on":["root-ok"],"tool":"report.generate","arguments":{"duration_ms":1000,"label":"child-ok"}},` +
		`{"node_id":"child-skipped","depends_on":["root-fail"],"tool":"report.generate","arguments":{"duration_ms":1000,"label":"must-skip"}},` +
		`{"node_id":"independent","tool":"report.generate","arguments":{"duration_ms":1000,"label":"independent"}}]}`
	status, raw = processRequest(t, client, http.MethodPost, base+"/api/v1/workflows", fixtureTokenA, workflowBody, "fixture-workflow")
	if status != http.StatusAccepted {
		t.Fatalf("submit real-tool workflow: %d %s", status, raw)
	}
	var workflow execution.WorkflowSnapshot
	if err := json.Unmarshal(raw, &workflow); err != nil || workflow.ID == "" {
		t.Fatalf("bad workflow response %s: %v", raw, err)
	}
	deadline := time.Now().Add(6 * time.Second)
	for {
		status, raw = processRequest(t, client, http.MethodGet, base+"/api/v1/workflows/"+workflow.ID, fixtureTokenA, "", "")
		if status != http.StatusOK || json.Unmarshal(raw, &workflow) != nil {
			t.Fatalf("get real-tool workflow: %d %s", status, raw)
		}
		if workflow.FinishedAt != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("real-tool workflow did not finish: %+v", workflow)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(workflow.Nodes) != 5 {
		t.Fatalf("wrong workflow nodes: %+v", workflow)
	}
	states := map[string]execution.State{}
	expectedReports := map[string]bool{first.ID: true}
	for _, node := range workflow.Nodes {
		states[node.NodeID] = node.State
		if node.State == execution.Succeeded && node.Tool == "report.generate" {
			fixtureResult(t, node)
			expectedReports[node.ID] = true
			if _, err := os.Stat(filepath.Join(outputDir, node.ID+".json")); err != nil {
				t.Fatalf("successful workflow report missing: %v", err)
			}
		}
		if node.NodeID == "child-skipped" {
			if _, err := os.Stat(filepath.Join(outputDir, node.ID+".json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("skipped workflow node produced report: %v", err)
			}
		}
	}
	if states["root-ok"] != execution.Succeeded || states["root-fail"] != execution.Failed || states["child-ok"] != execution.Succeeded || states["child-skipped"] != execution.Skipped || states["independent"] != execution.Succeeded {
		t.Fatalf("wrong real-tool DAG states: %+v", states)
	}
	records = fixtureLedger(t, outputDir)
	seen := map[string]bool{}
	for _, record := range records {
		id := record["execution_id"]
		if !expectedReports[id] || seen[id] {
			t.Fatalf("unexpected or duplicate ledger entry: %+v", record)
		}
		seen[id] = true
		reportBytes, err := os.ReadFile(filepath.Join(outputDir, id+".json"))
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(reportBytes)
		if record["report_sha256"] != hex.EncodeToString(digest[:]) {
			t.Fatalf("ledger hash does not match independent report: %+v", record)
		}
	}
	if len(seen) != len(expectedReports) {
		t.Fatalf("ledger is missing successful reports: seen=%+v expected=%+v", seen, expectedReports)
	}
}

func TestRealReportCrashReconcilesIndependentLedger(t *testing.T) {
	dir := t.TempDir()
	address := processTestAddress(t)
	configPath, outputDir, _ := realFixtureConfig(t, dir, address, "http://127.0.0.1:18081")
	child := exec.Command(os.Args[0], "-test.run=^TestProcessServerCrashChild$")
	child.Env = append(os.Environ(), "TOOLANE_PROCESS_SERVER_CHILD=1", "TOOLANE_PROCESS_SERVER_CONFIG="+configPath)
	child.Stdout, child.Stderr = nil, nil
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	childDone := make(chan error, 1)
	go func() { childDone <- child.Wait() }()
	t.Cleanup(func() { _ = child.Process.Kill() })
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 3 * time.Second}
	base := "http://" + address
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, err := client.Get(base + "/readyz")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case serviceErr := <-childDone:
			t.Fatalf("real fixture service failed: %v", serviceErr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("real fixture service not ready: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	reports := make([]execution.Snapshot, 2)
	for i := range reports {
		body := fmt.Sprintf(`{"tool":"report.generate","arguments":{"duration_ms":3000,"label":"after-crash-%d"}}`, i+1)
		reports[i] = processSubmit(t, client, base, fixtureTokenA, body, "")
	}
	for _, report := range reports {
		readyPath := filepath.Join(outputDir, report.ID+".started")
		awaitFixtureFile(t, readyPath)
		ready, err := os.ReadFile(readyPath)
		if err != nil {
			t.Fatal(err)
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(ready)))
		if err != nil || pid <= 0 {
			t.Fatalf("invalid report process marker %q: %v", ready, err)
		}
		t.Cleanup(func() { _ = syscall.Kill(-pid, syscall.SIGKILL) })
	}
	queued := processSubmit(t, client, base, fixtureTokenA, `{"tool":"data.summarize","arguments":{"repetitions":1}}`, "")
	if before := processSnapshot(t, client, base, fixtureTokenA, queued.ID); before.State != execution.Queued || before.ProcessStartedAt != nil {
		t.Fatalf("recovery candidate was already claimed: %+v", before)
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-childDone:
	case <-time.After(3 * time.Second):
		t.Fatal("killed real fixture service did not exit")
	}
	restartedBase, restartedClient, stop := startProcessTestServer(t, configPath, address)
	t.Cleanup(stop)
	for _, report := range reports {
		recovered := processSnapshot(t, restartedClient, restartedBase, fixtureTokenA, report.ID)
		if recovered.State != execution.Interrupted || recovered.ErrorCode != "service_restarted" || recovered.Result != nil {
			t.Fatalf("crashed report was treated as a known result: %+v", recovered)
		}
	}
	fixtureResult(t, awaitProcessSnapshot(t, restartedClient, restartedBase, fixtureTokenA, queued.ID, func(s execution.Snapshot) bool { return s.FinishedAt != nil }))
	deadline = time.Now().Add(5 * time.Second)
	for {
		if len(fixtureLedger(t, outputDir)) == len(reports) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("crashed reports did not leave expected independent side effects: %+v", reports)
		}
		time.Sleep(20 * time.Millisecond)
	}
	records := fixtureLedger(t, outputDir)
	byID := map[string]map[string]string{}
	for _, record := range records {
		if byID[record["execution_id"]] != nil {
			t.Fatalf("duplicate crashed report side effect: %+v", record)
		}
		byID[record["execution_id"]] = record
	}
	for i, report := range reports {
		reportBytes, err := os.ReadFile(filepath.Join(outputDir, report.ID+".json"))
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(reportBytes)
		record := byID[report.ID]
		if record == nil || record["label"] != fmt.Sprintf("after-crash-%d", i+1) || record["report_sha256"] != hex.EncodeToString(digest[:]) {
			t.Fatalf("crashed report and ledger disagree: %+v", record)
		}
		if after := processSnapshot(t, restartedClient, restartedBase, fixtureTokenA, report.ID); after.State != execution.Interrupted || after.Result != nil {
			t.Fatalf("late side effect changed unknown outcome: %+v", after)
		}
	}
}
