// Command scheduler compares FIFO and tenant round robin on the same bounded
// Mock workload. It records every submission and execution for later analysis.
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/haonox/toolane/internal/execution"
	"github.com/haonox/toolane/internal/tool"
)

type task struct {
	tenant  string
	delayMS int
}

type arguments struct {
	DelayMS int `json:"delay_ms"`
}

type row struct {
	scenario    string
	mode        execution.SchedulerMode
	repetition  int
	index       int
	task        task
	submittedAt time.Time
	rejection   string
	snapshot    execution.Snapshot
}

func main() {
	out := flag.String("out", "", "directory for raw CSV and metadata")
	repetitions := flag.Int("repetitions", 3, "runs per scenario and scheduler")
	seed := flag.Int64("seed", 20261018, "seed for mixed task durations")
	flag.Parse()
	if *out == "" || *repetitions <= 0 {
		fmt.Fprintln(os.Stderr, "usage: go run ./benchmarks/scheduler -out DIR [-repetitions N] [-seed N]")
		os.Exit(2)
	}
	if err := experiment(*out, *repetitions, *seed); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func experiment(out string, repetitions int, seed int64) error {
	if err := os.MkdirAll(out, 0755); err != nil {
		return err
	}
	config := execution.DefaultConfig()
	config.Workers = 4
	config.QueueCapacity = 256
	config.RetainedResults = 256
	config.TenantRunningLimit = 2
	config.TenantPendingLimit = 64
	meta := struct {
		GeneratedAt time.Time      `json:"generated_at"`
		GoVersion   string         `json:"go_version"`
		GOOS        string         `json:"goos"`
		GOARCH      string         `json:"goarch"`
		NumCPU      int            `json:"num_cpu"`
		GOMAXPROCS  int            `json:"gomaxprocs"`
		Workers     int            `json:"workers"`
		GlobalQueue int            `json:"global_pending_limit"`
		TenantRun   int            `json:"tenant_running_limit"`
		TenantQueue int            `json:"tenant_pending_limit"`
		Repetitions int            `json:"repetitions"`
		Seed        int64          `json:"seed"`
		Scenarios   map[string]int `json:"scenario_tasks"`
		ArrivalMode string         `json:"arrival_mode"`
	}{
		GeneratedAt: time.Now().UTC(), GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		NumCPU: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0), Workers: config.Workers,
		GlobalQueue: config.QueueCapacity, TenantRun: config.TenantRunningLimit,
		TenantQueue: config.TenantPendingLimit, Repetitions: repetitions, Seed: seed,
		Scenarios:   map[string]int{"short": 72, "long": 72, "mixed": 72, "burst": 72},
		ArrivalMode: "sequential grouped submissions while executor start gate is closed; all accepted work then released together",
	}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "metadata.json"), append(data, '\n'), 0644); err != nil {
		return err
	}
	file, err := os.Create(filepath.Join(out, "executions.csv"))
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	if err := writer.Write([]string{"scenario", "mode", "repetition", "arrival_index", "tenant", "delay_ms", "submitted_at", "accepted", "rejection", "execution_id", "status", "created_at", "ready_at", "running_at", "finished_at", "wait_ms", "total_ms"}); err != nil {
		return err
	}
	for _, scenario := range []string{"short", "long", "mixed", "burst"} {
		input := workload(scenario, seed)
		for repeat := 1; repeat <= repetitions; repeat++ {
			for _, mode := range []execution.SchedulerMode{execution.SchedulerFIFO, execution.SchedulerRoundRobin} {
				config.Scheduler = mode
				rows, err := run(config, scenario, mode, repeat, input)
				if err != nil {
					return fmt.Errorf("%s repetition %d %s: %w", scenario, repeat, mode, err)
				}
				for _, record := range rows {
					if err := writer.Write(csvRow(record)); err != nil {
						return err
					}
				}
				printSummary(rows)
			}
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
	}
	return file.Sync()
}

func workload(name string, seed int64) []task {
	rng := rand.New(rand.NewSource(seed))
	var result []task
	for _, tenant := range []string{"A", "B", "C"} {
		count := 24
		if name == "burst" {
			count = 12
			if tenant == "A" {
				count = 48
			}
		}
		for range count {
			delay := 2
			switch name {
			case "long":
				delay = 15
			case "mixed":
				if rng.Intn(2) == 1 {
					delay = 15
				}
			}
			result = append(result, task{tenant: tenant, delayMS: delay})
		}
	}
	return result
}

func run(config execution.Config, scenario string, mode execution.SchedulerMode, repetition int, input []task) (rows []row, retErr error) {
	gate := make(chan struct{})
	registry := tool.NewRegistry()
	if err := registry.Register(tool.Definition{
		Name: "bench.wait",
		Validate: func(raw json.RawMessage) error {
			var args arguments
			if err := json.Unmarshal(raw, &args); err != nil {
				return err
			}
			if args.DelayMS < 0 || args.DelayMS > 1000 {
				return errors.New("delay outside benchmark range")
			}
			return nil
		},
		Execute: func(ctx context.Context, raw json.RawMessage, _ tool.RunOptions) (tool.Outcome, error) {
			var args arguments
			if err := json.Unmarshal(raw, &args); err != nil {
				return tool.Outcome{}, err
			}
			select {
			case <-gate:
			case <-ctx.Done():
				return tool.Outcome{}, ctx.Err()
			}
			timer := time.NewTimer(time.Duration(args.DelayMS) * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
				return tool.Outcome{}, nil
			case <-ctx.Done():
				return tool.Outcome{}, ctx.Err()
			}
		},
	}); err != nil {
		return nil, err
	}
	m, err := execution.NewManager(config, registry)
	if err != nil {
		return nil, err
	}
	gateClosed := false
	defer func() {
		if !gateClosed {
			close(gate)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		retErr = errors.Join(retErr, m.Close(ctx))
	}()
	rows = make([]row, len(input))
	for i, item := range input {
		args, err := json.Marshal(arguments{DelayMS: item.delayMS})
		if err != nil {
			return nil, err
		}
		rows[i] = row{scenario: scenario, mode: mode, repetition: repetition, index: i, task: item, submittedAt: time.Now().UTC()}
		s, err := m.Submit(context.Background(), execution.Request{TenantID: item.tenant, Tool: "bench.wait", Arguments: args})
		if err != nil {
			rows[i].rejection = err.Error()
			continue
		}
		rows[i].snapshot = s
	}
	close(gate)
	gateClosed = true
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		remaining := 0
		for i := range rows {
			if rows[i].snapshot.ID == "" || rows[i].snapshot.FinishedAt != nil {
				continue
			}
			s, err := m.Get(rows[i].snapshot.ID)
			if err != nil {
				return nil, err
			}
			rows[i].snapshot = s
			if s.FinishedAt == nil {
				remaining++
			}
		}
		if remaining == 0 {
			return rows, nil
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			return nil, errors.New("benchmark tasks did not finish")
		}
	}
}

func csvRow(r row) []string {
	s := r.snapshot
	accepted := s.ID != ""
	wait, total := "", ""
	if s.RunningAt != nil {
		wait = strconv.FormatFloat(float64(s.RunningAt.Sub(s.CreatedAt))/float64(time.Millisecond), 'f', 3, 64)
	}
	if s.FinishedAt != nil {
		total = strconv.FormatFloat(float64(s.FinishedAt.Sub(s.CreatedAt))/float64(time.Millisecond), 'f', 3, 64)
	}
	return []string{
		r.scenario, string(r.mode), strconv.Itoa(r.repetition), strconv.Itoa(r.index), r.task.tenant,
		strconv.Itoa(r.task.delayMS), r.submittedAt.Format(time.RFC3339Nano), strconv.FormatBool(accepted), r.rejection,
		s.ID, string(s.State), formatTime(s.CreatedAt), formatTimePtr(s.ReadyAt), formatTimePtr(s.RunningAt), formatTimePtr(s.FinishedAt), wait, total,
	}
}

func formatTime(at time.Time) string {
	if at.IsZero() {
		return ""
	}
	return at.Format(time.RFC3339Nano)
}

func formatTimePtr(at *time.Time) string {
	if at == nil {
		return ""
	}
	return formatTime(*at)
}

func printSummary(rows []row) {
	claimed := slices.Clone(rows)
	claimed = slices.DeleteFunc(claimed, func(r row) bool { return r.snapshot.RunningAt == nil })
	sort.Slice(claimed, func(i, j int) bool {
		return claimed[i].snapshot.RunningAt.Before(*claimed[j].snapshot.RunningAt)
	})
	counts := map[string]int{"A": 0, "B": 0, "C": 0}
	for _, r := range claimed[:min(24, len(claimed))] {
		counts[r.task.tenant]++
	}
	sum := 0
	squares := 0
	for _, count := range counts {
		sum += count
		squares += count * count
	}
	jain := math.NaN()
	if squares > 0 {
		jain = float64(sum*sum) / float64(3*squares)
	}
	rejected := len(rows) - len(claimed)
	waits := make(map[string][]float64)
	for _, r := range claimed {
		waits[r.task.tenant] = append(waits[r.task.tenant], float64(r.snapshot.RunningAt.Sub(r.snapshot.CreatedAt))/float64(time.Millisecond))
	}
	for tenant := range waits {
		sort.Float64s(waits[tenant])
	}
	fmt.Printf("%s %s run=%d accepted=%d rejected=%d first24=[A:%d B:%d C:%d] jain=%.3f p95_wait_ms=[A:%.1f B:%.1f C:%.1f]\n",
		rows[0].scenario, rows[0].mode, rows[0].repetition, len(claimed), rejected,
		counts["A"], counts["B"], counts["C"], jain,
		percentile(waits["A"], .95), percentile(waits["B"], .95), percentile(waits["C"], .95))
}

func percentile(sorted []float64, fraction float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	index := int(math.Ceil(fraction*float64(len(sorted)))) - 1
	return sorted[max(0, index)]
}
