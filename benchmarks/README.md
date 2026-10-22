# Reproducible local experiments

`run_real_tools.py` runs the three trusted Process fixtures through the normal
HTTP and SQLite path. It starts a new Toolane service, private state directory,
three tenant credentials, two simulated Agents per tenant, and a controlled
loopback query service for **each** short scenario and repetition. It never
stores credentials in the output. Linux and Python 3 standard library are
required; Docker is not used.

Each run first completes six unmeasured warmup calls, one per simulated Agent.
Their IDs and states are saved in `warmup.json`.

Build the exact binary to measure, then choose a new output directory:

```bash
go build -o /tmp/toolane-bench ./cmd/toolane
python3 benchmarks/run_real_tools.py \
  --binary /tmp/toolane-bench --out /tmp/toolane-real-smoke --profile smoke
```

The smoke profile is run in CI. It checks end-to-end wiring, result correctness,
failure/timeout propagation, and the independent report ledger. Its small
sample is **not** a throughput claim.

For the controlled short matrix, run at least three independent repetitions
on one machine with a fixed binary and seed:

```bash
python3 benchmarks/run_real_tools.py \
  --binary /tmp/toolane-bench --out /tmp/toolane-real-short-1 \
  --profile short --repetitions 3 --seed 20261021
```

The short profile covers direct and Toolane serial baselines for tenant A and
one Agent, then scheduled CPU
and 50/200 ms I/O arrivals, fixed-concurrency I/O, 1-second reports, an
uneven tenant burst, overload, and a mixture with failure, timeout, and
cancellation. Planned and actual send times are both recorded; schedule lag
exposes when the client could not keep up. The first 24 start counts by tenant
are included in `summary.json` for inspecting fairness under comparable work.
There is no preset QPS target. Compare runs only with the same host, binary,
tool data, and workload options.

For an initial sustained run, use one continuously running service per
repetition. Its default duration is 1,800 seconds:

```bash
python3 benchmarks/run_real_tools.py \
  --binary /tmp/toolane-bench --out /tmp/toolane-real-soak-1 \
  --profile soak --duration-seconds 1800
```

Each output directory contains `metadata.json`, `summary.json`, `REPORT.md`,
and a SHA-256 `manifest.json` for the saved artifacts. Under
`runs/<scenario>-r<repetition>/` it stores:

- `executions.csv`: every planned task, input, submission time, response,
  final state, and separate queue, run, and end-to-end durations;
- `resources.csv` and `minutes.csv`: server CPU/RSS, scheduler gauges, SQLite
  main/WAL sizes, and per-minute completion and p95 duration;
- `ledger.jsonl` and `reports/*.json` for cases with successful reports,
  plus local service logs.

The runner checks real tool output and reconciles each successful report's
SHA-256 with the independent ledger. It fails on an unexpected state or
missing/duplicate side effect. `REPORT.md` lists coverage limits. Keep a
result only when the command exits successfully; a failed run may leave
partial diagnostics.

The older `run_http_service.py` and `http_service.py` measure Mock tasks, so
their throughput must not be compared directly with Process fixtures. The
removed Docker backend is not part of these measurements. The HTTP service
currently exposes tenant round robin only; a FIFO comparison must use the
internal scheduler benchmark and state its different boundary.
