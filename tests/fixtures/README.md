# Real Process fixtures

These fixtures enter Toolane through the same private `process_tools` config as an
administrator's trusted tools. They require Python 3 and run entirely on loopback.
The generator creates two private tenant tokens; tenant B cannot call
`report.generate`.

| Tool | Input | Independent check |
| --- | --- | --- |
| `data.summarize` | `repetitions` 1–200000 | Fixed `orders.csv` SHA-256 and per-tenant totals; one pass yields six rows and total 72. |
| `service.query` | `delay_ms` 0–1000, `status` 200–599 | A separate local HTTP server controls delay and response status. |
| `report.generate` | `duration_ms` 1000–3000, `label` | `<execution_id>.json` plus `ledger.jsonl`, kept outside Toolane's SQLite state. |

From the repository root, in separate terminals:

```bash
python3 tests/fixtures/query_service.py --port 18081
```

```bash
fixture_dir="$(mktemp -d)"
python3 tests/fixtures/make_config.py \
  --config "$fixture_dir/server.json" \
  --state-dir "$fixture_dir/state" \
  --output-dir "$fixture_dir/outputs" \
  --query-origin http://127.0.0.1:18081
go run ./cmd/toolane serve --config "$fixture_dir/server.json"
```

The generator prints both tokens. With tenant A's token in `TOOLANE_TOKEN`, an
Agent bridge call looks like:

```bash
export TOOLANE_TOKEN='tenant A token printed above'
python3 examples/agent_bridge.py describe
python3 examples/agent_bridge.py invoke --call-id report-1 \
  --tool report.generate \
  --arguments '{"duration_ms":1000,"label":"sample"}'
```

Compare the returned `execution_id` and `report_sha256` with the matching
`outputs/<execution_id>.json` and `outputs/ledger.jsonl` line. An accepted
idempotency key maps repeated submissions to one execution; it does not imply
exactly-once external effects across crashes. A forcibly killed service may
leave a Process tool running; `interrupted` means its outcome is unknown until
the output directory and any external systems are reconciled.

The Go HTTP integration test creates its own private state, output directory,
and query server. It checks the result against the independent report and
ledger, cancellation, tenant isolation, and a real-tool DAG:

```bash
go test ./cmd/toolane -run 'TestReal(ProcessFixturesThroughHTTP|FixtureGeneratorProducesValidServerConfig)' -count=1
python3 -m unittest discover -s tests/fixtures -p 'test_*.py'
```
