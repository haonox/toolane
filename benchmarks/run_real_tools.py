#!/usr/bin/env python3
"""Run reproducible real Process workloads against an isolated local Toolane service."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from urllib.request import ProxyHandler, build_opener

from http_service import command_version, cpu_model, elapsed_ms, percentile, service_sample

ROOT = Path(__file__).resolve().parents[1]
DATASET_SHA256 = hashlib.sha256((ROOT / "tests" / "fixtures" / "orders.csv").read_bytes()).hexdigest()
sys.path.insert(0, str(ROOT / "examples"))
sys.path.insert(0, str(ROOT / "tests" / "fixtures"))
from python_client import TERMINAL, ToolaneClient, ToolaneError  # noqa: E402
from make_config import make_tool  # noqa: E402

TENANTS = ("A", "B", "C")
FIELDS = ("scenario", "repetition", "index", "tenant", "agent", "tool", "arguments",
          "expected", "arrival_model", "scheduled_at", "scheduled_offset_ms", "submit_started_at",
          "submit_completed_at", "schedule_lag_ms", "submit_ms", "http_status",
          "rejection_code", "execution_id", "status", "error_code", "created_at",
          "ready_at", "running_at", "process_started_at", "finished_at", "queue_ms",
          "run_ms", "end_to_end_ms", "direct_ms", "result_json", "error_message", "output_truncated")


def now():
    return datetime.now(timezone.utc).isoformat()


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def task(tenant, agent, tool, arguments, expected="succeeded", **options):
    return {"tenant": tenant, "agent": agent, "tool": tool, "arguments": arguments,
            "expected": expected, **options}


def balanced(tool, arguments, count, seed):
    tasks = [task(TENANTS[i % 3], (i // 3) % 2, tool, dict(arguments)) for i in range(count)]
    random.Random(seed).shuffle(tasks)
    return tasks


def single_tenant(tool, arguments, count):
    return [task("A", 0, tool, dict(arguments)) for _ in range(count)]


def scenarios(profile, seed):
    if profile == "smoke":
        return [
            ("cpu_smoke", "scheduled", 5, balanced("data.summarize", {"repetitions": 2}, 12, seed)),
            ("io_smoke", "scheduled", 5, balanced("service.query", {"delay_ms": 50, "status": 200}, 12, seed + 1)),
            ("report_smoke", "scheduled", 20, [
                task("A", 0, "report.generate", {"duration_ms": 1000, "label": "smoke-a"}),
                task("B", 0, "service.query", {"delay_ms": 0, "status": 503}, "failed"),
                task("C", 0, "service.query", {"delay_ms": 500, "status": 200}, "timed_out", execution_timeout_ms=100),
            ]),
        ]
    if profile == "soak":
        return [("soak", "scheduled", 250, balanced("service.query", {"delay_ms": 50, "status": 200}, 18, seed))]
    burst = [task("A", i % 2, "report.generate", {"duration_ms": 1000, "label": f"burst-{i}"}) for i in range(12)]
    burst += [task(TENANTS[1 + i % 2], (i // 2) % 2, "service.query", {"delay_ms": 50, "status": 200},
                   offset_ms=200) for i in range(6)]
    overload = [task("A", i % 2, "report.generate", {"duration_ms": 1000, "label": f"overload-{i}"},
                     "succeeded_or_rejected") for i in range(72)]
    overload += [task(TENANTS[1 + i % 2], (i // 2) % 2, "service.query", {"delay_ms": 50, "status": 200}) for i in range(8)]
    mixed = balanced("data.summarize", {"repetitions": 1000}, 9, seed + 5)
    mixed += balanced("service.query", {"delay_ms": 200, "status": 200}, 9, seed + 6)
    mixed += [task(tenant, 0, "report.generate", {"duration_ms": 1000, "label": f"mixed-{tenant}"}) for tenant in TENANTS]
    mixed += [task("A", 1, "service.query", {"delay_ms": 0, "status": 503}, "failed"),
              task("B", 1, "service.query", {"delay_ms": 500, "status": 200}, "timed_out", execution_timeout_ms=100),
              task("C", 1, "report.generate", {"duration_ms": 3000, "label": "cancelled"},
                   "cancelled", cancel_after_ms=100)]
    random.Random(seed + 7).shuffle(mixed)
    return [
        ("cpu_direct", "direct", 0, single_tenant("data.summarize", {"repetitions": 1000}, 12)),
        ("cpu_serial", "serial", 0, single_tenant("data.summarize", {"repetitions": 1000}, 12)),
        ("io_direct", "direct", 0, single_tenant("service.query", {"delay_ms": 50, "status": 200}, 12)),
        ("io_serial", "serial", 0, single_tenant("service.query", {"delay_ms": 50, "status": 200}, 12)),
        ("cpu_steady", "scheduled", 20, balanced("data.summarize", {"repetitions": 1000}, 36, seed + 1)),
        ("cpu_pressure", "scheduled", 2, balanced("data.summarize", {"repetitions": 1000}, 72, seed + 2)),
        ("io50_steady", "scheduled", 20, balanced("service.query", {"delay_ms": 50, "status": 200}, 36, seed + 3)),
        ("io200_pressure", "scheduled", 5, balanced("service.query", {"delay_ms": 200, "status": 200}, 72, seed + 4)),
        ("io_closed_loop", "closed_loop", 0, balanced("service.query", {"delay_ms": 200, "status": 200}, 36, seed + 8)),
        ("long", "scheduled", 100, [task(TENANTS[i % 3], (i // 3) % 2, "report.generate",
                                         {"duration_ms": 1000, "label": f"long-{i}"}) for i in range(12)]),
        ("burst", "scheduled", 0, burst),
        ("overload", "scheduled", 0, overload),
        ("mixed", "scheduled", 20, mixed),
    ]


def fixture_config(private, http_port, metrics_port, query_port, tokens):
    script = ROOT / "tests" / "fixtures" / "process_tools.py"
    output_dir = private / "outputs"
    output_dir.mkdir(mode=0o700)
    python = Path(sys.executable).resolve()
    origin = f"http://127.0.0.1:{query_port}"
    integer = lambda minimum, maximum: {"type": "integer", "minimum": minimum, "maximum": maximum}
    tools = [
        make_tool("data.summarize", "summarize", "Summarize fixed orders CSV.",
                  {"repetitions": integer(1, 200000)}, ["repetitions"], ["repetitions"],
                  python, script, output_dir, origin),
        make_tool("service.query", "query", "Query controlled local HTTP service.",
                  {"delay_ms": integer(0, 1000), "status": integer(200, 599)},
                  ["delay_ms", "status"], ["delay_ms", "status"], python, script, output_dir, origin),
        make_tool("report.generate", "report", "Generate report and independent ledger record.",
                  {"duration_ms": integer(1000, 3000), "label": {"type": "string", "minLength": 1, "maxLength": 80}},
                  ["duration_ms", "label"], ["duration_ms", "label"], python, script, output_dir, origin),
    ]
    # The overload case intentionally fills a tenant's pending queue. Give
    # accepted long tasks enough queue time to finish rather than timing out.
    tools[2]["queue_timeout_ms"] = 120000
    config = {"listen": f"127.0.0.1:{http_port}", "metrics_listen": f"127.0.0.1:{metrics_port}",
              "state_dir": str(private / "state"), "process_tools": tools,
              "tokens": [{"token": tokens[tenant], "tenant_id": tenant,
                          "tools": [item["name"] for item in tools]} for tenant in TENANTS]}
    path = private / "server.json"
    with path.open("x") as output:
        json.dump(config, output)
    path.chmod(0o600)
    return path


def wait_ready(url, process):
    opener = build_opener(ProxyHandler({}))
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"child service exited during startup: {process.returncode}")
        try:
            with opener.open(url, timeout=0.5) as response:
                if response.status == 200:
                    return
        except OSError:
            pass
        time.sleep(0.05)
    raise TimeoutError(f"service did not become ready: {url}")


def stop(process):
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=12)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def warmup(clients, wait_seconds):
    records = []
    for tenant in TENANTS:
        for agent in range(2):
            client = clients[(tenant, agent)]
            accepted = client.request("POST", "/api/v1/executions", {"agent_id": f"{tenant}-{agent}",
                                                                     "tool": "data.summarize", "arguments": {"repetitions": 1}})
            deadline = time.monotonic() + wait_seconds
            while True:
                snapshot = client.get_execution(accepted["execution_id"])
                if snapshot["status"] in TERMINAL:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("warmup task did not finish")
                time.sleep(0.02)
            if snapshot["status"] != "succeeded":
                raise ValueError(f"warmup failed for tenant {tenant}: {snapshot['status']}")
            records.append({"tenant": tenant, "agent": agent, "execution_id": accepted["execution_id"],
                            "status": snapshot["status"]})
    return records


def sample(metrics_url, pid, state_dir, base_url):
    result = service_sample(metrics_url, pid)
    with build_opener(ProxyHandler({})).open(base_url + "/healthz", timeout=2) as response:
        result["health_status"] = response.status
    for name in ("toolane.sqlite", "toolane.sqlite-wal", "toolane.sqlite-shm"):
        path = state_dir / name
        result[name + "_bytes"] = path.stat().st_size if path.exists() else 0
    return result


def submit(client, item, index, scenario, repetition, mode, scheduled, start, start_wall, wait_seconds):
    if scheduled > time.monotonic():
        time.sleep(scheduled - time.monotonic())
    started_mono = time.monotonic()
    row = dict.fromkeys(FIELDS, "")
    row.update(scenario=scenario, repetition=repetition, index=index, tenant=item["tenant"],
               agent=item["agent"], tool=item["tool"],
               arguments=json.dumps(item["arguments"], sort_keys=True, separators=(",", ":")),
               expected=item["expected"], arrival_model=mode,
               scheduled_at=(start_wall + timedelta(seconds=scheduled - start)).isoformat(),
               scheduled_offset_ms=round((scheduled - start) * 1000, 3),
               submit_started_at=now(), schedule_lag_ms=round(max(0, started_mono - scheduled) * 1000, 3))
    body = {"agent_id": f"{item['tenant']}-{item['agent']}", "tool": item["tool"],
            "arguments": item["arguments"]}
    if "execution_timeout_ms" in item:
        body["execution_timeout_ms"] = item["execution_timeout_ms"]
    if "queue_timeout_ms" in item:
        body["queue_timeout_ms"] = item["queue_timeout_ms"]
    try:
        accepted = client.request("POST", "/api/v1/executions", body)
    except ToolaneError as error:
        if not str(error).startswith("HTTP 429: capacity_exhausted "):
            raise
        row["http_status"], row["rejection_code"] = 429, "capacity_exhausted"
    else:
        row["http_status"], row["execution_id"] = 202, accepted["execution_id"]
    row["submit_completed_at"] = now()
    row["submit_ms"] = round((time.monotonic() - started_mono) * 1000, 3)
    return row


def finish(client, item, row, wait_seconds):
    if row["http_status"] != 202:
        return row
    deadline = time.monotonic() + wait_seconds
    cancel_at = time.monotonic() + item.get("cancel_after_ms", 0) / 1000 if "cancel_after_ms" in item else None
    cancelled = False
    while True:
        if cancel_at is not None and not cancelled and time.monotonic() >= cancel_at:
            client.cancel_execution(row["execution_id"])
            cancelled = True
        snapshot = client.get_execution(row["execution_id"])
        if snapshot["status"] in TERMINAL:
            break
        if time.monotonic() >= deadline:
            raise TimeoutError(f"execution {row['execution_id']} did not finish")
        time.sleep(0.02)
    for field in ("created_at", "ready_at", "running_at", "process_started_at", "finished_at"):
        row[field] = snapshot.get(field) or ""
    row["status"], row["error_code"] = snapshot["status"], snapshot.get("error_code", "")
    row["result_json"] = json.dumps(snapshot.get("result"), sort_keys=True) if "result" in snapshot else ""
    row["error_message"] = snapshot.get("error_message", "")
    row["output_truncated"] = snapshot.get("output_truncated", False)
    row["queue_ms"] = elapsed_ms(row["created_at"], row["running_at"])
    row["run_ms"] = elapsed_ms(row["process_started_at"] or row["running_at"], row["finished_at"])
    row["end_to_end_ms"] = elapsed_ms(row["created_at"], row["finished_at"])
    if item["expected"] == "succeeded" and snapshot["status"] == "succeeded":
        result = snapshot.get("result") or {}
        if result.get("execution_id") != row["execution_id"]:
            raise ValueError(f"result/execution ID mismatch: {row['execution_id']}")
        if item["tool"] == "data.summarize" and (result.get("rows") != 6 * item["arguments"]["repetitions"] or
                                                   result.get("total") != 72 * item["arguments"]["repetitions"] or
                                                   result.get("dataset_sha256") != DATASET_SHA256):
            raise ValueError(f"incorrect data.summarize result: {row['execution_id']}")
        if item["tool"] == "service.query" and result.get("response") != item["arguments"]:
            raise ValueError(f"incorrect service.query result: {row['execution_id']}")
    return row


def direct(item, index, scenario, repetition, private, query_port, wait_seconds):
    row = dict.fromkeys(FIELDS, "")
    row.update(scenario=scenario, repetition=repetition, index=index, tenant=item["tenant"],
               agent=item["agent"], tool=item["tool"], arguments=json.dumps(item["arguments"], sort_keys=True),
               expected=item["expected"], arrival_model="direct", http_status="direct")
    execution_id = secrets.token_hex(16)
    mode = {"data.summarize": "summarize", "service.query": "query", "report.generate": "report"}[item["tool"]]
    argv = [sys.executable, str(ROOT / "tests" / "fixtures" / "process_tools.py"), mode]
    argv += [str(value) for value in item["arguments"].values()] + [execution_id]
    env = {"PYTHONDONTWRITEBYTECODE": "1", "TOOLANE_FIXTURE_OUTPUT_DIR": str(private / "outputs"),
           "TOOLANE_FIXTURE_QUERY_ORIGIN": f"http://127.0.0.1:{query_port}"}
    started = time.monotonic()
    completed = subprocess.run(argv, env=env, capture_output=True, text=True, timeout=wait_seconds)
    row["direct_ms"] = round((time.monotonic() - started) * 1000, 3)
    if completed.returncode != 0:
        raise RuntimeError(f"direct fixture exited {completed.returncode}: {completed.stderr[-500:]}")
    result = json.loads(completed.stdout)
    if result.get("execution_id") != execution_id:
        raise ValueError("direct fixture returned wrong execution ID")
    if item["tool"] == "data.summarize" and (result.get("rows") != 6 * item["arguments"]["repetitions"] or
                                               result.get("total") != 72 * item["arguments"]["repetitions"] or
                                               result.get("dataset_sha256") != DATASET_SHA256):
        raise ValueError("direct data.summarize returned wrong totals")
    if item["tool"] == "service.query" and result.get("response") != item["arguments"]:
        raise ValueError("direct service.query returned wrong payload")
    row["status"] = "succeeded"
    row["result_json"] = json.dumps(result, sort_keys=True)
    return row


def execute_tasks(items, scenario, repetition, mode, interval_ms, clients, private, query_port, wait_seconds):
    rows = []
    start = time.monotonic()
    start_wall = datetime.now(timezone.utc)
    if mode == "direct":
        for index, item in enumerate(items):
            rows.append(direct(item, index, scenario, repetition, private, query_port, wait_seconds))
        return rows, time.monotonic() - start
    if mode in {"serial", "closed_loop"}:
        def agent_run(group):
            owned = []
            for index, item in group:
                client = clients[(item["tenant"], item["agent"])]
                row = submit(client, item, index, scenario, repetition, mode, time.monotonic(), start, start_wall, wait_seconds)
                owned.append(finish(client, item, row, wait_seconds))
            return owned
        groups = [[(i, item) for i, item in enumerate(items)]] if mode == "serial" else [
            [(i, item) for i, item in enumerate(items) if (item["tenant"], item["agent"]) == (tenant, agent)]
            for tenant in TENANTS for agent in range(2)]
        with ThreadPoolExecutor(max_workers=len(groups)) as pool:
            for group_rows in pool.map(agent_run, groups):
                rows.extend(group_rows)
    else:
        with ThreadPoolExecutor(max_workers=16) as pool:
            submitted = []
            for index, item in enumerate(items):
                offset = item.get("offset_ms", index * interval_ms)
                scheduled = start + offset / 1000
                if time.monotonic() < scheduled:
                    time.sleep(scheduled - time.monotonic())
                client = clients[(item["tenant"], item["agent"])]
                submitted.append(pool.submit(submit, client, item, index, scenario, repetition, mode,
                                             scheduled, start, start_wall, wait_seconds))
            rows = [future.result() for future in submitted]
        with ThreadPoolExecutor(max_workers=16) as pool:
            rows = list(pool.map(lambda pair: finish(clients[(pair[1]["tenant"], pair[1]["agent"])],
                                                     pair[1], pair[0], wait_seconds), zip(rows, items)))
    rows.sort(key=lambda row: row["index"])
    return rows, time.monotonic() - start


def validate(rows, items, output_dir):
    expected_reports = {}
    for row, item in zip(rows, items):
        expected = item["expected"]
        if expected == "succeeded_or_rejected":
            valid = row["status"] == "succeeded" or row["http_status"] == 429
        else:
            valid = row["status"] == expected
        if not valid:
            raise ValueError(f"unexpected outcome for {row['scenario']} index {row['index']}: expected {expected}, got {row['status']} / {row['http_status']}")
        expected_codes = {"failed": "process_exit_nonzero", "timed_out": "execution_timeout", "cancelled": "cancelled"}
        if expected in expected_codes and row["error_code"] != expected_codes[expected]:
            raise ValueError(f"wrong error code for {row['scenario']} index {row['index']}: {row['error_code']}")
        if item["tool"] == "report.generate" and row["status"] == "succeeded":
            expected_reports[row["execution_id"]] = item["arguments"]["label"]
    ledger_path = output_dir / "ledger.jsonl"
    ledger = [json.loads(line) for line in ledger_path.read_text().splitlines()] if ledger_path.exists() else []
    seen = set()
    for record in ledger:
        execution_id = record["execution_id"]
        if execution_id in seen or expected_reports.get(execution_id) != record["label"]:
            raise ValueError(f"duplicate or unexpected report ledger entry: {execution_id}")
        seen.add(execution_id)
        report_bytes = (output_dir / (execution_id + ".json")).read_bytes()
        if hashlib.sha256(report_bytes).hexdigest() != record["report_sha256"]:
            raise ValueError(f"report/ledger hash mismatch: {execution_id}")
    if seen != set(expected_reports):
        raise ValueError(f"missing report side effects: {set(expected_reports) - seen}")


def write_csv(path, rows, fieldnames):
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def summarize(rows, elapsed):
    accepted = [row for row in rows if row["http_status"] == 202]
    started = sorted((row for row in accepted if row["process_started_at"]), key=lambda row: row["process_started_at"])
    succeeded = sum(row["status"] == "succeeded" for row in rows)
    rejected = sum(row["http_status"] == 429 for row in rows)
    result = {"submitted": len(rows), "accepted": len(accepted), "rejected": rejected,
              "success_rate": round(succeeded / len(rows), 4) if rows else None,
              "rejection_ratio": round(rejected / len(rows), 4) if rows else None,
              "statuses": dict(Counter(row["status"] for row in rows)), "elapsed_seconds": round(elapsed, 3),
              "completed_per_second": round(succeeded / elapsed, 3),
              "first_24_starts_by_tenant": dict(Counter(row["tenant"] for row in started[:24]))}
    for field in ("submit_ms", "schedule_lag_ms", "queue_ms", "run_ms", "end_to_end_ms", "direct_ms"):
        values = [float(row[field]) for row in rows if row[field] != ""]
        result[field] = {f"p{p}": percentile(values, p) for p in (50, 95, 99)}
    return result


def minute_rows(rows, samples):
    if not samples:
        return []
    origin = datetime.fromisoformat(samples[0]["at"])
    buckets = {}
    for row in rows:
        if not row["finished_at"]:
            continue
        minute = max(0, int((datetime.fromisoformat(row["finished_at"]) - origin).total_seconds() // 60))
        buckets.setdefault(minute, {"rows": [], "samples": []})["rows"].append(row)
    for point in samples:
        minute = max(0, int((datetime.fromisoformat(point["at"]) - origin).total_seconds() // 60))
        buckets.setdefault(minute, {"rows": [], "samples": []})["samples"].append(point)
    result = []
    for minute, data in sorted(buckets.items()):
        completed = data["rows"]
        values = [float(row["end_to_end_ms"]) for row in completed if row["end_to_end_ms"] != ""]
        resources = data["samples"]
        result.append({"minute": minute, "completed": len(completed), "end_to_end_p95_ms": percentile(values, 95),
                       "rss_peak_bytes": max((point.get("rss_bytes", 0) for point in resources), default=0),
                       "wal_peak_bytes": max((point.get("toolane.sqlite-wal_bytes", 0) for point in resources), default=0),
                       "cpu_seconds_end": max((point.get("cpu_seconds", 0) for point in resources), default=0)})
    return result


def run_case(binary, out, name, repetition, mode, interval_ms, items, wait_seconds, duration_seconds=None):
    run_dir = out / "runs" / f"{name}-r{repetition}"
    run_dir.mkdir(parents=True)
    with tempfile.TemporaryDirectory(prefix="toolane-real-bench-") as scratch:
        private = Path(scratch)
        ports = set()
        while len(ports) < 3:
            ports.add(free_port())
        http_port, metrics_port, query_port = sorted(ports)
        tokens = {tenant: secrets.token_hex(32) for tenant in TENANTS}
        config_path = fixture_config(private, http_port, metrics_port, query_port, tokens)
        query_log_path = run_dir / "query_service.log"
        service_log_path = run_dir / "service.log"
        with query_log_path.open("w") as query_log, service_log_path.open("w") as service_log:
            query = subprocess.Popen([sys.executable, str(ROOT / "tests" / "fixtures" / "query_service.py"),
                                      "--port", str(query_port)], stdout=query_log, stderr=query_log)
            try:
                wait_ready(f"http://127.0.0.1:{query_port}/query?delay_ms=0&status=200", query)
                service = subprocess.Popen([str(binary), "serve", "--config", str(config_path)],
                                           stdout=service_log, stderr=service_log)
                try:
                    base = f"http://127.0.0.1:{http_port}"
                    metrics = f"http://127.0.0.1:{metrics_port}"
                    wait_ready(base + "/readyz", service)
                    clients = {(tenant, agent): ToolaneClient(base, tokens[tenant])
                               for tenant in TENANTS for agent in range(2)}
                    (run_dir / "warmup.json").write_text(json.dumps(warmup(clients, wait_seconds), indent=2) + "\n")
                    samples = []
                    done = threading.Event()
                    def collect():
                        while not done.is_set():
                            try:
                                samples.append(sample(metrics, service.pid, private / "state", base))
                            except (OSError, ValueError) as error:
                                samples.append({"at": now(), "sample_error": str(error)})
                            done.wait(1)
                    sampler = threading.Thread(target=collect, daemon=True)
                    sampler.start()
                    try:
                        if duration_seconds is None:
                            rows, elapsed = execute_tasks(items, name, repetition, mode, interval_ms,
                                                          clients, private, query_port, wait_seconds)
                        else:
                            began = time.monotonic()
                            rows, all_items = [], []
                            while time.monotonic() - began < duration_seconds:
                                cycle_rows, _ = execute_tasks(items, name, repetition, mode, interval_ms,
                                                              clients, private, query_port, wait_seconds)
                                for row in cycle_rows:
                                    row["index"] += len(all_items)
                                rows.extend(cycle_rows)
                                all_items.extend(items)
                            elapsed = time.monotonic() - began
                            items = all_items
                    finally:
                        done.set()
                        sampler.join(timeout=2)
                    write_csv(run_dir / "executions.csv", rows, FIELDS)
                    sample_fields = sorted(set().union(*(item.keys() for item in samples))) if samples else ["at"]
                    write_csv(run_dir / "resources.csv", samples, sample_fields)
                    write_csv(run_dir / "minutes.csv", minute_rows(rows, samples),
                              ("minute", "completed", "end_to_end_p95_ms", "rss_peak_bytes", "wal_peak_bytes", "cpu_seconds_end"))
                    ledger_path = private / "outputs" / "ledger.jsonl"
                    if ledger_path.exists():
                        shutil.copy2(ledger_path, run_dir / "ledger.jsonl")
                    reports = list((private / "outputs").glob("*.json"))
                    if reports:
                        report_dir = run_dir / "reports"
                        report_dir.mkdir()
                        for report_path in reports:
                            shutil.copy2(report_path, report_dir / report_path.name)
                    validate(rows, items, private / "outputs")
                    if name == "overload" and not any(row["http_status"] == 429 for row in rows):
                        raise ValueError("overload scenario did not exercise admission rejection")
                    for point in samples:
                        if "sample_error" in point or point.get("health_status") != 200 or point.get("toolane_running", 0) > 4 or point.get("toolane_pending", 0) > 256:
                            raise ValueError(f"service health or capacity sample failed: {point}")
                    summary = summarize(rows, elapsed)
                    summary["rss_peak_bytes"] = max((point.get("rss_bytes", 0) for point in samples), default=0)
                    summary["wal_peak_bytes"] = max((point.get("toolane.sqlite-wal_bytes", 0) for point in samples), default=0)
                    summary["cpu_seconds_end"] = max((point.get("cpu_seconds", 0) for point in samples), default=0)
                    return rows, summary
                finally:
                    stop(service)
            finally:
                stop(query)


def report(metadata, summaries):
    lines = ["# Real Process HTTP experiment", "", f"- Generated: {metadata['generated_at']}",
             f"- Run status: {metadata['status']}",
             f"- Git revision: `{metadata['git_revision']}`", f"- Binary SHA-256: `{metadata['binary_sha256']}`",
             f"- Working tree dirty: {metadata['git_dirty']}",
             f"- Fixture SHA-256: `{metadata['fixture_sha256']}`", f"- CPU: {metadata['cpu_model']}",
             f"- Platform: {metadata['platform']}", f"- Profile: {metadata['profile']}; repetitions: {metadata['repetitions']}",
             "", "| Scenario | Rep | HTTP accepted | Rejected | Success | Queue p95 ms | Run p95 ms | End-to-end p95 ms | Direct p95 ms | First 24 starts A/B/C |",
             "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |"]
    for item in summaries:
        value = lambda name: item[name]["p95"] if item[name]["p95"] is not None else "—"
        starts = item["first_24_starts_by_tenant"]
        lines.append(f"| {item['scenario']} | {item['repetition']} | {item['accepted']} | {item['rejected']} | "
                     f"{item['statuses'].get('succeeded', 0)} | {value('queue_ms')} | {value('run_ms')} | "
                     f"{value('end_to_end_ms')} | {value('direct_ms')} | "
                     f"{starts.get('A', 0)}/{starts.get('B', 0)}/{starts.get('C', 0)} |")
    lines += ["", "Raw records are in `runs/*/executions.csv`; resource samples, per-minute summaries, service logs, independent report ledgers and report files are beside each run.",
              "Each run used a fresh temporary SQLite state directory, six successful warmup tasks, and fixed three-tenant, two-agent credentials. Tokens and private state directories are not retained.",
              "Scheduled submissions record planned and actual send times. `closed_loop` waits for each agent's previous task before submitting the next; it is not an open arrival stream.",
              "Direct baselines have zero HTTP admissions; their elapsed times appear in the direct column. First-24 counts describe execution starts, not completion fairness.",
              "Queue, run and end-to-end times use service timestamps; direct fixture times use the client's monotonic clock. Cross-machine timing comparisons are invalid.",
              "", "## Coverage limits", "", "- This script tests trusted Process tools through HTTP and SQLite. Toolane no longer has a Docker backend; the script does not measure LLM inference.",
              "- CPU and RSS samples cover the Toolane server process only; fixture subprocess usage is not included.",
              "- The smoke profile checks wiring only; its rates and percentiles are not capacity evidence.",
              "- This script does not run a FIFO HTTP server comparison because the public service fixes tenant round robin.",
              "- The short profile does not establish long-term memory stability. Use the 30-minute soak profile and inspect per-minute resource samples.",
              "- Forced-crash semantics and external side effects are asserted by Go integration tests, not by timing measurements here.", ""]
    return "\n".join(lines)


def write_manifest(directory):
    files = {}
    for path in sorted(directory.rglob("*")):
        if path.is_file() and path.name != "manifest.json":
            data = path.read_bytes()
            files[str(path.relative_to(directory))] = {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
    (directory / "manifest.json").write_text(json.dumps(files, indent=2) + "\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--profile", choices=("smoke", "short", "soak"), default="smoke")
    parser.add_argument("--repetitions", type=int)
    parser.add_argument("--seed", type=int, default=20261021)
    parser.add_argument("--wait-seconds", type=int, default=180)
    parser.add_argument("--duration-seconds", type=int, default=1800)
    args = parser.parse_args(argv)
    if args.repetitions is None:
        args.repetitions = 3 if args.profile == "short" else 1
    if not 1 <= args.repetitions <= 10 or not 5 <= args.wait_seconds <= 600 or not 60 <= args.duration_seconds <= 86400:
        parser.error("repetitions must be 1..10, wait 5..600 seconds, duration 60..86400 seconds")
    binary = args.binary.resolve(strict=True)
    if args.out.exists() and (not args.out.is_dir() or any(args.out.iterdir())):
        parser.error("output directory must be new or empty")
    args.out.mkdir(parents=True, exist_ok=True)
    fixture = ROOT / "tests" / "fixtures" / "process_tools.py"
    metadata = {"generated_at": now(), "profile": args.profile, "repetitions": args.repetitions,
                "status": "running",
                "seed": args.seed, "wait_seconds": args.wait_seconds,
                "duration_seconds": args.duration_seconds if args.profile == "soak" else None,
                "git_revision": command_version(["git", "rev-parse", "HEAD"]),
                "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                "binary_build_info": command_version(["go", "version", "-m", str(binary)]),
                "fixture_sha256": hashlib.sha256(fixture.read_bytes()).hexdigest(),
                "dataset_sha256": DATASET_SHA256,
                "python_version": platform.python_version(), "go_version": command_version(["go", "version"]),
                "platform": platform.platform(), "cpu_count": os.cpu_count(), "cpu_model": cpu_model(),
                "git_dirty": bool(command_version(["git", "status", "--porcelain"])),
                "service_defaults": {"workers": 4, "global_pending": 256, "tenant_pending": 64,
                                     "tenant_running": 2, "scheduler": "round_robin"},
                "warmup_tasks_per_run": 6,
                "arrival_model": "scheduled, serial, closed_loop, direct as recorded per scenario",
                "runs": []}
    summaries = []
    try:
        for repetition in range(1, args.repetitions + 1):
            cases = scenarios(args.profile, args.seed + repetition)
            for name, mode, interval, items in cases:
                rows, summary = run_case(binary, args.out, name, repetition, mode,
                                         interval, items, args.wait_seconds,
                                         args.duration_seconds if args.profile == "soak" else None)
                summary.update(scenario=name, repetition=repetition)
                summaries.append(summary)
                metadata["runs"].append({"scenario": name, "repetition": repetition,
                                         "task_count": len(rows), "arrival_mode": mode,
                                         "interval_ms": interval, "seed": args.seed + repetition})
                print(f"{name} repetition={repetition} accepted={summary['accepted']} rejected={summary['rejected']}", flush=True)
        metadata["status"] = "complete"
    except Exception as error:
        metadata["status"] = "failed"
        metadata["failure"] = str(error)
        raise
    finally:
        (args.out / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
        (args.out / "summary.json").write_text(json.dumps(summaries, indent=2) + "\n")
        (args.out / "REPORT.md").write_text(report(metadata, summaries))
        write_manifest(args.out)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, RuntimeError, TimeoutError, ValueError, ToolaneError) as error:
        print(f"real tool experiment failed: {error}", file=sys.stderr)
        sys.exit(1)
