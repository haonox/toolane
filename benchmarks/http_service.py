#!/usr/bin/env python3
"""Bounded, serial scheduled-arrival experiment against a local Mock HTTP service."""

import argparse
import csv
import json
import os
from pathlib import Path
import platform
import random
import subprocess
import sys
import time
from datetime import datetime, timezone
from urllib.request import ProxyHandler, build_opener

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from examples.python_client import TERMINAL, ToolaneClient, ToolaneError, local_origin  # noqa: E402


TENANTS = ("A", "B", "C")
SCENARIOS = ("short", "long", "mixed", "burst")
FIELDS = ("scenario", "repetition", "arrival_index", "tenant", "delay_ms", "scheduled_at",
          "submit_started_at", "submit_completed_at", "schedule_lag_ms", "submit_ms",
          "http_status", "rejection_code", "execution_id", "status", "error_code",
          "created_at", "ready_at", "running_at", "process_started_at", "finished_at",
          "queue_ms", "ready_wait_ms", "execution_ms", "total_ms")


def workload(scenario, seed):
    """Fixed task counts and tenant distribution for one repetition."""
    if scenario not in SCENARIOS:
        raise ValueError("unknown scenario")
    tenants = (["A"] * 48 + ["B"] * 12 + ["C"] * 12) if scenario == "burst" else list(TENANTS) * 24
    delays = [2] * 72 if scenario == "short" else [50] * 72 if scenario == "burst" else [15] * 72
    if scenario == "mixed":
        delays = [2] * 36 + [15] * 36
    rng = random.Random(seed)
    if scenario != "burst":
        rng.shuffle(tenants)
    if scenario == "mixed":
        rng.shuffle(delays)
    return list(zip(tenants, delays))


def iso_now():
    return datetime.now(timezone.utc).isoformat()


def elapsed_ms(start, end):
    if not start or not end:
        return ""
    return round((datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds() * 1000, 3)


def service_sample(metrics_url, pid):
    result = {"at": iso_now()}
    if metrics_url:
        with build_opener(ProxyHandler({})).open(metrics_url + "/metrics", timeout=5) as response:
            metrics = response.read(256 * 1024).decode("utf-8")
        for name in ("toolane_goroutines", "toolane_running", "toolane_queued", "toolane_pending"):
            for line in metrics.splitlines():
                if line.startswith(name + " "):
                    result[name] = int(float(line.split()[1]))
                    break
            else:
                raise ValueError("missing service metric " + name)
    if pid:
        stat = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()
        result["cpu_seconds"] = round((int(stat[11]) + int(stat[12])) / os.sysconf("SC_CLK_TCK"), 3)
        statm = Path(f"/proc/{pid}/statm").read_text().split()
        result["rss_bytes"] = int(statm[1]) * os.sysconf("SC_PAGE_SIZE")
    return result


def wait_terminal(client, execution_id, deadline):
    while True:
        result = client.get_execution(execution_id)
        if result.get("status") in TERMINAL:
            return result
        if time.monotonic() >= deadline:
            raise TimeoutError(f"execution {execution_id} did not terminate; inspect the service before rerunning")
        time.sleep(0.02)


def submit_batch(clients, tasks, scenario, repetition, interval_ms, wait_seconds, rows):
    start = time.monotonic()
    batch = []
    for index, (tenant, delay_ms) in enumerate(tasks):
        scheduled = start + index * interval_ms / 1000
        remaining = scheduled - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
        started_mono = time.monotonic()
        started = iso_now()
        row = dict.fromkeys(FIELDS, "")
        row.update(scenario=scenario, repetition=repetition, arrival_index=index, tenant=tenant,
                   delay_ms=delay_ms, scheduled_at=round(scheduled - start, 6),
                   submit_started_at=started, schedule_lag_ms=round(max(0, started_mono - scheduled) * 1000, 3))
        try:
            accepted = clients[tenant].submit_mock(delay_ms, "benchmark")
        except ToolaneError as exc:
            if not str(exc).startswith("HTTP 429: capacity_exhausted "):
                raise
            row["http_status"], row["rejection_code"] = 429, "capacity_exhausted"
        else:
            execution_id = accepted.get("execution_id")
            if not isinstance(execution_id, str) or len(execution_id) != 32:
                raise ValueError("accepted response has no execution ID")
            row["http_status"], row["execution_id"] = 202, execution_id
        row["submit_completed_at"] = iso_now()
        row["submit_ms"] = round((time.monotonic() - started_mono) * 1000, 3)
        batch.append(row)
    deadline = time.monotonic() + wait_seconds
    for row in batch:
        if row["http_status"] != 202:
            continue
        snapshot = wait_terminal(clients[row["tenant"]], row["execution_id"], deadline)
        for field in ("created_at", "ready_at", "running_at", "process_started_at", "finished_at"):
            row[field] = snapshot.get(field) or ""
        row["status"] = snapshot["status"]
        row["error_code"] = snapshot.get("error_code", "")
        row["queue_ms"] = elapsed_ms(row["created_at"], row["running_at"])
        row["ready_wait_ms"] = elapsed_ms(row["ready_at"], row["process_started_at"])
        row["execution_ms"] = elapsed_ms(row["process_started_at"] or row["running_at"], row["finished_at"])
        row["total_ms"] = elapsed_ms(row["created_at"], row["finished_at"])
    rows.extend(batch)
    return {"elapsed_seconds": round(time.monotonic() - start, 3),
            "accepted": sum(row["http_status"] == 202 for row in batch),
            "rejected": sum(row["http_status"] == 429 for row in batch)}


def command_version(command):
    try:
        return subprocess.check_output(command, text=True, stderr=subprocess.DEVNULL, timeout=5).strip()
    except (OSError, subprocess.SubprocessError):
        return "unavailable"


def cpu_model():
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unavailable"


def percentile(values, percentage):
    if not values:
        return None
    values = sorted(values)
    rank = max(0, (len(values) * percentage + 99) // 100 - 1)
    return values[rank]


def summarize(rows, batches):
    result = []
    for batch in batches:
        selected = [row for row in rows if row["scenario"] == batch["scenario"] and
                    row["repetition"] == batch["repetition"]]
        accepted = [row for row in selected if row["http_status"] == 202]
        item = {"scenario": batch["scenario"], "repetition": batch["repetition"],
                "accepted": len(accepted), "rejected": len(selected) - len(accepted),
                "rejection_ratio": round((len(selected) - len(accepted)) / len(selected), 6),
                "terminal_statuses": {state: sum(row["status"] == state for row in accepted) for state in sorted(TERMINAL)},
                "completed_per_second": round(len(accepted) / batch["elapsed_seconds"], 3),
                "max_schedule_lag_ms": max(row["schedule_lag_ms"] for row in selected)}
        for field in ("queue_ms", "ready_wait_ms", "execution_ms", "total_ms"):
            values = [row[field] for row in accepted if row[field] != ""]
            item[field] = {f"p{p}": percentile(values, p) for p in (50, 95, 99)}
        result.append(item)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--metrics-url", required=True)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--server-pid", type=int)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--seed", type=int, default=20261020)
    parser.add_argument("--interval-ms", type=int, default=2)
    parser.add_argument("--wait-seconds", type=int, default=30)
    args = parser.parse_args(argv)
    if not 1 <= args.repetitions <= 10 or not 0 <= args.interval_ms <= 1000 or not 1 <= args.wait_seconds <= 300 or (args.server_pid is not None and args.server_pid < 1):
        parser.error("repetitions must be 1..10, interval 0..1000 ms, wait 1..300 s, and PID positive")
    base_url, metrics_url = local_origin(args.base_url), local_origin(args.metrics_url)
    tokens = {tenant: os.environ.get("TOOLANE_TOKEN_" + tenant) for tenant in TENANTS}
    if any(not token for token in tokens.values()):
        parser.error("TOOLANE_TOKEN_A, TOOLANE_TOKEN_B and TOOLANE_TOKEN_C are required")
    if args.out.exists() and (not args.out.is_dir() or any(args.out.iterdir())):
        parser.error("output directory must be new or empty")
    args.out.mkdir(parents=True, exist_ok=True)
    clients = {tenant: ToolaneClient(base_url, token) for tenant, token in tokens.items()}
    metadata = {"generated_at": iso_now(), "experiment": "Mock HTTP service",
                "service_url": base_url, "metrics_url": metrics_url, "server_pid": args.server_pid,
                "python_version": platform.python_version(), "go_version": command_version(["go", "version"]),
                "platform": platform.platform(), "cpu_count": os.cpu_count(), "cpu_model": cpu_model(),
                "git_revision": command_version(["git", "rev-parse", "HEAD"]),
                "seed": args.seed, "repetitions": args.repetitions, "tasks_per_scenario": 72,
                "warmup_tasks_per_repetition": 6, "interval_ms": args.interval_ms,
                "arrival_model": "serial scheduled submissions; interval_ms except burst (zero); client request latency causes positive schedule lag",
                "scenario_delays_ms": {"short": 2, "long": 15, "mixed": [2, 15], "burst": 50},
                "poll_interval_ms": 20, "wait_seconds": args.wait_seconds,
                "notes": "Metrics and /proc samples taken before and after each measured batch; no credentials are stored.",
                "batches": []}
    rows = []
    for scenario_index, scenario in enumerate(SCENARIOS):
        for repetition in range(1, args.repetitions + 1):
            for tenant in TENANTS:
                for _ in range(2):
                    warmup = clients[tenant].submit_mock(2, "warmup")
                    wait_terminal(clients[tenant], warmup["execution_id"], time.monotonic() + args.wait_seconds)
            before = service_sample(metrics_url, args.server_pid)
            tasks = workload(scenario, args.seed + scenario_index * 1000 + repetition)
            result = submit_batch(clients, tasks, scenario, repetition,
                                  0 if scenario == "burst" else args.interval_ms, args.wait_seconds, rows)
            after = service_sample(metrics_url, args.server_pid)
            metadata["batches"].append({"scenario": scenario, "repetition": repetition,
                                         "before": before, "after": after, **result})
            print(f"{scenario} repetition={repetition} accepted={result['accepted']} rejected={result['rejected']} elapsed={result['elapsed_seconds']}s", flush=True)
    with (args.out / "executions.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    (args.out / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n")
    (args.out / "summary.json").write_text(json.dumps(summarize(rows, metadata["batches"]), indent=2) + "\n")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ToolaneError, OSError, TimeoutError, ValueError) as exc:
        print(f"HTTP experiment failed: {exc}", file=sys.stderr)
        sys.exit(1)
