#!/usr/bin/env python3
"""Build, start, stress and stop a local Toolane server using only benchmark assets."""

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


HERE = Path(__file__).resolve().parent
REPO = HERE.parent
GENERATED = HERE / "generated"
SERVER = GENERATED / "server.json"
LOCAL_HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def ready(base_url: str) -> bool:
    try:
        with LOCAL_HTTP.open(base_url + "/readyz", timeout=1) as response:
            return response.status == 200
    except (urllib.error.URLError, TimeoutError):
        return False


def metrics(url: str, path: Path) -> None:
    try:
        with LOCAL_HTTP.open(url + "/metrics", timeout=3) as response:
            path.write_bytes(response.read())
    except (urllib.error.URLError, TimeoutError) as error:
        path.write_text(f"metrics unavailable: {error}\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quick", action="store_true", help="run one complete tool call per configured tenant")
    parser.add_argument("--stages-file", type=Path, default=HERE / "config" / "stages.json")
    args = parser.parse_args()
    profile = json.loads((HERE / "config" / "profile.json").read_text(encoding="utf-8"))
    if ready("http://" + profile["business_listen"]):
        raise RuntimeError("benchmark business port is already serving; stop the previous server first")
    subprocess.run([sys.executable, str(HERE / "prepare.py")], check=True)
    config = json.loads(SERVER.read_text(encoding="utf-8"))
    base_url = "http://" + config["business_listen"]
    metrics_url = "http://" + config["metrics_listen"]
    if ready(base_url):
        raise RuntimeError(f"{base_url} is already serving; use dedicated benchmark ports")
    stages = ([{"name": "tenant-check", "concurrency": min(128, len(config["tokens"])),
                "requests": len(config["tokens"])}] if args.quick
              else json.loads(args.stages_file.read_text(encoding="utf-8"))["stages"])
    if not stages or any(stage["requests"] <= 0 or stage["concurrency"] <= 0 for stage in stages):
        raise ValueError("stages need positive concurrency and request counts")
    run_dir = GENERATED / "results" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{os.getpid()}")
    run_dir.mkdir(parents=True, mode=0o700)
    build_env = dict(os.environ, GOCACHE=str(GENERATED / "go-cache"))
    subprocess.run(["go", "build", "-o", str(GENERATED / "toolane"), "./cmd/toolane"], cwd=REPO, env=build_env, check=True)
    subprocess.run(["go", "build", "-o", str(GENERATED / "load"), "./benchmarks/http"], cwd=REPO, env=build_env, check=True)
    metadata = {"started_at": datetime.now(timezone.utc).isoformat(), "cpu_count": os.cpu_count(),
                "profile": profile, "stages": stages, "python": sys.version.split()[0]}
    (run_dir / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(f"Results: {run_dir}", flush=True)
    with (run_dir / "server.log").open("w", encoding="utf-8") as log:
        server = subprocess.Popen([str(GENERATED / "toolane"), "serve", "--config", str(SERVER)],
                                  cwd=REPO, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            for _ in range(100):
                if ready(base_url):
                    break
                if server.poll() is not None:
                    raise RuntimeError(f"server exited early; see {run_dir / 'server.log'}")
                time.sleep(0.1)
            else:
                raise RuntimeError(f"server did not become ready; see {run_dir / 'server.log'}")
            for stage in stages:
                name = stage["name"]
                output = run_dir / f"{name}.json"
                metrics(metrics_url, run_dir / f"{name}-before.prom")
                started = time.monotonic()
                command = [str(GENERATED / "load"), "-base-url", base_url, "-tokens-file", str(SERVER),
                           "-workload-file", str(HERE / "config" / "workload.json"),
                           "-requests", str(stage["requests"]), "-concurrency", str(stage["concurrency"]),
                           "-wait-timeout", "60s", "-output", str(output)]
                with (run_dir / f"{name}.stderr").open("w", encoding="utf-8") as errors:
                    result = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=errors, cwd=REPO)
                metrics(metrics_url, run_dir / f"{name}-after.prom")
                if result.returncode != 0:
                    raise RuntimeError(f"stage {name} failed; see {run_dir / f'{name}.stderr'}")
                summary = json.loads(output.read_text(encoding="utf-8"))
                per_tenant = summary["per_tenant"]
                active_tenants = len(per_tenant)
                min_calls = min(counts["submitted"] for counts in per_tenant.values())
                max_calls = max(counts["submitted"] for counts in per_tenant.values())
                min_success = min(counts["succeeded"] for counts in per_tenant.values())
                max_success = max(counts["succeeded"] for counts in per_tenant.values())
                print(f"{name}: concurrency={stage['concurrency']} calls={stage['requests']} "
                      f"tenants={active_tenants} calls/tenant={min_calls}..{max_calls} "
                      f"success/tenant={min_success}..{max_success} "
                      f"accepted={summary['admission_http_status'].get('202', 0)} "
                      f"rejected429={summary['admission_http_status'].get('429', 0)} "
                      f"succeeded={summary['final_status'].get('succeeded', 0)} "
                      f"rate={summary['succeeded_per_second']:.1f}/s "
                      f"p99={summary['end_to_end_latency']['p99_ms']:.1f}ms "
                      f"errors={sum(summary['errors'].values())} elapsed={time.monotonic()-started:.1f}s", flush=True)
                if any(key not in ("admission_http_429",) for key in summary["errors"]):
                    print("Stopped the matrix after unexpected errors; inspect the stage JSON", file=sys.stderr)
                    return 1
            return 0
        finally:
            if server.poll() is None:
                os.killpg(server.pid, signal.SIGTERM)
                try:
                    server.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    os.killpg(server.pid, signal.SIGKILL)
                    server.wait()
            shutil.rmtree(GENERATED / "__pycache__", ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
