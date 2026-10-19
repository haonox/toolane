#!/usr/bin/env python3
"""Start an isolated local Toolane service and run the bounded Mock HTTP experiment."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import time
from urllib.request import ProxyHandler, build_opener

from http_service import main as run_experiment


def free_loopback_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--seed", type=int, default=20261020)
    parser.add_argument("--interval-ms", type=int, default=2)
    args = parser.parse_args(argv)
    binary = args.binary.resolve(strict=True)
    with tempfile.TemporaryDirectory(prefix="toolane-http-benchmark-") as directory:
        private = Path(directory)
        http_port, metrics_port = free_loopback_port(), free_loopback_port()
        if http_port == metrics_port:
            metrics_port = free_loopback_port()
        tokens = {tenant: secrets.token_hex(24) for tenant in ("A", "B", "C")}
        config = {"listen": f"127.0.0.1:{http_port}", "metrics_listen": f"127.0.0.1:{metrics_port}",
                  "state_dir": str(private / "state"),
                  "tokens": [{"token": token, "tenant_id": tenant, "tools": ["mock.delay"]}
                             for tenant, token in tokens.items()]}
        config_path = private / "config.json"
        config_path.write_text(json.dumps(config))
        config_path.chmod(0o600)
        log_path = private / "service.log"
        with log_path.open("w") as log:
            process = subprocess.Popen([str(binary), "serve", "--config", str(config_path)],
                                       stdout=log, stderr=log)
            try:
                base = f"http://127.0.0.1:{http_port}"
                opener = build_opener(ProxyHandler({}))
                deadline = time.monotonic() + 10
                while True:
                    if process.poll() is not None:
                        raise RuntimeError("Toolane service exited during startup")
                    try:
                        with opener.open(base + "/readyz", timeout=0.5) as response:
                            if response.status == 200:
                                break
                    except OSError:
                        pass
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Toolane service did not become ready")
                    time.sleep(0.05)
                old = {"TOOLANE_TOKEN_" + tenant: os.environ.get("TOOLANE_TOKEN_" + tenant)
                       for tenant in tokens}
                try:
                    for tenant, token in tokens.items():
                        os.environ["TOOLANE_TOKEN_" + tenant] = token
                    status = run_experiment(["--base-url", base, "--metrics-url", f"http://127.0.0.1:{metrics_port}",
                                             "--server-pid", str(process.pid), "--out", str(args.out),
                                             "--repetitions", str(args.repetitions), "--seed", str(args.seed),
                                             "--interval-ms", str(args.interval_ms)])
                finally:
                    for name, value in old.items():
                        if value is None:
                            os.environ.pop(name, None)
                        else:
                            os.environ[name] = value
                if status != 0:
                    return status
                metadata_path = args.out / "metadata.json"
                metadata = json.loads(metadata_path.read_text())
                metadata["service_binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
                metadata["service_config"] = {"tool": "mock.delay", "tenants": 3,
                                               "workers": 4, "global_pending_limit": 256,
                                               "tenant_pending_limit": 64, "tenant_running_limit": 2,
                                               "state_dir": "fresh temporary directory"}
                metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n")
                return 0
            finally:
                if process.poll() is None:
                    process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                if process.returncode not in (0, -signal.SIGTERM):
                    print("Toolane service log tail:", file=sys.stderr)
                    print("\n".join(log_path.read_text().splitlines()[-10:]), file=sys.stderr)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
        print(f"HTTP experiment failed: {exc}", file=sys.stderr)
        sys.exit(1)
