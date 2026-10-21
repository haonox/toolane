#!/usr/bin/env python3
"""Deterministic Process fixtures; all paths and the query origin are admin-owned."""

import csv
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time
from urllib.parse import urlencode
from urllib.request import ProxyHandler, build_opener


def emit(value):
    print(json.dumps(value, sort_keys=True, separators=(",", ":")), flush=True)


def summarize(repetitions, execution_id):
    dataset = Path(__file__).with_name("orders.csv")
    data = dataset.read_bytes()
    rows = list(csv.DictReader(data.decode("utf-8").splitlines()))
    totals = {}
    for _ in range(repetitions):
        for row in rows:
            totals[row["tenant"]] = totals.get(row["tenant"], 0) + int(row["amount"])
    emit({"execution_id": execution_id, "rows": len(rows) * repetitions,
          "total": sum(totals.values()), "totals": totals,
          "dataset_sha256": hashlib.sha256(data).hexdigest()})


def query(delay_ms, status, execution_id):
    origin = os.environ["TOOLANE_FIXTURE_QUERY_ORIGIN"].rstrip("/")
    url = origin + "/query?" + urlencode({"delay_ms": delay_ms, "status": status})
    # The target is an administrator-fixed loopback service. Ignore ambient
    # proxy settings so the fixture never sends requests to an outside proxy.
    with build_opener(ProxyHandler({})).open(url, timeout=5) as response:
        payload = json.load(response)
    emit({"execution_id": execution_id, "response": payload})


def report(duration_ms, label, execution_id):
    output_dir = Path(os.environ["TOOLANE_FIXTURE_OUTPUT_DIR"])
    # The marker lets tests wait until the actual tool is running. It is not
    # evidence that the report side effect occurred.
    (output_dir / (execution_id + ".started")).write_text(str(os.getpid()) + "\n", encoding="utf-8")
    time.sleep(duration_ms / 1000)
    report_path = output_dir / (execution_id + ".json")
    report_bytes = json.dumps({"execution_id": execution_id, "label": label},
                              sort_keys=True).encode("utf-8") + b"\n"
    temporary_path = output_dir / (execution_id + ".tmp")
    with temporary_path.open("xb") as output:
        output.write(report_bytes)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary_path, report_path)
    record = json.dumps({"execution_id": execution_id, "label": label,
                         "report_sha256": hashlib.sha256(report_bytes).hexdigest()},
                        sort_keys=True).encode("utf-8") + b"\n"
    ledger = os.open(output_dir / "ledger.jsonl", os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
    try:
        if os.write(ledger, record) != len(record):
            raise OSError("incomplete ledger write")
        os.fsync(ledger)
    finally:
        os.close(ledger)
    emit({"execution_id": execution_id, "label": label,
          "report_sha256": hashlib.sha256(report_bytes).hexdigest()})


def main():
    if len(sys.argv) < 2:
        raise ValueError("expected a fixture mode")
    if not re.fullmatch(r"[0-9a-f]{32}", sys.argv[-1]):
        raise ValueError("expected a Toolane execution ID")
    mode = sys.argv[1]
    if mode == "summarize" and len(sys.argv) == 4:
        summarize(int(sys.argv[2]), sys.argv[3])
    elif mode == "query" and len(sys.argv) == 5:
        query(int(sys.argv[2]), int(sys.argv[3]), sys.argv[4])
    elif mode == "report" and len(sys.argv) == 5:
        report(int(sys.argv[2]), sys.argv[3], sys.argv[4])
    else:
        raise ValueError("invalid fixture arguments")


if __name__ == "__main__":
    main()
