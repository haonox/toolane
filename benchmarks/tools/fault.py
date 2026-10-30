#!/usr/bin/env python3
"""Controlled success, nonzero exit, timeout, or output-limit failure."""

import json
import sys
import time


def main() -> int:
    if len(sys.argv) != 3:
        return 2
    mode, execution_id = sys.argv[1:]
    if mode == "ok":
        print(json.dumps({"kind": "bench.fault", "execution_id": execution_id, "mode": mode}))
        return 0
    if mode == "exit":
        print("intentional benchmark failure", file=sys.stderr)
        return 7
    if mode == "timeout":
        time.sleep(2)
        return 0
    if mode == "output":
        print(json.dumps({"kind": "bench.fault", "execution_id": execution_id, "payload": "x" * 8192}))
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
