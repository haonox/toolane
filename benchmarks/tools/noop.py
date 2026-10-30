#!/usr/bin/env python3
"""Minimal valid Process result for measuring engine and process overhead."""

import json
import sys


def main() -> int:
    if len(sys.argv) != 2:
        return 2
    print(json.dumps({"kind": "bench.noop", "execution_id": sys.argv[1]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
