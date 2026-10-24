#!/usr/bin/env python3
"""Bounded I/O wait, representative of a customer database or remote service."""

import hashlib
import json
import sys
import time


def main() -> int:
    if len(sys.argv) != 4:
        return 2
    customer_id, delay_ms, execution_id = sys.argv[1:]
    time.sleep(int(delay_ms) / 1000)
    bucket = int(hashlib.sha256(customer_id.encode()).hexdigest()[:4], 16) % 3
    print(json.dumps({"kind": "bench.lookup", "execution_id": execution_id,
                      "customer_id": customer_id, "tier": ["basic", "plus", "enterprise"][bucket]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
