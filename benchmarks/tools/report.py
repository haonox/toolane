#!/usr/bin/env python3
"""Larger JSON result, representative of a report or search result set."""

import json
import sys


def main() -> int:
    if len(sys.argv) != 4:
        return 2
    account_id, items, execution_id = sys.argv[1:]
    rows = [{"item_id": f"{account_id}-{index:04d}", "amount_cents": 1000 + index * 37,
             "description": "Monthly account activity and reconciliation entry"}
            for index in range(int(items))]
    print(json.dumps({"kind": "bench.report", "execution_id": execution_id,
                      "account_id": account_id, "items": rows}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
