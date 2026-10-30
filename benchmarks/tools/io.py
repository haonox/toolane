#!/usr/bin/env python3
"""Read a bounded number of rows from a local, read-only SQLite fixture."""

import hashlib
import json
import sqlite3
import sys
from pathlib import Path


FIXTURE = Path(__file__).resolve().parents[1] / "generated" / "io-fixture.sqlite"
RECORDS = 1024


def main() -> int:
    if len(sys.argv) != 3:
        return 2
    rows, execution_id = int(sys.argv[1]), sys.argv[2]
    if not 1 <= rows <= 256:
        return 2
    first = int(hashlib.sha256(execution_id.encode()).hexdigest()[:8], 16) % (RECORDS - rows + 1) + 1
    digest = hashlib.sha256()
    read_bytes = 0
    with sqlite3.connect(f"file:{FIXTURE}?mode=ro", uri=True, timeout=2) as database:
        records = database.execute(
            "SELECT payload FROM records WHERE id >= ? AND id < ? ORDER BY id", (first, first + rows)
        ).fetchall()
    if len(records) != rows:
        raise RuntimeError("incomplete I/O fixture")
    for (payload,) in records:
        raw = payload.encode("ascii")
        digest.update(raw)
        read_bytes += len(raw)
    print(json.dumps({"kind": "bench.io", "execution_id": execution_id,
                      "rows": rows, "read_bytes": read_bytes, "sha256": digest.hexdigest()}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
