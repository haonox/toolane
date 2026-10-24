#!/usr/bin/env python3
"""CPU work, representative of document fingerprinting or validation."""

import hashlib
import json
import sys


def main() -> int:
    if len(sys.argv) != 4:
        return 2
    text, rounds, execution_id = sys.argv[1:]
    digest = text.encode("utf-8")
    for _ in range(int(rounds)):
        digest = hashlib.sha256(digest).digest()
    print(json.dumps({"kind": "bench.hash", "execution_id": execution_id,
                      "rounds": int(rounds), "sha256": digest.hex()}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
