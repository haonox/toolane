#!/usr/bin/env python3
"""Trusted example tool: one fixed argument, one JSON result on stdout."""

import hashlib
import json
import sys


def main():
    if len(sys.argv) != 3:
        print("expected text and Toolane execution ID", file=sys.stderr)
        return 2
    text, execution_id = sys.argv[1:]
    result = {
        "execution_id": execution_id,
        "characters": len(text),
        "words": len(text.split()),
        "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
    }
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
