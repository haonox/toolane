#!/usr/bin/env python3
"""Cheap text processing, representative of a short synchronous tool."""

import json
import re
import sys


def main() -> int:
    if len(sys.argv) != 3:
        return 2
    text, execution_id = sys.argv[1:]
    normalized = " ".join(text.casefold().split())
    words = re.findall(r"\w+", normalized)
    print(json.dumps({"kind": "bench.text", "execution_id": execution_id,
                      "normalized": normalized, "word_count": len(words)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
