#!/usr/bin/env python3
"""Create a private, portable two-tenant Process demo configuration."""

import argparse
import json
import os
from pathlib import Path
import secrets
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args()
    config_path = args.config.resolve()
    config_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if config_path.exists():
        parser.error("config already exists; choose a new path to avoid replacing credentials")
    script = (Path(__file__).resolve().parent / "tools" / "text_stats.py").resolve()
    token_a, token_b = secrets.token_hex(32), secrets.token_hex(32)
    schema = {
        "type": "object",
        "properties": {"text": {"type": "string", "minLength": 1, "maxLength": 2000}},
        "required": ["text"],
        "additionalProperties": False,
    }
    config = {
        "listen": "127.0.0.1:8080",
        "process_tools": [{
            "name": "text.stats",
            "description": "Count characters and words in text and return its SHA-256 hash.",
            "parameters": schema,
            "executable": str(Path(sys.executable).resolve()),
            "directory": str(script.parent.parent.parent),
            "environment": {"PYTHONDONTWRITEBYTECODE": "1"},
            "args": [
                {"literal": str(script)},
                {"parameter": "text"},
                {"execution_id": True},
            ],
            "run_timeout_ms": 5000,
            "max_output_bytes": 8192,
            "max_result_bytes": 8192,
        }],
        "tokens": [
            {"token": token_a, "tenant_id": "demoA", "tools": ["text.stats"]},
            {"token": token_b, "tenant_id": "demoB", "tools": ["text.stats"]},
        ],
    }
    fd = os.open(config_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as output:
        json.dump(config, output, ensure_ascii=False, indent=2)
        output.write("\n")
    print(f"config: {config_path}")
    print(f"tenant A token: {token_a}")
    print(f"tenant B token: {token_b}")


if __name__ == "__main__":
    main()
