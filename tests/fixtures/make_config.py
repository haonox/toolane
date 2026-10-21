#!/usr/bin/env python3
"""Create a private Toolane config for the three real Process fixtures."""

import argparse
import json
import os
from pathlib import Path
import secrets
import sys
from urllib.parse import urlsplit


def local_origin(raw):
    parsed = urlsplit(raw)
    if (parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost"}
            or parsed.port is None or parsed.username or parsed.password
            or parsed.path or parsed.query or parsed.fragment):
        raise ValueError("query origin must be a plain local HTTP origin with a port")
    return raw


def make_tool(name, mode, description, properties, required, args, python, script, output_dir, query_origin):
    return {
        "name": name, "description": description,
        "parameters": {"type": "object", "properties": properties,
                       "required": required, "additionalProperties": False},
        "executable": str(python), "directory": str(script.parent),
        "environment": {"PYTHONDONTWRITEBYTECODE": "1",
                        "TOOLANE_FIXTURE_OUTPUT_DIR": str(output_dir),
                        "TOOLANE_FIXTURE_QUERY_ORIGIN": query_origin},
        "args": [{"literal": str(script)}, {"literal": mode}] +
                [{"parameter": name} for name in args] + [{"execution_id": True}],
        "run_timeout_ms": 5000, "max_output_bytes": 8192, "max_result_bytes": 8192,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--query-origin", default="http://127.0.0.1:18081")
    parser.add_argument("--listen", default="127.0.0.1:8080")
    args = parser.parse_args()
    try:
        query_origin = local_origin(args.query_origin)
    except ValueError as error:
        parser.error(str(error))
    config_path = args.config.resolve()
    if config_path.exists():
        parser.error("config already exists; choose a new path")
    state_dir = args.state_dir.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    if output_dir.stat().st_mode & 0o077:
        parser.error("output directory must not be accessible to group or others")
    script = Path(__file__).resolve().with_name("process_tools.py")
    python = Path(sys.executable).resolve()
    token_a, token_b = secrets.token_hex(32), secrets.token_hex(32)
    integer = lambda minimum, maximum: {"type": "integer", "minimum": minimum, "maximum": maximum}
    config = {
        "listen": args.listen,
        "state_dir": str(state_dir),
        "process_tools": [
            make_tool("data.summarize", "summarize", "Summarize the fixed orders CSV.",
                      {"repetitions": integer(1, 200000)}, ["repetitions"], ["repetitions"],
                      python, script, output_dir, query_origin),
            make_tool("service.query", "query", "Query a controlled local HTTP service.",
                      {"delay_ms": integer(0, 1000), "status": integer(200, 599)},
                      ["delay_ms", "status"], ["delay_ms", "status"],
                      python, script, output_dir, query_origin),
            make_tool("report.generate", "report", "Generate a report and independent ledger record.",
                      {"duration_ms": integer(1000, 3000),
                       "label": {"type": "string", "minLength": 1, "maxLength": 80}},
                      ["duration_ms", "label"], ["duration_ms", "label"],
                      python, script, output_dir, query_origin),
        ],
        "tokens": [
            {"token": token_a, "tenant_id": "fixtureA",
             "tools": ["data.summarize", "service.query", "report.generate"]},
            {"token": token_b, "tenant_id": "fixtureB",
             "tools": ["data.summarize", "service.query"]},
        ],
    }
    config_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(config_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as output:
        json.dump(config, output, indent=2)
        output.write("\n")
    print(f"config: {config_path}")
    print(f"tenant A token: {token_a}")
    print(f"tenant B token: {token_b}")


if __name__ == "__main__":
    main()
