#!/usr/bin/env python3
"""Minimal Agent integration: discover tools and map a model call to Toolane."""

import argparse
import json
import math
import os
import sys
import time

from python_client import EXECUTION_ID, TERMINAL, ToolaneClient, ToolaneError


def model_tool_definitions(client):
    """Public descriptions suitable for an Agent's function-tool list."""
    return [{"type": "function", **definition} for definition in client.list_tools()["tools"]]


def invoke_tool_call(client, call_id, name, arguments, wait_seconds=20):
    """Return a call_id-matched output; never treat Toolane's ID as call_id."""
    if not isinstance(arguments, dict):
        raise ValueError("tool arguments must be a JSON object")
    accepted = client.submit_tool(name, arguments)
    execution_id = accepted.get("execution_id")
    if not isinstance(execution_id, str) or not EXECUTION_ID.fullmatch(execution_id):
        raise ToolaneError("accepted response has no valid execution ID")
    deadline = time.monotonic() + wait_seconds
    while True:
        snapshot = client.get_execution(execution_id)
        status = snapshot.get("status")
        if status in TERMINAL:
            if status == "succeeded":
                payload = {"execution_id": execution_id, "result": snapshot.get("result")}
            else:
                payload = {"execution_id": execution_id, "status": status,
                           "error_code": snapshot.get("error_code"),
                           "error_message": snapshot.get("error_message")}
            return {"call_id": call_id, "output": json.dumps(payload, ensure_ascii=False)}
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ToolaneError(f"wait timed out; execution {execution_id} remains managed by Toolane")
        time.sleep(min(0.2, remaining))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("describe", help="print public function-tool descriptions")
    invoke = commands.add_parser("invoke", help="submit and wait for one model tool call")
    invoke.add_argument("--call-id", required=True)
    invoke.add_argument("--tool", required=True)
    invoke.add_argument("--arguments", required=True, help="JSON object")
    invoke.add_argument("--wait-seconds", type=float, default=20)
    args = parser.parse_args()
    try:
        client = ToolaneClient(args.base_url, os.environ.get("TOOLANE_TOKEN"))
        if args.command == "describe":
            print(json.dumps(model_tool_definitions(client), ensure_ascii=False, indent=2))
            return 0
        if not math.isfinite(args.wait_seconds) or args.wait_seconds <= 0:
            raise ValueError("wait seconds must be finite and positive")
        result = invoke_tool_call(client, args.call_id, args.tool, json.loads(args.arguments), args.wait_seconds)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (ToolaneError, ValueError, KeyError) as exc:
        print(f"Agent bridge: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
