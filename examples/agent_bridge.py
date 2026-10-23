#!/usr/bin/env python3
"""Simulate one model function call through Toolane, without an LLM API key."""

import argparse
import ipaddress
import json
import os
import re
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


TOOL_NAME = "text.stats"
TERMINAL = {"succeeded", "failed", "timed_out", "cancelled"}
EXECUTION_ID = re.compile(r"[0-9a-f]{32}\Z")


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


class ToolaneClient:
    def __init__(self, base_url, token):
        parsed = urlsplit(base_url)
        try:
            local = parsed.hostname == "localhost" or ipaddress.ip_address(parsed.hostname).is_loopback
        except (ValueError, TypeError):
            local = False
        if (parsed.scheme != "http" or not local or parsed.port is None or
                parsed.username or parsed.password or parsed.path not in ("", "/") or
                parsed.query or parsed.fragment):
            raise ValueError("base URL must be a local HTTP origin with an explicit port")
        if not token:
            raise ValueError("set TOOLANE_TOKEN to a token from examples/server.json")
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.opener = build_opener(ProxyHandler({}), NoRedirect())

    def request(self, method, path, body=None):
        headers = {"Authorization": "Bearer " + self.token, "Accept": "application/json"}
        data = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        request = Request(self.base_url + path, data=data, headers=headers, method=method)
        try:
            with self.opener.open(request, timeout=10) as response:
                raw = response.read(2 * 1024 * 1024 + 1)
        except HTTPError as error:
            with error:
                detail = json.loads(error.read(8192))
            raise RuntimeError(f"Toolane HTTP {error.code}: {detail.get('code', 'unknown')}") from None
        except URLError as error:
            raise RuntimeError(f"cannot reach Toolane: {error.reason}") from None
        if len(raw) > 2 * 1024 * 1024:
            raise RuntimeError("Toolane response exceeds 2 MiB")
        return json.loads(raw)


def show(label, value):
    print(f"{label}:")
    print(json.dumps(value, ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--text", default="hello Toolane")
    parser.add_argument("--call-id", default="call_demo_1")
    parser.add_argument("--wait-seconds", type=float, default=20)
    args = parser.parse_args()
    if not args.call_id or not 0 < args.wait_seconds <= 300:
        parser.error("call ID must be nonempty and wait time must be 0..300 seconds")
    try:
        client = ToolaneClient(args.base_url, os.environ.get("TOOLANE_TOKEN"))

        # These are the public definitions an Agent would provide to a model.
        definitions = [{"type": "function", **item} for item in client.request("GET", "/api/v1/tools")["tools"]]
        if not any(item["name"] == TOOL_NAME for item in definitions):
            raise RuntimeError(f"this token cannot use {TOOL_NAME}")
        show("1. Tools available to the model", definitions)

        # This item stands in for a model response. No model API is called here.
        model_call = {"type": "function_call", "call_id": args.call_id,
                      "name": TOOL_NAME, "arguments": json.dumps({"text": args.text}, ensure_ascii=False)}
        show("2. Simulated model function call", model_call)

        accepted = client.request("POST", "/api/v1/executions", {
            "agent_id": "example-agent", "tool": model_call["name"],
            "arguments": json.loads(model_call["arguments"]),
        })
        execution_id = accepted.get("execution_id", "")
        if not EXECUTION_ID.fullmatch(execution_id):
            raise RuntimeError("Toolane did not return a valid execution ID")
        show("3. Toolane accepted the execution", accepted)

        deadline = time.monotonic() + args.wait_seconds
        while True:
            snapshot = client.request("GET", "/api/v1/executions/" + execution_id)
            if snapshot["status"] in TERMINAL:
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError(f"wait timed out; execution {execution_id} remains in Toolane")
            time.sleep(min(0.2, remaining))
        show("4. Toolane final state and result", snapshot)

        if snapshot["status"] == "succeeded":
            payload = {"execution_id": execution_id, "result": snapshot.get("result")}
        else:
            payload = {"execution_id": execution_id, "status": snapshot["status"],
                       "error_code": snapshot.get("error_code"),
                       "error_message": snapshot.get("error_message")}
        model_output = {"type": "function_call_output", "call_id": model_call["call_id"],
                        "output": json.dumps(payload, ensure_ascii=False)}
        show("5. Output the Agent would return to the model", model_output)
        return 0 if snapshot["status"] == "succeeded" else 1
    except (RuntimeError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"Agent bridge: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
