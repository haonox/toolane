#!/usr/bin/env python3
"""Small standard-library client for a locally running Toolane service."""

import argparse
import ipaddress
import json
import math
import os
import re
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


TERMINAL = {"succeeded", "failed", "timed_out", "cancelled", "skipped", "interrupted"}
EXECUTION_ID = re.compile(r"[0-9a-f]{32}\Z")
MAX_RESPONSE_BYTES = 2 * 1024 * 1024


class ToolaneError(Exception):
    """An API or transport failure that is safe to show without credentials."""


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


def local_origin(value):
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password:
        raise ValueError("base URL must be a local HTTP(S) origin without credentials")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise ValueError("base URL must not contain a path, query or fragment")
    try:
        host = parsed.hostname
        port = parsed.port
        loopback = host == "localhost" or ipaddress.ip_address(host).is_loopback
    except (ValueError, TypeError):
        loopback = False
    if not loopback or port is None:
        raise ValueError("base URL must use a loopback host and explicit port")
    return value.rstrip("/")


def execution_path(execution_id):
    if not EXECUTION_ID.fullmatch(execution_id):
        raise ValueError("execution ID must contain 32 lowercase hexadecimal characters")
    return "/api/v1/executions/" + execution_id


class ToolaneClient:
    def __init__(self, base_url, token, timeout_seconds=10):
        self.base_url = local_origin(base_url)
        if not token or any(ord(char) < 33 or ord(char) > 126 for char in token):
            raise ValueError("TOOLANE_TOKEN must be a nonempty printable ASCII token")
        self.token = token
        self.timeout_seconds = timeout_seconds
        # Keep the local credential away from ambient proxy settings and redirects.
        self.opener = build_opener(ProxyHandler({}), NoRedirect())

    def request(self, method, path, body=None, idempotency_key=None):
        headers = {"Authorization": "Bearer " + self.token, "Accept": "application/json"}
        data = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(body, separators=(",", ":")).encode("utf-8")
        if idempotency_key is not None:
            if (not 1 <= len(idempotency_key) <= 128 or "," in idempotency_key or
                    any(ord(char) < 33 or ord(char) > 126 for char in idempotency_key)):
                raise ValueError("idempotency key must be 1..128 printable ASCII characters without commas")
            headers["Idempotency-Key"] = idempotency_key
        request = Request(self.base_url + path, data=data, headers=headers, method=method)
        try:
            with self.opener.open(request, timeout=self.timeout_seconds) as response:
                raw = response.read(MAX_RESPONSE_BYTES + 1)
                if len(raw) > MAX_RESPONSE_BYTES:
                    raise ToolaneError("response exceeds 2 MiB")
        except HTTPError as exc:
            with exc:
                try:
                    detail = json.loads(exc.read(8192))
                except (ValueError, UnicodeError):
                    detail = {}
            if not isinstance(detail, dict):
                detail = {}
            code = detail.get("code", "http_error")
            request_id = detail.get("request_id", "unknown")
            raise ToolaneError(f"HTTP {exc.code}: {code} (request_id={request_id})") from None
        except URLError as exc:
            raise ToolaneError(f"connection failed: {exc.reason}") from None
        except OSError as exc:
            raise ToolaneError(f"connection failed: {exc}") from None
        try:
            result = json.loads(raw)
        except (ValueError, UnicodeError):
            raise ToolaneError("service returned invalid JSON") from None
        if not isinstance(result, dict):
            raise ToolaneError("service returned an unexpected response")
        return result

    def submit_mock(self, delay_ms, value, idempotency_key=None):
        return self.request("POST", "/api/v1/executions", {
            "agent_id": "python-example", "tool": "mock.delay",
            "arguments": {"delay_ms": delay_ms, "value": value},
        }, idempotency_key)

    def list_tools(self):
        return self.request("GET", "/api/v1/tools")

    def submit_tool(self, name, arguments, idempotency_key=None):
        if not isinstance(name, str) or not name or not isinstance(arguments, dict):
            raise ValueError("tool name and JSON object arguments are required")
        return self.request("POST", "/api/v1/executions", {
            "agent_id": "python-example", "tool": name, "arguments": arguments,
        }, idempotency_key)

    def get_execution(self, execution_id):
        return self.request("GET", execution_path(execution_id))

    def cancel_execution(self, execution_id):
        return self.request("POST", execution_path(execution_id) + "/cancel")


def print_json(value):
    print(json.dumps(value, ensure_ascii=False, indent=2))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="submit mock.delay and wait for its result")
    run.add_argument("--delay-ms", type=int, default=100)
    run.add_argument("--value", default="hello from Python")
    run.add_argument("--idempotency-key")
    run.add_argument("--wait-seconds", type=float, default=20)
    run.add_argument("--poll-seconds", type=float, default=0.2)
    get = commands.add_parser("get", help="query an execution")
    get.add_argument("execution_id")
    cancel = commands.add_parser("cancel", help="request execution cancellation")
    cancel.add_argument("execution_id")
    args = parser.parse_args(argv)
    if args.command == "run" and (not 0 <= args.delay_ms <= 60000 or
                                  not math.isfinite(args.wait_seconds) or args.wait_seconds <= 0 or
                                  not math.isfinite(args.poll_seconds) or args.poll_seconds <= 0):
        parser.error("delay must be 0..60000 ms and wait/poll seconds must be finite and positive")
    try:
        client = ToolaneClient(args.base_url, os.environ.get("TOOLANE_TOKEN"))
        if args.command == "get":
            print_json(client.get_execution(args.execution_id))
            return 0
        if args.command == "cancel":
            print_json(client.cancel_execution(args.execution_id))
            return 0
        accepted = client.submit_mock(args.delay_ms, args.value, args.idempotency_key)
        execution_id = accepted.get("execution_id")
        if not isinstance(execution_id, str) or not EXECUTION_ID.fullmatch(execution_id):
            raise ToolaneError("accepted response has no valid execution ID")
        print(f"accepted execution_id={execution_id}", flush=True)
        deadline = time.monotonic() + args.wait_seconds
        while True:
            snapshot = client.get_execution(execution_id)
            if snapshot.get("status") in TERMINAL:
                print_json(snapshot)
                return 0 if snapshot["status"] == "succeeded" else 1
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ToolaneError(f"wait timed out; execution {execution_id} remains managed by Toolane")
            time.sleep(min(args.poll_seconds, remaining))
    except (ToolaneError, ValueError) as exc:
        print(f"Toolane: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("\nInterrupted locally; an accepted execution remains managed by Toolane.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
