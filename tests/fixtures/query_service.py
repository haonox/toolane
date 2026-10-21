#!/usr/bin/env python3
"""Local controllable HTTP target for the service.query Process fixture."""

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import time
from urllib.parse import parse_qs, urlsplit


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        target = urlsplit(self.path)
        if target.path != "/query":
            self.send_error(404)
            return
        values = parse_qs(target.query)
        try:
            delay = int(values["delay_ms"][0])
            status = int(values["status"][0])
            if not 0 <= delay <= 1000 or not 200 <= status <= 599:
                raise ValueError("out of range")
        except (KeyError, ValueError, IndexError):
            self.send_error(400)
            return
        time.sleep(delay / 1000)
        payload = json.dumps({"delay_ms": delay, "status": status}).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        try:
            self.wfile.write(payload)
        except BrokenPipeError:
            # A cancelled or timed-out client can close before the delay ends.
            pass

    def log_message(self, format, *args):
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=18081)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("port must be between 1 and 65535")
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"http://127.0.0.1:{server.server_port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
