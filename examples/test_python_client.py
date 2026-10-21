"""Safety checks for the standalone Python example."""

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from python_client import ToolaneClient, ToolaneError, local_origin
from agent_bridge import invoke_tool_call, model_tool_definitions


class PythonClientSafetyTests(unittest.TestCase):
    def test_only_local_origin_without_embedded_credentials(self):
        self.assertEqual(local_origin("http://127.0.0.1:8080/"), "http://127.0.0.1:8080")
        self.assertEqual(local_origin("http://[::1]:8080"), "http://[::1]:8080")
        for url in (
            "http://example.com:8080", "http://user:secret@localhost:8080",
            "http://localhost:8080/api", "http://localhost:8080?token=secret",
            "http://localhost", "file:///tmp/toolane",
        ):
            with self.subTest(url=url), self.assertRaises(ValueError):
                local_origin(url)

    def test_redirect_does_not_forward_bearer_token(self):
        requests = []

        class RedirectHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                requests.append((self.path, self.headers.get("Authorization")))
                self.send_response(302)
                self.send_header("Location", "http://127.0.0.1:%d/steal" % self.server.server_port)
                self.end_headers()

            def log_message(self, format, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), RedirectHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            client = ToolaneClient("http://127.0.0.1:%d" % server.server_port, "test-token-123456789")
            with self.assertRaisesRegex(ToolaneError, "HTTP 302"):
                client.get_execution("a" * 32)
            self.assertEqual(requests, [("/api/v1/executions/" + "a" * 32, "Bearer test-token-123456789")])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_invalid_idempotency_key_is_rejected_before_request(self):
        client = ToolaneClient("http://127.0.0.1:8080", "test-token-123456789")
        with self.assertRaises(ValueError):
            client.submit_mock(1, "value", "contains,comma")

    def test_agent_bridge_maps_call_and_execution_ids_and_unknown_outcome(self):
        class FakeClient:
            def __init__(self, status):
                self.status = status
                self.submission = None

            def list_tools(self):
                return {"tools": [{"name": "text.stats", "description": "Count words",
                                   "parameters": {"type": "object"}}]}

            def submit_tool(self, name, arguments):
                self.submission = (name, arguments)
                return {"execution_id": "a" * 32}

            def get_execution(self, execution_id):
                self.asserted_id = execution_id
                return {"status": self.status, "result": {"words": 2},
                        "error_code": "service_restarted", "error_message": "result unknown"}

        successful = FakeClient("succeeded")
        self.assertEqual(model_tool_definitions(successful)[0]["type"], "function")
        response = invoke_tool_call(successful, "model-call-1", "text.stats", {"text": "hello world"})
        self.assertEqual(response["call_id"], "model-call-1")
        self.assertEqual(successful.submission, ("text.stats", {"text": "hello world"}))
        self.assertEqual(successful.asserted_id, "a" * 32)
        self.assertEqual(json.loads(response["output"])["result"], {"words": 2})
        unknown = invoke_tool_call(FakeClient("interrupted"), "model-call-2", "text.stats", {"text": "x"})
        self.assertTrue(json.loads(unknown["output"])["result_unknown"])


if __name__ == "__main__":
    unittest.main()
