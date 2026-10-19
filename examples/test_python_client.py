"""Safety checks for the standalone Python example."""

import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from python_client import ToolaneClient, ToolaneError, local_origin


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


if __name__ == "__main__":
    unittest.main()
