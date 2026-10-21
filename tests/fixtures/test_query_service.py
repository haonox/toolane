"""Smoke test for the standalone local query target used outside Go tests."""

import json
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import ProxyHandler, build_opener

from query_service import Handler


class QueryServiceTests(unittest.TestCase):
    def test_delay_and_failure_are_controllable(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        origin = f"http://127.0.0.1:{server.server_port}"
        opener = build_opener(ProxyHandler({}))
        try:
            with opener.open(origin + "/query?delay_ms=50&status=200") as response:
                self.assertEqual(json.load(response), {"delay_ms": 50, "status": 200})
            with self.assertRaises(HTTPError) as failure:
                opener.open(origin + "/query?delay_ms=0&status=503")
            self.assertEqual(failure.exception.code, 503)
            failure.exception.close()
            with self.assertRaises(HTTPError) as invalid:
                opener.open(origin + "/query?delay_ms=1001&status=200")
            self.assertEqual(invalid.exception.code, 400)
            invalid.exception.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
