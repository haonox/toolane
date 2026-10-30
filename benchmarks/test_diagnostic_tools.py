"""Checks for optional benchmark tools without starting a Toolane server."""

import contextlib
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parent


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class DiagnosticToolsTests(unittest.TestCase):
    def test_noop_returns_minimal_result(self):
        result = subprocess.run(
            [sys.executable, str(ROOT / "tools" / "noop.py"), "exec-1"],
            capture_output=True, text=True, check=True,
        )
        self.assertEqual(json.loads(result.stdout), {"kind": "bench.noop", "execution_id": "exec-1"})

    def test_io_reads_fixture_and_reports_bytes(self):
        prepare = load("benchmark_prepare", ROOT / "prepare.py")
        tool = load("benchmark_io", ROOT / "tools" / "io.py")
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / "fixture.sqlite"
            with patch.object(prepare, "FIXTURE", fixture), patch.object(tool, "FIXTURE", fixture):
                prepare.create_io_fixture()
                output = io.StringIO()
                with patch.object(sys, "argv", ["io.py", "64", "exec-1"]), contextlib.redirect_stdout(output):
                    self.assertEqual(tool.main(), 0)
                result = json.loads(output.getvalue())
                self.assertEqual(result["kind"], "bench.io")
                self.assertEqual(result["execution_id"], "exec-1")
                self.assertEqual(result["rows"], 64)
                self.assertEqual(result["read_bytes"], 64 * 1024)
                self.assertEqual(len(result["sha256"]), 64)

    def test_fault_modes_have_distinct_behaviors(self):
        script = str(ROOT / "tools" / "fault.py")
        success = subprocess.run([sys.executable, script, "ok", "exec-1"], capture_output=True, text=True)
        failure = subprocess.run([sys.executable, script, "exit", "exec-1"], capture_output=True, text=True)
        oversized = subprocess.run([sys.executable, script, "output", "exec-1"], capture_output=True, text=True)
        self.assertEqual(json.loads(success.stdout)["mode"], "ok")
        self.assertEqual(failure.returncode, 7)
        self.assertGreater(len(oversized.stdout.encode()), 4096)
        with self.assertRaises(subprocess.TimeoutExpired):
            subprocess.run([sys.executable, script, "timeout", "exec-1"], timeout=0.1, capture_output=True)


if __name__ == "__main__":
    unittest.main()
