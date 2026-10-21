import unittest
from pathlib import Path
import tempfile
import json
import hashlib

from run_real_tools import minute_rows, scenarios, summarize, write_manifest


class RealToolExperimentTests(unittest.TestCase):
    def test_workloads_are_repeatable_and_cover_tenants_and_agents(self):
        for profile in ("smoke", "short", "soak"):
            first = scenarios(profile, 17)
            self.assertEqual(first, scenarios(profile, 17))
            self.assertEqual({item["agent"] for _, _, _, items in first for item in items}, {0, 1})
            for name, mode, interval, items in first:
                self.assertGreater(len(items), 0, name)
                self.assertGreaterEqual(interval, 0)
                self.assertIn(mode, {"direct", "serial", "scheduled", "closed_loop"})
                expected_tenants = {"A"} if mode in {"direct", "serial"} else {"A", "B", "C"}
                self.assertEqual({item["tenant"] for item in items}, expected_tenants)
        short = {name: items for name, _, _, items in scenarios("short", 17)}
        self.assertGreaterEqual(len(short["overload"]), 70)
        self.assertTrue(any(item["expected"] == "cancelled" for item in short["mixed"]))
        self.assertTrue(any(item["expected"] == "timed_out" for item in short["mixed"]))

    def test_summary_and_minute_buckets_use_terminal_timestamps(self):
        rows = [{"http_status": 202, "status": "succeeded", "process_started_at": "2026-01-01T00:00:01+00:00",
                 "finished_at": "2026-01-01T00:00:02+00:00", "tenant": "A", "submit_ms": 2,
                 "schedule_lag_ms": 0, "queue_ms": 10, "run_ms": 100, "end_to_end_ms": 110, "direct_ms": ""},
                {"http_status": 429, "status": "", "process_started_at": "", "finished_at": "",
                 "tenant": "B", "submit_ms": 3, "schedule_lag_ms": 1, "queue_ms": "",
                 "run_ms": "", "end_to_end_ms": "", "direct_ms": ""}]
        summary = summarize(rows, 1)
        self.assertEqual((summary["accepted"], summary["rejected"]), (1, 1))
        self.assertEqual((summary["success_rate"], summary["rejection_ratio"]), (0.5, 0.5))
        self.assertEqual(summary["queue_ms"]["p95"], 10)
        samples = [{"at": "2026-01-01T00:00:00+00:00", "rss_bytes": 1024, "toolane.sqlite-wal_bytes": 512,
                    "cpu_seconds": 0.1}]
        minutes = minute_rows(rows, samples)
        self.assertEqual(minutes[0]["completed"], 1)
        self.assertEqual(minutes[0]["end_to_end_p95_ms"], 110)

    def test_manifest_hashes_saved_raw_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "runs").mkdir()
            (root / "runs" / "executions.csv").write_bytes(b"id,status\na,succeeded\n")
            write_manifest(root)
            manifest = json.loads((root / "manifest.json").read_text())
            self.assertEqual(manifest["runs/executions.csv"]["sha256"],
                             hashlib.sha256(b"id,status\na,succeeded\n").hexdigest())
            self.assertNotIn("manifest.json", manifest)


if __name__ == "__main__":
    unittest.main()
