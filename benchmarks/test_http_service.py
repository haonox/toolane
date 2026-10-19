import unittest

from http_service import percentile, workload


class HTTPExperimentTests(unittest.TestCase):
    def test_scenarios_are_bounded_and_repeatable(self):
        for scenario in ("short", "long", "mixed", "burst"):
            tasks = workload(scenario, 17)
            self.assertEqual(tasks, workload(scenario, 17))
            self.assertEqual(len(tasks), 72)
            self.assertEqual(sum(delay == 2 for _, delay in tasks),
                             36 if scenario == "mixed" else 72 if scenario == "short" else 0)
            if scenario == "burst":
                self.assertEqual({delay for _, delay in tasks}, {50})
            self.assertEqual(sum(tenant == "A" for tenant, _ in tasks),
                             48 if scenario == "burst" else 24)

    def test_percentile_uses_nearest_rank(self):
        self.assertIsNone(percentile([], 95))
        self.assertEqual(percentile([5, 1, 2, 4, 3], 50), 3)
        self.assertEqual(percentile(list(range(1, 101)), 95), 95)
        self.assertEqual(percentile(list(range(1, 101)), 99), 99)


if __name__ == "__main__":
    unittest.main()
