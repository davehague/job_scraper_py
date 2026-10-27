import unittest
from datetime import datetime, timedelta, timezone

from check_freshness import evaluate

NOW = datetime(2026, 10, 27, 8, 0, tzinfo=timezone.utc)


class EvaluateTests(unittest.TestCase):
    def test_no_eligible_users_is_not_stale_even_with_old_jobs(self):
        result = evaluate(0, NOW - timedelta(days=60), NOW)
        self.assertFalse(result.stale)

    def test_eligible_users_and_recent_job_is_healthy(self):
        result = evaluate(1, NOW - timedelta(hours=6), NOW)
        self.assertFalse(result.stale)

    def test_eligible_users_and_job_two_days_old_is_still_healthy(self):
        result = evaluate(1, NOW - timedelta(days=2), NOW)
        self.assertFalse(result.stale)

    def test_eligible_users_and_old_job_is_stale(self):
        # The 2026-08-29 .. 2026-10-01 outage: newest row 2026-08-28, users eligible.
        result = evaluate(3, datetime(2026, 8, 28, tzinfo=timezone.utc), datetime(2026, 9, 5, tzinfo=timezone.utc))
        self.assertTrue(result.stale)
        self.assertIn("2026-08-28", result.message)

    def test_eligible_users_and_empty_jobs_table_is_stale(self):
        result = evaluate(1, None, NOW)
        self.assertTrue(result.stale)


if __name__ == '__main__':
    unittest.main()
