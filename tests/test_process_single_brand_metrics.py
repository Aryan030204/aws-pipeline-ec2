import unittest

# Safe import order: aws_background first (see test_brand_cron_scheduling.py) -
# pipeline/orchestration.py does `import aws_background` at module level, and
# aws_background.py does `from pipeline.orchestration import (...)`.
import aws_background
import pipeline.orchestration as orchestration

EXPECTED_METRIC_KEYS = {
    "brand_name",
    "brand_index",
    "new_orders_count",
    "updated_orders_count",
    "affected_dates_count",
    "sessions_synced",
    "atc_sessions_synced",
    "success",
    "error_message",
    "duration_seconds",
}


class EmptyBrandMetricsTests(unittest.TestCase):
    def test_shape_matches_the_full_metrics_dict(self):
        metrics = orchestration._empty_brand_metrics(1, "PTS")
        self.assertEqual(set(metrics.keys()), EXPECTED_METRIC_KEYS)

    def test_defaults(self):
        metrics = orchestration._empty_brand_metrics(1, "PTS")
        self.assertEqual(metrics["brand_name"], "PTS")
        self.assertEqual(metrics["brand_index"], 1)
        self.assertEqual(metrics["new_orders_count"], 0)
        self.assertTrue(metrics["success"])
        self.assertEqual(metrics["error_message"], "")

    def test_success_and_error_message_are_settable(self):
        metrics = orchestration._empty_brand_metrics(2, "BBB", success=False, error_message="boom")
        self.assertFalse(metrics["success"])
        self.assertEqual(metrics["error_message"], "boom")


class ProcessSingleBrandEarlyReturnTests(unittest.TestCase):
    def test_missing_db_config_returns_a_failed_metrics_dict_not_none(self):
        # brand_index 99999 is guaranteed not to be in brand_db_configs.
        result = orchestration.process_single_brand(99999)

        self.assertIsInstance(result, dict)
        self.assertEqual(set(result.keys()), EXPECTED_METRIC_KEYS)
        self.assertFalse(result["success"])
        self.assertIn("no database config", result["error_message"])


if __name__ == "__main__":
    unittest.main()
