import inspect
import os
import unittest
from unittest import mock

from workers import intent_events_worker as worker


class IntentEventsWorkerTests(unittest.TestCase):
    def test_dockerfile_runs_worker_as_a_service(self):
        dockerfile_path = "Dockerfile.intent-events-worker"
        with open(dockerfile_path, "r", encoding="utf-8") as handle:
            content = handle.read()

        self.assertIn("requirements.txt", content)
        self.assertIn('CMD ["python", "workers/intent_events_worker.py"]', content)

    def test_docker_compose_defines_intent_events_worker_service(self):
        with open("docker-compose.yml", "r", encoding="utf-8") as handle:
            content = handle.read()

        self.assertIn("intent-events-worker:", content)
        self.assertIn("Dockerfile.intent-events-worker", content)

    def test_scheduled_mode_uses_a_35_minute_interval_not_cron(self):
        # 35 doesn't evenly divide 60, so this must be an IntervalTrigger,
        # not a cron trigger with minute=*/35 (which would produce an
        # uneven 25/35-minute cycle instead of a true "every 35 minutes").
        source = inspect.getsource(worker._run_scheduled)
        self.assertIn('trigger="interval"', source)
        self.assertIn("minutes=35", source)
        self.assertNotIn('trigger="cron"', source)

    def test_scheduled_mode_prevents_overlapping_runs(self):
        source = inspect.getsource(worker._run_scheduled)
        self.assertIn("coalesce=True", source)
        self.assertIn("max_instances=1", source)

    def test_self_run_env_var_toggle_is_present(self):
        source = inspect.getsource(worker)
        self.assertIn("INTENT_EVENTS_WORKER_SELF_RUN", source)

    def test_resolve_brand_index_by_db_database_matches_case_insensitively(self):
        with mock.patch.object(worker.aws_background, "active_brand_indices", [1]), \
             mock.patch.dict(os.environ, {"DB_DATABASE_1": "BBB"}):
            self.assertEqual(worker._resolve_brand_index_by_db_database("bbb"), 1)

    def test_resolve_brand_index_by_db_database_no_match_returns_none(self):
        with mock.patch.object(worker.aws_background, "active_brand_indices", [1]), \
             mock.patch.dict(os.environ, {"DB_DATABASE_1": "BBB"}):
            self.assertIsNone(worker._resolve_brand_index_by_db_database("Some Other DB"))

    def test_unmapped_db_database_is_skipped_not_raised(self):
        with mock.patch.object(worker.aws_background, "initialize_brand_configs"), \
             mock.patch.object(worker.aws_background, "_parse_intent_db_map", return_value={"bbb_shop": "NOT_A_REAL_DB"}), \
             mock.patch.object(worker.aws_background, "active_brand_indices", []), \
             mock.patch.object(worker.aws_background, "sync_intent_events_for_brand") as mock_sync:
            worker.run_worker()  # must not raise

        mock_sync.assert_not_called()

    def test_mapped_db_database_is_synced(self):
        with mock.patch.object(worker.aws_background, "initialize_brand_configs"), \
             mock.patch.object(worker.aws_background, "_parse_intent_db_map", return_value={"bbb_shop": "BBB"}), \
             mock.patch.object(worker.aws_background, "active_brand_indices", [1]), \
             mock.patch.dict(os.environ, {"DB_DATABASE_1": "BBB"}), \
             mock.patch.object(worker.aws_background, "sync_intent_events_for_brand") as mock_sync:
            worker.run_worker()

        mock_sync.assert_called_once_with(
            brand_index=1, mongo_brand_id="bbb_shop", brand_label="BBB"
        )


if __name__ == "__main__":
    unittest.main()
