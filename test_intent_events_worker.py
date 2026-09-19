import inspect
import os
import unittest
from unittest import mock

from workers import intent_events_worker as worker


def _fake_init_brands():
    # Stand-in for initialize_brand_configs(), which (re)populates
    # active_brand_indices - run_worker() now clears it first.
    worker.aws_background.active_brand_indices.append(1)


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

    def test_scheduled_mode_uses_a_15_minute_interval(self):
        source = inspect.getsource(worker._run_scheduled)
        self.assertIn('trigger="interval"', source)
        self.assertIn("minutes=15", source)
        self.assertNotIn('trigger="cron"', source)

    def test_scheduled_mode_prevents_overlapping_runs(self):
        source = inspect.getsource(worker._run_scheduled)
        self.assertIn("coalesce=True", source)
        self.assertIn("max_instances=1", source)

    def test_self_run_env_var_toggle_is_present(self):
        source = inspect.getsource(worker)
        self.assertIn("INTENT_EVENTS_WORKER_SELF_RUN", source)

    def test_scheduled_mode_runs_once_immediately_before_starting_scheduler(self):
        # A container reboot/redeploy shouldn't have to wait up to 15
        # minutes for the first run - _run_scheduled must call run_worker()
        # once immediately, before scheduler.start() takes over for the
        # recurring 15-minute cadence.
        call_order = []

        def fake_run_worker():
            call_order.append("run_worker")

        fake_scheduler = mock.Mock()
        fake_scheduler.start.side_effect = lambda: call_order.append("scheduler.start")

        with mock.patch.object(worker, "run_worker", side_effect=fake_run_worker), \
             mock.patch(
                 "apscheduler.schedulers.blocking.BlockingScheduler",
                 return_value=fake_scheduler,
             ):
            worker._run_scheduled()

        self.assertEqual(call_order, ["run_worker", "scheduler.start"])

    def test_scheduled_mode_immediate_run_failure_does_not_prevent_scheduler_start(self):
        fake_scheduler = mock.Mock()

        with mock.patch.object(worker, "run_worker", side_effect=RuntimeError("boom")), \
             mock.patch(
                 "apscheduler.schedulers.blocking.BlockingScheduler",
                 return_value=fake_scheduler,
             ):
            worker._run_scheduled()  # must not raise

        fake_scheduler.start.assert_called_once()

    def test_resolve_brand_index_by_db_database_matches_case_insensitively(self):
        with mock.patch.object(worker.aws_background, "active_brand_indices", [1]), \
             mock.patch.dict(os.environ, {"DB_DATABASE_1": "BBB"}):
            self.assertEqual(worker._resolve_brand_index_by_db_database("bbb"), 1)

    def test_resolve_brand_index_by_db_database_no_match_returns_none(self):
        with mock.patch.object(worker.aws_background, "active_brand_indices", [1]), \
             mock.patch.dict(os.environ, {"DB_DATABASE_1": "BBB"}):
            self.assertIsNone(worker._resolve_brand_index_by_db_database("Some Other DB"))

    def test_run_worker_resets_active_brand_indices_before_each_init(self):
        # initialize_brand_configs() appends without resetting; the list must
        # be emptied first or it grows on every 15-minute run.
        seen = []
        indices = [1, 2, 1, 2]

        with mock.patch.object(worker.aws_background, "active_brand_indices", indices), \
             mock.patch.object(
                 worker.aws_background, "initialize_brand_configs",
                 side_effect=lambda: seen.append(list(indices)),
             ), \
             mock.patch.object(worker.aws_background, "_parse_intent_db_map", return_value={}):
            worker.run_worker()

        self.assertEqual(seen, [[]])

    def test_unmapped_db_database_is_skipped_not_raised(self):
        with mock.patch.object(worker.aws_background, "initialize_brand_configs", side_effect=_fake_init_brands), \
             mock.patch.object(worker.aws_background, "_parse_intent_db_map", return_value={"bbb_shop": "NOT_A_REAL_DB"}), \
             mock.patch.object(worker.aws_background, "active_brand_indices", []), \
             mock.patch.object(worker.aws_background, "sync_intent_events_for_brand") as mock_sync_events, \
             mock.patch.object(worker.aws_background, "sync_click_events_for_brand") as mock_sync_clicks, \
             mock.patch.object(worker.aws_background, "sync_session_history_for_brand") as mock_sync_sessions:
            worker.run_worker()  # must not raise

        mock_sync_events.assert_not_called()
        mock_sync_clicks.assert_not_called()
        mock_sync_sessions.assert_not_called()

    def test_mapped_db_database_is_synced(self):
        with mock.patch.object(worker.aws_background, "initialize_brand_configs", side_effect=_fake_init_brands), \
             mock.patch.object(worker.aws_background, "_parse_intent_db_map", return_value={"bbb_shop": "BBB"}), \
             mock.patch.object(worker.aws_background, "active_brand_indices", [1]), \
             mock.patch.dict(os.environ, {"DB_DATABASE_1": "BBB"}), \
             mock.patch.object(worker.aws_background, "sync_intent_events_for_brand") as mock_sync_events, \
             mock.patch.object(worker.aws_background, "sync_click_events_for_brand") as mock_sync_clicks, \
             mock.patch.object(worker.aws_background, "sync_session_history_for_brand") as mock_sync_sessions:
            worker.run_worker()

        mock_sync_events.assert_called_once_with(
            brand_index=1, mongo_brand_id="bbb_shop", brand_label="BBB"
        )
        mock_sync_clicks.assert_called_once_with(
            brand_index=1, mongo_brand_id="bbb_shop", brand_label="BBB"
        )
        mock_sync_sessions.assert_called_once_with(
            brand_index=1, mongo_brand_id="bbb_shop", brand_label="BBB"
        )

    def test_click_events_failure_does_not_block_behavioral_events_and_vice_versa(self):
        with mock.patch.object(worker.aws_background, "initialize_brand_configs", side_effect=_fake_init_brands), \
             mock.patch.object(worker.aws_background, "_parse_intent_db_map", return_value={"bbb_shop": "BBB"}), \
             mock.patch.object(worker.aws_background, "active_brand_indices", [1]), \
             mock.patch.dict(os.environ, {"DB_DATABASE_1": "BBB"}), \
             mock.patch.object(worker.aws_background, "sync_intent_events_for_brand") as mock_sync_events, \
             mock.patch.object(
                 worker.aws_background, "sync_click_events_for_brand",
                 side_effect=RuntimeError("boom"),
             ) as mock_sync_clicks, \
             mock.patch.object(worker.aws_background, "sync_session_history_for_brand") as mock_sync_sessions:
            worker.run_worker()  # must not raise despite the click-sync failure

        mock_sync_events.assert_called_once()
        mock_sync_clicks.assert_called_once()
        mock_sync_sessions.assert_called_once()

    def test_session_history_failure_does_not_block_the_other_two_syncs(self):
        with mock.patch.object(worker.aws_background, "initialize_brand_configs", side_effect=_fake_init_brands), \
             mock.patch.object(worker.aws_background, "_parse_intent_db_map", return_value={"bbb_shop": "BBB"}), \
             mock.patch.object(worker.aws_background, "active_brand_indices", [1]), \
             mock.patch.dict(os.environ, {"DB_DATABASE_1": "BBB"}), \
             mock.patch.object(worker.aws_background, "sync_intent_events_for_brand") as mock_sync_events, \
             mock.patch.object(worker.aws_background, "sync_click_events_for_brand") as mock_sync_clicks, \
             mock.patch.object(
                 worker.aws_background, "sync_session_history_for_brand",
                 side_effect=RuntimeError("boom"),
             ) as mock_sync_sessions:
            worker.run_worker()  # must not raise despite the session-history failure

        mock_sync_events.assert_called_once()
        mock_sync_clicks.assert_called_once()
        mock_sync_sessions.assert_called_once()


if __name__ == "__main__":
    unittest.main()
