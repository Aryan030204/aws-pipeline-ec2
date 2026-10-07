import inspect
import unittest
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from workers import variant_wise_events_worker as worker


class VariantWiseEventsWorkerTests(unittest.TestCase):
    def test_compute_rebuild_dates(self):
        tz = ZoneInfo("Asia/Kolkata")
        current_dt = datetime(2026, 8, 18, 0, 0, tzinfo=tz)
        self.assertEqual(
            worker.compute_rebuild_dates(tz, current_dt=current_dt),
            [date(2026, 8, 16), date(2026, 8, 17)],
        )

    def test_compute_rebuild_dates_uses_given_timezone(self):
        # A brand in a timezone far enough ahead that "now" in UTC is already
        # tomorrow in the brand's local time - the rebuild window should be
        # computed against the brand's own date, not UTC's.
        tz = ZoneInfo("Pacific/Auckland")  # UTC+12/+13
        current_dt = datetime(2026, 8, 17, 23, 0, tzinfo=ZoneInfo("UTC"))
        local_today = current_dt.astimezone(tz).date()
        self.assertEqual(
            worker.compute_rebuild_dates(tz, current_dt=current_dt),
            [local_today - timedelta(days=2), local_today - timedelta(days=1)],
        )

    def test_dockerfile_runs_worker_as_a_service(self):
        dockerfile_path = "Dockerfile.variant-wise-events-worker"
        with open(dockerfile_path, "r", encoding="utf-8") as handle:
            content = handle.read()

        self.assertIn("requirements.txt", content)
        self.assertIn('CMD ["python", "workers/variant_wise_events_worker.py"]', content)

    def test_docker_compose_defines_variant_wise_events_worker_service(self):
        with open("docker-compose.yml", "r", encoding="utf-8") as handle:
            content = handle.read()

        self.assertIn("variant-wise-events-worker:", content)
        self.assertIn("Dockerfile.variant-wise-events-worker", content)

    def test_scheduled_mode_uses_a_30_minute_interval(self):
        source = inspect.getsource(worker._run_scheduled)
        self.assertIn('trigger="interval"', source)
        self.assertIn("minutes=30", source)
        self.assertNotIn('trigger="cron"', source)

    def test_scheduled_mode_runs_once_immediately_before_starting_scheduler(self):
        from unittest import mock

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

    def test_self_run_env_var_toggle_is_present(self):
        source = inspect.getsource(worker)
        self.assertIn("VARIANT_WISE_EVENTS_WORKER_SELF_RUN", source)


if __name__ == "__main__":
    unittest.main()
