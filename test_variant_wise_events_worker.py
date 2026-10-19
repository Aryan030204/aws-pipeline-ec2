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

    def test_scheduled_mode_is_one_daily_cron_run_in_ist_with_no_startup_run(self):
        from unittest import mock

        fake_scheduler = mock.Mock()
        with mock.patch.object(worker, "run_worker") as run_worker, \
             mock.patch(
                 "apscheduler.schedulers.blocking.BlockingScheduler",
                 return_value=fake_scheduler,
             ):
            worker._run_scheduled()

        run_worker.assert_not_called()
        fake_scheduler.add_job.assert_called_once()
        kwargs = fake_scheduler.add_job.call_args.kwargs
        self.assertEqual(kwargs["trigger"], "cron")
        self.assertEqual((kwargs["hour"], kwargs["minute"]), (0, 15))
        self.assertEqual(kwargs["timezone"], "Asia/Kolkata")
        self.assertEqual(kwargs["max_instances"], 1)
        self.assertNotIn("minutes", kwargs)
        fake_scheduler.start.assert_called_once()

    def test_self_run_env_var_toggle_is_present(self):
        source = inspect.getsource(worker)
        self.assertIn("VARIANT_WISE_EVENTS_WORKER_SELF_RUN", source)


class SingleInstanceLockTests(unittest.TestCase):
    def setUp(self):
        import os
        import tempfile

        fd, self.lock_path = tempfile.mkstemp(suffix=".lock")
        os.close(fd)
        self.addCleanup(lambda: os.path.exists(self.lock_path) and os.remove(self.lock_path))

    def test_lock_held_means_rebuild_is_skipped(self):
        from unittest import mock

        with mock.patch.object(worker, "LOCK_PATH", self.lock_path), \
             mock.patch.object(worker, "_run_rebuild") as rebuild:
            with worker._single_instance_lock(self.lock_path) as holder:
                self.assertTrue(holder)
                worker.run_worker()  # second acquirer: must return without scanning

        rebuild.assert_not_called()

    def test_lock_is_released_after_normal_completion(self):
        from unittest import mock

        with mock.patch.object(worker, "LOCK_PATH", self.lock_path), \
             mock.patch.object(worker, "_run_rebuild") as rebuild:
            worker.run_worker()
            rebuild.assert_called_once()
            with worker._single_instance_lock(self.lock_path) as reacquired:
                self.assertTrue(reacquired)

    def test_lock_is_released_after_exception(self):
        from unittest import mock

        with mock.patch.object(worker, "LOCK_PATH", self.lock_path), \
             mock.patch.object(worker, "_run_rebuild", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                worker.run_worker()
            with worker._single_instance_lock(self.lock_path) as reacquired:
                self.assertTrue(reacquired)

    def test_second_acquirer_does_not_wait(self):
        with worker._single_instance_lock(self.lock_path) as first:
            self.assertTrue(first)
            with worker._single_instance_lock(self.lock_path) as second:
                self.assertFalse(second)


if __name__ == "__main__":
    unittest.main()
