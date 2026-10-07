import inspect
import unittest
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from workers import checkout_login_status_worker as worker


class CheckoutLoginStatusWorkerTests(unittest.TestCase):
    def test_compute_rebuild_dates(self):
        # CASE 8 / CASE 9: D-1 and D-2 are processed, current day excluded.
        tz = ZoneInfo("Asia/Kolkata")
        current_dt = datetime(2026, 9, 10, 0, 5, tzinfo=tz)
        self.assertEqual(
            worker.compute_rebuild_dates(tz, current_dt=current_dt),
            [date(2026, 9, 8), date(2026, 9, 9)],
        )
        self.assertNotIn(date(2026, 9, 10), worker.compute_rebuild_dates(tz, current_dt=current_dt))

    def test_compute_rebuild_dates_uses_given_timezone(self):
        tz = ZoneInfo("Pacific/Auckland")  # UTC+12/+13
        current_dt = datetime(2026, 9, 9, 23, 0, tzinfo=ZoneInfo("UTC"))
        local_today = current_dt.astimezone(tz).date()
        self.assertEqual(
            worker.compute_rebuild_dates(tz, current_dt=current_dt),
            [local_today - timedelta(days=2), local_today - timedelta(days=1)],
        )

    def test_dockerfile_runs_worker_as_a_service(self):
        dockerfile_path = "Dockerfile.checkout-login-status-worker"
        with open(dockerfile_path, "r", encoding="utf-8") as handle:
            content = handle.read()

        self.assertIn("requirements.txt", content)
        self.assertIn('CMD ["python", "workers/checkout_login_status_worker.py"]', content)

    def test_docker_compose_defines_checkout_login_status_worker_service(self):
        with open("docker-compose.yml", "r", encoding="utf-8") as handle:
            content = handle.read()

        self.assertIn("checkout-login-status-worker:", content)
        self.assertIn("Dockerfile.checkout-login-status-worker", content)

    def test_scheduled_mode_uses_every_6_hours_ist_cron(self):
        # Runs 4x/day at 00:00, 06:00, 12:00, 18:00 IST.
        source = inspect.getsource(worker._run_scheduled)
        self.assertIn('hour="0,6,12,18"', source)
        self.assertIn("minute=0", source)
        self.assertIn('timezone="Asia/Kolkata"', source)

    def test_scheduled_mode_prevents_overlapping_runs(self):
        source = inspect.getsource(worker._run_scheduled)
        self.assertIn("coalesce=True", source)
        self.assertIn("max_instances=1", source)

    def test_self_run_env_var_toggle_is_present(self):
        source = inspect.getsource(worker)
        self.assertIn("CHECKOUT_LOGIN_STATUS_WORKER_SELF_RUN", source)


if __name__ == "__main__":
    unittest.main()
