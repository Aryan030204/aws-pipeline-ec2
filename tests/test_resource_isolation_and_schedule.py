"""Resource isolation (docker-compose) and the staggered midnight schedule of the daily workers."""
import inspect
import os
import unittest
from unittest import mock

try:
    import yaml
except ModuleNotFoundError:  # PyYAML is only needed for the compose checks
    yaml = None

from workers import checkout_login_status_worker, events_summary_worker, pnl_worker, variant_wise_events_worker

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@unittest.skipIf(yaml is None, "PyYAML not installed")
class ComposeIsolationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(os.path.join(ROOT, "docker-compose.yml"), encoding="utf-8") as handle:
            cls.services = yaml.safe_load(handle)["services"]

    def test_every_pipeline_container_has_memory_cpu_pid_and_log_limits(self):
        self.assertEqual(
            set(self.services),
            {"shopify-pipeline", "events-summary-worker", "variant-wise-events-worker",
             "checkout-login-status-worker", "pnl-worker"},
        )
        for name, svc in self.services.items():
            with self.subTest(service=name):
                self.assertTrue(svc.get("mem_limit"))
                self.assertTrue(svc.get("cpus"))
                self.assertGreater(svc.get("pids_limit", 0), 0)
                options = svc["logging"]["options"]
                self.assertEqual(svc["logging"]["driver"], "json-file")
                self.assertEqual((options["max-size"], options["max-file"]), ("50m", "3"))

    def test_memory_and_swap_limits_are_the_same_value_so_there_is_no_swap_spill(self):
        for name, svc in self.services.items():
            with self.subTest(service=name):
                self.assertEqual(svc["memswap_limit"], svc["mem_limit"])

    def test_restart_policy_is_kept(self):
        for svc in self.services.values():
            self.assertEqual(svc["restart"], "always")

    def test_events_worker_gets_a_startup_delay(self):
        self.assertIn("EVENTS_WORKER_STARTUP_DELAY_S", self.services["events-summary-worker"]["environment"])


class MidnightScheduleTests(unittest.TestCase):
    """IST minute of each daily job. None may sit on the ETL's 10-minute ticks, and they must differ."""

    def test_checkout_login_status_runs_at_minute_7(self):
        self.assertIn("minute=7", inspect.getsource(checkout_login_status_worker._run_scheduled))

    def test_pnl_worker_runs_at_0012_ist(self):
        fake = mock.Mock()
        with mock.patch("apscheduler.schedulers.blocking.BlockingScheduler", return_value=fake):
            pnl_worker._run_scheduled()
        kwargs = fake.add_job.call_args.kwargs
        self.assertEqual((kwargs["hour"], kwargs["minute"], kwargs["timezone"]), (18, 42, "UTC"))  # 00:12 IST

    def test_variant_wise_runs_at_0017_ist(self):
        fake = mock.Mock()
        with mock.patch("apscheduler.schedulers.blocking.BlockingScheduler", return_value=fake):
            variant_wise_events_worker._run_scheduled()
        kwargs = fake.add_job.call_args.kwargs
        self.assertEqual((kwargs["hour"], kwargs["minute"]), (0, 17))

    def test_no_daily_job_starts_on_an_etl_tick_and_none_collide(self):
        ist_minutes = {
            "events-summary": (events_summary_worker.MASTER_CRON_UTC_MINUTE - 30) % 60,  # 18:33 UTC -> 00:03 IST
            "checkout-login": 7,
            "pnl": (42 - 30) % 60,  # 18:42 UTC -> 00:12 IST
            "variant-wise": 17,
        }
        self.assertEqual(ist_minutes["events-summary"], 3)
        self.assertEqual(ist_minutes["pnl"], 12)
        self.assertEqual(len(set(ist_minutes.values())), len(ist_minutes))
        for name, minute in ist_minutes.items():
            self.assertNotEqual(minute % 10, 0, f"{name} would start on an ETL tick")


if __name__ == "__main__":
    unittest.main()
