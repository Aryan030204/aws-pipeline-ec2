"""BACKFILL_MODE=true: the pipeline starts ONE backfill run immediately when the container starts (instead of waiting for
the next :00/:10/... tick) and registers no recurring ticks. Regular mode and SELF_RUN=false are unchanged."""
import inspect
import threading
import unittest
from datetime import datetime
from unittest import mock

from apscheduler.schedulers.background import BackgroundScheduler

import aws_background as ab
from pipeline.state import IST

ACTIVE = [1, 2, 4, 5]


def register(sched, self_run=True, backfill_mode=False, groups=None, active=ACTIVE):
    with mock.patch.object(ab, "active_brand_indices", list(active)):
        return ab.register_pipeline_schedule(sched, self_run, backfill_mode, groups or {})


class BackfillStartupRegistrationTests(unittest.TestCase):
    def test_backfill_mode_schedules_one_immediate_run_and_no_recurring_job(self):
        sched = mock.Mock()
        before = datetime.now(IST)
        mode = register(sched, backfill_mode=True)
        after = datetime.now(IST)

        self.assertEqual(mode, "backfill_once")
        sched.add_job.assert_called_once()
        args, kwargs = sched.add_job.call_args
        self.assertIs(args[0], ab._execute_pipeline_run)
        self.assertIsNone(kwargs["trigger"])  # a one-off (date) job, like /trigger
        self.assertGreaterEqual(kwargs["next_run_time"], before)  # "now", not the next tick
        self.assertLessEqual(kwargs["next_run_time"], after)
        self.assertIsNotNone(kwargs["next_run_time"].tzinfo)
        self.assertEqual(kwargs["id"], "backfill_startup_run")
        self.assertEqual(kwargs["args"], [ACTIVE])  # every active brand; BACKFILL_BRAND_INDICES filters inside the run
        self.assertEqual(kwargs["kwargs"], {"run_id": None})
        self.assertTrue(kwargs["coalesce"])
        self.assertEqual(kwargs["max_instances"], 1)
        # no cron jobs at all
        self.assertNotIn("cron", args)
        self.assertNotIn("cron", [kwargs.get("trigger")])

    def test_backfill_mode_ignores_the_brands_cron_map_and_registers_no_cron_groups(self):
        sched = mock.Mock()
        register(sched, backfill_mode=True, groups={"15,45": [1, 2], "0,30": [4]})
        self.assertEqual(sched.add_job.call_count, 1)  # still only the startup run

    def test_the_passed_brand_list_is_a_snapshot_not_the_live_list(self):
        sched = mock.Mock()
        register(sched, backfill_mode=True)
        passed = sched.add_job.call_args.kwargs["args"][0]
        self.assertEqual(passed, ACTIVE)
        self.assertIsNot(passed, ab.active_brand_indices)

    def test_self_run_false_wins_over_backfill_mode(self):
        for backfill in (True, False):
            sched = mock.Mock()
            self.assertEqual(register(sched, self_run=False, backfill_mode=backfill), "manual")
            sched.add_job.assert_not_called()

    def test_regular_mode_is_unchanged(self):
        sched = mock.Mock()
        mode = register(sched, backfill_mode=False, groups={"15,45": [1, 2]})
        self.assertEqual(mode, "regular")
        calls = sched.add_job.call_args_list
        self.assertEqual(len(calls), 2)

        group_args, group_kwargs = calls[0]
        self.assertEqual(group_args[1], "cron")
        self.assertEqual((group_kwargs["minute"], group_kwargs["second"]), ("15,45", 0))
        self.assertEqual(group_kwargs["id"], "pipeline_job_group_15_45")

        main_args, main_kwargs = calls[1]
        self.assertIs(main_args[0], ab.run_data_pipeline)
        self.assertEqual(main_args[1], "cron")
        self.assertEqual(main_kwargs["minute"], "0,10,20,30,40,50")
        self.assertEqual(main_kwargs["id"], "pipeline_job")
        for kwargs in (group_kwargs, main_kwargs):
            self.assertEqual((kwargs["coalesce"], kwargs["max_instances"], kwargs["misfire_grace_time"]), (True, 1, 120))
            self.assertNotIn("next_run_time", kwargs)  # regular mode does NOT run at startup

    def test_regular_mode_without_a_cron_map_registers_only_the_fallback_job(self):
        sched = mock.Mock()
        register(sched, backfill_mode=False)
        self.assertEqual([c.kwargs["id"] for c in sched.add_job.call_args_list], ["pipeline_job"])

    def test_the_main_block_uses_the_shared_helper_with_the_effective_flags(self):
        source = inspect.getsource(ab)
        main_block = source[source.index('if __name__ == "__main__":'):]
        self.assertIn("register_pipeline_schedule(scheduler, SELF_RUN, BACKFILL_MODE, brand_cron_groups)", main_block)
        # the old inline registration (which ran ticks in backfill mode too) is gone from the main block
        self.assertNotIn("scheduler.add_job(", main_block)


class RealSchedulerStartupRunTests(unittest.TestCase):
    """A real APScheduler: the backfill run actually fires right after start, once, and nothing recurs."""

    def test_the_run_fires_immediately_once_with_the_active_brands(self):
        ran = threading.Event()
        calls = []

        def fake_run(target_indices, run_id=None):
            calls.append((list(target_indices), run_id))
            ran.set()

        sched = BackgroundScheduler(timezone=IST)
        try:
            with mock.patch.object(ab, "_execute_pipeline_run", fake_run):
                self.assertEqual(register(sched, backfill_mode=True), "backfill_once")
                self.assertEqual([j.id for j in sched.get_jobs()], ["backfill_startup_run"])
                sched.start()
                self.assertTrue(ran.wait(timeout=10), "the backfill run did not start by itself")
            self.assertEqual(calls, [(ACTIVE, None)])
            # a one-off job: it is gone after running, so nothing repeats every 10 minutes
            for _ in range(50):
                if not sched.get_jobs():
                    break
                threading.Event().wait(0.1)
            self.assertEqual(sched.get_jobs(), [])
            self.assertEqual(len(calls), 1)
        finally:
            sched.shutdown(wait=False)

    def test_regular_mode_registers_only_future_cron_jobs_so_nothing_runs_at_startup(self):
        ran = threading.Event()
        sched = BackgroundScheduler(timezone=IST)
        try:
            with mock.patch.object(ab, "run_data_pipeline", lambda: ran.set()):
                register(sched, backfill_mode=False)
                sched.start()
                self.assertFalse(ran.wait(timeout=1.0))
            job = sched.get_job("pipeline_job")
            self.assertIsNotNone(job)
            self.assertGreater(job.next_run_time, datetime.now(IST))  # waits for the next :00/:10/... tick
        finally:
            sched.shutdown(wait=False)


if __name__ == "__main__":
    unittest.main()
