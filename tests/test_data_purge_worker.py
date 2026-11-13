"""Tests for workers/data_purge_worker.py: wiring, deployment files, safe defaults and brand selection.
(The purge logic itself is covered by tests/test_data_purge.py.) No database, no S3."""
import os
import unittest
from contextlib import contextmanager
from datetime import datetime
from unittest import mock
from zoneinfo import ZoneInfo

# Safe import order (see test_mongo_events.py): aws_background first.
import aws_background  # noqa: F401
from pipeline import data_purge as dp
from workers import data_purge_worker as worker
from workers import registry

IST = ZoneInfo("Asia/Kolkata")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read(name):
    with open(os.path.join(ROOT, name), encoding="utf-8") as handle:
        return handle.read()


@contextmanager
def purge_env(**values):
    """os.environ with every DATA_PURGE_* variable and PURGE_TEST removed (the worker loads .env on import, so a
    developer's local settings must not leak into these tests), then `values` applied."""
    clean = {k: v for k, v in os.environ.items() if not k.startswith("DATA_PURGE_") and k != "PURGE_TEST"}
    clean.update(values)
    with mock.patch.dict(os.environ, clean, clear=True):
        yield


@contextmanager
def fake_brands(indices=(1,), **env):
    base = {f"BRAND_NAME_{i}": f"Brand {i}" for i in indices}
    base.update({f"BRAND_TAG_{i}": f"tag{i}" for i in indices})
    base.update(env)
    with mock.patch.dict(os.environ, base), \
         mock.patch.object(worker.aws_background, "initialize_brand_configs") as init, \
         mock.patch.object(worker.aws_background, "active_brand_indices", list(indices)), \
         mock.patch.object(worker.aws_background, "get_brand_timezone", return_value=IST), \
         mock.patch.object(worker.aws_background, "get_db_cursor") as get_cursor:
        yield init, get_cursor


class DeploymentTests(unittest.TestCase):
    def test_dockerfile_follows_the_worker_pattern(self):
        dockerfile = _read("Dockerfile.data-purge-worker")
        self.assertIn("requirements.txt", dockerfile)
        self.assertIn('CMD ["python", "workers/data_purge_worker.py"]', dockerfile)

    def test_compose_service_exists_with_limits_and_is_not_enabled_by_default(self):
        compose = _read("docker-compose.yml")
        self.assertIn("data-purge-worker:", compose)
        self.assertIn("dockerfile: Dockerfile.data-purge-worker", compose)
        self.assertIn("container_name: data-purge-worker", compose)
        active = [l for l in compose.splitlines() if not l.strip().startswith("#")]
        service = "\n".join(active[next(i for i, l in enumerate(active) if "data-purge-worker:" in l):])
        for needle in ("mem_limit", "memswap_limit", "cpus", "pids_limit", "restart: always", "DB_READ_TIMEOUT_S"):
            self.assertIn(needle, service)
        # deploying must not switch the purge on: no enable/mode variable is set in compose
        for forbidden in ("DATA_PURGE_ENABLED", "DATA_PURGE_ARCHIVE_ONLY", "DATA_PURGE_DELETE_TABLES", "DATA_PURGE_DRY_RUN"):
            self.assertNotIn(forbidden, "\n".join(active))

    def test_boto3_is_a_requirement(self):
        self.assertIn("boto3", _read("requirements.txt"))

    def test_registered_for_manual_runs(self):
        spec = registry.get_spec("data-purge")
        self.assertEqual(spec.module, "workers.data_purge_worker")
        self.assertEqual(spec.self_run_env, "DATA_PURGE_WORKER_SELF_RUN")
        self.assertTrue(spec.accepts_brand and spec.accepts_as_of)
        self.assertIn("DATA_PURGE_WORKER_SELF_RUN", _read("workers/data_purge_worker.py"))
        argv = registry.build_cli_command("data-purge", as_of="2026-10-01T03:23:00+05:30", brand="pts")
        self.assertEqual(argv[-6:], ["workers.run", "data-purge", "--as-of", "2026-10-01T03:23:00+05:30", "--brand-id", "PTS"])


class SafeDefaultsTests(unittest.TestCase):
    def test_disabled_does_nothing_at_all(self):
        with purge_env(), fake_brands() as (init, get_cursor), \
             mock.patch.object(worker.data_purge, "S3ArchiveStore") as store, \
             mock.patch.object(worker.data_purge, "run_purge") as run_purge:
            worker._run_purge(None, None)
        init.assert_not_called()  # no brand configs, no DB
        get_cursor.assert_not_called()
        store.assert_not_called()  # no S3
        run_purge.assert_not_called()

    def test_explicitly_disabled_is_also_a_no_op(self):
        with purge_env(DATA_PURGE_ENABLED="false", DATA_PURGE_ARCHIVE_ONLY="false"), fake_brands(), \
             mock.patch.object(worker.data_purge, "run_purge") as run_purge:
            worker._run_purge(None, None)
        run_purge.assert_not_called()

    def test_invalid_configuration_refuses_to_run(self):
        for env in ({"DATA_PURGE_ENABLED": "yes"},
                    {"DATA_PURGE_ENABLED": "true"},  # no bucket
                    {"DATA_PURGE_ENABLED": "true", "DATA_PURGE_S3_BUCKET": "b", "DATA_PURGE_ARCHIVE_ONLY": "false"}):  # no allowlist
            with self.subTest(env=env), purge_env(**env), fake_brands(), \
                 mock.patch.object(worker.data_purge, "run_purge") as run_purge:
                with self.assertRaises(dp.ConfigError):
                    worker._run_purge(None, None)
                run_purge.assert_not_called()

    def test_purge_test_runs_archive_only_and_never_a_full_mode(self):
        env = dict(DATA_PURGE_ENABLED="true", DATA_PURGE_S3_BUCKET="b", DATA_PURGE_ARCHIVE_ONLY="false",
                   DATA_PURGE_DELETE_TABLES="raw_mongo_events", PURGE_TEST="true")
        with purge_env(**env), fake_brands(), \
             mock.patch.object(worker.data_purge, "S3ArchiveStore") as store_cls, \
             mock.patch.object(worker.data_purge, "run_purge", return_value=[]) as run_purge:
            worker._run_purge(None, None)
        store_cls.assert_called_once()  # it exports, so an S3 client is built
        run = run_purge.call_args.args[1]
        self.assertEqual((run.cfg.mode, run.cfg.purge_test), (dp.Mode.ARCHIVE_ONLY, True))

    def test_purge_test_alone_does_not_enable_a_disabled_worker(self):
        with purge_env(PURGE_TEST="true"), fake_brands() as (init, _), \
             mock.patch.object(worker.data_purge, "run_purge") as run_purge:
            worker._run_purge(None, None)
        init.assert_not_called()
        run_purge.assert_not_called()

    def test_compose_does_not_set_purge_test(self):
        self.assertNotIn("PURGE_TEST", _read("docker-compose.yml"))

    def test_dry_run_needs_no_bucket_and_never_builds_an_s3_client(self):
        with purge_env(DATA_PURGE_DRY_RUN="true"), fake_brands(), \
             mock.patch.object(worker.data_purge, "S3ArchiveStore") as store, \
             mock.patch.object(worker.data_purge, "run_purge", return_value=[]) as run_purge:
            worker._run_purge(datetime(2026, 10, 1, 3, 23, tzinfo=IST), None)
        store.assert_not_called()
        run = run_purge.call_args.args[1]
        self.assertEqual((run.cfg.mode, run.store, run.cutoff), (dp.Mode.DRY_RUN, None, dp.compute_cutoff(datetime(2026, 10, 1, tzinfo=IST))))


class StartupPassTests(unittest.TestCase):
    """PURGE_TEST=true runs one export-only pass at container start; nothing else runs at startup."""

    def _schedule(self, env, run_worker_error=None):
        sched = mock.Mock()
        order = []
        sched.start.side_effect = lambda: order.append("scheduler.start")

        def fake_run_worker(*args, **kwargs):
            order.append("run_worker")
            if run_worker_error:
                raise run_worker_error

        with purge_env(**env),              mock.patch("apscheduler.schedulers.blocking.BlockingScheduler", return_value=sched),              mock.patch.object(worker, "run_worker", side_effect=fake_run_worker) as run_worker:
            worker._run_scheduled()
        return sched, run_worker, order

    def test_no_startup_run_by_default(self):
        sched, run_worker, order = self._schedule({})
        run_worker.assert_not_called()
        self.assertEqual(order, ["scheduler.start"])
        sched.add_job.assert_called_once()

    def test_purge_test_false_or_garbage_does_not_run_at_startup(self):
        for value in ("false", "", "yes", "1", "ture"):
            with self.subTest(value=value):
                _, run_worker, _ = self._schedule({"PURGE_TEST": value})
                run_worker.assert_not_called()

    def test_purge_test_true_runs_once_before_the_scheduler_starts_and_keeps_the_monthly_job(self):
        sched, run_worker, order = self._schedule({"PURGE_TEST": "true"})
        run_worker.assert_called_once_with()
        self.assertEqual(order, ["run_worker", "scheduler.start"])  # immediately, then the schedule
        kwargs = sched.add_job.call_args.kwargs
        self.assertEqual((kwargs["day"], kwargs["hour"], kwargs["minute"]), (1, 3, 23))  # schedule unchanged

    def test_a_failing_startup_pass_does_not_stop_the_container_or_the_schedule(self):
        sched, run_worker, order = self._schedule({"PURGE_TEST": "true"}, run_worker_error=RuntimeError("boom"))
        run_worker.assert_called_once()
        self.assertEqual(order, ["run_worker", "scheduler.start"])  # no exception escaped; it still schedules

    def test_startup_pass_is_still_gated_by_the_normal_config(self):
        # PURGE_TEST without DATA_PURGE_ENABLED: the pass runs but does nothing
        with purge_env(PURGE_TEST="true"), fake_brands() as (init, _),              mock.patch.object(worker.data_purge, "run_purge") as run_purge:
            worker._run_startup_test_pass()
        init.assert_not_called()
        run_purge.assert_not_called()

    def test_startup_pass_with_purge_test_never_deletes(self):
        env = dict(DATA_PURGE_ENABLED="true", DATA_PURGE_S3_BUCKET="b", DATA_PURGE_ARCHIVE_ONLY="false",
                   DATA_PURGE_DELETE_TABLES="raw_mongo_events", PURGE_TEST="true")
        with purge_env(**env), fake_brands(),              mock.patch.object(worker.data_purge, "S3ArchiveStore"),              mock.patch.object(worker.data_purge, "run_purge", return_value=[]) as run_purge:
            worker._run_startup_test_pass()
        run = run_purge.call_args.args[1]
        self.assertEqual((run.cfg.mode, run.cfg.purge_test), (dp.Mode.ARCHIVE_ONLY, True))


class RunWiringTests(unittest.TestCase):
    def test_archive_only_builds_the_store_and_uses_the_run_month(self):
        env = dict(DATA_PURGE_ENABLED="true", DATA_PURGE_S3_BUCKET="my-bucket", DATA_PURGE_S3_PREFIX="archive")
        with purge_env(**env), fake_brands(), \
             mock.patch.object(worker.data_purge, "S3ArchiveStore") as store_cls, \
             mock.patch.object(worker.data_purge, "run_purge", return_value=[]) as run_purge:
            worker._run_purge(datetime(2026, 11, 1, 3, 23, tzinfo=IST), None)
        store_cls.assert_called_once_with("my-bucket", sse=None, kms_key_id=None)
        run = run_purge.call_args.args[1]
        self.assertEqual((run.month, str(run.cutoff), run.cfg.mode, run.store), ("2026-11", "2026-05-01", dp.Mode.ARCHIVE_ONLY, store_cls.return_value))
        self.assertEqual(run.cfg.prefix, "archive")

    def test_brands_are_run_in_order_with_stable_names_and_their_own_cursor(self):
        with purge_env(DATA_PURGE_DRY_RUN="true"), fake_brands((3, 1)) as (_, get_cursor), \
             mock.patch.object(worker.data_purge, "run_purge", return_value=[]) as run_purge:
            worker._run_purge(None, None)
        brands = run_purge.call_args.args[0]
        self.assertEqual([b.name for b in brands], ["tag3", "tag1"])  # BRAND_TAG when present
        self.assertEqual([b.index for b in brands], [3, 1])
        brands[0].cursor_ctx()
        get_cursor.assert_called_once_with(3)

    def test_brand_key_restricts_the_run(self):
        for key, expected in (("tag2", ["tag2"]), ("TAG2", ["tag2"]), ("brand 2", ["tag2"]), ("2", ["tag2"]),
                              ("dash-9", ["tag2"]), ("nobody", [])):
            with self.subTest(key=key), purge_env(DATA_PURGE_DRY_RUN="true"), \
                 fake_brands((1, 2, 3), BRAND_ID_2="dash-9"), \
                 mock.patch.object(worker.data_purge, "run_purge", return_value=[]) as run_purge:
                worker._run_purge(None, key)
                if expected:
                    self.assertEqual([b.name for b in run_purge.call_args.args[0]], expected)
                else:
                    run_purge.assert_not_called()

    def test_any_failed_table_makes_the_run_fail_loudly(self):
        failed = [dp.TableResult(brand="b", table="t", status=dp.Status.FAILED, reason="boom")]
        ok = [dp.TableResult(brand="b", table="t", status=dp.Status.ARCHIVED_ONLY)]
        with purge_env(DATA_PURGE_DRY_RUN="true"), fake_brands():
            with mock.patch.object(worker.data_purge, "run_purge", return_value=failed):
                with self.assertRaises(RuntimeError):
                    worker._run_purge(None, None)
            with mock.patch.object(worker.data_purge, "run_purge", return_value=ok):
                worker._run_purge(None, None)  # no raise

    def test_a_delete_error_after_a_good_archive_is_also_a_failure(self):
        results = [dp.TableResult(brand="b", table="t", status=dp.Status.ARCHIVED_ONLY, error=True)]
        with purge_env(DATA_PURGE_DRY_RUN="true"), fake_brands(), \
             mock.patch.object(worker.data_purge, "run_purge", return_value=results):
            with self.assertRaises(RuntimeError):
                worker._run_purge(None, None)

    def test_summary_is_logged(self):
        results = [dp.TableResult(brand="b", table="shopify_orders", status=dp.Status.NO_DATA, reason="no rows eligible for purge")]
        with purge_env(DATA_PURGE_DRY_RUN="true"), fake_brands(), \
             mock.patch.object(worker.data_purge, "run_purge", return_value=results), \
             self.assertLogs(worker.logger, level="INFO") as logs:
            worker._run_purge(None, None)
        self.assertIn("DATA PURGE SUMMARY", "\n".join(logs.output))

    def test_a_second_instance_does_not_run(self):
        @contextmanager
        def held(*_a, **_k):
            yield False

        with mock.patch.object(worker, "_single_instance_lock", held), \
             mock.patch.object(worker, "_run_purge") as run:
            worker.run_worker()
        run.assert_not_called()

    def test_run_worker_passes_its_arguments_through(self):
        @contextmanager
        def free(*_a, **_k):
            yield True

        when = datetime(2026, 10, 1, tzinfo=IST)
        with mock.patch.object(worker, "_single_instance_lock", free), mock.patch.object(worker, "_run_purge") as run:
            worker.run_worker(current_dt=when, brand_key="pts")
        run.assert_called_once_with(when, "pts")


if __name__ == "__main__":
    unittest.main()
