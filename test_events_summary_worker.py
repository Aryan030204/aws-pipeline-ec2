import unittest
from datetime import date, datetime, timezone
import sys
import types

try:
    import pymongo  # noqa: F401
except ModuleNotFoundError:
    fake_pymongo = types.ModuleType("pymongo")
    fake_pymongo.MongoClient = object
    sys.modules["pymongo"] = fake_pymongo

from workers import events_summary_worker as worker


class DummyConnection:
    def __init__(self):
        self.commit_count = 0

    def commit(self):
        self.commit_count += 1


class DummyCursor:
    def __init__(self, fetch_rows=None):
        self.fetch_rows = list(fetch_rows or [])
        self.executed = []
        self.executemany_calls = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def executemany(self, sql, rows):
        self.executemany_calls.append((sql, list(rows)))

    def fetchall(self):
        return list(self.fetch_rows)


class EventsSummaryWorkerTests(unittest.TestCase):
    def test_compute_rebuild_dates(self):
        current_dt = datetime(2026, 8, 18, 0, 0, tzinfo=worker.IST)
        self.assertEqual(
            worker.compute_rebuild_dates(current_dt=current_dt),
            [date(2026, 8, 16), date(2026, 8, 17)],
        )

    def test_sanitize_event_type(self):
        self.assertEqual(worker.sanitize_event_type("Checkout Initiated"), "checkout_initiated_count")
        self.assertEqual(worker.sanitize_event_type("buy-now"), "buy_now_count")
        self.assertEqual(worker.sanitize_event_type("LOGIN!!!"), "login_count")
        self.assertIsNone(worker.sanitize_event_type("___"))

    def test_aggregate_event_documents_uses_ist_dates(self):
        docs = [
            {
                "createdAt": datetime(2026, 8, 16, 20, 0, tzinfo=timezone.utc),
                "event_type": "login",
                "shop_name": "tmc",
                "session_id": "s1",
                "cart_token": "c1",
            },
            {
                "createdAt": datetime(2026, 8, 16, 21, 0, tzinfo=timezone.utc),
                "event_type": "checkout initiated",
                "shop_name": "tmc",
                "session_id": "s2",
                "cart_token": "c2",
            },
            {
                "createdAt": datetime(2026, 8, 17, 18, 40, tzinfo=timezone.utc),
                "event_type": "checkout initiated",
                "shop_name": "shyle",
                "session_id": "s3",
                "cart_token": "c3",
            },
        ]

        aggregated, columns = worker.aggregate_event_documents(
            docs,
            {date(2026, 8, 17)},
            tz=worker.IST,
        )

        self.assertEqual(columns, {"login_count", "checkout_initiated_count"})
        self.assertEqual(
            dict(aggregated[(date(2026, 8, 17), "tmc")]),
            {"login_count": 1, "checkout_initiated_count": 1},
        )
        self.assertNotIn((date(2026, 8, 17), "shyle"), aggregated)

    def test_aggregate_event_documents_drops_repeated_pairs(self):
        docs = [
            {
                "createdAt": datetime(2026, 8, 17, 10, 0, tzinfo=timezone.utc),
                "event_type": "checkout initiated",
                "shop_name": "tmc",
                "session_id": "s1",
                "cart_token": "c1",
            },
            {
                "createdAt": datetime(2026, 8, 17, 11, 0, tzinfo=timezone.utc),
                "event_type": "checkout initiated",
                "shop_name": "tmc",
                "session_id": "s1",
                "cart_token": "c1",
            },
            {
                "createdAt": datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc),
                "event_type": "checkout initiated",
                "shop_name": "tmc",
                "session_id": "s2",
                "cart_token": "c2",
            },
            {
                "createdAt": datetime(2026, 8, 17, 13, 0, tzinfo=timezone.utc),
                "event_type": "login",
                "shop_name": "tmc",
                "session_id": "s3",
            },
        ]

        aggregated, columns = worker.aggregate_event_documents(
            docs,
            {date(2026, 8, 17)},
            tz=worker.IST,
        )

        self.assertEqual(columns, {"checkout_initiated_count"})
        self.assertEqual(
            dict(aggregated[(date(2026, 8, 17), "tmc")]),
            {"checkout_initiated_count": 1},
        )

    def test_ensure_event_columns_adds_missing_columns(self):
        cursor = DummyCursor(fetch_rows=[{"Field": "date"}, {"Field": "shop_name"}])
        connection = DummyConnection()

        worker.ensure_event_columns(
            cursor,
            connection,
            {"login_count", "checkout_initiated_count"},
        )

        executed_sql = " ".join(sql for sql, _params in cursor.executed)
        self.assertIn("ADD COLUMN `checkout_initiated_count` INT NOT NULL DEFAULT 0", executed_sql)
        self.assertIn("ADD COLUMN `login_count` INT NOT NULL DEFAULT 0", executed_sql)
        self.assertEqual(connection.commit_count, 1)

    def test_build_event_summary_rows_sets_missing_counts_to_zero(self):
        aggregated = {
            (date(2026, 8, 16), "tmc"): {"login_count": 3},
            (date(2026, 8, 17), "tmc"): {"checkout_initiated_count": 2},
        }

        rows = worker.build_event_summary_rows(
            aggregated,
            ["checkout_initiated_count", "login_count"],
        )

        self.assertEqual(
            rows,
            [
                (date(2026, 8, 16), "tmc", 0, 3),
                (date(2026, 8, 17), "tmc", 2, 0),
            ],
        )

    def test_summarize_aggregated_counts_groups_by_shop_and_date(self):
        aggregated = {
            (date(2026, 8, 16), "tmc"): {"login_count": 3, "buy_now_count": 0},
            (date(2026, 8, 17), "tmc"): {"checkout_initiated_count": 2},
            (date(2026, 8, 17), "shyle"): {"login_count": 1},
        }

        summary = worker.summarize_aggregated_counts(aggregated)

        self.assertEqual(summary["tmc"]["rows"], 2)
        self.assertEqual(summary["tmc"]["total_events"], 5)
        self.assertEqual(
            summary["tmc"]["dates"]["2026-08-16"],
            {"total_events": 3, "event_counts": {"login_count": 3}},
        )
        self.assertEqual(
            summary["shyle"]["dates"]["2026-08-17"],
            {"total_events": 1, "event_counts": {"login_count": 1}},
        )

    def test_dockerfile_runs_worker_as_a_service(self):
        dockerfile_path = "Dockerfile.events-worker"
        with open(dockerfile_path, "r", encoding="utf-8") as handle:
            content = handle.read()

        self.assertIn("requirements.txt", content)
        self.assertIn('CMD ["python", "workers/events_summary_worker.py"]', content)

    def test_docker_compose_defines_events_worker_service(self):
        with open("docker-compose.yml", "r", encoding="utf-8") as handle:
            content = handle.read()

        self.assertIn("events-summary-worker:", content)
        self.assertIn("Dockerfile.events-worker", content)

    def test_scheduled_mode_uses_same_cron_as_former_github_actions_workflow(self):
        # Former GitHub Actions schedule was cron: '30 18 * * *' (UTC) == 00:00 IST.
        # The container replicates this via APScheduler; assert the job is wired
        # with the same hour/minute/timezone rather than re-parsing source text.
        import inspect

        source = inspect.getsource(worker._run_scheduled)
        self.assertIn('hour=18', source)
        self.assertIn('minute=30', source)
        self.assertIn('timezone="UTC"', source)

    def test_scheduled_mode_runs_master_summary_once_immediately_before_starting_scheduler(self):
        from unittest import mock

        fake_scheduler = mock.Mock()
        with mock.patch(
            "apscheduler.schedulers.blocking.BlockingScheduler", return_value=fake_scheduler
        ), mock.patch.object(worker, "_run_master_events_summary") as master, \
             mock.patch.object(worker, "run_incremental_events_job") as incremental:
            worker._run_scheduled()

        master.assert_called_once_with()
        incremental.assert_called_once_with()
        fake_scheduler.start.assert_called_once()


class _FakeDbCursorCtx:
    """Fakes aws_background.get_db_cursor(brand_index) as a context manager
    yielding (cursor, connection) sentinels - good enough since every call
    in run_incremental_events_job just threads them through unchanged."""

    def __init__(self, cursor="cursor", connection="connection"):
        self.cursor = cursor
        self.connection = connection

    def __call__(self, brand_index):
        return self

    def __enter__(self):
        return self.cursor, self.connection

    def __exit__(self, *exc):
        return False


class RawEventsPhaseTests(unittest.TestCase):
    """The per-brand incremental events job added to run_worker (raw ingest
    into each brand's own mongo_events_summary table, then the CI/buy-now
    rollups for whatever dates that ingest touched) and its isolation from
    the existing master events_summary rebuild."""

    _SQL_FN_NAMES = (
        "sync_checkout_initiated_ci_events_sql",
        "sync_checkout_initiated_variant_events_sql",
        "sync_buy_now_events_sql",
        "sync_buy_now_variant_events_sql",
        "update_overall_summary_event_columns_sql",
    )

    def _fake_aws_background(self, brands, ci_brands=None, affected_dates=None):
        from unittest import mock

        fake = mock.Mock()
        fake.active_brand_indices = []
        fake.get_db_cursor = _FakeDbCursorCtx()

        def init():
            fake.active_brand_indices.extend(brands)

        fake.initialize_brand_configs.side_effect = init
        fake.is_ci_events_brand.side_effect = lambda name, key: (
            ci_brands is None or name in ci_brands
        )
        fake.sync_mongo_events_raw_for_brand.return_value = (
            affected_dates if affected_dates is not None else {"2026-09-24", "2026-09-25"}
        )
        for fn_name in self._SQL_FN_NAMES:
            setattr(fake, fn_name, mock.Mock())
        return fake

    def test_master_failure_does_not_stop_incremental_job_and_is_still_raised(self):
        from unittest import mock

        with mock.patch.object(worker, "_run_master_events_summary", side_effect=RuntimeError("master boom")), \
             mock.patch.object(worker, "run_incremental_events_job") as incremental:
            with self.assertRaises(RuntimeError):
                worker.run_worker()

        incremental.assert_called_once()

    def test_incremental_failure_does_not_fail_or_block_master_summary(self):
        from unittest import mock

        with mock.patch.object(worker, "_run_master_events_summary") as master, \
             mock.patch.object(worker, "run_incremental_events_job", side_effect=RuntimeError("boom")):
            worker.run_worker()  # must not raise

        master.assert_called_once()

    def test_syncs_each_ci_brand_and_skips_others(self):
        from unittest import mock

        fake = self._fake_aws_background([1, 2], ci_brands={"Brand_1"})
        with mock.patch.dict(sys.modules, {"aws_background": fake}):
            worker.run_incremental_events_job()

        self.assertEqual(fake.sync_mongo_events_raw_for_brand.call_count, 1)
        kwargs = fake.sync_mongo_events_raw_for_brand.call_args.kwargs
        self.assertEqual(kwargs["brand_index"], 1)
        self.assertEqual(kwargs["cursor"], "cursor")
        self.assertEqual(kwargs["connection"], "connection")

    def test_affected_dates_drive_the_sql_rollup_window(self):
        from unittest import mock

        fake = self._fake_aws_background([1], affected_dates={"2026-09-24", "2026-09-26", "2026-09-25"})
        with mock.patch.dict(sys.modules, {"aws_background": fake}):
            worker.run_incremental_events_job()

        for fn_name in self._SQL_FN_NAMES:
            fn = getattr(fake, fn_name)
            fn.assert_called_once()
            kwargs = fn.call_args.kwargs
            self.assertEqual(kwargs["start_date"], date(2026, 9, 24))
            self.assertEqual(kwargs["end_date"], date(2026, 9, 26))
            self.assertEqual(kwargs["brand_index"], 1)

    def test_no_affected_dates_skips_the_sql_rollups_entirely(self):
        from unittest import mock

        fake = self._fake_aws_background([1], affected_dates=set())
        with mock.patch.dict(sys.modules, {"aws_background": fake}):
            worker.run_incremental_events_job()

        for fn_name in self._SQL_FN_NAMES:
            getattr(fake, fn_name).assert_not_called()

    def test_one_sql_rollup_function_failing_does_not_stop_the_others(self):
        from unittest import mock

        fake = self._fake_aws_background([1])
        fake.sync_checkout_initiated_ci_events_sql.side_effect = RuntimeError("boom")
        with mock.patch.dict(sys.modules, {"aws_background": fake}):
            worker.run_incremental_events_job()  # must not raise

        for fn_name in self._SQL_FN_NAMES:
            getattr(fake, fn_name).assert_called_once()

    def test_one_brand_failing_does_not_stop_the_next(self):
        from unittest import mock

        fake = self._fake_aws_background([1, 2])
        fake.sync_mongo_events_raw_for_brand.side_effect = [RuntimeError("boom"), {"2026-09-24"}]
        with mock.patch.dict(sys.modules, {"aws_background": fake}):
            worker.run_incremental_events_job()  # must not raise

        self.assertEqual(fake.sync_mongo_events_raw_for_brand.call_count, 2)

    def test_brand_list_is_cleared_before_each_init(self):
        from unittest import mock

        fake = self._fake_aws_background([1])
        fake.active_brand_indices.extend([1, 1, 1])  # stale entries from a previous run
        with mock.patch.dict(sys.modules, {"aws_background": fake}):
            worker.run_incremental_events_job()

        self.assertEqual(fake.active_brand_indices, [1])


if __name__ == "__main__":
    unittest.main()
