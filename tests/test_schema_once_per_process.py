"""Per-tick DB overhead removed from the pipeline (EBS burst balance):
  * `SET SESSION autocommit=1` no longer leaks into pooled connections,
  * preflight / ensure_summary_tables / returns_fact DDL checks run once per brand per process,
  * the three blind overall_summary ALTERs (guaranteed to fail on an up-to-date table) are guarded column checks."""
import unittest
from unittest import mock

import aws_background  # noqa: F401  (must load first: the pipeline modules import it circularly)
from pipeline import db, schema_migrations as sm, shopify_orders
from pipeline.session_utils import autocommit_session


class RecordingCursor:
    """Records statements; SHOW COLUMNS FROM overall_summary answers from `overall_columns`."""

    def __init__(self, overall_columns=("date", "adjusted_total_sessions", "ci_events", "buy_now_events"), fail_on=None):
        self.statements = []
        self.overall_columns = overall_columns
        self.fail_on = fail_on
        self._result = []

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        self.statements.append(s)
        if self.fail_on and self.fail_on in s:
            raise RuntimeError("boom")
        if s == "SHOW COLUMNS FROM overall_summary":
            self._result = [{"Field": c} for c in self.overall_columns]
        else:
            self._result = []

    def fetchall(self):
        out, self._result = self._result, []
        return out

    def fetchone(self):
        return None

    def alters(self):
        return [s for s in self.statements if s.startswith("ALTER TABLE overall_summary")]


class FakeConn:
    def commit(self):
        pass


class AutocommitSessionTests(unittest.TestCase):
    def test_sets_then_always_resets_to_zero(self):
        cur = RecordingCursor()
        with autocommit_session(cur):
            self.assertEqual(cur.statements, ["SET SESSION autocommit=1"])
        self.assertEqual(cur.statements, ["SET SESSION autocommit=1", "SET SESSION autocommit=0"])

    def test_resets_when_the_block_raises(self):
        cur = RecordingCursor()
        with self.assertRaises(ValueError):
            with autocommit_session(cur):
                raise ValueError("x")
        self.assertEqual(cur.statements[-1], "SET SESSION autocommit=0")

    def test_a_broken_connection_does_not_mask_the_original_error(self):
        cur = RecordingCursor(fail_on="autocommit=0")
        with self.assertRaises(ValueError):
            with autocommit_session(cur):
                raise ValueError("original")

    def test_db_module_reexports_the_same_helper(self):
        self.assertIs(db.autocommit_session, autocommit_session)

    def test_the_incremental_summary_run_uses_the_helper_not_a_bare_set_session(self):
        import inspect
        from pipeline import summary_incremental as si

        src = inspect.getsource(si.execute_summary_queries_incremental)
        self.assertIn("autocommit_session(cursor)", src)
        self.assertNotIn('execute("SET SESSION autocommit=1")', src)


class EnsureSummaryTablesTests(unittest.TestCase):
    def setUp(self):
        sm.reset_schema_memo()
        self.addCleanup(sm.reset_schema_memo)

    def test_runs_once_per_brand_when_a_brand_index_is_given(self):
        cur = RecordingCursor()
        sm.ensure_summary_tables(cur, FakeConn(), brand_index=3)
        first = len(cur.statements)
        self.assertGreater(first, 20)  # the CREATE TABLE IF NOT EXISTS statements
        sm.ensure_summary_tables(cur, FakeConn(), brand_index=3)
        sm.ensure_summary_tables(cur, FakeConn(), brand_index=3)
        self.assertEqual(len(cur.statements), first)

    def test_other_brands_are_not_skipped(self):
        cur = RecordingCursor()
        sm.ensure_summary_tables(cur, FakeConn(), brand_index=3)
        first = len(cur.statements)
        sm.ensure_summary_tables(cur, FakeConn(), brand_index=4)
        self.assertEqual(len(cur.statements), first * 2)

    def test_without_a_brand_index_it_always_runs_as_before(self):
        cur = RecordingCursor()
        sm.ensure_summary_tables(cur, FakeConn())
        first = len(cur.statements)
        sm.ensure_summary_tables(cur, FakeConn())
        self.assertEqual(len(cur.statements), first * 2)

    def test_a_failed_attempt_is_retried_next_time(self):
        failing = RecordingCursor(fail_on="CREATE TABLE IF NOT EXISTS daily_ci_events_summary")
        with self.assertRaises(RuntimeError):
            sm.ensure_summary_tables(failing, FakeConn(), brand_index=3)
        healthy = RecordingCursor()
        sm.ensure_summary_tables(healthy, FakeConn(), brand_index=3)
        self.assertGreater(len(healthy.statements), 20)  # ran again, was not remembered
        ran = len(healthy.statements)
        sm.ensure_summary_tables(healthy, FakeConn(), brand_index=3)
        self.assertEqual(len(healthy.statements), ran)  # now remembered

    def test_no_blind_alters_on_an_up_to_date_overall_summary(self):
        cur = RecordingCursor()
        sm.ensure_summary_tables(cur, FakeConn())
        self.assertEqual(cur.alters(), [])
        self.assertIn("SHOW COLUMNS FROM overall_summary", cur.statements)

    def test_only_missing_overall_summary_columns_are_added(self):
        cur = RecordingCursor(overall_columns=("date", "ci_events"))
        sm.ensure_summary_tables(cur, FakeConn())
        self.assertEqual(
            cur.alters(),
            [
                "ALTER TABLE overall_summary ADD COLUMN adjusted_total_sessions INT DEFAULT 0",
                "ALTER TABLE overall_summary ADD COLUMN buy_now_events INT DEFAULT 0",
            ],
        )

    def test_a_real_alter_failure_is_no_longer_swallowed(self):
        cur = RecordingCursor(overall_columns=("date",), fail_on="ALTER TABLE overall_summary")
        with self.assertRaises(RuntimeError):
            sm.ensure_summary_tables(cur, FakeConn(), brand_index=3)
        self.assertNotIn(3, sm._summary_tables_done)


class PreflightOncePerProcessTests(unittest.TestCase):
    def setUp(self):
        sm.reset_schema_memo()
        self.addCleanup(sm.reset_schema_memo)

    def test_runs_once_per_brand_then_is_a_no_op(self):
        with mock.patch.object(sm, "_run_brand_preflight_schema_migrations") as run:
            sm.run_brand_preflight_schema_migrations(5)
            sm.run_brand_preflight_schema_migrations(5)
            sm.run_brand_preflight_schema_migrations(6)
        self.assertEqual([c.args for c in run.call_args_list], [(5,), (6,)])

    def test_force_always_runs(self):
        with mock.patch.object(sm, "_run_brand_preflight_schema_migrations") as run:
            sm.run_brand_preflight_schema_migrations(5)
            sm.run_brand_preflight_schema_migrations(5, force=True)
        self.assertEqual(run.call_count, 2)

    def test_an_exception_is_not_remembered(self):
        with mock.patch.object(sm, "_run_brand_preflight_schema_migrations", side_effect=[RuntimeError("x"), None]) as run:
            with self.assertRaises(RuntimeError):
                sm.run_brand_preflight_schema_migrations(5)
            sm.run_brand_preflight_schema_migrations(5)
            sm.run_brand_preflight_schema_migrations(5)
        self.assertEqual(run.call_count, 2)

    def test_a_swallowed_column_failure_is_not_remembered(self):
        calls = []

        def impl(brand_index):
            calls.append(brand_index)
            if len(calls) == 1:
                sm._note_ensure_failure(brand_index)

        with mock.patch.object(sm, "_run_brand_preflight_schema_migrations", side_effect=impl):
            sm.run_brand_preflight_schema_migrations(5)  # fails softly -> retried
            sm.run_brand_preflight_schema_migrations(5)  # clean -> remembered
            sm.run_brand_preflight_schema_migrations(5)
        self.assertEqual(calls, [5, 5])

    def test_ensure_columns_reports_failures_to_the_memo(self):
        class Boom:
            def connect(self):
                raise RuntimeError("db down")

        with mock.patch.dict(sm.sqlalchemy_engines, {9: Boom()}):
            sm._ensure_columns(9, "t", {"c": "INT"})
        self.assertIn(9, sm._ensure_failures)

    def test_the_orchestrator_still_calls_the_public_preflight_function(self):
        import inspect
        from pipeline import orchestration

        self.assertIn("run_brand_preflight_schema_migrations(idx)", inspect.getsource(orchestration._execute_pipeline_run))


class ReturnsFactOncePerProcessTests(unittest.TestCase):
    def setUp(self):
        shopify_orders._RETURNS_FACT_READY.clear()
        self.addCleanup(shopify_orders._RETURNS_FACT_READY.clear)

    def test_checked_once_per_brand(self):
        cur = RecordingCursor()
        shopify_orders._ensure_returns_fact(cur, FakeConn(), brand_index=2)
        first = len(cur.statements)
        self.assertGreater(first, 0)
        shopify_orders._ensure_returns_fact(cur, FakeConn(), brand_index=2)
        self.assertEqual(len(cur.statements), first)
        shopify_orders._ensure_returns_fact(cur, FakeConn(), brand_index=3)
        self.assertEqual(len(cur.statements), first * 2)

    def test_without_a_brand_index_it_always_runs(self):
        cur = RecordingCursor()
        shopify_orders._ensure_returns_fact(cur, FakeConn())
        first = len(cur.statements)
        shopify_orders._ensure_returns_fact(cur, FakeConn())
        self.assertEqual(len(cur.statements), first * 2)

    def test_a_failed_migration_check_is_retried(self):
        failing = RecordingCursor(fail_on="SHOW COLUMNS FROM returns_fact LIKE 'event_type'")
        shopify_orders._ensure_returns_fact(failing, FakeConn(), brand_index=2)  # logs a warning, does not raise
        self.assertNotIn(2, shopify_orders._RETURNS_FACT_READY)
        healthy = RecordingCursor()
        shopify_orders._ensure_returns_fact(healthy, FakeConn(), brand_index=2)
        self.assertGreater(len(healthy.statements), 0)
        self.assertIn(2, shopify_orders._RETURNS_FACT_READY)


if __name__ == "__main__":
    unittest.main()
