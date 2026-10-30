import unittest
from contextlib import contextmanager
from unittest import mock

import aws_background  # noqa: F401  (import before pipeline modules; see other tests)
from pipeline import payment_mode as pm
from pipeline import schema_migrations as sm
from pipeline import shopify_orders as so
from pipeline import summary_incremental as si


class ClassifyPaymentModeTests(unittest.TestCase):
    def test_no_gateway_name_is_unmapped(self):
        for value in (None, "", "   "):
            self.assertEqual(pm.classify_payment_mode(value), "unmapped", repr(value))

    def test_ppcod_wins_over_cod(self):
        self.assertEqual(pm.classify_payment_mode("Gokwik PPCOD"), "partially_paid")
        self.assertEqual(
            pm.classify_payment_mode("Gokwik PPCOD, Cash on Delivery (COD)"), "partially_paid"
        )

    def test_cod_names(self):
        for value in ("Cash on Delivery (COD)", "cash_on_delivery", "razorpay, Cash on Delivery (COD)"):
            self.assertEqual(pm.classify_payment_mode(value), "cod", value)

    def test_any_other_gateway_is_prepaid(self):
        for value in ("razorpay", "Razorpay", "shopify_payments", "gokwik"):
            self.assertEqual(pm.classify_payment_mode(value), "prepaid", value)

    def test_matching_is_case_insensitive_like_the_sql_collation(self):
        self.assertEqual(pm.classify_payment_mode("CASH_ON_DELIVERY"), "cod")
        self.assertEqual(pm.classify_payment_mode("gokwik ppcod"), "partially_paid")

    def test_shopify_orders_classifier_uses_the_shared_rule(self):
        self.assertEqual(so._classify_payment_mode(None), "unmapped")
        self.assertEqual(so._classify_payment_mode("razorpay"), "prepaid")


class SqlFragmentTests(unittest.TestCase):
    def test_four_buckets_defined(self):
        self.assertEqual(set(pm.payment_predicates("x")), {"unmapped", "ppcod", "cod", "prepaid"})

    def test_cod_and_prepaid_require_a_non_blank_gateway(self):
        p = pm.payment_predicates("x")
        for bucket in ("cod", "prepaid"):
            self.assertIn("x IS NOT NULL AND TRIM(x) <> ''", p[bucket])
        self.assertIn("x IS NULL OR TRIM(x) = ''", p["unmapped"])

    def test_breakdown_select_has_all_eight_outputs(self):
        sql = pm.breakdown_select_sql()
        for alias in (
            "prepaid_orders", "cod_orders", "ppcod_orders", "unmapped_orders",
            "cod_sales", "prepaid_sales", "partially_prepaid_sales", "unmapped_sales",
        ):
            self.assertIn(f"AS {alias}", sql)

    def test_summary_sql_no_longer_hardcodes_the_rule(self):
        import inspect

        self.assertNotIn("Gokwik PPCOD", inspect.getsource(si))


class _RecordingCursor:
    def __init__(self):
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append(" ".join(str(sql).split()))

    def fetchall(self):
        return []


class _Conn:
    def commit(self):
        pass


class SummaryQueriesIncludeUnmappedTests(unittest.TestCase):
    def _insert_sql(self, fn, *args):
        cursor = _RecordingCursor()
        with mock.patch.object(si, "merge_staging_to_main_by_date"):
            fn(cursor, _Conn(), *args)
        return next(s for s in cursor.statements if s.startswith("INSERT INTO"))

    def test_order_summary_writes_unmapped_columns_in_every_union_branch(self):
        sql = self._insert_sql(si.update_order_summary_incremental, 1, "Brand", {"2026-10-22"})
        for col in ("unmapped_orders", "overall_unmapped_orders", "rto_unmapped_orders_returned", "overall_unmapped_sales"):
            self.assertIn(col, sql.split(")")[0], col)  # in the INSERT column list
        self.assertEqual(sql.count("AS unmapped_sales_created"), 3)  # created + cancel + rto branches
        self.assertEqual(sql.count("AS unmapped_created"), 3)

    def test_hour_wise_sales_writes_unmapped_columns(self):
        sql = self._insert_sql(si.update_hour_wise_sales_incremental, "Brand", {"2026-10-22"})
        self.assertIn("number_of_unmapped_orders, unmapped_sales", sql)
        self.assertIn("AS number_of_unmapped_orders", sql)
        self.assertIn("AS unmapped_sales", sql)

    def test_citywise_tables_write_unmapped_orders(self):
        for fn in (si.update_daily_citywise_summary_incremental, si.update_hourly_citywise_summary_incremental):
            sql = self._insert_sql(fn, "Brand", {"2026-10-22"})
            self.assertIn("unmapped_orders, sessions, atc_sessions", sql)
            self.assertIn("AS unmapped_orders", sql)

    def test_overall_summary_carries_unmapped_columns(self):
        sql = self._insert_sql(
            lambda c, conn, *a: si.update_overall_summary_incremental(1, c, conn, "key", "Brand", {"2026-10-22"})
        )
        self.assertIn("unmapped_orders, unmapped_sales", sql)
        self.assertIn("o.overall_unmapped_orders", sql)
        self.assertIn("o.overall_unmapped_sales", sql)

    def test_payment_dashboard_stage_tables_are_recreated_each_run(self):
        for fn, table in (
            (si.update_dashboard_discount_payment_daily_incremental, "dashboard_discount_payment_daily"),
            (si.update_dashboard_discount_payment_hourly_incremental, "dashboard_discount_payment_hourly"),
        ):
            cursor = _RecordingCursor()
            with mock.patch.object(si, "merge_staging_to_main_by_date"):
                fn(cursor, _Conn(), "Brand", {"2026-10-22"})
            self.assertIn(f"DROP TABLE IF EXISTS {table}_stage", cursor.statements)
            self.assertIn(f"CREATE TABLE {table}_stage LIKE {table}", cursor.statements)
            self.assertFalse(any("TRUNCATE" in s for s in cursor.statements))


class _FakeResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeEngine:
    """engine.connect() -> conn; conn.execute(text) records SQL and answers
    SHOW COLUMNS from a per-table column-type map."""

    def __init__(self, enum_types):
        self.enum_types = enum_types
        self.executed = []

    @contextmanager
    def connect(self):
        engine = self

        class Conn:
            def execute(self, statement):
                sql = str(statement)
                engine.executed.append(sql)
                if sql.startswith("SHOW COLUMNS"):
                    table = sql.split("FROM ")[1].split(" ")[0]
                    t = engine.enum_types.get(table)
                    return _FakeResult(None if t is None else ("payment_mode", t))
                return _FakeResult(None)

            def commit(self):
                pass

        yield Conn()


class SchemaMigrationTests(unittest.TestCase):
    OLD = "enum('prepaid','cod','partially_paid')"
    NEW = "enum('prepaid','cod','partially_paid','unmapped')"

    def _run_enum_migration(self, enum_types):
        engine = _FakeEngine(enum_types)
        with mock.patch.dict(sm.sqlalchemy_engines, {7: engine}, clear=False):
            sm.ensure_payment_mode_unmapped_enum(7)
        return [s for s in engine.executed if s.startswith("ALTER TABLE")]

    def test_enum_is_extended_when_unmapped_is_missing(self):
        alters = self._run_enum_migration(
            {"dashboard_order_discount_fact": self.OLD, "dashboard_discount_payment_daily": self.OLD,
             "dashboard_discount_payment_hourly": self.OLD}
        )
        self.assertEqual(len(alters), 3)
        self.assertTrue(all("'unmapped'" in a and "MODIFY COLUMN payment_mode" in a for a in alters))

    def test_enum_untouched_when_already_extended_or_table_missing(self):
        alters = self._run_enum_migration(
            {"dashboard_order_discount_fact": self.NEW, "dashboard_discount_payment_daily": None}
        )
        self.assertEqual(alters, [])

    def test_unmapped_columns_added_to_all_payment_breakdown_tables(self):
        with mock.patch.object(sm, "_ensure_columns") as ensure:
            sm.ensure_payment_unmapped_columns(7)
        added = {call.args[1]: set(call.args[2]) for call in ensure.call_args_list}
        self.assertEqual(
            added,
            {
                "order_summary": {"unmapped_orders", "overall_unmapped_orders",
                                  "rto_unmapped_orders_returned", "overall_unmapped_sales"},
                "hour_wise_sales": {"number_of_unmapped_orders", "unmapped_sales"},
                "overall_summary": {"unmapped_orders", "unmapped_sales"},
                "daily_citywise_summary": {"unmapped_orders"},
                "hourly_citywise_summary": {"unmapped_orders"},
            },
        )

    def test_preflight_runs_the_unmapped_migrations(self):
        with mock.patch.multiple(
            sm,
            ensure_user_agent_column=mock.DEFAULT, ensure_customer_tag_column=mock.DEFAULT,
            ensure_bundle_property_columns=mock.DEFAULT, ensure_device_summary_columns=mock.DEFAULT,
            ensure_ci_events_column=mock.DEFAULT, ensure_buy_now_events_column=mock.DEFAULT,
            ensure_overall_summary_event_columns=mock.DEFAULT, ensure_hour_wise_ppcod_column=mock.DEFAULT,
            ensure_hour_wise_payment_sales_columns=mock.DEFAULT,
            ensure_overall_summary_payment_sales_columns=mock.DEFAULT, ensure_utm_names_column=mock.DEFAULT,
            ensure_payment_unmapped_columns=mock.DEFAULT, ensure_payment_mode_unmapped_enum=mock.DEFAULT,
        ) as mocks:
            sm.run_brand_preflight_schema_migrations(7)
        mocks["ensure_payment_unmapped_columns"].assert_called_once_with(7)
        mocks["ensure_payment_mode_unmapped_enum"].assert_called_once_with(7)


if __name__ == "__main__":
    unittest.main()
