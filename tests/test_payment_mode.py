import os
import unittest
from contextlib import contextmanager
from unittest import mock

import aws_background  # noqa: F401  (import before pipeline modules; see other tests)
from pipeline import payment_mode as pm
from pipeline import schema_migrations as sm
from pipeline import shopify_orders as so
from pipeline import summary_incremental as si


_env_patch = None


def setUpModule():
    """The default brand list must not depend on a developer's .env (aws_background loads it on import)."""
    global _env_patch
    _env_patch = mock.patch.dict(os.environ)
    _env_patch.start()
    os.environ.pop("UNMAPPED_PAYMENT_BRANDS", None)


def tearDownModule():
    _env_patch.stop()


class ClassifyPaymentModeTests(unittest.TestCase):
    """The UNMAPPED rule (unmapped_bucket=True): only the brands listed in UNMAPPED_PAYMENT_BRANDS."""

    def test_no_gateway_name_is_unmapped(self):
        for value in (None, "", "   "):
            self.assertEqual(pm.classify_payment_mode(value, True), "unmapped", repr(value))

    def test_ppcod_wins_over_cod(self):
        self.assertEqual(pm.classify_payment_mode("Gokwik PPCOD", True), "partially_paid")
        self.assertEqual(
            pm.classify_payment_mode("Gokwik PPCOD, Cash on Delivery (COD)", True), "partially_paid"
        )

    def test_cod_names(self):
        for value in ("Cash on Delivery (COD)", "cash_on_delivery", "razorpay, Cash on Delivery (COD)"):
            self.assertEqual(pm.classify_payment_mode(value, True), "cod", value)

    def test_any_other_gateway_is_prepaid(self):
        for value in ("razorpay", "Razorpay", "shopify_payments", "gokwik"):
            self.assertEqual(pm.classify_payment_mode(value, True), "prepaid", value)

    def test_matching_is_case_insensitive_like_the_sql_collation(self):
        self.assertEqual(pm.classify_payment_mode("CASH_ON_DELIVERY", True), "cod")
        self.assertEqual(pm.classify_payment_mode("gokwik ppcod", True), "partially_paid")

    def test_shopify_orders_classifier_uses_the_shared_rule(self):
        self.assertEqual(so._classify_payment_mode(None, True), "unmapped")
        self.assertEqual(so._classify_payment_mode("razorpay", True), "prepaid")


class LegacyClassifyPaymentModeTests(unittest.TestCase):
    """The LEGACY rule - the default, and what every brand except the unmapped-rule brands uses:
    a blank gateway name means COD (the behaviour before the unmapped bucket existed)."""

    def test_default_is_the_legacy_rule(self):
        self.assertEqual(pm.classify_payment_mode(None), "cod")
        self.assertEqual(so._classify_payment_mode(None), "cod")

    def test_blank_gateway_is_cod_and_never_unmapped(self):
        for value in (None, ""):
            self.assertEqual(pm.classify_payment_mode(value, False), "cod", repr(value))

    def test_ppcod_wins_over_cod(self):
        self.assertEqual(pm.classify_payment_mode("Gokwik PPCOD", False), "partially_paid")
        self.assertEqual(pm.classify_payment_mode("Gokwik PPCOD, Cash on Delivery (COD)", False), "partially_paid")

    def test_cod_names_and_prepaid(self):
        for value in ("Cash on Delivery (COD)", "cash_on_delivery", "razorpay, Cash on Delivery (COD)"):
            self.assertEqual(pm.classify_payment_mode(value, False), "cod", value)
        for value in ("razorpay", "shopify_payments", "gokwik"):
            self.assertEqual(pm.classify_payment_mode(value, False), "prepaid", value)

    def test_it_never_returns_unmapped(self):
        for value in (None, "", " ", "razorpay", "Cash on Delivery (COD)", "Gokwik PPCOD"):
            self.assertNotEqual(pm.classify_payment_mode(value, False), "unmapped", repr(value))


class BrandSwitchTests(unittest.TestCase):
    def test_only_editeveryday_uses_the_unmapped_rule_by_default(self):
        env = {}
        for name in ("EDITEVERYDAY", "editeveryday", "Edit Everyday", "EDIT-EVERYDAY", " editeveryday "):
            self.assertTrue(pm.brand_uses_unmapped_bucket(name, env=env), name)
        for name in ("BBB", "BlaBliBluLife", "Personal Touch Skincare", "PTS", "TMC", "AJMAL", "VAMA", "3TENX",
                     "SHYLENEW", "AJMAL_KSA", "URBAN HINDU", "SORRYSUGAR", "NEULIFE", "", None):
            self.assertFalse(pm.brand_uses_unmapped_bucket(name, env=env), repr(name))

    def test_matches_on_the_brand_tag_too(self):
        self.assertTrue(pm.brand_uses_unmapped_bucket("Some Full Name", "editeveryday", env={}))
        self.assertFalse(pm.brand_uses_unmapped_bucket("Some Full Name", "bbb", env={}))

    def test_resolves_the_tag_from_the_brand_environment_when_only_the_name_is_known(self):
        env = {"BRAND_NAME_15": "Edit Every Day Store", "BRAND_TAG_15": "editeveryday",
               "BRAND_NAME_2": "BlaBliBluLife", "BRAND_TAG_2": "bbb"}
        self.assertTrue(pm.brand_uses_unmapped_bucket("Edit Every Day Store", env=env))
        self.assertFalse(pm.brand_uses_unmapped_bucket("BlaBliBluLife", env=env))

    def test_the_brand_list_is_configurable_and_can_be_emptied(self):
        env = {"UNMAPPED_PAYMENT_BRANDS": "editeveryday, sorrysugar"}
        self.assertTrue(pm.brand_uses_unmapped_bucket("SORRYSUGAR", env=env))
        self.assertTrue(pm.brand_uses_unmapped_bucket("EDITEVERYDAY", env=env))
        self.assertFalse(pm.brand_uses_unmapped_bucket("BBB", env=env))
        none = {"UNMAPPED_PAYMENT_BRANDS": ""}
        self.assertFalse(pm.brand_uses_unmapped_bucket("EDITEVERYDAY", env=none))

    def test_reads_the_real_environment_by_default(self):
        with mock.patch.dict("os.environ", {"UNMAPPED_PAYMENT_BRANDS": "bbb"}):
            self.assertTrue(pm.brand_uses_unmapped_bucket("BBB"))
            self.assertFalse(pm.brand_uses_unmapped_bucket("EDITEVERYDAY"))


class SqlFragmentTests(unittest.TestCase):
    def test_four_buckets_defined_for_both_rules(self):
        for flag in (False, True):
            self.assertEqual(set(pm.payment_predicates("x", flag)), {"unmapped", "ppcod", "cod", "prepaid"})

    def test_default_predicates_are_the_legacy_ones(self):
        self.assertEqual(pm.payment_predicates("x"), pm.payment_predicates("x", False))

    def test_unmapped_rule_cod_and_prepaid_require_a_non_blank_gateway(self):
        p = pm.payment_predicates("x", True)
        for bucket in ("cod", "prepaid"):
            self.assertIn("x IS NOT NULL AND TRIM(x) <> ''", p[bucket])
        self.assertIn("x IS NULL OR TRIM(x) = ''", p["unmapped"])

    def test_legacy_rule_counts_blank_as_cod_and_has_no_unmapped_bucket(self):
        p = pm.payment_predicates("x", False)
        self.assertIn("x IS NULL OR x = ''", p["cod"])
        self.assertNotIn("x IS NULL", p["prepaid"].replace("x IS NOT NULL", ""))
        self.assertEqual(p["unmapped"], "(1 = 0)")

    def test_sql_and_python_agree_for_both_rules(self):
        """Evaluate the generated predicates in SQLite and compare with the Python classifier."""
        import sqlite3

        names = [None, "", "Cash on Delivery (COD)", "cash_on_delivery", "razorpay", "shopify_payments", "gokwik",
                 "Gokwik PPCOD", "razorpay, Cash on Delivery (COD)", "Gokwik PPCOD, Cash on Delivery (COD)"]
        for flag in (False, True):
            p = pm.payment_predicates("g", flag)
            conn = sqlite3.connect(":memory:")
            conn.execute("CREATE TABLE t (id INTEGER, g TEXT)")
            conn.executemany("INSERT INTO t VALUES (?, ?)", list(enumerate(names)))
            rows = conn.execute(
                f"SELECT id, CASE WHEN {p['unmapped']} THEN 'unmapped' WHEN {p['ppcod']} THEN 'partially_paid' "
                f"WHEN {p['cod']} THEN 'cod' WHEN {p['prepaid']} THEN 'prepaid' ELSE 'NONE' END FROM t"
            ).fetchall()
            for idx, bucket in rows:
                self.assertEqual(bucket, pm.classify_payment_mode(names[idx], flag), (flag, names[idx]))
            # exactly one bucket per order, never zero or two
            for idx, name in enumerate(names):
                hits = conn.execute(
                    f"SELECT COALESCE({p['unmapped']}, 0) + COALESCE({p['ppcod']}, 0) + COALESCE({p['cod']}, 0) "
                    f"+ COALESCE({p['prepaid']}, 0) FROM t WHERE id = ?",  # CASE WHEN NULL is false, like here
                    (idx,),
                ).fetchone()[0]
                self.assertEqual(hits, 1, (flag, name))
            conn.close()

    def test_legacy_sql_puts_every_order_in_cod_prepaid_or_ppcod_so_totals_add_up(self):
        import sqlite3

        p = pm.payment_predicates("g", False)
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE t (id INTEGER, g TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?)",
                         [(1, None), (2, None), (3, "razorpay"), (4, "Gokwik PPCOD"), (5, "Cash on Delivery (COD)")])
        cod, prepaid, ppcod, unmapped = conn.execute(
            f"SELECT SUM({p['cod']}), SUM({p['prepaid']}), SUM({p['ppcod']}), SUM({p['unmapped']}) FROM t"
        ).fetchone()
        conn.close()
        self.assertEqual((cod, prepaid, ppcod, unmapped), (3, 1, 1, 0))  # 5 orders = 3 + 1 + 1: BBB's blanks are COD

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

    def test_blank_gateway_is_cod_for_every_brand_except_editeveryday(self):
        for fn, args in (
            (si.update_order_summary_incremental, (1, "BBB", {"2026-11-11"})),
            (si.update_hour_wise_sales_incremental, ("BBB", {"2026-11-11"})),
            (si.update_daily_citywise_summary_incremental, ("BBB", {"2026-11-11"})),
            (si.update_hourly_citywise_summary_incremental, ("BBB", {"2026-11-11"})),
        ):
            sql = self._insert_sql(fn, *args)
            self.assertIn("(1 = 0)", sql, fn.__name__)  # no unmapped bucket
            self.assertIn("payment_gateway_names IS NULL OR payment_gateway_names = ''", sql, fn.__name__)
            self.assertNotIn("TRIM(payment_gateway_names)", sql, fn.__name__)

    def test_editeveryday_keeps_the_unmapped_bucket_in_every_summary(self):
        for fn, args in (
            (si.update_order_summary_incremental, (15, "EDITEVERYDAY", {"2026-11-11"})),
            (si.update_hour_wise_sales_incremental, ("EDITEVERYDAY", {"2026-11-11"})),
            (si.update_daily_citywise_summary_incremental, ("EDITEVERYDAY", {"2026-11-11"})),
            (si.update_hourly_citywise_summary_incremental, ("EDITEVERYDAY", {"2026-11-11"})),
        ):
            sql = self._insert_sql(fn, *args)
            self.assertIn("payment_gateway_names IS NULL OR TRIM(payment_gateway_names) = ''", sql, fn.__name__)
            self.assertNotIn("(1 = 0)", sql, fn.__name__)

    def test_order_summary_returned_orders_use_the_same_per_brand_rule(self):
        legacy = self._insert_sql(si.update_order_summary_incremental, 2, "BBB", {"2026-11-11"})
        new = self._insert_sql(si.update_order_summary_incremental, 15, "EDITEVERYDAY", {"2026-11-11"})
        self.assertIn("h.payment_gateway_names IS NULL OR h.payment_gateway_names = ''", legacy)
        self.assertIn("h.payment_gateway_names IS NULL OR TRIM(h.payment_gateway_names) = ''", new)

    def test_payment_breakdown_backfill_follows_the_brand_rule(self):
        for brand, expect_legacy in (("BBB", True), ("EDITEVERYDAY", False)):
            cursor = _RecordingCursor()

            @contextmanager
            def fake_cursor(*_a, **_k):
                yield cursor, _Conn()

            with mock.patch.object(si, "get_db_cursor", fake_cursor):
                si.backfill_payment_mode_breakdown_columns(1, brand, {"2026-11-11"})
            hourly = next(s for s in cursor.statements if "GROUP BY date, hour" in s)
            self.assertEqual("(1 = 0)" in hourly, expect_legacy, brand)

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


class DiscountFactPaymentModeTests(unittest.TestCase):
    ORDER = {
        "id": 1, "name": "#1", "created_at": "2026-11-11T10:00:00+05:30", "total_price": "999",
        "discount_codes": [{"code": "SAVE10", "amount": "99"}], "payment_gateway_names": [],
    }

    def _mode(self, unmapped_bucket):
        from zoneinfo import ZoneInfo

        rows = so._normalize_discount_code_rows_from_orders(
            [self.ORDER], ZoneInfo("Asia/Kolkata"), unmapped_bucket=unmapped_bucket
        )
        return rows[0][8]  # payment_mode column

    def test_blank_gateway_is_cod_by_default_and_unmapped_only_when_asked(self):
        from zoneinfo import ZoneInfo

        self.assertEqual(self._mode(False), "cod")
        self.assertEqual(self._mode(True), "unmapped")
        default = so._normalize_discount_code_rows_from_orders([self.ORDER], ZoneInfo("Asia/Kolkata"))
        self.assertEqual(default[0][8], "cod")

    def test_the_upsert_picks_the_rule_from_the_brand(self):
        from zoneinfo import ZoneInfo

        for brand, expected in (("BBB", False), ("EDITEVERYDAY", True)):
            with mock.patch.object(so, "_normalize_discount_code_rows_from_orders", return_value=[]) as norm:
                so.upsert_dashboard_order_discount_fact_from_orders(
                    [self.ORDER], _RecordingCursor(), _Conn(), brand, ZoneInfo("Asia/Kolkata")
                )
            self.assertEqual(norm.call_args.kwargs["unmapped_bucket"], expected, brand)


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
