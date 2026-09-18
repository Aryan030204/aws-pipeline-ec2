import os
import unittest
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from workers import pnl_worker


IST = ZoneInfo("Asia/Kolkata")


def _doc(gst_pct=18, **costs):
    """A total_config document with the given cost lines; e.g.
    _doc(cogs=("percentage", 30), rent=("flat", 3000), meta=None)."""
    return {
        "brand_id": "BBB",
        "gst_pct": gst_pct,
        "costs": {
            key: (None if spec is None else {"value_type": spec[0], "value": spec[1]})
            for key, spec in costs.items()
        },
    }


def _config(**kwargs):
    return pnl_worker.build_brand_config("BBB", _doc(**kwargs))


DEFAULTS = pnl_worker.build_brand_config("BBB", None)


class TargetDatesTests(unittest.TestCase):
    def test_default_window_is_yesterday_and_day_before(self):
        now = datetime(2026, 9, 15, 0, 10, tzinfo=IST)
        self.assertEqual(
            pnl_worker.compute_target_dates(IST, current_dt=now, lookback_days=2, start_date="", end_date=""),
            [date(2026, 9, 13), date(2026, 9, 14)],
        )

    def test_explicit_range_wins(self):
        self.assertEqual(
            pnl_worker.compute_target_dates(IST, lookback_days=2, start_date="2026-09-01", end_date="2026-09-03"),
            [date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 3)],
        )


class BrandConfigTests(unittest.TestCase):
    def test_missing_document_means_all_defaults(self):
        self.assertEqual(DEFAULTS.source, pnl_worker.CONFIG_SOURCE_DEFAULT)
        self.assertEqual(DEFAULTS.gst_pct, Decimal("18"))
        self.assertEqual(DEFAULTS.defaulted, set(pnl_worker.COST_COLUMNS))
        self.assertEqual(set(DEFAULTS.costs), set(pnl_worker.COST_COLUMNS))
        self.assertEqual(DEFAULTS.costs["cogs"], {"value_type": "percentage", "value": Decimal("32")})

    def test_document_lines_win_and_null_lines_fall_back_per_line(self):
        config = _config(gst_pct=12, cogs=("percentage", 30), rent=("flat", 90000), meta=None)
        self.assertEqual(config.source, pnl_worker.CONFIG_SOURCE_MONGO)
        self.assertEqual(config.gst_pct, Decimal("12"))
        self.assertEqual(config.costs["cogs"], {"value_type": "percentage", "value": Decimal("30")})
        self.assertEqual(config.costs["rent"], {"value_type": "flat", "value": Decimal("90000")})
        self.assertEqual(config.costs["meta"], pnl_worker.default_cost_line("meta"))
        self.assertIn("meta", config.defaulted)
        self.assertIn("salaries", config.defaulted)  # absent key, not just null
        self.assertNotIn("cogs", config.defaulted)

    def test_unusable_lines_and_gst_fall_back(self):
        config = _config(gst_pct=250, cogs=("weird", 30), rent=("flat", "abc"), shipping=("flat", -5))
        self.assertEqual(config.gst_pct, Decimal("18"))
        for column in ("cogs", "rent", "shipping"):
            self.assertIn(column, config.defaulted)

    def test_gst_rate_is_inclusive_share(self):
        self.assertEqual(_config(gst_pct=18).gst_rate, Decimal(18) / Decimal(118))
        self.assertEqual(_config(gst_pct=0).gst_rate, Decimal(0))

    def test_percentage_is_share_of_net_sales(self):
        line = {"value_type": "percentage", "value": Decimal("32")}
        self.assertEqual(pnl_worker.cost_line_amount(line, date(2026, 9, 15), Decimal("1000.00")), Decimal("320.00"))

    def test_flat_is_a_monthly_amount_prorated_per_day(self):
        line = {"value_type": "flat", "value": Decimal("3000")}
        self.assertEqual(pnl_worker.cost_line_amount(line, date(2026, 9, 15), Decimal("0")), Decimal("100.00"))
        self.assertEqual(pnl_worker.cost_line_amount(line, date(2026, 2, 1), Decimal("0")), Decimal("107.14"))


class ConfigFetchTests(unittest.TestCase):
    def test_reads_one_document_by_brand_id_from_total_config(self):
        fake_client = MagicMock()
        collection = fake_client.__getitem__.return_value.__getitem__.return_value
        collection.find_one.return_value = _doc(cogs=("percentage", 30))
        with patch.object(pnl_worker, "PNL_CONFIG_MONGO_URI", "mongodb://x"), patch(
            "pymongo.MongoClient", return_value=fake_client
        ):
            doc = pnl_worker.fetch_total_config_doc("BBB")
        self.assertEqual(doc["costs"]["cogs"]["value"], 30)
        self.assertEqual(collection.find_one.call_args[0][0], {"brand_id": "BBB"})
        self.assertEqual(pnl_worker.PNL_CONFIG_COLLECTION, "total_config")
        fake_client.close.assert_called_once()

    def test_missing_mongo_uri_falls_back_to_defaults(self):
        with patch.object(pnl_worker, "PNL_CONFIG_MONGO_URI", ""):
            self.assertIsNone(pnl_worker.fetch_total_config_doc("BBB"))

    def test_resolve_returns_defaults_when_brand_has_no_document(self):
        with patch.object(pnl_worker, "fetch_total_config_doc", return_value=None):
            config = pnl_worker.resolve_brand_config("BBB")
        self.assertEqual(config.source, pnl_worker.CONFIG_SOURCE_DEFAULT)


class PnlRowTests(unittest.TestCase):
    def test_cascade_matches_dashboard_model(self):
        day = date(2026, 9, 15)
        sales = {
            "gross_sales": "1180.00",
            "discounts": "100.00",
            "total_orders": 7,
            "cod_sales": "300.00",
            "prepaid_sales": "800.00",
            "partially_prepaid_sales": "80.00",
            "rto_orders": 1,
        }
        returns = {"cancellations": Decimal("80.00"), "rto_returns": Decimal("50.00")}
        config = _config(
            cogs=("percentage", 30),
            rent=("flat", 3000),
            meta=("flat", 999),
            **{c: ("flat", 0) for c in pnl_worker.COST_COLUMNS if c not in ("cogs", "rent", "meta")},
        )

        row = pnl_worker.compute_pnl_row(day, sales, returns, Decimal("120.00"), config, brand_key="BBB")

        # (1180 - 100 - 80) = 1000 taxable -> GST 152.54 -> net 847.46
        self.assertEqual(row["gst"], Decimal("152.54"))
        self.assertEqual(row["gst_pct"], Decimal("18.00"))
        self.assertEqual(row["net_sales"], Decimal("847.46"))
        self.assertEqual(row["cogs"], Decimal("254.24"))
        self.assertEqual(row["gross_margin"], Decimal("593.22"))
        self.assertEqual(row["cm1"], Decimal("593.22"))
        # synced Meta spend beats the configured 999
        self.assertEqual(row["meta"], Decimal("120.00"))
        self.assertEqual(row["meta_spend_synced"], 1)
        self.assertEqual(row["cm2"], Decimal("473.22"))
        self.assertEqual(row["cm3"], Decimal("473.22"))
        self.assertEqual(row["rent"], Decimal("100.00"))
        self.assertEqual(row["ebitda"], Decimal("373.22"))
        self.assertEqual(row["rto_returns"], Decimal("50.00"))
        self.assertEqual(row["total_orders"], 7)
        self.assertEqual(row["rto_orders"], 1)
        self.assertEqual(row["brand_key"], "BBB")
        self.assertEqual(row["cost_config_source"], "mongo")

    def test_gst_pct_from_config_drives_the_gst_line(self):
        sales = {"gross_sales": "1120.00", "discounts": "0"}
        config = _config(gst_pct=12, **{c: ("flat", 0) for c in pnl_worker.COST_COLUMNS})
        row = pnl_worker.compute_pnl_row(date(2026, 9, 15), sales, None, None, config)
        # 1120 * 12/112 = 120
        self.assertEqual(row["gst"], Decimal("120.00"))
        self.assertEqual(row["net_sales"], Decimal("1000.00"))
        self.assertEqual(row["gst_pct"], Decimal("12.00"))

    def test_day_without_orders_still_carries_fixed_costs(self):
        config = _config(rent=("flat", 3000), **{c: ("flat", 0) for c in pnl_worker.COST_COLUMNS if c != "rent"})
        row = pnl_worker.compute_pnl_row(date(2026, 9, 15), None, None, None, config)
        self.assertEqual(row["net_sales"], Decimal("0.00"))
        self.assertEqual(row["rent"], Decimal("100.00"))
        self.assertEqual(row["ebitda"], Decimal("-100.00"))
        self.assertEqual(row["meta"], Decimal("0.00"))
        self.assertEqual(row["meta_spend_synced"], 0)

    def test_row_covers_every_table_column(self):
        row = pnl_worker.compute_pnl_row(date(2026, 9, 15), None, None, None, DEFAULTS, brand_key="BBB")
        self.assertEqual(set(row), set(pnl_worker.ALL_COLUMNS))
        self.assertEqual(row["cost_config_source"], pnl_worker.CONFIG_SOURCE_DEFAULT)

    def test_all_defaults_cost_every_line_as_percent_of_net_sales(self):
        sales = {"gross_sales": "1180.00", "discounts": "0", "total_orders": 1}
        row = pnl_worker.compute_pnl_row(date(2026, 9, 15), sales, None, None, DEFAULTS)
        # 1180 taxable -> GST 180 -> net 1000, so each default % is the amount
        self.assertEqual(row["net_sales"], Decimal("1000.00"))
        self.assertEqual(row["cogs"], Decimal("320.00"))
        self.assertEqual(row["packaging"], Decimal("15.00"))
        self.assertEqual(row["meta"], Decimal("80.00"))
        self.assertEqual(row["cost_config_source"], "default")
        total_pct = sum(Decimal(v) for v in pnl_worker.DEFAULT_COST_PERCENTAGES.values())
        self.assertEqual(row["ebitda"], Decimal("1000.00") - Decimal("10") * total_pct)


class ReturnsRowsTests(unittest.TestCase):
    def test_cancels_refunds_and_rto_are_separate_buckets(self):
        cursor = MagicMock()
        cursor.fetchall.side_effect = [
            [{"d": date(2026, 9, 15), "cancel_amount": "300.00", "rto_amount": "120.00"}],
            [{"d": date(2026, 9, 15), "refund_amount": "45.50"}, {"d": date(2026, 9, 16), "refund_amount": "10.00"}],
        ]
        rows = pnl_worker.fetch_returns_rows(cursor, date(2026, 9, 15), date(2026, 9, 16))

        self.assertEqual(rows[date(2026, 9, 15)], {"cancellations": Decimal("345.50"), "rto_returns": Decimal("120.00")})
        self.assertEqual(rows[date(2026, 9, 16)], {"cancellations": Decimal("10.00"), "rto_returns": Decimal("0")})
        refund_sql = cursor.execute.call_args_list[1][0][0]
        self.assertIn("NOT EXISTS", refund_sql)


class TableOwnershipTests(unittest.TestCase):
    def test_worker_runs_no_ddl(self):
        """Table creation lives in migrations/003_overall_pnl.sql, not here."""
        import inspect
        import re

        self.assertEqual(pnl_worker.PNL_TABLE, "overall_pnl")
        self.assertFalse(hasattr(pnl_worker, "ensure_overall_pnl_table"))
        src = inspect.getsource(pnl_worker)
        for verb in ("CREATE TABLE", "ALTER TABLE", "DROP TABLE"):
            self.assertIsNone(re.search(rf"\b{verb}\b", src, re.IGNORECASE), f"worker contains {verb}")

    def test_migration_creates_overall_pnl_if_not_exists(self):
        migration_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "migrations", "003_overall_pnl.sql")
        with open(migration_path) as fh:
            sql = fh.read()
        self.assertIn("CREATE TABLE IF NOT EXISTS `overall_pnl`", sql)
        self.assertIn("PRIMARY KEY (`brand_key`, `date`)", sql)
        for column in pnl_worker.ALL_COLUMNS:
            self.assertIn(f"`{column}`", sql, f"migration is missing column {column}")

    def test_upsert_never_overwrites_key_columns(self):
        cursor, connection = MagicMock(), MagicMock()
        row = pnl_worker.compute_pnl_row(date(2026, 9, 15), None, None, None, DEFAULTS, brand_key="BBB")
        with patch("pipeline.db.executemany_chunked", return_value=1) as chunked:
            pnl_worker.upsert_pnl_rows(cursor, connection, [row])
        sql, values = chunked.call_args[0][2], chunked.call_args[0][3]
        self.assertIn("INSERT INTO `overall_pnl`", sql)
        self.assertNotIn("`brand_key` = VALUES", sql)
        self.assertNotIn("`date` = VALUES", sql)
        self.assertEqual(values[0][:2], ("BBB", date(2026, 9, 15)))

    def test_source_reads_never_write(self):
        import inspect
        import re

        for fn in (pnl_worker.fetch_sales_rows, pnl_worker.fetch_returns_rows, pnl_worker.fetch_meta_spend_rows):
            src = inspect.getsource(fn)
            for verb in ("INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP", "TRUNCATE"):
                self.assertIsNone(re.search(rf"\b{verb}\b", src, re.IGNORECASE), f"{fn.__name__} contains {verb}")


if __name__ == "__main__":
    unittest.main()
