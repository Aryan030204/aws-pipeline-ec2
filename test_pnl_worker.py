import unittest
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from workers import pnl_worker


IST = ZoneInfo("Asia/Kolkata")


def _config(category, value, value_type="flat", frequency="recurring", effective_from="2026-09-01", effective_to=None):
    return {
        "category": category,
        "value": value,
        "value_type": value_type,
        "frequency": frequency,
        "effective_from": effective_from,
        "effective_to": effective_to,
    }


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


class ConfigResolutionTests(unittest.TestCase):
    def test_latest_effective_from_wins_and_windows_are_respected(self):
        configs = [
            _config("rent", 1000, effective_from="2026-08-01"),
            _config("rent", 2000, effective_from="2026-09-10"),
            _config("rent", 3000, effective_from="2026-09-20"),
            _config("salaries", 500, effective_from="2026-08-01", effective_to="2026-09-05"),
            _config("bogus", 1, effective_from="2026-08-01"),
            _config("rent", 9999, value_type="weird", effective_from="2026-09-01"),
        ]
        active = pnl_worker.configs_effective_on(configs, date(2026, 9, 15))
        self.assertEqual(set(active), {"rent"})
        self.assertEqual(active["rent"]["value"], 2000)

    def test_percentage_is_share_of_net_sales(self):
        amount = pnl_worker.configured_amount(
            _config("cogs", 32, value_type="percentage"), date(2026, 9, 15), Decimal("1000.00")
        )
        self.assertEqual(amount, Decimal("320.00"))

    def test_flat_recurring_is_prorated_over_the_month(self):
        amount = pnl_worker.configured_amount(_config("rent", 3000), date(2026, 9, 15), Decimal("0"))
        self.assertEqual(amount, Decimal("100.00"))

    def test_flat_one_time_lands_only_on_effective_from(self):
        config = _config("technology", 1200, frequency="one_time", effective_from="2026-09-15")
        self.assertEqual(pnl_worker.configured_amount(config, date(2026, 9, 15), Decimal("0")), Decimal("1200.00"))
        self.assertEqual(pnl_worker.configured_amount(config, date(2026, 9, 16), Decimal("0")), Decimal("0"))


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
        configs = [
            _config("cogs", 30, value_type="percentage"),
            _config("rent", 3000),
            _config("meta", 999),
        ]

        row = pnl_worker.compute_pnl_row(day, sales, returns, Decimal("120.00"), configs)

        # (1180 - 100 - 80) = 1000 taxable -> GST 152.54 -> net 847.46
        self.assertEqual(row["gst"], Decimal("152.54"))
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

    def test_day_without_orders_still_carries_fixed_costs(self):
        row = pnl_worker.compute_pnl_row(date(2026, 9, 15), None, None, None, [_config("rent", 3000)])
        self.assertEqual(row["net_sales"], Decimal("0.00"))
        self.assertEqual(row["rent"], Decimal("100.00"))
        self.assertEqual(row["ebitda"], Decimal("-100.00"))
        self.assertEqual(row["meta"], Decimal("0"))
        self.assertEqual(row["meta_spend_synced"], 0)

    def test_row_covers_every_table_column(self):
        row = pnl_worker.compute_pnl_row(date(2026, 9, 15), None, None, None, [], brand_key="BBB")
        self.assertEqual(set(row), set(pnl_worker.ALL_COLUMNS))
        self.assertEqual(row["cost_config_source"], pnl_worker.CONFIG_SOURCE_MONGO)
        self.assertEqual(row["brand_key"], "BBB")

    def test_default_configs_cost_every_line_as_percent_of_net_sales(self):
        defaults = pnl_worker.default_cost_configs()
        self.assertEqual({c["category"] for c in defaults}, set(pnl_worker.COST_COLUMNS))

        sales = {"gross_sales": "1180.00", "discounts": "0", "total_orders": 1}
        row = pnl_worker.compute_pnl_row(
            date(2026, 9, 15), sales, None, None, defaults, pnl_worker.CONFIG_SOURCE_DEFAULT
        )
        # 1180 taxable -> GST 180 -> net 1000, so each default % is the amount
        self.assertEqual(row["net_sales"], Decimal("1000.00"))
        self.assertEqual(row["cogs"], Decimal("320.00"))
        self.assertEqual(row["packaging"], Decimal("15.00"))
        self.assertEqual(row["meta"], Decimal("80.00"))
        self.assertEqual(row["meta_spend_synced"], 0)
        self.assertEqual(row["cost_config_source"], "default")
        total_pct = sum(Decimal(v) for v in pnl_worker.DEFAULT_COST_PERCENTAGES.values())
        self.assertEqual(row["ebitda"], Decimal("1000.00") - Decimal("10") * total_pct)


class ConfigSourceTests(unittest.TestCase):
    def test_mongo_configs_win_when_present(self):
        with patch.object(pnl_worker, "fetch_brand_cost_configs", return_value=[_config("rent", 1)]):
            configs, source = pnl_worker.resolve_brand_cost_configs("BBB")
        self.assertEqual(source, pnl_worker.CONFIG_SOURCE_MONGO)
        self.assertEqual(configs[0]["category"], "rent")

    def test_defaults_used_when_brand_has_no_documents(self):
        with patch.object(pnl_worker, "fetch_brand_cost_configs", return_value=[]):
            configs, source = pnl_worker.resolve_brand_cost_configs("BBB")
        self.assertEqual(source, pnl_worker.CONFIG_SOURCE_DEFAULT)
        self.assertEqual(len(configs), len(pnl_worker.COST_COLUMNS))

    def test_missing_mongo_uri_falls_back_to_defaults(self):
        with patch.object(pnl_worker, "PNL_CONFIG_MONGO_URI", ""):
            self.assertEqual(pnl_worker.fetch_brand_cost_configs("BBB"), [])


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
    def test_only_overall_pnl_is_created(self):
        cursor, connection = MagicMock(), MagicMock()
        pnl_worker.ensure_overall_pnl_table(cursor, connection)
        ddl = cursor.execute.call_args[0][0]
        self.assertEqual(pnl_worker.PNL_TABLE, "overall_pnl")
        self.assertIn("CREATE TABLE IF NOT EXISTS `overall_pnl`", ddl)
        self.assertIn("`cost_config_source`", ddl)
        self.assertIn("PRIMARY KEY (`brand_key`, `date`)", ddl)
        self.assertEqual(cursor.execute.call_count, 1)

    def test_upsert_never_overwrites_key_columns(self):
        cursor, connection = MagicMock(), MagicMock()
        row = pnl_worker.compute_pnl_row(date(2026, 9, 15), None, None, None, [], brand_key="BBB")
        with patch("pipeline.db.executemany_chunked", return_value=1) as chunked:
            pnl_worker.upsert_pnl_rows(cursor, connection, [row])
        sql, values = chunked.call_args[0][2], chunked.call_args[0][3]
        self.assertIn("INSERT INTO `overall_pnl`", sql)
        self.assertNotIn("`brand_key` = VALUES", sql)
        self.assertNotIn("`date` = VALUES", sql)
        self.assertEqual(values[0][:2], ("BBB", date(2026, 9, 15)))


if __name__ == "__main__":
    unittest.main()
