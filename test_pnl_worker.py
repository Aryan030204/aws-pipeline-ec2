import unittest
from datetime import date, datetime
from decimal import Decimal
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
        row = pnl_worker.compute_pnl_row(date(2026, 9, 15), None, None, None, [])
        self.assertEqual(set(row), set(pnl_worker.ALL_COLUMNS))


if __name__ == "__main__":
    unittest.main()
