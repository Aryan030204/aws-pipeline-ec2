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

    def test_workflow_exists_and_has_schedule(self):
        workflow_path = ".github/workflows/events-summary-worker.yml"
        with open(workflow_path, "r", encoding="utf-8") as handle:
            content = handle.read()

        self.assertIn("workflow_dispatch:", content)
        self.assertIn("cron: '30 18 * * *'", content)
        self.assertIn("workers/events_summary_worker.py", content)


if __name__ == "__main__":
    unittest.main()
