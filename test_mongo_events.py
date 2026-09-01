import sys
import types
import unittest
from datetime import date, datetime, timezone
from unittest import mock
from zoneinfo import ZoneInfo

try:
    import pymongo  # noqa: F401
except ModuleNotFoundError:
    fake_pymongo = types.ModuleType("pymongo")
    fake_pymongo.MongoClient = object
    sys.modules["pymongo"] = fake_pymongo

# aws_background must be imported before pipeline.mongo_events: mongo_events
# does `import aws_background` at module level (to reach TEST_MODE-adjacent
# facade names), and aws_background.py in turn re-exports from
# pipeline.mongo_events. Importing aws_background first lets Python's normal
# import machinery resolve that cleanly; importing pipeline.mongo_events
# directly here would start a circular partial-init instead (same class of
# issue __main__ execution has, documented at the top of aws_background.py).
import aws_background  # noqa: F401
from pipeline import mongo_events


IST = ZoneInfo("Asia/Kolkata")


class _DummyConnection:
    def __init__(self):
        self.commit_count = 0

    def commit(self):
        self.commit_count += 1


class _DummyCursor:
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


class _FakeMongoClient:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class SanitizeEventTypeColumnTests(unittest.TestCase):
    def test_normalizes_common_cases(self):
        self.assertEqual(
            mongo_events._sanitize_event_type_column("Checkout Initiated"),
            "checkout_initiated_count",
        )
        self.assertEqual(mongo_events._sanitize_event_type_column("buy-now"), "buy_now_count")
        self.assertEqual(mongo_events._sanitize_event_type_column("ADD_TO_CART!!!"), "add_to_cart_count")
        self.assertIsNone(mongo_events._sanitize_event_type_column("___"))
        self.assertIsNone(mongo_events._sanitize_event_type_column(None))


class QuoteIdentifierTests(unittest.TestCase):
    def test_quotes_safe_identifier(self):
        self.assertEqual(mongo_events._quote_identifier("checkout_initiated_count"), "`checkout_initiated_count`")

    def test_rejects_unsafe_identifier(self):
        with self.assertRaises(ValueError):
            mongo_events._quote_identifier("bad; DROP TABLE x --")


class IterUniqueDailyPairEventsAllTypesTests(unittest.TestCase):
    def _mock_collection(self, docs):
        fake_client = _FakeMongoClient()
        fake_collection = mock.Mock()
        fake_collection.find.return_value = list(docs)
        return mock.patch.object(
            mongo_events,
            "_resolve_mongo_sessions_collection",
            return_value=(fake_client, fake_collection),
        ), fake_client

    def test_same_pair_different_event_types_both_count_independently(self):
        docs = [
            {
                "session_id": "s1",
                "cart_token": "c1",
                "event_type": "checkout_initiated",
                "createdAt": datetime(2026, 8, 17, 10, 0, tzinfo=timezone.utc),
                "variantId": "v1",
            },
            {
                "session_id": "s1",
                "cart_token": "c1",
                "event_type": "buy_now",
                "createdAt": datetime(2026, 8, 17, 10, 5, tzinfo=timezone.utc),
                "variantId": "v1",
            },
        ]
        patcher, fake_client = self._mock_collection(docs)
        with patcher:
            events = mongo_events._iter_unique_daily_pair_events_all_types_from_mongo(
                start_date=date(2026, 8, 17),
                end_date=date(2026, 8, 17),
                mongo_shop_name="tmc",
                tz=IST,
            )

        self.assertTrue(fake_client.closed)
        columns = sorted(e["event_column"] for e in events)
        self.assertEqual(columns, ["buy_now_count", "checkout_initiated_count"])

    def test_same_pair_same_event_type_twice_is_dropped(self):
        docs = [
            {
                "session_id": "s1",
                "cart_token": "c1",
                "event_type": "checkout_initiated",
                "createdAt": datetime(2026, 8, 17, 10, 0, tzinfo=timezone.utc),
                "variantId": "v1",
            },
            {
                "session_id": "s1",
                "cart_token": "c1",
                "event_type": "checkout_initiated",
                "createdAt": datetime(2026, 8, 17, 11, 0, tzinfo=timezone.utc),
                "variantId": "v1",
            },
        ]
        patcher, _ = self._mock_collection(docs)
        with patcher:
            events = mongo_events._iter_unique_daily_pair_events_all_types_from_mongo(
                start_date=date(2026, 8, 17),
                end_date=date(2026, 8, 17),
                mongo_shop_name="tmc",
                tz=IST,
            )

        self.assertEqual(events, [])

    def test_missing_session_id_or_cart_token_is_skipped(self):
        docs = [
            {
                "session_id": "",
                "cart_token": "c1",
                "event_type": "checkout_initiated",
                "createdAt": datetime(2026, 8, 17, 10, 0, tzinfo=timezone.utc),
                "variantId": "v1",
            },
        ]
        patcher, _ = self._mock_collection(docs)
        with patcher:
            events = mongo_events._iter_unique_daily_pair_events_all_types_from_mongo(
                start_date=date(2026, 8, 17),
                end_date=date(2026, 8, 17),
                mongo_shop_name="tmc",
                tz=IST,
            )

        self.assertEqual(events, [])


class AggregateVariantWiseEventCountsTests(unittest.TestCase):
    def test_aggregates_by_date_variant_and_event_column(self):
        fake_events = [
            {"date": date(2026, 8, 17), "event_column": "checkout_initiated_count", "variant_ids": ["v1", "v2"]},
            {"date": date(2026, 8, 17), "event_column": "buy_now_count", "variant_ids": ["v1"]},
        ]
        with mock.patch.object(
            mongo_events, "_iter_unique_daily_pair_events_all_types_from_mongo", return_value=fake_events
        ):
            aggregated, event_columns = mongo_events._aggregate_variant_wise_event_counts(
                start_date=date(2026, 8, 17),
                end_date=date(2026, 8, 17),
                mongo_shop_name="tmc",
                tz=IST,
            )

        self.assertEqual(event_columns, {"checkout_initiated_count", "buy_now_count"})
        self.assertEqual(
            aggregated[(date(2026, 8, 17), "v1")],
            {"checkout_initiated_count": 1, "buy_now_count": 1},
        )
        self.assertEqual(
            aggregated[(date(2026, 8, 17), "v2")],
            {"checkout_initiated_count": 1},
        )


class UpsertVariantWiseEventsSummaryTests(unittest.TestCase):
    def test_builds_dynamic_insert_and_ensures_columns(self):
        cursor = _DummyCursor(fetch_rows=[{"Field": "date"}, {"Field": "variant_id"}])
        connection = _DummyConnection()
        aggregated = {
            (date(2026, 8, 17), "v1"): {"checkout_initiated_count": 2, "buy_now_count": 1},
            (date(2026, 8, 17), "v2"): {"checkout_initiated_count": 1},
        }
        event_columns = {"checkout_initiated_count", "buy_now_count"}

        inserted = mongo_events._upsert_variant_wise_events_summary(
            cursor, connection, date(2026, 8, 17), date(2026, 8, 17), aggregated, event_columns
        )

        self.assertEqual(inserted, 2)
        alter_statements = [sql for sql, _params in cursor.executed if "ALTER TABLE" in sql]
        self.assertEqual(len(alter_statements), 2)
        self.assertTrue(any("buy_now_count" in s for s in alter_statements))
        self.assertTrue(any("checkout_initiated_count" in s for s in alter_statements))

        delete_statements = [sql for sql, _params in cursor.executed if sql.strip().startswith("DELETE")]
        self.assertEqual(len(delete_statements), 1)

        self.assertEqual(len(cursor.executemany_calls), 1)
        insert_sql, rows = cursor.executemany_calls[0]
        self.assertIn("INSERT INTO `variant_wise_events_summary`", insert_sql)
        self.assertIn("ON DUPLICATE KEY UPDATE", insert_sql)
        self.assertEqual(
            rows,
            [
                (date(2026, 8, 17), "v1", 1, 2),
                (date(2026, 8, 17), "v2", 0, 1),
            ],
        )
        self.assertEqual(connection.commit_count, 2)  # ensure_columns + final commit

    def test_no_event_columns_is_a_no_op(self):
        cursor = _DummyCursor()
        connection = _DummyConnection()

        inserted = mongo_events._upsert_variant_wise_events_summary(
            cursor, connection, date(2026, 8, 17), date(2026, 8, 17), {}, set()
        )

        self.assertEqual(inserted, 0)
        self.assertEqual(cursor.executed, [])
        self.assertEqual(cursor.executemany_calls, [])


if __name__ == "__main__":
    unittest.main()
