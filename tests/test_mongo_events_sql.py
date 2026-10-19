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

# Safe import order (see test_mongo_events.py's comment): aws_background
# first, then the pipeline module.
import aws_background  # noqa: F401
from pipeline import mongo_events_sql as mes

IST = ZoneInfo("Asia/Kolkata")


class _DummyConnection:
    def __init__(self):
        self.commit_count = 0

    def commit(self):
        self.commit_count += 1


class _FakeCursor:
    """Fakes a dict-cursor's SELECT ... FROM mongo_events_summary - rows are
    given as dicts with event_type/session_id/cart_token/variantId/
    created_at_utc, matching what _fetch_candidate_rows expects back."""

    def __init__(self, rows):
        self._rows = rows
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return list(self._rows)


def _row(event_type, session_id, cart_token, created_at, variant_id=None):
    return {
        "event_type": event_type,
        "session_id": session_id,
        "cart_token": cart_token,
        "variantId": variant_id,
        "created_at_utc": created_at,
    }


class FetchCandidateRowsTests(unittest.TestCase):
    def test_queries_mongo_events_summary_with_date_window_and_event_type(self):
        cursor = _FakeCursor([])
        mes._fetch_candidate_rows(cursor, date(2026, 8, 17), date(2026, 8, 17), IST, event_type="buy_now")

        sql, params = cursor.executed[0]
        self.assertIn("FROM mongo_events_summary", sql)
        self.assertIn("event_type = %s", sql)
        self.assertEqual(params[-1], "buy_now")

    def test_no_event_type_filter_omits_the_clause(self):
        cursor = _FakeCursor([])
        mes._fetch_candidate_rows(cursor, date(2026, 8, 17), date(2026, 8, 17), IST, event_type=None)

        sql, params = cursor.executed[0]
        self.assertNotIn("event_type = %s", sql)
        self.assertEqual(len(params), 2)

    def test_maps_rows_into_mongo_doc_shape(self):
        created = datetime(2026, 8, 17, 10, 0)
        cursor = _FakeCursor([_row("buy_now", "s1", "c1", created, "v1")])
        rows = mes._fetch_candidate_rows(cursor, date(2026, 8, 17), date(2026, 8, 17), IST, event_type="buy_now")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["session_id"], "s1")
        self.assertEqual(rows[0]["cart_token"], "c1")
        self.assertEqual(rows[0]["variantId"], "v1")
        self.assertEqual(rows[0]["createdAt"], created)

    def test_tuple_rows_are_also_supported(self):
        created = datetime(2026, 8, 17, 10, 0)
        cursor = _FakeCursor([("buy_now", "s1", "c1", "v1", created)])
        rows = mes._fetch_candidate_rows(cursor, date(2026, 8, 17), date(2026, 8, 17), IST, event_type="buy_now")
        self.assertEqual(rows[0]["session_id"], "s1")

    def test_rows_missing_created_at_are_dropped(self):
        cursor = _FakeCursor([_row("buy_now", "s1", "c1", None)])
        rows = mes._fetch_candidate_rows(cursor, date(2026, 8, 17), date(2026, 8, 17), IST, event_type="buy_now")
        self.assertEqual(rows, [])


class IterCountedPairEventsFromSqlTests(unittest.TestCase):
    """The trickiest logic in this module: a greedy sequential scan, not a
    simple consecutive-row gap check - see the module/plan docstring."""

    def _run(self, rows):
        cursor = _FakeCursor(rows)
        with mock.patch.object(mes, "_fetch_candidate_rows", return_value=[
            {
                "session_id": r[0], "cart_token": r[1], "createdAt": r[2],
                "variantId": r[3] if len(r) > 3 else None,
            }
            for r in rows
        ]):
            return mes._iter_counted_pair_events_from_sql(
                cursor, date(2026, 8, 17), date(2026, 8, 17), "buy_now", IST, dedupe_window_minutes=30
            )

    def test_burst_of_25min_gaps_still_produces_multiple_counts(self):
        # t=0, 25, 50, 75 min - no single consecutive gap exceeds 30min, but
        # the greedy "since last COUNTED" rule still counts t=0 and t=50.
        base = datetime(2026, 8, 17, 10, 0, tzinfo=timezone.utc)
        rows = [
            ("s1", "c1", base),
            ("s1", "c1", base.replace(minute=25)),
            ("s1", "c1", base.replace(minute=50)),
            ("s1", "c1", base.replace(hour=11, minute=15)),
        ]
        events = self._run(rows)
        self.assertEqual(len(events), 2)

    def test_single_event_counts_once(self):
        base = datetime(2026, 8, 17, 10, 0, tzinfo=timezone.utc)
        events = self._run([("s1", "c1", base)])
        self.assertEqual(len(events), 1)

    def test_two_events_over_30min_apart_both_count(self):
        base = datetime(2026, 8, 17, 10, 0, tzinfo=timezone.utc)
        events = self._run([("s1", "c1", base), ("s1", "c1", base.replace(hour=11))])
        self.assertEqual(len(events), 2)

    def test_missing_session_id_or_cart_token_is_skipped(self):
        base = datetime(2026, 8, 17, 10, 0, tzinfo=timezone.utc)
        events = self._run([("", "c1", base)])
        self.assertEqual(events, [])

    def test_records_hour_and_variant_ids(self):
        base = datetime(2026, 8, 17, 15, 30, tzinfo=timezone.utc)  # 21:00 IST
        events = self._run([("s1", "c1", base, "v9")])
        self.assertEqual(events[0]["hour"], 21)
        self.assertEqual(events[0]["variant_ids"], ["v9"])


class IterUniqueDailyPairEventsFromSqlTests(unittest.TestCase):
    def _run(self, docs):
        cursor = _FakeCursor([])
        with mock.patch.object(mes, "_fetch_candidate_rows", return_value=docs):
            return mes._iter_unique_daily_pair_events_from_sql(
                cursor, date(2026, 8, 17), date(2026, 8, 17), "checkout_initiated", IST
            )

    def test_single_occurrence_counts(self):
        docs = [{"session_id": "s1", "cart_token": "c1", "createdAt": datetime(2026, 8, 17, 10, 0), "variantId": "v1"}]
        events = self._run(docs)
        self.assertEqual(len(events), 1)

    def test_duplicate_pair_is_dropped_entirely(self):
        docs = [
            {"session_id": "s1", "cart_token": "c1", "createdAt": datetime(2026, 8, 17, 10, 0), "variantId": "v1"},
            {"session_id": "s1", "cart_token": "c1", "createdAt": datetime(2026, 8, 17, 11, 0), "variantId": "v1"},
        ]
        events = self._run(docs)
        self.assertEqual(events, [])


class IterUniqueDailyPairEventsAllTypesFromSqlTests(unittest.TestCase):
    def _run(self, docs):
        cursor = _FakeCursor([])
        with mock.patch.object(mes, "_fetch_candidate_rows", return_value=docs):
            return mes._iter_unique_daily_pair_events_all_types_from_sql(
                cursor, date(2026, 8, 17), date(2026, 8, 17), IST
            )

    def test_same_pair_different_event_types_both_count_independently(self):
        docs = [
            {"session_id": "s1", "cart_token": "c1", "event_type": "checkout_initiated",
             "createdAt": datetime(2026, 8, 17, 10, 0), "variantId": "v1"},
            {"session_id": "s1", "cart_token": "c1", "event_type": "buy_now",
             "createdAt": datetime(2026, 8, 17, 10, 5), "variantId": "v1"},
        ]
        events = self._run(docs)
        columns = sorted(e["event_column"] for e in events)
        self.assertEqual(columns, ["buy_now_count", "checkout_initiated_count"])

    def test_same_pair_same_event_type_twice_is_dropped(self):
        docs = [
            {"session_id": "s1", "cart_token": "c1", "event_type": "checkout_initiated",
             "createdAt": datetime(2026, 8, 17, 10, 0), "variantId": "v1"},
            {"session_id": "s1", "cart_token": "c1", "event_type": "checkout_initiated",
             "createdAt": datetime(2026, 8, 17, 11, 0), "variantId": "v1"},
        ]
        self.assertEqual(self._run(docs), [])

    def test_missing_session_id_or_cart_token_is_skipped(self):
        docs = [{"session_id": "", "cart_token": "c1", "event_type": "checkout_initiated",
                 "createdAt": datetime(2026, 8, 17, 10, 0), "variantId": "v1"}]
        self.assertEqual(self._run(docs), [])


class AggregateVariantWiseEventCountsFromSqlTests(unittest.TestCase):
    def test_aggregates_by_date_variant_and_event_column(self):
        fake_events = [
            {"date": date(2026, 8, 17), "event_column": "checkout_initiated_count", "variant_ids": ["v1", "v2"]},
            {"date": date(2026, 8, 17), "event_column": "buy_now_count", "variant_ids": ["v1"]},
        ]
        cursor = _FakeCursor([])
        with mock.patch.object(mes, "_iter_unique_daily_pair_events_all_types_from_sql", return_value=fake_events):
            aggregated, event_columns = mes._aggregate_variant_wise_event_counts_from_sql(
                cursor, date(2026, 8, 17), date(2026, 8, 17), IST
            )

        self.assertEqual(event_columns, {"checkout_initiated_count", "buy_now_count"})
        self.assertEqual(aggregated[(date(2026, 8, 17), "v1")], {"checkout_initiated_count": 1, "buy_now_count": 1})
        self.assertEqual(aggregated[(date(2026, 8, 17), "v2")], {"checkout_initiated_count": 1})


class BrandGateTests(unittest.TestCase):
    def test_non_ci_events_brand_returns_none(self):
        self.assertIsNone(mes._brand_gate("Some Random Brand", "random", "test"))

    def test_ci_events_brand_returns_shop_name(self):
        self.assertEqual(mes._brand_gate("TMC", "tmc", "test"), "tmc")


class PublicSyncFunctionsSkipNonCiEventsBrandsTests(unittest.TestCase):
    """Each public sync_*_sql function must bail out before touching the DB
    at all for a brand that isn't CI-events-enabled."""

    def test_sync_checkout_initiated_ci_events_sql_skips(self):
        cursor = _FakeCursor([])
        mes.sync_checkout_initiated_ci_events_sql(
            1, "NotCiBrand", date(2026, 8, 17), date(2026, 8, 17), cursor=cursor, connection=_DummyConnection()
        )
        self.assertEqual(cursor.executed, [])

    def test_update_overall_summary_event_columns_sql_skips(self):
        cursor = _FakeCursor([])
        mes.update_overall_summary_event_columns_sql(
            1, "NotCiBrand", date(2026, 8, 17), date(2026, 8, 17), cursor=cursor, connection=_DummyConnection()
        )
        self.assertEqual(cursor.executed, [])


if __name__ == "__main__":
    unittest.main()
