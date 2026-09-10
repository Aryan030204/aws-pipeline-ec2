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

# Same import-order requirement as test_mongo_events.py: aws_background must
# be imported first (it does `import aws_background` at module level, and
# aws_background.py re-exports from this module in turn), otherwise a direct
# `from pipeline import checkout_login_status` starts a circular partial-init.
import aws_background  # noqa: F401
from pipeline import checkout_login_status as cls


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


def _checkout_doc(session_id, cart_token, created_at, variant_id="v1"):
    return {
        "session_id": session_id,
        "cart_token": cart_token,
        "event_type": "checkout_initiated",
        "createdAt": created_at,
        "variantId": variant_id,
    }


class _MockCollectionMixin:
    def _mock_collection(self, find_docs=None, distinct_values=None):
        fake_client = _FakeMongoClient()
        fake_collection = mock.Mock()
        fake_collection.find.return_value = list(find_docs or [])
        fake_collection.distinct.return_value = list(distinct_values or [])
        return (
            mock.patch.object(
                cls,
                "_resolve_mongo_sessions_collection",
                return_value=(fake_client, fake_collection),
            ),
            fake_client,
        )


class FetchLoginCartTokensTests(_MockCollectionMixin, unittest.TestCase):
    def test_returns_distinct_cart_tokens(self):
        patcher, fake_client = self._mock_collection(distinct_values=["c1", "c2", "c1", ""])
        with patcher:
            tokens = cls._fetch_login_cart_tokens(
                start_date=date(2026, 8, 17),
                end_date=date(2026, 8, 17),
                mongo_shop_name="tmc",
                tz=IST,
            )
        self.assertEqual(tokens, {"c1", "c2"})
        self.assertTrue(fake_client.closed)


class IterUniqueDailyCheckoutPairsTests(_MockCollectionMixin, unittest.TestCase):
    def test_duplicate_checkout_docs_same_pair_counts_once(self):
        # CASE 3: duplicate (cart_token, session_id) documents.
        created = datetime(2026, 8, 17, 10, 0, tzinfo=timezone.utc)
        docs = [
            _checkout_doc("s1", "c1", created),
            _checkout_doc("s1", "c1", created.replace(hour=11)),
        ]
        patcher, _ = self._mock_collection(find_docs=docs)
        with patcher:
            pairs = cls._iter_unique_daily_checkout_pairs(
                start_date=date(2026, 8, 17), end_date=date(2026, 8, 17),
                mongo_shop_name="tmc", tz=IST,
            )
        # Same semantics as pipeline.mongo_events' dedup: >1 doc for the same
        # pair on the same day means it's dropped by the "exactly once"
        # rule, not double-counted.
        self.assertEqual(pairs, [])

    def test_distinct_pairs_each_counted_once(self):
        # CASE 5: multiple distinct checkout pairs.
        created = datetime(2026, 8, 17, 10, 0, tzinfo=timezone.utc)
        docs = [
            _checkout_doc("s1", "c1", created, variant_id="v1"),
            _checkout_doc("s2", "c2", created, variant_id="v2"),
        ]
        patcher, _ = self._mock_collection(find_docs=docs)
        with patcher:
            pairs = cls._iter_unique_daily_checkout_pairs(
                start_date=date(2026, 8, 17), end_date=date(2026, 8, 17),
                mongo_shop_name="tmc", tz=IST,
            )
        self.assertEqual(len(pairs), 2)
        self.assertEqual({p["cart_token"] for p in pairs}, {"c1", "c2"})

    def test_missing_session_id_or_cart_token_is_skipped(self):
        created = datetime(2026, 8, 17, 10, 0, tzinfo=timezone.utc)
        docs = [_checkout_doc("", "c1", created)]
        patcher, _ = self._mock_collection(find_docs=docs)
        with patcher:
            pairs = cls._iter_unique_daily_checkout_pairs(
                start_date=date(2026, 8, 17), end_date=date(2026, 8, 17),
                mongo_shop_name="tmc", tz=IST,
            )
        self.assertEqual(pairs, [])


class ClassifyAndAggregateTests(unittest.TestCase):
    def test_checkout_with_matching_login_is_logged_in(self):
        # CASE 1
        pairs = [{"date": date(2026, 8, 17), "cart_token": "c1", "variant_ids": ["v1"]}]
        with mock.patch.object(cls, "_fetch_login_cart_tokens", return_value={"c1"}), \
             mock.patch.object(cls, "_iter_unique_daily_checkout_pairs", return_value=pairs), \
             mock.patch.object(cls, "_fetch_daily_unique_pair_event_counts_from_mongo", return_value={date(2026, 8, 17): 1}):
            daily, variant = cls._classify_and_aggregate(
                start_date=date(2026, 8, 17), end_date=date(2026, 8, 17),
                mongo_shop_name="tmc", tz=IST,
            )
        d = daily[date(2026, 8, 17)]
        self.assertEqual(d["total_checkout_events"], 1)
        self.assertEqual(d["total_login_events"], 1)
        self.assertEqual(d["logged_in_users"], 1)
        self.assertEqual(d["non_logged_in_users"], 0)

    def test_checkout_without_matching_login_is_non_logged_in(self):
        # CASE 2 / CASE 7
        pairs = [{"date": date(2026, 8, 17), "cart_token": "c1", "variant_ids": ["v1"]}]
        with mock.patch.object(cls, "_fetch_login_cart_tokens", return_value=set()), \
             mock.patch.object(cls, "_iter_unique_daily_checkout_pairs", return_value=pairs), \
             mock.patch.object(cls, "_fetch_daily_unique_pair_event_counts_from_mongo", return_value={}):
            daily, variant = cls._classify_and_aggregate(
                start_date=date(2026, 8, 17), end_date=date(2026, 8, 17),
                mongo_shop_name="tmc", tz=IST,
            )
        d = daily[date(2026, 8, 17)]
        self.assertEqual(d["total_checkout_events"], 1)
        self.assertEqual(d["logged_in_users"], 0)
        self.assertEqual(d["non_logged_in_users"], 1)

    def test_multiple_matching_logins_still_counts_checkout_once(self):
        # CASE 4 / CASE 6: same cart_token could have several login docs in
        # Mongo, but _fetch_login_cart_tokens already reduces that to a set
        # (membership, not count) - a checkout can only ever land in exactly
        # one of logged_in/non_logged_in regardless of how many login docs
        # share that cart_token.
        pairs = [{"date": date(2026, 8, 17), "cart_token": "c1", "variant_ids": ["v1"]}]
        with mock.patch.object(cls, "_fetch_login_cart_tokens", return_value={"c1"}), \
             mock.patch.object(cls, "_iter_unique_daily_checkout_pairs", return_value=pairs), \
             mock.patch.object(cls, "_fetch_daily_unique_pair_event_counts_from_mongo", return_value={date(2026, 8, 17): 3}):
            daily, variant = cls._classify_and_aggregate(
                start_date=date(2026, 8, 17), end_date=date(2026, 8, 17),
                mongo_shop_name="tmc", tz=IST,
            )
        d = daily[date(2026, 8, 17)]
        self.assertEqual(d["logged_in_users"], 1)
        self.assertEqual(d["non_logged_in_users"], 0)

    def test_variant_level_aggregation_and_multi_variant_explode(self):
        # CASE 6 / CASE 8 (variant A vs variant B get separate rows), plus a
        # multi-variant checkout contributing to both of its variants.
        pairs = [
            {"date": date(2026, 8, 17), "cart_token": "c1", "variant_ids": ["vA", "vB"]},
            {"date": date(2026, 8, 17), "cart_token": "c2", "variant_ids": ["vB"]},
        ]
        with mock.patch.object(cls, "_fetch_login_cart_tokens", return_value={"c1"}), \
             mock.patch.object(cls, "_iter_unique_daily_checkout_pairs", return_value=pairs), \
             mock.patch.object(cls, "_fetch_daily_unique_pair_event_counts_from_mongo", return_value={}):
            daily, variant = cls._classify_and_aggregate(
                start_date=date(2026, 8, 17), end_date=date(2026, 8, 17),
                mongo_shop_name="tmc", tz=IST,
            )
        vA = variant[(date(2026, 8, 17), "vA")]
        vB = variant[(date(2026, 8, 17), "vB")]
        self.assertEqual(vA, {"total_checkout_events": 1, "logged_in_users": 1, "non_logged_in_users": 0})
        self.assertEqual(
            vB, {"total_checkout_events": 2, "logged_in_users": 1, "non_logged_in_users": 1}
        )

    def test_no_events_for_date_yields_explicit_zero_row(self):
        # CASE 10: matches the existing convention (see
        # mongo_events._upsert_overall_summary_event_totals) of writing an
        # explicit zero row for every date in the window, not omitting rows
        # for dates with no activity.
        with mock.patch.object(cls, "_fetch_login_cart_tokens", return_value=set()), \
             mock.patch.object(cls, "_iter_unique_daily_checkout_pairs", return_value=[]), \
             mock.patch.object(cls, "_fetch_daily_unique_pair_event_counts_from_mongo", return_value={}):
            daily, variant = cls._classify_and_aggregate(
                start_date=date(2026, 8, 17), end_date=date(2026, 8, 17),
                mongo_shop_name="tmc", tz=IST,
            )
        self.assertEqual(
            daily[date(2026, 8, 17)],
            {"total_checkout_events": 0, "total_login_events": 0, "logged_in_users": 0, "non_logged_in_users": 0},
        )
        self.assertEqual(variant, {})


class ValidateInvariantTests(unittest.TestCase):
    def test_passes_when_totals_add_up(self):
        daily = {date(2026, 8, 17): {"total_checkout_events": 2, "logged_in_users": 1, "non_logged_in_users": 1}}
        variant = {(date(2026, 8, 17), "v1"): {"total_checkout_events": 1, "logged_in_users": 1, "non_logged_in_users": 0}}
        self.assertTrue(cls._validate_invariant("tmc", daily, variant))

    def test_fails_when_daily_totals_do_not_add_up(self):
        daily = {date(2026, 8, 17): {"total_checkout_events": 5, "logged_in_users": 1, "non_logged_in_users": 1}}
        self.assertFalse(cls._validate_invariant("tmc", daily, {}))

    def test_fails_when_variant_totals_do_not_add_up(self):
        variant = {(date(2026, 8, 17), "v1"): {"total_checkout_events": 5, "logged_in_users": 1, "non_logged_in_users": 1}}
        self.assertFalse(cls._validate_invariant("tmc", {}, variant))


class UpsertDailySummaryTests(unittest.TestCase):
    def test_full_replace_not_additive(self):
        cursor = _DummyCursor()
        connection = _DummyConnection()
        daily_totals = {
            date(2026, 8, 17): {
                "total_checkout_events": 3,
                "total_login_events": 2,
                "logged_in_users": 2,
                "non_logged_in_users": 1,
            }
        }

        inserted_first = cls._upsert_daily_checkout_user_summary(
            cursor, connection, date(2026, 8, 17), date(2026, 8, 17), daily_totals
        )
        inserted_second = cls._upsert_daily_checkout_user_summary(
            cursor, connection, date(2026, 8, 17), date(2026, 8, 17), daily_totals
        )

        # CASE 9 / CASE 11: re-running with the same input twice must
        # produce the same row values both times - proven here by the SQL
        # being a DELETE-then-INSERT-ON-DUPLICATE-KEY-UPDATE (full replace)
        # rather than an additive UPDATE ... = existing + new.
        self.assertEqual(inserted_first, 1)
        self.assertEqual(inserted_second, 1)
        delete_statements = [sql for sql, _ in cursor.executed if sql.strip().startswith("DELETE")]
        self.assertEqual(len(delete_statements), 2)

        insert_sql, rows = cursor.executemany_calls[0]
        self.assertIn("INSERT INTO `daily_checkout_user_summary`", insert_sql)
        self.assertIn("ON DUPLICATE KEY UPDATE", insert_sql)
        self.assertNotIn("+", insert_sql)  # no additive `col = col + VALUES(col)`
        self.assertEqual(rows, [(date(2026, 8, 17), 3, 2, 2, 1)])
        self.assertEqual(cursor.executemany_calls[0], cursor.executemany_calls[1])


class UpsertVariantSummaryTests(unittest.TestCase):
    def test_builds_expected_insert(self):
        cursor = _DummyCursor()
        connection = _DummyConnection()
        variant_totals = {
            (date(2026, 8, 17), "v1"): {"total_checkout_events": 2, "logged_in_users": 1, "non_logged_in_users": 1},
            (date(2026, 8, 17), "v2"): {"total_checkout_events": 1, "logged_in_users": 0, "non_logged_in_users": 1},
        }

        inserted = cls._upsert_daily_checkout_user_variant_summary(
            cursor, connection, date(2026, 8, 17), date(2026, 8, 17), variant_totals
        )

        self.assertEqual(inserted, 2)
        insert_sql, rows = cursor.executemany_calls[0]
        self.assertIn("INSERT INTO `daily_checkout_user_variant_summary`", insert_sql)
        self.assertIn("ON DUPLICATE KEY UPDATE", insert_sql)
        self.assertNotIn("total_login_events", insert_sql)  # explicitly excluded from the variant table
        self.assertEqual(
            rows,
            [
                (date(2026, 8, 17), "v1", 2, 1, 1),
                (date(2026, 8, 17), "v2", 1, 0, 1),
            ],
        )


if __name__ == "__main__":
    unittest.main()
