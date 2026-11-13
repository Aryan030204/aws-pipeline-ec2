import sys
import types
import unittest
from datetime import date, datetime, timedelta, timezone
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
    """Fakes a dict-cursor's SELECT ... FROM raw_mongo_events - rows are
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
    def test_queries_raw_mongo_events_with_date_window_and_event_type(self):
        cursor = _FakeCursor([])
        mes._fetch_candidate_rows(cursor, date(2026, 8, 17), date(2026, 8, 17), IST, event_type="buy_now")

        sql, params = cursor.executed[0]
        self.assertIn("FROM raw_mongo_events", sql)
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


class _ChunkedCursor:
    """Like a mysql cursor that supports fetchmany; records how it was read."""

    def __init__(self, rows):
        self.rows = list(rows)
        self.pos = 0
        self.fetchmany_sizes = []
        self.fetchall_called = False
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchmany(self, size):
        self.fetchmany_sizes.append(size)
        chunk = self.rows[self.pos:self.pos + size]
        self.pos += size
        return chunk

    def fetchall(self):
        self.fetchall_called = True
        return self.rows


class StreamedFetchTests(unittest.TestCase):
    def _rows(self, n):
        base = datetime(2026, 8, 17, 10, 0, tzinfo=timezone.utc)
        return [
            ("buy_now", f"s{i % 40}", f"c{i % 40}", f"v{i % 3}", (base + timedelta(minutes=i % 600)).replace(tzinfo=None))
            for i in range(n)
        ]

    def test_rows_are_read_in_chunks_not_with_fetchall(self):
        cursor = _ChunkedCursor(self._rows(12000))
        with mock.patch.object(mes, "CANDIDATE_FETCH_CHUNK", 5000):
            streamed = list(mes._fetch_candidate_rows(cursor, date(2026, 8, 17), date(2026, 8, 17), IST, stream=True))

        self.assertEqual(len(streamed), 12000)
        self.assertFalse(cursor.fetchall_called)
        self.assertEqual(cursor.fetchmany_sizes, [5000, 5000, 5000, 5000])  # last call returns the empty page

    def test_default_call_still_returns_a_list(self):
        cursor = _ChunkedCursor(self._rows(10))
        rows = mes._fetch_candidate_rows(cursor, date(2026, 8, 17), date(2026, 8, 17), IST)
        self.assertIsInstance(rows, list)
        self.assertEqual(len(rows), 10)

    def test_cursor_without_fetchmany_falls_back_to_fetchall(self):
        cursor = _FakeCursor(self._rows(7))
        rows = list(mes._fetch_candidate_rows(cursor, date(2026, 8, 17), date(2026, 8, 17), IST, stream=True))
        self.assertEqual(len(rows), 7)

    def test_pair_dedup_matches_the_previous_algorithm(self):
        rows = self._rows(3000)
        cursor = _ChunkedCursor(rows)
        got = mes._iter_unique_daily_pair_events_from_sql(cursor, date(2026, 8, 17), date(2026, 8, 17), "buy_now", IST)

        # oracle: the old "collect every event per pair, keep pairs seen once" rule
        pairs = {}
        for event_type, session_id, cart_token, variant, created in rows:
            local_date = created.replace(tzinfo=timezone.utc).astimezone(IST).date()
            if local_date != date(2026, 8, 17):
                continue
            pairs.setdefault((local_date, session_id, cart_token), []).append(variant)
        expected = sorted((k[0], tuple(v)) for k, v in pairs.items() if len(v) == 1)

        self.assertEqual(sorted((e["date"], tuple(e["variant_ids"])) for e in got), [(d, tuple(mes._normalize_variant_ids(v[0]))) for d, v in expected])

    def test_all_types_dedup_counts_a_pair_once_per_event_type(self):
        base = datetime(2026, 8, 17, 10, 0)
        rows = [
            ("buy_now", "s1", "c1", "v1", base),
            ("login", "s1", "c1", "v1", base),
            ("buy_now", "s2", "c2", "v1", base),
            ("buy_now", "s2", "c2", "v1", base + timedelta(minutes=1)),  # repeat -> dropped
        ]
        got = mes._iter_unique_daily_pair_events_all_types_from_sql(
            _ChunkedCursor(rows), date(2026, 8, 17), date(2026, 8, 17), IST
        )
        self.assertEqual(sorted(e["event_column"] for e in got), ["buy_now_count", "login_count"])


class _TypeFilteringCursor:
    """Behaves like the real table for the one query shape used here: returns only rows whose
    event_type matches the event_type = %s / IN (...) parameters, and counts every execute()."""

    def __init__(self, rows, fail_first=False):
        self._all = rows
        self._result = []
        self.queries = []
        self._fail_first = fail_first

    def execute(self, sql, params=None):
        self.queries.append((sql, params))
        if self._fail_first:
            self._fail_first = False
            raise RuntimeError("db hiccup")
        types = set(params[2:]) if params and len(params) > 2 else None
        self._result = [r for r in self._all if types is None or r[0] in types]

    def fetchmany(self, size):
        chunk, self._result = self._result[:size], self._result[size:]
        return chunk

    def fetchall(self):
        result, self._result = self._result, []
        return result


def _rollup_inputs():
    """A busy day: repeats inside/outside the 30-minute window, singles, several variants, and
    event types the rollups must not see (login, TAB_Click)."""
    base = datetime(2026, 8, 17, 4, 0)  # UTC; 09:30 IST
    rows = []
    for i in range(40):
        t = base + timedelta(minutes=7 * i)
        rows.append(("checkout_initiated", f"s{i % 9}", f"c{i % 7}", f"{100 + i % 4}", t))
        if i % 3 == 0:
            rows.append(("buy_now", f"s{i % 5}", f"c{i % 4}", f"{200 + i % 3}", t))
        if i % 4 == 0:
            rows.append(("login", f"s{i % 9}", f"c{i % 7}", None, t))
        if i % 5 == 0:
            rows.append(("TAB_Click", f"s{i}", f"c{i}", None, t))
    return rows


def _run_all_rollup_reads(cursor):
    """The same six reads the five rollups make per cycle (events_summary_worker order)."""
    d = date(2026, 8, 17)
    return {
        "ci_hourly": mes._fetch_event_hourly_counts_from_sql(cursor, d, d, "checkout_initiated", "ci_events", IST),
        "ci_variant": mes._fetch_event_variant_counts_from_sql(cursor, d, d, "checkout_initiated", "n", IST),
        "bn_hourly": mes._fetch_event_hourly_counts_from_sql(cursor, d, d, "buy_now", "buy_now_events", IST),
        "bn_variant": mes._fetch_event_variant_counts_from_sql(cursor, d, d, "buy_now", "n", IST),
        "ci_total": mes._fetch_daily_unique_pair_event_counts_from_sql(cursor, d, d, "checkout_initiated", IST),
        "bn_total": mes._fetch_daily_unique_pair_event_counts_from_sql(cursor, d, d, "buy_now", IST),
    }


class CandidateRowCacheTests(unittest.TestCase):
    D = date(2026, 8, 17)

    def test_without_the_cache_every_rollup_read_is_its_own_query(self):
        cursor = _TypeFilteringCursor(_rollup_inputs())
        _run_all_rollup_reads(cursor)
        self.assertEqual(len(cursor.queries), 6)

    def test_with_the_cache_the_window_is_read_once_for_all_rollups(self):
        cursor = _TypeFilteringCursor(_rollup_inputs())
        with mes.cached_candidate_rows(cursor, self.D, self.D, IST):
            _run_all_rollup_reads(cursor)

        self.assertEqual(len(cursor.queries), 1)
        sql, params = cursor.queries[0]
        self.assertIn("FROM raw_mongo_events", sql)
        self.assertIn("event_type IN (%s, %s)", sql)
        self.assertEqual(tuple(params[2:]), ("checkout_initiated", "buy_now"))

    def test_cached_results_are_identical_to_the_uncached_sql_path(self):
        rows = _rollup_inputs()
        plain = _run_all_rollup_reads(_TypeFilteringCursor(rows))
        with mes.cached_candidate_rows(_TypeFilteringCursor(rows), self.D, self.D, IST):
            cached = _run_all_rollup_reads(_TypeFilteringCursor(rows))  # reads must not hit this cursor

        self.assertEqual(cached, plain)
        self.assertTrue(any(plain.values()))  # the fixture really exercises the algorithms

    def test_cache_only_holds_rollup_event_types(self):
        cursor = _TypeFilteringCursor(_rollup_inputs())
        with mes.cached_candidate_rows(cursor, self.D, self.D, IST) as cache:
            self.assertEqual(
                {t for t in ("checkout_initiated", "buy_now") if cache.rows(t)},
                {"checkout_initiated", "buy_now"},
            )
            with self.assertRaises(KeyError):
                cache.rows("login")

    def test_other_event_types_and_other_windows_still_query_sql(self):
        cursor = _TypeFilteringCursor(_rollup_inputs())
        with mes.cached_candidate_rows(cursor, self.D, self.D, IST):
            logins = mes._fetch_candidate_rows(cursor, self.D, self.D, IST, event_type="login")
            other_day = mes._fetch_candidate_rows(
                cursor, self.D, self.D + timedelta(days=1), IST, event_type="buy_now"
            )
            all_types = mes._fetch_candidate_rows(cursor, self.D, self.D, IST, event_type=None)

        self.assertTrue(logins and all(r["event_type"] == "login" for r in logins))
        self.assertTrue(other_day)
        self.assertTrue(any(r["event_type"] == "TAB_Click" for r in all_types))
        self.assertEqual(len(cursor.queries), 1 + 3)  # the cache load + three fall-throughs

    def test_cache_is_gone_after_the_block_and_restored_when_nested(self):
        cursor = _TypeFilteringCursor(_rollup_inputs())
        with mes.cached_candidate_rows(cursor, self.D, self.D, IST) as outer:
            with mes.cached_candidate_rows(cursor, self.D, self.D, IST) as inner:
                self.assertIsNot(outer, inner)
            self.assertIs(mes._active_cache(), outer)
        self.assertIsNone(mes._active_cache())

        before = len(cursor.queries)
        mes._fetch_candidate_rows(cursor, self.D, self.D, IST, event_type="buy_now")
        self.assertEqual(len(cursor.queries), before + 1)

    def test_over_the_row_cap_nothing_is_cached_and_rollups_stream_individually(self):
        cursor = _TypeFilteringCursor(_rollup_inputs())
        with mock.patch.object(mes, "CANDIDATE_CACHE_MAX_ROWS", 5):
            with mes.cached_candidate_rows(cursor, self.D, self.D, IST) as cache:
                self.assertIsNone(cache)
                results = _run_all_rollup_reads(cursor)

        self.assertEqual(len(cursor.queries), 1 + 6)  # the aborted load + six normal reads
        self.assertEqual(results, _run_all_rollup_reads(_TypeFilteringCursor(_rollup_inputs())))

    def test_a_failing_load_falls_back_to_normal_queries_instead_of_raising(self):
        cursor = _TypeFilteringCursor(_rollup_inputs(), fail_first=True)
        with mes.cached_candidate_rows(cursor, self.D, self.D, IST) as cache:
            self.assertIsNone(cache)
            results = _run_all_rollup_reads(cursor)
        self.assertEqual(results, _run_all_rollup_reads(_TypeFilteringCursor(_rollup_inputs())))

    def test_dict_cursor_rows_are_cached_too(self):
        rows = [
            {"event_type": "buy_now", "session_id": "s1", "cart_token": "c1",
             "variantId": "9", "created_at_utc": datetime(2026, 8, 17, 4, 0)},
        ]

        class _DictCursor(_TypeFilteringCursor):
            def execute(self, sql, params=None):
                self.queries.append((sql, params))
                self._result = list(rows)

        cursor = _DictCursor([])
        with mes.cached_candidate_rows(cursor, self.D, self.D, IST):
            got = mes._fetch_candidate_rows(cursor, self.D, self.D, IST, event_type="buy_now")
        self.assertEqual(got[0]["session_id"], "s1")
        self.assertEqual(got[0]["variantId"], "9")
        self.assertEqual(len(cursor.queries), 1)
