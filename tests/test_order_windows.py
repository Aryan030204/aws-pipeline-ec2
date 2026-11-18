"""Bounded order windows (memory): the tiling of a fetch window, the exact-range replace per sub-window, watermark safety
for incomplete fetches, the once-only products-sold update, insert chunk sizing and backfill concurrency.

Everything runs against in-memory SQLite and fake Shopify/DB helpers: no network, no real database. There are NO memory
or RSS assertions here on purpose (allocation behaviour is environment dependent); memory numbers live in
scripts/memory_benchmark.py."""
import asyncio
import os
import unittest
from datetime import datetime, timedelta
from unittest import mock
from urllib.parse import unquote
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

# Safe import order (see other tests): aws_background first.
import aws_background  # noqa: F401
from pipeline import mem_probe, orchestration
from pipeline import shopify_orders as so

IST = ZoneInfo("Asia/Kolkata")
NY = ZoneInfo("America/New_York")
BRAND = 7
SEC = timedelta(seconds=1)


def at(day, hour=0, minute=0, second=0, micro=0, month=11, tz=IST):
    return datetime(2026, month, day, hour, minute, second, micro, tzinfo=tz)


def to_utc(fetch_bound):
    return datetime.fromisoformat(unquote(fetch_bound)).astimezone(ZoneInfo("UTC"))


# ---------------------------------------------------------------------------------------------------
# Tiling
# ---------------------------------------------------------------------------------------------------
class SplitOrderWindowTests(unittest.TestCase):
    def test_tiles_cover_the_window_exactly_with_shared_boundaries(self):
        start, end = at(1, 0, 0, 0), at(30, 23, 59, 59)
        tiles = so.split_order_window(start, end, 24)
        self.assertEqual(len(tiles), 30)
        self.assertEqual(tiles[0].start, start)
        self.assertEqual(tiles[-1].end_exclusive, end + SEC)  # the inclusive end becomes an exclusive bound
        for previous, following in zip(tiles, tiles[1:]):
            self.assertEqual(previous.end_exclusive, following.start)  # no gap, no overlap, by construction
        self.assertTrue(all(t.start < t.end_exclusive for t in tiles))
        self.assertEqual(sum((t.end_exclusive - t.start for t in tiles), timedelta()), (end + SEC) - start)

    def test_the_last_tile_is_the_remainder(self):
        tiles = so.split_order_window(at(1, 0), at(2, 0, 59, 59), 6)  # 25 hours
        self.assertEqual([t.end_exclusive - t.start for t in tiles], [timedelta(hours=6)] * 4 + [timedelta(hours=1)])

    def test_a_window_not_longer_than_the_span_is_one_tile_identical_to_the_old_single_request(self):
        start, end = at(13, 10, 0, 0), at(13, 10, 9, 59)
        (tile,) = so.split_order_window(start, end, 24)
        self.assertEqual(so.order_window_fetch_bounds(tile, IST), (so.convert_to_desired_format(start, IST), so.convert_to_desired_format(end, IST)))
        # exactly one span (inclusive end) is also a single tile
        self.assertEqual(len(so.split_order_window(at(13, 0), at(13, 23, 59, 59), 24)), 1)
        self.assertEqual(len(so.split_order_window(at(13, 0), at(14, 0, 0, 0), 24)), 2)  # one second more -> two tiles

    def test_unbounded_spans_and_an_inverted_window_are_one_tile(self):
        for span in (0, -1, None):
            self.assertEqual(len(so.split_order_window(at(1), at(30, 23, 59, 59), span)), 1, span)
        (tile,) = so.split_order_window(at(13, 12), at(13, 11), 24)  # start after end: the old code just sent it as is
        self.assertEqual(tile.start, at(13, 12))

    def test_consecutive_fetch_bounds_differ_by_exactly_one_second(self):
        tiles = so.split_order_window(at(1), at(10, 23, 59, 59), 6)
        bounds = [so.order_window_fetch_bounds(t, IST) for t in tiles]
        self.assertEqual(to_utc(bounds[0][0]), at(1).astimezone(ZoneInfo("UTC")))
        self.assertEqual(to_utc(bounds[-1][1]), at(10, 23, 59, 59).astimezone(ZoneInfo("UTC")))
        for (_, prev_max), (next_min, _) in zip(bounds, bounds[1:]):
            self.assertEqual(to_utc(next_min) - to_utc(prev_max), SEC)

    def test_a_dst_timezone_has_no_gap_or_overlap_across_spring_forward_and_fall_back(self):
        for start, end in ((at(6, 0, month=3, tz=NY), at(10, 23, 59, 59, month=3, tz=NY)),   # 2026-03-08 spring forward
                           (at(30, 0, month=10, tz=NY), at(3, 23, 59, 59, month=11, tz=NY))):  # 2026-11-01 fall back
            for span in (24, 6, 5):
                with self.subTest(start=start, span=span):
                    tiles = so.split_order_window(start, end, span)
                    bounds = [so.order_window_fetch_bounds(t, NY) for t in tiles]
                    self.assertEqual(to_utc(bounds[0][0]), start.astimezone(ZoneInfo("UTC")))
                    self.assertEqual(to_utc(bounds[-1][1]), end.astimezone(ZoneInfo("UTC")))
                    for (_, prev_max), (next_min, _) in zip(bounds, bounds[1:]):
                        self.assertEqual(to_utc(next_min) - to_utc(prev_max), SEC)

    def test_span_env_parsing(self):
        self.assertEqual(so.order_window_span_hours({}), 24)
        self.assertEqual(so.order_window_span_hours({"ORDER_FETCH_WINDOW_HOURS": "6"}), 6)
        self.assertEqual(so.order_window_span_hours({"ORDER_FETCH_WINDOW_HOURS": "0"}), 0)
        self.assertEqual(so.order_window_span_hours({"ORDER_FETCH_WINDOW_HOURS": "0.5"}), 0.5)
        self.assertEqual(so.order_window_span_hours({"ORDER_FETCH_WINDOW_HOURS": "soon"}), 24)
        self.assertEqual(so.order_window_span_hours({"ORDER_FETCH_WINDOW_HOURS": "  "}), 24)


# ---------------------------------------------------------------------------------------------------
# Insert chunk sizing
# ---------------------------------------------------------------------------------------------------
class ChunkSizingTests(unittest.TestCase):
    def test_default_chunk_for_the_real_85_column_frame_is_about_235_rows(self):
        rows = so.compute_insert_chunk_rows(85, 1000, 20000)
        self.assertEqual(rows, 235)
        self.assertLess(rows * 85, 65535)

    def test_never_exceeds_batch_size_the_row_ceiling_or_the_parameter_cap(self):
        for columns in (1, 5, 85, 90, 500):
            for batch in (1, 50, 1000, 5000):
                for cap in (1000, 20000, 65535):
                    rows = so.compute_insert_chunk_rows(columns, batch, cap)
                    self.assertGreaterEqual(rows, 1)
                    self.assertLessEqual(rows, batch)
                    self.assertLessEqual(rows, so.MAX_INSERT_ROWS)
                    self.assertTrue(rows * columns <= cap or rows == 1, (columns, batch, cap, rows))

    def test_small_frames_are_not_fragmented(self):
        self.assertGreaterEqual(so.compute_insert_chunk_rows(85, 1000, 20000), 100)  # 100 rows -> one chunk
        self.assertEqual(so.compute_insert_chunk_rows(3, 1000, 20000), 1000)  # narrow frames keep the full batch

    def test_a_row_wider_than_the_cap_still_gets_one_row(self):
        self.assertEqual(so.compute_insert_chunk_rows(100000, 1000, 20000), 1)

    def test_env_overrides_and_clamping(self):
        self.assertEqual(so.insert_max_params({}), 20000)
        self.assertEqual(so.insert_max_params({"ORDERS_INSERT_MAX_PARAMS": "30000"}), 30000)
        self.assertEqual(so.insert_max_params({"ORDERS_INSERT_MAX_PARAMS": "999999"}), 65535)
        self.assertEqual(so.insert_max_params({"ORDERS_INSERT_MAX_PARAMS": "x"}), 20000)
        with mock.patch.dict(os.environ, {"ORDERS_INSERT_MAX_PARAMS": "8500"}):
            self.assertEqual(so.compute_insert_chunk_rows(85, 1000), 100)


# ---------------------------------------------------------------------------------------------------
# Brand concurrency
# ---------------------------------------------------------------------------------------------------
class ConcurrencyTests(unittest.TestCase):
    def test_normal_runs_use_max_concurrent_brands_with_default_3(self):
        self.assertEqual(orchestration.resolve_brand_concurrency(12, False, {}), (3, "MAX_CONCURRENT_BRANDS"))
        self.assertEqual(orchestration.resolve_brand_concurrency(12, False, {"MAX_CONCURRENT_BRANDS": "5"})[0], 5)

    def test_backfill_runs_use_their_own_setting_default_1(self):
        self.assertEqual(orchestration.resolve_brand_concurrency(12, True, {}), (1, "BACKFILL_MAX_CONCURRENT_BRANDS"))
        env = {"BACKFILL_MAX_CONCURRENT_BRANDS": "2", "MAX_CONCURRENT_BRANDS": "5"}
        self.assertEqual(orchestration.resolve_brand_concurrency(12, True, env)[0], 2)  # not MAX_CONCURRENT_BRANDS

    def test_clamped_between_one_and_the_number_of_brands_and_tolerant_of_garbage(self):
        self.assertEqual(orchestration.resolve_brand_concurrency(2, False, {"MAX_CONCURRENT_BRANDS": "9"})[0], 2)
        self.assertEqual(orchestration.resolve_brand_concurrency(5, False, {"MAX_CONCURRENT_BRANDS": "0"})[0], 1)
        self.assertEqual(orchestration.resolve_brand_concurrency(5, False, {"MAX_CONCURRENT_BRANDS": "-3"})[0], 1)
        self.assertEqual(orchestration.resolve_brand_concurrency(5, False, {"MAX_CONCURRENT_BRANDS": "many"})[0], 3)
        self.assertEqual(orchestration.resolve_brand_concurrency(0, False, {})[0], 1)

    def test_the_run_uses_the_resolver_and_a_failing_brand_thread_still_surfaces(self):
        # _execute_pipeline_run feeds the resolver's value to ThreadPoolExecutor and keeps its existing as_completed
        # handling: an exception in a worker thread is turned into a failed metrics row, not swallowed.
        import inspect

        source = inspect.getsource(orchestration._execute_pipeline_run)
        self.assertIn("resolve_brand_concurrency(len(target_indices), backfill_run)", source)
        self.assertIn("ThreadPoolExecutor(max_workers=max_workers)", source)
        self.assertIn("fut.result()", source)
        self.assertIn("success=False, error_message=str(e)", source)


# ---------------------------------------------------------------------------------------------------
# fetch_orders status flag
# ---------------------------------------------------------------------------------------------------
class _Resp:
    def __init__(self, status=200, orders=None, link=""):
        self.status, self._orders, self.headers = status, orders or [], {"Link": link}

    async def json(self):
        return {"orders": self._orders}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _Session:
    def __init__(self, script):
        self.script = list(script)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def get(self, url, headers=None):
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


NEXT = '<https://shop/next>; rel="next"'


def run_fetch(script, strict=False, status=None):
    async def no_sleep(_):
        return None

    with mock.patch.object(so.aiohttp, "ClientSession", return_value=_Session(script)), \
         mock.patch.object(so.aiohttp, "TCPConnector"), mock.patch.object(so.asyncio, "sleep", no_sleep):
        kwargs = {} if status is None else {"status": status}
        return asyncio.run(so.fetch_orders_async("https://shop/admin", "tok", "s", "e", "created_at_min", strict=strict, **kwargs))


class FetchStatusTests(unittest.TestCase):
    def test_a_complete_fetch_reports_complete(self):
        status = {}
        run_fetch([_Resp(200, [{"id": 1}], NEXT), _Resp(200, [{"id": 2}])], status=status)
        self.assertEqual(status, {"complete": True})

    def test_a_non_strict_early_end_is_reported_and_still_returns_the_pages_it_got(self):
        for failure in (_Resp(500), ConnectionError("reset")):
            status = {}
            orders = run_fetch([_Resp(200, [{"id": 1}], NEXT), failure], status=status)
            self.assertEqual(status, {"complete": False})
            self.assertEqual([o["id"] for o in orders], [1])  # the caller (not the fetch) decides to drop them

    def test_a_strict_failure_still_raises_and_reports_incomplete(self):
        status = {}
        with self.assertRaises(so.OrderFetchIncomplete):
            run_fetch([_Resp(200, [{"id": 1}], NEXT), _Resp(500)], strict=True, status=status)
        self.assertEqual(status, {"complete": False})

    def test_status_is_optional(self):
        self.assertEqual([o["id"] for o in run_fetch([_Resp(200, [{"id": 1}])])], [1])


# ---------------------------------------------------------------------------------------------------
# Stream harness: real _process_order_stream + real load/transform-shape on SQLite, fake Shopify
# ---------------------------------------------------------------------------------------------------
def order(order_id, created, updated=None, items=None, financial_status="paid"):
    return {
        "id": order_id,
        "created_at": created.isoformat(),
        "updated_at": (updated or created).isoformat(),
        "financial_status": financial_status,
        "refunds": [],
        "cancelled_at": None,
        "line_items": items or [],
    }


def fake_transform(orders, _mapping, _tz):
    def wall(iso):
        return datetime.fromisoformat(iso).replace(tzinfo=None)  # store wall-clock, whole seconds (format_datetime)

    created = [wall(o["created_at"]) for o in orders]
    updated = [wall(o["updated_at"]) for o in orders]
    return pd.DataFrame(
        {
            "order_id": [str(o["id"]) for o in orders],
            "created_at": pd.to_datetime(created),
            "updated_at": pd.to_datetime(updated),
            "created_date": [c.strftime("%Y-%m-%d") for c in created],
            "created_time": [c.strftime("%H:%M:%S") for c in created],
            "total_price": [100.0] * len(orders),
        }
    )


class Stream:
    """Drives orchestration._process_order_stream against SQLite with a scripted fake Shopify."""

    def __init__(self, dataset, *, backfill=None, span="24", inventory=False, fail=None):
        self.engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
        self.dataset = dataset  # list of order dicts (the Shopify side)
        self.backfill = backfill  # (start, end) aware, or None for an incremental run
        self.span = span
        self.inventory = inventory
        self.fail = fail or {}  # {call_number: "incomplete" | "strict"}
        self.calls = []  # (min_bound, max_bound) of every fetch
        self.returns_seen = []
        self.errors = []
        self.units_mock = mock.Mock()
        self.fetch_end_limit = None

    # -- the fake Shopify ------------------------------------------------------------------------
    def fetch(self, _api, _token, mn, mx, field, strict=False, status=None):
        self.calls.append((mn, mx))
        number = len(self.calls)
        lo, hi = datetime.fromisoformat(unquote(mn)), datetime.fromisoformat(unquote(mx))
        key = "created_at" if field == "created_at_min" else "updated_at"
        hits = [o for o in self.dataset if lo <= datetime.fromisoformat(o[key]) <= hi]
        mode = self.fail.get(number)
        if mode == "strict":
            raise so.OrderFetchIncomplete("Shopify returned HTTP 500 after 2 order(s)")
        if mode == "incomplete":
            if status is not None:
                status["complete"] = False
            return hits[: max(1, len(hits) // 2)]  # the pages collected before the failure
        if status is not None:
            status["complete"] = True
        return list(hits)

    def last_order(self, _brand, table, cursor=None):
        col = "updated_at" if "update" in table else "created_at"
        with self.engine.connect() as conn:
            try:
                value = conn.execute(text(f"SELECT MAX({col}) FROM {table}")).scalar()
            except Exception:
                return None
        if value is None:
            return None
        parsed = pd.Timestamp(value).to_pydatetime()
        return {"order_id": "x", "created_at": parsed, "updated_at": parsed}

    # -- plumbing --------------------------------------------------------------------------------
    def __enter__(self):
        brand_window = self.backfill or (None, None)
        self.patches = [
            mock.patch.dict(so.sqlalchemy_engines, {BRAND: self.engine}),
            mock.patch.dict(os.environ, {"ORDER_FETCH_WINDOW_HOURS": self.span}),
            mock.patch.object(aws_background, "is_backfill_active_for", return_value=self.backfill is not None),
            mock.patch.object(aws_background, "get_backfill_window_for_brand", return_value=brand_window),
            mock.patch.object(aws_background, "INVENTORY_PIPELINE_SWITCH", self.inventory, create=True),
            mock.patch.object(orchestration, "fetch_orders", self.fetch),
            mock.patch.object(orchestration, "transform_orders_to_df_optimized", fake_transform),
            mock.patch.object(orchestration, "get_last_order", self.last_order),
            mock.patch.object(orchestration, "upsert_dashboard_order_discount_fact_from_orders", return_value=set()),
            mock.patch.object(orchestration, "upsert_returns_fact_from_orders", side_effect=self._returns),
            mock.patch.object(orchestration, "upsert_products_sold_units_daily_from_orders", self.units_mock),
        ]
        for p in self.patches:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in reversed(self.patches):
            p.stop()
        return False

    def _returns(self, brand_index, orders, cursor=None, connection=None):
        self.returns_seen.extend(str(o["id"]) for o in orders)
        return set()

    def seed(self, table, rows):
        """rows: [(order_id, naive datetime)] inserted straight into the table (creating it with the real schema)."""
        df = pd.DataFrame(
            {
                "order_id": [r[0] for r in rows],
                "created_at": pd.to_datetime([r[1] for r in rows]),
                "updated_at": pd.to_datetime([r[1] for r in rows]),
                "created_date": [r[1].strftime("%Y-%m-%d") for r in rows],
                "created_time": [r[1].strftime("%H:%M:%S") for r in rows],
                "total_price": [1.0] * len(rows),
            }
        )
        so.load_data_to_sql_optimized(df, BRAND, "B", table, 1000, replace_window=None)

    def run(self, process_type="NEW"):
        process = (
            {"type": "NEW", "date_field": "created_at_min", "table": "shopify_orders"}
            if process_type == "NEW"
            else {"type": "UPDATED", "date_field": "updated_at_min", "table": "shopify_orders_update"}
        )
        errors = []
        totals = orchestration._process_order_stream(
            process, brand_index=BRAND, brand_name="B", tz=IST, api_base_url="https://shop", access_token="t",
            shop_name="shop", app_id_mapping={}, cursor=None, connection=None, errors=errors,
        )
        self.errors = errors
        return totals

    def rows(self, table="shopify_orders"):
        with self.engine.connect() as conn:
            return sorted((r[0], pd.Timestamp(r[1]).to_pydatetime()) for r in conn.execute(text(f"SELECT order_id, created_at FROM {table}")))

    def ids(self, table="shopify_orders"):
        return [r[0] for r in self.rows(table)]


def naive(dt):
    return dt.replace(tzinfo=None)


# ---------------------------------------------------------------------------------------------------
# Exact-range replace per sub-window (backfill)
# ---------------------------------------------------------------------------------------------------
class BackfillSubWindowTests(unittest.TestCase):
    START, END = at(10, 0, 0, 0), at(13, 23, 59, 59)

    def _seed_rows(self):
        rows = [("old-start", naive(self.START)), ("old-end", naive(self.END)),
                ("outside-before", naive(self.START - SEC)), ("outside-after", naive(self.END + SEC))]
        for day in (11, 12, 13):
            b = naive(at(day))
            rows += [(f"old-{day}-m1", b - SEC), (f"old-{day}-b", b), (f"old-{day}-sub", b - timedelta(microseconds=500000))]
        return rows

    def _shopify(self):
        """Shopify's side: a fresh version of every whole-second in-range row."""
        times = [self.START, self.END] + [at(d) - SEC for d in (11, 12, 13)] + [at(d) for d in (11, 12, 13)]
        return [order(f"new-{t.strftime('%d-%H%M%S')}", t) for t in times]

    def _run(self, span):
        with Stream(self._shopify(), backfill=(self.START, self.END), span=span) as s:
            s.seed("shopify_orders", self._seed_rows())
            s.run()
            return s.ids(), len(s.calls)

    def test_every_boundary_row_is_replaced_exactly_once_and_the_result_equals_the_unsplit_backfill(self):
        split_ids, calls = self._run("24")
        whole_ids, whole_calls = self._run("0")
        self.assertEqual(calls, 4)
        self.assertEqual(whole_calls, 1)
        self.assertEqual(split_ids, whole_ids)  # sub-windows change HOW it is fetched, not WHAT ends up in the table
        # rows outside [START, END] are untouched; every in-range old row (incl. the sub-second ones) is gone; no dupes
        self.assertIn("outside-before", split_ids)
        self.assertIn("outside-after", split_ids)
        self.assertFalse([i for i in split_ids if i.startswith("old-")])
        self.assertEqual(len(split_ids), len(set(split_ids)))
        self.assertEqual(len([i for i in split_ids if i.startswith("new-")]), 8)

    def test_an_empty_sub_window_deletes_nothing_so_its_stale_rows_stay(self):
        # Documented behaviour: a sub-window Shopify returns nothing for is not replaced (the 'nothing in hand = nothing
        # deleted' rule, per sub-window). The unsplit backfill WOULD have removed them because other days had orders.
        shopify = [o for o in self._shopify() if not o["id"].startswith("new-12-")]  # nothing at all on Nov 12
        stale = [("stale-12", naive(at(12, 12)))]
        with Stream(shopify, backfill=(self.START, self.END), span="24") as s:
            s.seed("shopify_orders", self._seed_rows() + stale)
            s.run()
            self.assertIn("stale-12", s.ids())
        with Stream(shopify, backfill=(self.START, self.END), span="0") as s:
            s.seed("shopify_orders", self._seed_rows() + stale)
            s.run()
            self.assertNotIn("stale-12", s.ids())

    def test_a_strict_failure_mid_backfill_leaves_earlier_windows_replaced_and_later_ones_untouched_and_a_rerun_heals(self):
        with Stream(self._shopify(), backfill=(self.START, self.END), span="24", fail={3: "strict"}) as s:
            s.seed("shopify_orders", self._seed_rows())
            with self.assertRaises(so.OrderFetchIncomplete):
                s.run()
            ids = s.ids()
            self.assertTrue([i for i in ids if i.startswith("new-")])  # windows 1-2 were replaced...
            self.assertIn("old-13-b", ids)  # ...window 3 (Nov 12) and 4 (Nov 13) still hold the old rows
            self.assertIn("old-end", ids)
            self.assertEqual(len(s.calls), 3)
            s.fail = {}
            s.run()  # the re-run replaces everything
            first = s.ids()
            s.run()  # and is idempotent
            self.assertEqual(s.ids(), first)
            self.assertFalse([i for i in first if i.startswith("old-")])
            self.assertEqual(len(first), len(set(first)))

    def test_an_incomplete_non_strict_window_never_reaches_a_backfill_either(self):
        # (a backfill is always strict; this documents that the partial list is not loaded even if status says so)
        with Stream(self._shopify(), backfill=(self.START, self.END), span="24", fail={2: "incomplete"}) as s:
            s.seed("shopify_orders", self._seed_rows())
            s.run()
            self.assertEqual(len(s.calls), 2)
            self.assertIn("old-11-b", s.ids())  # window 2's rows were not replaced by the partial pages
            self.assertTrue(s.errors)


# ---------------------------------------------------------------------------------------------------
# Watermark safety (incremental)
# ---------------------------------------------------------------------------------------------------
class WatermarkSafetyTests(unittest.TestCase):
    def _dataset(self, watermark):
        """One order every 90 minutes after the watermark, up to ~10 minutes ago."""
        now = datetime.now(IST).replace(microsecond=0)
        out, t, n = [], watermark + timedelta(minutes=90), 0
        while t < now - timedelta(minutes=10):
            out.append(order(f"s{n:03d}", t, updated=t))
            t, n = t + timedelta(minutes=90), n + 1
        return out

    def _check(self, process_type):
        watermark = (datetime.now(IST) - timedelta(hours=26)).replace(microsecond=0)
        dataset = self._dataset(watermark)
        table = "shopify_orders" if process_type == "NEW" else "shopify_orders_update"
        with Stream(dataset, span="6", fail={3: "incomplete"}) as s:
            s.seed(table, [("seed", naive(watermark))])
            s.run(process_type)
            self.assertEqual(len(s.calls), 3)  # windows 4 and 5 were never fetched
            loaded = [i for i in s.ids(table) if i != "seed"]
            first_two_windows_end = naive(watermark) + timedelta(hours=12)
            self.assertTrue(loaded)
            ok = [(i, t) for i, t in s.rows(table) if i != "seed"]
            self.assertTrue(all(t <= first_two_windows_end for _, t in ok), "an incomplete window's pages must not be loaded")
            self.assertLessEqual(s.last_order(BRAND, table)[("updated_at" if process_type == "UPDATED" else "created_at")], first_two_windows_end)
            self.assertTrue(s.errors and "incomplete" in s.errors[0])
            # next run: the fetch works; it must resume from the watermark and complete the data with no duplicates
            column = "updated_at" if process_type == "UPDATED" else "created_at"
            watermark_after_failure = s.last_order(BRAND, table)[column]
            s.fail, s.calls = {}, []
            s.run(process_type)
            self.assertEqual(sorted(s.ids(table)), sorted(["seed"] + [o["id"] for o in dataset]))
            self.assertEqual(len(s.ids(table)), len(set(s.ids(table))))
            resumed_from = datetime.fromisoformat(unquote(s.calls[0][0]))
            self.assertEqual(resumed_from, (watermark_after_failure + SEC).replace(tzinfo=IST))  # watermark + 1s, as ever

    def test_an_incomplete_new_orders_window_cannot_advance_the_watermark_and_the_next_run_retries_it(self):
        self._check("NEW")

    def test_an_incomplete_updated_orders_window_cannot_advance_the_watermark_and_the_next_run_retries_it(self):
        self._check("UPDATED")

    def test_a_short_incremental_window_is_one_request(self):
        watermark = (datetime.now(IST) - timedelta(minutes=30)).replace(microsecond=0)
        with Stream([order("a", watermark + timedelta(minutes=5))], span="24") as s:
            s.seed("shopify_orders", [("seed", naive(watermark))])
            totals = s.run()
            self.assertEqual(len(s.calls), 1)
            self.assertEqual(totals.orders, 1)
            self.assertEqual(s.ids(), ["a", "seed"])

    def test_a_failed_load_stops_later_windows_so_the_watermark_cannot_jump_past_it(self):
        watermark = (datetime.now(IST) - timedelta(hours=14)).replace(microsecond=0)
        dataset = self._dataset(watermark)
        with Stream(dataset, span="6") as s:
            s.seed("shopify_orders", [("seed", naive(watermark))])
            real_load = so.load_data_to_sql_optimized
            attempts = []

            def flaky(*args, **kwargs):
                attempts.append(1)
                return False if len(attempts) == 2 else real_load(*args, **kwargs)

            with mock.patch.object(orchestration, "load_data_to_sql_optimized", flaky):
                s.run()
            self.assertEqual(len(attempts), 2)  # window 3 was not even fetched after window 2's load failed
            self.assertEqual(len(s.calls), 2)
            self.assertTrue(any("load failed" in e for e in s.errors))


# ---------------------------------------------------------------------------------------------------
# One-pass equivalence + products-sold units applied once
# ---------------------------------------------------------------------------------------------------
class EquivalenceTests(unittest.TestCase):
    def _dataset(self, start):
        items = lambda pid, vid, qty: [{"product_id": pid, "variant_id": vid, "quantity": qty}]  # noqa: E731
        out = []
        for n in range(24):
            t = start + timedelta(hours=1, minutes=7 * n) + timedelta(hours=n)
            out.append(order(f"o{n:02d}", t, items=items(100 + n % 3, 500 + n % 5, 1 + n % 4),
                             financial_status="refunded" if n % 7 == 0 else "paid"))
        return out

    def _run(self, span):
        start = (datetime.now(IST) - timedelta(days=2)).replace(microsecond=0, second=0, minute=0)
        end = start + timedelta(hours=40)
        with Stream(self._dataset(start), backfill=(start, end), span=span, inventory=True) as s:
            totals = s.run()
            return totals, sorted(s.returns_seen), s.units_mock, s.ids(), len(s.calls)

    def test_split_and_unsplit_runs_have_identical_results(self):
        split_totals, split_returns, split_units, split_ids, split_calls = self._run("6")
        whole_totals, whole_returns, whole_units, whole_ids, whole_calls = self._run("0")
        self.assertGreater(split_calls, 1)
        self.assertEqual(whole_calls, 1)
        self.assertEqual(split_totals, whole_totals)  # orders, affected dates, rto dates
        self.assertEqual(split_returns, whole_returns)  # the same orders reached the returns_fact upsert
        self.assertEqual(split_ids, whole_ids)

    def test_products_sold_units_are_aggregated_across_sub_windows_and_applied_once(self):
        *_, split_units, _, split_calls = self._run("6")
        *_, whole_units, _, _ = self._run("0")
        self.assertGreater(split_calls, 1)
        split_units.assert_called_once()  # NOT once per sub-window (the update is additive)
        whole_units.assert_called_once()
        split_kwargs, whole_kwargs = split_units.call_args.kwargs, whole_units.call_args.kwargs
        self.assertEqual(split_kwargs["orders_list"], [])
        self.assertEqual(split_kwargs["precomputed_units"], whole_kwargs["precomputed_units"])
        pairs, products = split_kwargs["precomputed_units"]
        self.assertTrue(pairs and products)

    def test_merge_product_variant_units_adds_per_key(self):
        into = ({(1, 2): {"7d": 1, "30d": 2, "90d": 3}}, {1: {"7d": 1, "30d": 2, "90d": 3}})
        other = ({(1, 2): {"7d": 4, "30d": 4, "90d": 4}, (5, 6): {"7d": 1, "30d": 1, "90d": 1}},
                 {1: {"7d": 4, "30d": 4, "90d": 4}, 5: {"7d": 1, "30d": 1, "90d": 1}})
        merged = so.merge_product_variant_units(into, other)
        self.assertEqual(merged[0][(1, 2)], {"7d": 5, "30d": 6, "90d": 7})
        self.assertEqual(merged[0][(5, 6)], {"7d": 1, "30d": 1, "90d": 1})
        self.assertEqual(merged[1][1], {"7d": 5, "30d": 6, "90d": 7})

    def test_precomputed_units_are_used_instead_of_re_collecting(self):
        # With precomputed units the order list may be empty; the function must not return early.
        cursor = mock.Mock()
        cursor.fetchall.return_value = []
        with mock.patch.object(so, "_ensure_products_sold_units_daily"), \
             mock.patch.object(so, "_fetch_product_sold_units_from_shopifyql", return_value={}) as baseline, \
             mock.patch.object(so, "executemany_chunked") as upsert, \
             mock.patch.object(so.aws_background, "get_brand_timezone", return_value=IST):
            so.upsert_products_sold_units_daily_from_orders(
                BRAND, [], "B", "shop", "tok", cursor=cursor, connection=mock.Mock(),
                precomputed_units=({(1, 2): {"7d": 3, "30d": 3, "90d": 3}}, {1: {"7d": 3, "30d": 3, "90d": 3}}),
            )
        self.assertTrue(cursor.execute.called)  # it did not return early on the empty order list


# ---------------------------------------------------------------------------------------------------
# Product traffic: dates are rebuilt in bounded batches, with the same result
# ---------------------------------------------------------------------------------------------------
class _TrafficCursor:
    """Fake cursor for update_product_traffic_split_incremental: serves 3 products x 3 sources for each requested date."""

    def __init__(self):
        self.selects, self.deletes, self.truncates, self.stage_rows, self._rows = [], [], 0, [], []

    def execute(self, sql, params=None):
        text_sql = " ".join(sql.split())
        if text_sql.startswith("TRUNCATE"):
            self.truncates += 1
        elif "FROM product_sessions_snapshot" in text_sql:
            self.selects.append(tuple(params))
            self._rows = [
                {"date": d, "product_id": f"p{p}", "utm_source": src, "sessions": 5 + p, "atc_sessions": p}
                for d in params for p in range(3) for src in ("facebook", "google", None)
            ]
        elif text_sql.startswith("DELETE FROM product_traffic_split"):
            self.deletes.append(tuple(params))

    def fetchall(self):
        return self._rows

    def executemany(self, sql, rows):
        self.stage_rows.extend(rows)


class ProductTrafficBatchingTests(unittest.TestCase):
    DATES = [f"2026-10-{d:02d}" for d in range(1, 41 - 10)] + [f"2026-11-{d:02d}" for d in range(1, 11)]  # 40 dates

    def _run(self, batch):
        from pipeline import utm_traffic

        cursor = _TrafficCursor()
        with mock.patch.dict(os.environ, {"PRODUCT_TRAFFIC_DATE_BATCH": str(batch)}):
            utm_traffic.update_product_traffic_split_incremental(cursor, mock.Mock(), "B", self.DATES)
        return cursor

    def test_dates_are_rebuilt_in_bounded_batches(self):
        cursor = self._run(14)
        self.assertEqual(len(self.DATES), 40)
        self.assertEqual([len(p) for p in cursor.selects], [14, 14, 12])  # never more than the batch per read
        self.assertEqual([len(p) for p in cursor.deletes], [14, 14, 12])
        self.assertEqual(cursor.truncates, 3)

    def test_batched_and_unbatched_produce_identical_rows_and_cover_every_date_once(self):
        batched, whole = self._run(14), self._run(0)
        self.assertEqual(len(whole.selects), 1)
        self.assertEqual(sorted(batched.stage_rows), sorted(whole.stage_rows))
        covered = [d for batch in batched.selects for d in batch]
        self.assertEqual(sorted(covered), sorted(self.DATES))  # every date once: no gap, no overlap
        self.assertEqual(len(covered), len(set(covered)))

    def test_a_window_within_one_batch_is_a_single_pass(self):
        cursor = self._run(100)
        self.assertEqual(len(cursor.selects), 1)

    def test_env_parsing(self):
        from pipeline import utm_traffic

        self.assertEqual(utm_traffic.product_traffic_date_batch_size({}), 14)
        self.assertEqual(utm_traffic.product_traffic_date_batch_size({"PRODUCT_TRAFFIC_DATE_BATCH": "7"}), 7)
        self.assertEqual(utm_traffic.product_traffic_date_batch_size({"PRODUCT_TRAFFIC_DATE_BATCH": "0"}), 0)
        self.assertEqual(utm_traffic.product_traffic_date_batch_size({"PRODUCT_TRAFFIC_DATE_BATCH": "-3"}), 0)
        self.assertEqual(utm_traffic.product_traffic_date_batch_size({"PRODUCT_TRAFFIC_DATE_BATCH": "x"}), 14)

    def test_no_dates_does_nothing(self):
        from pipeline import utm_traffic

        cursor = _TrafficCursor()
        utm_traffic.update_product_traffic_split_incremental(cursor, mock.Mock(), "B", [])
        self.assertEqual((cursor.selects, cursor.truncates), ([], 0))


# ---------------------------------------------------------------------------------------------------
# Load contract + mem probe
# ---------------------------------------------------------------------------------------------------
class LoadContractTests(unittest.TestCase):
    def test_load_reports_success_empty_and_failure(self):
        engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
        df = fake_transform([order("a", at(13, 10))], {}, IST)
        with mock.patch.dict(so.sqlalchemy_engines, {BRAND: engine}):
            self.assertTrue(so.load_data_to_sql_optimized(df, BRAND, "B", "shopify_orders", 1000, replace_window=None))
            self.assertTrue(so.load_data_to_sql_optimized(df.iloc[0:0], BRAND, "B", "shopify_orders", 1000, replace_window=None))
        self.assertFalse(so.load_data_to_sql_optimized(df, 999, "B", "shopify_orders", 1000, replace_window=None))  # no engine

    def test_omitting_replace_window_still_derives_it_from_the_backfill_env(self):
        with mock.patch.object(so, "backfill_replace_window", return_value=None) as derived, \
             mock.patch.dict(so.sqlalchemy_engines, {BRAND: create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})}):
            so.load_data_to_sql_optimized(fake_transform([order("a", at(13, 10))], {}, IST), BRAND, "B", "shopify_orders", 1000)
        derived.assert_called_once()

    def test_the_wiring_keeps_backfill_fetches_strict(self):
        import inspect

        self.assertIn("strict=aws_background.is_backfill_active_for(brand_index)", inspect.getsource(orchestration._fetch_and_load_window))

    def test_the_main_body_no_longer_holds_the_order_list(self):
        import inspect

        source = inspect.getsource(orchestration.process_single_brand)
        for name in ("orders_list", "filtered", "fetch_orders(", "transform_orders_to_df_optimized("):
            self.assertNotIn(name, source)


class MemProbeTests(unittest.TestCase):
    STATUS = "Name:\tpython\nVmHWM:\t  526324 kB\nVmRSS:\t  408376 kB\nThreads:\t8\n"

    def test_off_by_default_logs_nothing(self):
        with mock.patch.dict(os.environ, {"MEM_PROBE": ""}), mock.patch.object(mem_probe.logger, "info") as info:
            mem_probe.log_mem("stage", 1, orders=5)
        info.assert_not_called()

    def test_parses_proc_status(self):
        import tempfile

        with tempfile.NamedTemporaryFile("w", suffix=".status", delete=False) as handle:
            handle.write(self.STATUS)
        self.addCleanup(os.unlink, handle.name)
        self.assertEqual(mem_probe.read_proc_status(handle.name), {"hwm_mb": 514.0, "rss_mb": 398.8, "threads": 8})
        self.assertEqual(mem_probe.read_proc_status(handle.name + ".missing"), {})

    def test_on_logs_numbers_only(self):
        status = {"rss_mb": 398.8, "hwm_mb": 514.0, "threads": 8}
        line = mem_probe.format_probe("order_window_new", 3, status, None, {"orders": 120, "rows": 260, "note": "secret text"})
        self.assertEqual(line, "MEM_PROBE stage=order_window_new brand_index=3 rss_mb=398.8 hwm_mb=514.0 threads=8 orders=120 rows=260")
        self.assertNotIn("secret", line)
        with mock.patch.dict(os.environ, {"MEM_PROBE": "true"}), \
             mock.patch.object(mem_probe, "read_proc_status", return_value=status), \
             mock.patch.object(mem_probe.logger, "info") as info:
            mem_probe.log_mem("order_window_new", 3, orders=120)
        info.assert_called_once()
        self.assertTrue(info.call_args.args[0].startswith("MEM_PROBE stage=order_window_new"))

    def test_a_missing_proc_file_is_silent(self):
        with mock.patch.dict(os.environ, {"MEM_PROBE": "true"}), \
             mock.patch.object(mem_probe, "read_proc_status", return_value={}), \
             mock.patch.object(mem_probe.logger, "info") as info:
            mem_probe.log_mem("x")
        info.assert_not_called()


if __name__ == "__main__":
    unittest.main()
