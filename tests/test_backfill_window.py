"""Backfill window replace: the delete must cover exactly the range that is re-fetched (no whole-day wipes), the delete
and the insert must be one transaction, and a partial Shopify fetch must never be accepted by a backfill.
Everything runs against in-memory SQLite and a fake Shopify client: no database, no network."""
import asyncio
import inspect
import unittest
from datetime import datetime
from unittest import mock
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

# Safe import order (see other tests): aws_background first.
import aws_background  # noqa: F401
from pipeline import orchestration
from pipeline import shopify_orders as so

IST = ZoneInfo("Asia/Kolkata")
BRAND = 7


def make_engine():
    return create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})


def order_df(rows, ts_col="created_at"):
    """rows: [(order_id, datetime)] -> a frame shaped like transform_orders_to_df_optimized's output (subset)."""
    return pd.DataFrame(
        {
            "order_id": [r[0] for r in rows],
            ts_col: pd.to_datetime([r[1] for r in rows]),
            "created_date": [r[1].strftime("%Y-%m-%d") for r in rows],
            "created_time": [r[1].strftime("%H:%M:%S") for r in rows],
            "total_price": [100.0] * len(rows),
        }
    )


def day(day_of_month, hour, minute=0, second=0, month=11):
    return datetime(2026, month, day_of_month, hour, minute, second)


class Env:
    """Patches the module globals load_data_to_sql_optimized reads: the engine and the backfill config."""

    def __init__(self, engine, window=None):
        self.engine = engine
        self.window = window  # (start, end) in brand tz, or None = not a backfill

    def __enter__(self):
        self.patches = [mock.patch.dict(so.sqlalchemy_engines, {BRAND: self.engine})]
        active = self.window is not None
        start, end = (self.window or (None, None))
        self.patches += [
            mock.patch.object(so.aws_background, "is_backfill_active_for", return_value=active),
            mock.patch.object(so.aws_background, "BACKFILL_START_IST", start, create=True),
            mock.patch.object(so.aws_background, "BACKFILL_END_IST", end, create=True),
            mock.patch.object(
                so.aws_background,
                "get_backfill_window_for_brand",
                return_value=(start.replace(tzinfo=IST) if start else None, end.replace(tzinfo=IST) if end else None),
            ),
        ]
        for p in self.patches:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in reversed(self.patches):
            p.stop()
        return False


def ids(engine, table="shopify_orders"):
    with engine.connect() as conn:
        return [r[0] for r in conn.execute(text(f"SELECT order_id FROM {table} ORDER BY order_id"))]


def seed(engine, rows, table="shopify_orders", ts_col="created_at"):
    """Existing rows, loaded with no backfill active."""
    with Env(engine):
        so.load_data_to_sql_optimized(order_df(rows, ts_col), BRAND, "BBB", table)


class WindowSelectionTests(unittest.TestCase):
    def _window(self, table, start, end, active=True):
        with Env(make_engine(), (start, end) if active else None):
            return so.backfill_replace_window(BRAND, table)

    def test_no_window_unless_a_backfill_is_active_for_the_brand(self):
        self.assertIsNone(self._window("shopify_orders", day(13, 18), day(14, 0), active=False))

    def test_only_the_two_order_tables_are_replaced_and_on_the_right_column(self):
        w = self._window("shopify_orders", day(13, 18), day(14, 0))
        self.assertEqual((w.column, w.start, w.end), ("created_at", day(13, 18), day(14, 0)))
        u = self._window("shopify_orders_update", day(13, 18), day(14, 0))
        self.assertEqual(u.column, "updated_at")
        self.assertIsNone(self._window("returns_fact", day(13, 18), day(14, 0)))

    def test_the_window_is_the_exact_time_range_not_whole_dates(self):
        w = self._window("shopify_orders", day(13, 18, 30), day(14, 6, 15, 59))
        self.assertEqual((w.start, w.end), (day(13, 18, 30), day(14, 6, 15, 59)))  # time of day kept
        self.assertIsNone(w.start.tzinfo)  # store wall-clock, matches the stored created_at

    def test_a_missing_window_means_no_replace(self):
        with Env(make_engine(), (day(13, 0), day(14, 0))):
            with mock.patch.object(so.aws_background, "get_backfill_window_for_brand", return_value=(None, None)):
                self.assertIsNone(so.backfill_replace_window(BRAND, "shopify_orders"))


class ExactRangeDeleteTests(unittest.TestCase):
    """The reported bug: a 6 PM - 12 AM window on Nov 13 wiped Nov 13 00:00-18:00 and Nov 14 for good."""

    def setUp(self):
        self.engine = make_engine()
        self.existing = [
            ("n13-0030", day(13, 0, 30)),
            ("n13-0900", day(13, 9)),
            ("n13-1759", day(13, 17, 59, 59)),
            ("n13-1800", day(13, 18, 0, 0)),  # exactly the start: inside
            ("n13-2000", day(13, 20)),
            ("n13-2359", day(13, 23, 59, 59)),
            ("n14-0000", day(14, 0, 0, 0)),  # exactly the end: inside
            ("n14-0001", day(14, 0, 0, 1)),  # one second after the end: outside
            ("n14-0930", day(14, 9, 30)),
            ("n12-1000", day(12, 10)),
        ]
        seed(self.engine, self.existing)

    def test_only_rows_inside_the_refetched_range_are_replaced(self):
        window = (day(13, 18), day(14, 0))
        refetched = [("n13-1800", day(13, 18, 0, 0)), ("n13-2000", day(13, 20)), ("n13-2359", day(13, 23, 59, 59)),
                     ("n14-0000", day(14, 0, 0, 0)), ("new-2100", day(13, 21))]
        with Env(self.engine, window):
            so.load_data_to_sql_optimized(order_df(refetched), BRAND, "BBB", "shopify_orders")

        remaining = set(ids(self.engine))
        # everything before 6 PM on the window's first date survives (the old code wiped all of it)
        self.assertTrue({"n13-0030", "n13-0900", "n13-1759"} <= remaining)
        # everything after the window on its last date survives
        self.assertTrue({"n14-0001", "n14-0930"} <= remaining)
        self.assertIn("n12-1000", remaining)
        # rows inside were replaced, not duplicated
        self.assertEqual(sorted(i for i in ids(self.engine) if i in {"n13-1800", "n13-2000", "n13-2359", "n14-0000"}),
                         ["n13-1800", "n13-2000", "n13-2359", "n14-0000"])
        self.assertIn("new-2100", remaining)
        self.assertEqual(len(ids(self.engine)), len(self.existing) + 1)  # 10 existing + 1 brand-new order

    def test_an_order_deleted_in_shopify_inside_the_range_disappears(self):
        with Env(self.engine, (day(13, 18), day(14, 0))):
            so.load_data_to_sql_optimized(order_df([("n13-1800", day(13, 18))]), BRAND, "BBB", "shopify_orders")
        remaining = set(ids(self.engine))
        self.assertNotIn("n13-2000", remaining)  # was in the window and is no longer returned by Shopify
        self.assertIn("n13-1759", remaining)

    def test_a_whole_day_window_replaces_exactly_that_day(self):
        with Env(self.engine, (day(13, 0), day(13, 23, 59, 59))):
            so.load_data_to_sql_optimized(order_df([("only", day(13, 12))]), BRAND, "BBB", "shopify_orders")
        remaining = set(ids(self.engine))
        self.assertTrue({"n12-1000", "n14-0000", "n14-0001", "n14-0930"} <= remaining)
        self.assertFalse(any(i.startswith("n13-") for i in remaining))
        self.assertIn("only", remaining)

    def test_a_date_only_end_does_not_touch_that_date(self):
        # END=2026-11-14 means 2026-11-14 00:00:00: Nov 14's orders after midnight are neither fetched nor deleted
        with Env(self.engine, (day(13, 0), day(14, 0))):
            so.load_data_to_sql_optimized(order_df([("n13-x", day(13, 5))]), BRAND, "BBB", "shopify_orders")
        remaining = set(ids(self.engine))
        self.assertTrue({"n14-0001", "n14-0930"} <= remaining)

    def test_the_update_table_is_replaced_by_updated_at(self):
        engine = make_engine()
        seed(engine, [("u1", day(13, 9)), ("u2", day(13, 19)), ("u3", day(13, 22))], "shopify_orders_update", "updated_at")
        with Env(engine, (day(13, 18), day(14, 0))):
            so.load_data_to_sql_optimized(order_df([("u2", day(13, 19))], "updated_at"), BRAND, "BBB", "shopify_orders_update")
        self.assertEqual(ids(engine, "shopify_orders_update"), ["u1", "u2"])  # u1 (before 6 PM) survives, u3 replaced


class AtomicityTests(unittest.TestCase):
    def setUp(self):
        self.engine = make_engine()
        self.existing = [("a", day(13, 19)), ("b", day(13, 20)), ("c", day(13, 9))]
        seed(self.engine, self.existing)
        self.bad = order_df([("z", day(13, 19, 30))])
        self.bad["no_such_column"] = 1  # makes the INSERT fail after the DELETE already ran

    def test_a_failed_insert_rolls_the_delete_back_and_aborts_the_backfill(self):
        with Env(self.engine, (day(13, 18), day(14, 0))):
            with self.assertRaises(Exception):
                so.load_data_to_sql_optimized(self.bad, BRAND, "BBB", "shopify_orders")
        self.assertEqual(ids(self.engine), ["a", "b", "c"])  # nothing was lost

    def test_outside_a_backfill_a_failed_insert_is_still_swallowed_as_before(self):
        with Env(self.engine):
            so.load_data_to_sql_optimized(self.bad, BRAND, "BBB", "shopify_orders")  # must not raise
        self.assertEqual(ids(self.engine), ["a", "b", "c"])

    def test_an_empty_frame_deletes_nothing_even_with_a_window(self):
        with Env(self.engine, (day(13, 0), day(14, 0))):
            so.load_data_to_sql_optimized(order_df([]), BRAND, "BBB", "shopify_orders")
        self.assertEqual(ids(self.engine), ["a", "b", "c"])

    def test_without_a_backfill_rows_are_appended_and_nothing_is_deleted(self):
        with Env(self.engine):
            so.load_data_to_sql_optimized(order_df([("d", day(13, 21))]), BRAND, "BBB", "shopify_orders")
        self.assertEqual(ids(self.engine), ["a", "b", "c", "d"])

    def test_the_delete_and_the_insert_share_one_transaction(self):
        from sqlalchemy import event

        events = []
        event.listen(self.engine, "commit", lambda conn: events.append("COMMIT"))
        event.listen(self.engine, "rollback", lambda conn: events.append("ROLLBACK"))

        def record(conn, cursor, statement, *args):
            verb = statement.lstrip().split(None, 1)[0].upper()
            if verb in ("DELETE", "INSERT"):
                events.append(verb)

        event.listen(self.engine, "before_cursor_execute", record)
        with Env(self.engine, (day(13, 18), day(14, 0))):
            so.load_data_to_sql_optimized(order_df([("e", day(13, 21))]), BRAND, "BBB", "shopify_orders")

        delete_at, insert_at = events.index("DELETE"), events.index("INSERT")
        self.assertLess(delete_at, insert_at)
        # no commit between the delete and the insert (the old code committed the delete first), and it commits after
        self.assertNotIn("COMMIT", events[delete_at:insert_at])
        self.assertEqual(events[insert_at + 1:], ["COMMIT"])


# ---------------------------------------------------------------------------------------------------
# Strict Shopify fetch
# ---------------------------------------------------------------------------------------------------
class _FakeResponse:
    def __init__(self, status=200, orders=None, link=""):
        self.status = status
        self._orders = orders or []
        self.headers = {"Link": link}

    async def json(self):
        return {"orders": self._orders}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeSession:
    """Serves the scripted responses in order; an Exception instance in the script is raised from get()."""

    def __init__(self, script):
        self.script = list(script)
        self.urls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def get(self, url, headers=None):
        self.urls.append(url)
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


NEXT = '<https://shop/next-page>; rel="next"'


def run_fetch(script, strict):
    session = _FakeSession(script)

    async def no_sleep(_):
        return None

    with mock.patch.object(so.aiohttp, "ClientSession", return_value=session), \
         mock.patch.object(so.aiohttp, "TCPConnector"), \
         mock.patch.object(so.asyncio, "sleep", no_sleep):
        return asyncio.run(so.fetch_orders_async("https://shop/admin", "tok", "s", "e", "created_at_min", strict=strict)), session


class StrictFetchTests(unittest.TestCase):
    PAGE1 = _FakeResponse(200, [{"id": 1}, {"id": 2}], NEXT)

    def _page1(self):
        return _FakeResponse(200, [{"id": 1}, {"id": 2}], NEXT)

    def test_a_complete_fetch_returns_every_page_in_both_modes(self):
        for strict in (False, True):
            orders, session = run_fetch([self._page1(), _FakeResponse(200, [{"id": 3}])], strict)
            self.assertEqual([o["id"] for o in orders], [1, 2, 3], strict)
            self.assertEqual(len(session.urls), 2)

    def test_a_non_strict_fetch_keeps_the_old_partial_result_on_an_http_error(self):
        orders, _ = run_fetch([self._page1(), _FakeResponse(500)], strict=False)
        self.assertEqual([o["id"] for o in orders], [1, 2])

    def test_a_strict_fetch_raises_on_an_http_error_mid_pagination(self):
        with self.assertRaises(so.OrderFetchIncomplete) as ctx:
            run_fetch([self._page1(), _FakeResponse(500)], strict=True)
        self.assertIn("500", str(ctx.exception))
        self.assertIn("2 order", str(ctx.exception))

    def test_a_strict_fetch_raises_when_the_very_first_request_fails(self):
        with self.assertRaises(so.OrderFetchIncomplete):
            run_fetch([_FakeResponse(503)], strict=True)

    def test_a_strict_fetch_raises_on_a_network_exception_and_non_strict_returns_partial(self):
        with self.assertRaises(so.OrderFetchIncomplete):
            run_fetch([self._page1(), ConnectionError("reset")], strict=True)
        orders, _ = run_fetch([self._page1(), ConnectionError("reset")], strict=False)
        self.assertEqual([o["id"] for o in orders], [1, 2])

    def test_a_strict_fetch_with_no_orders_is_a_valid_empty_result(self):
        orders, _ = run_fetch([_FakeResponse(200, [])], strict=True)
        self.assertEqual(orders, [])

    def test_the_sync_wrapper_passes_strict_through_and_defaults_to_non_strict(self):
        captured = {}

        async def fake_async(*args, **kwargs):
            captured["kwargs"] = kwargs
            return []

        with mock.patch.object(so, "fetch_orders_async", fake_async):
            so.fetch_orders("b", "t", "s", "e", "created_at_min")
            self.assertFalse(captured["kwargs"]["strict"])
            so.fetch_orders("b", "t", "s", "e", "created_at_min", strict=True)
            self.assertTrue(captured["kwargs"]["strict"])


class OrchestrationWiringTests(unittest.TestCase):
    def test_backfill_runs_fetch_in_strict_mode_and_normal_ticks_do_not(self):
        src = inspect.getsource(orchestration.process_single_brand)
        self.assertIn("strict=aws_background.is_backfill_active_for(brand_index)", src)

    def test_the_old_whole_date_delete_is_gone(self):
        src = inspect.getsource(so)
        self.assertNotIn("{date_column} >= :window_start", src)
        self.assertNotIn("_replace_backfill_window_if_needed", src)


if __name__ == "__main__":
    unittest.main()
