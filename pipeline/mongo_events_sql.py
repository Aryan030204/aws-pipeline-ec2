"""
SQL-sourced replacements for pipeline/mongo_events.py's checkout-initiated /
buy-now / variant-wise sync functions. These used to query alerts.sessions
(Mongo) directly; now they read the same brand's own raw_mongo_events
table (MySQL - see pipeline/mongo_events_raw.py, which ingests it from
Mongo and is the only thing that still talks to alerts.sessions for these
metrics).

Deliberately NOT a rewrite of the dedup logic as a one-shot SQL aggregate
query: pipeline/mongo_events.py's existing dedup rules are stateful/
sequential in ways a GROUP BY can't losslessly reproduce (see this
function's docstrings below, and the module docstring in
pipeline/mongo_events_raw.py's sibling plan notes). Instead, every
candidate-row fetch here pulls rows from raw_mongo_events via SQL and
feeds them through the *exact same* dedup algorithms pipeline/mongo_events.py
already uses (copied, not re-derived) - only the row source changed, from a
Mongo cursor to a MySQL cursor. The upsert/table-maintenance helpers that
never touched Mongo in the first place (_upsert_overall_summary_event_totals,
_do_upsert_hourly_event_counts, _do_upsert_variant_event_counts, the
variant-wise-events-summary table helpers) are imported unchanged from
pipeline.mongo_events, not duplicated.
"""

import os
import threading
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

from pipeline.state import IST, logger
from pipeline.db import get_db_cursor
from pipeline.mongo_events import (
    ROLLUP_EVENT_TYPES,
    is_ci_events_brand,
    get_ci_events_mongo_shop_name,
    _normalize_variant_ids,
    _sanitize_event_type_column,
    _upsert_overall_summary_event_totals,
    _do_upsert_hourly_event_counts,
    _do_upsert_variant_event_counts,
    _ensure_variant_wise_events_summary_table,
    _upsert_variant_wise_events_summary,
)

import aws_background

# Ingested by pipeline/mongo_events_raw.py. (Replaces the legacy mongo_events_summary
# table; its history is copied into this one automatically by that module.)
MONGO_EVENTS_SUMMARY_TABLE = "raw_mongo_events"


# Rows are pulled from MySQL in chunks of this size instead of one fetchall(), so a busy brand/day
# is not copied into Python in full (one list of raw rows + one list of mapped dicts) at once.
CANDIDATE_FETCH_CHUNK = 5000

# One incremental cycle runs five rollups that each used to re-read the same day's checkout_initiated
# and buy_now rows (six scans, each needing a clustered-index lookup per row). cached_candidate_rows()
# reads them ONCE and serves all five. Rows are held as compact tuples; above this many rows the cache
# is not used and every rollup streams from SQL exactly as before (bounds worker memory).
CANDIDATE_CACHE_MAX_ROWS = int(os.environ.get("MONGO_EVENTS_ROW_CACHE_MAX_ROWS", "300000"))

_cache_state = threading.local()


class _CandidateRowCache:
    """The (event_type, session_id, cart_token, variantId, created_at_utc) rows of one window."""

    def __init__(self, start_date: date, end_date: date, tz: ZoneInfo, rows_by_type: Dict[str, List[tuple]]):
        self.start_date = start_date
        self.end_date = end_date
        self.tz = tz
        self._rows_by_type = rows_by_type

    def covers(self, start_date: date, end_date: date, tz: ZoneInfo, event_type: Optional[str]) -> bool:
        return (
            event_type in self._rows_by_type
            and start_date == self.start_date
            and end_date == self.end_date
            and tz == self.tz
        )

    def rows(self, event_type: str) -> List[tuple]:
        return self._rows_by_type[event_type]

    def __len__(self) -> int:
        return sum(len(rows) for rows in self._rows_by_type.values())


def _active_cache() -> Optional[_CandidateRowCache]:
    return getattr(_cache_state, "cache", None)


def _row_to_tuple(row) -> tuple:
    if isinstance(row, dict):
        return (
            row.get("event_type"), row.get("session_id"), row.get("cart_token"),
            row.get("variantId"), row.get("created_at_utc"),
        )
    return tuple(row)


def _load_candidate_cache(
    cursor, start_date: date, end_date: date, tz: ZoneInfo, event_types: Tuple[str, ...]
) -> Optional[_CandidateRowCache]:
    """One query for all the given event types over the window. Returns None (caller streams per rollup
    instead) if it would exceed CANDIDATE_CACHE_MAX_ROWS."""
    rows_by_type: Dict[str, List[tuple]] = {event_type: [] for event_type in event_types}
    total = 0
    for row in _stream_cursor_rows(cursor, start_date, end_date, tz, event_type=None, event_types=event_types):
        as_tuple = _row_to_tuple(row)
        bucket = rows_by_type.get(as_tuple[0])
        if bucket is None:
            continue
        bucket.append(as_tuple)
        total += 1
        if total > CANDIDATE_CACHE_MAX_ROWS:
            logger.warning(
                f"candidate-row cache skipped: more than {CANDIDATE_CACHE_MAX_ROWS} rows for "
                f"{start_date}..{end_date}; rollups will stream from SQL individually"
            )
            return None
    return _CandidateRowCache(start_date, end_date, tz, rows_by_type)


@contextmanager
def cached_candidate_rows(
    cursor,
    start_date: date,
    end_date: date,
    tz: ZoneInfo,
    event_types: Tuple[str, ...] = ROLLUP_EVENT_TYPES,
):
    """Read the checkout_initiated / buy_now rows of [start_date, end_date] once and serve every
    _fetch_candidate_rows call for that exact window and event type from memory while the block runs.
    Results are identical to the per-call SQL (same rows, same mapping). If loading fails or the window
    is too large, nothing is cached and every call falls back to its own SQL query."""
    cache = None
    try:
        cache = _load_candidate_cache(cursor, start_date, end_date, tz, tuple(event_types))
    except Exception as e:
        logger.warning(f"candidate-row cache unavailable ({e}); rollups will query SQL individually")
    previous = _active_cache()
    _cache_state.cache = cache
    try:
        yield cache
    finally:
        _cache_state.cache = previous


def _iter_candidate_rows(
    cursor,
    start_date: date,
    end_date: date,
    tz: ZoneInfo,
    event_type: Optional[str] = None,
) -> Iterator[Dict[str, Any]]:
    """Streaming version of _fetch_candidate_rows: yields one mapped row at a time.
    The query must be fully consumed before the cursor is reused."""
    cache = _active_cache()
    if cache is not None and cache.covers(start_date, end_date, tz, event_type):
        rows = cache.rows(event_type)
    else:
        rows = _stream_cursor_rows(cursor, start_date, end_date, tz, event_type)
    for row in rows:
        mapped = _map_candidate_row(row)
        if mapped is not None:
            yield mapped


def _fetch_candidate_rows(
    cursor,
    start_date: date,
    end_date: date,
    tz: ZoneInfo,
    event_type: Optional[str] = None,
    stream: bool = False,
) -> List[Dict[str, Any]]:
    """
    Pulls candidate rows from this brand's own raw_mongo_events table
    (no shop_name filter needed - the table only ever holds this one
    brand's events) and maps each row into the same shape the dedup
    functions below already expect from a Mongo document: "createdAt" (a
    real datetime, UTC), "session_id", "cart_token", "variantId", and
    "event_type". event_type=None fetches every event type in the window
    (used by the variant-wise-events path, which dedupes per type itself).
    """
    rows = _iter_candidate_rows(cursor, start_date, end_date, tz, event_type)
    # stream=True hands back the generator (rows read in chunks, consumed once); the default keeps the
    # list that callers which sort or re-iterate rely on.
    return rows if stream else list(rows)


def _stream_cursor_rows(
    cursor,
    start_date: date,
    end_date: date,
    tz: ZoneInfo,
    event_type: Optional[str],
    event_types: Optional[Tuple[str, ...]] = None,
):
    start_utc, end_utc = _ist_date_range_to_utc_bounds(start_date, end_date, tz)
    where = ["created_at_utc >= %s", "created_at_utc < %s"]
    params: List[Any] = [start_utc.replace(tzinfo=None), end_utc.replace(tzinfo=None)]
    if event_type is not None:
        where.append("event_type = %s")
        params.append(event_type)
    elif event_types:
        where.append(f"event_type IN ({', '.join(['%s'] * len(event_types))})")
        params.extend(event_types)

    cursor.execute(
        f"""
        SELECT event_type, session_id, cart_token, variantId, created_at_utc
        FROM {MONGO_EVENTS_SUMMARY_TABLE}
        WHERE {' AND '.join(where)}
        """,
        tuple(params),
    )
    fetchmany = getattr(cursor, "fetchmany", None)
    if not callable(fetchmany):
        yield from (cursor.fetchall() or [])
        return
    while True:
        chunk = fetchmany(CANDIDATE_FETCH_CHUNK)
        if not chunk:
            return
        yield from chunk


def _map_candidate_row(row) -> Optional[Dict[str, Any]]:
    if isinstance(row, dict):
        event_type_val = row.get("event_type")
        session_id = row.get("session_id")
        cart_token = row.get("cart_token")
        variant_id = row.get("variantId")
        created_at = row.get("created_at_utc")
    else:
        event_type_val, session_id, cart_token, variant_id, created_at = row
    if not isinstance(created_at, datetime):
        return None
    return {
        "event_type": event_type_val,
        "session_id": session_id,
        "cart_token": cart_token,
        "variantId": variant_id,
        "createdAt": created_at.replace(tzinfo=None),  # naive UTC, matches Mongo's tz_aware=True->UTC
    }


def _ist_date_range_to_utc_bounds(
    start_date: date, end_date: date, tz: ZoneInfo = IST
) -> Tuple[datetime, datetime]:
    """Same bound computation as pipeline.mongo_events' version - kept as a
    local copy (not imported) since this one deliberately returns bounds the
    caller treats as naive-UTC-equivalent when binding to created_at_utc."""
    start_tz = datetime.combine(start_date, datetime.min.time(), tzinfo=tz)
    end_tz_exclusive = datetime.combine(
        end_date + timedelta(days=1), datetime.min.time(), tzinfo=tz
    )
    return (
        start_tz.astimezone(timezone.utc),
        end_tz_exclusive.astimezone(timezone.utc),
    )


def _extract_session_id(doc: Dict[str, Any]) -> Optional[str]:
    session_id = str(doc.get("session_id") or "").strip()
    return session_id or None


def _extract_cart_token(doc: Dict[str, Any]) -> Optional[str]:
    cart_token = str(doc.get("cart_token") or "").strip()
    return cart_token or None


def _iter_counted_pair_events_from_sql(
    cursor,
    start_date: date,
    end_date: date,
    event_type: str,
    tz: ZoneInfo = IST,
    dedupe_window_minutes: int = 30,
) -> List[Dict[str, Any]]:
    """
    Copy of pipeline.mongo_events._iter_counted_pair_events_from_mongo's
    exact dedup algorithm (a greedy sequential scan: an event counts only if
    it's more than `dedupe_window_minutes` after the last *counted* event
    for its (date, session_id, cart_token) key - not simply the gap since
    the previous row, which is why this can't be a one-shot SQL aggregate),
    sourced from raw_mongo_events instead of Mongo. Rows are sorted by
    createdAt in Python since the source query doesn't guarantee order.
    """
    dedupe_window = timedelta(minutes=dedupe_window_minutes)
    rows = _fetch_candidate_rows(cursor, start_date, end_date, tz, event_type=event_type)
    rows.sort(key=lambda r: r["createdAt"])

    counted_events: List[Dict[str, Any]] = []
    last_counted_at_by_pair: Dict[Tuple[date, str, str], datetime] = {}
    for doc in rows:
        session_id = _extract_session_id(doc)
        cart_token = _extract_cart_token(doc)
        if not session_id or not cart_token:
            continue

        created_at = doc["createdAt"]
        local_created_at = created_at.replace(tzinfo=timezone.utc).astimezone(tz)
        local_date = local_created_at.date()
        pair_key = (local_date, session_id, cart_token)

        last_counted_at = last_counted_at_by_pair.get(pair_key)
        if last_counted_at is not None and created_at - last_counted_at <= dedupe_window:
            continue

        if local_date < start_date or local_date > end_date:
            continue

        counted_events.append(
            {
                "date": local_date,
                "hour": local_created_at.hour,
                "variant_ids": _normalize_variant_ids(doc.get("variantId")),
            }
        )
        last_counted_at_by_pair[pair_key] = created_at

    return counted_events


def _iter_unique_daily_pair_events_from_sql(
    cursor,
    start_date: date,
    end_date: date,
    event_type: str,
    tz: ZoneInfo = IST,
) -> List[Dict[str, Any]]:
    """
    Copy of pipeline.mongo_events._iter_unique_daily_pair_events_from_mongo:
    a (date, session_id, cart_token) pair counts only if it occurs exactly
    once in the window - two or more occurrences drop the whole pair.
    """
    # Streamed: for each pair only its first event is kept, and None marks "seen more than once"
    # (the pair is then dropped). Same result as collecting every event per pair, with one small
    # dict per distinct pair instead of every row.
    pair_events: Dict[Tuple[date, str, str], Optional[Dict[str, Any]]] = {}
    for doc in _fetch_candidate_rows(cursor, start_date, end_date, tz, event_type=event_type, stream=True):
        session_id = _extract_session_id(doc)
        cart_token = _extract_cart_token(doc)
        if not session_id or not cart_token:
            continue

        created_at = doc["createdAt"].replace(tzinfo=timezone.utc)
        local_date = created_at.astimezone(tz).date()
        if local_date < start_date or local_date > end_date:
            continue

        pair_key = (local_date, session_id, cart_token)
        if pair_key in pair_events:
            pair_events[pair_key] = None
        else:
            pair_events[pair_key] = {"date": local_date, "variant_ids": _normalize_variant_ids(doc.get("variantId"))}

    return [event for event in pair_events.values() if event is not None]


def _iter_unique_daily_pair_events_all_types_from_sql(
    cursor,
    start_date: date,
    end_date: date,
    tz: ZoneInfo = IST,
) -> List[Dict[str, Any]]:
    """
    Copy of pipeline.mongo_events._iter_unique_daily_pair_events_all_types_from_mongo:
    same "exactly once per day" dedup, but across every event_type in one
    pass (the dedup key includes the sanitized event type, so each type is
    still deduped independently).
    """
    # Streamed, one small event kept per distinct pair (None = seen more than once, so dropped).
    pair_events: Dict[Tuple[date, str, str, str], Optional[Dict[str, Any]]] = {}
    for doc in _fetch_candidate_rows(cursor, start_date, end_date, tz, event_type=None, stream=True):
        session_id = _extract_session_id(doc)
        cart_token = _extract_cart_token(doc)
        event_column = _sanitize_event_type_column(doc.get("event_type"))
        if not session_id or not cart_token or not event_column:
            continue

        created_at = doc["createdAt"].replace(tzinfo=timezone.utc)
        local_date = created_at.astimezone(tz).date()
        if local_date < start_date or local_date > end_date:
            continue

        pair_key = (local_date, session_id, cart_token, event_column)
        if pair_key in pair_events:
            pair_events[pair_key] = None
        else:
            pair_events[pair_key] = {
                "date": local_date,
                "event_column": event_column,
                "variant_ids": _normalize_variant_ids(doc.get("variantId")),
            }

    return [event for event in pair_events.values() if event is not None]


def _fetch_daily_unique_pair_event_counts_from_sql(
    cursor, start_date: date, end_date: date, event_type: str, tz: ZoneInfo = IST
) -> Dict[date, int]:
    counts: Dict[date, int] = {}
    for event in _iter_unique_daily_pair_events_from_sql(cursor, start_date, end_date, event_type, tz):
        counts[event["date"]] = counts.get(event["date"], 0) + 1
    return counts


def _fetch_event_hourly_counts_from_sql(
    cursor, start_date: date, end_date: date, event_type: str, metric_key: str, tz: ZoneInfo = IST
) -> List[Dict[str, Any]]:
    counts: Dict[Tuple[date, int], int] = {}
    for event in _iter_counted_pair_events_from_sql(cursor, start_date, end_date, event_type, tz):
        bucket_key = (event["date"], event["hour"])
        counts[bucket_key] = counts.get(bucket_key, 0) + 1
    return [
        {"date": bucket_date, "hour": bucket_hour, metric_key: count}
        for (bucket_date, bucket_hour), count in sorted(counts.items())
    ]


def _fetch_event_variant_counts_from_sql(
    cursor, start_date: date, end_date: date, event_type: str, metric_key: str, tz: ZoneInfo = IST
) -> List[Dict[str, Any]]:
    counts: Dict[Tuple[date, str], int] = {}
    for event in _iter_unique_daily_pair_events_from_sql(cursor, start_date, end_date, event_type, tz):
        for vid in event["variant_ids"]:
            key = (event["date"], vid)
            counts[key] = counts.get(key, 0) + 1
    return [
        {"date": bucket_date, "variant_id": vid, metric_key: count}
        for (bucket_date, vid), count in sorted(counts.items())
    ]


def _aggregate_variant_wise_event_counts_from_sql(
    cursor, start_date: date, end_date: date, tz: ZoneInfo = IST
) -> Tuple[Dict[Tuple[date, str], Dict[str, int]], Set[str]]:
    unique_events = _iter_unique_daily_pair_events_all_types_from_sql(cursor, start_date, end_date, tz)
    aggregated: Dict[Tuple[date, str], Dict[str, int]] = {}
    event_columns: Set[str] = set()
    for event in unique_events:
        event_column = event["event_column"]
        event_columns.add(event_column)
        for variant_id in event["variant_ids"]:
            key = (event["date"], variant_id)
            bucket = aggregated.setdefault(key, {})
            bucket[event_column] = bucket.get(event_column, 0) + 1
    return aggregated, event_columns


# ---------------------------------------------------------------------------
# Public sync functions - same shape/names (with an _sql suffix) as their
# Mongo-sourced counterparts in pipeline/mongo_events.py, so callers (the
# main pipeline, events-summary-worker, backfill scripts) barely change.
# ---------------------------------------------------------------------------

def _brand_gate(brand_name: str, brand_key: Optional[str], what: str) -> Optional[str]:
    """Shared brand-gating + logging for every sync_* below. Returns the
    Mongo shop-name mapping purely as a "this brand is onboarded" check -
    none of these functions touch Mongo, but a brand with no shop-name
    mapping was never going to have rows in raw_mongo_events either."""
    if not is_ci_events_brand(brand_name, brand_key):
        logger.info(f"Skipping {what} for {brand_name}: brand not enabled for ci_events")
        return None
    mongo_shop_name = get_ci_events_mongo_shop_name(brand_name, brand_key)
    if not mongo_shop_name:
        logger.info(f"Skipping {what} for {brand_name}: no Mongo shop_name mapping")
        return None
    return mongo_shop_name


def update_overall_summary_event_columns_sql(
    brand_index: int,
    brand_name: str,
    start_date: date,
    end_date: date,
    brand_key: Optional[str] = None,
    cursor=None,
    connection=None,
) -> None:
    if _brand_gate(brand_name, brand_key, "overall_summary event totals") is None:
        return

    tz = aws_background.get_brand_timezone(brand_index)

    def _do(c, conn) -> None:
        try:
            ci_counts = _fetch_daily_unique_pair_event_counts_from_sql(
                c, start_date, end_date, "checkout_initiated", tz
            )
            buy_now_counts = _fetch_daily_unique_pair_event_counts_from_sql(
                c, start_date, end_date, "buy_now", tz
            )
        except Exception as e:
            logger.error(f"Error fetching overall_summary event totals for {brand_name}: {e}")
            return
        # Targeted upsert (date, ci_events, buy_now_events only) - does not
        # touch any other overall_summary column, so it never clobbers the
        # order-driven columns written elsewhere.
        _upsert_overall_summary_event_totals(c, conn, start_date, end_date, ci_counts, buy_now_counts)
        logger.info(f"Updated overall_summary event totals for {brand_name} ({start_date}..{end_date})")

    if cursor is not None:
        _do(cursor, connection)
        return
    try:
        with get_db_cursor(brand_index) as (c, conn):
            _do(c, conn)
    except Exception as e:
        logger.error(f"Error updating overall_summary event totals for {brand_name}: {e}")


def sync_checkout_initiated_ci_events_sql(
    brand_index: int,
    brand_name: str,
    start_date: date,
    end_date: date,
    brand_key: Optional[str] = None,
    cursor=None,
    connection=None,
) -> None:
    if _brand_gate(brand_name, brand_key, "checkout-initiated ci_events sync") is None:
        return
    tz = aws_background.get_brand_timezone(brand_index)

    def _do(c, conn) -> None:
        try:
            results = _fetch_event_hourly_counts_from_sql(
                c, start_date, end_date, "checkout_initiated", "ci_events", tz
            )
        except Exception as e:
            logger.error(f"Error fetching checkout-initiated events for {brand_name}: {e}")
            return
        _do_upsert_hourly_event_counts(
            results, c, conn, brand_name, start_date, end_date, "ci_events", "checkout-initiated ci_events"
        )

    if cursor is not None:
        _do(cursor, connection)
        return
    try:
        with get_db_cursor(brand_index) as (c, conn):
            _do(c, conn)
    except Exception as e:
        logger.error(f"Error updating checkout-initiated ci_events for {brand_name}: {e}")


def sync_buy_now_events_sql(
    brand_index: int,
    brand_name: str,
    start_date: date,
    end_date: date,
    brand_key: Optional[str] = None,
    cursor=None,
    connection=None,
) -> None:
    if _brand_gate(brand_name, brand_key, "buy-now events sync") is None:
        return
    tz = aws_background.get_brand_timezone(brand_index)

    def _do(c, conn) -> None:
        try:
            results = _fetch_event_hourly_counts_from_sql(
                c, start_date, end_date, "buy_now", "buy_now_events", tz
            )
        except Exception as e:
            logger.error(f"Error fetching buy-now events for {brand_name}: {e}")
            return
        _do_upsert_hourly_event_counts(
            results, c, conn, brand_name, start_date, end_date, "buy_now_events", "buy-now events"
        )

    if cursor is not None:
        _do(cursor, connection)
        return
    try:
        with get_db_cursor(brand_index) as (c, conn):
            _do(c, conn)
    except Exception as e:
        logger.error(f"Error updating buy-now events for {brand_name}: {e}")


def sync_checkout_initiated_variant_events_sql(
    brand_index: int,
    brand_name: str,
    start_date: date,
    end_date: date,
    brand_key: Optional[str] = None,
    cursor=None,
    connection=None,
) -> None:
    if _brand_gate(brand_name, brand_key, "checkout-initiated variant events sync") is None:
        return
    tz = aws_background.get_brand_timezone(brand_index)

    def _do(c, conn) -> None:
        try:
            results = _fetch_event_variant_counts_from_sql(
                c, start_date, end_date, "checkout_initiated", "checkout_initiated_count", tz
            )
        except Exception as e:
            logger.error(f"Error fetching checkout-initiated variant events for {brand_name}: {e}")
            return
        _do_upsert_variant_event_counts(
            results, c, conn, brand_name, start_date, end_date,
            "daily_ci_events_summary", "checkout_initiated_count", "checkout-initiated variant events",
        )

    if cursor is not None:
        _do(cursor, connection)
        return
    try:
        with get_db_cursor(brand_index) as (c, conn):
            _do(c, conn)
    except Exception as e:
        logger.error(f"Error updating checkout-initiated variant events for {brand_name}: {e}")


def sync_buy_now_variant_events_sql(
    brand_index: int,
    brand_name: str,
    start_date: date,
    end_date: date,
    brand_key: Optional[str] = None,
    cursor=None,
    connection=None,
) -> None:
    if _brand_gate(brand_name, brand_key, "buy-now variant events sync") is None:
        return
    tz = aws_background.get_brand_timezone(brand_index)

    def _do(c, conn) -> None:
        try:
            results = _fetch_event_variant_counts_from_sql(
                c, start_date, end_date, "buy_now", "buy_now_count", tz
            )
        except Exception as e:
            logger.error(f"Error fetching buy-now variant events for {brand_name}: {e}")
            return
        _do_upsert_variant_event_counts(
            results, c, conn, brand_name, start_date, end_date,
            "daily_buy_now_events_summary", "buy_now_count", "buy-now variant events",
        )

    if cursor is not None:
        _do(cursor, connection)
        return
    try:
        with get_db_cursor(brand_index) as (c, conn):
            _do(c, conn)
    except Exception as e:
        logger.error(f"Error updating buy-now variant events for {brand_name}: {e}")


def sync_variant_wise_events_summary_sql(
    brand_index: int,
    brand_name: str,
    start_date: date,
    end_date: date,
    brand_key: Optional[str] = None,
    cursor=None,
    connection=None,
) -> None:
    if _brand_gate(brand_name, brand_key, "variant-wise events summary sync") is None:
        return
    tz = aws_background.get_brand_timezone(brand_index)

    def _do(c, conn) -> None:
        try:
            aggregated, event_columns = _aggregate_variant_wise_event_counts_from_sql(
                c, start_date, end_date, tz
            )
        except Exception as e:
            logger.error(f"Error fetching variant-wise events for {brand_name}: {e}")
            return
        _ensure_variant_wise_events_summary_table(c, conn)
        inserted = _upsert_variant_wise_events_summary(c, conn, start_date, end_date, aggregated, event_columns)
        logger.info(
            f"Updated variant-wise events summary for {brand_name} "
            f"({len(event_columns)} event type(s), {inserted} row(s), {start_date}..{end_date})"
        )

    if cursor is not None:
        _do(cursor, connection)
        return
    try:
        with get_db_cursor(brand_index) as (c, conn):
            _do(c, conn)
    except Exception as e:
        logger.error(f"Error updating variant-wise events summary for {brand_name}: {e}")
