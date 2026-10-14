"""
SQL-sourced replacements for pipeline/mongo_events.py's checkout-initiated /
buy-now / variant-wise sync functions. These used to query alerts.sessions
(Mongo) directly; now they read the same brand's own mongo_events_summary
table (MySQL - see pipeline/mongo_events_raw.py, which ingests it from
Mongo and is the only thing that still talks to alerts.sessions for these
metrics).

Deliberately NOT a rewrite of the dedup logic as a one-shot SQL aggregate
query: pipeline/mongo_events.py's existing dedup rules are stateful/
sequential in ways a GROUP BY can't losslessly reproduce (see this
function's docstrings below, and the module docstring in
pipeline/mongo_events_raw.py's sibling plan notes). Instead, every
candidate-row fetch here pulls rows from mongo_events_summary via SQL and
feeds them through the *exact same* dedup algorithms pipeline/mongo_events.py
already uses (copied, not re-derived) - only the row source changed, from a
Mongo cursor to a MySQL cursor. The upsert/table-maintenance helpers that
never touched Mongo in the first place (_upsert_overall_summary_event_totals,
_do_upsert_hourly_event_counts, _do_upsert_variant_event_counts, the
variant-wise-events-summary table helpers) are imported unchanged from
pipeline.mongo_events, not duplicated.
"""

from contextlib import contextmanager
from contextvars import ContextVar
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

from pipeline.state import IST, logger
from pipeline.db import get_db_cursor
from pipeline.mongo_events import (
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

MONGO_EVENTS_SUMMARY_TABLE = "mongo_events_summary"

# Per-run memo for _fetch_candidate_rows. The sync_* functions below each
# fetch their own rows (a holdover from the per-metric Mongo queries they
# replaced), so one brand's rollup run used to re-read the same window of
# this wide table several times - e.g. checkout_initiated three times in the
# incremental events job. Inside candidate_rows_cache(), a repeat call with
# identical arguments reuses the first result instead of hitting MySQL again.
# The key is the exact query arguments, so results are identical to the
# uncached path; outside the context manager nothing is cached.
_candidate_rows_cache: ContextVar[Optional[Dict[Tuple[Any, ...], List[Dict[str, Any]]]]] = ContextVar(
    "_candidate_rows_cache", default=None
)


@contextmanager
def candidate_rows_cache() -> Iterator[None]:
    """Scope one brand's rollup run so repeated identical candidate-row
    fetches hit MySQL once. Keep the scope to a single run - the cache never
    sees rows ingested after the first fetch."""
    token = _candidate_rows_cache.set({})
    try:
        yield
    finally:
        _candidate_rows_cache.reset(token)


def _fetch_candidate_rows(
    cursor,
    start_date: date,
    end_date: date,
    tz: ZoneInfo,
    event_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Pulls candidate rows from this brand's own mongo_events_summary table
    (no shop_name filter needed - the table only ever holds this one
    brand's events) and maps each row into the same shape the dedup
    functions below already expect from a Mongo document: "createdAt" (a
    real datetime, UTC), "session_id", "cart_token", "variantId", and
    "event_type". event_type=None fetches every event type in the window
    (used by the variant-wise-events path, which dedupes per type itself).
    """
    cache = _candidate_rows_cache.get()
    cache_key = (id(cursor), start_date, end_date, tz, event_type)
    if cache is not None and cache_key in cache:
        # Fresh list each time: callers sort it in place.
        return list(cache[cache_key])

    start_utc, end_utc = _ist_date_range_to_utc_bounds(start_date, end_date, tz)
    where = ["created_at_utc >= %s", "created_at_utc < %s"]
    params: List[Any] = [start_utc.replace(tzinfo=None), end_utc.replace(tzinfo=None)]
    if event_type is not None:
        where.append("event_type = %s")
        params.append(event_type)

    cursor.execute(
        f"""
        SELECT event_type, session_id, cart_token, variantId, created_at_utc
        FROM {MONGO_EVENTS_SUMMARY_TABLE}
        WHERE {' AND '.join(where)}
        """,
        tuple(params),
    )
    rows = cursor.fetchall() or []

    mapped: List[Dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            event_type_val = row.get("event_type")
            session_id = row.get("session_id")
            cart_token = row.get("cart_token")
            variant_id = row.get("variantId")
            created_at = row.get("created_at_utc")
        else:
            event_type_val, session_id, cart_token, variant_id, created_at = row
        if not isinstance(created_at, datetime):
            continue
        mapped.append(
            {
                "event_type": event_type_val,
                "session_id": session_id,
                "cart_token": cart_token,
                "variantId": variant_id,
                "createdAt": created_at.replace(tzinfo=None),  # naive UTC, matches Mongo's tz_aware=True->UTC
            }
        )
    if cache is not None:
        cache[cache_key] = mapped
        return list(mapped)
    return mapped


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
    sourced from mongo_events_summary instead of Mongo. Rows are sorted by
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
    rows = _fetch_candidate_rows(cursor, start_date, end_date, tz, event_type=event_type)

    pair_events: Dict[Tuple[date, str, str], List[Dict[str, Any]]] = {}
    for doc in rows:
        session_id = _extract_session_id(doc)
        cart_token = _extract_cart_token(doc)
        if not session_id or not cart_token:
            continue

        created_at = doc["createdAt"].replace(tzinfo=timezone.utc)
        local_date = created_at.astimezone(tz).date()
        if local_date < start_date or local_date > end_date:
            continue

        pair_key = (local_date, session_id, cart_token)
        pair_events.setdefault(pair_key, []).append(
            {"date": local_date, "variant_ids": _normalize_variant_ids(doc.get("variantId"))}
        )

    unique_events: List[Dict[str, Any]] = []
    for events in pair_events.values():
        if len(events) == 1:
            unique_events.append(events[0])
    return unique_events


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
    rows = _fetch_candidate_rows(cursor, start_date, end_date, tz, event_type=None)

    pair_events: Dict[Tuple[date, str, str, str], List[Dict[str, Any]]] = {}
    for doc in rows:
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
        pair_events.setdefault(pair_key, []).append(
            {
                "date": local_date,
                "event_column": event_column,
                "variant_ids": _normalize_variant_ids(doc.get("variantId")),
            }
        )

    unique_events: List[Dict[str, Any]] = []
    for events in pair_events.values():
        if len(events) == 1:
            unique_events.append(events[0])
    return unique_events


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
    mapping was never going to have rows in mongo_events_summary either."""
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
