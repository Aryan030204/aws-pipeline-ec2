"""
Checkout-initiated / buy-now Mongo event sync: dedupe pair-events by
(date, session_id, cart_token), fetch hourly and per-variant counts, and
upsert into hourly_sessions_summary_shopify / daily_ci_events_summary /
daily_buy_now_events_summary, plus the overall_summary event-total
columns.
"""

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from pymongo import MongoClient

from pipeline.state import IST, logger
from pipeline.db import get_db_cursor

# aws_background still owns MONGO_URI and get_brand_timezone
# (brand_config_init/misc_utils, used broadly across categories). Imported
# at module level here is safe: attribute access only happens when these
# functions are actually CALLED, during real pipeline execution, long
# after aws_background.py has fully finished loading (see
# pipeline/shopifyql_sessions.py and others for the same pattern).
import aws_background


def _normalize_brand_token(value: Optional[str]) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").strip().lower())


CI_EVENTS_BRANDS = {
    "ajmal",
    "tmc",
    "pts",
    "bbb",
    "vama",
    "3tenx",
    "shylenew",
    "ajmalksa",
    "ajmaluk",
    "ajmalus",
    "ajmalbh"
}

CI_EVENTS_MONGO_SHOP_NAME_MAP = {
    "ajmal": "Ajmal_IN",
    "ajmalksa": "ajmal_ksa",
    "ajmaluk": "ajmal_uk",
    "ajmalus": "ajmal_us",
    "ajmalbh": "ajmal_bh",
    "tmc": "tmc",
    "pts": "pts",
    "bbb": "bbb",
    "vama": "vama",
    "3tenx": "3tenx",
    "shylenew": "shyle",
}


def is_ci_events_brand(
    brand_name: Optional[str], brand_key: Optional[str] = None
) -> bool:
    normalized_tokens = {
        _normalize_brand_token(brand_name),
        _normalize_brand_token(brand_key),
    }
    return any(token in CI_EVENTS_BRANDS for token in normalized_tokens)


def get_ci_events_mongo_shop_name(
    brand_name: Optional[str], brand_key: Optional[str] = None
) -> Optional[str]:
    normalized_tokens = (
        _normalize_brand_token(brand_key),
        _normalize_brand_token(brand_name),
    )
    for token in normalized_tokens:
        if token in CI_EVENTS_MONGO_SHOP_NAME_MAP:
            return CI_EVENTS_MONGO_SHOP_NAME_MAP[token]
    return None


def _resolve_mongo_sessions_collection():
    if not aws_background.MONGO_URI:
        raise ValueError("MONGO_URI is not configured")

    client = MongoClient(aws_background.MONGO_URI, tz_aware=True)
    return client, client["alerts"]["sessions"]


def _ist_date_range_to_utc_bounds(
    start_date: date, end_date: date, tz: ZoneInfo = IST
) -> Tuple[datetime, datetime]:
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


def _normalize_variant_ids(variant_val: Any) -> List[str]:
    variant_ids: List[str] = []
    if isinstance(variant_val, list):
        variant_ids = [str(v).strip() for v in variant_val if v is not None]
    elif isinstance(variant_val, (int, float)):
        variant_ids = [str(int(variant_val)).strip()]
    elif variant_val is not None:
        val_str = str(variant_val).strip()
        if val_str:
            variant_ids = [v.strip() for v in val_str.split(",") if v.strip()]
    return sorted(set(variant_ids))


def _iter_counted_pair_events_from_mongo(
    start_date: date,
    end_date: date,
    mongo_shop_name: str,
    event_type: str,
    tz: ZoneInfo = IST,
    dedupe_window_minutes: int = 30,
) -> List[Dict[str, Any]]:
    start_utc, end_utc = _ist_date_range_to_utc_bounds(start_date, end_date, tz)
    dedupe_window = timedelta(minutes=dedupe_window_minutes)
    client = None
    try:
        client, collection = _resolve_mongo_sessions_collection()
        cursor = (
            collection.find(
                {
                    "event_type": event_type,
                    "shop_name": mongo_shop_name,
                    "createdAt": {"$gte": start_utc, "$lt": end_utc},
                },
                {"createdAt": 1, "session_id": 1, "cart_token": 1, "variantId": 1},
            )
            .sort("createdAt", 1)
        )

        counted_events: List[Dict[str, Any]] = []
        last_counted_at_by_pair: Dict[Tuple[date, str, str], datetime] = {}
        for doc in cursor:
            session_id = _extract_session_id(doc)
            cart_token = _extract_cart_token(doc)
            if not session_id or not cart_token:
                continue

            created_at = doc.get("createdAt")
            if not isinstance(created_at, datetime):
                continue
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)

            local_created_at = created_at.astimezone(tz)
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
    finally:
        if client is not None:
            client.close()


def _iter_unique_daily_pair_events_from_mongo(
    start_date: date,
    end_date: date,
    mongo_shop_name: str,
    event_type: str,
    tz: ZoneInfo = IST,
) -> List[Dict[str, Any]]:
    start_utc, end_utc = _ist_date_range_to_utc_bounds(start_date, end_date, tz)
    client = None
    try:
        client, collection = _resolve_mongo_sessions_collection()
        cursor = collection.find(
            {
                "event_type": event_type,
                "shop_name": mongo_shop_name,
                "createdAt": {"$gte": start_utc, "$lt": end_utc},
            },
            {"createdAt": 1, "session_id": 1, "cart_token": 1, "variantId": 1},
        )

        pair_events: Dict[Tuple[date, str, str], List[Dict[str, Any]]] = {}
        for doc in cursor:
            session_id = _extract_session_id(doc)
            cart_token = _extract_cart_token(doc)
            if not session_id or not cart_token:
                continue

            created_at = doc.get("createdAt")
            if not isinstance(created_at, datetime):
                continue
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)

            local_created_at = created_at.astimezone(tz)
            local_date = local_created_at.date()
            if local_date < start_date or local_date > end_date:
                continue

            pair_key = (local_date, session_id, cart_token)
            pair_events.setdefault(pair_key, []).append(
                {
                    "date": local_date,
                    "variant_ids": _normalize_variant_ids(doc.get("variantId")),
                }
            )

        unique_events: List[Dict[str, Any]] = []
        for events in pair_events.values():
            if len(events) == 1:
                unique_events.append(events[0])

        return unique_events
    finally:
        if client is not None:
            client.close()


def _fetch_daily_unique_pair_event_counts_from_mongo(
    start_date: date,
    end_date: date,
    mongo_shop_name: str,
    event_type: str,
    tz: ZoneInfo = IST,
) -> Dict[date, int]:
    counts: Dict[date, int] = {}
    unique_events = _iter_unique_daily_pair_events_from_mongo(
        start_date=start_date,
        end_date=end_date,
        mongo_shop_name=mongo_shop_name,
        event_type=event_type,
        tz=tz,
    )
    for event in unique_events:
        event_date = event["date"]
        counts[event_date] = counts.get(event_date, 0) + 1
    return counts


def _upsert_overall_summary_event_totals(
    cursor,
    connection,
    start_date: date,
    end_date: date,
    ci_counts: Dict[date, int],
    buy_now_counts: Dict[date, int],
):
    all_dates = []
    current = start_date
    while current <= end_date:
        all_dates.append(current)
        current += timedelta(days=1)

    for bucket_date in all_dates:
        cursor.execute(
            """
            INSERT INTO overall_summary (date, ci_events, buy_now_events)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                ci_events = VALUES(ci_events),
                buy_now_events = VALUES(buy_now_events)
            """,
            (
                bucket_date,
                int(ci_counts.get(bucket_date, 0)),
                int(buy_now_counts.get(bucket_date, 0)),
            ),
        )

    connection.commit()


def update_overall_summary_event_columns(
    brand_index: int,
    brand_name: str,
    start_date: date,
    end_date: date,
    brand_key: Optional[str] = None,
    cursor=None,
    connection=None,
):
    if not is_ci_events_brand(brand_name, brand_key):
        logger.info(
            f"Skipping overall_summary event totals for {brand_name}: brand not enabled for Mongo event sync"
        )
        return

    mongo_shop_name = get_ci_events_mongo_shop_name(brand_name, brand_key)
    if not mongo_shop_name:
        logger.info(
            f"Skipping overall_summary event totals for {brand_name}: no Mongo shop_name mapping"
        )
        return

    tz = aws_background.get_brand_timezone(brand_index)
    try:
        ci_counts = _fetch_daily_unique_pair_event_counts_from_mongo(
            start_date=start_date,
            end_date=end_date,
            mongo_shop_name=mongo_shop_name,
            event_type="checkout_initiated",
            tz=tz,
        )
        buy_now_counts = _fetch_daily_unique_pair_event_counts_from_mongo(
            start_date=start_date,
            end_date=end_date,
            mongo_shop_name=mongo_shop_name,
            event_type="buy_now",
            tz=tz,
        )
    except Exception as e:
        logger.error(
            f"Error fetching overall_summary event totals from Mongo for {brand_name}: {e}"
        )
        return

    if cursor is not None:
        _upsert_overall_summary_event_totals(
            cursor, connection, start_date, end_date, ci_counts, buy_now_counts
        )
        logger.info(
            f"Updated overall_summary event totals for {brand_name} ({start_date}..{end_date})"
        )
        return

    try:
        with get_db_cursor(brand_index) as (c, conn):
            _upsert_overall_summary_event_totals(
                c, conn, start_date, end_date, ci_counts, buy_now_counts
            )
        logger.info(
            f"Updated overall_summary event totals for {brand_name} ({start_date}..{end_date})"
        )
    except Exception as e:
        logger.error(f"Error updating overall_summary event totals for {brand_name}: {e}")


def _fetch_event_hourly_counts_from_mongo(
    start_date: date,
    end_date: date,
    mongo_shop_name: str,
    event_type: str,
    metric_key: str,
    tz: ZoneInfo = IST,
) -> List[Dict[str, Any]]:
    counts: Dict[Tuple[date, int], int] = {}
    counted_events = _iter_counted_pair_events_from_mongo(
        start_date=start_date,
        end_date=end_date,
        mongo_shop_name=mongo_shop_name,
        event_type=event_type,
        tz=tz,
    )

    for event in counted_events:
        bucket_key = (event["date"], event["hour"])
        counts[bucket_key] = counts.get(bucket_key, 0) + 1

    return [
        {"date": bucket_date, "hour": bucket_hour, metric_key: count}
        for (bucket_date, bucket_hour), count in sorted(counts.items())
    ]


def fetch_checkout_initiated_hourly_counts_from_mongo(
    start_date: date,
    end_date: date,
    mongo_shop_name: str,
    tz: ZoneInfo = IST,
) -> List[Dict[str, Any]]:
    return _fetch_event_hourly_counts_from_mongo(
        start_date=start_date,
        end_date=end_date,
        mongo_shop_name=mongo_shop_name,
        event_type="checkout_initiated",
        metric_key="ci_events",
        tz=tz,
    )


def fetch_buy_now_hourly_counts_from_mongo(
    start_date: date,
    end_date: date,
    mongo_shop_name: str,
    tz: ZoneInfo = IST,
) -> List[Dict[str, Any]]:
    return _fetch_event_hourly_counts_from_mongo(
        start_date=start_date,
        end_date=end_date,
        mongo_shop_name=mongo_shop_name,
        event_type="buy_now",
        metric_key="buy_now_events",
        tz=tz,
    )


def _do_upsert_hourly_event_counts(
    results: List[Dict[str, Any]],
    cursor,
    connection,
    brand_name: str,
    start_date: date,
    end_date: date,
    column_name: str,
    success_label: str,
):
    cursor.execute(
        f"""
        UPDATE hourly_sessions_summary_shopify
        SET {column_name} = 0
        WHERE date BETWEEN %s AND %s
        """,
        (start_date, end_date),
    )

    for row in results:
        cursor.execute(
            f"""
            INSERT INTO hourly_sessions_summary_shopify (date, hour, {column_name})
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                {column_name} = VALUES({column_name})
            """,
            (row["date"], row["hour"], row[column_name]),
        )

    connection.commit()
    logger.info(
        f"Updated {success_label} for {brand_name} "
        f"({len(results)} hour buckets, {start_date}..{end_date})"
    )


def sync_checkout_initiated_ci_events(
    brand_index: int,
    brand_name: str,
    start_date: date,
    end_date: date,
    brand_key: Optional[str] = None,
    cursor=None,
    connection=None,
):
    if not is_ci_events_brand(brand_name, brand_key):
        logger.info(
            f"Skipping checkout-initiated Mongo sync for {brand_name}: brand not enabled for ci_events"
        )
        return

    mongo_shop_name = get_ci_events_mongo_shop_name(brand_name, brand_key)
    if not mongo_shop_name:
        logger.info(
            f"Skipping checkout-initiated Mongo sync for {brand_name}: no Mongo shop_name mapping"
        )
        return

    tz = aws_background.get_brand_timezone(brand_index)
    try:
        results = fetch_checkout_initiated_hourly_counts_from_mongo(
            start_date=start_date,
            end_date=end_date,
            mongo_shop_name=mongo_shop_name,
            tz=tz,
        )
    except Exception as e:
        logger.error(
            f"Error fetching checkout-initiated Mongo events for {brand_name}: {e}"
        )
        return

    if cursor is not None:
        _do_upsert_hourly_event_counts(
            results,
            cursor,
            connection,
            brand_name,
            start_date,
            end_date,
            "ci_events",
            "checkout-initiated ci_events",
        )
        return

    try:
        with get_db_cursor(brand_index) as (c, conn):
            _do_upsert_hourly_event_counts(
                results,
                c,
                conn,
                brand_name,
                start_date,
                end_date,
                "ci_events",
                "checkout-initiated ci_events",
            )
    except Exception as e:
        logger.error(f"Error updating checkout-initiated ci_events for {brand_name}: {e}")


def sync_buy_now_events(
    brand_index: int,
    brand_name: str,
    start_date: date,
    end_date: date,
    brand_key: Optional[str] = None,
    cursor=None,
    connection=None,
):
    if not is_ci_events_brand(brand_name, brand_key):
        logger.info(
            f"Skipping buy-now Mongo sync for {brand_name}: brand not enabled for buy_now_events"
        )
        return

    mongo_shop_name = get_ci_events_mongo_shop_name(brand_name, brand_key)
    if not mongo_shop_name:
        logger.info(
            f"Skipping buy-now Mongo sync for {brand_name}: no Mongo shop_name mapping"
        )
        return

    tz = aws_background.get_brand_timezone(brand_index)
    try:
        results = fetch_buy_now_hourly_counts_from_mongo(
            start_date=start_date,
            end_date=end_date,
            mongo_shop_name=mongo_shop_name,
            tz=tz,
        )
    except Exception as e:
        logger.error(f"Error fetching buy-now Mongo events for {brand_name}: {e}")
        return

    if cursor is not None:
        _do_upsert_hourly_event_counts(
            results,
            cursor,
            connection,
            brand_name,
            start_date,
            end_date,
            "buy_now_events",
            "buy-now events",
        )
        return

    try:
        with get_db_cursor(brand_index) as (c, conn):
            _do_upsert_hourly_event_counts(
                results,
                c,
                conn,
                brand_name,
                start_date,
                end_date,
                "buy_now_events",
                "buy-now events",
            )
    except Exception as e:
        logger.error(f"Error updating buy-now events for {brand_name}: {e}")


def _fetch_event_variant_counts_from_mongo(
    start_date: date,
    end_date: date,
    mongo_shop_name: str,
    event_type: str,
    metric_key: str,
    tz: ZoneInfo = IST,
) -> List[Dict[str, Any]]:
    counts: Dict[Tuple[date, str], int] = {}
    counted_events = _iter_unique_daily_pair_events_from_mongo(
        start_date=start_date,
        end_date=end_date,
        mongo_shop_name=mongo_shop_name,
        event_type=event_type,
        tz=tz,
    )

    for event in counted_events:
        event_date = event["date"]
        for vid in event["variant_ids"]:
            key = (event_date, vid)
            counts[key] = counts.get(key, 0) + 1

    return [
        {
            "date": bucket_date,
            "variant_id": vid,
            metric_key: count,
        }
        for (bucket_date, vid), count in sorted(counts.items())
    ]


def fetch_checkout_initiated_variant_counts_from_mongo(
    start_date: date,
    end_date: date,
    mongo_shop_name: str,
    tz: ZoneInfo = IST,
) -> List[Dict[str, Any]]:
    return _fetch_event_variant_counts_from_mongo(
        start_date=start_date,
        end_date=end_date,
        mongo_shop_name=mongo_shop_name,
        event_type="checkout_initiated",
        metric_key="checkout_initiated_count",
        tz=tz,
    )


def fetch_buy_now_variant_counts_from_mongo(
    start_date: date,
    end_date: date,
    mongo_shop_name: str,
    tz: ZoneInfo = IST,
) -> List[Dict[str, Any]]:
    return _fetch_event_variant_counts_from_mongo(
        start_date=start_date,
        end_date=end_date,
        mongo_shop_name=mongo_shop_name,
        event_type="buy_now",
        metric_key="buy_now_count",
        tz=tz,
    )


def _do_upsert_variant_event_counts(
    results: List[Dict[str, Any]],
    cursor,
    connection,
    brand_name: str,
    start_date: date,
    end_date: date,
    table_name: str,
    column_name: str,
    success_label: str,
):
    cursor.execute(
        f"""
        DELETE FROM {table_name}
        WHERE date BETWEEN %s AND %s
        """,
        (start_date, end_date),
    )

    for row in results:
        cursor.execute(
            f"""
            INSERT INTO {table_name} (date, variant_id, {column_name})
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                {column_name} = VALUES({column_name})
            """,
            (row["date"], row["variant_id"], row[column_name]),
        )

    connection.commit()
    logger.info(
        f"Updated {success_label} for {brand_name} "
        f"({len(results)} variant-date rows, {start_date}..{end_date})"
    )


def sync_checkout_initiated_variant_events(
    brand_index: int,
    brand_name: str,
    start_date: date,
    end_date: date,
    brand_key: Optional[str] = None,
    cursor=None,
    connection=None,
):
    if not is_ci_events_brand(brand_name, brand_key):
        logger.info(
            f"Skipping checkout-initiated variant Mongo sync for {brand_name}: brand not enabled for ci_events"
        )
        return

    mongo_shop_name = get_ci_events_mongo_shop_name(brand_name, brand_key)
    if not mongo_shop_name:
        logger.info(
            f"Skipping checkout-initiated variant Mongo sync for {brand_name}: no Mongo shop_name mapping"
        )
        return

    tz = aws_background.get_brand_timezone(brand_index)
    try:
        results = fetch_checkout_initiated_variant_counts_from_mongo(
            start_date=start_date,
            end_date=end_date,
            mongo_shop_name=mongo_shop_name,
            tz=tz,
        )
    except Exception as e:
        logger.error(
            f"Error fetching checkout-initiated variant Mongo events for {brand_name}: {e}"
        )
        return

    if cursor is not None:
        _do_upsert_variant_event_counts(
            results,
            cursor,
            connection,
            brand_name,
            start_date,
            end_date,
            "daily_ci_events_summary",
            "checkout_initiated_count",
            "checkout-initiated variant events",
        )
        return

    try:
        with get_db_cursor(brand_index) as (c, conn):
            _do_upsert_variant_event_counts(
                results,
                c,
                conn,
                brand_name,
                start_date,
                end_date,
                "daily_ci_events_summary",
                "checkout_initiated_count",
                "checkout-initiated variant events",
            )
    except Exception as e:
        logger.error(f"Error updating checkout-initiated variant events for {brand_name}: {e}")


def sync_buy_now_variant_events(
    brand_index: int,
    brand_name: str,
    start_date: date,
    end_date: date,
    brand_key: Optional[str] = None,
    cursor=None,
    connection=None,
):
    if not is_ci_events_brand(brand_name, brand_key):
        logger.info(
            f"Skipping buy-now variant Mongo sync for {brand_name}: brand not enabled for buy_now_events"
        )
        return

    mongo_shop_name = get_ci_events_mongo_shop_name(brand_name, brand_key)
    if not mongo_shop_name:
        logger.info(
            f"Skipping buy-now variant Mongo sync for {brand_name}: no Mongo shop_name mapping"
        )
        return

    tz = aws_background.get_brand_timezone(brand_index)
    try:
        results = fetch_buy_now_variant_counts_from_mongo(
            start_date=start_date,
            end_date=end_date,
            mongo_shop_name=mongo_shop_name,
            tz=tz,
        )
    except Exception as e:
        logger.error(f"Error fetching buy-now variant Mongo events for {brand_name}: {e}")
        return

    if cursor is not None:
        _do_upsert_variant_event_counts(
            results,
            cursor,
            connection,
            brand_name,
            start_date,
            end_date,
            "daily_buy_now_events_summary",
            "buy_now_count",
            "buy-now variant events",
        )
        return

    try:
        with get_db_cursor(brand_index) as (c, conn):
            _do_upsert_variant_event_counts(
                results,
                c,
                conn,
                brand_name,
                start_date,
                end_date,
                "daily_buy_now_events_summary",
                "buy_now_count",
                "buy-now variant events",
            )
    except Exception as e:
        logger.error(f"Error updating buy-now variant events for {brand_name}: {e}")
