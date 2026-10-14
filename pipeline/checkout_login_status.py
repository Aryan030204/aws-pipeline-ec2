"""
Daily checkout login-status rollups: for every deduplicated checkout_initiated
(date, session_id, cart_token) pair, classify it as logged-in or
non-logged-in based on whether a login event with the same cart_token
occurred in the same window, then upsert daily and daily+variant summaries.

Reads from this brand's own mongo_events_summary table (MySQL), not Mongo
directly - pipeline/mongo_events_raw.py is the only thing that still queries
alerts.sessions for these event types (see that module's docstring).
Reuses pipeline.mongo_events_sql's row-fetch/dedup building blocks rather
than duplicating them - only the genuinely new pieces (cart_token-preserving
checkout dedup, login cart-token lookup, classification, and the two new
tables' DDL/upsert) live here.
"""

from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

from pipeline.state import IST, logger
from pipeline.db import get_db_cursor
from pipeline.mongo_events import (
    is_ci_events_brand,
    get_ci_events_mongo_shop_name,
    _normalize_variant_ids,
)
from pipeline.mongo_events_sql import (
    candidate_rows_cache,
    _fetch_candidate_rows,
    _extract_session_id,
    _extract_cart_token,
    _fetch_daily_unique_pair_event_counts_from_sql,
)

import aws_background

DAILY_SUMMARY_TABLE = "daily_checkout_user_summary"
DAILY_VARIANT_SUMMARY_TABLE = "daily_checkout_user_variant_summary"


def _fetch_login_cart_tokens(
    cursor,
    start_date: date,
    end_date: date,
    tz: ZoneInfo = IST,
) -> Set[str]:
    """The cart_tokens that had a login event in the window, from this
    brand's own mongo_events_summary table. Used as an O(1)-lookup set for
    classification, not fetched per-checkout."""
    rows = _fetch_candidate_rows(cursor, start_date, end_date, tz, event_type="login")
    return {
        token
        for row in rows
        for token in [_extract_cart_token(row)]
        if token
    }


def _iter_unique_daily_checkout_pairs(
    cursor,
    start_date: date,
    end_date: date,
    tz: ZoneInfo = IST,
) -> List[Dict[str, Any]]:
    """
    Same "exactly once per day" (date, session_id, cart_token) dedup as
    pipeline.mongo_events_sql._iter_unique_daily_pair_events_from_sql, fixed
    to event_type=checkout_initiated - but this also keeps cart_token in the
    result (the shared helper intentionally drops it; its other callers
    never needed it, and it must stay unchanged for them). cart_token is
    used only in-memory for the login-classification join below - never
    logged or persisted.
    """
    import datetime as _datetime_module

    rows = _fetch_candidate_rows(cursor, start_date, end_date, tz, event_type="checkout_initiated")

    pair_events: Dict[Tuple[date, str, str], List[Dict[str, Any]]] = {}
    for doc in rows:
        session_id = _extract_session_id(doc)
        cart_token = _extract_cart_token(doc)
        if not session_id or not cart_token:
            continue

        created_at = doc.get("createdAt")
        if not isinstance(created_at, _datetime_module.datetime):
            continue
        local_created_at = created_at.replace(tzinfo=_datetime_module.timezone.utc).astimezone(tz)
        local_date = local_created_at.date()
        if local_date < start_date or local_date > end_date:
            continue

        pair_key = (local_date, session_id, cart_token)
        pair_events.setdefault(pair_key, []).append(
            {
                "date": local_date,
                "cart_token": cart_token,
                "variant_ids": _normalize_variant_ids(doc.get("variantId")),
            }
        )

    unique_pairs: List[Dict[str, Any]] = []
    for events in pair_events.values():
        if len(events) == 1:
            unique_pairs.append(events[0])

    return unique_pairs


def _classify_and_aggregate(
    cursor,
    start_date: date,
    end_date: date,
    tz: ZoneInfo = IST,
) -> Tuple[Dict[date, Dict[str, int]], Dict[Tuple[date, str], Dict[str, int]]]:
    """
    Returns (daily_totals, variant_totals):
      daily_totals[date] = {total_checkout_events, total_login_events,
                             logged_in_users, non_logged_in_users}
      variant_totals[(date, variant_id)] = {total_checkout_events,
                                             logged_in_users,
                                             non_logged_in_users}
    """
    login_cart_tokens = _fetch_login_cart_tokens(cursor, start_date, end_date, tz)
    unique_pairs = _iter_unique_daily_checkout_pairs(cursor, start_date, end_date, tz)

    daily_totals: Dict[date, Dict[str, int]] = {}
    variant_totals: Dict[Tuple[date, str], Dict[str, int]] = {}

    def _bucket(store: dict, key, keys=("total_checkout_events", "logged_in_users", "non_logged_in_users")):
        return store.setdefault(key, {k: 0 for k in keys})

    for pair in unique_pairs:
        pair_date = pair["date"]
        is_logged_in = pair["cart_token"] in login_cart_tokens

        day_bucket = _bucket(
            daily_totals,
            pair_date,
            keys=("total_checkout_events", "total_login_events", "logged_in_users", "non_logged_in_users"),
        )
        day_bucket["total_checkout_events"] += 1
        day_bucket["logged_in_users" if is_logged_in else "non_logged_in_users"] += 1

        for variant_id in pair["variant_ids"]:
            v_bucket = _bucket(variant_totals, (pair_date, variant_id))
            v_bucket["total_checkout_events"] += 1
            v_bucket["logged_in_users" if is_logged_in else "non_logged_in_users"] += 1

    login_counts = _fetch_daily_unique_pair_event_counts_from_sql(
        cursor, start_date, end_date, "login", tz
    )
    current = start_date
    while current <= end_date:
        bucket = _bucket(
            daily_totals,
            current,
            keys=("total_checkout_events", "total_login_events", "logged_in_users", "non_logged_in_users"),
        )
        bucket["total_login_events"] = login_counts.get(current, 0)
        current += timedelta(days=1)

    return daily_totals, variant_totals


def _validate_invariant(
    brand_name: str,
    daily_totals: Dict[date, Dict[str, int]],
    variant_totals: Dict[Tuple[date, str], Dict[str, int]],
) -> bool:
    for d, totals in daily_totals.items():
        if totals["total_checkout_events"] != totals["logged_in_users"] + totals["non_logged_in_users"]:
            logger.error(
                f"Checkout login-status invariant failed for {brand_name} on {d}: "
                f"total={totals['total_checkout_events']} logged_in={totals['logged_in_users']} "
                f"non_logged_in={totals['non_logged_in_users']}"
            )
            return False
    for (d, variant_id), totals in variant_totals.items():
        if totals["total_checkout_events"] != totals["logged_in_users"] + totals["non_logged_in_users"]:
            logger.error(
                f"Checkout login-status variant invariant failed for {brand_name} on {d} "
                f"variant={variant_id}: total={totals['total_checkout_events']} "
                f"logged_in={totals['logged_in_users']} non_logged_in={totals['non_logged_in_users']}"
            )
            return False
    return True


def _ensure_daily_checkout_user_summary_table(cursor, connection) -> None:
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{DAILY_SUMMARY_TABLE}` (
            `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            `date` DATE NOT NULL,
            `total_checkout_events` INT UNSIGNED NOT NULL DEFAULT 0,
            `total_login_events` INT UNSIGNED NOT NULL DEFAULT 0,
            `logged_in_users` INT UNSIGNED NOT NULL DEFAULT 0,
            `non_logged_in_users` INT UNSIGNED NOT NULL DEFAULT 0,
            `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            UNIQUE KEY `uk_date` (`date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    connection.commit()


def _ensure_daily_checkout_user_variant_summary_table(cursor, connection) -> None:
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{DAILY_VARIANT_SUMMARY_TABLE}` (
            `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            `date` DATE NOT NULL,
            `variant_id` VARCHAR(64) NOT NULL,
            `total_checkout_events` INT UNSIGNED NOT NULL DEFAULT 0,
            `logged_in_users` INT UNSIGNED NOT NULL DEFAULT 0,
            `non_logged_in_users` INT UNSIGNED NOT NULL DEFAULT 0,
            `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            UNIQUE KEY `uk_date_variant` (`date`, `variant_id`),
            KEY `idx_date` (`date`),
            KEY `idx_variant_id` (`variant_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    connection.commit()


def _upsert_daily_checkout_user_summary(
    cursor,
    connection,
    start_date: date,
    end_date: date,
    daily_totals: Dict[date, Dict[str, int]],
) -> int:
    cursor.execute(
        f"DELETE FROM `{DAILY_SUMMARY_TABLE}` WHERE `date` BETWEEN %s AND %s",
        (start_date, end_date),
    )
    rows = [
        (
            d,
            totals["total_checkout_events"],
            totals["total_login_events"],
            totals["logged_in_users"],
            totals["non_logged_in_users"],
        )
        for d, totals in sorted(daily_totals.items())
    ]
    if rows:
        cursor.executemany(
            f"""
            INSERT INTO `{DAILY_SUMMARY_TABLE}`
                (`date`, `total_checkout_events`, `total_login_events`, `logged_in_users`, `non_logged_in_users`)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                `total_checkout_events` = VALUES(`total_checkout_events`),
                `total_login_events` = VALUES(`total_login_events`),
                `logged_in_users` = VALUES(`logged_in_users`),
                `non_logged_in_users` = VALUES(`non_logged_in_users`)
            """,
            rows,
        )
    connection.commit()
    return len(rows)


def _upsert_daily_checkout_user_variant_summary(
    cursor,
    connection,
    start_date: date,
    end_date: date,
    variant_totals: Dict[Tuple[date, str], Dict[str, int]],
) -> int:
    cursor.execute(
        f"DELETE FROM `{DAILY_VARIANT_SUMMARY_TABLE}` WHERE `date` BETWEEN %s AND %s",
        (start_date, end_date),
    )
    rows = [
        (
            d,
            variant_id,
            totals["total_checkout_events"],
            totals["logged_in_users"],
            totals["non_logged_in_users"],
        )
        for (d, variant_id), totals in sorted(variant_totals.items())
    ]
    if rows:
        cursor.executemany(
            f"""
            INSERT INTO `{DAILY_VARIANT_SUMMARY_TABLE}`
                (`date`, `variant_id`, `total_checkout_events`, `logged_in_users`, `non_logged_in_users`)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                `total_checkout_events` = VALUES(`total_checkout_events`),
                `logged_in_users` = VALUES(`logged_in_users`),
                `non_logged_in_users` = VALUES(`non_logged_in_users`)
            """,
            rows,
        )
    connection.commit()
    return len(rows)


def sync_daily_checkout_login_status(
    brand_index: int,
    brand_name: str,
    start_date: date,
    end_date: date,
    brand_key: Optional[str] = None,
    cursor=None,
    connection=None,
) -> None:
    if not is_ci_events_brand(brand_name, brand_key):
        logger.info(
            f"Skipping checkout login-status rollup for {brand_name}: brand not enabled for ci_events"
        )
        return

    mongo_shop_name = get_ci_events_mongo_shop_name(brand_name, brand_key)
    if not mongo_shop_name:
        logger.info(
            f"Skipping checkout login-status rollup for {brand_name}: no Mongo shop_name mapping"
        )
        return

    tz = aws_background.get_brand_timezone(brand_index)

    def _do(c, conn) -> None:
        try:
            # login rows are requested twice (cart-token set + daily
            # counts); fetch them from mongo_events_summary once.
            with candidate_rows_cache():
                daily_totals, variant_totals = _classify_and_aggregate(c, start_date, end_date, tz)
        except Exception as e:
            logger.error(f"Error fetching checkout login-status data for {brand_name}: {e}")
            return

        if not _validate_invariant(brand_name, daily_totals, variant_totals):
            logger.error(
                f"Skipping DB write for checkout login-status rollup ({brand_name}, "
                f"{start_date}..{end_date}) due to invariant failure"
            )
            return

        _ensure_daily_checkout_user_summary_table(c, conn)
        _ensure_daily_checkout_user_variant_summary_table(c, conn)
        daily_rows = _upsert_daily_checkout_user_summary(c, conn, start_date, end_date, daily_totals)
        variant_rows = _upsert_daily_checkout_user_variant_summary(c, conn, start_date, end_date, variant_totals)
        total_checkouts = sum(t["total_checkout_events"] for t in daily_totals.values())
        total_logins = sum(t["total_login_events"] for t in daily_totals.values())
        logged_in = sum(t["logged_in_users"] for t in daily_totals.values())
        non_logged_in = sum(t["non_logged_in_users"] for t in daily_totals.values())
        variant_count = len({vid for (_, vid) in variant_totals.keys()})
        logger.info(
            f"Updated checkout login-status rollup for {brand_name} ({start_date}..{end_date}): "
            f"checkout_events={total_checkouts} login_events={total_logins} "
            f"logged_in={logged_in} non_logged_in={non_logged_in} "
            f"daily_rows={daily_rows} variant_rows={variant_rows} variants={variant_count}"
        )

    if cursor is not None:
        _do(cursor, connection)
        return

    try:
        with get_db_cursor(brand_index) as (c, conn):
            _do(c, conn)
    except Exception as e:
        logger.error(f"Error writing checkout login-status rollup for {brand_name}: {e}")
