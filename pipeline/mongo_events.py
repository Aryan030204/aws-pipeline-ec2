"""
Shared CI-events brand gating, Mongo connection helper, and the pure-SQL
table/upsert maintenance for checkout-initiated/buy-now/variant-wise event
rollups (hourly_sessions_summary_shopify, daily_ci_events_summary,
daily_buy_now_events_summary, variant_wise_events_summary, overall_summary's
event-total columns).

The Mongo-querying sync functions that used to live here (sync_checkout_
initiated_ci_events, sync_buy_now_events, their variant counterparts,
sync_variant_wise_events_summary, update_overall_summary_event_columns) have
moved to pipeline/mongo_events_sql.py, which reads the same brand's own
mongo_events_summary table (MySQL) instead of querying alerts.sessions
(Mongo) directly - see that module's docstring. pipeline/mongo_events_raw.py
is now the only thing in this pipeline that still queries alerts.sessions.
The upsert/table-maintenance functions below never touched Mongo in the
first place, so they're unchanged and reused as-is by mongo_events_sql.py.
"""

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
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
    "ajmalbh",
    "urban_hindu",
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
    "urban_hindu": "URBAN HINDU"
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


# ---------------------------
# Variant-wise events summary (dynamic event types) - table/upsert helpers
# ---------------------------
VARIANT_WISE_EVENTS_TABLE = "variant_wise_events_summary"
EVENT_COLUMN_SUFFIX = "_count"


def _sanitize_event_type_column(event_type: Any) -> Optional[str]:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(event_type or "").strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized:
        return None
    return f"{normalized}{EVENT_COLUMN_SUFFIX}"


def _quote_identifier(identifier: str) -> str:
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", identifier or ""):
        raise ValueError(f"Unsafe SQL identifier: {identifier!r}")
    return f"`{identifier}`"


def _ensure_variant_wise_events_summary_table(cursor, connection) -> None:
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_quote_identifier(VARIANT_WISE_EVENTS_TABLE)} (
            `date` DATE NOT NULL,
            `variant_id` VARCHAR(255) NOT NULL,
            `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`date`, `variant_id`),
            KEY `idx_date` (`date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    connection.commit()


def _get_existing_variant_wise_columns(cursor) -> Set[str]:
    cursor.execute(f"SHOW COLUMNS FROM {_quote_identifier(VARIANT_WISE_EVENTS_TABLE)}")
    return {
        str(row["Field"] if isinstance(row, dict) else row[0])
        for row in cursor.fetchall() or []
    }


def _ensure_variant_wise_event_columns(cursor, connection, event_columns) -> None:
    existing_columns = _get_existing_variant_wise_columns(cursor)
    missing_columns = sorted(set(event_columns) - existing_columns)
    for column in missing_columns:
        cursor.execute(
            f"ALTER TABLE {_quote_identifier(VARIANT_WISE_EVENTS_TABLE)} "
            f"ADD COLUMN {_quote_identifier(column)} INT NOT NULL DEFAULT 0"
        )
    if missing_columns:
        connection.commit()


def _upsert_variant_wise_events_summary(
    cursor,
    connection,
    start_date: date,
    end_date: date,
    aggregated: Dict[Tuple[date, str], Dict[str, int]],
    event_columns,
) -> int:
    if not event_columns:
        return 0

    _ensure_variant_wise_event_columns(cursor, connection, event_columns)

    cursor.execute(
        f"DELETE FROM {_quote_identifier(VARIANT_WISE_EVENTS_TABLE)} "
        f"WHERE `date` BETWEEN %s AND %s",
        (start_date, end_date),
    )

    sorted_columns = sorted(event_columns)
    quoted_columns = ", ".join(_quote_identifier(c) for c in sorted_columns)
    placeholders = ", ".join(["%s"] * (2 + len(sorted_columns)))
    update_clause = ", ".join(
        f"{_quote_identifier(c)} = VALUES({_quote_identifier(c)})" for c in sorted_columns
    )
    sql = (
        f"INSERT INTO {_quote_identifier(VARIANT_WISE_EVENTS_TABLE)} "
        f"(`date`, `variant_id`, {quoted_columns}) "
        f"VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_clause}"
    )

    rows = [
        (bucket_date, variant_id, *[counts.get(c, 0) for c in sorted_columns])
        for (bucket_date, variant_id), counts in sorted(aggregated.items())
    ]
    if rows:
        cursor.executemany(sql, rows)
    connection.commit()
    return len(rows)
