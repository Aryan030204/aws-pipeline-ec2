"""
Intent behavioral events ingestion: reads tracking events (product_viewed,
etc.) from a separate MongoDB cluster (INTENT_MONGO_URI, intent_sessions.events
- distinct from MONGO_URI/alerts.sessions used elsewhere in this pipeline)
and ingests them into each applicable brand's own behavioral_events table.

Brand routing is driven entirely by INTENT_DB_MAP (a JSON object mapping the
Mongo document's brand_id to this pipeline's existing DB_DATABASE_<i>
values - the actual per-brand MySQL database name, matched
case-insensitively by the worker), independent of the CI_EVENTS_BRANDS
gating used by pipeline/mongo_events.py.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from pymongo import MongoClient

from pipeline.state import IST, logger
from pipeline.db import (
    get_db_cursor,
    executemany_chunked,
    get_pipeline_metadata_timestamp,
    update_pipeline_metadata_timestamp,
)

INTENT_METADATA_KEY = "intent_events_last_occurred_at"
INTENT_OVERLAP = timedelta(minutes=5)
INTENT_DEFAULT_LOOKBACK = timedelta(hours=1)

# Table's fixed, nullable columns pulled from the nested `raw` payload - not
# dynamic/on-the-fly like variant_wise_events_summary's event columns.
_RAW_NUMERIC_FIELDS = {"quantity", "price", "checkout_total", "scroll_percent"}
_RAW_FIELDS = (
    "product_id",
    "variant_id",
    "product_title",
    "variant_title",
    "quantity",
    "price",
    "currency",
    "checkout_total",
    "scroll_percent",
)


def _resolve_intent_events_collection():
    mongo_uri = os.environ.get("INTENT_MONGO_URI")
    if not mongo_uri:
        raise ValueError("INTENT_MONGO_URI is not configured")

    client = MongoClient(mongo_uri, tz_aware=True)
    return client, client["intent_sessions"]["events"]


def _parse_intent_db_map() -> Dict[str, str]:
    raw = os.environ.get("INTENT_DB_MAP", "{}")
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        logger.warning("Invalid JSON in INTENT_DB_MAP. Using empty mapping.")
        return {}

    if not isinstance(parsed, dict):
        logger.warning("INTENT_DB_MAP must be a JSON object. Using empty mapping.")
        return {}

    return {str(k): str(v) for k, v in parsed.items()}


def _coerce_numeric(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_behavioral_event_row(doc: Dict[str, Any]) -> Optional[Tuple]:
    event_id = str(doc.get("event_id") or "").strip()
    occurred_at = doc.get("occurred_at")
    if not event_id or not isinstance(occurred_at, datetime):
        logger.warning("Skipping intent event with missing event_id/occurred_at")
        return None

    raw = doc.get("raw") or {}
    if not isinstance(raw, dict):
        raw = {}

    raw_values = {}
    for field in _RAW_FIELDS:
        value = raw.get(field)
        if field in _RAW_NUMERIC_FIELDS:
            value = _coerce_numeric(value)
        elif value is not None:
            value = str(value)
        raw_values[field] = value

    return (
        event_id,
        str(doc.get("event_name") or "").strip() or None,
        (str(doc.get("client_id")).strip() or None) if doc.get("client_id") is not None else None,
        (str(doc.get("visitor_id")).strip() or None) if doc.get("visitor_id") is not None else None,
        (str(doc.get("session_id")).strip() or None) if doc.get("session_id") is not None else None,
        doc.get("url") or None,
        doc.get("referrer") or None,
        doc.get("user_agent") or None,
        raw_values["product_id"],
        raw_values["variant_id"],
        raw_values["product_title"],
        raw_values["variant_title"],
        raw_values["quantity"],
        raw_values["price"],
        raw_values["currency"],
        raw_values["checkout_total"],
        raw_values["scroll_percent"],
        json.dumps(raw, default=str),
        occurred_at,
    )


def _ensure_behavioral_events_table(cursor, connection) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS behavioral_events (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

            event_id VARCHAR(100) NOT NULL,
            event_name VARCHAR(100) NOT NULL,

            client_id VARCHAR(100) NULL,
            visitor_id VARCHAR(100) NULL,
            session_id VARCHAR(100) NULL,

            url TEXT NULL,
            referrer TEXT NULL,
            user_agent TEXT NULL,

            product_id VARCHAR(100) NULL,
            variant_id VARCHAR(100) NULL,
            product_title VARCHAR(500) NULL,
            variant_title VARCHAR(500) NULL,

            quantity INT NULL,
            price DECIMAL(18, 4) NULL,
            currency VARCHAR(10) NULL,

            checkout_total DECIMAL(18, 4) NULL,

            scroll_percent DECIMAL(5, 2) NULL,

            data JSON NULL,

            occurred_at DATETIME(6) NOT NULL,
            ingested_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

            PRIMARY KEY (id),
            UNIQUE KEY uq_event_id (event_id),
            KEY idx_occurred_at (occurred_at),
            KEY idx_session_time (session_id, occurred_at),
            KEY idx_client_time (client_id, occurred_at),
            KEY idx_event_time (event_name, occurred_at),
            KEY idx_product_time (product_id, occurred_at),
            KEY idx_variant_time (variant_id, occurred_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    connection.commit()


_UPSERT_COLUMNS = (
    "event_id",
    "event_name",
    "client_id",
    "visitor_id",
    "session_id",
    "url",
    "referrer",
    "user_agent",
    "product_id",
    "variant_id",
    "product_title",
    "variant_title",
    "quantity",
    "price",
    "currency",
    "checkout_total",
    "scroll_percent",
    "data",
    "occurred_at",
)
# ingested_at is deliberately excluded from the UPDATE clause: it should
# reflect the event's *first* ingestion, not be reset by a later re-upsert
# of the same event_id (e.g. the watermark-overlap safety margin).
_UPDATE_COLUMNS = tuple(c for c in _UPSERT_COLUMNS if c != "event_id")


def _upsert_behavioral_events(cursor, connection, rows: List[Tuple]) -> int:
    if not rows:
        return 0

    columns_sql = ", ".join(_UPSERT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(_UPSERT_COLUMNS))
    update_sql = ", ".join(f"{c} = VALUES({c})" for c in _UPDATE_COLUMNS)
    sql = (
        f"INSERT INTO behavioral_events ({columns_sql}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_sql}"
    )
    return executemany_chunked(cursor, connection, sql, rows)


def sync_intent_events_for_brand(
    brand_index: int,
    mongo_brand_id: str,
    brand_label: str,
    cursor=None,
    connection=None,
) -> None:
    client = None
    try:
        client, collection = _resolve_intent_events_collection()

        def _do_sync(c, conn):
            _ensure_behavioral_events_table(c, conn)

            watermark = get_pipeline_metadata_timestamp(
                c, INTENT_METADATA_KEY, default=datetime.now(IST) - INTENT_DEFAULT_LOOKBACK
            )
            query_lower_bound = (watermark - INTENT_OVERLAP).astimezone(timezone.utc)

            docs = collection.find(
                {
                    "brand_id": mongo_brand_id,
                    "occurred_at": {"$gt": query_lower_bound},
                }
            ).sort("occurred_at", 1)

            rows: List[Tuple] = []
            max_occurred_at: Optional[datetime] = None
            for doc in docs:
                row = _extract_behavioral_event_row(doc)
                if row is None:
                    continue
                rows.append(row)
                occurred_at = row[-1]
                if occurred_at.tzinfo is None:
                    occurred_at = occurred_at.replace(tzinfo=timezone.utc)
                if max_occurred_at is None or occurred_at > max_occurred_at:
                    max_occurred_at = occurred_at

            inserted = _upsert_behavioral_events(c, conn, rows)

            if max_occurred_at is not None:
                update_pipeline_metadata_timestamp(
                    c, conn, INTENT_METADATA_KEY, max_occurred_at.astimezone(IST)
                )

            logger.info(
                f"Ingested intent behavioral events for brand={brand_label} "
                f"(mongo_brand_id={mongo_brand_id}): {inserted} row(s)"
            )

        if cursor is not None:
            _do_sync(cursor, connection)
        else:
            with get_db_cursor(brand_index) as (c, conn):
                _do_sync(c, conn)
    except Exception as e:
        logger.error(
            f"Error ingesting intent behavioral events for brand={brand_label} "
            f"(mongo_brand_id={mongo_brand_id}): {e}"
        )
    finally:
        if client is not None:
            client.close()
