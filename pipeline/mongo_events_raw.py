"""
Raw alerts.sessions event ingestion: every document from the main cluster's
alerts.sessions collection is stored, one row per event, in a per-brand
mongo_events_summary table (in the brand's own database, not the master DB).

Unlike the aggregate syncs in pipeline/mongo_events.py (counts only), this
keeps the full payload. The table's schema follows the documents' keys:
- Nested objects are flattened into columns (data.click -> data_click).
- A key never seen before gets its column created automatically (type
  inferred from the first non-null value), and the value is ingested in
  the same run.
- createdAt/updatedAt (UTC in Mongo) become four columns:
  created_at_utc, updated_at_utc, created_at_ist, updated_at_ist.
- _id and idempotency_key are not stored.

Rows are keyed by event_id and upserted, so re-running the same window is
idempotent (no duplicates), and late-arriving events are picked up by the
rolling window the worker/backfill pass in.
"""

import hashlib
import json
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from pipeline.state import IST, logger
from pipeline.db import get_db_cursor, executemany_chunked
from pipeline.mongo_events import (
    _ist_date_range_to_utc_bounds,
    _resolve_mongo_sessions_collection,
    get_ci_events_mongo_shop_name,
    is_ci_events_brand,
)

# aws_background owns get_brand_timezone; attribute access only happens when
# the sync is actually called (same pattern as pipeline/mongo_events.py).
import aws_background

RAW_EVENTS_TABLE = "mongo_events_summary"

# Total-column guardrail (fixed + dynamic). A runaway key producing thousands
# of columns would blow MySQL's row-size/column limits; past this cap, new keys
# are skipped with an error log instead.
MAX_COLUMNS = int(os.environ.get("MONGO_EVENTS_MAX_COLUMNS", "400"))
BATCH_SIZE = int(os.environ.get("MONGO_EVENTS_RAW_BATCH_SIZE", "2000"))

MAX_FLATTEN_DEPTH = 5
MYSQL_MAX_IDENTIFIER_LENGTH = 64
_BIGINT_MIN = -(2**63)
_BIGINT_MAX = 2**63 - 1

# Top-level keys never stored as-is: _id / idempotency_key are deliberately
# dropped; createdAt/updatedAt are replaced by the four *_utc/*_ist columns;
# event_id is the primary key column.
_EXCLUDED_TOP_LEVEL_KEYS = {"_id", "idempotency_key", "createdAt", "updatedAt", "event_id"}

# Columns created up front. Anything else is discovered dynamically.
# variantId is TEXT because it is null / int / list / comma-string in the wild.
_FIXED_COLUMNS: Dict[str, str] = {
    "event_id": "VARCHAR(64) NOT NULL",
    "event_type": "VARCHAR(100) NULL",
    "session_id": "VARCHAR(100) NULL",
    "shop_name": "VARCHAR(100) NULL",
    "cart_token": "VARCHAR(255) NULL",
    "variantId": "TEXT NULL",
    "user_agent": "TEXT NULL",
    "url": "TEXT NULL",
    "created_at_utc": "DATETIME(3) NULL",
    "updated_at_utc": "DATETIME(3) NULL",
    "created_at_ist": "DATETIME(3) NULL",
    "updated_at_ist": "DATETIME(3) NULL",
}

# Sentinel: value cannot be stored in its column's type (stored as NULL).
_MISMATCH = object()


def _quote(identifier: str) -> str:
    """Backtick-quote a column/table name (keys like __v or variantId are
    valid here, unlike under the stricter [a-zA-Z_][a-zA-Z0-9_]* helpers)."""
    return "`" + str(identifier).replace("`", "``") + "`"


def _sanitize_column_name(key: Any) -> str:
    """Keep the Mongo key as-is, replacing only what an identifier can't
    hold; over-long names are truncated with a short hash for uniqueness."""
    raw = str(key)
    cleaned = "".join(ch if (ch.isascii() and (ch.isalnum() or ch in "_$")) else "_" for ch in raw)
    if not cleaned:
        cleaned = "_"
    if len(cleaned) > MYSQL_MAX_IDENTIFIER_LENGTH:
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8]
        cleaned = cleaned[: MYSQL_MAX_IDENTIFIER_LENGTH - 9] + "_" + digest
    return cleaned


def _flatten(doc: Dict[str, Any], prefix: str = "", depth: int = 0) -> Dict[str, Any]:
    """Nested dicts become <parent>_<child> columns (recursively, up to
    MAX_FLATTEN_DEPTH, below which the remaining object is kept as one
    value). Arrays are not flattened - they stay a single value."""
    flat: Dict[str, Any] = {}
    for key, value in doc.items():
        name = f"{prefix}_{key}" if prefix else str(key)
        if isinstance(value, dict) and depth < MAX_FLATTEN_DEPTH:
            flat.update(_flatten(value, name, depth + 1))
        else:
            flat[name] = value
    return flat


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _naive(value: datetime) -> datetime:
    """MySQL DATETIME has no tz and the driver drops it, so store the
    wall-clock of whichever zone the value is already expressed in."""
    return value.replace(tzinfo=None)


def _build_record(doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Flatten one Mongo document into {column_name: raw_value}. Returns None
    when the document lacks event_id or createdAt (can't be keyed/windowed)."""
    event_id = str(doc.get("event_id") or "").strip()
    created_at = doc.get("createdAt")
    if not event_id or not isinstance(created_at, datetime):
        return None

    created_utc = _to_utc(created_at)
    updated_at = doc.get("updatedAt")
    updated_utc = _to_utc(updated_at) if isinstance(updated_at, datetime) else None

    record: Dict[str, Any] = {
        "event_id": event_id,
        "created_at_utc": _naive(created_utc),
        "created_at_ist": _naive(created_utc.astimezone(IST)),
        "updated_at_utc": _naive(updated_utc) if updated_utc else None,
        "updated_at_ist": _naive(updated_utc.astimezone(IST)) if updated_utc else None,
    }

    payload = {k: v for k, v in doc.items() if k not in _EXCLUDED_TOP_LEVEL_KEYS}
    for key, value in _flatten(payload).items():
        record[_sanitize_column_name(key)] = value
    return record


def _infer_sql_type(value: Any) -> str:
    if isinstance(value, bool):  # before int: bool is an int subclass
        return "TINYINT(1)"
    if isinstance(value, int):
        return "BIGINT" if _BIGINT_MIN <= value <= _BIGINT_MAX else "TEXT"
    if isinstance(value, float):
        return "DOUBLE"
    if isinstance(value, datetime):
        return "DATETIME(3)"
    if isinstance(value, (list, tuple, dict)):
        return "JSON"
    # str and anything exotic (ObjectId, Decimal128, ...): TEXT, not VARCHAR -
    # wide flattened tables would exceed MySQL's 65,535-byte row limit.
    return "TEXT"


def _category_from_sql_type(sql_type: str) -> str:
    t = str(sql_type).lower()
    if t.startswith("tinyint"):
        return "bool"
    if t.startswith(("bigint", "int", "smallint", "mediumint")):
        return "int"
    if t.startswith(("double", "float", "decimal")):
        return "float"
    if t.startswith(("datetime", "timestamp")):
        return "datetime"
    if t.startswith("json"):
        return "json"
    return "str"


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _coerce_value(value: Any, category: str) -> Any:
    """Convert a raw Mongo value to what its column type accepts. Returns
    _MISMATCH when it can't (caller stores NULL) - a stray odd value must
    never fail a whole batch."""
    if value is None:
        return None
    if category == "json":
        return json.dumps(value, default=_json_default)
    if category == "str":
        if isinstance(value, str):
            return value
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (list, tuple, dict)):
            return json.dumps(value, default=_json_default)
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)
    if category == "int":
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value if _BIGINT_MIN <= value <= _BIGINT_MAX else _MISMATCH
        if isinstance(value, float) and value.is_integer():
            return int(value)
        return _MISMATCH
    if category == "bool":
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int) and value in (0, 1):
            return value
        return _MISMATCH
    if category == "float":
        if isinstance(value, (bool, int, float)):
            return float(value)
        return _MISMATCH
    if category == "datetime":
        if isinstance(value, datetime):
            return _naive(_to_utc(value)) if value.tzinfo is not None else value
        return _MISMATCH
    return _MISMATCH


class _ColumnCatalog:
    """The table's columns, cached for one brand run: case-insensitive name
    lookup (MySQL column names are case-insensitive, so `variantid` must
    reuse `variantId` rather than collide with it) plus each column's type
    category for coercion. Refreshed only by our own ALTERs."""

    def __init__(self, cursor, connection):
        self._cursor = cursor
        self._connection = connection
        self._columns: Dict[str, Tuple[str, str]] = {}
        self._load()

    def _load(self) -> None:
        self._cursor.execute(f"SHOW COLUMNS FROM {_quote(RAW_EVENTS_TABLE)}")
        for row in self._cursor.fetchall() or []:
            if isinstance(row, dict):
                name, sql_type = row["Field"], row["Type"]
            else:
                name, sql_type = row[0], row[1]
            if isinstance(sql_type, (bytes, bytearray)):
                sql_type = sql_type.decode()
            name = str(name)
            self._columns[name.lower()] = (name, _category_from_sql_type(sql_type))

    def get(self, name: str) -> Optional[Tuple[str, str]]:
        return self._columns.get(name.lower())

    def __len__(self) -> int:
        return len(self._columns)

    def add(self, definitions: List[Tuple[str, str]]) -> List[str]:
        """ALTER TABLE ... ADD COLUMN for each (name, sql_type). Tries one
        statement, falls back to one column at a time so a single bad column
        (e.g. row-size limit) doesn't block the rest. Returns names added."""
        added: List[str] = []
        if not definitions:
            return added

        def _alter(defs: List[Tuple[str, str]]) -> None:
            clauses = ", ".join(f"ADD COLUMN {_quote(n)} {t} NULL" for n, t in defs)
            self._cursor.execute(f"ALTER TABLE {_quote(RAW_EVENTS_TABLE)} {clauses}")
            self._connection.commit()

        try:
            _alter(definitions)
            groups = [definitions]
        except Exception as e:
            logger.warning(
                f"[raw events] combined ALTER for {len(definitions)} new column(s) failed "
                f"({e}); retrying one at a time"
            )
            groups = []
            for definition in definitions:
                try:
                    _alter([definition])
                    groups.append([definition])
                except Exception as single_error:
                    logger.error(
                        f"[raw events] could not add column {definition[0]!r} "
                        f"({definition[1]}): {single_error}"
                    )
        for group in groups:
            for name, sql_type in group:
                self._columns[name.lower()] = (name, _category_from_sql_type(sql_type))
                added.append(name)
        return added


def _ensure_raw_events_table(cursor, connection) -> None:
    fixed = ",\n            ".join(f"{_quote(n)} {t}" for n, t in _FIXED_COLUMNS.items())
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_quote(RAW_EVENTS_TABLE)} (
            {fixed},
            `ingested_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            PRIMARY KEY (`event_id`),
            KEY `idx_event_type` (`event_type`),
            KEY `idx_session_id` (`session_id`),
            KEY `idx_cart_token` (`cart_token`),
            KEY `idx_created_at_utc` (`created_at_utc`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    connection.commit()


def _flush_batch(
    cursor,
    connection,
    catalog: _ColumnCatalog,
    records: List[Dict[str, Any]],
    warned: set,
    log_prefix: str,
) -> int:
    """Discover+create any new columns, coerce values, and upsert one batch."""
    if not records:
        return 0

    # 1. Discover new keys. A key that is null in every record so far creates
    #    no column yet (its type is unknowable); it will once a value appears.
    new_definitions: Dict[str, Tuple[str, str]] = {}
    for record in records:
        for name, value in record.items():
            lower = name.lower()
            if value is None or catalog.get(name) or lower in new_definitions:
                continue
            if len(catalog) + len(new_definitions) >= MAX_COLUMNS:
                if "__cap__" not in warned:
                    warned.add("__cap__")
                    logger.error(
                        f"{log_prefix}: column cap of {MAX_COLUMNS} reached; new keys "
                        f"(e.g. {name!r}) are skipped. Raise MONGO_EVENTS_MAX_COLUMNS if intended."
                    )
                continue
            new_definitions[lower] = (name, _infer_sql_type(value))

    if new_definitions:
        added = catalog.add(list(new_definitions.values()))
        if added:
            logger.info(f"{log_prefix}: discovered and added {len(added)} new column(s): {added}")

    # 2. Map/coerce each record onto real column names.
    mapped_records: List[Dict[str, Any]] = []
    column_order: Dict[str, None] = {name: None for name in _FIXED_COLUMNS}
    for record in records:
        mapped: Dict[str, Any] = {}
        for name, value in record.items():
            entry = catalog.get(name)
            if entry is None:
                continue  # null-only/unknown-type key, capped, or failed ALTER
            actual, category = entry
            coerced = _coerce_value(value, category)
            if coerced is _MISMATCH:
                if actual.lower() not in warned:
                    warned.add(actual.lower())
                    logger.warning(
                        f"{log_prefix}: value {value!r} does not fit column {actual!r} "
                        f"({category}); storing NULL (further mismatches for this column "
                        "this run are not logged)"
                    )
                coerced = None
            mapped[actual] = coerced
            column_order.setdefault(actual, None)
        mapped_records.append(mapped)

    columns = [c for c in column_order if any(c in m for m in mapped_records)]
    if "event_id" not in columns:
        return 0

    rows = [tuple(m.get(c) for c in columns) for m in mapped_records]
    quoted_columns = ", ".join(_quote(c) for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    update_clause = ", ".join(
        f"{_quote(c)} = VALUES({_quote(c)})" for c in columns if c != "event_id"
    )
    sql = (
        f"INSERT INTO {_quote(RAW_EVENTS_TABLE)} ({quoted_columns}) "
        f"VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_clause}"
    )
    return executemany_chunked(cursor, connection, sql, rows)


def compute_raw_rebuild_window(tz: ZoneInfo, current_dt: Optional[datetime] = None) -> Tuple[date, date]:
    """Same rolling "day-before-yesterday .. yesterday" window the other
    daily Mongo workers use, in the brand's own timezone."""
    local_now = current_dt.astimezone(tz) if current_dt else datetime.now(tz)
    today = local_now.date()
    return today - timedelta(days=2), today - timedelta(days=1)


def sync_mongo_events_raw_for_brand(
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
            f"Skipping raw Mongo events sync for {brand_name}: brand not enabled for ci_events"
        )
        return

    mongo_shop_name = get_ci_events_mongo_shop_name(brand_name, brand_key)
    if not mongo_shop_name:
        logger.info(
            f"Skipping raw Mongo events sync for {brand_name}: no Mongo shop_name mapping"
        )
        return

    tz = aws_background.get_brand_timezone(brand_index)
    start_utc, end_utc = _ist_date_range_to_utc_bounds(start_date, end_date, tz)
    log_prefix = f"[raw events] brand={brand_name} (shop_name={mongo_shop_name})"

    def _do_sync(c, conn, collection) -> None:
        started_at = time.monotonic()
        logger.info(
            f"{log_prefix}: starting ingestion for {start_date}..{end_date} ({tz}); "
            f"createdAt in [{start_utc:%Y-%m-%d %H:%M:%S}, {end_utc:%Y-%m-%d %H:%M:%S}) UTC"
        )

        _ensure_raw_events_table(c, conn)
        catalog = _ColumnCatalog(c, conn)
        # Belt and braces for a table created by an older version of this code.
        catalog.add([(n, t.replace(" NOT NULL", "").replace(" NULL", ""))
                     for n, t in _FIXED_COLUMNS.items() if catalog.get(n) is None])

        # No .sort(): order is irrelevant for an idempotent upsert, and an
        # unindexed sort fails past Mongo's 32MB in-memory limit. Streamed in
        # batches (not list(cursor)) - raw payloads are large.
        docs = collection.find(
            {"shop_name": mongo_shop_name, "createdAt": {"$gte": start_utc, "$lt": end_utc}},
            {"_id": 0},
            batch_size=BATCH_SIZE,
        )

        warned: set = set()
        batch: List[Dict[str, Any]] = []
        seen = skipped = upserted = 0
        for doc in docs:
            seen += 1
            record = _build_record(doc)
            if record is None:
                skipped += 1
                continue
            batch.append(record)
            if len(batch) >= BATCH_SIZE:
                upserted += _flush_batch(c, conn, catalog, batch, warned, log_prefix)
                batch = []
                logger.info(
                    f"{log_prefix}: read {seen} document(s), upserted {upserted} so far "
                    f"({time.monotonic() - started_at:.1f}s elapsed)"
                )
        upserted += _flush_batch(c, conn, catalog, batch, warned, log_prefix)

        if skipped:
            logger.warning(
                f"{log_prefix}: skipped {skipped} document(s) missing event_id or createdAt"
            )
        logger.info(
            f"{log_prefix}: finished - read {seen} document(s), upserted {upserted} row(s) "
            f"into {RAW_EVENTS_TABLE} in {time.monotonic() - started_at:.1f}s"
        )

    client = None
    try:
        client, collection = _resolve_mongo_sessions_collection()
        if cursor is not None:
            _do_sync(cursor, connection, collection)
        else:
            with get_db_cursor(brand_index) as (c, conn):
                _do_sync(c, conn, collection)
    except Exception as e:
        logger.error(f"Error ingesting raw Mongo events for {brand_name}: {e}")
    finally:
        if client is not None:
            client.close()
