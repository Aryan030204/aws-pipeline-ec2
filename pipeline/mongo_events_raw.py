"""
Raw alerts.sessions event ingestion: every document from the main cluster's
alerts.sessions collection is stored, one row per event, in a per-brand
raw_mongo_events table (in the brand's own database, not the master DB).

This replaces the older mongo_events_summary table, whose per-key typed columns
turned values into NULL (a value that did not fit the column type inferred from
the first value ever seen) and dropped keys once MySQL's row-size limit was
reached. The design here cannot do either:

- Nested objects are flattened into columns (data.custom_data.order_name ->
  data_custom_data_order_name). A key never seen before gets its column created
  automatically and the value is ingested in the same run.
- Every dynamic column is MEDIUMTEXT and every value is stored as text
  (numbers as their digits, booleans as true/false, lists/objects as JSON,
  empty strings as empty strings). There is no inferred type, so there is no
  type mismatch and no table rebuild (ADD COLUMN is cheap).
- A value that still cannot get its own column (column cap reached, MySQL
  row-size limit, two keys that collide on one case-insensitive column name,
  or a value longer than an indexed column allows) is kept in the same row's
  `extra_data` column as JSON {flattened_name: original_value}. A document's
  value is therefore always stored - either in its column or in extra_data -
  before the source document is deleted from Mongo.
- createdAt/updatedAt (UTC in Mongo) become four columns:
  created_at_utc, updated_at_utc, created_at_ist, updated_at_ist.
- _id and idempotency_key are not stored.

Rows are keyed by event_id and upserted, so re-running the same window is
idempotent (no duplicates), and late-arriving events are picked up by the
rolling window the worker/backfill pass in.

Migration is automatic, per brand, on the first run after deploy: the table is
created if missing, and the history in the legacy mongo_events_summary table
(the columns the rollups read: event_id/type, session, cart_token, variantId,
user_agent, url, timestamps) is copied into it newest-first, in small chunks,
a few days per run, resumable and idempotent (INSERT IGNORE). The legacy table
is left untouched.
"""

import hashlib
import json
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from pipeline.state import IST, logger
from pipeline.db import (
    get_db_cursor,
    executemany_chunked,
    get_pipeline_metadata_timestamp,
    update_pipeline_metadata_timestamp,
    EXECUTEMANY_CHUNK_SIZE,
)
from pipeline.mongo_events import (
    ROLLUP_EVENT_TYPES,
    _ist_date_range_to_utc_bounds,
    _resolve_mongo_sessions_collection,
    get_ci_events_mongo_shop_name,
    is_ci_events_brand,
)

# aws_background owns get_brand_timezone; attribute access only happens when
# the sync is actually called (same pattern as pipeline/mongo_events.py).
import aws_background

RAW_EVENTS_TABLE = "raw_mongo_events"
LEGACY_EVENTS_TABLE = "mongo_events_summary"

# Total-column guardrail (fixed + dynamic). Every dynamic column is a
# TEXT-family column that counts ~20 bytes toward InnoDB's 8,126-byte row
# limit, so ~380 columns is the hard ceiling; 300 leaves headroom. Past it (or
# once MySQL itself refuses a column) new keys are stored in extra_data.
MAX_COLUMNS = int(os.environ.get("MONGO_EVENTS_MAX_COLUMNS", "300"))
BATCH_SIZE = int(os.environ.get("MONGO_EVENTS_RAW_BATCH_SIZE", "2000"))

# Incremental ingest (live worker path, no explicit start/end given): watermark
# on createdAt (server-set, not user/tracker-written). The metadata key is the
# same one the legacy table used, so the cutover to raw_mongo_events continues
# exactly where the old ingest stopped. 5-minute overlap re-checks the boundary
# in case of any in-flight writes at query time; the upsert is idempotent so
# re-matching a handful of already-ingested docs is harmless.
RAW_EVENTS_METADATA_KEY = "mongo_events_raw_last_created_at"
RAW_EVENTS_OVERLAP = timedelta(minutes=5)
# First run under this watermark key: look back further than one 15-minute
# cycle. Safe - idempotent upsert, and ingested docs get deleted.
RAW_EVENTS_DEFAULT_LOOKBACK = timedelta(hours=24)

# Deletes each batch's documents from Mongo once it's durably upserted into
# raw_mongo_events, so the alerts.sessions collection doesn't grow unbounded.
MONGO_EVENTS_DELETE_INGESTED_DOCS = (
    os.environ.get("MONGO_EVENTS_DELETE_INGESTED_DOCS", "true").strip().lower() == "true"
)

# Legacy history copy (mongo_events_summary -> raw_mongo_events). OFF by default: the rollup tables
# already hold every past day, the copy is a heavy sustained write load (millions of wide rows with
# several indexes each), and old dates can still be recomputed from the untouched legacy table if ever
# needed. Set MONGO_EVENTS_LEGACY_COPY=true to (re)start it; it resumes where it stopped.
LEGACY_COPY_ENABLED = (
    os.environ.get("MONGO_EVENTS_LEGACY_COPY", "false").strip().lower() == "true"
)
LEGACY_COPY_CHUNK_HOURS = int(os.environ.get("MONGO_EVENTS_LEGACY_COPY_CHUNK_HOURS", "2"))
LEGACY_COPY_CHUNKS_PER_RUN = int(os.environ.get("MONGO_EVENTS_LEGACY_COPY_CHUNKS_PER_RUN", "12"))
# Wall-clock budget per brand per run. The copy runs AFTER live ingestion and
# stops starting new chunks once this is spent, so it can never push a
# 15-minute cycle past its interval (history just takes more cycles).
LEGACY_COPY_MAX_SECONDS = float(os.environ.get("MONGO_EVENTS_LEGACY_COPY_MAX_SECONDS", "45"))
LEGACY_COPY_PROGRESS_KEY = "raw_mongo_events_legacy_copy_oldest_utc"
LEGACY_COPY_DONE_KEY = "raw_mongo_events_legacy_copy_done_at"
_LEGACY_COPY_COLUMNS = [
    "event_id", "event_type", "session_id", "shop_name", "cart_token",
    "variantId", "user_agent", "url",
    "created_at_utc", "updated_at_utc", "created_at_ist", "updated_at_ist",
]

MAX_FLATTEN_DEPTH = 5
MYSQL_MAX_IDENTIFIER_LENGTH = 64

# Top-level keys never stored as-is: _id / idempotency_key are deliberately
# dropped; createdAt/updatedAt are replaced by the four *_utc/*_ist columns;
# event_id is the primary key column.
_EXCLUDED_TOP_LEVEL_KEYS = {"_id", "idempotency_key", "createdAt", "updatedAt", "event_id"}

# Columns created up front. Anything else is discovered dynamically (MEDIUMTEXT).
_FIXED_COLUMNS: Dict[str, str] = {
    "event_id": "VARCHAR(255) NOT NULL",
    "event_type": "VARCHAR(255) NULL",
    "session_id": "VARCHAR(255) NULL",
    "shop_name": "VARCHAR(255) NULL",
    "cart_token": "VARCHAR(255) NULL",
    "variantId": "MEDIUMTEXT NULL",
    "user_agent": "MEDIUMTEXT NULL",
    "url": "MEDIUMTEXT NULL",
    "created_at_utc": "DATETIME(3) NULL",
    "updated_at_utc": "DATETIME(3) NULL",
    "created_at_ist": "DATETIME(3) NULL",
    "updated_at_ist": "DATETIME(3) NULL",
    "extra_data": "LONGTEXT NULL",
}
_TIMESTAMP_COLUMNS = {"created_at_utc", "updated_at_utc", "created_at_ist", "updated_at_ist"}
# Indexed VARCHAR columns: a longer value keeps its full text in extra_data and
# a prefix here, instead of failing the whole batch with "Data too long".
_VARCHAR_LIMITS = {
    "event_id": 255,
    "event_type": 255,
    "session_id": 255,
    "shop_name": 255,
    "cart_token": 255,
}
# Names a Mongo key can never claim (they would collide with our own columns);
# such a key's value goes to extra_data under its original name.
_RESERVED_NAMES = {"extra_data", "ingested_at"}


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


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _to_text(value: Any) -> Optional[str]:
    """Every stored value is text: nothing is ever rejected for its type.
    Empty strings stay empty strings (not NULL)."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool):  # before int: bool is an int subclass
        return "true" if value else "false"
    if isinstance(value, (list, tuple, dict)):
        return json.dumps(value, default=_json_default)
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _is_row_size_error(error: Exception) -> bool:
    text = str(error)
    return "1118" in text or "row size too large" in text.lower()


class _ColumnCatalog:
    """The table's columns, cached for one brand run: case-insensitive name
    lookup (MySQL column names are case-insensitive, so `variantid` must
    reuse `variantId` rather than collide with it). Refreshed only by our own
    ALTERs."""

    def __init__(self, cursor, connection):
        self._cursor = cursor
        self._connection = connection
        self._columns: Dict[str, str] = {}
        # Lower-cased names whose ADD failed this run: not retried on every
        # batch (their values go to extra_data instead).
        self.failed: set = set()
        # True once MySQL refused a column for row size: no further ADDs this run.
        self.full = False
        self._load()

    def _load(self) -> None:
        self._cursor.execute(f"SHOW COLUMNS FROM {_quote(RAW_EVENTS_TABLE)}")
        for row in self._cursor.fetchall() or []:
            name = row["Field"] if isinstance(row, dict) else row[0]
            self._columns[str(name).lower()] = str(name)

    def get(self, name: str) -> Optional[str]:
        return self._columns.get(name.lower())

    def __len__(self) -> int:
        return len(self._columns)

    def add(self, names: List[str]) -> List[str]:
        """ALTER TABLE ... ADD COLUMN <name> MEDIUMTEXT NULL for each name.
        Tries one statement, falls back to one column at a time so a single
        bad column doesn't block the rest. Stops adding as soon as MySQL
        reports the row-size limit (1118) - everything after goes to
        extra_data. Returns the names added."""
        added: List[str] = []
        if not names:
            return added

        def _alter(batch: List[str]) -> None:
            clauses = ", ".join(f"ADD COLUMN {_quote(n)} MEDIUMTEXT NULL" for n in batch)
            self._cursor.execute(f"ALTER TABLE {_quote(RAW_EVENTS_TABLE)} {clauses}")
            self._connection.commit()

        try:
            _alter(names)
            added = list(names)
        except Exception as e:
            logger.warning(
                f"[raw events] combined ALTER for {len(names)} new column(s) failed "
                f"({e}); retrying one at a time"
            )
            for index, name in enumerate(names):
                try:
                    _alter([name])
                    added.append(name)
                except Exception as single_error:
                    self.failed.add(name.lower())
                    logger.error(
                        f"[raw events] could not add column {name!r}: {single_error}; "
                        "its values go to extra_data"
                    )
                    if _is_row_size_error(single_error):
                        self.full = True
                        self.failed.update(n.lower() for n in names[index + 1:])
                        break
        for name in added:
            self._columns[name.lower()] = name
        return added


def _ensure_raw_events_table(cursor, connection) -> None:
    # Indexes are deliberately minimal - every secondary index is another write per ingested event:
    # (event_type, created_at_utc) serves the rollups' "this type in this window" reads and covers the
    # event_type-only lookups; created_at_utc serves the all-types window scan. Nothing in the pipeline
    # queries by session_id / cart_token alone. Tables created earlier keep their old indexes until
    # migrations/008_raw_mongo_events_indexes.sql is applied.
    fixed = ",\n            ".join(f"{_quote(n)} {t}" for n, t in _FIXED_COLUMNS.items())
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_quote(RAW_EVENTS_TABLE)} (
            {fixed},
            `ingested_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            PRIMARY KEY (`event_id`),
            KEY `idx_type_created` (`event_type`, `created_at_utc`),
            KEY `idx_created_at_utc` (`created_at_utc`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC
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
    """Discover+create any new columns, convert values to text, and upsert one
    batch. Whatever cannot be placed in a column goes to the row's extra_data."""
    if not records:
        return 0

    # 1. Discover new keys. A key that is null in every record so far creates
    #    no column yet (there is nothing to store); it will once a value appears.
    if not catalog.full:
        new_names: Dict[str, str] = {}
        for record in records:
            for name, value in record.items():
                if value is None:
                    continue
                lower = name.lower()
                if (
                    lower in _RESERVED_NAMES
                    or catalog.get(name) is not None
                    or lower in new_names
                    or lower in catalog.failed
                ):
                    continue
                if len(catalog) + len(new_names) >= MAX_COLUMNS:
                    if "__cap__" not in warned:
                        warned.add("__cap__")
                        logger.error(
                            f"{log_prefix}: column cap of {MAX_COLUMNS} reached; new keys "
                            f"(e.g. {name!r}) are stored in extra_data. Raise "
                            "MONGO_EVENTS_MAX_COLUMNS only if the table has row-size room."
                        )
                    continue
                new_names[lower] = name
        if new_names:
            added = catalog.add(list(new_names.values()))
            if added:
                logger.info(f"{log_prefix}: discovered and added {len(added)} new column(s): {added}")

    # 2. Map each record onto real column names, converting values to text.
    mapped_records: List[Dict[str, Any]] = []
    column_order: Dict[str, None] = {name: None for name in _FIXED_COLUMNS}
    for record in records:
        mapped: Dict[str, Any] = {}
        extra: Dict[str, Any] = {}
        used: set = set()
        for name, value in record.items():
            if value is None:
                continue
            lower = name.lower()
            actual = None if lower in _RESERVED_NAMES else catalog.get(name)
            if actual is None or lower in used:
                # no column (cap / failed ALTER / reserved name) or a second key
                # that collides, case-insensitively, with one already mapped
                extra[name] = value
                if ("no-col", lower) not in warned:
                    warned.add(("no-col", lower))
                    logger.warning(
                        f"{log_prefix}: no own column for key {name!r}; its values are "
                        "kept in extra_data (logged once per key per run)"
                    )
                continue
            used.add(lower)
            if actual in _TIMESTAMP_COLUMNS:
                mapped[actual] = value
            else:
                text = _to_text(value)
                limit = _VARCHAR_LIMITS.get(actual)
                if limit is not None and text is not None and len(text) > limit:
                    extra[name] = text  # full value preserved
                    text = text[:limit]
                mapped[actual] = text
            column_order.setdefault(actual, None)
        if extra:
            mapped["extra_data"] = json.dumps(extra, default=_json_default)
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


def _table_columns(cursor, table: str) -> set:
    cursor.execute(
        "SELECT column_name AS col FROM information_schema.columns "
        "WHERE table_schema = DATABASE() AND table_name = %s",
        (table,),
    )
    names = set()
    for row in cursor.fetchall() or []:
        names.add(str(row["col"] if isinstance(row, dict) else row[0]))
    return names


def _floor_hour(value: datetime) -> datetime:
    return value.replace(minute=0, second=0, microsecond=0)


def _copy_legacy_history(cursor, connection, log_prefix: str) -> int:
    """Copy the legacy mongo_events_summary rows into raw_mongo_events, newest
    first, LEGACY_COPY_CHUNK_HOURS at a time, at most LEGACY_COPY_CHUNKS_PER_RUN
    chunks and LEGACY_COPY_MAX_SECONDS per call (so one 15-minute cycle never
    gets long). Newest-first means the days the rollups recompute are complete
    first. Progress is stored in
    pipeline_metadata, so it resumes across runs; INSERT IGNORE means rows
    already ingested live (which are authoritative) are never overwritten.
    Returns the number of rows copied this call."""
    if not LEGACY_COPY_ENABLED:
        return 0
    if get_pipeline_metadata_timestamp(cursor, LEGACY_COPY_DONE_KEY) is not None:
        return 0

    legacy_columns = _table_columns(cursor, LEGACY_EVENTS_TABLE)
    if not legacy_columns:
        update_pipeline_metadata_timestamp(cursor, connection, LEGACY_COPY_DONE_KEY, datetime.now(IST))
        return 0
    columns = [c for c in _LEGACY_COPY_COLUMNS if c in legacy_columns]
    if "event_id" not in columns or "created_at_utc" not in columns:
        logger.warning(f"{log_prefix}: legacy table lacks event_id/created_at_utc; history copy skipped")
        return 0

    cursor.execute(
        f"SELECT MIN(created_at_utc) AS lo, MAX(created_at_utc) AS hi FROM {_quote(LEGACY_EVENTS_TABLE)}"
    )
    row = cursor.fetchone()
    lo = row["lo"] if isinstance(row, dict) else (row[0] if row else None)
    hi = row["hi"] if isinstance(row, dict) else (row[1] if row else None)
    if lo is None or hi is None:
        update_pipeline_metadata_timestamp(cursor, connection, LEGACY_COPY_DONE_KEY, datetime.now(IST))
        return 0

    floor_lo = _floor_hour(lo)
    stored = get_pipeline_metadata_timestamp(cursor, LEGACY_COPY_PROGRESS_KEY)
    if stored is None:
        boundary = _floor_hour(hi) + timedelta(hours=1)  # exclusive upper bound, covers hi
    else:
        boundary = stored.astimezone(timezone.utc).replace(tzinfo=None)

    chunk = timedelta(hours=max(1, LEGACY_COPY_CHUNK_HOURS))
    quoted = ", ".join(_quote(c) for c in columns)
    sql = (
        f"INSERT IGNORE INTO {_quote(RAW_EVENTS_TABLE)} ({quoted}) "
        f"SELECT {quoted} FROM {_quote(LEGACY_EVENTS_TABLE)} "
        "WHERE created_at_utc >= %s AND created_at_utc < %s"
    )

    copied = 0
    chunks = 0
    copy_started = time.monotonic()
    while boundary > floor_lo and chunks < LEGACY_COPY_CHUNKS_PER_RUN:
        # always do at least one chunk (so progress is guaranteed), then stop
        # once the time budget is spent
        if chunks > 0 and time.monotonic() - copy_started >= LEGACY_COPY_MAX_SECONDS:
            break
        start = max(boundary - chunk, floor_lo)
        cursor.execute(sql, (start, boundary))
        copied += max(getattr(cursor, "rowcount", 0) or 0, 0)
        connection.commit()
        boundary = start
        update_pipeline_metadata_timestamp(
            cursor, connection, LEGACY_COPY_PROGRESS_KEY,
            boundary.replace(tzinfo=timezone.utc).astimezone(IST),
        )
        chunks += 1

    finished = boundary <= floor_lo
    if finished:
        update_pipeline_metadata_timestamp(cursor, connection, LEGACY_COPY_DONE_KEY, datetime.now(IST))
    logger.info(
        f"{log_prefix}: legacy history copy {'finished' if finished else 'in progress'} - "
        f"{copied} row(s) copied this run, copied back to {boundary:%Y-%m-%d %H:%M} UTC "
        f"(oldest legacy row {lo:%Y-%m-%d %H:%M} UTC)"
    )
    return copied


def _delete_ingested_documents(collection, event_ids: List[str]) -> int:
    """
    Deletes documents by event_id, chunked at EXECUTEMANY_CHUNK_SIZE (same
    constant the MySQL side chunks on). Only ever called after a batch's
    upsert has already returned successfully - see the per-batch
    delete-after-ingest call in sync_mongo_events_raw_for_brand below.
    """
    deleted = 0
    for i in range(0, len(event_ids), EXECUTEMANY_CHUNK_SIZE):
        chunk = event_ids[i : i + EXECUTEMANY_CHUNK_SIZE]
        result = collection.delete_many({"event_id": {"$in": chunk}})
        deleted += result.deleted_count
    return deleted


def sync_mongo_events_raw_for_brand(
    brand_index: int,
    brand_name: str,
    brand_key: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    cursor=None,
    connection=None,
) -> "set":
    """
    Returns the set of IST date strings (YYYY-MM-DD) touched by whatever was
    upserted this call - callers (events-summary-worker) use this to know
    which dates' CI/buy-now rollups need recomputing from raw_mongo_events,
    without rescanning dates nothing changed for. Empty set on a brand-skip
    or an error (logged, not raised - matches every other per-brand sync in
    this pipeline).

    Default path (start_date/end_date both omitted - the live worker, every
    15 minutes): incremental, watermarked on createdAt, stored per brand in
    pipeline_metadata. Explicit start_date/end_date (the backfill script):
    a fixed date window instead, same as before - the watermark is left
    untouched in that path, so a backfill never rewinds or fast-forwards the
    live worker's incremental position.

    Either way, once a batch is durably upserted, its source documents are
    deleted from Mongo (MONGO_EVENTS_DELETE_INGESTED_DOCS) so alerts.sessions
    doesn't grow unbounded.
    """
    if not is_ci_events_brand(brand_name, brand_key):
        logger.info(
            f"Skipping raw Mongo events sync for {brand_name}: brand not enabled for ci_events"
        )
        return set()

    mongo_shop_name = get_ci_events_mongo_shop_name(brand_name, brand_key)
    if not mongo_shop_name:
        logger.info(
            f"Skipping raw Mongo events sync for {brand_name}: no Mongo shop_name mapping"
        )
        return set()

    tz = aws_background.get_brand_timezone(brand_index)
    explicit_window = start_date is not None and end_date is not None
    if explicit_window:
        query_lower_bound, query_upper_bound = _ist_date_range_to_utc_bounds(start_date, end_date, tz)
    else:
        query_upper_bound = None  # no upper bound - incremental, open-ended
    log_prefix = f"[raw events] brand={brand_name} (shop_name={mongo_shop_name})"

    affected_dates: set = set()

    def _do_sync(c, conn, collection) -> None:
        started_at = time.monotonic()
        _ensure_raw_events_table(c, conn)
        catalog = _ColumnCatalog(c, conn)

        if explicit_window:
            lower_bound = query_lower_bound
            logger.info(
                f"{log_prefix}: starting ingestion for {start_date}..{end_date} ({tz}); "
                f"createdAt in [{lower_bound:%Y-%m-%d %H:%M:%S}, {query_upper_bound:%Y-%m-%d %H:%M:%S}) UTC"
            )
        else:
            stored_watermark = get_pipeline_metadata_timestamp(c, RAW_EVENTS_METADATA_KEY)
            watermark = stored_watermark or (datetime.now(IST) - RAW_EVENTS_DEFAULT_LOOKBACK)
            lower_bound = (watermark - RAW_EVENTS_OVERLAP).astimezone(timezone.utc)
            if stored_watermark is None:
                logger.info(
                    f"{log_prefix}: no stored watermark (first run) - looking back "
                    f"{RAW_EVENTS_DEFAULT_LOOKBACK}"
                )
            logger.info(
                f"{log_prefix}: starting ingestion; createdAt > "
                f"{lower_bound:%Y-%m-%d %H:%M:%S} UTC (stored watermark "
                f"{watermark.strftime('%Y-%m-%d %H:%M:%S')} IST minus "
                f"{int(RAW_EVENTS_OVERLAP.total_seconds() // 60)}m overlap)"
            )

        created_at_filter: Dict[str, Any] = {"$gt": lower_bound} if not explicit_window else {
            "$gte": lower_bound,
            "$lt": query_upper_bound,
        }
        query = {"shop_name": mongo_shop_name, "createdAt": created_at_filter}

        # No .sort(): order is irrelevant for an idempotent upsert, and an
        # unindexed sort fails past Mongo's 32MB in-memory limit. Streamed in
        # batches (not list(cursor)) - raw payloads are large.
        docs = collection.find(query, {"_id": 0}, batch_size=BATCH_SIZE)

        warned: set = set()
        batch: List[Dict[str, Any]] = []
        batch_created_ats: List[datetime] = []
        seen = skipped = upserted = deleted = 0
        max_created_at: Optional[datetime] = None

        def _flush(current_batch: List[Dict[str, Any]], current_created_ats: List[datetime]) -> None:
            nonlocal upserted, deleted
            if not current_batch:
                return
            flushed = _flush_batch(c, conn, catalog, current_batch, warned, log_prefix)
            upserted += flushed
            for r in current_batch:
                # Only checkout_initiated / buy_now events change what the rollups compute, so only
                # their dates are reported back (every event type is still ingested above).
                ist_dt = r.get("created_at_ist")
                if ist_dt is not None and r.get("event_type") in ROLLUP_EVENT_TYPES:
                    affected_dates.add(ist_dt.date().isoformat())
            if MONGO_EVENTS_DELETE_INGESTED_DOCS:
                try:
                    event_ids = [r["event_id"] for r in current_batch]
                    deleted += _delete_ingested_documents(collection, event_ids)
                except Exception as e:
                    logger.error(
                        f"{log_prefix}: error deleting {len(current_batch)} ingested "
                        f"document(s) from Mongo: {e}"
                    )
            if not explicit_window and current_created_ats:
                # Watermark advance happens here, per batch, right after that
                # batch's upsert (and delete attempt) - so a mid-run failure
                # on a later batch still leaves the watermark correctly past
                # every batch that *did* complete, instead of all-or-nothing.
                batch_max = max(current_created_ats)
                nonlocal max_created_at
                if max_created_at is None or batch_max > max_created_at:
                    max_created_at = batch_max
                update_pipeline_metadata_timestamp(
                    c, conn, RAW_EVENTS_METADATA_KEY, max_created_at.astimezone(IST)
                )
            logger.info(
                f"{log_prefix}: read {seen} document(s), upserted {upserted} so far, "
                f"deleted {deleted} ingested doc(s) ({time.monotonic() - started_at:.1f}s elapsed)"
            )

        for doc in docs:
            seen += 1
            created_at = doc.get("createdAt")
            record = _build_record(doc)
            if record is None:
                skipped += 1
                continue
            batch.append(record)
            if isinstance(created_at, datetime):
                batch_created_ats.append(
                    created_at if created_at.tzinfo else created_at.replace(tzinfo=timezone.utc)
                )
            if len(batch) >= BATCH_SIZE:
                _flush(batch, batch_created_ats)
                batch = []
                batch_created_ats = []
        _flush(batch, batch_created_ats)

        if skipped:
            logger.warning(
                f"{log_prefix}: skipped {skipped} document(s) missing event_id or createdAt"
            )
        logger.info(
            f"{log_prefix}: finished - read {seen} document(s), upserted {upserted} row(s) "
            f"into {RAW_EVENTS_TABLE}, deleted {deleted} ingested document(s) from Mongo, "
            f"in {time.monotonic() - started_at:.1f}s"
        )

        # Automatic migration of the legacy table's history - AFTER live
        # ingestion, within a time budget, so it never delays fresh events or
        # their rollups. A failure here just retries next run.
        try:
            _copy_legacy_history(c, conn, log_prefix)
        except Exception as e:
            logger.error(f"{log_prefix}: legacy history copy failed (will retry next run): {e}")

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

    return affected_dates
