"""
Monthly data purge: archive old rows to S3, verify the archive, and only then delete them.

Retention rule: keep the most recent 6 calendar months. The cutoff is
    first day of the run's month (IST)  -  6 calendar months
(2026-10-01 -> 2026-04-01) and a row is eligible when `date_column < cutoff`, strictly.

SAFETY MODEL (the point of this module):
  * Nothing is deleted unless the mode allows it AND the table is in DATA_PURGE_DELETE_TABLES AND a
    complete, verified archive (CSV + manifest) exists in S3. Defaults: disabled, archive-only,
    empty delete allowlist. Anything unparseable in the config refuses to run (fail closed).
  * The manifest (`<table>.manifest.json`) is the only proof of a completed archive and is never
    overwritten. A CSV without a manifest is either positively confirmed (completion tags + size +
    row count + header) or deterministically replaced; it is never trusted for deletion.
  * Deletes are batched (`DELETE ... ORDER BY <date> LIMIT n`, commit per batch, optional sleep) and
    capped at the number of rows that were archived.
  * Every table is validated live (information_schema + EXPLAIN) before anything happens: it must be a
    base table, the date column must exist with an accepted type, and the date column must lead an
    index that the export and delete plans actually use. Otherwise the table is skipped with a reason.
  * OPERATIONAL INVARIANT: rows with `date < cutoff` are treated as immutable while a purge runs (no
    concurrent mutation, re-insertion or backfill). The recount check is only a sanity check, not a
    proof that the exact same rows were exported.

This module deliberately does not import aws_background (the worker passes brand contexts in) and loads
boto3 lazily, so it is cheap to import and fully testable with fakes.
"""

import csv
import hashlib
import io
import json
import logging
import os
import re
import time
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import date, datetime, time as dt_time, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

logger = logging.getLogger("data_purge")

RETENTION_MONTHS = 6
SCHEDULER_TZ = ZoneInfo("Asia/Kolkata")
WORKER_VERSION = "1"
MANIFEST_SCHEMA_VERSION = 1
NULL_TOKEN = "\\N"  # a CSV field that is exactly \N means SQL NULL
MIN_PLAUSIBLE_DATE = date(2000, 1, 1)
CSV_CHUNK_BYTES = 256 * 1024
HEADER_PROBE_BYTES = 1 << 20


# --------------------------------------------------------------------------------------
# Configuration (strict, fail closed)
# --------------------------------------------------------------------------------------
class ConfigError(ValueError):
    """Unsafe or unparseable configuration. The worker refuses to run."""


class Mode(Enum):
    DISABLED = "DISABLED"
    DRY_RUN = "DRY_RUN"
    ARCHIVE_ONLY = "ARCHIVE_ONLY"
    FULL = "FULL"


class Status:
    SUCCESS = "SUCCESS"
    ARCHIVED_ONLY = "ARCHIVED_ONLY"
    FAILED = "FAILED"
    NO_DATA = "NO_DATA"
    DRY_RUN = "DRY_RUN"
    SKIPPED = "SKIPPED"
    SKIPPED_AMBIGUOUS = "SKIPPED_AMBIGUOUS"
    SKIPPED_MISSING = "SKIPPED_MISSING"
    SKIPPED_VIEW = "SKIPPED_VIEW"
    SKIPPED_TYPE_MISMATCH = "SKIPPED_TYPE_MISMATCH"
    SKIPPED_NO_INDEX = "SKIPPED_NO_INDEX"
    SKIPPED_BAD_PLAN = "SKIPPED_BAD_PLAN"
    SKIPPED_UNCONFIRMED = "SKIPPED_UNCONFIRMED"
    SKIPPED_CONFLICT = "SKIPPED_CONFLICT"
    SKIPPED_LOCKED = "SKIPPED_LOCKED"


@dataclass(frozen=True)
class PurgeConfig:
    mode: Mode
    # What the mode would be WITHOUT dry-run (DISABLED / ARCHIVE_ONLY / FULL); lets a dry run report
    # honestly whether deletion would be supported.
    intended_mode: Mode
    # PURGE_TEST=true: export to S3 but NEVER delete, whatever else is configured (a hard override).
    purge_test: bool
    tables: Tuple[str, ...]
    delete_tables: Tuple[str, ...]
    bucket: Optional[str]
    prefix: str
    delete_batch_size: int
    export_batch_size: int
    sleep_between_batches_s: float
    max_window_rows: int
    sse: Optional[str]
    kms_key_id: Optional[str]


def _parse_bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    value = raw.strip().lower()
    if value == "true":
        return True
    if value == "false":
        return False
    raise ConfigError(f"{name} must be exactly 'true' or 'false', got {raw!r}")


def _parse_int(env: Mapping[str, str], name: str, default: int, low: int, high: int) -> int:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw.strip())
    except ValueError:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from None
    if not low <= value <= high:
        raise ConfigError(f"{name} must be between {low} and {high}, got {value}")
    return value


def _parse_float(env: Mapping[str, str], name: str, default: float, low: float, high: float) -> float:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = float(raw.strip())
    except ValueError:
        raise ConfigError(f"{name} must be a number, got {raw!r}") from None
    if not low <= value <= high:
        raise ConfigError(f"{name} must be between {low} and {high}, got {value}")
    return value


def _parse_tables(
    env: Mapping[str, str], name: str, default: Tuple[str, ...], known: Sequence[str]
) -> Tuple[str, ...]:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    names: List[str] = []
    for part in raw.split(","):
        table = part.strip().lower()
        if not table:
            continue
        if table not in known:
            raise ConfigError(f"{name} lists unknown table {table!r}; known: {', '.join(known)}")
        if table not in names:
            names.append(table)
    return tuple(names)


def load_config(env: Optional[Mapping[str, str]] = None) -> PurgeConfig:
    """Resolve the mode and every setting. Raises ConfigError on anything unsafe or unparseable.

    Mode (DRY_RUN always wins; destructive only when ARCHIVE_ONLY is explicitly 'false'):
      DRY_RUN=true                                   -> DRY_RUN   (read-only; runs even if disabled)
      ENABLED=false (default)                        -> DISABLED  (does nothing destructive)
      ENABLED=true, ARCHIVE_ONLY=true (default)      -> ARCHIVE_ONLY
      ENABLED=true, ARCHIVE_ONLY=false               -> FULL (deletes only tables in DATA_PURGE_DELETE_TABLES)
    PURGE_TEST=true is a hard override on top of that: it caps any enabled run at ARCHIVE_ONLY (export + verify +
    manifest, never a DELETE), ignoring ARCHIVE_ONLY / DATA_PURGE_DELETE_TABLES. It does not enable a disabled
    worker and does not change DRY_RUN.
    """
    env = os.environ if env is None else env
    known = [spec.table for spec in PURGE_TABLES]

    dry_run = _parse_bool(env, "DATA_PURGE_DRY_RUN", False)
    enabled = _parse_bool(env, "DATA_PURGE_ENABLED", False)
    archive_only = _parse_bool(env, "DATA_PURGE_ARCHIVE_ONLY", True)
    purge_test = _parse_bool(env, "PURGE_TEST", False)

    tables = _parse_tables(env, "DATA_PURGE_TABLES", tuple(known), known)
    delete_tables = _parse_tables(env, "DATA_PURGE_DELETE_TABLES", (), known)
    if not set(delete_tables) <= set(tables):
        raise ConfigError("DATA_PURGE_DELETE_TABLES must be a subset of DATA_PURGE_TABLES")

    if not enabled:
        intended = Mode.DISABLED
    elif archive_only or purge_test:
        intended = Mode.ARCHIVE_ONLY
    else:
        intended = Mode.FULL
    mode = Mode.DRY_RUN if dry_run else intended

    if mode is Mode.FULL and not delete_tables:  # unreachable under PURGE_TEST (it never yields FULL)
        raise ConfigError(
            "DATA_PURGE_ARCHIVE_ONLY=false requires an explicit DATA_PURGE_DELETE_TABLES allowlist"
        )

    bucket = (env.get("DATA_PURGE_S3_BUCKET") or "").strip() or None
    if mode in (Mode.ARCHIVE_ONLY, Mode.FULL):
        if not bucket:
            raise ConfigError("DATA_PURGE_S3_BUCKET is required for archive/delete runs")
        if re.search(r"\s", bucket):
            raise ConfigError("DATA_PURGE_S3_BUCKET must not contain whitespace")

    sse = (env.get("DATA_PURGE_S3_SSE") or "").strip() or None
    if sse is not None and sse not in ("AES256", "aws:kms"):
        raise ConfigError("DATA_PURGE_S3_SSE must be 'AES256' or 'aws:kms'")
    kms_key_id = (env.get("DATA_PURGE_S3_KMS_KEY_ID") or "").strip() or None
    if kms_key_id and sse != "aws:kms":
        raise ConfigError("DATA_PURGE_S3_KMS_KEY_ID requires DATA_PURGE_S3_SSE=aws:kms")

    return PurgeConfig(
        mode=mode,
        intended_mode=intended,
        purge_test=purge_test,
        tables=tables,
        delete_tables=delete_tables,
        bucket=bucket,
        prefix=((env.get("DATA_PURGE_S3_PREFIX") or "data-purge").strip().strip("/")) or "data-purge",
        # Conservative, explicitly untuned starting values: tune from the per-batch logs.
        delete_batch_size=_parse_int(env, "DATA_PURGE_BATCH_SIZE", 1000, 1, 100_000),
        export_batch_size=_parse_int(env, "DATA_PURGE_EXPORT_BATCH_SIZE", 2000, 1, 100_000),
        sleep_between_batches_s=_parse_float(env, "DATA_PURGE_SLEEP_BETWEEN_BATCHES_S", 1.0, 0.0, 60.0),
        max_window_rows=_parse_int(env, "DATA_PURGE_MAX_WINDOW_ROWS", 200_000, 1000, 5_000_000),
        sse=sse,
        kms_key_id=kms_key_id,
    )


# --------------------------------------------------------------------------------------
# Cutoff (calendar-month arithmetic)
# --------------------------------------------------------------------------------------
def _as_ist(run_dt: datetime) -> datetime:
    if run_dt.tzinfo is None:
        return run_dt.replace(tzinfo=SCHEDULER_TZ)
    return run_dt.astimezone(SCHEDULER_TZ)


def compute_cutoff(run_dt: datetime, months: int = RETENTION_MONTHS) -> date:
    """First day of the run's (IST) month minus `months` calendar months. Never '- 180 days'."""
    local = _as_ist(run_dt)
    month_index = local.year * 12 + (local.month - 1) - months
    return date(month_index // 12, month_index % 12 + 1, 1)


def archive_month_label(run_dt: datetime) -> str:
    local = _as_ist(run_dt)
    return f"{local.year:04d}-{local.month:02d}"


class DateKind(Enum):
    LOCAL_DATE = "local_date"  # brand-local 'YYYY-MM-DD' string / DATE
    LOCAL_DATETIME = "local_datetime"  # naive DATETIME already stored as local (IST) wall-clock time
    UTC_DATETIME = "utc_datetime"  # naive UTC DATETIME


def start_param(kind: DateKind, day: date, tz: ZoneInfo) -> Any:
    """The SQL parameter that means 'the start of local day `day`' for a column of this kind."""
    if kind is DateKind.LOCAL_DATE:
        return day.isoformat()
    if kind is DateKind.LOCAL_DATETIME:
        return datetime.combine(day, dt_time.min)  # the column already holds local wall-clock time
    local_midnight = datetime.combine(day, dt_time.min, tzinfo=tz)
    return local_midnight.astimezone(timezone.utc).replace(tzinfo=None)


def cutoff_param(kind: DateKind, cutoff: date, tz: ZoneInfo) -> Any:
    """`date_col < cutoff_param(...)` is the whole purge condition."""
    return start_param(kind, cutoff, tz)


def _to_local_day(value: Any, kind: DateKind, tz: ZoneInfo) -> date:
    if isinstance(value, datetime):
        if kind is DateKind.UTC_DATETIME:
            return value.replace(tzinfo=timezone.utc).astimezone(tz).date()
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return datetime.strptime(value.strip()[:10], "%Y-%m-%d").date()
    raise ValueError(f"unsupported date value type {type(value).__name__}")


# --------------------------------------------------------------------------------------
# Per-table configuration (explicit; never guessed)
# --------------------------------------------------------------------------------------
@dataclass(frozen=True)
class TableSpec:
    table: str
    date_column: Optional[str]
    date_kind: Optional[DateKind]
    allowed_types: Tuple[str, ...]
    # False = the schema is only inferred from application reads; validated and reported in dry-run,
    # but archive/delete are skipped until the live schema is confirmed and this is flipped.
    schema_confirmed: bool
    note: str = ""


_LOCAL_TYPES = ("varchar", "char", "date")
_UTC_TYPES = ("datetime", "timestamp")

PURGE_TABLES: Tuple[TableSpec, ...] = (
    TableSpec("shopify_orders_update", "updated_date", DateKind.LOCAL_DATE, _LOCAL_TYPES, True,
              "DDL in repo (no PK in code); append-only; live pipeline only reads max(updated_at)"),
    TableSpec("mongo_events_summary", "created_at_utc", DateKind.UTC_DATETIME, _UTC_TYPES, False,
              "legacy table, no DDL in repo; column inferred from the legacy copy reads"),
    TableSpec("shopify_orders", "created_date", DateKind.LOCAL_DATE, _LOCAL_TYPES, True,
              "DDL in repo (no PK in code); old dates can still be re-targeted by cancel/RTO/refund rebuilds"),
    TableSpec("raw_mongo_events", "created_at_ist", DateKind.LOCAL_DATETIME, _UTC_TYPES, True,
              "created_at_ist (IST wall-clock DATETIME(3)); PK event_id; REQUIRES an index leading with "
              "created_at_ist (migrations/009) - the table is skipped until it exists; the archive is the "
              "only backup of these events"),
    TableSpec("product_sessions_snapshot", "date", DateKind.LOCAL_DATE, _LOCAL_TYPES + ("datetime",), False,
              "no DDL/writer in repo; column inferred from reads"),
    TableSpec("mv_product_sessions_by_path_daily", None, None, (), False,
              "not referenced anywhere in the repo; may be a view - skipped until its schema is verified"),
    TableSpec("mv_product_sessions_by_type_daily", None, None, (), False,
              "not referenced anywhere in the repo; may be a view - skipped until its schema is verified"),
)
SPECS_BY_NAME: Dict[str, TableSpec] = {spec.table: spec for spec in PURGE_TABLES}

# Env flags (read from THIS container's environment only) that mean something may re-write old rows.
CONFLICT_ENV_FLAGS: Dict[str, Tuple[str, ...]] = {
    "BACKFILL_MODE": ("shopify_orders", "shopify_orders_update"),
    "DISCOUNT_DASHBOARD_BACKFILL": ("shopify_orders",),
    "MONGO_EVENTS_LEGACY_COPY": ("raw_mongo_events", "mongo_events_summary"),
}


# --------------------------------------------------------------------------------------
# Small SQL helpers
# --------------------------------------------------------------------------------------
def q(identifier: str) -> str:
    return "`" + str(identifier).replace("`", "``") + "`"


def _fetch_dicts(cursor) -> List[Dict[str, Any]]:
    """All rows as dicts with lower-cased keys, for dict and tuple cursors alike."""
    rows = cursor.fetchall() or []
    names: Optional[List[str]] = None
    out: List[Dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            out.append({str(k).lower(): v for k, v in row.items()})
        else:
            if names is None:
                names = [str(d[0]).lower() for d in (cursor.description or [])]
            out.append(dict(zip(names, row)))
    return out


def _scalar(cursor, sql: str, params: Sequence[Any] = ()) -> Any:
    cursor.execute(sql, tuple(params))
    rows = cursor.fetchall() or []
    if not rows:
        return None
    row = rows[0]
    if isinstance(row, dict):
        return next(iter(row.values()))
    return row[0]


def _row_values(row: Any, columns: Sequence[str]) -> Tuple[Any, ...]:
    if isinstance(row, dict):
        return tuple(row[c] for c in columns)
    return tuple(row)


def _safe_error(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {str(exc)[:300]}"


# --------------------------------------------------------------------------------------
# Live validation (information_schema + EXPLAIN) - fail closed
# --------------------------------------------------------------------------------------
@dataclass
class TableValidation:
    ok: bool = False
    status: str = Status.SKIPPED
    reason: str = ""
    table_type: Optional[str] = None
    columns: List[str] = field(default_factory=list)
    column_types: Dict[str, str] = field(default_factory=dict)
    date_type: Optional[str] = None
    date_index: Optional[str] = None
    primary_key: Optional[Tuple[str, ...]] = None
    strategy: Optional[str] = None  # 'keyset' | 'date_window'
    explain_export: str = ""
    explain_delete: str = ""
    explain_rows_estimate: Optional[int] = None
    table_rows: Optional[int] = None
    size_bytes: Optional[int] = None


def _fail(v: TableValidation, status: str, reason: str) -> TableValidation:
    v.ok = False
    v.status = status
    v.reason = reason
    return v


def _plan_verdict(plan: List[Dict[str, Any]], date_column: str, leading: Dict[str, str]) -> Tuple[bool, str, Optional[int]]:
    """A plan is acceptable when every step reads an index that LEADS with the date column, as a
    range/ref lookup, without a filesort."""
    if not plan:
        return False, "EXPLAIN returned no rows", None
    estimate: Optional[int] = None
    for step in plan:
        key = step.get("key")
        access = str(step.get("type") or "").lower()
        extra = str(step.get("extra") or "").lower()
        rows = step.get("rows")
        if isinstance(rows, (int, float, Decimal)):
            estimate = int(rows) if estimate is None else estimate + int(rows)
        if not key:
            return False, f"no index used (type={access or '?'})", estimate
        if leading.get(str(key)) != date_column:
            return False, f"index {key} does not lead with {date_column}", estimate
        if access not in ("range", "ref", "eq_ref", "const"):
            return False, f"access type {access or '?'} is not an index range lookup", estimate
        if "filesort" in extra:
            return False, "plan needs a filesort", estimate
    return True, "ok", estimate


def _sample_explain_params(spec: TableSpec, tz: ZoneInfo, cutoff: date) -> Tuple[Any, Any]:
    return start_param(spec.date_kind, cutoff - timedelta(days=1), tz), cutoff_param(spec.date_kind, cutoff, tz)


def validate_table(cursor, spec: TableSpec, tz: ZoneInfo, cutoff: date, cfg: PurgeConfig) -> TableValidation:
    """Everything is read from the live schema; nothing is inferred from migration files."""
    v = TableValidation()
    if spec.date_column is None or spec.date_kind is None:
        return _fail(v, Status.SKIPPED_AMBIGUOUS, "no purge date column is configured for this table")

    cursor.execute(
        "SELECT table_type AS table_type, table_rows AS table_rows, data_length AS data_length, "
        "index_length AS index_length FROM information_schema.tables "
        "WHERE table_schema = DATABASE() AND table_name = %s",
        (spec.table,),
    )
    tables = _fetch_dicts(cursor)
    if not tables:
        return _fail(v, Status.SKIPPED_MISSING, "table does not exist in this database")
    info = tables[0]
    v.table_type = str(info.get("table_type") or "")
    if v.table_type.upper() != "BASE TABLE":
        return _fail(v, Status.SKIPPED_VIEW, f"{spec.table} is a {v.table_type}, not a BASE TABLE")
    try:
        v.table_rows = int(info["table_rows"]) if info.get("table_rows") is not None else None
        v.size_bytes = int(info.get("data_length") or 0) + int(info.get("index_length") or 0)
    except (TypeError, ValueError):
        pass

    cursor.execute(
        "SELECT column_name AS column_name, data_type AS data_type, is_nullable AS is_nullable "
        "FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = %s "
        "ORDER BY ordinal_position",
        (spec.table,),
    )
    columns = _fetch_dicts(cursor)
    nullable: Dict[str, bool] = {}
    for col in columns:
        name = str(col["column_name"])
        v.columns.append(name)
        v.column_types[name] = str(col.get("data_type") or "").lower()
        nullable[name] = str(col.get("is_nullable") or "").upper() == "YES"
    if spec.date_column not in v.column_types:
        return _fail(v, Status.SKIPPED_TYPE_MISMATCH, f"date column {spec.date_column} does not exist")
    v.date_type = v.column_types[spec.date_column]
    if v.date_type not in spec.allowed_types:
        return _fail(
            v, Status.SKIPPED_TYPE_MISMATCH,
            f"date column {spec.date_column} is {v.date_type}; expected one of {', '.join(spec.allowed_types)}",
        )

    cursor.execute(
        "SELECT index_name AS index_name, non_unique AS non_unique, seq_in_index AS seq_in_index, "
        "column_name AS column_name FROM information_schema.statistics "
        "WHERE table_schema = DATABASE() AND table_name = %s ORDER BY index_name, seq_in_index",
        (spec.table,),
    )
    leading: Dict[str, str] = {}
    pk_cols: List[Tuple[int, str]] = []
    for row in _fetch_dicts(cursor):
        index_name = str(row["index_name"])
        seq = int(row["seq_in_index"])
        if seq == 1:
            leading[index_name] = str(row["column_name"])
        if index_name == "PRIMARY":
            pk_cols.append((seq, str(row["column_name"])))
    date_indexes = sorted(name for name, col in leading.items() if col == spec.date_column)
    if not date_indexes:
        return _fail(
            v, Status.SKIPPED_NO_INDEX,
            f"no index leads with {spec.date_column}; add one (see migrations) before this table can be purged",
        )
    v.date_index = date_indexes[0]

    if pk_cols:
        key = tuple(name for _, name in sorted(pk_cols))
        if not any(nullable.get(c, True) for c in key):
            v.primary_key = key
    # keyset paging only with a real unique key; otherwise per-day windows (never pretend to be deterministic)
    v.strategy = "keyset" if v.primary_key else "date_window"

    # EXPLAIN the exact export and delete shapes
    sample_lower, sample_upper = _sample_explain_params(spec, tz, cutoff)
    cutoff_value = cutoff_param(spec.date_kind, cutoff, tz)
    try:
        if v.strategy == "keyset":
            sql, params = keyset_query(spec.table, v.columns, spec.date_column, v.primary_key, cutoff_value, None, 1)
        else:
            sql, params = window_query(spec.table, v.columns, spec.date_column, sample_lower, sample_upper)
        cursor.execute("EXPLAIN " + sql, params)
        export_ok, export_msg, estimate = _plan_verdict(_fetch_dicts(cursor), spec.date_column, leading)
        d_sql, d_params = delete_query(spec.table, spec.date_column, cutoff_value, 1)
        cursor.execute("EXPLAIN " + d_sql, d_params)
        delete_ok, delete_msg, _ = _plan_verdict(_fetch_dicts(cursor), spec.date_column, leading)
    except Exception as exc:
        return _fail(v, Status.SKIPPED_BAD_PLAN, f"could not EXPLAIN the export/delete queries: {_safe_error(exc)}")
    v.explain_export, v.explain_delete = export_msg, delete_msg
    v.explain_rows_estimate = estimate
    if not export_ok:
        return _fail(v, Status.SKIPPED_BAD_PLAN, f"export plan rejected: {export_msg}")
    if not delete_ok:
        return _fail(v, Status.SKIPPED_BAD_PLAN, f"delete plan rejected: {delete_msg}")

    v.ok = True
    v.status = "OK"
    return v


# --------------------------------------------------------------------------------------
# Preflight guards (best effort; none of these is a lock)
# --------------------------------------------------------------------------------------
_WRITE_VERBS = re.compile(r"^\s*(insert|update|delete|replace|alter|load|truncate|drop|rename)\b", re.IGNORECASE)


def check_conflicts(cursor, table: str, env: Mapping[str, str]) -> Optional[str]:
    """Reason string when something suggests old rows of `table` are being (re)written, else None.

    1. Env flags in this container (BACKFILL_MODE etc.): reflects the deployed .env only; manual backfill
       scripts cannot be seen. 2. Live processlist: other sessions on this schema currently running a write
       statement that names the table (needs the PROCESS privilege to see other users; best effort).
    """
    for flag, tables in CONFLICT_ENV_FLAGS.items():
        if table in tables and str(env.get(flag, "")).strip().lower() == "true":
            return f"{flag}=true in this container's environment"
    try:
        cursor.execute("SELECT DATABASE() AS db, CONNECTION_ID() AS id")
        me = _fetch_dicts(cursor)[0]
        cursor.execute("SHOW FULL PROCESSLIST")
        pattern = re.compile(r"\b" + re.escape(table) + r"\b", re.IGNORECASE)
        for proc in _fetch_dicts(cursor):
            if proc.get("db") != me.get("db") or proc.get("id") == me.get("id"):
                continue
            statement = str(proc.get("info") or "")
            if statement and _WRITE_VERBS.match(statement) and pattern.search(statement):
                verb = statement.split(None, 1)[0].upper()  # never log the statement itself (may hold data)
                return f"another session (id {proc.get('id')}) is running {verb} against {table}"
    except Exception as exc:
        logger.warning("preflight processlist check unavailable for %s: %s", table, _safe_error(exc))
    return None


# --------------------------------------------------------------------------------------
# Query builders (one place, so EXPLAIN and execution can never diverge)
# --------------------------------------------------------------------------------------
def _cols_sql(columns: Sequence[str]) -> str:
    return ", ".join(q(c) for c in columns)


def keyset_query(table, columns, date_col, key_cols, cutoff_value, last, batch):
    order = [date_col] + [k for k in key_cols if k != date_col]
    order_sql = ", ".join(q(c) for c in order)
    if last is None:
        return (
            f"SELECT {_cols_sql(columns)} FROM {q(table)} WHERE {q(date_col)} < %s "
            f"ORDER BY {order_sql} LIMIT %s",
            (cutoff_value, batch),
        )
    placeholders = ", ".join(["%s"] * len(order))
    return (
        f"SELECT {_cols_sql(columns)} FROM {q(table)} WHERE ({order_sql}) > ({placeholders}) "
        f"AND {q(date_col)} < %s ORDER BY {order_sql} LIMIT %s",
        (*last, cutoff_value, batch),
    )


def _window_where(date_col, lower):
    if lower is None:
        return f"{q(date_col)} < %s"
    return f"{q(date_col)} >= %s AND {q(date_col)} < %s"


def window_query(table, columns, date_col, lower, upper):
    params = (upper,) if lower is None else (lower, upper)
    return f"SELECT {_cols_sql(columns)} FROM {q(table)} WHERE {_window_where(date_col, lower)}", params


def window_count_query(table, date_col, lower, upper):
    params = (upper,) if lower is None else (lower, upper)
    return f"SELECT COUNT(*) AS n FROM {q(table)} WHERE {_window_where(date_col, lower)}", params


def delete_query(table, date_col, cutoff_value, n):
    return (
        f"DELETE FROM {q(table)} WHERE {q(date_col)} < %s ORDER BY {q(date_col)} LIMIT %s",
        (cutoff_value, n),
    )


def count_eligible(cursor, spec: TableSpec, cutoff_value: Any, hint_ms: Optional[int] = None) -> int:
    hint = f"/*+ MAX_EXECUTION_TIME({int(hint_ms)}) */ " if hint_ms else ""
    return int(_scalar(
        cursor, f"SELECT {hint}COUNT(*) AS n FROM {q(spec.table)} WHERE {q(spec.date_column)} < %s", (cutoff_value,)
    ) or 0)


def _min_max_eligible(cursor, spec: TableSpec, cutoff_value: Any) -> Tuple[Any, Any]:
    cursor.execute(
        f"SELECT MIN({q(spec.date_column)}) AS lo, MAX({q(spec.date_column)}) AS hi "
        f"FROM {q(spec.table)} WHERE {q(spec.date_column)} < %s",
        (cutoff_value,),
    )
    rows = _fetch_dicts(cursor)
    return (rows[0].get("lo"), rows[0].get("hi")) if rows else (None, None)


# --------------------------------------------------------------------------------------
# Streaming CSV export (bounded memory; never materialises the dataset)
# --------------------------------------------------------------------------------------
class ArchiveError(RuntimeError):
    """The archive could not be created or verified. Nothing is deleted."""


class ArchiveInvalid(ArchiveError):
    """An existing archive does not match this operation. Never overwritten; needs manual review."""


@dataclass
class _Counters:
    rows: int = 0
    bytes: int = 0
    sha256: Any = field(default_factory=hashlib.sha256)


def _cell(value: Any) -> str:
    if value is None:
        return NULL_TOKEN
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, bool):
        return "1" if value else "0"
    text = str(value)
    # a literal string starting with a backslash gets one extra, so only NULL is ever exactly \N
    return "\\" + text if text.startswith("\\") else text


def csv_chunks(columns: Sequence[str], rows: Iterator[Tuple[Any, ...]], counters: _Counters) -> Iterator[bytes]:
    """Header + rows as UTF-8 CSV chunks of ~CSV_CHUNK_BYTES. Counts rows/bytes and hashes as it goes."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)

    def flush() -> Optional[bytes]:
        text = buffer.getvalue()
        if not text:
            return None
        buffer.seek(0)
        buffer.truncate(0)
        data = text.encode("utf-8")
        counters.bytes += len(data)
        counters.sha256.update(data)
        return data

    writer.writerow(list(columns))
    for values in rows:
        writer.writerow([_cell(v) for v in values])
        counters.rows += 1
        if buffer.tell() >= CSV_CHUNK_BYTES:
            data = flush()
            if data:
                yield data
    data = flush()
    if data:
        yield data


class _GeneratorReader:
    """File-like object over a bytes generator: pulls from the generator only as the uploader reads."""

    def __init__(self, chunks: Iterator[bytes]):
        self._chunks = iter(chunks)
        self._buffer = b""
        self._done = False

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            data = self._buffer + b"".join(self._chunks)
            self._buffer, self._done = b"", True
            return data
        while len(self._buffer) < size and not self._done:
            try:
                self._buffer += next(self._chunks)
            except StopIteration:
                self._done = True
        out, self._buffer = self._buffer[:size], self._buffer[size:]
        return out

    def readable(self) -> bool:
        return True


def _iter_keyset_rows(cursor, spec, v: TableValidation, cutoff_value, batch) -> Iterator[Tuple[Any, ...]]:
    order = [spec.date_column] + [k for k in v.primary_key if k != spec.date_column]
    last = None
    while True:
        sql, params = keyset_query(spec.table, v.columns, spec.date_column, v.primary_key, cutoff_value, last, batch)
        cursor.execute(sql, params)
        rows = cursor.fetchall() or []
        if not rows:
            return
        for row in rows:
            yield _row_values(row, v.columns)
        last_values = _row_values(rows[-1], v.columns)
        last = tuple(last_values[v.columns.index(c)] for c in order)
        if len(rows) < batch:
            return


def _iter_window_rows(cursor, spec, v: TableValidation, tz, cutoff: date, cutoff_value, cfg) -> Iterator[Tuple[Any, ...]]:
    """One query per calendar-day window. Windows tile `date_col < cutoff` exactly (the first window has no
    lower bound), so export and delete cover the same set."""
    lo, _hi = _min_max_eligible(cursor, spec, cutoff_value)
    if lo is None:
        return
    try:
        first_day = _to_local_day(lo, spec.date_kind, tz)
    except (ValueError, TypeError) as exc:
        raise ArchiveError(f"oldest {spec.date_column} value is not a parseable date: {_safe_error(exc)}") from None
    if first_day < MIN_PLAUSIBLE_DATE:
        raise ArchiveError(f"oldest {spec.date_column} value ({first_day}) is implausibly old; refusing to window it")
    day = first_day
    while day < cutoff:
        lower = None if day == first_day else start_param(spec.date_kind, day, tz)
        upper = start_param(spec.date_kind, day + timedelta(days=1), tz)
        count_sql, count_params = window_count_query(spec.table, spec.date_column, lower, upper)
        in_window = int(_scalar(cursor, count_sql, count_params) or 0)
        if in_window > cfg.max_window_rows:
            raise ArchiveError(
                f"{in_window} rows in the window ending {upper} exceed DATA_PURGE_MAX_WINDOW_ROWS={cfg.max_window_rows}"
            )
        if in_window:
            sql, params = window_query(spec.table, v.columns, spec.date_column, lower, upper)
            cursor.execute(sql, params)
            while True:
                rows = cursor.fetchmany(cfg.export_batch_size)
                if not rows:
                    break
                for row in rows:
                    yield _row_values(row, v.columns)
        day += timedelta(days=1)


def iter_export_rows(cursor, spec, v, tz, cutoff, cutoff_value, cfg) -> Iterator[Tuple[Any, ...]]:
    if v.strategy == "keyset":
        return _iter_keyset_rows(cursor, spec, v, cutoff_value, cfg.export_batch_size)
    return _iter_window_rows(cursor, spec, v, tz, cutoff, cutoff_value, cfg)


# --------------------------------------------------------------------------------------
# S3 archive store (thin boto3 wrapper; tests use an in-memory fake with the same methods)
# --------------------------------------------------------------------------------------
@dataclass(frozen=True)
class ObjectInfo:
    size: int


class S3ArchiveStore:
    """boto3 is imported lazily; credentials come from the default chain (instance role / env)."""

    def __init__(self, bucket: str, client: Any = None, sse: Optional[str] = None, kms_key_id: Optional[str] = None):
        if client is None:
            import boto3

            client = boto3.client("s3")
        self.bucket = bucket
        self.client = client
        self._sse = sse
        self._kms = kms_key_id

    def _sse_args(self) -> Dict[str, Any]:
        args: Dict[str, Any] = {}
        if self._sse:
            args["ServerSideEncryption"] = self._sse
            if self._kms:
                args["SSEKMSKeyId"] = self._kms
        return args

    @staticmethod
    def _code(exc: Exception) -> str:
        response = getattr(exc, "response", None) or {}
        return str(response.get("Error", {}).get("Code", ""))

    def head(self, key: str) -> Optional[ObjectInfo]:
        try:
            response = self.client.head_object(Bucket=self.bucket, Key=key)
        except Exception as exc:
            if self._code(exc) in ("404", "NoSuchKey", "NotFound"):
                return None
            raise
        return ObjectInfo(size=int(response["ContentLength"]))

    def upload_stream(self, key: str, fileobj: Any) -> None:
        from boto3.s3.transfer import TransferConfig

        config = TransferConfig(
            multipart_threshold=8 * 1024 * 1024,
            multipart_chunksize=16 * 1024 * 1024,
            max_concurrency=1,
            use_threads=False,
        )
        extra = {"ContentType": "text/csv", **self._sse_args()}
        self.client.upload_fileobj(fileobj, self.bucket, key, ExtraArgs=extra, Config=config)

    def get_tags(self, key: str) -> Dict[str, str]:
        response = self.client.get_object_tagging(Bucket=self.bucket, Key=key)
        return {t["Key"]: t["Value"] for t in response.get("TagSet", [])}

    def put_tags(self, key: str, tags: Dict[str, str]) -> None:
        self.client.put_object_tagging(
            Bucket=self.bucket, Key=key,
            Tagging={"TagSet": [{"Key": k, "Value": str(v)} for k, v in tags.items()]},
        )

    def get_json(self, key: str) -> Optional[Dict[str, Any]]:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
        except Exception as exc:
            if self._code(exc) in ("404", "NoSuchKey", "NotFound"):
                return None
            raise
        return json.loads(response["Body"].read().decode("utf-8"))

    def read_head(self, key: str, nbytes: int) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=key, Range=f"bytes=0-{nbytes - 1}")
        return response["Body"].read()

    def put_json_if_absent(self, key: str, obj: Dict[str, Any]) -> bool:
        """Create the object only if it does not exist. True if written, False if it already existed."""
        body = json.dumps(obj, indent=2, sort_keys=True, default=str).encode("utf-8")
        kwargs = dict(Bucket=self.bucket, Key=key, Body=body, ContentType="application/json", **self._sse_args())
        try:
            try:
                self.client.put_object(IfNoneMatch="*", **kwargs)
            except Exception as exc:
                if exc.__class__.__name__ == "ParamValidationError":  # older botocore: check-then-put
                    if self.head(key) is not None:
                        return False
                    self.client.put_object(**kwargs)
                else:
                    raise
        except Exception as exc:
            if self._code(exc) in ("PreconditionFailed", "ConditionalRequestConflict", "412"):
                return False
            raise
        return True


# --------------------------------------------------------------------------------------
# Archive keys, manifest and verification
# --------------------------------------------------------------------------------------
def brand_slug(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "_", name.strip()).strip("_") or "brand"


def archive_keys(cfg: PurgeConfig, month: str, brand: str, table: str) -> Tuple[str, str, str]:
    base = f"{cfg.prefix}/{month}/{brand_slug(brand)}/{table}"
    return f"{base}.csv", f"{base}.manifest.json", f"{base}.purge-complete.json"


def build_manifest(run: "RunContext", brand: "BrandContext", spec: TableSpec, v: TableValidation, csv_key: str,
                   rows: int, csv_bytes: int, sha256: str, lo: Any, hi: Any) -> Dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "table": spec.table,
        "brand": brand.name,
        "run_month": run.month,
        "cutoff": run.cutoff.isoformat(),
        "date_column": spec.date_column,
        "date_kind": spec.date_kind.value,
        "row_count": rows,
        "csv_bytes": csv_bytes,
        "sha256": sha256,
        "min_date": None if lo is None else str(lo),
        "max_date": None if hi is None else str(hi),
        "columns": list(v.columns),
        "csv_key": csv_key,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "worker_version": WORKER_VERSION,
    }


def validate_manifest(manifest: Mapping[str, Any], run: "RunContext", brand: "BrandContext", spec: TableSpec,
                      v: TableValidation, csv_key: str) -> None:
    """Raises ArchiveInvalid unless the manifest describes exactly this operation."""
    expected = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "table": spec.table,
        "brand": brand.name,
        "run_month": run.month,
        "cutoff": run.cutoff.isoformat(),
        "date_column": spec.date_column,
        "date_kind": spec.date_kind.value,
        "csv_key": csv_key,
    }
    for key, want in expected.items():
        if manifest.get(key) != want:
            raise ArchiveInvalid(f"manifest {key} is {manifest.get(key)!r}, expected {want!r}")
    rows, size, digest = manifest.get("row_count"), manifest.get("csv_bytes"), manifest.get("sha256")
    if not isinstance(rows, int) or rows <= 0 or not isinstance(size, int) or size <= 0:
        raise ArchiveInvalid("manifest row_count/csv_bytes are not positive integers")
    if not isinstance(digest, str) or len(digest) != 64:
        raise ArchiveInvalid("manifest sha256 is missing or malformed")
    if list(manifest.get("columns") or []) != list(v.columns):
        raise ArchiveInvalid("the table's columns changed since the archive was written (new data would be lost)")


def _verify_csv_object(store, csv_key: str, expected_bytes: int, sha256: Optional[str]) -> None:
    info = store.head(csv_key)
    if info is None:
        raise ArchiveError(f"archive CSV {csv_key} does not exist in S3")
    if info.size <= 0 or info.size != expected_bytes:
        raise ArchiveError(f"archive CSV size {info.size} != expected {expected_bytes}")
    if sha256:
        try:
            tags = store.get_tags(csv_key) or {}
        except Exception:
            tags = {}
        tagged = tags.get("purge-sha256")
        if tagged and tagged != sha256:
            raise ArchiveInvalid("archive CSV sha256 tag does not match the manifest")


def _header_matches(store, csv_key: str, columns: Sequence[str]) -> bool:
    try:
        head = store.read_head(csv_key, HEADER_PROBE_BYTES).decode("utf-8", errors="replace")
        first = next(csv.reader(io.StringIO(head)), [])
    except Exception:
        return False
    return list(first) == list(columns)


# --------------------------------------------------------------------------------------
# Per-table processing
# --------------------------------------------------------------------------------------
@dataclass
class BrandContext:
    index: int
    name: str
    tz: ZoneInfo
    cursor_ctx: Callable[[], AbstractContextManager]  # () -> context manager yielding (cursor, connection)


@dataclass
class RunContext:
    cfg: PurgeConfig
    run_dt: datetime
    cutoff: date
    month: str
    store: Optional[Any]
    env: Mapping[str, str] = field(default_factory=dict)


@dataclass
class TableResult:
    brand: str
    table: str
    status: str = Status.FAILED
    reason: str = ""
    eligible: Optional[int] = None
    eligible_kind: str = "exact"
    exported: int = 0
    csv_bytes: int = 0
    archived: bool = False
    deleted: int = 0
    s3_key: Optional[str] = None
    duration_s: float = 0.0
    error: bool = False
    details: Dict[str, Any] = field(default_factory=dict)


def _lock_name(db: Any, table: str) -> str:
    return f"dp:{db}:{table}"[:64]


def process_table(run: RunContext, brand: BrandContext, spec: TableSpec) -> TableResult:
    """Never raises: every failure becomes a TableResult so other tables/brands continue."""
    started = time.monotonic()
    result = TableResult(brand=brand.name, table=spec.table)
    logger.info("table_started brand=%s table=%s mode=%s cutoff=%s", brand.name, spec.table, run.cfg.mode.value, run.cutoff)
    try:
        _process_table(run, brand, spec, result)
    except ArchiveInvalid as exc:
        result.status, result.reason, result.error = Status.FAILED, f"existing archive invalid: {exc}", True
        logger.error("archive_invalid brand=%s table=%s reason=%s", brand.name, spec.table, exc)
    except Exception as exc:
        result.error = True
        if result.archived:
            result.status, result.reason = Status.ARCHIVED_ONLY, f"archive verified but delete did not complete: {_safe_error(exc)}"
        else:
            result.status, result.reason = Status.FAILED, _safe_error(exc)
        logger.error("table_failed brand=%s table=%s status=%s reason=%s", brand.name, spec.table, result.status, result.reason)
    result.duration_s = time.monotonic() - started
    logger.info(
        "table_completed brand=%s table=%s status=%s eligible=%s exported=%s deleted=%s duration_s=%.1f reason=%s",
        brand.name, spec.table, result.status, result.eligible, result.exported, result.deleted,
        result.duration_s, result.reason,
    )
    return result


def _process_table(run: RunContext, brand: BrandContext, spec: TableSpec, result: TableResult) -> None:
    cfg = run.cfg
    if spec.date_column is None:
        result.status, result.reason = Status.SKIPPED_AMBIGUOUS, spec.note or "no purge date column configured"
        if cfg.mode is Mode.DRY_RUN and spec.table in cfg.tables:
            _describe_ambiguous(brand, spec, result)  # read-only facts to help confirm the schema
        return
    if spec.table not in cfg.tables:
        result.status, result.reason = Status.SKIPPED, "not in DATA_PURGE_TABLES"
        return
    with brand.cursor_ctx() as (cursor, connection):
        db = _scalar(cursor, "SELECT DATABASE() AS db")
        lock = _lock_name(db, spec.table)
        if _scalar(cursor, "SELECT GET_LOCK(%s, 0) AS got", (lock,)) != 1:
            result.status, result.reason = Status.SKIPPED_LOCKED, "another purge holds the lock for this table"
            return
        try:
            _process_locked(run, brand, spec, result, cursor, connection)
        finally:
            try:
                cursor.execute("SELECT RELEASE_LOCK(%s) AS released", (lock,))
                cursor.fetchall()
            except Exception:
                pass


def _describe_ambiguous(brand: BrandContext, spec: TableSpec, result: TableResult) -> None:
    try:
        with brand.cursor_ctx() as (cursor, _connection):
            cursor.execute(
                "SELECT table_type AS table_type, table_rows AS table_rows FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = %s",
                (spec.table,),
            )
            tables = _fetch_dicts(cursor)
            if not tables:
                result.details["live_schema"] = "table does not exist in this database"
                return
            cursor.execute(
                "SELECT column_name AS column_name, data_type AS data_type FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = %s ORDER BY ordinal_position",
                (spec.table,),
            )
            date_like = [
                f"{c['column_name']}({c['data_type']})" for c in _fetch_dicts(cursor)
                if str(c.get("data_type")).lower() in ("date", "datetime", "timestamp")
            ]
            result.details["live_schema"] = (
                f"{tables[0].get('table_type')}; date-like columns: {', '.join(date_like) or 'none'}; "
                f"approx rows: {tables[0].get('table_rows')}"
            )
    except Exception as exc:
        result.details["live_schema"] = f"could not be inspected: {_safe_error(exc)}"


def _process_locked(run, brand, spec, result, cursor, connection) -> None:
    cfg = run.cfg
    dry = cfg.mode is Mode.DRY_RUN
    v = validate_table(cursor, spec, brand.tz, run.cutoff, cfg)
    result.details.update(
        table_type=v.table_type, date_column=spec.date_column, date_type=v.date_type, index=v.date_index,
        unique_key=",".join(v.primary_key) if v.primary_key else None, strategy=v.strategy,
        export_plan=v.explain_export or None, delete_plan=v.explain_delete or None,
        table_rows_approx=v.table_rows, size_bytes=v.size_bytes,
    )
    if not v.ok:
        result.status, result.reason = v.status, v.reason
        result.details["archiveable"], result.details["archiveable_reason"] = False, v.reason
        return

    unconfirmed = not spec.schema_confirmed
    conflict = check_conflicts(cursor, spec.table, run.env)
    cutoff_value = cutoff_param(spec.date_kind, run.cutoff, brand.tz)

    if dry:
        _dry_run_report(run, spec, v, result, cursor, cutoff_value, unconfirmed, conflict)
        return
    if unconfirmed:
        result.status = Status.SKIPPED_UNCONFIRMED
        result.reason = "schema not yet confirmed against the live database; run a dry run and confirm it first"
        return
    if conflict:
        result.status, result.reason = Status.SKIPPED_CONFLICT, conflict
        return

    eligible = count_eligible(cursor, spec, cutoff_value)
    result.eligible = eligible
    csv_key, manifest_key, complete_key = archive_keys(cfg, run.month, brand.name, spec.table)
    result.s3_key = csv_key
    store = run.store

    manifest = store.get_json(manifest_key)
    if manifest is None and eligible == 0:
        result.status, result.reason = Status.NO_DATA, "no rows eligible for purge"
        return

    if manifest is not None:
        validate_manifest(manifest, run, brand, spec, v, csv_key)
        _verify_csv_object(store, csv_key, manifest["csv_bytes"], manifest["sha256"])
        logger.info("archive_reused brand=%s table=%s rows=%s", brand.name, spec.table, manifest["row_count"])
    else:
        manifest = _create_archive(run, brand, spec, v, cursor, cutoff_value, eligible, csv_key, manifest_key)
    result.archived = True
    result.exported = manifest["row_count"]
    result.csv_bytes = manifest["csv_bytes"]

    if eligible == 0:
        result.status, result.reason = Status.SUCCESS, "already purged (archive verified, nothing left to delete)"
        return
    _maybe_delete(run, brand, spec, result, cursor, connection, manifest, eligible, cutoff_value, complete_key)


def _dry_run_report(run, spec, v: TableValidation, result, cursor, cutoff_value, unconfirmed, conflict) -> None:
    """Read-only: no S3 writes, no deletes, no full-table scans."""
    cfg = run.cfg
    try:
        result.eligible = count_eligible(cursor, spec, cutoff_value, hint_ms=60_000)
        result.eligible_kind = "exact"
    except Exception as exc:
        if v.explain_rows_estimate is not None:
            result.eligible, result.eligible_kind = v.explain_rows_estimate, "estimated"
        else:
            result.eligible, result.eligible_kind = None, "skipped (expensive)"
        logger.warning("dry-run exact count skipped for %s: %s", spec.table, _safe_error(exc))
    try:
        lo, _ = _min_max_eligible(cursor, spec, cutoff_value)
        result.details["oldest_eligible"] = None if lo is None else str(lo)
    except Exception:
        result.details["oldest_eligible"] = "skipped"

    archiveable, why = True, "ok"
    if unconfirmed:
        archiveable, why = False, "schema not confirmed against the live database yet"
    elif conflict:
        archiveable, why = False, f"conflict: {conflict}"
    result.details["archiveable"], result.details["archiveable_reason"] = archiveable, why

    deletable, dwhy = archiveable, why
    if archiveable:
        if cfg.purge_test:
            deletable, dwhy = False, "PURGE_TEST=true: export only, never deletes"
        elif cfg.intended_mode is Mode.DISABLED:
            deletable, dwhy = False, "DATA_PURGE_ENABLED is false"
        elif cfg.intended_mode is Mode.ARCHIVE_ONLY:
            deletable, dwhy = False, "DATA_PURGE_ARCHIVE_ONLY is true (default): archive only"
        elif spec.table not in cfg.delete_tables:
            deletable, dwhy = False, "not in DATA_PURGE_DELETE_TABLES"
        else:
            dwhy = "ok (would delete in batches after a verified archive)"
    result.details["deletion_supported"], result.details["deletion_reason"] = deletable, dwhy
    result.details["conflict"] = conflict
    result.status = Status.DRY_RUN
    result.reason = "dry run (read-only)"


def _create_archive(run, brand, spec, v, cursor, cutoff_value, eligible, csv_key, manifest_key) -> Dict[str, Any]:
    store = run.store
    lo, hi = _min_max_eligible(cursor, spec, cutoff_value)

    info = store.head(csv_key)
    if info is not None:
        reused = _try_reuse_orphan(run, brand, spec, v, store, info, csv_key, manifest_key, eligible, lo, hi)
        if reused is not None:
            return reused
        logger.warning("replacing unconfirmed CSV without manifest brand=%s table=%s key=%s", brand.name, spec.table, csv_key)

    counters = _Counters()
    rows = iter_export_rows(cursor, spec, v, brand.tz, run.cutoff, cutoff_value, run.cfg)
    logger.info("export_started brand=%s table=%s eligible=%s strategy=%s", brand.name, spec.table, eligible, v.strategy)
    store.upload_stream(csv_key, _GeneratorReader(csv_chunks(v.columns, rows, counters)))
    logger.info("export_completed brand=%s table=%s rows=%s csv_bytes=%s", brand.name, spec.table, counters.rows, counters.bytes)

    info = store.head(csv_key)
    if info is None or counters.bytes <= 0 or counters.rows <= 0 or info.size != counters.bytes:
        raise ArchiveError(
            f"S3 verification failed: expected {counters.bytes} bytes / {counters.rows} rows, found "
            f"{None if info is None else info.size} bytes"
        )
    digest = counters.sha256.hexdigest()
    try:
        store.put_tags(csv_key, {
            "purge-status": "complete", "purge-rows": str(counters.rows), "purge-bytes": str(counters.bytes),
            "purge-cutoff": run.cutoff.isoformat(), "purge-sha256": digest,
        })
    except Exception as exc:  # best effort: the manifest is authoritative
        logger.warning("could not tag %s (non-fatal): %s", csv_key, _safe_error(exc))
    logger.info("s3_verified brand=%s table=%s key=%s", brand.name, spec.table, csv_key)

    # Sanity check only (not proof the same rows were exported): counts must agree before and after.
    recount = count_eligible(cursor, spec, cutoff_value)
    if counters.rows != eligible or counters.rows != recount:
        raise ArchiveError(
            f"row-count sanity check failed (eligible before={eligible}, exported={counters.rows}, recount={recount}); "
            "rows changed during the export - no manifest written, nothing deleted"
        )

    manifest = build_manifest(run, brand, spec, v, csv_key, counters.rows, counters.bytes, digest, lo, hi)
    return _write_manifest(run, brand, spec, v, store, manifest_key, csv_key, manifest)


def _write_manifest(run, brand, spec, v, store, manifest_key, csv_key, manifest) -> Dict[str, Any]:
    if store.put_json_if_absent(manifest_key, manifest):
        return manifest
    existing = store.get_json(manifest_key)  # someone else wrote it first: it must be valid for this operation
    if existing is None:
        raise ArchiveError("manifest write was rejected but no manifest can be read back")
    validate_manifest(existing, run, brand, spec, v, csv_key)
    _verify_csv_object(store, csv_key, existing["csv_bytes"], existing["sha256"])
    return existing


def _try_reuse_orphan(run, brand, spec, v, store, info, csv_key, manifest_key, eligible, lo, hi) -> Optional[Dict[str, Any]]:
    """A CSV exists but no manifest (crash between upload and manifest). Reuse it ONLY when it is positively
    confirmed complete; otherwise return None so the caller replaces it deterministically."""
    try:
        tags = store.get_tags(csv_key) or {}
    except Exception:
        return None
    needed = ("purge-status", "purge-rows", "purge-bytes", "purge-cutoff", "purge-sha256")
    if any(k not in tags for k in needed) or tags["purge-status"] != "complete":
        return None
    try:
        rows, size = int(tags["purge-rows"]), int(tags["purge-bytes"])
    except ValueError:
        return None
    if tags["purge-cutoff"] != run.cutoff.isoformat() or info.size != size or size <= 0:
        return None
    if rows != eligible or rows <= 0:
        return None
    if not _header_matches(store, csv_key, v.columns):
        return None
    manifest = build_manifest(run, brand, spec, v, csv_key, rows, size, tags["purge-sha256"], lo, hi)
    logger.info("orphan CSV positively confirmed; writing its manifest brand=%s table=%s", brand.name, spec.table)
    return _write_manifest(run, brand, spec, v, store, manifest_key, csv_key, manifest)


# --------------------------------------------------------------------------------------
# Delete (only reachable with a verified archive)
# --------------------------------------------------------------------------------------
def _maybe_delete(run, brand, spec, result, cursor, connection, manifest, eligible, cutoff_value, complete_key) -> None:
    cfg = run.cfg
    if cfg.purge_test:  # belt and braces: PURGE_TEST can never reach a DELETE even if the mode logic changes
        result.status, result.reason = Status.ARCHIVED_ONLY, "PURGE_TEST=true: archive verified, nothing deleted"
        return
    if cfg.mode is not Mode.FULL:
        result.status, result.reason = Status.ARCHIVED_ONLY, "archive-only mode: archive verified, nothing deleted"
        return
    if spec.table not in cfg.delete_tables:
        result.status, result.reason = Status.ARCHIVED_ONLY, "archive verified; table is not in DATA_PURGE_DELETE_TABLES"
        return
    cap = int(manifest["row_count"])
    if eligible > cap:
        result.status, result.error = Status.FAILED, True
        result.reason = (f"{eligible} rows are eligible but only {cap} were archived (rows appeared after the archive); "
                         "refusing to delete - manual review needed")
        return
    if not _table_would_keep_rows(cursor, spec, cutoff_value):
        result.status = Status.ARCHIVED_ONLY
        result.reason = "every row is older than the cutoff; refusing to empty the table (ingestion watermarks need a row)"
        return

    deleted = delete_in_batches(cursor, connection, spec, cutoff_value, cap, cfg, brand.name)
    result.deleted = deleted
    remaining = _scalar(
        cursor, f"SELECT 1 AS n FROM {q(spec.table)} WHERE {q(spec.date_column)} < %s LIMIT 1", (cutoff_value,)
    )
    if remaining:
        result.status, result.error = Status.FAILED, True
        result.reason = f"{deleted} rows deleted (cap {cap}) but eligible rows remain; manual review needed"
        return
    result.status, result.reason = Status.SUCCESS, "archived, verified and deleted"
    try:
        run.store.put_json_if_absent(complete_key, {
            "table": spec.table, "brand": brand.name, "run_month": run.month, "cutoff": run.cutoff.isoformat(),
            "rows_deleted": deleted, "completed_at": datetime.now(timezone.utc).isoformat(),
        })
    except Exception as exc:
        logger.warning("could not write completion marker (non-fatal): %s", _safe_error(exc))


def _table_would_keep_rows(cursor, spec: TableSpec, cutoff_value: Any) -> bool:
    kept = _scalar(
        cursor, f"SELECT 1 AS n FROM {q(spec.table)} WHERE {q(spec.date_column)} >= %s LIMIT 1", (cutoff_value,)
    )
    if kept:
        return True
    null_row = _scalar(cursor, f"SELECT 1 AS n FROM {q(spec.table)} WHERE {q(spec.date_column)} IS NULL LIMIT 1")
    return bool(null_row)


def delete_in_batches(cursor, connection, spec: TableSpec, cutoff_value: Any, cap_rows: int,
                      cfg: PurgeConfig, brand_name: str) -> int:
    """Batched `DELETE ... ORDER BY date LIMIT n`, commit per batch, never more than `cap_rows` in total."""
    deleted = 0
    batch_no = 0
    started = time.monotonic()
    while deleted < cap_rows:
        n = min(cfg.delete_batch_size, cap_rows - deleted)
        sql, params = delete_query(spec.table, spec.date_column, cutoff_value, n)
        batch_started = time.monotonic()
        try:
            cursor.execute(sql, params)
            affected = int(cursor.rowcount or 0)
            connection.commit()
        except Exception as exc:
            logger.error("delete_batch_failed brand=%s table=%s batch=%s error=%s", brand_name, spec.table, batch_no + 1, _safe_error(exc))
            raise
        if affected <= 0:
            break
        batch_no += 1
        deleted += affected
        elapsed = max(time.monotonic() - batch_started, 1e-9)
        logger.info(
            "delete_batch brand=%s table=%s batch=%s rows=%s batch_s=%.2f cumulative=%s rows_per_s=%.0f",
            brand_name, spec.table, batch_no, affected, elapsed, deleted,
            deleted / max(time.monotonic() - started, 1e-9),
        )
        if cfg.sleep_between_batches_s > 0:
            time.sleep(cfg.sleep_between_batches_s)
    return deleted


# --------------------------------------------------------------------------------------
# Run + summary
# --------------------------------------------------------------------------------------
def run_purge(brands: Sequence[BrandContext], run: RunContext) -> List[TableResult]:
    """Brands and tables strictly sequentially; one failure never stops the others."""
    results: List[TableResult] = []
    logger.info(
        "data_purge_started mode=%s purge_test=%s run=%s cutoff=%s archive_month=%s tz=%s tables=%s delete_tables=%s",
        run.cfg.mode.value, run.cfg.purge_test, run.run_dt.isoformat(), run.cutoff, run.month, SCHEDULER_TZ.key,
        ",".join(run.cfg.tables), ",".join(run.cfg.delete_tables) or "-",
    )
    for brand in brands:
        for spec in PURGE_TABLES:
            results.append(process_table(run, brand, spec))
    return results


def any_failure(results: Sequence[TableResult]) -> bool:
    return any(r.status == Status.FAILED or r.error for r in results)


def _human_bytes(n: int) -> str:
    size = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{n} B"


def format_summary(results: Sequence[TableResult], run: RunContext, duration_s: float) -> str:
    cfg = run.cfg
    where = f"s3://{cfg.bucket}/{cfg.prefix}/{run.month}/" if cfg.bucket else "(no bucket - read-only)"
    lines = [
        "DATA PURGE SUMMARY",
        "",
        f"Mode: {cfg.mode.value}" + ("  (PURGE_TEST=true: export only, nothing is deleted)" if cfg.purge_test else ""),
        f"Run date: {_as_ist(run.run_dt).date()} (IST)",
        f"Cutoff: {run.cutoff}  (rows with date < cutoff)",
        f"Archive: {where}",
    ]
    total_archived = total_deleted = total_bytes = 0
    for r in results:
        lines.append("")
        lines.append(f"{r.brand} / {r.table}")
        if r.eligible is not None:
            suffix = "" if r.eligible_kind == "exact" else f" ({r.eligible_kind})"
            lines.append(f"  eligible: {r.eligible:,}{suffix}")
        else:
            lines.append(f"  eligible: n/a ({r.eligible_kind})")
        if r.status != Status.DRY_RUN:
            lines.append(f"  exported: {r.exported:,}")
            lines.append(f"  archived: {'YES' if r.archived else 'NO'}")
            lines.append(f"  deleted: {r.deleted:,}")
        lines.append(f"  status: {r.status}" + (f" - {r.reason}" if r.reason else ""))
        if r.details.get("live_schema"):
            lines.append(f"  live schema: {r.details['live_schema']}")
        if r.status == Status.DRY_RUN:
            d = r.details
            lines.append(
                f"  date column: {d.get('date_column')} ({d.get('date_type')}), index: {d.get('index')}, "
                f"unique key: {d.get('unique_key') or 'none'}, strategy: {d.get('strategy')}"
            )
            lines.append(f"  archiveable: {d.get('archiveable')} ({d.get('archiveable_reason')})")
            lines.append(f"  deletion supported: {d.get('deletion_supported')} ({d.get('deletion_reason')})")
            lines.append(f"  oldest eligible: {d.get('oldest_eligible')}, table size: "
                         f"{_human_bytes(d['size_bytes']) if d.get('size_bytes') is not None else 'skipped'}")
        total_archived += r.exported if r.archived else 0
        total_deleted += r.deleted
        total_bytes += r.csv_bytes
    lines += [
        "",
        f"Total rows archived: {total_archived:,}",
        f"Total rows deleted: {total_deleted:,}",
        f"Total archive size: {_human_bytes(total_bytes)}",
        f"Duration: {duration_s:.1f}s",
    ]
    return "\n".join(lines)
