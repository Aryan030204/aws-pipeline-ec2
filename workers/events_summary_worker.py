import base64
import hashlib
import logging
import os
import re
import sys
import tempfile
from collections import defaultdict
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple
from zoneinfo import ZoneInfo

import certifi
import mysql.connector
from dotenv import load_dotenv
from pymongo import MongoClient


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Run directly (`python workers/events_summary_worker.py`), sys.path[0] is
# workers/, not the repo root - needed so the per-brand raw-events phase can
# import aws_background.py. Same pattern the other brand-aware workers use.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

IST = ZoneInfo("Asia/Kolkata")
MONGO_URI = os.environ.get("MONGO_URI", "").strip()
MASTER_DB_HOST = os.environ.get("MASTER_DB_HOST", "").strip()
MASTER_DB_PORT = int(os.environ.get("MASTER_DB_PORT", "3306"))
MASTER_DB_USER = os.environ.get("MASTER_DB_USER", "").strip()
MASTER_DB_PASSWORD = os.environ.get("MASTER_DB_PASSWORD", "").strip()
MASTER_DB_NAME = os.environ.get("MASTER_DB_NAME", "").strip()
MONGO_DB_NAME = os.environ.get("MONGO_EVENTS_DB_NAME", "alerts").strip() or "alerts"
MONGO_COLLECTION_NAME = (
    os.environ.get("MONGO_EVENTS_COLLECTION_NAME", "sessions").strip() or "sessions"
)
EVENTS_SUMMARY_TABLE = "events_summary"
EVENT_COLUMN_SUFFIX = "_count"

# Mongo cursor batch size for the master job: documents are processed as they stream in and are
# never collected into a list (see aggregate_event_documents).
EVENTS_FETCH_BATCH_SIZE = int(os.environ.get("EVENTS_SUMMARY_FETCH_BATCH_SIZE", "5000") or 5000)
# Wait this long after the container starts before the first incremental run, so a reboot/restart does
# not start every pipeline's work at the same moment. The expensive master rebuild never runs at startup.
EVENTS_WORKER_STARTUP_DELAY_S = int(os.environ.get("EVENTS_WORKER_STARTUP_DELAY_S", "90") or 0)
# Master rebuild: 00:03 IST (18:33 UTC). Offset from the other midnight jobs and the ETL's :00 tick.
MASTER_CRON_UTC_HOUR = 18
MASTER_CRON_UTC_MINUTE = 33


def now_ist() -> datetime:
    return datetime.now(IST)


def compute_rebuild_dates(current_dt: Optional[datetime] = None) -> List[date]:
    local_now = current_dt.astimezone(IST) if current_dt else now_ist()
    today = local_now.date()
    return [today - timedelta(days=2), today - timedelta(days=1)]


def compute_utc_bounds_for_dates(target_dates: Sequence[date]) -> Tuple[datetime, datetime]:
    if not target_dates:
        raise ValueError("target_dates must not be empty")

    start_date = min(target_dates)
    end_date = max(target_dates) + timedelta(days=1)
    start_dt = datetime.combine(start_date, time.min, tzinfo=IST)
    end_dt = datetime.combine(end_date, time.min, tzinfo=IST)
    return start_dt.astimezone(timezone.utc), end_dt.astimezone(timezone.utc)


def sanitize_event_type(event_type: object) -> Optional[str]:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(event_type or "").strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized:
        return None
    return f"{normalized}{EVENT_COLUMN_SUFFIX}"


def quote_identifier(identifier: str) -> str:
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", identifier or ""):
        raise ValueError(f"Unsafe SQL identifier: {identifier!r}")
    return f"`{identifier}`"


def _extract_session_id(doc: Dict) -> Optional[str]:
    session_id = str(doc.get("session_id") or "").strip()
    return session_id or None


def _extract_cart_token(doc: Dict) -> Optional[str]:
    cart_token = str(doc.get("cart_token") or "").strip()
    return cart_token or None


def _pair_digest(local_date: date, shop_name: str, column_name: str, session_id: str, cart_token: str) -> int:
    """128-bit digest of one (date, shop, event column, session, cart token) pair.

    The dedup rule only needs to know whether a pair was seen once or more than once, so the pair
    is remembered as a 16-byte digest instead of keeping every Mongo document. Parts are
    length-prefixed so different splits of the same characters cannot produce the same input.
    """
    h = hashlib.blake2b(digest_size=16)
    for part in (local_date.isoformat(), shop_name, column_name, session_id, cart_token):
        encoded = part.encode("utf-8")
        h.update(len(encoded).to_bytes(4, "big"))
        h.update(encoded)
    return int.from_bytes(h.digest(), "big")


def aggregate_event_documents(
    docs: Iterable[Dict],
    target_dates: Set[date],
    tz: ZoneInfo = IST,
    stats: Optional[Dict[str, int]] = None,
) -> Tuple[Dict[Tuple[date, str], Dict[str, int]], Set[str]]:
    """Counts events per (IST date, shop, event column), keeping only pairs seen exactly once.

    Rule (unchanged): events sharing (date, shop, event_type, session_id, cart_token) are repeats;
    when a pair occurs more than once it is dropped entirely, when it occurs once it counts as 1.

    `docs` is consumed as a stream (any iterable, one pass). Only a digest per distinct pair is
    kept, never the documents. `stats["docs"]` receives the number of documents read.
    """
    counts: Dict[Tuple[date, str], Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    seen_once: Set[int] = set()
    seen_repeated: Set[int] = set()
    read = 0

    for doc in docs:
        read += 1
        created_at = doc.get("createdAt")
        if not isinstance(created_at, datetime):
            continue
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)

        local_dt = created_at.astimezone(tz)
        local_date = local_dt.date()
        if local_date not in target_dates:
            continue

        shop_name = str(doc.get("shop_name") or "").strip()
        if not shop_name:
            continue

        column_name = sanitize_event_type(doc.get("event_type"))
        if not column_name:
            logger.warning("Skipping event with unusable event_type=%r", doc.get("event_type"))
            continue

        session_id = _extract_session_id(doc)
        cart_token = _extract_cart_token(doc)
        if not session_id or not cart_token:
            continue

        digest = _pair_digest(local_date, shop_name, column_name, session_id, cart_token)
        if digest in seen_repeated:
            continue
        bucket = counts[(local_date, shop_name)]
        if digest in seen_once:
            # Second sighting: the pair is a repeat, so take back the count it earned as unique.
            seen_once.discard(digest)
            seen_repeated.add(digest)
            bucket[column_name] -= 1
        else:
            seen_once.add(digest)
            bucket[column_name] += 1

    if stats is not None:
        stats["docs"] = read

    # Only pairs that stayed unique leave a count behind; drop columns/rows that fell back to zero.
    aggregated: Dict[Tuple[date, str], Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    event_columns: Set[str] = set()
    for key, columns in counts.items():
        for column_name, value in columns.items():
            if value > 0:
                aggregated[key][column_name] = value
                event_columns.add(column_name)
    return aggregated, event_columns


def summarize_aggregated_counts(
    aggregated: Dict[Tuple[date, str], Dict[str, int]],
) -> Dict[str, Dict[str, object]]:
    summary: Dict[str, Dict[str, object]] = {}

    for (bucket_date, shop_name), counts in sorted(aggregated.items()):
        non_zero_counts = {
            column: int(value)
            for column, value in sorted(counts.items())
            if int(value or 0) > 0
        }
        total_events = sum(non_zero_counts.values())
        shop_summary = summary.setdefault(
            shop_name,
            {
                "rows": 0,
                "total_events": 0,
                "dates": {},
            },
        )
        shop_summary["rows"] += 1
        shop_summary["total_events"] += total_events
        shop_summary["dates"][bucket_date.isoformat()] = {
            "total_events": total_events,
            "event_counts": non_zero_counts,
        }

    return summary


def _write_b64_to_tempfile(b64_str: str, prefix: str) -> str:
    data = base64.b64decode(b64_str.encode("utf-8"))
    fd, path = tempfile.mkstemp(prefix=prefix, suffix=".pem")
    with os.fdopen(fd, "wb") as handle:
        handle.write(data)
    return path


def resolve_master_ca_bundle_path() -> Optional[str]:
    mode = os.environ.get("MASTER_DB_TLS_CA_MODE", "certifi").strip().lower()
    if mode == "certifi":
        return certifi.where()
    if mode == "rds":
        path = os.environ.get("MASTER_RDS_CA_PATH", "").strip()
        if path and Path(path).exists():
            return path
        bundle_b64 = os.environ.get("MASTER_RDS_CA_BUNDLE_B64", "").strip()
        if bundle_b64:
            return _write_b64_to_tempfile(bundle_b64, prefix="master-rds-ca-")
        return None
    if mode == "custom":
        path = os.environ.get("MASTER_CUSTOM_CA_PATH", "").strip()
        if path and Path(path).exists():
            return path
        bundle_b64 = os.environ.get("MASTER_CUSTOM_CA_BUNDLE_B64", "").strip()
        if bundle_b64:
            return _write_b64_to_tempfile(bundle_b64, prefix="master-custom-ca-")
        return None
    if mode == "none":
        return None
    return certifi.where()


def build_master_db_config() -> Dict[str, object]:
    missing = [
        name
        for name, value in (
            ("MASTER_DB_HOST", MASTER_DB_HOST),
            ("MASTER_DB_USER", MASTER_DB_USER),
            ("MASTER_DB_PASSWORD", MASTER_DB_PASSWORD),
            ("MASTER_DB_NAME", MASTER_DB_NAME),
        )
        if not value
    ]
    if missing:
        raise ValueError(f"Missing required master DB env vars: {', '.join(missing)}")

    config: Dict[str, object] = {
        "host": MASTER_DB_HOST,
        "port": MASTER_DB_PORT,
        "user": MASTER_DB_USER,
        "password": MASTER_DB_PASSWORD,
        "database": MASTER_DB_NAME,
        "connection_timeout": int(os.environ.get("MASTER_DB_CONNECT_TIMEOUT_S", "10")),
        "read_timeout": int(os.environ.get("MASTER_DB_READ_TIMEOUT_S", "120")),
        "write_timeout": int(os.environ.get("MASTER_DB_WRITE_TIMEOUT_S", "120")),
    }

    ca_path = resolve_master_ca_bundle_path()
    if MASTER_DB_HOST.lower() not in {"localhost", "127.0.0.1"} and ca_path:
        config.update(
            {
                "ssl_ca": ca_path,
                "ssl_verify_cert": os.environ.get(
                    "MASTER_DB_SSL_VERIFY_CERT", "true"
                ).strip().lower()
                == "true",
                "ssl_verify_identity": os.environ.get(
                    "MASTER_DB_SSL_VERIFY_IDENTITY", "false"
                ).strip().lower()
                == "true",
            }
        )

    return config


@contextmanager
def get_master_db_cursor(dictionary: bool = True):
    connection = mysql.connector.connect(**build_master_db_config())
    cursor = connection.cursor(dictionary=dictionary)
    try:
        yield cursor, connection
    finally:
        try:
            cursor.close()
        finally:
            connection.close()


def resolve_mongo_collection():
    if not MONGO_URI:
        raise ValueError("MONGO_URI is not configured")
    client = MongoClient(MONGO_URI, tz_aware=True)
    return client, client[MONGO_DB_NAME][MONGO_COLLECTION_NAME]


def ensure_events_summary_table(cursor, connection) -> None:
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {quote_identifier(EVENTS_SUMMARY_TABLE)} (
            `date` DATE NOT NULL,
            `shop_name` VARCHAR(255) NOT NULL,
            `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`date`, `shop_name`),
            KEY `idx_shop_name` (`shop_name`),
            KEY `idx_updated_at` (`updated_at`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    connection.commit()


def get_existing_columns(cursor) -> Set[str]:
    cursor.execute(f"SHOW COLUMNS FROM {quote_identifier(EVENTS_SUMMARY_TABLE)}")
    return {
        str(row["Field"] if isinstance(row, dict) else row[0])
        for row in cursor.fetchall() or []
    }


def get_event_count_columns(existing_columns: Iterable[str]) -> List[str]:
    return sorted(
        column
        for column in existing_columns
        if column.endswith(EVENT_COLUMN_SUFFIX)
        and re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", column)
    )


def ensure_event_columns(cursor, connection, event_columns: Iterable[str]) -> None:
    existing_columns = get_existing_columns(cursor)
    missing_columns = sorted(set(event_columns) - existing_columns)
    for column in missing_columns:
        cursor.execute(
            f"ALTER TABLE {quote_identifier(EVENTS_SUMMARY_TABLE)} "
            f"ADD COLUMN {quote_identifier(column)} INT NOT NULL DEFAULT 0"
        )
    if missing_columns:
        connection.commit()


def delete_target_dates(cursor, connection, target_dates: Sequence[date]) -> None:
    if not target_dates:
        return
    placeholders = ", ".join(["%s"] * len(target_dates))
    cursor.execute(
        f"DELETE FROM {quote_identifier(EVENTS_SUMMARY_TABLE)} "
        f"WHERE `date` IN ({placeholders})",
        tuple(target_dates),
    )
    connection.commit()


def build_event_summary_rows(
    aggregated: Dict[Tuple[date, str], Dict[str, int]],
    all_event_columns: Sequence[str],
) -> List[Tuple]:
    rows: List[Tuple] = []
    for (bucket_date, shop_name), counts in sorted(aggregated.items()):
        values = [bucket_date, shop_name]
        for column in all_event_columns:
            values.append(int(counts.get(column, 0)))
        rows.append(tuple(values))
    return rows


def insert_event_summary_rows(
    cursor,
    connection,
    aggregated: Dict[Tuple[date, str], Dict[str, int]],
    all_event_columns: Sequence[str],
) -> int:
    rows = build_event_summary_rows(aggregated, all_event_columns)
    if not rows:
        return 0

    columns = ["date", "shop_name", *all_event_columns]
    quoted_columns = ", ".join(quote_identifier(column) for column in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = (
        f"INSERT INTO {quote_identifier(EVENTS_SUMMARY_TABLE)} "
        f"({quoted_columns}) VALUES ({placeholders})"
    )
    cursor.executemany(sql, rows)
    connection.commit()
    return len(rows)


def fetch_mongo_events_for_dates(target_dates: Sequence[date]) -> Iterator[Dict]:
    """Streams the events of the given IST dates from Mongo, one batch at a time.

    This is a generator: documents are never gathered into a list. The Mongo client is closed
    when the stream is exhausted or abandoned.
    """
    start_utc, end_utc = compute_utc_bounds_for_dates(target_dates)
    client = None
    try:
        client, collection = resolve_mongo_collection()
        cursor = collection.find(
            {
                "createdAt": {"$gte": start_utc, "$lt": end_utc},
            },
            {
                "createdAt": 1,
                "event_type": 1,
                "shop_name": 1,
                "session_id": 1,
                "cart_token": 1,
            },
            batch_size=EVENTS_FETCH_BATCH_SIZE,
        )
        for doc in cursor:
            yield doc
    finally:
        if client is not None:
            client.close()


# Sync functions run against each brand's freshly-ingested mongo_events_summary
# rows, per affected-date window, after the raw ingest below - these used to
# run inside the main pipeline every 10 minutes, querying alerts.sessions
# (Mongo) directly; see pipeline/mongo_events_sql.py and
# pipeline/orchestration.py's update_sessions_summary for where they moved
# from.
_CI_BUY_NOW_SYNC_FN_NAMES = (
    "sync_checkout_initiated_ci_events_sql",
    "sync_checkout_initiated_variant_events_sql",
    "sync_buy_now_events_sql",
    "sync_buy_now_variant_events_sql",
    "update_overall_summary_event_columns_sql",
)


def run_incremental_events_job() -> None:
    """
    Every 15 minutes: incremental raw ingest (pipeline/mongo_events_raw.py -
    watermarked, deletes each document from Mongo once it's durably
    upserted) into each CI-events brand's own mongo_events_summary table,
    then recomputes that brand's CI/buy-now rollups
    (hourly_sessions_summary_shopify's ci_events/buy_now_events,
    daily_ci_events_summary, daily_buy_now_events_summary, their variant
    tables, and overall_summary's event-total columns) for whatever dates
    this run's ingest actually touched - nothing to do if a brand had no new
    events this run.

    aws_background is imported lazily so the master-summary phase (daily,
    unrelated cadence) stays self-contained. Each brand uses one shared DB
    connection for both the ingest and the rollup recompute; a brand's
    failure doesn't stop the next brand.
    """
    import aws_background

    aws_background.active_brand_indices.clear()
    aws_background.initialize_brand_configs()

    if not aws_background.active_brand_indices:
        logger.warning("No active brands with valid database configs. Skipping incremental events job.")
        return

    for brand_index in aws_background.active_brand_indices:
        brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
        brand_key = os.environ.get(f"BRAND_TAG_{brand_index}")

        if not aws_background.is_ci_events_brand(brand_name, brand_key):
            logger.info(
                "Skipping incremental events job for %s: brand not enabled for ci_events", brand_name
            )
            continue

        try:
            with aws_background.get_db_cursor(brand_index) as (cursor, connection):
                affected_dates = aws_background.sync_mongo_events_raw_for_brand(
                    brand_index=brand_index,
                    brand_name=brand_name,
                    brand_key=brand_key,
                    cursor=cursor,
                    connection=connection,
                )

                if not affected_dates:
                    continue

                start_date = min(date.fromisoformat(d) for d in affected_dates)
                end_date = max(date.fromisoformat(d) for d in affected_dates)
                logger.info(
                    "Recomputing CI/buy-now rollups for %s: %s..%s (%s affected date(s))",
                    brand_name, start_date, end_date, len(affected_dates),
                )

                for fn_name in _CI_BUY_NOW_SYNC_FN_NAMES:
                    try:
                        getattr(aws_background, fn_name)(
                            brand_index=brand_index,
                            brand_name=brand_name,
                            start_date=start_date,
                            end_date=end_date,
                            brand_key=brand_key,
                            cursor=cursor,
                            connection=connection,
                        )
                    except Exception as e:
                        logger.error("Error running %s for %s: %s", fn_name, brand_name, e)
        except Exception as e:
            logger.error("Error in incremental events job for %s: %s", brand_name, e)

    logger.info("Incremental events job complete.")


def run_worker(current_dt: Optional[datetime] = None) -> None:
    """
    Manual/one-off entry point (EVENTS_WORKER_SELF_RUN=false): runs both
    independently-scheduled phases once. The live scheduled container runs
    them on their own separate cadences instead - see _run_scheduled.
    """
    try:
        _run_master_events_summary(current_dt)
    except Exception as e:
        logger.error("Master events_summary rebuild failed: %s", e)
        master_error = e
    else:
        master_error = None

    # Independent of the master summary above: a failure in either must not
    # stop the other from running.
    try:
        run_incremental_events_job()
    except Exception as e:
        logger.error("Incremental events job failed: %s", e)

    if master_error is not None:
        raise master_error


def _run_master_events_summary(current_dt: Optional[datetime] = None) -> None:
    target_dates = compute_rebuild_dates(current_dt=current_dt)
    logger.info(
        "Starting events summary worker for IST dates: %s",
        ", ".join(day.isoformat() for day in target_dates),
    )

    # One IST day at a time, streamed. A pair key includes the date, so processing days separately
    # gives the same result as one pass, and the dedup state of a day is released before the next.
    aggregated: Dict[Tuple[date, str], Dict[str, int]] = {}
    new_event_columns: Set[str] = set()
    docs_read = 0
    for target_day in target_dates:
        day_stats: Dict[str, int] = {}
        day_aggregated, day_columns = aggregate_event_documents(
            fetch_mongo_events_for_dates([target_day]), {target_day}, tz=IST, stats=day_stats
        )
        aggregated.update(day_aggregated)
        new_event_columns |= day_columns
        docs_read += day_stats.get("docs", 0)
    logger.info(
        "Streamed %s Mongo doc(s), aggregated into %s shop-date row(s), discovered %s event column(s)",
        docs_read,
        len(aggregated),
        len(new_event_columns),
    )
    for shop_name, shop_summary in summarize_aggregated_counts(aggregated).items():
        logger.info(
            "[%s] Aggregated %s shop-date row(s), %s total event(s)",
            shop_name,
            shop_summary["rows"],
            shop_summary["total_events"],
        )
        for bucket_date, date_summary in shop_summary["dates"].items():
            logger.info(
                "[%s] Date %s -> total_events=%s, counts=%s",
                shop_name,
                bucket_date,
                date_summary["total_events"],
                date_summary["event_counts"],
            )

    with get_master_db_cursor(dictionary=True) as (cursor, connection):
        ensure_events_summary_table(cursor, connection)
        ensure_event_columns(cursor, connection, new_event_columns)
        all_columns = get_event_count_columns(get_existing_columns(cursor))

        # Delete and reinsert the full 2-day window so reruns reset absent counts to zero.
        delete_target_dates(cursor, connection, target_dates)
        inserted = insert_event_summary_rows(cursor, connection, aggregated, all_columns)
        for (bucket_date, shop_name), counts in sorted(aggregated.items()):
            non_zero_columns = sorted(
                column for column, value in counts.items() if int(value or 0) > 0
            )
            logger.info(
                "[%s] Upserted events_summary row for %s with %s non-zero event column(s): %s",
                shop_name,
                bucket_date.isoformat(),
                len(non_zero_columns),
                non_zero_columns,
            )

    logger.info(
        "Events summary worker complete: rebuilt %s date(s), inserted %s row(s)",
        len(target_dates),
        inserted,
    )


def _run_scheduled() -> None:
    """
    Long-running mode for the standalone container: two independent
    cadences on one scheduler.
      - The master events_summary table: daily at 00:03 IST (18:33 UTC), offset
        from the ETL's :00 tick and the other midnight jobs. It is NOT run at
        startup: it re-reads two full days of Mongo events, so a container
        restart (or a crash loop) must never trigger it. After an outage the
        next scheduled run rebuilds the same two-day window.
      - The incremental raw-ingest + CI/buy-now rollup job: every 15 minutes.
        Its first run is delayed by EVENTS_WORKER_STARTUP_DELAY_S after start
        (default 90 s) so that a restart does not make every pipeline start
        working at the same moment; it is watermarked, so a delay loses nothing.
    Set EVENTS_WORKER_SELF_RUN=false to instead run both once and exit
    (e.g. for a one-off/manual invocation) - see run_worker.
    """
    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        _run_master_events_summary,
        trigger="cron",
        hour=MASTER_CRON_UTC_HOUR,
        minute=MASTER_CRON_UTC_MINUTE,
        timezone="UTC",
        id="events_summary_worker_job",
        coalesce=True,
        max_instances=1,
        misfire_grace_time=3600,
    )

    # Same job id as the 15-minute cadence, so the delayed first run and the later runs can never
    # overlap (max_instances=1).
    scheduler.add_job(
        run_incremental_events_job,
        trigger="interval",
        minutes=15,
        next_run_time=datetime.now(timezone.utc) + timedelta(seconds=max(0, EVENTS_WORKER_STARTUP_DELAY_S)),
        id="incremental_events_job",
        coalesce=True,
        max_instances=1,
        misfire_grace_time=600,
    )
    logger.info(
        "Events summary worker scheduler started (master table: cron %02d:%02d UTC / 00:03 IST, not run "
        "at startup; incremental events job: every 15 minutes, first run in %ss)",
        MASTER_CRON_UTC_HOUR,
        MASTER_CRON_UTC_MINUTE,
        EVENTS_WORKER_STARTUP_DELAY_S,
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Events summary worker scheduler stopped")

if __name__ == "__main__":
    self_run = os.environ.get("EVENTS_WORKER_SELF_RUN", "true").strip().lower() == "true"
    if self_run:
        _run_scheduled()
    else:
        run_worker()
