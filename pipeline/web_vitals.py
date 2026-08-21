"""
Web vitals summary sync: pulls page-performance test results from the
'pagespeed_brands.test_results' MongoDB collection, aggregates them into
daily (date, url) buckets, and upserts daily_web_vitals_summary /
web_vitals_summary. Watermarked via the generic pipeline_metadata table.
"""

import hashlib
import os
import re
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pymongo import MongoClient

from pipeline.state import IST, logger
from pipeline.db import (
    get_db_cursor,
    executemany_chunked,
    ensure_pipeline_metadata_table,
    get_pipeline_metadata_timestamp,
    update_pipeline_metadata_timestamp,
)
from pipeline.schema_migrations import ensure_summary_tables

SPEED_MONGO_URI = os.environ.get("SPEED_MONGO_URI", "").strip()

WEB_VITALS_METADATA_KEY = "last_web_vitals_created_at"


def _resolve_speed_mongo_test_results_collection():
    if not SPEED_MONGO_URI:
        raise ValueError("SPEED_MONGO_URI is not configured")

    client = MongoClient(SPEED_MONGO_URI, tz_aware=True)
    return client, client["pagespeed_brands"]["test_results"]


def _parse_iso_datetime_string(
    value: Any,
    field_name: str,
    brand_name: str,
    mongo_object_id: str,
    required: bool = False,
) -> Optional[datetime]:
    if value in (None, ""):
        if required:
            logger.warning(
                f"⚠️ [{brand_name}] Skipping web vitals row {mongo_object_id}: missing {field_name}"
            )
        return None

    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).strip())
        except ValueError:
            logger.warning(
                f"⚠️ [{brand_name}] Skipping web vitals row {mongo_object_id}: invalid {field_name}='{value}'"
            )
            return None

    return parsed.replace(tzinfo=IST) if parsed.tzinfo is None else parsed.astimezone(IST)


def _parse_optional_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_optional_decimal(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_optional_date(value: Any) -> Optional[date]:
    if value in (None, ""):
        return None
    try:
        return datetime.fromisoformat(str(value).strip()).date()
    except ValueError:
        try:
            return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
        except ValueError:
            return None


def _parse_optional_time(value: Any) -> Optional[datetime.time]:
    if value in (None, ""):
        return None
    try:
        return datetime.strptime(str(value).strip(), "%H:%M:%S").time()
    except ValueError:
        return None


def _coerce_web_vitals_row(
    doc: Dict[str, Any],
    brand_name: str,
) -> Optional[Dict[str, Any]]:
    mongo_object_id = str(doc.get("_id", "")).strip()
    if not mongo_object_id:
        logger.warning(f"⚠️ [{brand_name}] Skipping web vitals row with missing Mongo _id")
        return None

    created_at = _parse_iso_datetime_string(
        doc.get("created_at"),
        "created_at",
        brand_name,
        mongo_object_id,
        required=True,
    )
    if created_at is None:
        return None

    updated_at = _parse_iso_datetime_string(
        doc.get("updated_at"),
        "updated_at",
        brand_name,
        mongo_object_id,
        required=False,
    )

    brand_key = str(doc.get("brand_key", "")).strip()
    if not brand_key:
        logger.warning(
            f"⚠️ [{brand_name}] Skipping web vitals row {mongo_object_id}: missing brand_key"
        )
        return None

    url = str(doc.get("url", "")).strip()
    if not url:
        logger.warning(
            f"⚠️ [{brand_name}] Skipping web vitals row {mongo_object_id}: missing url"
        )
        return None

    return {
        "mongo_object_id": mongo_object_id,
        "brand_name": str(doc.get("brand_name", "")).strip() or brand_name,
        "brand_key": brand_key,
        "page_name": str(doc.get("page_name", "")).strip() or None,
        "url": url,
        "rank": _parse_optional_int(doc.get("rank")),
        "sessions": _parse_optional_int(doc.get("sessions")),
        "date": _parse_optional_date(doc.get("date")),
        "time": _parse_optional_time(doc.get("time")),
        "performance": _parse_optional_int(doc.get("performance")),
        "lcp": _parse_optional_decimal(doc.get("lcp")),
        "inp": _parse_optional_decimal(doc.get("inp")),
        "cls": _parse_optional_decimal(doc.get("cls")),
        "fcp": _parse_optional_decimal(doc.get("fcp")),
        "ttfb": _parse_optional_decimal(doc.get("ttfb")),
        "created_at": created_at.replace(tzinfo=None),
        "updated_at": updated_at.replace(tzinfo=None) if updated_at else None,
    }


def _web_vitals_url_hash(url: str) -> str:
    return hashlib.sha256((url or "").encode("utf-8")).hexdigest()


def _daily_web_vitals_key_from_row(row: Dict[str, Any]) -> Optional[Tuple[date, str]]:
    row_date = row.get("date")
    row_url = (row.get("url") or "").strip()
    if not row_date or not row_url:
        return None
    return row_date, row_url


def _normalize_mysql_date_value(value: Any) -> Optional[date]:
    if value in (None, ""):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return datetime.fromisoformat(str(value)).date()
    except ValueError:
        try:
            return datetime.strptime(str(value), "%Y-%m-%d").date()
        except ValueError:
            return None


def _aggregate_daily_web_vitals_rows(
    raw_rows: List[Dict[str, Any]],
    brand_name: str,
) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[date, str], Dict[str, Any]] = {}
    metric_fields = ("performance", "fcp", "lcp", "inp", "ttfb", "cls")

    for row in raw_rows:
        row_date = _normalize_mysql_date_value(row.get("date"))
        row_url = (row.get("url") or "").strip()
        if not row_date or not row_url:
            continue

        key = (row_date, row_url)
        created_at = row.get("created_at")
        if isinstance(created_at, str):
            try:
                created_at = datetime.fromisoformat(created_at)
            except ValueError:
                created_at = None

        entry = grouped.get(key)
        if entry is None:
            entry = {
                "date": row_date,
                "url": row_url,
                "url_hash": _web_vitals_url_hash(row_url),
                "brand_name": (row.get("brand_name") or brand_name),
                "brand_key": (row.get("brand_key") or "").strip(),
                "page_name": (row.get("page_name") or "").strip() or None,
                "sample_count": 0,
                "_metric_sums": {field: 0.0 for field in metric_fields},
                "_metric_counts": {field: 0 for field in metric_fields},
                "_latest_page_name_at": created_at,
            }
            grouped[key] = entry

        entry["sample_count"] += 1

        page_name = (row.get("page_name") or "").strip() or None
        latest_page_name_at = entry.get("_latest_page_name_at")
        if page_name and (
            latest_page_name_at is None
            or (created_at is not None and created_at >= latest_page_name_at)
        ):
            entry["page_name"] = page_name
            entry["_latest_page_name_at"] = created_at

        if row.get("brand_name"):
            entry["brand_name"] = row.get("brand_name")
        if row.get("brand_key"):
            entry["brand_key"] = str(row.get("brand_key")).strip()

        for field in metric_fields:
            value = row.get(field)
            if value is None:
                continue
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                continue
            entry["_metric_sums"][field] += numeric
            entry["_metric_counts"][field] += 1

    aggregate_rows: List[Dict[str, Any]] = []
    for entry in grouped.values():
        aggregate_rows.append(
            {
                "date": entry["date"],
                "url_hash": entry["url_hash"],
                "url": entry["url"],
                "page_name": entry["page_name"],
                "brand_name": entry["brand_name"],
                "brand_key": entry["brand_key"],
                "avg_performance": (
                    entry["_metric_sums"]["performance"] / entry["_metric_counts"]["performance"]
                    if entry["_metric_counts"]["performance"]
                    else None
                ),
                "avg_fcp": (
                    entry["_metric_sums"]["fcp"] / entry["_metric_counts"]["fcp"]
                    if entry["_metric_counts"]["fcp"]
                    else None
                ),
                "avg_lcp": (
                    entry["_metric_sums"]["lcp"] / entry["_metric_counts"]["lcp"]
                    if entry["_metric_counts"]["lcp"]
                    else None
                ),
                "avg_inp": (
                    entry["_metric_sums"]["inp"] / entry["_metric_counts"]["inp"]
                    if entry["_metric_counts"]["inp"]
                    else None
                ),
                "avg_ttfb": (
                    entry["_metric_sums"]["ttfb"] / entry["_metric_counts"]["ttfb"]
                    if entry["_metric_counts"]["ttfb"]
                    else None
                ),
                "avg_cls": (
                    entry["_metric_sums"]["cls"] / entry["_metric_counts"]["cls"]
                    if entry["_metric_counts"]["cls"]
                    else None
                ),
                "sample_count": entry["sample_count"],
            }
        )
    return aggregate_rows


def _fetch_raw_web_vitals_rows_for_keys(
    cursor,
    affected_keys: List[Tuple[date, str]],
) -> List[Dict[str, Any]]:
    if not affected_keys:
        return []

    dates = sorted({key_date for key_date, _ in affected_keys})
    urls = sorted({key_url for _, key_url in affected_keys})
    date_placeholders = ",".join(["%s"] * len(dates))
    url_placeholders = ",".join(["%s"] * len(urls))
    sql = f"""
        SELECT
            brand_name,
            brand_key,
            page_name,
            url,
            `date`,
            performance,
            fcp,
            lcp,
            inp,
            ttfb,
            cls,
            created_at
        FROM web_vitals_summary
        WHERE `date` IN ({date_placeholders})
          AND url IN ({url_placeholders})
    """
    cursor.execute(sql, tuple(dates + urls))
    rows = cursor.fetchall() or []

    affected_set = set(affected_keys)
    normalized_rows = []
    for row in rows:
        row_date = _normalize_mysql_date_value(
            row["date"] if isinstance(row, dict) else row[4]
        )
        row_url = (row["url"] if isinstance(row, dict) else row[3]) or ""
        if row_date and (row_date, row_url) in affected_set:
            normalized_rows.append(row if isinstance(row, dict) else {})
    return normalized_rows


def _fetch_raw_web_vitals_rows_for_window(
    cursor,
    start_date: date,
    end_date: date,
) -> List[Dict[str, Any]]:
    cursor.execute(
        """
        SELECT
            brand_name,
            brand_key,
            page_name,
            url,
            `date`,
            performance,
            fcp,
            lcp,
            inp,
            ttfb,
            cls,
            created_at
        FROM web_vitals_summary
        WHERE `date` BETWEEN %s AND %s
          AND `date` IS NOT NULL
          AND url IS NOT NULL
        """,
        (start_date, end_date),
    )
    return cursor.fetchall() or []


def _upsert_daily_web_vitals_summary_rows(
    rows: List[Dict[str, Any]],
    cursor,
    connection,
    brand_name: str,
) -> int:
    if not rows:
        return 0

    sql = """
        INSERT INTO daily_web_vitals_summary (
            `date`,
            url_hash,
            url,
            page_name,
            brand_name,
            brand_key,
            avg_performance,
            avg_fcp,
            avg_lcp,
            avg_inp,
            avg_ttfb,
            avg_cls,
            sample_count
        ) VALUES (
            %(date)s,
            %(url_hash)s,
            %(url)s,
            %(page_name)s,
            %(brand_name)s,
            %(brand_key)s,
            %(avg_performance)s,
            %(avg_fcp)s,
            %(avg_lcp)s,
            %(avg_inp)s,
            %(avg_ttfb)s,
            %(avg_cls)s,
            %(sample_count)s
        )
        ON DUPLICATE KEY UPDATE
            url = VALUES(url),
            page_name = VALUES(page_name),
            brand_name = VALUES(brand_name),
            brand_key = VALUES(brand_key),
            avg_performance = VALUES(avg_performance),
            avg_fcp = VALUES(avg_fcp),
            avg_lcp = VALUES(avg_lcp),
            avg_inp = VALUES(avg_inp),
            avg_ttfb = VALUES(avg_ttfb),
            avg_cls = VALUES(avg_cls),
            sample_count = VALUES(sample_count)
    """
    inserted = executemany_chunked(cursor, connection, sql, rows)
    logger.info(f"✅ Daily web vitals summary updated for {brand_name} ({inserted} rows)")
    return inserted


def rebuild_daily_web_vitals_summary_for_keys(
    cursor,
    connection,
    brand_name: str,
    affected_keys: Iterable[Tuple[date, str]],
) -> int:
    normalized_keys = sorted({key for key in affected_keys if key and key[0] and key[1]})
    if not normalized_keys:
        return 0

    delete_rows = [
        (key_date, _web_vitals_url_hash(key_url))
        for key_date, key_url in normalized_keys
    ]
    executemany_chunked(
        cursor,
        connection,
        "DELETE FROM daily_web_vitals_summary WHERE `date` = %s AND url_hash = %s",
        delete_rows,
    )

    raw_rows = _fetch_raw_web_vitals_rows_for_keys(cursor, normalized_keys)
    aggregate_rows = _aggregate_daily_web_vitals_rows(raw_rows, brand_name)
    return _upsert_daily_web_vitals_summary_rows(
        aggregate_rows, cursor, connection, brand_name
    )


def rebuild_daily_web_vitals_summary_for_window(
    brand_index: int,
    brand_name: str,
    start_date: date,
    end_date: date,
    cursor=None,
    connection=None,
) -> int:
    managed_cursor = cursor is None or connection is None
    if managed_cursor:
        with get_db_cursor(brand_index) as (inner_cursor, inner_connection):
            return rebuild_daily_web_vitals_summary_for_window(
                brand_index=brand_index,
                brand_name=brand_name,
                start_date=start_date,
                end_date=end_date,
                cursor=inner_cursor,
                connection=inner_connection,
            )

    ensure_summary_tables(cursor, connection)
    cursor.execute(
        "DELETE FROM daily_web_vitals_summary WHERE `date` BETWEEN %s AND %s",
        (start_date, end_date),
    )
    connection.commit()

    raw_rows = _fetch_raw_web_vitals_rows_for_window(cursor, start_date, end_date)
    aggregate_rows = _aggregate_daily_web_vitals_rows(raw_rows, brand_name)
    return _upsert_daily_web_vitals_summary_rows(
        aggregate_rows, cursor, connection, brand_name
    )


def get_last_web_vitals_created_at(cursor) -> Optional[datetime]:
    ensure_pipeline_metadata_table(cursor, None)
    return get_pipeline_metadata_timestamp(cursor, WEB_VITALS_METADATA_KEY)


def update_last_web_vitals_created_at(cursor, connection, new_timestamp: datetime):
    update_pipeline_metadata_timestamp(
        cursor, connection, WEB_VITALS_METADATA_KEY, new_timestamp.replace(tzinfo=None)
    )


def fetch_web_vitals_documents_from_mongo(
    brand_key: str,
    brand_name: str,
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
    created_after: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    client = None
    docs: List[Dict[str, Any]] = []
    normalized_brand_key = (brand_key or "").strip().lower()
    query: Dict[str, Any] = {
        "brand_key": {
            "$regex": f"^{re.escape(normalized_brand_key)}$",
            "$options": "i",
        },
        "created_at": {"$exists": True, "$ne": None},
    }
    if created_after is not None:
        query["created_at"]["$gt"] = created_after.isoformat()

    try:
        client, collection = _resolve_speed_mongo_test_results_collection()
        cursor = collection.find(query).sort("created_at", 1)
        for doc in cursor:
            row = _coerce_web_vitals_row(doc, brand_name)
            if row is None:
                continue

            created_at_ist = row["created_at"].replace(tzinfo=IST)
            if start_dt and created_at_ist < start_dt:
                continue
            if end_dt and created_at_ist > end_dt:
                continue
            docs.append(row)
        return docs
    finally:
        if client is not None:
            client.close()


def _do_upsert_web_vitals_summary(
    rows: List[Dict[str, Any]],
    cursor,
    connection,
    brand_name: str,
) -> Tuple[int, Optional[datetime]]:
    if not rows:
        return 0, None

    sql = """
        INSERT INTO web_vitals_summary (
            mongo_object_id,
            brand_name,
            brand_key,
            page_name,
            url,
            `rank`,
            sessions,
            `date`,
            `time`,
            performance,
            lcp,
            inp,
            cls,
            fcp,
            ttfb,
            created_at,
            updated_at
        ) VALUES (
            %(mongo_object_id)s,
            %(brand_name)s,
            %(brand_key)s,
            %(page_name)s,
            %(url)s,
            %(rank)s,
            %(sessions)s,
            %(date)s,
            %(time)s,
            %(performance)s,
            %(lcp)s,
            %(inp)s,
            %(cls)s,
            %(fcp)s,
            %(ttfb)s,
            %(created_at)s,
            %(updated_at)s
        )
        ON DUPLICATE KEY UPDATE
            mongo_object_id = VALUES(mongo_object_id),
            updated_at = VALUES(updated_at)
    """
    executemany_chunked(cursor, connection, sql, rows)
    max_created_at = max(row["created_at"] for row in rows)
    logger.info(
        f"✅ Web vitals summary updated for {brand_name} ({len(rows)} rows)"
    )
    return len(rows), max_created_at.replace(tzinfo=IST)


def sync_web_vitals_summary(
    brand_index: int,
    brand_name: str,
    brand_key: Optional[str] = None,
    cursor=None,
    connection=None,
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
    use_watermark: bool = True,
) -> int:
    effective_brand_key = (brand_key or "").strip()
    if not effective_brand_key:
        logger.info(f"Skipping web vitals sync for {brand_name}: missing brand_key")
        return 0

    managed_cursor = cursor is None or connection is None
    if managed_cursor:
        with get_db_cursor(brand_index) as (inner_cursor, inner_connection):
            return sync_web_vitals_summary(
                brand_index=brand_index,
                brand_name=brand_name,
                brand_key=effective_brand_key,
                cursor=inner_cursor,
                connection=inner_connection,
                start_dt=start_dt,
                end_dt=end_dt,
                use_watermark=use_watermark,
            )

    ensure_pipeline_metadata_table(cursor, connection)
    ensure_summary_tables(cursor, connection)

    created_after = None
    if use_watermark and start_dt is None and end_dt is None:
        created_after = get_last_web_vitals_created_at(cursor)

    try:
        rows = fetch_web_vitals_documents_from_mongo(
            brand_key=effective_brand_key,
            brand_name=brand_name,
            start_dt=start_dt,
            end_dt=end_dt,
            created_after=created_after,
        )
    except Exception as e:
        logger.error(f"❌ Error fetching web vitals Mongo data for {brand_name}: {e}")
        return 0

    row_count, max_created_at = _do_upsert_web_vitals_summary(
        rows, cursor, connection, brand_name
    )

    affected_daily_keys = []
    if rows:
        affected_daily_keys = [
            key for key in (_daily_web_vitals_key_from_row(row) for row in rows) if key
        ]
        rebuild_daily_web_vitals_summary_for_keys(
            cursor, connection, brand_name, affected_daily_keys
        )

    if max_created_at and use_watermark:
        update_last_web_vitals_created_at(cursor, connection, max_created_at)

    if row_count == 0:
        logger.info(f"✔️ No new web vitals rows for {brand_name}")
    return row_count
