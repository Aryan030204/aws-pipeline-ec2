# -*- coding: utf-8 -*-
"""
Populate returns_fact.order_created_date for historical cancellation rows in
bounded batches, then rebuild only the affected created-date summaries.

Usage:
    python "backfill scripts/backfill_returns_fact_order_created_date.py"
"""

import logging
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

try:
    from aws_background import (
        _ensure_returns_fact,
        active_brand_indices,
        ensure_summary_tables,
        execute_summary_queries_incremental,
        executemany_chunked,
        get_db_cursor,
        initialize_brand_configs,
    )
except ImportError:
    logger.error("Could not import from aws_background.py. Run this script in the repo root.")
    sys.exit(1)


def _parse_env_date(raw: str) -> Optional[datetime.date]:
    if not raw:
        return None
    date_str = raw.split(" ")[0].split("T")[0]
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def _fetch_batch_order_ids(
    cursor,
    batch_size: int,
    last_order_id: int = 0,
    start_date=None,
    end_date=None,
) -> List[int]:
    where_clauses = [
        "event_type IN ('CANCEL', 'CANCEL (RTO)')",
        "order_created_date IS NULL",
        "order_id > %s",
    ]
    params: List[object] = [last_order_id]

    if start_date:
        where_clauses.append("event_date >= %s")
        params.append(start_date)
    if end_date:
        where_clauses.append("event_date <= %s")
        params.append(end_date)

    params.append(batch_size)
    cursor.execute(
        f"""
        SELECT DISTINCT order_id
        FROM returns_fact
        WHERE {' AND '.join(where_clauses)}
        ORDER BY order_id
        LIMIT %s
        """,
        tuple(params),
    )
    rows = cursor.fetchall()
    return [int(row[0] if not isinstance(row, dict) else row["order_id"]) for row in rows]


def _fetch_created_dates_for_orders(cursor, order_ids: List[int]) -> Dict[int, str]:
    if not order_ids:
        return {}
    placeholders = ", ".join(["%s"] * len(order_ids))
    cursor.execute(
        f"""
        SELECT CAST(order_id AS UNSIGNED) AS order_id, MAX(created_date) AS created_date
        FROM shopify_orders
        WHERE CAST(order_id AS UNSIGNED) IN ({placeholders})
        GROUP BY CAST(order_id AS UNSIGNED)
        """,
        tuple(order_ids),
    )
    mapping: Dict[int, str] = {}
    for row in cursor.fetchall():
        order_id = int(row[0] if not isinstance(row, dict) else row["order_id"])
        created_date = row[1] if not isinstance(row, dict) else row["created_date"]
        if created_date:
            mapping[order_id] = str(created_date)
    return mapping


def _update_returns_fact_created_dates(
    cursor,
    connection,
    mapping: Dict[int, str],
) -> Set[str]:
    if not mapping:
        return set()

    rows: List[Tuple[str, int]] = [
        (created_date, order_id) for order_id, created_date in mapping.items()
    ]
    executemany_chunked(
        cursor,
        connection,
        """
        UPDATE returns_fact
        SET order_created_date = %s
        WHERE order_id = %s
          AND event_type IN ('CANCEL', 'CANCEL (RTO)')
          AND order_created_date IS NULL
        """,
        rows,
    )
    return {created_date for created_date, _ in rows}


def run_backfill():
    backfill_mode = os.getenv("BACKFILL_MODE", "false").strip().lower() == "true"
    if not backfill_mode:
        logger.error("BACKFILL_MODE is false. Exiting.")
        return

    batch_size = int(os.getenv("RETURNS_FACT_CREATED_DATE_BACKFILL_BATCH_SIZE", "500"))
    if batch_size <= 0:
        logger.error("RETURNS_FACT_CREATED_DATE_BACKFILL_BATCH_SIZE must be positive.")
        return

    target_indices_str = os.getenv("BACKFILL_BRAND_INDICES", "").strip()
    target_indices = []
    if target_indices_str:
        target_indices = [
            int(idx.strip()) for idx in target_indices_str.split(",") if idx.strip()
        ]
        logger.info(f"Targeting specific brands: {target_indices}")

    try:
        start_date = _parse_env_date(os.getenv("BACKFILL_START_IST", ""))
        end_date = _parse_env_date(os.getenv("BACKFILL_END_IST", ""))
    except ValueError as exc:
        logger.error(f"Invalid BACKFILL_* date format: {exc}")
        return

    initialize_brand_configs()
    if not active_brand_indices:
        logger.warning("No active brands found based on current configuration.")
        return

    indices_to_process = active_brand_indices
    if target_indices:
        indices_to_process = [i for i in active_brand_indices if i in target_indices]

    if not indices_to_process:
        logger.warning("No matching brands to process after filtering.")
        return

    processed_count = 0
    for brand_index in indices_to_process:
        brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
        brand_key = os.environ.get(f"BRAND_KEY_{brand_index}", "")
        last_order_id = 0
        total_updated_orders = 0

        try:
            with get_db_cursor(brand_index, dictionary=False) as (cursor, connection):
                ensure_summary_tables(cursor, connection)
                _ensure_returns_fact(cursor, connection)

                while True:
                    batch_order_ids = _fetch_batch_order_ids(
                        cursor,
                        batch_size=batch_size,
                        last_order_id=last_order_id,
                        start_date=start_date,
                        end_date=end_date,
                    )
                    if not batch_order_ids:
                        break

                    last_order_id = batch_order_ids[-1]
                    created_dates_by_order = _fetch_created_dates_for_orders(
                        cursor, batch_order_ids
                    )
                    affected_dates = _update_returns_fact_created_dates(
                        cursor, connection, created_dates_by_order
                    )
                    total_updated_orders += len(created_dates_by_order)

                    logger.info(
                        f"[{brand_name}] Updated order_created_date for "
                        f"{len(created_dates_by_order)} order(s) in batch ending at order_id {last_order_id}"
                    )

                    if affected_dates:
                        execute_summary_queries_incremental(
                            brand_index,
                            brand_name,
                            brand_key,
                            affected_dates,
                            rto_target_dates=affected_dates,
                        )

                processed_count += 1
                logger.info(
                    f"[{brand_name}] Historical returns_fact created-date backfill complete. "
                    f"Updated {total_updated_orders} order(s)."
                )
        except Exception as exc:
            logger.error(
                f"[{brand_name}] Failed returns_fact created-date backfill: {exc}"
            )
            traceback.print_exc()

    logger.info(
        f"returns_fact order_created_date backfill complete. Processed {processed_count} brand(s)."
    )


if __name__ == "__main__":
    run_backfill()
