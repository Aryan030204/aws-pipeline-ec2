# -*- coding: utf-8 -*-
"""
Standalone script to backfill 'rto_summary' from existing shopify_orders rows.
Usage: python "backfill scripts/backfill_rto_summary.py"
"""

import os
import sys
import logging
import traceback
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

try:
    from aws_background import (
        active_brand_indices,
        ensure_summary_tables,
        get_db_cursor,
        initialize_brand_configs,
        update_rto_summary_incremental,
    )
except ImportError:
    logger.error("Could not import from aws_background.py. Run this script in the repo root.")
    sys.exit(1)


def _parse_env_date(raw: str):
    if not raw:
        return None

    date_str = raw.split(" ")[0].split("T")[0]
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def _fetch_target_dates(cursor, start_date=None, end_date=None):
    where_clauses = []
    params = []

    if start_date:
        where_clauses.append("created_date >= %s")
        params.append(start_date.isoformat())
    if end_date:
        where_clauses.append("created_date <= %s")
        params.append(end_date.isoformat())

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    cursor.execute(
        f"""
        SELECT DISTINCT created_date
        FROM shopify_orders
        {where_sql}
        ORDER BY created_date
        """,
        tuple(params),
    )

    dates = set()
    for row in cursor.fetchall():
        value = row.get("created_date") if isinstance(row, dict) else row[0]
        if value:
            dates.add(str(value))
    return dates


def run_backfill():
    backfill_mode = os.getenv("BACKFILL_MODE", "false").strip().lower() == "true"
    if not backfill_mode:
        logger.error("BACKFILL_MODE is false. Exiting.")
        return

    target_indices_str = os.getenv("BACKFILL_BRAND_INDICES", "")
    target_indices = []
    if target_indices_str:
        target_indices = [
            int(idx.strip()) for idx in target_indices_str.split(",") if idx.strip()
        ]
        logger.info(f"Targeting specific brands: {target_indices}")

    start_date = end_date = None
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

        try:
            with get_db_cursor(brand_index, dictionary=False) as (cursor, connection):
                ensure_summary_tables(cursor, connection)
                target_dates = _fetch_target_dates(cursor, start_date=start_date, end_date=end_date)
                if not target_dates:
                    logger.info(f"[{brand_name}] No shopify_orders dates found for requested window.")
                    continue

                logger.info(
                    f"[{brand_name}] Backfilling rto_summary for {len(target_dates)} date(s)"
                )
                update_rto_summary_incremental(
                    cursor,
                    connection,
                    brand_index,
                    brand_name,
                    target_dates,
                )
                processed_count += 1
        except Exception as exc:
            logger.error(f"[{brand_name}] Failed to backfill rto_summary: {exc}")
            traceback.print_exc()

    logger.info(
        f"RTO summary backfill complete. Processed {processed_count} brand(s)."
    )


if __name__ == "__main__":
    run_backfill()
