# -*- coding: utf-8 -*-
"""
Standalone script to backfill `product_traffic_split` from `product_sessions_snapshot`.
Usage:
    python "backfill scripts/backfill_product_traffic_split.py" --start-date 2026-07-01 --end-date 2026-07-07
    python "backfill scripts/backfill_product_traffic_split.py" --start-date 2026-07-01 --end-date 2026-07-07 --brand-index 0
    python "backfill scripts/backfill_product_traffic_split.py"
"""

import argparse
import logging
import os
import sys
import traceback
from datetime import datetime, timedelta
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
        BACKFILL_BRAND_INDICES,
        BACKFILL_END_IST,
        BACKFILL_MODE,
        BACKFILL_START_IST,
        active_brand_indices,
        ensure_summary_tables,
        get_db_cursor,
        is_backfill_active_for,
        initialize_brand_configs,
        update_product_traffic_split_incremental,
    )
except ImportError:
    logger.error("Could not import from aws_background.py. Run this script in the repo root.")
    sys.exit(1)


DEFAULT_BATCH_DAYS = 7


def _parse_date(raw: str):
    return datetime.strptime(raw, "%Y-%m-%d").date()


def _iter_date_batches(start_date, end_date, batch_days: int):
    current = start_date
    span = max(1, int(batch_days))
    while current <= end_date:
        batch_end = min(current + timedelta(days=span - 1), end_date)
        yield current, batch_end
        current = batch_end + timedelta(days=1)


def _resolve_target_dates(cursor, start_date, end_date):
    cursor.execute(
        """
        SELECT DISTINCT `date`
        FROM product_sessions_snapshot
        WHERE `date` BETWEEN %s AND %s
        ORDER BY `date`
        """,
        (start_date.isoformat(), end_date.isoformat()),
    )

    target_dates = set()
    for row in cursor.fetchall():
        value = row.get("date") if isinstance(row, dict) else row[0]
        if value:
            target_dates.add(str(value))
    return target_dates


def run_backfill(start_date, end_date, batch_days=DEFAULT_BATCH_DAYS, target_brand_indices=None):
    initialize_brand_configs()
    if not active_brand_indices:
        logger.warning("No active brands found based on current configuration.")
        return

    indices_to_process = active_brand_indices
    if target_brand_indices:
        wanted = set(target_brand_indices)
        indices_to_process = [i for i in active_brand_indices if i in wanted]
    elif BACKFILL_MODE and BACKFILL_BRAND_INDICES:
        indices_to_process = [
            i for i in active_brand_indices if is_backfill_active_for(i)
        ]

    if not indices_to_process:
        logger.warning("No matching brands to process after filtering.")
        return

    processed_count = 0
    for brand_index in indices_to_process:
        brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")

        try:
            with get_db_cursor(brand_index) as (cursor, connection):
                ensure_summary_tables(cursor, connection)

                for batch_start, batch_end in _iter_date_batches(start_date, end_date, batch_days):
                    logger.info(
                        f"[{brand_name}] Backfilling product_traffic_split for {batch_start} -> {batch_end}"
                    )
                    target_dates = _resolve_target_dates(cursor, batch_start, batch_end)
                    update_product_traffic_split_incremental(
                        cursor,
                        connection,
                        brand_name,
                        target_dates,
                    )

                processed_count += 1
        except Exception as exc:
            logger.error(f"[{brand_name}] Failed to backfill product_traffic_split: {exc}")
            traceback.print_exc()

    logger.info(
        f"Product traffic split backfill complete. Processed {processed_count} brand(s)."
    )


def main():
    parser = argparse.ArgumentParser(description="Backfill product traffic split")
    parser.add_argument("--start-date", help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", help="End date in YYYY-MM-DD format")
    parser.add_argument("--brand-index", type=int, help="Optional brand index")
    parser.add_argument(
        "--batch-days",
        type=int,
        default=DEFAULT_BATCH_DAYS,
        help=f"Number of days to process per batch (default: {DEFAULT_BATCH_DAYS})",
    )
    args = parser.parse_args()

    if args.start_date or args.end_date:
        if not (args.start_date and args.end_date):
            raise ValueError("Both --start-date and --end-date are required together")
        start_date = _parse_date(args.start_date)
        end_date = _parse_date(args.end_date)
    else:
        if not BACKFILL_MODE:
            raise ValueError(
                "Provide --start-date/--end-date or enable BACKFILL_MODE with BACKFILL_START_IST and BACKFILL_END_IST"
            )

        if not BACKFILL_START_IST or not BACKFILL_END_IST:
            raise ValueError(
                "BACKFILL_START_IST and BACKFILL_END_IST must be set when using env-driven backfill"
            )
        start_date = BACKFILL_START_IST.date()
        end_date = BACKFILL_END_IST.date()

    target_brand_indices = None
    if args.brand_index is not None:
        target_brand_indices = [args.brand_index]
    elif BACKFILL_MODE and BACKFILL_BRAND_INDICES:
        target_brand_indices = sorted(BACKFILL_BRAND_INDICES)

    if start_date > end_date:
        raise ValueError("--start-date cannot be after --end-date")
    if args.batch_days < 1:
        raise ValueError("--batch-days must be at least 1")

    run_backfill(
        start_date,
        end_date,
        batch_days=args.batch_days,
        target_brand_indices=target_brand_indices,
    )


if __name__ == "__main__":
    main()
