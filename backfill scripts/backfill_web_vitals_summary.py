"""
Focused backfill for Mongo web vitals into per-brand web_vitals_summary tables.
"""

import argparse
import logging
import os
import sys
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

try:
    import aws_background
except ImportError:
    traceback.print_exc()
    print("Error: Could not import aws_background.py. Run this script from the repo environment.")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WEB-VITALS-BACKFILL] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")


def _parse_ist_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


def _ensure_ist_dt(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


def backfill_brand(brand_index: int, start_dt: datetime, end_dt: datetime):
    brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
    brand_key = os.environ.get(f"BRAND_TAG_{brand_index}")

    if not brand_key:
        logger.info(f"Skipping {brand_name}: missing BRAND_TAG_{brand_index}.")
        return

    with aws_background.get_db_cursor(brand_index) as (cursor, connection):
        aws_background.ensure_summary_tables(cursor, connection)
        inserted = aws_background.sync_web_vitals_summary(
            brand_index=brand_index,
            brand_name=brand_name,
            brand_key=brand_key,
            cursor=cursor,
            connection=connection,
            start_dt=start_dt,
            end_dt=end_dt,
            use_watermark=False,
        )
        logger.info(
            f"[{brand_name}] Web vitals backfill finished with {inserted} row(s) for {start_dt} -> {end_dt} (IST)"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Focused backfill for Mongo web vitals into web_vitals_summary"
    )
    parser.add_argument(
        "--start",
        type=str,
        help="Start datetime in IST (YYYY-MM-DDTHH:mm:ss). Defaults to BACKFILL_START_IST.",
    )
    parser.add_argument(
        "--end",
        type=str,
        help="End datetime in IST (YYYY-MM-DDTHH:mm:ss). Defaults to BACKFILL_END_IST.",
    )
    parser.add_argument(
        "--brand-index",
        type=int,
        help="Specific brand index to process. Defaults to BACKFILL_BRAND_INDICES or all active brands.",
    )
    args = parser.parse_args()

    logger.info("Initializing brand configurations and database pools...")
    aws_background.initialize_brand_configs()

    start_dt = _parse_ist_dt(args.start) if args.start else aws_background.BACKFILL_START_IST
    end_dt = _parse_ist_dt(args.end) if args.end else aws_background.BACKFILL_END_IST

    if not start_dt or not end_dt:
        logger.error(
            "Missing backfill window. Provide --start/--end or set BACKFILL_START_IST/BACKFILL_END_IST."
        )
        sys.exit(1)

    start_dt = _ensure_ist_dt(start_dt)
    end_dt = _ensure_ist_dt(end_dt)

    if start_dt > end_dt:
        logger.error(f"Invalid window: start {start_dt} is after end {end_dt}")
        sys.exit(1)

    if args.brand_index is not None:
        target_indices = [args.brand_index]
    elif aws_background.BACKFILL_BRAND_INDICES:
        target_indices = [
            idx
            for idx in aws_background.active_brand_indices
            if idx in aws_background.BACKFILL_BRAND_INDICES
        ]
    else:
        target_indices = aws_background.active_brand_indices

    if not target_indices:
        if aws_background.BACKFILL_BRAND_INDICES:
            logger.error(
                "No target brands available for web vitals backfill: "
                f"active brands={aws_background.active_brand_indices}, "
                f"BACKFILL_BRAND_INDICES={sorted(aws_background.BACKFILL_BRAND_INDICES)}. "
                "Pass --brand-index for a local run or update BACKFILL_BRAND_INDICES."
            )
        else:
            logger.error("No active brands available for web vitals backfill.")
        sys.exit(1)

    logger.info(
        f"Starting web vitals backfill for {len(target_indices)} brand(s), "
        f"window {start_dt} -> {end_dt} (IST)"
    )

    for brand_index in target_indices:
        try:
            backfill_brand(brand_index, start_dt, end_dt)
        except Exception as e:
            logger.error(f"❌ web vitals backfill failed for brand index {brand_index}: {e}")
            traceback.print_exc()


if __name__ == "__main__":
    main()
