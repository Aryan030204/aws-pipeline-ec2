"""
Backfill for the per-brand mongo_events_summary table (full raw alerts.sessions
events, one row per event - see pipeline/mongo_events_raw.py).

The daily events-summary-worker only keeps a rolling 2-day window; use this to
load older history for the selected window. Runs one day at a time so a long
history never holds a single giant Mongo cursor open. Safe to re-run: rows are
upserted by event_id.
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

try:
    import aws_background
except ImportError:
    import traceback

    traceback.print_exc()
    print("Error: Could not import aws_background.py. Run this script from the repo environment.")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [RAW-EVENTS-BACKFILL] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")


def _parse_ist_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


def backfill_brand(brand_index: int, start_dt: datetime, end_dt: datetime):
    brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
    brand_key = os.environ.get(f"BRAND_TAG_{brand_index}")

    if not aws_background.is_ci_events_brand(brand_name, brand_key):
        logger.info(f"Skipping {brand_name}: brand not enabled for raw Mongo events.")
        return

    day = start_dt.date()
    last_day = end_dt.date()
    while day <= last_day:
        logger.info(f"{brand_name}: backfilling raw events for {day}")
        aws_background.sync_mongo_events_raw_for_brand(
            brand_index=brand_index,
            brand_name=brand_name,
            brand_key=brand_key,
            start_date=day,
            end_date=day,
        )
        day += timedelta(days=1)


def main():
    parser = argparse.ArgumentParser(
        description="Backfill the per-brand mongo_events_summary table from alerts.sessions"
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
        logger.error("No active brands available for raw Mongo events backfill.")
        sys.exit(1)

    logger.info(
        f"Starting raw Mongo events backfill for {len(target_indices)} brand(s), "
        f"window {start_dt} -> {end_dt} (IST)"
    )

    for brand_index in target_indices:
        try:
            backfill_brand(brand_index, start_dt, end_dt)
        except Exception as e:
            logger.exception(f"Brand {brand_index} failed: {e}")

    logger.info("Raw Mongo events backfill completed.")


if __name__ == "__main__":
    main()
