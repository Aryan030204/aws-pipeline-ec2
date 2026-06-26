"""
Focused backfill for hourly checkout-initiated event counts.

Updates only hourly_sessions_summary_shopify.ci_events for the selected window.
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

try:
    import aws_background
except ImportError:
    print("Error: Could not import aws_background.py. Run this script from the repo environment.")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CI-BACKFILL] %(levelname)s %(message)s",
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
        logger.info(
            f"Skipping {brand_name}: brand not enabled for ci_events backfill."
        )
        return

    aws_background.ensure_ci_events_column(brand_index, "hourly_sessions_summary_shopify")
    aws_background.sync_checkout_initiated_ci_events(
        brand_index=brand_index,
        brand_name=brand_name,
        brand_key=brand_key,
        start_date=start_dt.date(),
        end_date=end_dt.date(),
    )


def main():
    parser = argparse.ArgumentParser(
        description="Focused backfill for hourly checkout-initiated ci_events"
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
        logger.error("No active brands available for ci_events backfill.")
        sys.exit(1)

    logger.info(
        f"Starting ci_events backfill for {len(target_indices)} brand(s), "
        f"window {start_dt} -> {end_dt} (IST)"
    )

    for brand_index in target_indices:
        try:
            backfill_brand(brand_index, start_dt, end_dt)
        except Exception as e:
            logger.error(f"❌ ci_events backfill failed for brand index {brand_index}: {e}")


if __name__ == "__main__":
    main()
