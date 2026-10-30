"""
Focused backfill for cod/prepaid/ppcod/unmapped order-count and sales-amount
columns (unmapped = orders with no payment gateway name).

Updates only:
- hour_wise_sales.number_of_cod_orders, number_of_prepaid_orders,
  number_of_ppcod_orders, cod_sales, prepaid_sales, partially_prepaid_sales,
  number_of_unmapped_orders, unmapped_sales
- overall_summary.cod_orders, prepaid_orders, partially_paid_orders,
  cod_sales, prepaid_sales, partially_prepaid_sales,
  unmapped_orders, unmapped_sales

No other column on either table is touched.
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
    format="%(asctime)s [PAYMENT-BREAKDOWN-BACKFILL] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")


def _parse_ist_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


def _dates_between(start_dt: datetime, end_dt: datetime):
    current = start_dt.date()
    last = end_dt.date()
    while current <= last:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def backfill_brand(brand_index: int, start_dt: datetime, end_dt: datetime):
    brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")

    aws_background.ensure_hour_wise_ppcod_column(brand_index)
    aws_background.ensure_hour_wise_payment_sales_columns(brand_index)
    aws_background.ensure_overall_summary_payment_sales_columns(brand_index)
    aws_background.ensure_payment_unmapped_columns(brand_index)

    target_dates = list(_dates_between(start_dt, end_dt))
    aws_background.backfill_payment_mode_breakdown_columns(
        brand_index=brand_index,
        brand_name=brand_name,
        target_dates=target_dates,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Focused backfill for hour_wise_sales/overall_summary cod-prepaid-ppcod columns"
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
        logger.error("No active brands available for payment mode breakdown backfill.")
        sys.exit(1)

    logger.info(
        f"Starting payment mode breakdown backfill for {len(target_indices)} brand(s), "
        f"window {start_dt} -> {end_dt} (IST)"
    )

    for brand_index in target_indices:
        try:
            backfill_brand(brand_index, start_dt, end_dt)
        except Exception as e:
            logger.exception(f"Brand {brand_index} failed: {e}")

    logger.info("Payment mode breakdown backfill completed.")


if __name__ == "__main__":
    main()
