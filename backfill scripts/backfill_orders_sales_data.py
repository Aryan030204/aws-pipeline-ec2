"""
Full orders + sales backfill: overwrites the raw order tables
(shopify_orders/shopify_orders_update) and every order/sales-driven rollup
built from them - sales_summary, order_summary, discount_summary,
gross_summary, hour_wise_sales, daily/hourly citywise summary (order
columns), dashboard discount tables, overall_summary (order/sales columns),
rto_summary, shopify_orders UTM daily, discount dashboard fact, returns
fact, products-sold-units, and bundle rollups - for a date window, across
all active brands or one specific brand.

Reuses process_single_brand() (pipeline/orchestration.py) exactly as the
live scheduled pipeline does, just with BACKFILL_MODE forced on for the
requested window - this script is a thin driver, not a second copy of the
order/summary logic, so "raw tables, rollups, or any other thing" about
orders/sales stays defined in one place.

Sessions/ATC sessions are deliberately NOT touched here (process_single_brand
already skips all four session syncs, overall and city-wise, whenever
BACKFILL_MODE is active - see pipeline/orchestration.py's
update_sessions_summary) - use backfill_sessions_data.py for that. DB schema
migrations are NOT run either (also already skipped under BACKFILL_MODE, by
the same design as sessions - both are considered live-pipeline-only steps
that don't belong in a backfill run).

Overwrite, not addon: pipeline/shopify_orders.py's load_data_to_sql_optimized
already DELETEs the raw order tables for the requested window before
re-inserting when BACKFILL_MODE is active (its
_replace_backfill_window_if_needed), and every downstream rollup this
triggers is upserted or DELETE+INSERT per date
(pipeline/summary_incremental.py's merge_staging_to_main_by_date), so
re-running this script for the same window is idempotent and always ends up
reflecting Shopify's current data for that window, not a merge with
whatever was there before.
"""

import argparse
import logging
import os
import sys

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
    format="%(asctime)s [ORDERS-SALES-BACKFILL] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Overwrite-backfill orders and every sales rollup for a date window "
            "(no sessions/ATC sessions, no schema migrations - see module docstring)"
        )
    )
    parser.add_argument(
        "--start",
        type=str,
        help="Start datetime (YYYY-MM-DD or YYYY-MM-DDTHH:mm:ss), interpreted in each "
        "brand's own store timezone. Defaults to BACKFILL_START_IST.",
    )
    parser.add_argument(
        "--end",
        type=str,
        help="End datetime, same format. Defaults to BACKFILL_END_IST.",
    )
    parser.add_argument(
        "--brand-index",
        type=int,
        help="Specific brand index to process. Defaults to BACKFILL_BRAND_INDICES or all active brands.",
    )
    args = parser.parse_args()

    start_dt = aws_background._parse_backfill_dt(args.start) if args.start else aws_background.BACKFILL_START_IST
    end_dt = aws_background._parse_backfill_dt(args.end) if args.end else aws_background.BACKFILL_END_IST

    if not start_dt or not end_dt:
        logger.error(
            "Missing backfill window. Provide --start/--end or set BACKFILL_START_IST/BACKFILL_END_IST."
        )
        sys.exit(1)
    if start_dt >= end_dt:
        logger.error(f"Invalid window: start {start_dt} is not before end {end_dt}")
        sys.exit(1)

    # Force backfill mode on for this run regardless of the live .env value -
    # this script's entire purpose is a backfill. Every function it touches
    # via process_single_brand (load_data_to_sql_optimized,
    # execute_summary_queries_incremental, ...) reads these same
    # aws_background globals rather than taking a window as a parameter.
    aws_background.BACKFILL_MODE = True
    aws_background.BACKFILL_START_IST = start_dt
    aws_background.BACKFILL_END_IST = end_dt
    if args.brand_index is not None:
        aws_background.BACKFILL_BRAND_INDICES = {args.brand_index}

    logger.info("Initializing brand configurations and database pools...")
    aws_background.initialize_brand_configs()

    if args.brand_index is not None:
        target_indices = [args.brand_index]
    elif aws_background.BACKFILL_BRAND_INDICES:
        target_indices = [
            idx
            for idx in aws_background.active_brand_indices
            if idx in aws_background.BACKFILL_BRAND_INDICES
        ]
    else:
        target_indices = list(aws_background.active_brand_indices)

    if not target_indices:
        logger.error("No active brands available for orders/sales backfill.")
        sys.exit(1)

    logger.info(
        f"Starting orders/sales backfill for {len(target_indices)} brand(s), "
        f"window {start_dt} -> {end_dt} (each brand's own store timezone)"
    )

    for brand_index in target_indices:
        brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
        logger.info(f"--- {brand_name} (index {brand_index}) ---")
        try:
            aws_background.process_single_brand(brand_index)
        except Exception as e:
            logger.exception(f"Brand {brand_index} ({brand_name}) failed: {e}")

    logger.info("Orders/sales backfill completed.")


if __name__ == "__main__":
    main()
