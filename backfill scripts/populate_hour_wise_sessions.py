# -*- coding: utf-8 -*-
"""
Populate Hour-Wise Sales Sessions from hourly_sessions_summary_shopify

Reads session data from `hourly_sessions_summary_shopify` and overwrites
the `number_of_sessions` and `number_of_atc_sessions` columns in
`hour_wise_sales` for the specified date range.

Strategy:
  - For each (date, hour) in the source table, UPDATE the matching row
    in hour_wise_sales. If no matching row exists, INSERT a new row with
    order/sales columns defaulting to 0.
  - This overwrites (not sums) session values.

Env variables respected:
  - BACKFILL_MODE       (true/false)
  - BACKFILL_START_IST  (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)
  - BACKFILL_END_IST    (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)
  - BACKFILL_BRAND_INDICES (comma-separated brand indices)
  - TEST_MODE           (true/false – disables DB SSL)

Usage:
    python populate_hour_wise_sessions.py
    python populate_hour_wise_sessions.py --brand-index 0
    python populate_hour_wise_sessions.py --start 2025-01-01 --end 2025-01-31
    python populate_hour_wise_sessions.py --start 2025-01-01 --end 2025-01-31 --brand-index 0
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

# Load .env file before anything else
load_dotenv()

# Ensure we can import aws_background
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    import aws_background
except ImportError:
    print("Error: Could not import aws_background.py. Make sure this script is in the same directory.")
    sys.exit(1)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [POPULATE-HWS] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")


def populate_hour_wise_sessions(brand_index: int, start_date: str, end_date: str):
    """
    Overwrite number_of_sessions and number_of_atc_sessions in hour_wise_sales
    using data from hourly_sessions_summary_shopify for the given date range.

    Only updates EXISTING rows in hour_wise_sales — does not insert new rows.
    Uses number_of_sessions directly from the source table (not device column sums).
    """
    brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")

    logger.info(f"🚀 Populating hour_wise_sales for {brand_name}")
    logger.info(f"   Date range: {start_date} -> {end_date}")

    try:
        with aws_background.get_db_cursor(brand_index) as (cursor, connection):
            # Single UPDATE JOIN: overwrites session columns in hour_wise_sales
            # using data from hourly_sessions_summary_shopify.
            # Only touches rows that already exist in hour_wise_sales.
            update_sql = """
                UPDATE hour_wise_sales hws
                JOIN hourly_sessions_summary_shopify hss
                  ON hws.date = hss.date AND hws.hour = hss.hour
                SET
                  hws.number_of_sessions = COALESCE(hss.number_of_sessions, 0),
                  hws.number_of_atc_sessions = COALESCE(hss.number_of_atc_sessions, 0)
                WHERE hws.date BETWEEN %s AND %s
            """

            logger.info(f"⏳ Executing UPDATE for {brand_name}...")
            cursor.execute(update_sql, (start_date, end_date))
            affected_rows = cursor.rowcount

            connection.commit()
            logger.info(f"✅ [{brand_name}] Overwrote session data for {affected_rows} hour-slots in hour_wise_sales")

    except Exception as e:
        logger.error(f"❌ Error populating hour_wise_sales for {brand_name}: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Populate hour_wise_sales sessions from hourly_sessions_summary_shopify"
    )
    parser.add_argument("--brand-index", type=int, help="Brand index from .env (optional, defaults to all/filtered)")
    parser.add_argument("--start", type=str, help="Start Date YYYY-MM-DD (optional if BACKFILL_START_IST env set)")
    parser.add_argument("--end", type=str, help="End Date YYYY-MM-DD (optional if BACKFILL_END_IST env set)")

    args = parser.parse_args()

    # Initialize configs (loads env vars, creates DB pools)
    aws_background.initialize_brand_configs()

    # TEST_MODE visibility
    if aws_background.TEST_MODE:
        logger.info("⚠️  TEST_MODE is ENABLED. DB SSL will be DISABLED.")

    # BACKFILL_MODE check
    is_manual_override = bool(args.start and args.end)
    if not aws_background.BACKFILL_MODE and not is_manual_override:
        logger.error("🛑 BACKFILL_MODE is DISABLED in .env and no CLI dates provided. Script exiting.")
        sys.exit(0)

    # --- Determine date range ---
    s_dt = None
    e_dt = None

    # 1. Try CLI args
    if args.start:
        try:
            s_dt = datetime.fromisoformat(args.start).replace(tzinfo=IST)
        except ValueError:
            logger.error("Invalid --start format. Use YYYY-MM-DD or ISO format.")
            sys.exit(1)

    if args.end:
        try:
            e_dt = datetime.fromisoformat(args.end).replace(tzinfo=IST)
        except ValueError:
            logger.error("Invalid --end format. Use YYYY-MM-DD or ISO format.")
            sys.exit(1)

    # 2. Fallback to env vars
    if not s_dt:
        if aws_background.BACKFILL_START_IST:
            s_dt = aws_background.BACKFILL_START_IST
            logger.info(f"Using BACKFILL_START_IST from env: {s_dt}")
        else:
            logger.error("No start date provided! Use --start or set BACKFILL_START_IST (.env)")
            sys.exit(1)

    if not e_dt:
        if aws_background.BACKFILL_END_IST:
            e_dt = aws_background.BACKFILL_END_IST
            logger.info(f"Using BACKFILL_END_IST from env: {e_dt}")
        else:
            logger.error("No end date provided! Use --end or set BACKFILL_END_IST (.env)")
            sys.exit(1)

    # Validate
    if s_dt >= e_dt:
        logger.error(f"Start date {s_dt} must be before end date {e_dt}")
        sys.exit(1)

    start_date_str = s_dt.date().isoformat()
    end_date_str = e_dt.date().isoformat()

    # --- Determine target brands ---
    target_indices = []

    if args.brand_index is not None:
        # CLI-specified brand
        target_indices = [args.brand_index]
    else:
        if aws_background.BACKFILL_BRAND_INDICES:
            logger.info(f"Using BACKFILL_BRAND_INDICES from env: {aws_background.BACKFILL_BRAND_INDICES}")
            target_indices = [
                i for i in aws_background.active_brand_indices
                if aws_background.is_backfill_active_for(i)
            ]
        else:
            logger.info("No brand filter. Processing active brands based on BACKFILL_MODE.")
            target_indices = [
                i for i in aws_background.active_brand_indices
                if aws_background.is_backfill_active_for(i)
            ]

        # Manual override: if CLI dates provided but no brands matched via backfill filter
        if is_manual_override and not target_indices:
            if args.brand_index is not None:
                target_indices = [args.brand_index]
            elif aws_background.BACKFILL_BRAND_INDICES:
                # Still respect the brand index filter even in manual override
                target_indices = [
                    i for i in aws_background.active_brand_indices
                    if i in aws_background.BACKFILL_BRAND_INDICES
                ]
            else:
                target_indices = aws_background.active_brand_indices

    if not target_indices:
        logger.error("No valid brands found to process.")
        sys.exit(1)

    logger.info(f"🎯 Targeting Brands: {target_indices}")
    logger.info(f"📅 Date Range: {start_date_str} -> {end_date_str}")

    for idx in target_indices:
        try:
            populate_hour_wise_sessions(idx, start_date_str, end_date_str)
        except Exception as e:
            logger.error(f"Failed populate for brand {idx}: {e}")

    logger.info("🏁 Done.")
