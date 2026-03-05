# -*- coding: utf-8 -*-
"""
Backfill Sessions to Hour-Wise Sales Script
Usage:
    python backfill_hour_wise_sessions.py --start "2024-01-01T00:00:00" --end "2024-01-02T00:00:00" --brand-index 0
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

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
    format='%(asctime)s [SYNC-SESSIONS] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")

def sync_sessions_to_hour_wise(brand_index: int, start_dt: datetime, end_dt: datetime):
    # Reuse configuration
    aws_background.initialize_brand_configs()
    
    if brand_index not in aws_background.db_connection_pools:
        logger.error(f"❌ No connection pool found for brand index {brand_index}.")
        return

    brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
    
    start_date_str = start_dt.date().isoformat()
    end_date_str = end_dt.date().isoformat()
    
    logger.info(f"🚀 Starting Session Sync for {brand_name}")
    logger.info(f"   Sync Window: {start_date_str} -> {end_date_str}")

    try:
        with aws_background.get_db_cursor(brand_index) as (cursor, connection):
            # Sync logic: Update hour_wise_sales using data from hourly_sessions_summary_shopify
            # This follows the requirement: "it will not perform new writes" (meaning no new 
            # remote data fetch) but rather populates/updates from an existing local table.
            
            sql = """
            UPDATE hour_wise_sales hws
            JOIN hourly_sessions_summary_shopify hss 
              ON hws.date = hss.date AND hws.hour = hss.hour
            SET 
              hws.number_of_sessions = hss.number_of_sessions,
              hws.number_of_atc_sessions = hss.number_of_atc_sessions
            WHERE hws.date BETWEEN %s AND %s
            """
            
            logger.info(f"⏳ Executing sync query for {brand_name}...")
            cursor.execute(sql, (start_date_str, end_date_str))
            affected_rows = cursor.rowcount
            
            connection.commit()
            logger.info(f"✅ Successfully synced sessions for {affected_rows} hour-slots in hour_wise_sales.")

    except Exception as e:
        logger.error(f"❌ Exception during session sync for {brand_name}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Sessions to Hour-Wise Sales")
    parser.add_argument("--brand-index", type=int, help="Brand index from .env")
    parser.add_argument("--start", type=str, help="Start Date YYYY-MM-DDTHH:mm:ss")
    parser.add_argument("--end", type=str, help="End Date YYYY-MM-DDTHH:mm:ss")
    
    args = parser.parse_args()
    
    # Initialize configs (loads env vars)
    aws_background.initialize_brand_configs()
    
    # 0. Rule Enforcement: TEST_MODE visibility
    if aws_background.TEST_MODE:
        logger.info("⚠️  TEST_MODE is ENABLED. DB SSL will be DISABLED.")
    
    # 0. Rule Enforcement: BACKFILL_MODE check
    is_manual_override = bool(args.start and args.end)
    if not aws_background.BACKFILL_MODE and not is_manual_override:
        logger.error("🛑 BACKFILL_MODE is DISABLED in .env and no CLI dates provided. Script exiting.")
        sys.exit(0)

    # Determine Date Range
    s_dt = None
    e_dt = None
    
    if args.start:
        try:
            s_dt = datetime.fromisoformat(args.start).replace(tzinfo=IST)
        except ValueError:
            logger.error("Invalid --start format. Use ISO format.")
            sys.exit(1)
    
    if args.end:
        try:
            e_dt = datetime.fromisoformat(args.end).replace(tzinfo=IST)
        except ValueError:
            logger.error("Invalid --end format. Use ISO format.")
            sys.exit(1)

    # Fallback to Env if no CLI
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
            
    if s_dt >= e_dt:
        logger.error(f"Start date {s_dt} must be before end date {e_dt}")
        sys.exit(1)
        
    # Determine Target Brands
    target_indices = []
    if args.brand_index is not None:
        target_indices = [args.brand_index]
    else:
        # Use is_backfill_active_for logic (checks both Toggle and Index Filter)
        target_indices = [i for i in aws_background.active_brand_indices if aws_background.is_backfill_active_for(i)]
        
        # Override for manual CLI override
        if is_manual_override and not target_indices:
            target_indices = aws_background.active_brand_indices

    if not target_indices:
        logger.error("No valid brands found to process.")
        sys.exit(1)
        
    logger.info(f"Targeting Brands: {target_indices}")
    
    for idx in target_indices:
        try:
            sync_sessions_to_hour_wise(idx, s_dt, e_dt)
        except Exception as e:
            logger.error(f"Failed sync for brand {idx}: {e}")
