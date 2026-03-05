# -*- coding: utf-8 -*-
"""
Standalone script to backfill 'overall_utm_summary' using env vars for date range.
Usage: python utm_sessions_backfill.py
"""

import os
import sys
import logging
import traceback
from datetime import datetime
from dotenv import load_dotenv

# Load env before imports to ensure config bits are set if needed at module level
load_dotenv()

# Set up logging to stdout
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Import necessary components from main script
# We need to import:
# 1. initialize_brand_configs (to populate brand lists)
# 2. process_and_upload_utm_summary (the logic)
# 3. active_brand_indices (global list)
# 4. BACKFILL_START_IST, BACKFILL_END_IST (from env, but we'll parse here too)
try:
    from aws_background import (
        initialize_brand_configs, 
        process_and_upload_utm_summary,
        active_brand_indices
    )
except ImportError:
    logger.error("❌ Could not import from aws_background.py. Run this script in the same directory.")
    sys.exit(1)

def run_backfill():
    # 1. Parse Environment Variables for Date Range and Brands
    start_str = os.getenv("BACKFILL_START_IST")
    end_str = os.getenv("BACKFILL_END_IST")
    
    if not start_str or not end_str:
        logger.error("❌ Missing BACKFILL_START_IST or BACKFILL_END_IST in .env or environment.")
        return

    # Check for specific brand filter
    target_indices_str = os.getenv("BACKFILL_BRAND_INDICES", "")
    target_indices = []
    if target_indices_str:
        target_indices = [int(idx.strip()) for idx in target_indices_str.split(",") if idx.strip()]
        logger.info(f"Targeting specific brands: {target_indices}")

    try:
        # Validate date format (YYYY-MM-DD HH:MM:SS or YYYY-MM-DD)
        # We only need the DATE part for the query
        s_date = start_str.split(" ")[0].split("T")[0]
        e_date = end_str.split(" ")[0].split("T")[0]
        # Quick validation
        datetime.strptime(s_date, "%Y-%m-%d")
        datetime.strptime(e_date, "%Y-%m-%d")
    except ValueError as e:
        logger.error(f"❌ Invalid date format in env vars: {e}. Expected YYYY-MM-DD...")
        return

    logger.info(f"\n{'='*60}\nUTM BACKFILL TRIGGERED: {s_date} -> {e_date}\n{'='*60}")

    # 2. Initialize Brands
    initialize_brand_configs()
    
    if not active_brand_indices:
        logger.warning("No active brands found based on current configuration.")
        return

    # 3. Iterate and Process
    processed_count = 0
    
    # If filtered list is provided, intersect with active
    indices_to_process = active_brand_indices
    if target_indices:
        indices_to_process = [i for i in active_brand_indices if i in target_indices]

    if not indices_to_process:
        logger.warning("No matching brands to process after filtering.")
        return

    for brand_index in indices_to_process:
        brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
        shop_name = os.environ.get(f"SHOP_NAME_{brand_index}")
        access_token = os.environ.get(f"ACCESS_TOKEN_{brand_index}")
        
        if not shop_name or not access_token:
            logger.warning(f"Skipping {brand_name} (missing credentials)")
            continue
            
        logger.info(f"👉 Processing {brand_name}...")
        try:
            process_and_upload_utm_summary(
                brand_index=brand_index,
                brand_name=brand_name,
                shop_name=shop_name,
                access_token=access_token,
                min_date=s_date,
                max_date=e_date
            )
            logger.info(f"✅ Successfully backfilled 'overall_utm_summary' (including utm_names) for {brand_name}")
            processed_count += 1
        except Exception as e:
            logger.error(f"❌ Failed to backfill {brand_name}: {e}")
            traceback.print_exc()

    logger.info(f"\n{'='*60}\nBACKFILL COMPLETE. Processed {processed_count} brands.\n{'='*60}")

if __name__ == "__main__":
    run_backfill()
