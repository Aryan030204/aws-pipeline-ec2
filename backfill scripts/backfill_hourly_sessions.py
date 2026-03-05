# -*- coding: utf-8 -*-
"""
Backfill Hourly Sessions Script
Usage:
    python backfill_hourly_sessions.py --start "2024-01-01T00:00:00" --end "2024-01-02T00:00:00" --brand-index 0
"""

import os
import sys
import argparse
import logging
import time
from datetime import datetime, timedelta
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
    format='%(asctime)s [BACKFILL] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")

# ... (Imports remain similar, maybe add json)
import json

def backfill_via_shopifyql(brand_index: int, start_dt: datetime, end_dt: datetime):
    # Reuse configuration
    aws_background.initialize_brand_configs()
    
    if brand_index not in aws_background.db_connection_pools:
        logger.error(f"❌ No connection pool found for brand index {brand_index}.")
        return

    brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
    shop_name = os.environ.get(f"SHOP_NAME_{brand_index}")
    access_token = os.environ.get(f"ACCESS_TOKEN_{brand_index}")
    
    if not (shop_name and access_token):
        logger.error(f"❌ Missing Shop Name or Access Token for brand {brand_index}")
        return

    # To be safe with timezones and "day" grouping, we'll request the full range 
    # but ensure we format dates as YYYY-MM-DD for the query.
    start_date_str = start_dt.date().isoformat()
    end_date_str = end_dt.date().isoformat()
    
    logger.info(f"🚀 Starting ShopifyQL Backfill for {brand_name}")
    logger.info(f"   Query Window: {start_date_str} -> {end_date_str} (Inclusive)")

    # ShopifyQL Query
    # Note: 'hour' in ShopifyQL is 0-23
    query = f"""
    FROM sessions
    SHOW sessions, sessions_with_cart_additions, hour, day
    GROUP BY day, hour
    WITH CURRENCY 'INR'
    SINCE {start_date_str} UNTIL {end_date_str}
    ORDER BY day ASC, hour ASC
    LIMIT 10000
    VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values
    """
    
    ql_flat = query.replace("\n", " ").strip().replace('"', '\\"')
    graphql_query = f'''
    query {{
      shopifyqlQuery(query: "{ql_flat}") {{
        tableData {{
          rows
          columns {{
            name
            dataType
          }}
        }}
        parseErrors
      }}
    }}
    '''
    
    url = f"https://{shop_name}.myshopify.com/admin/api/2025-10/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token,
    }

    # Execute
    try:
        # Use aws_background's session factory if available, else standard
        if hasattr(aws_background, '_make_shopifyql_session'):
             session = aws_background._make_shopifyql_session()
        else:
             session = aws_background.requests.Session()

        logger.info("⏳ Sending ShopifyQL query...")
        resp = session.post(url, headers=headers, data=json.dumps({"query": graphql_query}), timeout=60)
        
        if resp.status_code != 200:
            logger.error(f"❌ Shopify API Error: {resp.status_code} {resp.text}")
            return

        body = resp.json()
        if body.get("errors"):
            logger.error(f"❌ GraphQL Errors: {body['errors']}")
            return
            
        ql_result = (body.get("data") or {}).get("shopifyqlQuery") or {}
        if ql_result.get("parseErrors"):
            logger.error(f"❌ Parse Errors: {ql_result['parseErrors']}")
            return

        table_data = ql_result.get("tableData")
        if not table_data:
            logger.warning("⚠️ No data returned.")
            return

        # Format rows (Handle legacy list vs new dict format)
        # Using aws_background helper if possible, or implementing simplified version
        rows = []
        raw_rows = table_data.get("rows") or []
        cols = table_data.get("columns") or []
        
        for r in raw_rows:
            if isinstance(r, dict):
                rows.append(r)
            elif isinstance(r, list):
                # Map by index
                row_dict = {}
                for idx, val in enumerate(r):
                    if idx < len(cols):
                        row_dict[cols[idx]['name']] = val
                rows.append(row_dict)

        logger.info(f"✅ Received {len(rows)} hour-slots from Shopify.")

        # Upsert to DB
        with aws_background.get_db_cursor(brand_index) as (cursor, connection):
             cursor.execute("""
                CREATE TABLE IF NOT EXISTS hourly_sessions_summary_shopify (
                    date DATE NOT NULL,
                    hour TINYINT UNSIGNED NOT NULL,
                    number_of_sessions INT DEFAULT 0,
                    number_of_atc_sessions INT DEFAULT 0,
                    mobile_sessions INT DEFAULT 0,
                    mobile_atc_sessions INT DEFAULT 0,
                    desktop_sessions INT DEFAULT 0,
                    desktop_atc_sessions INT DEFAULT 0,
                    tablet_sessions INT DEFAULT 0,
                    tablet_atc_sessions INT DEFAULT 0,
                    other_sessions INT DEFAULT 0,
                    other_atc_sessions INT DEFAULT 0,
                    adjusted_number_of_sessions INT DEFAULT NULL,
                    PRIMARY KEY (date, hour)
                );
             """)
             
             count = 0
             logger.info(f"DEBUG: Processing {len(rows)} rows...")
             
             for i, row in enumerate(rows):
                 # Parse fields
                 d_str = row.get("day")
                 h_val = row.get("hour")
                 s_val = row.get("sessions")
                 atc_val = row.get("sessions_with_cart_additions")
                 
                 # Debug first few rows
                 if i < 3:
                     logger.info(f"DEBUG Row {i}: day={d_str} hour={h_val} sess={s_val} atc={atc_val}")

                 if not h_val:
                     logger.warning(f"Skipping row {i}: Missing hour. Row: {row}")
                     continue
                     
                 try:
                     # h_val is an ISO timestamp (e.g., '2026-01-20T18:30:00Z')
                     # Convert to IST to get the correct Date and Hour bucket
                     dt_utc = datetime.fromisoformat(str(h_val).replace("Z", "+00:00"))
                     dt_ist = dt_utc.astimezone(IST)
                     
                     d_dt = dt_ist.date()
                     h_int = dt_ist.hour
                     
                     s_int = int(s_val) if s_val is not None else 0
                     atc_int = int(atc_val) if atc_val is not None else 0
                 except ValueError as e:
                     logger.error(f"Skipping row {i} due to ValueError: {e}. Row: {row}")
                     continue

                 # log invalid hour just in case
                 if not (0 <= h_int <= 23):
                     logger.warning(f"Skipping row {i}: Invalid hour {h_int}")
                     continue

                 cursor.execute("""
                    INSERT INTO hourly_sessions_summary_shopify (date, hour, number_of_sessions, number_of_atc_sessions)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        number_of_sessions = VALUES(number_of_sessions),
                        number_of_atc_sessions = VALUES(number_of_atc_sessions);
                 """, (d_dt, h_int, s_int, atc_int))
                 count += 1
             
             connection.commit()
             logger.info(f"✅ Upserted {count} hourly records into DB.")

    except Exception as e:
        logger.error(f"❌ Exception during ShopifyQL execution: {e}")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill Hourly Sessions")
    parser.add_argument("--brand-index", type=int, help="Brand index from .env (optional, defaults to all/filtered)")
    parser.add_argument("--start", type=str, help="Start Date YYYY-MM-DDTHH:mm:ss (optional if BACKFILL_START_IST env set)")
    parser.add_argument("--end", type=str, help="End Date YYYY-MM-DDTHH:mm:ss (optional if BACKFILL_END_IST env set)")
    
    args = parser.parse_args()
    
    # Initialize configs (loads env vars)
    aws_background.initialize_brand_configs()
    
    # 0. Rule Enforcement: TEST_MODE visibility
    if aws_background.TEST_MODE:
        logger.info("⚠️  TEST_MODE is ENABLED. DB SSL will be DISABLED.")
    
    # 0. Rule Enforcement: BACKFILL_MODE check
    # If BACKFILL_MODE is false, only allow run if CLI dates are provided (manual override)
    is_manual_override = bool(args.start and args.end)
    if not aws_background.BACKFILL_MODE and not is_manual_override:
        logger.error("🛑 BACKFILL_MODE is DISABLED in .env and no CLI dates provided. Script exiting.")
        sys.exit(0)
    
    # Determine Date Range
    s_dt = None
    e_dt = None
    
    # 1. Try CLI
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

    # 2. Try Env (if CLI missing)
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
            
    # Validate Range
    if s_dt >= e_dt:
        logger.error(f"Start date {s_dt} must be before end date {e_dt}")
        sys.exit(1)
        
    # Determine Target Brands
    target_indices = []
    
    if args.brand_index is not None:
        # CLI Specific Brand
        target_indices = [args.brand_index]
    else:
        # Check Env Filter
        if aws_background.BACKFILL_BRAND_INDICES:
            logger.info(f"Using BACKFILL_BRAND_INDICES from env: {aws_background.BACKFILL_BRAND_INDICES}")
            # Intersect with active to ensure validity and respect is_backfill_active_for
            target_indices = [i for i in aws_background.active_brand_indices if aws_background.is_backfill_active_for(i)]
        else:
            # All Active Brands (but still respect is_backfill_active_for which checks BACKFILL_MODE)
            logger.info("No brand filter. Backfilling active brands based on BACKFILL_MODE.")
            target_indices = [i for i in aws_background.active_brand_indices if aws_background.is_backfill_active_for(i)]
            
        # If we have manual CLI dates, we should include the brand_index if provided, 
        # or all active if not, OVERRIDING the is_backfill_active_for check if it was False due to toggle
        if is_manual_override:
             if args.brand_index is not None:
                 target_indices = [args.brand_index]
             else:
                 target_indices = aws_background.active_brand_indices
            
    if not target_indices:
        logger.error("No valid brands found to backfill.")
        sys.exit(1)
        
    logger.info(f"Targeting Brands: {target_indices}")
    
    for idx in target_indices:
        try:
            backfill_via_shopifyql(idx, s_dt, e_dt)
        except Exception as e:
            logger.error(f"Failed backfill for brand {idx}: {e}")

