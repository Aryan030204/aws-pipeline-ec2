# -*- coding: utf-8 -*-
"""
Surgical Backfill Script for User Agent and Device-level Sessions.
This script specifically updates only the new columns (user_agent, mobile_sessions, etc.)
without affecting other metrics like total sales or triggering summary recalculations.

Usage:
    python -m dotenv run -- python backfill_ua_and_device_sessions.py --start "2024-01-01T00:00:00" --end "2024-01-02T23:59:59" --brand-index 1
"""

import os
import sys
import argparse
import logging
import traceback
import json
from datetime import datetime
from zoneinfo import ZoneInfo

# Ensure we can import aws_background
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    import aws_background
except ImportError:
    print("Error: Could not import aws_background.py. Please run from the pipeline directory.")
    sys.exit(1)

# Setup specialized logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [SURGICAL-BACKFILL] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")

def surgical_backfill_brand(idx, s_dt, e_dt):
    brand_label = os.environ.get(f"BRAND_NAME_{idx}", f"Brand_{idx}")
    shop_name = os.environ.get(f"SHOP_NAME_{idx}")
    api_version = os.environ.get(f"API_VERSION_{idx}")
    access_token = os.environ.get(f"ACCESS_TOKEN_{idx}")
    api_base_url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}"

    if not all([shop_name, access_token, api_version]):
        logger.error(f"❌ Missing credentials for {brand_label}. Skipping.")
        return

    # 1. Ensure Schema
    aws_background.ensure_user_agent_column(idx, 'shopify_orders')
    aws_background.ensure_user_agent_column(idx, 'shopify_orders_update')
    aws_background.ensure_device_summary_columns(idx, 'hourly_sessions_summary')
    aws_background.ensure_device_summary_columns(idx, 'hourly_sessions_summary_shopify')

    # 2. Backfill User Agent (Surgical UPDATE)
    logger.info(f"[{brand_label}] Fetching orders for UA backfill...")
    start_fmt = aws_background.convert_to_desired_format(s_dt)
    end_fmt = aws_background.convert_to_desired_format(e_dt)
    
    # We fetch NEW orders for the range. We don't care about 'updated_at' here as UA is set at creation.
    # However, to be safe, we check both new and old range if needed.
    orders = aws_background.fetch_orders(api_base_url, access_token, start_fmt, end_fmt, "created_at")
    
    if orders:
        logger.info(f"[{brand_label}] Found {len(orders)} orders. Extracting UAs and updating...")
        with aws_background.get_db_cursor(idx) as (cursor, connection):
            updated_count = 0
            for o in orders:
                oid = str(o.get('id'))
                ua = "unknown"
                note_attrs = o.get('note_attributes', [])
                if isinstance(note_attrs, list):
                    for attr in note_attrs:
                        if attr.get('name') == 'user_agent':
                            ua = attr.get('value', 'unknown')
                            break
                
                # Surgical update on both tables
                sql = "UPDATE shopify_orders SET user_agent = %s WHERE order_id = %s"
                cursor.execute(sql, (ua, oid))
                cnt1 = cursor.rowcount
                
                sql_upd = "UPDATE shopify_orders_update SET user_agent = %s WHERE order_id = %s"
                cursor.execute(sql_upd, (ua, oid))
                cnt2 = cursor.rowcount
                
                if cnt1 > 0 or cnt2 > 0:
                    updated_count += 1
            
            connection.commit()
            logger.info(f"✅ [{brand_label}] Surgically updated user_agent for {updated_count} unique orders (affected rows: {updated_count} orders).")
    else:
        logger.info(f"[{brand_label}] No orders found in range for UA backfill.")

    # 3. Backfill Device Sessions (Surgical UPDATE)
    logger.info(f"[{brand_label}] Fetching device sessions via ShopifyQL...")
    # ShopifyQL assumes date strings
    s_date = s_dt.date().isoformat()
    e_date = e_dt.date().isoformat()
    
    session_results = aws_background.fetch_shopify_hourly_sessions_via_shopifyql(
        shop_name, api_version, access_token, s_date, e_date
    )
    
    if session_results:
        logger.info(f"[{brand_label}] Found {len(session_results)} hourly records. Updating device columns...")
        with aws_background.get_db_cursor(idx) as (cursor, connection):
            for r in session_results:
                # Calculate totals for consistency
                total_s = r["mobile_sessions"] + r["desktop_sessions"] + r["tablet_sessions"] + r["other_sessions"]
                total_a = r["mobile_atc_sessions"] + r["desktop_atc_sessions"] + r["tablet_atc_sessions"] + r["other_atc_sessions"]

                # Surgical update hitting device columns AND syncing the total
                sql_dev = """
                    INSERT INTO hourly_sessions_summary 
                    (date, hour, number_of_sessions, number_of_atc_sessions,
                     mobile_sessions, mobile_atc_sessions, desktop_sessions, desktop_atc_sessions, 
                     tablet_sessions, tablet_atc_sessions, other_sessions, other_atc_sessions)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        number_of_sessions = VALUES(number_of_sessions),
                        number_of_atc_sessions = VALUES(number_of_atc_sessions),
                        mobile_sessions = VALUES(mobile_sessions), mobile_atc_sessions = VALUES(mobile_atc_sessions),
                        desktop_sessions = VALUES(desktop_sessions), desktop_atc_sessions = VALUES(desktop_atc_sessions),
                        tablet_sessions = VALUES(tablet_sessions), tablet_atc_sessions = VALUES(tablet_atc_sessions),
                        other_sessions = VALUES(other_sessions), other_atc_sessions = VALUES(other_atc_sessions);
                """
                cursor.execute(sql_dev, (
                    r["date"], r["hour"], total_s, total_a,
                    r["mobile_sessions"], r["mobile_atc_sessions"],
                    r["desktop_sessions"], r["desktop_atc_sessions"], r["tablet_sessions"], r["tablet_atc_sessions"],
                    r["other_sessions"], r["other_atc_sessions"]
                ))

                sql_dev_shop = """
                    INSERT INTO hourly_sessions_summary_shopify 
                    (date, hour, number_of_sessions, number_of_atc_sessions,
                     mobile_sessions, mobile_atc_sessions, desktop_sessions, desktop_atc_sessions, 
                     tablet_sessions, tablet_atc_sessions, other_sessions, other_atc_sessions)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        number_of_sessions = VALUES(number_of_sessions),
                        number_of_atc_sessions = VALUES(number_of_atc_sessions),
                        mobile_sessions = VALUES(mobile_sessions), mobile_atc_sessions = VALUES(mobile_atc_sessions),
                        desktop_sessions = VALUES(desktop_sessions), desktop_atc_sessions = VALUES(desktop_atc_sessions),
                        tablet_sessions = VALUES(tablet_sessions), tablet_atc_sessions = VALUES(tablet_atc_sessions),
                        other_sessions = VALUES(other_sessions), other_atc_sessions = VALUES(other_atc_sessions);
                """
                cursor.execute(sql_dev_shop, (
                    r["date"], r["hour"], total_s, total_a,
                    r["mobile_sessions"], r["mobile_atc_sessions"],
                    r["desktop_sessions"], r["desktop_atc_sessions"], r["tablet_sessions"], r["tablet_atc_sessions"],
                    r["other_sessions"], r["other_atc_sessions"]
                ))
            
            connection.commit()
            logger.info(f"✅ [{brand_label}] Surgically updated device sessions for {len(session_results)} hours.")
    else:
        logger.info(f"[{brand_label}] No session data found in range.")

def main():
    parser = argparse.ArgumentParser(description="Surgical Backfill for User Agent and Device Sessions")
    parser.add_argument("--start", type=str, help="Start Date YYYY-MM-DDTHH:mm:ss (IST). Defaults to BACKFILL_START_IST env var.")
    parser.add_argument("--end", type=str, help="End Date YYYY-MM-DDTHH:mm:ss (IST). Defaults to BACKFILL_END_IST env var.")
    parser.add_argument("--brand-index", type=int, help="Specific brand index (optional). Defaults to BACKFILL_BRAND_INDICES env var.")
    
    args = parser.parse_args()
    
    # 1. Bootstrap AWS Background (loads .env and initializes configurations)
    logger.info("Initializing brand configurations and database pools...")
    aws_background.initialize_brand_configs()
    
    # 2. Resolve Dates (Arg > Env > Error)
    s_dt = None
    e_dt = None

    if args.start:
        try:
            s_dt = datetime.fromisoformat(args.start).replace(tzinfo=IST)
        except ValueError:
            logger.error(f"❌ Invalid --start format '{args.start}'. Use YYYY-MM-DDTHH:mm:ss")
            sys.exit(1)
    else:
        s_dt = aws_background.BACKFILL_START_IST

    if args.end:
        try:
            e_dt = datetime.fromisoformat(args.end).replace(tzinfo=IST)
        except ValueError:
            logger.error(f"❌ Invalid --end format '{args.end}'. Use YYYY-MM-DDTHH:mm:ss")
            sys.exit(1)
    else:
        e_dt = aws_background.BACKFILL_END_IST

    if not s_dt or not e_dt:
        logger.error("❌ Missing backfill window. Provide --start/--end OR set BACKFILL_START_IST/BACKFILL_END_IST in .env")
        sys.exit(1)
        
    if s_dt >= e_dt:
        logger.error(f"❌ Invalid window: {s_dt} is not before {e_dt}")
        sys.exit(1)

    # 3. Identify Target Brands (Arg > Env > All Active)
    if args.brand_index is not None:
        target_indices = [args.brand_index]
    else:
        # If env filter exists and has values, use it (already parsed into a set in aws_background)
        if aws_background.BACKFILL_BRAND_INDICES:
            target_indices = [i for i in aws_background.active_brand_indices if i in aws_background.BACKFILL_BRAND_INDICES]
        else:
            # Otherwise process all active brands
            target_indices = aws_background.active_brand_indices

    if not target_indices:
        logger.error("❌ No valid active brands found to backfill. Ensure BRAND_NAME_X and connection strings are set.")
        sys.exit(1)

    logger.info(f"{'='*60}")
    logger.info(f"🚀 SURGICAL BACKFILL INITIATED")
    logger.info(f"📅 Range: {s_dt} to {e_dt} (IST)")
    logger.info(f"🎯 Brands: {target_indices}")
    logger.info(f"🛠️ TEST_MODE: {aws_background.TEST_MODE}")
    logger.info(f"{'='*60}")

    for idx in target_indices:
        try:
            surgical_backfill_brand(idx, s_dt, e_dt)
        except Exception as e:
            logger.error(f"❌ Critical failure for brand index {idx}: {e}")
            traceback.print_exc()

    logger.info(f"{'='*60}")
    logger.info("🏁 Surgical backfill process finished.")
    logger.info(f"{'='*60}")

if __name__ == "__main__":
    main()
