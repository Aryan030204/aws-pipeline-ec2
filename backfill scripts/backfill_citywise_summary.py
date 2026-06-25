# -*- coding: utf-8 -*-
"""
Standalone script to backfill citywise session source tables and summaries.
Usage:
    python "backfill scripts/backfill_citywise_summary.py" --start-date 2026-06-01 --end-date 2026-06-07
    python "backfill scripts/backfill_citywise_summary.py" --start-date 2026-06-01 --end-date 2026-06-07 --brand-index 0
"""

import argparse
import logging
import os
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

try:
    from aws_background import (
        active_brand_indices,
        ensure_summary_tables,
        get_brand_timezone,
        get_db_cursor,
        initialize_brand_configs,
        update_daily_city_sessions_summary_from_shopifyql,
        update_daily_citywise_summary_incremental,
        update_hourly_city_sessions_summary_from_shopifyql,
        update_hourly_citywise_summary_incremental,
    )
except ImportError:
    logger.error("Could not import from aws_background.py. Run this script in the repo root.")
    sys.exit(1)


def _parse_date(raw: str):
    return datetime.strptime(raw, "%Y-%m-%d").date()


def _date_range_iso_set(start_date, end_date):
    current = start_date
    values = set()
    while current <= end_date:
        values.add(current.isoformat())
        current += timedelta(days=1)
    return values


def run_backfill(start_date, end_date, target_brand_index=None):
    initialize_brand_configs()
    if not active_brand_indices:
        logger.warning("No active brands found based on current configuration.")
        return

    indices_to_process = active_brand_indices
    if target_brand_index is not None:
        indices_to_process = [i for i in active_brand_indices if i == target_brand_index]

    if not indices_to_process:
        logger.warning("No matching brands to process after filtering.")
        return

    target_dates = _date_range_iso_set(start_date, end_date)
    processed_count = 0

    for brand_index in indices_to_process:
        brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
        shop_name = os.environ.get(f"SHOP_NAME_{brand_index}")
        api_version = os.environ.get(f"API_VERSION_{brand_index}", "2024-04")
        access_token = os.environ.get(f"ACCESS_TOKEN_{brand_index}")

        if not shop_name or not access_token:
            logger.warning(f"[{brand_name}] Missing Shopify credentials. Skipping.")
            continue

        try:
            with get_db_cursor(brand_index) as (cursor, connection):
                ensure_summary_tables(cursor, connection)
                brand_tz = get_brand_timezone(brand_index)

                daily_dates = update_daily_city_sessions_summary_from_shopifyql(
                    brand_index=brand_index,
                    brand_name=brand_name,
                    shop_name=shop_name,
                    api_version=api_version,
                    access_token=access_token,
                    tz=brand_tz,
                    cursor=cursor,
                    connection=connection,
                    start_date=start_date,
                    end_date=end_date,
                )

                hourly_dates = update_hourly_city_sessions_summary_from_shopifyql(
                    brand_index=brand_index,
                    brand_name=brand_name,
                    shop_name=shop_name,
                    api_version=api_version,
                    access_token=access_token,
                    tz=brand_tz,
                    cursor=cursor,
                    connection=connection,
                    start_date=start_date,
                    end_date=end_date,
                )

                rebuild_dates = target_dates | daily_dates | hourly_dates
                if not rebuild_dates:
                    logger.info(f"[{brand_name}] No citywise dates returned for requested window.")
                    continue

                update_daily_citywise_summary_incremental(
                    cursor,
                    connection,
                    brand_name,
                    rebuild_dates,
                )
                update_hourly_citywise_summary_incremental(
                    cursor,
                    connection,
                    brand_name,
                    rebuild_dates,
                )

                logger.info(
                    f"[{brand_name}] Backfilled citywise summaries for {len(rebuild_dates)} date(s)"
                )
                processed_count += 1
        except Exception as exc:
            logger.error(f"[{brand_name}] Failed to backfill citywise summaries: {exc}")
            traceback.print_exc()

    logger.info(
        f"Citywise summary backfill complete. Processed {processed_count} brand(s)."
    )


def main():
    parser = argparse.ArgumentParser(description="Backfill citywise summary tables")
    parser.add_argument("--start-date", required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", required=True, help="End date in YYYY-MM-DD format")
    parser.add_argument("--brand-index", type=int, help="Optional brand index")
    args = parser.parse_args()

    start_date = _parse_date(args.start_date)
    end_date = _parse_date(args.end_date)
    if start_date > end_date:
        raise ValueError("--start-date cannot be after --end-date")

    run_backfill(start_date, end_date, target_brand_index=args.brand_index)


if __name__ == "__main__":
    main()
