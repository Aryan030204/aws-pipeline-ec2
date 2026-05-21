# -*- coding: utf-8 -*-
"""
Standalone script to backfill TMC bundle rollups for a custom date range.
Usage: python "backfill scripts/bundle_rollup_backfill.py"
"""

import logging
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

load_dotenv()
if os.getenv("TEST_MODE", "false").strip().lower() == "true":
    load_dotenv(REPO_ROOT / ".env.local", override=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

try:
    from aws_background import active_brand_indices, get_db_cursor, initialize_brand_configs
    from bundle_rollups import (
        build_target_dates,
        execute_bundle_rollups_incremental,
        is_bundle_rollup_brand,
    )
except ImportError:
    logger.error("Could not import bundle rollup dependencies. Run this script from the repo root.")
    sys.exit(1)


def _parse_date_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    try:
        return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
    except ValueError as exc:
        raise ValueError(f"Invalid {name}: expected YYYY-MM-DD") from exc


def _parse_bundle_filter() -> list[int] | None:
    raw = os.getenv("BUNDLE_BACKFILL_BUNDLES", "").strip()
    if not raw:
        return None

    values = []
    for part in raw.split(","):
        cleaned = part.strip()
        if not cleaned:
            continue
        values.append(int(cleaned))
    return values or None


def _find_target_brand_index() -> int | None:
    for brand_index in active_brand_indices:
        brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", "")
        brand_key = os.environ.get(f"BRAND_TAG_{brand_index}", "")
        shop_name = os.environ.get(f"SHOP_NAME_{brand_index}", "")
        if is_bundle_rollup_brand(brand_name, brand_key, shop_name):
            return brand_index
    return None


def run_backfill() -> None:
    start_date = _parse_date_env("BUNDLE_BACKFILL_START_DATE")
    end_date = _parse_date_env("BUNDLE_BACKFILL_END_DATE")
    if start_date > end_date:
        raise ValueError("BUNDLE_BACKFILL_START_DATE cannot be after BUNDLE_BACKFILL_END_DATE")

    bundle_ids = _parse_bundle_filter()
    initialize_brand_configs()

    if not active_brand_indices:
        logger.warning("No active brands found based on current configuration.")
        return

    brand_index = _find_target_brand_index()
    if brand_index is None:
        logger.warning("No TMC bundle-rollup brand found in active brand configuration.")
        return

    brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
    shop_name = os.environ.get(f"SHOP_NAME_{brand_index}")
    access_token = os.environ.get(f"ACCESS_TOKEN_{brand_index}")
    api_version = os.environ.get(f"API_VERSION_{brand_index}", "2025-07")
    if not shop_name or not access_token:
        raise ValueError(f"Missing Shopify credentials for {brand_name}")

    target_dates = build_target_dates(start_date, end_date)

    logger.info(
        "\n%s\nBUNDLE ROLLUP BACKFILL: %s -> %s%s\n%s",
        "=" * 60,
        start_date,
        end_date,
        f" | bundles={bundle_ids}" if bundle_ids else "",
        "=" * 60,
    )

    with get_db_cursor(brand_index) as (cursor, connection):
        daily_count, product_count = execute_bundle_rollups_incremental(
            cursor=cursor,
            connection=connection,
            brand_name=brand_name,
            target_dates=target_dates,
            shop_name=shop_name,
            access_token=access_token,
            api_version=api_version,
            bundle_ids=bundle_ids,
        )

    logger.info(
        "Bundle backfill complete for %s: daily_rows=%s, product_rows=%s",
        brand_name,
        daily_count,
        product_count,
    )


if __name__ == "__main__":
    try:
        run_backfill()
    except Exception as exc:
        logger.error("Bundle rollup backfill failed: %s", exc)
        traceback.print_exc()
        sys.exit(1)
