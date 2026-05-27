# -*- coding: utf-8 -*-
"""
Standalone script to rebuild product fill-rate materialization for a limited rolling window.
Usage: python "backfill scripts/product_fill_rate_backfill.py"
"""

import logging
import os
import sys
import traceback
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
    from product_fill_rate import (
        build_fill_rate_backfill_dates,
        calculate_fill_rate_percentages,
        execute_fill_rate_pipeline_incremental,
        fetch_fast_mover_product_ids,
    )
except ImportError:
    logger.error("Could not import product fill-rate dependencies. Run this script from the repo root.")
    sys.exit(1)


def _parse_backfill_range() -> str:
    value = os.getenv("FILL_RATE_BACKFILL_RANGE", "").strip().lower()
    if not value:
        raise ValueError("Missing required environment variable: FILL_RATE_BACKFILL_RANGE")
    if value not in {"7d", "30d", "90d"}:
        raise ValueError("FILL_RATE_BACKFILL_RANGE must be one of: 7d, 30d, 90d")
    return value


def _parse_target_brand_indices() -> list[int] | None:
    raw = os.getenv("FILL_RATE_BACKFILL_BRAND_INDICES", "").strip()
    if not raw:
        return None
    return [int(part.strip()) for part in raw.split(",") if part.strip()]


def run_backfill() -> None:
    range_name = _parse_backfill_range()
    target_dates = build_fill_rate_backfill_dates(range_name)

    initialize_brand_configs()
    if not active_brand_indices:
        logger.warning("No active brands found based on current configuration.")
        return

    target_indices = _parse_target_brand_indices()
    indices_to_process = active_brand_indices
    if target_indices:
        indices_to_process = [idx for idx in active_brand_indices if idx in target_indices]

    if not indices_to_process:
        logger.warning("No matching brands to process after filtering.")
        return

    logger.info(
        "\n%s\nPRODUCT FILL-RATE BACKFILL: range=%s (%s -> %s)\n%s",
        "=" * 60,
        range_name,
        target_dates[0],
        target_dates[-1],
        "=" * 60,
    )

    for brand_index in indices_to_process:
        brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
        with get_db_cursor(brand_index) as (cursor, connection):
            fast_movers = fetch_fast_mover_product_ids(cursor, brand_name)
            daily_rows, metric_rows = execute_fill_rate_pipeline_incremental(
                cursor=cursor,
                connection=connection,
                brand_name=brand_name,
                target_dates=target_dates,
                fast_mover_product_ids=fast_movers,
            )
            calc_rows = calculate_fill_rate_percentages(
                cursor=cursor,
                connection=connection,
                brand_name=brand_name,
                product_ids=fast_movers,
            )

        logger.info(
            "✅ [%s] Fill-rate backfill complete: daily_rows=%s, metric_rows=%s, calc_rows=%s",
            brand_name,
            daily_rows,
            metric_rows,
            calc_rows,
        )


if __name__ == "__main__":
    try:
        run_backfill()
    except Exception as exc:
        logger.error("Product fill-rate backfill failed: %s", exc)
        traceback.print_exc()
        sys.exit(1)
