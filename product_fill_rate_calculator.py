# -*- coding: utf-8 -*-
"""
Standalone script to calculate fill-rate percentages in product_fill_rate_metrics.
Usage: python product_fill_rate_calculator.py
"""

import logging
import os
import sys
import traceback
from pathlib import Path

from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parent

load_dotenv()
if os.getenv("TEST_MODE", "false").strip().lower() == "true":
    load_dotenv(REPO_ROOT / ".env.local", override=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

try:
    from aws_background import active_brand_indices, get_db_cursor, initialize_brand_configs
    from product_fill_rate import calculate_fill_rate_percentages
except ImportError:
    logger.error("Could not import fill-rate dependencies. Run this script from the repo root.")
    sys.exit(1)


def _parse_target_brand_indices() -> list[int] | None:
    raw = os.getenv("FILL_RATE_BRAND_INDICES", "").strip()
    if not raw:
        return None
    return [int(part.strip()) for part in raw.split(",") if part.strip()]


def run() -> None:
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
        "\n%s\nPRODUCT FILL-RATE CALCULATION STARTED\n%s",
        "=" * 60,
        "=" * 60,
    )

    for brand_index in indices_to_process:
        brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
        with get_db_cursor(brand_index) as (cursor, connection):
            updated = calculate_fill_rate_percentages(
                cursor=cursor,
                connection=connection,
                brand_name=brand_name,
            )
        logger.info("✅ [%s] Fill-rate calculation updated %s row(s)", brand_name, updated)

    logger.info(
        "\n%s\nPRODUCT FILL-RATE CALCULATION COMPLETE\n%s",
        "=" * 60,
        "=" * 60,
    )


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        logger.error("Product fill-rate calculation failed: %s", exc)
        traceback.print_exc()
        sys.exit(1)
