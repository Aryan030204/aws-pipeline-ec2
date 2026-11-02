"""
Focused backfill of shopify_orders.landing_page_path for HISTORICAL orders,
derived from the existing full_url column.

Why it is separate: the live ingestion path (pipeline/shopify_orders.py) now
persists Shopify's landing_site and the normalized landing_page_path for every
new/updated order. Orders loaded before that change have landing_page_path =
NULL. This script fills ONLY rows where landing_page_path IS NULL and full_url
is present, using the SAME Python normalization the live ingestion uses
(pipeline/landing_path.py, incl. percent-decoding).

What it does NOT do: it makes NO Shopify API calls, so historical landing_site
values (which were never stored) cannot be recovered here. Recovering those
needs the normal orders backfill mode, which re-fetches a window from Shopify
and now stores landing_site.

Safety: dry-run by default (counts only). Pass --apply to write. Updates are
batched per created_date and order_id (index-friendly); no other column is touched.
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

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

from pipeline.landing_path import is_collection_landing_path, normalize_landing_path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [LANDING-PATH-BACKFILL] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")


def _parse_ist_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


def _dates_between(start_dt: datetime, end_dt: datetime):
    current = start_dt.date()
    last = end_dt.date()
    while current <= last:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def backfill_brand(brand_index: int, dates, apply: bool):
    brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")

    if apply:
        aws_background.ensure_landing_page_columns(brand_index, "shopify_orders")

    total_candidates = 0
    total_derivable = 0
    total_collection = 0
    total_updated = 0
    with aws_background.get_db_cursor(brand_index) as (cursor, connection):
        cursor.execute("SHOW COLUMNS FROM shopify_orders LIKE 'landing_page_path'")
        has_column = bool(cursor.fetchall())
        null_filter = "AND landing_page_path IS NULL" if has_column else ""

        for created_date in dates:
            # One row per order (line-item rows of an order share full_url).
            cursor.execute(
                f"""
                SELECT order_id, MAX(full_url) AS full_url
                FROM shopify_orders
                WHERE created_date = %s
                  AND full_url IS NOT NULL AND TRIM(full_url) <> ''
                  {null_filter}
                GROUP BY order_id
                """,
                (created_date,),
            )
            updates = []
            for row in cursor.fetchall():
                order_id = row["order_id"] if isinstance(row, dict) else row[0]
                full_url = row["full_url"] if isinstance(row, dict) else row[1]
                total_candidates += 1
                path = normalize_landing_path(full_url)
                if path:
                    total_derivable += 1
                    total_collection += is_collection_landing_path(path)
                    updates.append((path, created_date, order_id))

            if apply and has_column:
                for i in range(0, len(updates), 1000):
                    cursor.executemany(
                        """
                        UPDATE shopify_orders
                        SET landing_page_path = %s
                        WHERE created_date = %s AND order_id = %s
                          AND landing_page_path IS NULL
                        """,
                        updates[i : i + 1000],
                    )
                    total_updated += cursor.rowcount
                    connection.commit()

    mode = "APPLIED" if apply else "DRY-RUN"
    logger.info(
        f"[{brand_name}] {mode}: {total_candidates} order(s) with full_url and no landing_page_path "
        f"across {len(dates)} date(s); {total_derivable} derivable; "
        f"{total_collection} exact-collection landings"
        + (f"; {total_updated} row(s) updated" if apply else "")
    )


def main():
    parser = argparse.ArgumentParser(
        description="SQL-only backfill of shopify_orders.landing_page_path from full_url (no Shopify calls)"
    )
    parser.add_argument("--start", type=str, help="Start datetime in IST (YYYY-MM-DDTHH:mm:ss). Defaults to BACKFILL_START_IST.")
    parser.add_argument("--end", type=str, help="End datetime in IST (YYYY-MM-DDTHH:mm:ss). Defaults to BACKFILL_END_IST.")
    parser.add_argument("--brand-index", type=int, help="Specific brand index. Defaults to BACKFILL_BRAND_INDICES or all active brands.")
    parser.add_argument("--apply", action="store_true", help="Write the updates. Without it the script only counts (dry-run).")
    args = parser.parse_args()

    aws_background.initialize_brand_configs()

    start_dt = _parse_ist_dt(args.start) if args.start else aws_background.BACKFILL_START_IST
    end_dt = _parse_ist_dt(args.end) if args.end else aws_background.BACKFILL_END_IST
    if not start_dt or not end_dt:
        logger.error("Missing window. Provide --start/--end or set BACKFILL_START_IST/BACKFILL_END_IST.")
        sys.exit(1)
    if start_dt > end_dt:
        logger.error(f"Invalid window: start {start_dt} is after end {end_dt}")
        sys.exit(1)

    if args.brand_index is not None:
        target_indices = [args.brand_index]
    elif aws_background.BACKFILL_BRAND_INDICES:
        target_indices = [i for i in aws_background.active_brand_indices if i in aws_background.BACKFILL_BRAND_INDICES]
    else:
        target_indices = aws_background.active_brand_indices
    if not target_indices:
        logger.error("No active brands available.")
        sys.exit(1)

    dates = list(_dates_between(start_dt, end_dt))
    logger.info(f"{'APPLY' if args.apply else 'DRY-RUN'} landing_page_path backfill: {len(target_indices)} brand(s), {len(dates)} date(s) ({dates[0]}..{dates[-1]})")
    for brand_index in target_indices:
        try:
            backfill_brand(brand_index, dates, args.apply)
        except Exception as e:
            logger.exception(f"Brand {brand_index} failed: {e}")


if __name__ == "__main__":
    main()
