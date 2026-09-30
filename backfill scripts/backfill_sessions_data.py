"""
Full sessions + ATC sessions backfill: overwrites every sessions/ATC-sessions
table - overall daily (sessions_summary), overall hourly
(hourly_sessions_summary + the session columns of hourly_sessions_summary_shopify),
city-wise daily (daily_city_sessions_summary_shopify), city-wise hourly
(hourly_city_sessions_summary_shopify) - for a date window, across all
active brands or one specific brand. Also recomputes the downstream tables
that blend sessions with order data for the same dates (overall_summary,
daily_citywise_summary, hourly_citywise_summary, hour_wise_sales), so those
stay consistent with the freshly overwritten session numbers.

Calls the four ShopifyQL session syncs (pipeline/shopifyql_sessions.py)
directly rather than going through pipeline/orchestration.py's
update_sessions_summary wrapper, because that wrapper deliberately SKIPS all
four whenever BACKFILL_MODE is active (see its docstring / the intent event
worker's session-skip logic) - this script's entire purpose is the opposite:
backfill sessions on purpose. Orders/sales are NOT touched here at all - use
backfill_orders_sales_data.py for that. DB schema migrations are NOT run
either, for the same reason they're skipped in the orders/sales script: a
backfill run is not the place for schema changes.

Overwrite, not addon: before re-fetching, this DELETEs the requested window
from every table that holds ONLY session data (sessions_summary,
hourly_sessions_summary, daily_city_sessions_summary_shopify,
hourly_city_sessions_summary_shopify), and zeroes just the session-related
columns of hourly_sessions_summary_shopify (which also holds
ci_events/buy_now_events from a different sync - only its *_sessions columns
are touched). That way a genuinely-zero-session date/hour in the window ends
up zero, not left with stale nonzero data from a previous run - simply
upserting whatever ShopifyQL returns wouldn't clear those. Every downstream
table this then recomputes is DELETE+INSERT per date
(pipeline/summary_incremental.py's merge_staging_to_main_by_date), so
re-running this script for the same window is idempotent.
"""

import argparse
import logging
import os
import sys
from datetime import timedelta

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SESSIONS-BACKFILL] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _clear_pure_session_tables(cursor, connection, brand_name: str, start_str: str, end_str: str) -> None:
    """Tables holding ONLY session data - safe to DELETE the whole window,
    nothing else lives in these rows."""
    for table in (
        "sessions_summary",
        "hourly_sessions_summary",
        "daily_city_sessions_summary_shopify",
        "hourly_city_sessions_summary_shopify",
    ):
        cursor.execute(f"DELETE FROM {table} WHERE date BETWEEN %s AND %s", (start_str, end_str))
    connection.commit()
    logger.info(f"[{brand_name}] Cleared {start_str}..{end_str} from the pure session tables")


def _zero_shopify_hourly_session_columns(cursor, connection, brand_name: str, start_str: str, end_str: str) -> None:
    """
    hourly_sessions_summary_shopify also holds ci_events/buy_now_events
    (from a different, unrelated sync) - so instead of deleting rows, zero
    only the columns that hold session data. Every device/overall session
    column in this table ends in "_sessions" (number_of_sessions,
    number_of_atc_sessions, mobile_sessions, mobile_atc_sessions, ...) - ci_events
    and buy_now_events don't, so discovering columns by that suffix is a
    reliable, migration-order-independent way to hit exactly the right set.
    """
    try:
        cursor.execute("SHOW COLUMNS FROM hourly_sessions_summary_shopify")
        rows = cursor.fetchall() or []
    except Exception as e:
        logger.warning(
            f"[{brand_name}] Could not read hourly_sessions_summary_shopify columns "
            f"(table may not exist yet) - skipping zero-out: {e}"
        )
        return

    session_columns = [
        (row["Field"] if isinstance(row, dict) else row[0])
        for row in rows
        if (row["Field"] if isinstance(row, dict) else row[0]).endswith("_sessions")
    ]
    if not session_columns:
        return

    set_clause = ", ".join(f"`{c}` = 0" for c in session_columns)
    cursor.execute(
        f"UPDATE hourly_sessions_summary_shopify SET {set_clause} WHERE date BETWEEN %s AND %s",
        (start_str, end_str),
    )
    connection.commit()
    logger.info(
        f"[{brand_name}] Zeroed {len(session_columns)} session column(s) in "
        f"hourly_sessions_summary_shopify for {start_str}..{end_str}"
    )


def backfill_brand(brand_index: int) -> None:
    from pipeline.shopifyql_sessions import (
        update_sessions_summary_from_shopifyql,
        update_hourly_sessions_summary_from_shopifyql,
        update_daily_city_sessions_summary_from_shopifyql,
        update_hourly_city_sessions_summary_from_shopifyql,
    )
    from pipeline.summary_incremental import (
        update_hour_wise_sales_incremental,
        update_daily_citywise_summary_incremental,
        update_hourly_citywise_summary_incremental,
        update_overall_summary_incremental,
    )

    brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
    brand_key = os.environ.get(f"BRAND_TAG_{brand_index}")
    shop_name = os.environ.get(f"SHOP_NAME_{brand_index}")
    api_version = os.environ.get(f"API_VERSION_{brand_index}")
    access_token = os.environ.get(f"ACCESS_TOKEN_{brand_index}")
    tz = aws_background.get_brand_timezone(brand_index)

    start_local, end_local = aws_background.get_backfill_window_for_brand(brand_index)
    if not start_local or not end_local:
        logger.warning(f"[{brand_name}] Missing localized backfill window. Skipping.")
        return
    start_date, end_date = start_local.date(), end_local.date()
    start_str, end_str = start_date.isoformat(), end_date.isoformat()
    target_dates = {
        (start_date + timedelta(days=n)).isoformat()
        for n in range((end_date - start_date).days + 1)
    }

    logger.info(f"--- {brand_name} (index {brand_index}): {start_str}..{end_str} ({tz.key}) ---")

    with aws_background.get_db_cursor(brand_index) as (cursor, connection):
        aws_background.ensure_session_tables(cursor, connection)
        aws_background.ensure_summary_tables(cursor, connection)

        _clear_pure_session_tables(cursor, connection, brand_name, start_str, end_str)
        _zero_shopify_hourly_session_columns(cursor, connection, brand_name, start_str, end_str)

        # Overall daily/hourly: these two only look at aws_background's
        # BACKFILL_* globals internally (no explicit window param), which
        # main() below has already forced on for this whole run.
        update_sessions_summary_from_shopifyql(
            brand_index=brand_index, brand_name=brand_name, shop_name=shop_name,
            api_version=api_version, access_token=access_token, tz=tz,
            cursor=cursor, connection=connection,
        )
        update_hourly_sessions_summary_from_shopifyql(
            brand_index=brand_index, brand_name=brand_name, shop_name=shop_name,
            api_version=api_version, access_token=access_token, tz=tz,
            cursor=cursor, connection=connection,
        )

        # City-wise: these two take the window explicitly.
        update_daily_city_sessions_summary_from_shopifyql(
            brand_index=brand_index, brand_name=brand_name, shop_name=shop_name,
            api_version=api_version, access_token=access_token, tz=tz,
            cursor=cursor, connection=connection,
            start_date=start_date, end_date=end_date,
        )
        update_hourly_city_sessions_summary_from_shopifyql(
            brand_index=brand_index, brand_name=brand_name, shop_name=shop_name,
            api_version=api_version, access_token=access_token, tz=tz,
            cursor=cursor, connection=connection,
            start_date=start_date, end_date=end_date,
        )

        # Recompute the tables that blend sessions with (unchanged) order
        # data for the same dates, so they reflect the freshly overwritten
        # session numbers.
        update_hour_wise_sales_incremental(cursor, connection, brand_name, target_dates)
        update_daily_citywise_summary_incremental(cursor, connection, brand_name, target_dates)
        update_hourly_citywise_summary_incremental(cursor, connection, brand_name, target_dates)
        update_overall_summary_incremental(
            brand_index, cursor, connection, brand_key, brand_name, target_dates
        )

    logger.info(f"[{brand_name}] Sessions backfill complete for {start_str}..{end_str}")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Overwrite-backfill overall + city-wise sessions/ATC sessions (and the "
            "downstream tables that blend them with orders) for a date window - no "
            "orders/sales, no schema migrations (see module docstring)"
        )
    )
    parser.add_argument(
        "--start",
        type=str,
        help="Start datetime (YYYY-MM-DD or YYYY-MM-DDTHH:mm:ss), interpreted in each "
        "brand's own store timezone. Defaults to BACKFILL_START_IST.",
    )
    parser.add_argument(
        "--end",
        type=str,
        help="End datetime, same format. Defaults to BACKFILL_END_IST.",
    )
    parser.add_argument(
        "--brand-index",
        type=int,
        help="Specific brand index to process. Defaults to BACKFILL_BRAND_INDICES or all active brands.",
    )
    args = parser.parse_args()

    start_dt = aws_background._parse_backfill_dt(args.start) if args.start else aws_background.BACKFILL_START_IST
    end_dt = aws_background._parse_backfill_dt(args.end) if args.end else aws_background.BACKFILL_END_IST

    if not start_dt or not end_dt:
        logger.error(
            "Missing backfill window. Provide --start/--end or set BACKFILL_START_IST/BACKFILL_END_IST."
        )
        sys.exit(1)
    if start_dt >= end_dt:
        logger.error(f"Invalid window: start {start_dt} is not before end {end_dt}")
        sys.exit(1)

    # Force backfill mode on for this run - the ShopifyQL overall session
    # syncs only pick up a backfill window via these aws_background globals
    # (no explicit window parameter), and get_backfill_window_for_brand
    # (used for the city-wise calls below) reads them too.
    aws_background.BACKFILL_MODE = True
    aws_background.BACKFILL_START_IST = start_dt
    aws_background.BACKFILL_END_IST = end_dt
    if args.brand_index is not None:
        aws_background.BACKFILL_BRAND_INDICES = {args.brand_index}

    logger.info("Initializing brand configurations and database pools...")
    aws_background.initialize_brand_configs()

    if args.brand_index is not None:
        target_indices = [args.brand_index]
    elif aws_background.BACKFILL_BRAND_INDICES:
        target_indices = [
            idx
            for idx in aws_background.active_brand_indices
            if idx in aws_background.BACKFILL_BRAND_INDICES
        ]
    else:
        target_indices = list(aws_background.active_brand_indices)

    if not target_indices:
        logger.error("No active brands available for sessions backfill.")
        sys.exit(1)

    logger.info(
        f"Starting sessions backfill for {len(target_indices)} brand(s), "
        f"window {start_dt} -> {end_dt} (each brand's own store timezone)"
    )

    for brand_index in target_indices:
        try:
            backfill_brand(brand_index)
        except Exception as e:
            brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
            logger.exception(f"Brand {brand_index} ({brand_name}) failed: {e}")

    logger.info("Sessions backfill completed.")


if __name__ == "__main__":
    main()
