"""
Per-brand pipeline orchestration: session/web-vitals refresh
(update_sessions_summary), the full per-brand order ETL pass
(process_single_brand), the top-level job runner invoked by APScheduler
and manual triggers (run_data_pipeline), and the Flask routes
(/trigger, /health).
"""

import json
import os
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Dict, List, NamedTuple, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import mysql.connector
from flask import jsonify, request

from bundle_rollups import execute_bundle_rollups_incremental, is_bundle_rollup_brand

from pipeline.state import (
    IST,
    logger,
    app,
    scheduler,
    active_brand_indices,
    brand_db_configs,
    http_session,
    _db_context,
    _last_logs_handler,
)
from pipeline.db import (
    get_db_cursor,
    timed,
    log_db_connection_metrics,
    update_last_fetch_timestamp,
)
from pipeline.schema_migrations import ensure_summary_tables, run_brand_preflight_schema_migrations
from pipeline.notifications import (
    _derive_error_code,
    trigger_cache_pipeline,
    trigger_utm_pipeline,
)
from pipeline import mem_probe
from pipeline.metrics_logging import log_pipeline_run_metrics_async
from pipeline.shopify_orders import (
    fetch_orders,
    transform_orders_to_df_optimized,
    load_data_to_sql_optimized,
    convert_to_desired_format,
    backfill_dashboard_order_discount_fact_from_shopify_orders,
    upsert_dashboard_order_discount_fact_from_orders,
    upsert_products_sold_units_daily_from_orders,
    upsert_returns_fact_from_orders,
    get_affected_dates_new_orders,
    get_affected_dates_updates,
    get_rto_affected_dates_updates,
    collect_product_variant_window_units,
    merge_product_variant_units,
    order_window_fetch_bounds,
    order_window_replace,
    order_window_span_hours,
    split_order_window,
)
from pipeline.summary_incremental import (
    execute_summary_queries_incremental,
    execute_discount_dashboard_backfill_only,
    update_daily_citywise_summary_incremental,
    update_hour_wise_sales_incremental,
    update_hourly_citywise_summary_incremental,
    push_web_vitals_metrics_to_qstash,
)
from pipeline.web_vitals import sync_web_vitals_summary
from pipeline.shopifyql_sessions import (
    update_daily_city_sessions_summary_from_shopifyql,
    update_hourly_city_sessions_summary_from_shopifyql,
    update_hourly_sessions_summary_from_shopifyql,
    update_sessions_summary_from_shopifyql,
)
from pipeline.utm_traffic import (
    process_and_upload_referrer_summary,
    process_and_upload_utm_summary,
    refresh_product_traffic_split_for_window,
)

# aws_background still owns the brand_config_init category (backfill-window
# config, discount-dashboard-backfill flags, manager webhook endpoints,
# PIPELINE_AUTH_HEADER/PIPELINE_ID/SELF_RUN, INVENTORY_PIPELINE_SWITCH,
# get_brand_timezone, now_ist) and initialize_brand_configs itself. Imported
# at module level here is safe: attribute access only happens when these
# functions are actually CALLED, during real pipeline execution, long after
# aws_background.py has fully finished loading (see pipeline/shopifyql_sessions.py
# and others for the same pattern).
import aws_background


def _get_last_order_with_cursor(cursor, table_name: str) -> Optional[Dict]:
    order_by_col = "updated_at" if "update" in table_name else "created_at"
    cursor.execute(f"""
        SELECT order_id, created_at, updated_at
        FROM {table_name}
        ORDER BY {order_by_col} DESC
        LIMIT 1
    """)
    return cursor.fetchone()


def get_last_order(brand_index: int, table_name: str, cursor=None) -> Optional[Dict]:
    try:
        if cursor is None:
            with get_db_cursor(brand_index) as (c, _):
                return _get_last_order_with_cursor(c, table_name)
        else:
            return _get_last_order_with_cursor(cursor, table_name)
    except mysql.connector.Error as err:
        logger.error(f"Error getting last order from {table_name}: {err}")
        return None


# ---------------------------
# Sessions summary (logic preserved)
# ---------------------------
def ensure_session_tables(cursor, connection) -> None:
    """
    sessions_summary (overall daily) and hourly_sessions_summary (overall
    hourly) aren't in schema_migrations.py's ensure_summary_tables - they're
    created here instead. Factored out of update_sessions_summary so
    backfill scripts/backfill_sessions_data.py, which calls the ShopifyQL
    session syncs directly (bypassing update_sessions_summary's own
    backfill-mode skip), can ensure the same tables exist without
    duplicating this SQL.
    """
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sessions_summary (
            date DATE PRIMARY KEY,
            number_of_sessions INT DEFAULT 0,
            number_of_atc_sessions INT DEFAULT 0
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hourly_sessions_summary (
            date DATE NOT NULL,
            hour TINYINT UNSIGNED NOT NULL,
            number_of_sessions INT DEFAULT 0,
            number_of_atc_sessions INT DEFAULT 0,
            PRIMARY KEY (date, hour)
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_metadata (
            key_name VARCHAR(50) PRIMARY KEY,
            key_value DATETIME
        );
    """)
    connection.commit()


def update_sessions_summary(
    brand_index: int,
    brand_name: str,
    session_url: str,
    x_brand_name: str,
    x_api_key: str,
    shop_name: str,
    api_version: str,
    access_token: str,
    tz: ZoneInfo,
):
    """
    Mixed mode:
      - NO LONGER calls in-house sessions API.
      - Uses ShopifyQL for both daily and hourly sessions truth.

    Returns {"sessions_synced": int, "atc_sessions_synced": int} - the overall
    (daily) session/ATC-session totals ShopifyQL returned this call, for
    pipeline run-metrics logging (see process_single_brand). 0/0 whenever
    backfill mode skips this brand's sync, or on any error below.
    """
    sessions_synced = 0
    atc_sessions_synced = 0
    try:
        with get_db_cursor(brand_index) as (cursor, connection):
            with timed("Ensure session tables"):
                ensure_session_tables(cursor, connection)

            # --- DAILY & HOURLY: Use ShopifyQL as source of truth ---
            # Skipped during backfill: the overall (not city-wise) sessions/ATC
            # sessions numbers must not be backfilled - only "since last run"
            # incremental updates for these two. City-wise, web vitals,
            # checkout-initiated/buy-now events etc. below are unaffected.
            if aws_background.is_backfill_active_for(brand_index):
                logger.info(
                    f"⏭️ Skipping overall sessions/ATC sessions sync for {brand_name}: "
                    "backfill mode is active"
                )
            else:
                sessions_synced, atc_sessions_synced = update_sessions_summary_from_shopifyql(
                    brand_index=brand_index,
                    brand_name=brand_name,
                    shop_name=shop_name,
                    api_version=api_version,
                    access_token=access_token,
                    tz=tz,
                    cursor=cursor,
                    connection=connection,
                ) or (0, 0)

                update_hourly_sessions_summary_from_shopifyql(
                    brand_index=brand_index,
                    brand_name=brand_name,
                    shop_name=shop_name,
                    api_version=api_version,
                    access_token=access_token,
                    tz=tz,
                    cursor=cursor,
                    connection=connection,
                )

            # Skipped during backfill too - "no sessions/ATC sessions
            # backfill at all" covers city-wise as well as overall.
            if aws_background.is_backfill_active_for(brand_index):
                logger.info(
                    f"⏭️ Skipping city-wise sessions/ATC sessions sync for {brand_name}: "
                    "backfill mode is active"
                )
                city_daily_dates = set()
                city_hourly_dates = set()
            else:
                city_daily_dates = update_daily_city_sessions_summary_from_shopifyql(
                    brand_index=brand_index,
                    brand_name=brand_name,
                    shop_name=shop_name,
                    api_version=api_version,
                    access_token=access_token,
                    tz=tz,
                    cursor=cursor,
                    connection=connection,
                )

                city_hourly_dates = update_hourly_city_sessions_summary_from_shopifyql(
                    brand_index=brand_index,
                    brand_name=brand_name,
                    shop_name=shop_name,
                    api_version=api_version,
                    access_token=access_token,
                    tz=tz,
                    cursor=cursor,
                    connection=connection,
                )

            # Checkout-initiated/buy-now event sync (ci_events, buy_now_events,
            # their variant tables) used to run here, querying alerts.sessions
            # directly every 10 minutes. That's now owned entirely by
            # events-summary-worker (every 15 minutes, reading raw_mongo_events
            # instead) - see pipeline/mongo_events_sql.py.

            web_vitals_start_dt = None
            web_vitals_end_dt = None
            use_web_vitals_watermark = True
            if aws_background.is_backfill_active_for(brand_index):
                backfill_start_dt, backfill_end_dt = aws_background.get_backfill_window_for_brand(brand_index)
                if backfill_start_dt and backfill_end_dt:
                    web_vitals_start_dt = backfill_start_dt
                    web_vitals_end_dt = backfill_end_dt
                    use_web_vitals_watermark = False

            sync_web_vitals_summary(
                brand_index=brand_index,
                brand_name=brand_name,
                brand_key=os.environ.get(f"BRAND_TAG_{brand_index}"),
                cursor=cursor,
                connection=connection,
                start_dt=web_vitals_start_dt,
                end_dt=web_vitals_end_dt,
                use_watermark=use_web_vitals_watermark,
            )

            try:
                push_web_vitals_metrics_to_qstash(
                    brand_index=brand_index,
                    brand_name=brand_name,
                    brand_key=os.environ.get(f"BRAND_TAG_{brand_index}"),
                    cursor=cursor,
                    connection=connection,
                    tz=tz,
                )
            except Exception as e:
                logger.error(
                    f"❌ Error while publishing web vitals QStash event for {brand_name}: {e}"
                )

            update_last_fetch_timestamp(cursor, connection, datetime.now(tz))
            logger.info(f"✅ Sessions updated via ShopifyQL for {brand_name}")

            # --- SYNC: Ensure hour_wise_sales picks up these latest sessions ---
            today_str = datetime.now(tz).date().isoformat()
            update_hour_wise_sales_incremental(
                cursor, connection, brand_name, {today_str}
            )

            city_target_dates = city_daily_dates | city_hourly_dates
            if city_target_dates:
                update_daily_citywise_summary_incremental(
                    cursor, connection, brand_name, city_target_dates
                )
                update_hourly_citywise_summary_incremental(
                    cursor, connection, brand_name, city_target_dates
                )

    except Exception as e:
        logger.error(f"❌ Error updating sessions for {brand_name}: {e}")
        traceback.print_exc()
        return {"sessions_synced": 0, "atc_sessions_synced": 0}

    return {"sessions_synced": sessions_synced, "atc_sessions_synced": atc_sessions_synced}


# ---------------------------
# Per-brand processing
# ---------------------------
def _empty_brand_metrics(brand_index: int, brand_name: str, success: bool = True, error_message: str = "") -> Dict[str, Any]:
    """Baseline shape for process_single_brand's return value - every exit
    path (including the early-return ones below) returns this shape, not
    None, so the run-metrics logging in _execute_pipeline_run always has a
    row to log per brand instead of silently skipping one."""
    return {
        "brand_name": brand_name,
        "brand_index": brand_index,
        "new_orders_count": 0,
        "updated_orders_count": 0,
        "affected_dates_count": 0,
        "sessions_synced": 0,
        "atc_sessions_synced": 0,
        "success": success,
        "error_message": error_message,
        "duration_seconds": 0.0,
    }


class _WindowResult(NamedTuple):
    complete: bool  # False: the Shopify fetch ended early; nothing of this window was loaded
    loaded: bool  # False: the SQL load failed (and was only logged)
    orders: int
    affected_dates: Set[str]
    rto_affected_dates: Set[str]
    product_units: Any  # (pair_window_qty, product_window_qty) or None


class _OrderStreamTotals(NamedTuple):
    orders: int
    affected_dates: Set[str]
    rto_affected_dates: Set[str]


_NO_RESULT = _WindowResult(True, True, 0, set(), set(), None)


def _fetch_and_load_window(
    process: Dict[str, str],
    window,
    replace_window,
    *,
    brand_index: int,
    brand_name: str,
    tz,
    api_base_url: str,
    access_token: str,
    app_id_mapping: Dict,
    cursor,
    connection,
    label: str,
) -> _WindowResult:
    """Fetch ONE sub-window, load it, run its per-order side effects, and return only small aggregates.

    The whole list of orders (and the transform frame) are locals of this call: they are released when it returns,
    BEFORE the next sub-window is fetched, so a run's memory is bounded by one window, not by the whole date range."""
    min_str, max_str = order_window_fetch_bounds(window, tz)
    status: Dict[str, Any] = {}
    # A backfill replaces its whole window, so it must never be fed a partial fetch: strict mode raises (and this
    # brand's run fails, nothing deleted) instead of returning what it managed to get.
    orders_list = fetch_orders(
        api_base_url,
        access_token,
        min_str,
        max_str,
        process["date_field"],
        strict=aws_background.is_backfill_active_for(brand_index),
        status=status,
    )
    if status.get("complete") is False:
        # The incremental watermark is MAX(created_at / updated_at) of the table, so the pages of an incomplete window
        # must not be loaded: they could move it past orders that were never fetched. Dropping them keeps the watermark
        # at the end of the last complete window, and the next run retries from there.
        return _WindowResult(False, True, 0, set(), set(), None)

    if not orders_list:
        logger.info(f"No new {process['type']} orders to process for {brand_name} ({label})")
        return _NO_RESULT

    existing_ids = set()
    original_count = len(orders_list)
    filtered = (
        [o for o in orders_list if str(o.get("id")) not in existing_ids]
        if existing_ids
        else orders_list
    )
    if not filtered:
        logger.info(
            f"Fetched {original_count} orders, all duplicates. Nothing to insert."
        )
        return _NO_RESULT

    logger.info(
        f"Fetched {original_count}, removed {original_count - len(filtered)} duplicates. "
        f"Processing {len(filtered)} orders ({label})."
    )
    orders_in_window = len(filtered)
    affected_dates: Set[str] = set()
    rto_affected_dates: Set[str] = set()
    product_units = None

    # Track affected dates for NEW orders before load
    if process["type"] == "NEW":
        new_dates = get_affected_dates_new_orders(filtered, tz)
        if new_dates:
            affected_dates.update(new_dates)
            rto_new_dates = get_rto_affected_dates_updates(filtered, tz)
            if rto_new_dates:
                rto_affected_dates.update(rto_new_dates)
            logger.info(
                f"📅 Affected dates for {process['type']}: "
                f"{len(new_dates)} date(s) ({min(new_dates)}..{max(new_dates)})"
            )

    df = transform_orders_to_df_optimized(filtered, app_id_mapping, tz)
    rows_in_window = len(df)

    # Load immediately for this window
    batch_size = int(os.environ.get("BATCH_SIZE", 1000))
    loaded = load_data_to_sql_optimized(
        df,
        brand_index,
        brand_name,
        process["table"],
        batch_size,
        replace_window=replace_window,
    )

    if process["type"] == "NEW":
        discount_fact_dates = upsert_dashboard_order_discount_fact_from_orders(
            filtered, cursor, connection, brand_name, tz
        )
        if discount_fact_dates:
            affected_dates.update(discount_fact_dates)

    # Upsert returns_fact from the same filtered snapshots
    changed_return_dates = upsert_returns_fact_from_orders(
        brand_index, filtered, cursor=cursor, connection=connection
    )

    if process["type"] == "NEW" and aws_background.INVENTORY_PIPELINE_SWITCH:
        # Aggregated per window, merged across windows by the caller and applied ONCE (the update is additive).
        product_units = collect_product_variant_window_units(filtered, tz)

    # UPDATED orders: build affected dates from updated_at + returns_fact deltas.
    if process["type"] == "UPDATED":
        upd_dates = get_affected_dates_updates(
            filtered,
            tz,
            extra_event_dates=changed_return_dates,
        )
        rto_upd_dates = get_rto_affected_dates_updates(filtered, tz)
        if rto_upd_dates:
            rto_affected_dates.update(rto_upd_dates)
        if upd_dates:
            affected_dates.update(upd_dates)
            logger.info(
                f"📅 Affected dates for {process['type']}: "
                f"{len(upd_dates)} date(s) ({min(upd_dates)}..{max(upd_dates)})"
            )
        else:
            logger.info(
                "✔️ UPDATED orders produced no new/changed affected dates"
            )
        if rto_upd_dates:
            logger.info(
                f"[{brand_name}] RTO affected created_date(s) from UPDATED orders: "
                f"{len(rto_upd_dates)} ({min(rto_upd_dates)}..{max(rto_upd_dates)})"
            )

    mem_probe.log_mem(
        f"order_window_{process['type'].lower()}", brand_index, orders=orders_in_window, rows=rows_in_window
    )
    return _WindowResult(
        True, loaded is not False, orders_in_window, affected_dates, rto_affected_dates, product_units
    )


def _process_order_stream(
    process: Dict[str, str],
    *,
    brand_index: int,
    brand_name: str,
    tz,
    api_base_url: str,
    access_token: str,
    shop_name: str,
    app_id_mapping: Dict,
    cursor,
    connection,
    errors: List[str],
) -> Optional[_OrderStreamTotals]:
    """Process one order stream (NEW -> shopify_orders, UPDATED -> shopify_orders_update) for a brand.

    The overall fetch window is the same as ever (the BACKFILL_* window, or `watermark + 1s .. now`), but it is walked in
    ascending sub-windows of ORDER_FETCH_WINDOW_HOURS (default 24h; a shorter window, i.e. a normal 10-minute tick,
    stays ONE request). Each sub-window is fetched, loaded (a backfill replaces exactly that sub-window in the same
    transaction as its insert) and released before the next one, so memory is bounded by one window.

    Returns None when the stream has nothing to do (no watermark / no backfill window), else the aggregates."""
    timestamp_col = "created_at" if process["type"] == "NEW" else "updated_at"
    last_order = get_last_order(brand_index, process["table"], cursor=cursor)
    backfill = aws_background.is_backfill_active_for(brand_index)

    if backfill:
        # Use the configured backfill window in the brand's store timezone.
        start_dt, end_dt = aws_background.get_backfill_window_for_brand(brand_index)
        if not start_dt or not end_dt:
            logger.warning(
                f"🛑 Missing localized backfill window for {brand_name}. Skipping."
            )
            return None
        logger.info(
            f"🔁 BACKFILL MODE: {process['type']} from {start_dt} to {end_dt} "
            f"({tz.key})"
        )
        window_start, window_end = start_dt, end_dt
    else:
        if not last_order:
            logger.warning(
                f"🛑 No initial data in '{process['table']}' for {brand_name}. Skipping."
            )
            return None
        last_ts = last_order[timestamp_col]
        if last_ts is None:
            logger.warning(
                f"🛑 Last {timestamp_col} watermark is missing for {brand_name}. Skipping."
            )
            return None
        # Advance by one second because the Shopify start boundary is inclusive.
        next_ts = last_ts + timedelta(seconds=1)
        window_start = next_ts.replace(tzinfo=tz) if next_ts.tzinfo is None else next_ts.astimezone(tz)
        window_end = datetime.now(tz)
        logger.info(f"Fetching orders after: {last_ts} (API start: {next_ts})")

    tiles = split_order_window(window_start, window_end, order_window_span_hours())
    total_windows = len(tiles)
    if total_windows > 1:
        logger.info(
            f"📦 {brand_name}: {process['type']} fetch window split into {total_windows} sub-window(s) "
            f"of up to {order_window_span_hours():g}h to bound memory"
        )

    total_orders = 0
    affected_dates: Set[str] = set()
    rto_affected_dates: Set[str] = set()
    product_units = None

    for number, window in enumerate(tiles, start=1):
        replace_window = (
            order_window_replace(window, process["table"], window_end, is_last=(number == total_windows))
            if backfill
            else None
        )
        label = f"window {number}/{total_windows}"
        result = _fetch_and_load_window(
            process,
            window,
            replace_window,
            brand_index=brand_index,
            brand_name=brand_name,
            tz=tz,
            api_base_url=api_base_url,
            access_token=access_token,
            app_id_mapping=app_id_mapping,
            cursor=cursor,
            connection=connection,
            label=label,
        )
        if not result.complete:
            message = (
                f"{process['type']} orders: Shopify fetch incomplete at {label}; nothing was loaded for it and the "
                "remaining windows were not fetched (the next run resumes from the last loaded order)"
            )
            logger.error(f"[{brand_name}] {message}")
            errors.append(message)
            break
        total_orders += result.orders
        affected_dates.update(result.affected_dates)
        rto_affected_dates.update(result.rto_affected_dates)
        if result.product_units is not None:
            if product_units is None:
                product_units = ({}, {})
            merge_product_variant_units(product_units, result.product_units)
        if not result.loaded:
            # The SQL load failed (logged). Do not fetch/load LATER windows: the watermark would move past this one.
            message = f"{process['type']} orders: load failed at {label}; later windows were not processed"
            logger.error(f"[{brand_name}] {message}")
            errors.append(message)
            break

    if product_units is not None:
        upsert_products_sold_units_daily_from_orders(
            brand_index=brand_index,
            orders_list=[],
            brand_name=brand_name,
            shop_name=shop_name,
            access_token=access_token,
            cursor=cursor,
            connection=connection,
            precomputed_units=product_units,
        )

    mem_probe.log_mem(f"order_stream_{process['type'].lower()}", brand_index, orders=total_orders, windows=total_windows)
    return _OrderStreamTotals(total_orders, affected_dates, rto_affected_dates)


def process_single_brand(brand_index: int, job_id: Optional[str] = None) -> Dict[str, Any]:
    brand_started_at = time.monotonic()
    if brand_index not in brand_db_configs:
        logger.error(f"❌ Skipping brand {brand_index}: no database config")
        return _empty_brand_metrics(
            brand_index, f"Brand_{brand_index}", success=False, error_message="no database config"
        )
    _db_context.job_id = job_id
    brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
    brand_key = os.environ.get(
        f"BRAND_TAG_{brand_index}"
    )  # used for session_adjustment_buckets & alerts
    # Sub-phase failures below are individually caught and logged (a bundle-
    # rollup or UTM hiccup shouldn't abort the rest of the brand's run) but
    # are collected here so they still show up in this brand's run-metrics
    # row instead of being invisible outside the container logs.
    errors: List[str] = []

    logger.info(f"\n{'=' * 50}\nSTARTING PROCESS FOR SHOP: {brand_name}\n{'=' * 50}")
    log_db_connection_metrics(f"DB_CONNECTION_METRICS_BRAND_START brand={brand_name}")

    if aws_background.DISCOUNT_DASHBOARD_ONLY_MODE:
        execute_discount_dashboard_backfill_only(brand_index, brand_name)
        log_db_connection_metrics(f"DB_CONNECTION_METRICS_BRAND_COMPLETE brand={brand_name}")
        _db_context.job_id = None
        metrics = _empty_brand_metrics(brand_index, brand_name)
        metrics["duration_seconds"] = round(time.monotonic() - brand_started_at, 2)
        return metrics

    tz = aws_background.get_brand_timezone(brand_index)
    shop_name = os.environ.get(f"SHOP_NAME_{brand_index}")
    api_version = os.environ.get(f"API_VERSION_{brand_index}")
    access_token = os.environ.get(f"ACCESS_TOKEN_{brand_index}")
    session_url = os.environ.get(f"SESSION_URL_{brand_index}", "")
    x_brand_name = ""
    x_api_key = ""
    api_base_url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}"

    # mapping
    app_id_mapping_str = os.environ.get(f"APP_ID_MAPPING_{brand_index}", "{}")
    try:
        app_id_mapping = json.loads(app_id_mapping_str)
    except json.JSONDecodeError:
        logger.warning(
            f"⚠️ Invalid JSON in APP_ID_MAPPING_{brand_index} for {brand_name}. Using empty mapping."
        )
        app_id_mapping = {}
    # >>> Add these logs right here <<<
    logger.info(
        f"APP_ID_MAPPING_{brand_index} for {brand_name}: "
        f"{'EMPTY' if not app_id_mapping else list(app_id_mapping.keys())}"
    )
    # Optional: also log a short hash/length to catch truncation/mis-set envs
    logger.info(f"APP_ID_MAPPING_{brand_index} length={len(app_id_mapping_str)}")

    # Sessions
    session_metrics = update_sessions_summary(
        brand_index,
        brand_name,
        session_url,
        x_brand_name,
        x_api_key,
        shop_name,
        api_version,
        access_token,
        tz,
    ) or {}
    sessions_synced = session_metrics.get("sessions_synced", 0)
    atc_sessions_synced = session_metrics.get("atc_sessions_synced", 0)

    # Track affected dates across all order processing (sparse set, not a range)
    affected_dates: Set[str] = set()
    rto_affected_dates: Set[str] = set()
    new_orders_count = 0
    updated_orders_count = 0

    # Orders (NEW + UPDATED) with single connection/cursor reused
    process_types = [
        {"type": "NEW", "date_field": "created_at_min", "table": "shopify_orders"},
        {
            "type": "UPDATED",
            "date_field": "updated_at_min",
            "table": "shopify_orders_update",
        },
    ]

    with get_db_cursor(brand_index) as (cursor, connection):
        ensure_summary_tables(cursor, connection, brand_index=brand_index)

        for process in process_types:
            logger.info(
                f"\n--- Processing {process['type']} orders for {brand_name} ---"
            )
            totals = _process_order_stream(
                process,
                brand_index=brand_index,
                brand_name=brand_name,
                tz=tz,
                api_base_url=api_base_url,
                access_token=access_token,
                shop_name=shop_name,
                app_id_mapping=app_id_mapping,
                cursor=cursor,
                connection=connection,
                errors=errors,
            )
            if totals is None:
                continue
            if process["type"] == "NEW":
                new_orders_count = totals.orders
            else:
                updated_orders_count = totals.orders
            affected_dates.update(totals.affected_dates)
            rto_affected_dates.update(totals.rto_affected_dates)

        if aws_background.DISCOUNT_DASHBOARD_BACKFILL:
            logger.info(f"🔁 Starting discount dashboard backfill for {brand_name}")
            discount_backfill_dates = (
                backfill_dashboard_order_discount_fact_from_shopify_orders(
                    cursor, connection, brand_name
                )
            )
            if discount_backfill_dates:
                affected_dates.update(discount_backfill_dates)
                logger.info(
                    f"📅 Discount dashboard backfill affected "
                    f"{len(discount_backfill_dates)} date(s) "
                    f"({min(discount_backfill_dates)}..{max(discount_backfill_dates)})"
                )

    # Record completion
    with timed("Record pipeline completion timestamp"):
        with get_db_cursor(brand_index) as (cursor, connection):
            cursor.execute(
                """
                INSERT INTO pipeline_metadata (key_name, key_value)
                VALUES ('last_pipeline_completion_time', %s)
                ON DUPLICATE KEY UPDATE key_value = VALUES(key_value);
            """,
                (aws_background.now_ist(),),
            )
            connection.commit()
            logger.info(f"✅ Recorded pipeline completion for {brand_name}")

    # Summaries (INCREMENTAL - only affected dates, sparse)
    if affected_dates or rto_affected_dates:
        ordered = sorted(affected_dates)
        rto_ordered = sorted(rto_affected_dates)
        logger.info(
            f"\n--- Updating summaries for {brand_name} "
            f"(INCREMENTAL: "
            f"{len(ordered)} standard date(s)"
            f"{f', {ordered[0]}..{ordered[-1]}' if ordered else ''}; "
            f"{len(rto_ordered)} rto date(s)"
            f"{f', {rto_ordered[0]}..{rto_ordered[-1]}' if rto_ordered else ''}) ---"
        )
        execute_summary_queries_incremental(
            brand_index,
            brand_name,
            brand_key,
            affected_dates,
            rto_target_dates=rto_affected_dates,
        )
    else:
        logger.info(
            f"✔️ No order-based summary updates needed for {brand_name} (no affected dates)"
        )

    if is_bundle_rollup_brand(brand_name, brand_key, shop_name):
        try:
            with get_db_cursor(brand_index) as (cursor, connection):
                execute_bundle_rollups_incremental(
                    cursor=cursor,
                    connection=connection,
                    brand_name=brand_name,
                    target_dates=affected_dates,
                    shop_name=shop_name,
                    access_token=access_token,
                    api_version=api_version,
                )
        except Exception as e:
            logger.error(f"Bundle rollup refresh failed for {brand_name}: {e}")
            traceback.print_exc()
            errors.append(f"bundle rollups: {e}")

    # --- ShopifyQL Summaries (Always updated for 'today' or affected range) ---
    try:
        today_iso = aws_background.now_ist().date().isoformat()
        u_min = min(affected_dates) if affected_dates else today_iso
        u_max = max(affected_dates) if affected_dates else today_iso

        if aws_background.is_backfill_active_for(brand_index):
            backfill_start_dt, backfill_end_dt = aws_background.get_backfill_window_for_brand(brand_index)
            if backfill_start_dt and backfill_end_dt:
                u_min = backfill_start_dt.date().isoformat()
                u_max = backfill_end_dt.date().isoformat()

        logger.info(
            f"Refreshing UTM, product traffic split, and Referrer summaries for {brand_name} ({u_min} to {u_max})"
        )

        # 1. UTM
        try:
            process_and_upload_utm_summary(
                brand_index, brand_name, shop_name, access_token, str(u_min), str(u_max)
            )
        except Exception as e:
            logger.error(f"❌ Error updating UTM summary for {brand_name}: {e}")
            errors.append(f"UTM summary: {e}")

        # 2. Product traffic split
        try:
            refresh_product_traffic_split_for_window(
                brand_index,
                brand_name,
                str(u_min),
                str(u_max),
            )
        except Exception as e:
            logger.error(
                f"Error updating product traffic split for {brand_name}: {e}"
            )
            errors.append(f"product traffic split: {e}")

        # 3. Referrer
        try:
            process_and_upload_referrer_summary(
                brand_index, brand_name, shop_name, access_token, str(u_min), str(u_max)
            )
        except Exception as e:
            logger.error(f"❌ Error updating Referrer summary for {brand_name}: {e}")
            errors.append(f"referrer summary: {e}")

    except Exception as e:
        logger.error(
            f"❌ Critical error in ShopifyQL summary block for {brand_name}: {e}"
        )
        traceback.print_exc()
        errors.append(f"ShopifyQL summary block: {e}")

    logger.info(f"✅ COMPLETED PROCESSING FOR {brand_name}")
    log_db_connection_metrics(f"DB_CONNECTION_METRICS_BRAND_COMPLETE brand={brand_name}")
    _db_context.job_id = None

    metrics = _empty_brand_metrics(brand_index, brand_name)
    metrics.update(
        {
            "new_orders_count": new_orders_count,
            "updated_orders_count": updated_orders_count,
            "affected_dates_count": len(affected_dates),
            "sessions_synced": sessions_synced,
            "atc_sessions_synced": atc_sessions_synced,
            "success": len(errors) == 0,
            "error_message": "; ".join(errors),
            "duration_seconds": round(time.monotonic() - brand_started_at, 2),
        }
    )
    return metrics


# ---------------------------
# Job runner
# ---------------------------
def _parse_brands_cron_map() -> Dict[int, str]:
    """
    BRANDS_CRON_MAP (JSON {"<brand_index>": "<minute-list>"}) lets some
    brands run on their own hourly cadence instead of the shared every-10-min
    job - e.g. {"3": "15,45", "1": "0,30"} runs brand 3 at :15/:45 and brand 1
    at :00/:30. Values are minute-lists only, same shape as the legacy job's
    own minute= kwarg (hourly, no hour/day fields). Same tolerant-JSON
    pattern as pipeline/intent_events.py's _parse_intent_db_map: invalid
    JSON, a non-dict, or a non-numeric key is logged and skipped rather than
    raising - a brand not present here (including an empty/unset map) simply
    falls through to the legacy fallback job (see run_data_pipeline below),
    so this is opt-in per brand and fully backward compatible.
    """
    raw = os.environ.get("BRANDS_CRON_MAP", "{}")
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        logger.warning("Invalid JSON in BRANDS_CRON_MAP. Using empty mapping.")
        return {}

    if not isinstance(parsed, dict):
        logger.warning("BRANDS_CRON_MAP must be a JSON object. Using empty mapping.")
        return {}

    result: Dict[int, str] = {}
    for key, value in parsed.items():
        try:
            brand_index = int(str(key).strip())
        except (TypeError, ValueError):
            logger.warning(f"Ignoring non-numeric BRANDS_CRON_MAP key: {key!r}")
            continue
        result[brand_index] = str(value).strip()
    return result


def _group_brands_by_cron(cron_map: Dict[int, str]) -> Dict[str, List[int]]:
    """cron-minute-value -> sorted brand indices sharing it, for registering
    one scheduler job per distinct cadence."""
    groups: Dict[str, List[int]] = {}
    for brand_index, cron_value in cron_map.items():
        groups.setdefault(cron_value, []).append(brand_index)
    return {cron_value: sorted(indices) for cron_value, indices in groups.items()}


DEFAULT_MAX_CONCURRENT_BRANDS = 3
DEFAULT_BACKFILL_MAX_CONCURRENT_BRANDS = 1


def resolve_brand_concurrency(n_brands: int, backfill_active: bool, env=None) -> Tuple[int, str]:
    """(worker threads, name of the env var that decided it). Normal runs use MAX_CONCURRENT_BRANDS (default 3).
    A backfill run uses BACKFILL_MAX_CONCURRENT_BRANDS (default 1): each brand thread holds its own sub-window of orders
    plus an insert chunk, so peak memory is roughly (threads x per-thread peak), and a backfill is a one-shot job where
    throughput matters less. Always between 1 and the number of brands."""
    source = os.environ if env is None else env
    if backfill_active:
        name, default = "BACKFILL_MAX_CONCURRENT_BRANDS", DEFAULT_BACKFILL_MAX_CONCURRENT_BRANDS
    else:
        name, default = "MAX_CONCURRENT_BRANDS", DEFAULT_MAX_CONCURRENT_BRANDS
    try:
        wanted = int(str(source.get(name, default)).strip())
    except ValueError:
        logger.warning(f"Invalid {name}={source.get(name)!r}; using {default}")
        wanted = default
    return max(1, min(n_brands, wanted)), name


def _execute_pipeline_run(target_indices: List[int], run_id: Optional[str] = None) -> None:
    """
    The actual ETL run: preflight migrations, per-brand ThreadPoolExecutor,
    cache/UTM triggers, and (when run_id is given - only manual/dashboard
    triggers pass one) the STARTED/COMPLETED/FAILED webhook calls. Shared by
    every caller - the legacy fallback job (run_data_pipeline), each
    per-cron-group job (_run_brand_group), and manual /trigger - so
    BACKFILL_MODE/BACKFILL_BRAND_INDICES filtering lives here, once, and
    applies uniformly regardless of who supplied target_indices.
    """
    succeeded = False
    failure_exception: Optional[Exception] = None

    job_start_time = aws_background.now_ist()
    job_id = f"job-{uuid.uuid4().hex[:8]}"
    logger.info(
        f"\n{'=' * 60}\nJOB TRIGGERED AT: {job_start_time.strftime('%Y-%m-%d %I:%M:%S %p')}\nJOB ID: {job_id}\nMANUAL TRIGGER: {bool(run_id)}\n{'=' * 60}"
    )

    if run_id and aws_background.MANAGER_STARTED_ENDPOINT:
        try:
            started_payload = {
                "runId": run_id,
                "pipelineId": aws_background.PIPELINE_ID,
                "status": "running"
            }
            logger.info(f"📤 Sending manual trigger STARTED webhook to {aws_background.MANAGER_STARTED_ENDPOINT}")
            http_session.post(
                aws_background.MANAGER_STARTED_ENDPOINT,
                json=started_payload,
                headers={"x-pipeline-key": aws_background.PIPELINE_AUTH_HEADER},
                timeout=10
            )
        except Exception as e:
            logger.error(f"❌ Failed to send STARTED webhook: {e}")

    try:
        if not target_indices:
            logger.warning("No active brands with valid database configs. Nothing to do.")
            return

        # Filter indices if in backfill mode to avoid processing unrelated brands
        if aws_background.BACKFILL_MODE and aws_background.BACKFILL_BRAND_INDICES:
            target_indices = [
                i for i in target_indices if i in aws_background.BACKFILL_BRAND_INDICES
            ]
            logger.info(
                f"🎯 BACKFILL_MODE is active. Filtering brands to: {target_indices}"
            )

        if not target_indices:
            logger.warning("No active brands match the current filters. Nothing to do.")
            return

        # Also runs in backfill mode: a brand reactivated after a long time
        # can still have old table layouts, and the backfill's first write
        # would otherwise fail with "Unknown column". The checks are
        # idempotent (SHOW COLUMNS, ALTER only what is missing), and in
        # backfill mode target_indices is already filtered to the backfill
        # brands, so only those brands are checked.
        logger.info("🧱 Running serial preflight schema migrations before brand workers...")
        for idx in target_indices:
            try:
                run_brand_preflight_schema_migrations(idx)
            except Exception as e:
                brand_name = os.environ.get(f"BRAND_NAME_{idx}", f"Brand_{idx}")
                logger.error(f"❌ Preflight schema migration failed for {brand_name}: {e}")
                traceback.print_exc()
                raise

        # Cap per-run concurrency to protect RDS I/O. The per-brand pipeline is
        # heavy (multi-table JOINs, summary rebuilds, session upserts); running
        # all brands at once drives EBSByteBalance% to 0. Override with env
        # MAX_CONCURRENT_BRANDS when scaling up the RDS instance.
        backfill_run = bool(aws_background.BACKFILL_MODE)
        max_workers, concurrency_env = resolve_brand_concurrency(len(target_indices), backfill_run)
        logger.info(
            f"Processing {len(target_indices)} brands with {max_workers} parallel workers"
            f" ({concurrency_env}={os.environ.get(concurrency_env, 'default')}"
            f"{', backfill mode' if backfill_run else ''})"
        )

        brand_run_metrics: List[Dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            logger.info(f"🚀 Starting {len(target_indices)} brand worker threads...")
            futures = {
                executor.submit(process_single_brand, i, job_id): i for i in target_indices
            }
            for fut in as_completed(futures):
                idx = futures[fut]
                name = os.environ.get(f"BRAND_NAME_{idx}", f"Brand_{idx}")
                try:
                    result = fut.result()
                    if isinstance(result, dict):
                        brand_run_metrics.append(result)
                    logger.info(f"✅ Successfully completed brand: {name}")
                except Exception as e:
                    logger.error(f"❌ Failed processing brand {name}: {e}")
                    traceback.print_exc()
                    brand_run_metrics.append(
                        _empty_brand_metrics(idx, name, success=False, error_message=str(e))
                    )
                finally:
                    log_db_connection_metrics(
                        f"DB_CONNECTION_METRICS_BRAND_FUTURE_DONE brand={name}"
                    )

        logger.info("🏁 All brand worker threads have returned.")

        end = aws_background.now_ist()
        dur = (end - job_start_time).total_seconds()
        logger.info(
            f"\n{'=' * 60}\nJOB COMPLETED AT: {end.strftime('%Y-%m-%d %I:%M:%S %p')}"
        )
        logger.info(f"Total Duration: {dur:.2f}s ({dur / 60:.2f} minutes)\n{'=' * 60}")

        # Best-effort, never blocks/fails the run - see pipeline/metrics_logging.py.
        try:
            log_pipeline_run_metrics_async(job_id, brand_run_metrics, dur)
        except Exception as e:
            logger.error(f"❌ Failed to schedule run-metrics logging: {e}")

        # Trigger cache pipeline rebuild
        trigger_cache_pipeline()

        # --- NEW: UTM Pipeline API Trigger ---
        trigger_utm_pipeline("ETL_COMPLETED", job_id)

        succeeded = True

    except Exception as e:
        logger.error(f"❌ PIPELINE FAILED with error: {e}")
        traceback.print_exc()
        failure_exception = e
    finally:
        if run_id and succeeded and aws_background.MANAGER_COMPLETED_ENDPOINT:
            try:
                completed_payload = {
                    "runId": run_id,
                    "pipelineId": aws_background.PIPELINE_ID,
                    "status": "completed"
                }
                logger.info(f"📤 Sending manual trigger COMPLETED webhook to {aws_background.MANAGER_COMPLETED_ENDPOINT}")
                http_session.post(
                    aws_background.MANAGER_COMPLETED_ENDPOINT,
                    json=completed_payload,
                    headers={"x-pipeline-key": aws_background.PIPELINE_AUTH_HEADER},
                    timeout=10
                )
            except Exception as e:
                logger.error(f"❌ Failed to send COMPLETED webhook: {e}")

        if run_id and (not succeeded) and aws_background.MANAGER_FAILED_ENDPOINT:
            try:
                recent_logs = _last_logs_handler.get_recent()
                error_body = "\n".join(recent_logs) if recent_logs else "No logs captured."
                error_code = _derive_error_code(failure_exception)
                failed_payload = {
                    "runId": run_id,
                    "pipelineId": aws_background.PIPELINE_ID,
                    "errorCode": error_code,
                    "errorBody": error_body,
                }
                logger.info(f"📤 Sending manual trigger FAILED webhook to {aws_background.MANAGER_FAILED_ENDPOINT}")
                http_session.post(
                    aws_background.MANAGER_FAILED_ENDPOINT,
                    json=failed_payload,
                    headers={"x-pipeline-key": aws_background.PIPELINE_AUTH_HEADER},
                    timeout=10,
                )
            except Exception as e:
                logger.error(f"❌ Failed to send FAILED webhook: {e}")


def run_data_pipeline() -> None:
    """
    The legacy fallback cron job (still id "pipeline_job", still
    minute="0,10,20,30,40,50" every hour - see aws_background.py's
    scheduler registration). Runs every active brand that ISN'T a key in
    BRANDS_CRON_MAP, so a brand with no per-brand cron entry (including
    every brand, when the map is empty/unset) keeps running exactly as
    before - opt-in per brand, fully backward compatible. Note the map is
    only read once at process startup (module import time, via the
    BRANDS_CRON_MAP env var), so changing it requires a restart to take
    effect, same as every other env-driven config in this pipeline.
    """
    mapped_indices = set(_parse_brands_cron_map())
    target_indices = [i for i in active_brand_indices if i not in mapped_indices]
    _execute_pipeline_run(target_indices, run_id=None)


def _run_brand_group(brand_indices: List[int]):
    """
    Builds one no-argument callable per distinct BRANDS_CRON_MAP cron value,
    for scheduler.add_job - brand_indices is that group's fixed brand-index
    list (captured at registration time); at call time it's intersected with
    the live active_brand_indices, so a brand deactivated later is simply
    skipped, same as the legacy job already does implicitly.
    """
    brand_index_set = set(brand_indices)

    def _run() -> None:
        target_indices = [i for i in active_brand_indices if i in brand_index_set]
        _execute_pipeline_run(target_indices, run_id=None)

    return _run


# ---------------------------
# Flask API Endpoints
# ---------------------------
@app.route("/trigger", methods=["POST"])
def manual_trigger():
    """
    Manual trigger endpoint to run the pipeline immediately.
    Expects 'x-pipeline-key' header for authentication and a JSON body with
    'runId' provided by the pipeline manager. Optional 'brandIds' (a list,
    or a single 'brandId') restricts the run to just those brands,
    independent of BRANDS_CRON_MAP grouping - omitted, it runs every active
    brand, same as before this option existed.
    """
    auth_key = request.headers.get("x-pipeline-key")
    if not auth_key or auth_key != aws_background.PIPELINE_AUTH_HEADER:
        logger.warning(f"🚫 Unauthorized trigger attempt from {request.remote_addr}")
        return jsonify({"error": "Unauthorized"}), 401

    logger.info("🚀 Manual trigger received via /trigger endpoint")

    request_body = request.get_json(silent=True) or {}
    run_id = request_body.get("runId")
    if not isinstance(run_id, str) or not run_id.strip():
        logger.warning("Manual trigger rejected: missing or invalid runId")
        return jsonify({"status": "error", "message": "runId is required"}), 400

    run_id = run_id.strip()
    logger.info(f"Manual trigger runId received: {run_id}")

    requested_brand_ids = request_body.get("brandIds")
    if requested_brand_ids is None:
        requested_brand_ids = request_body.get("brandId")
    if requested_brand_ids is not None and not isinstance(requested_brand_ids, list):
        requested_brand_ids = [requested_brand_ids]

    if requested_brand_ids is not None:
        try:
            requested_indices = {int(v) for v in requested_brand_ids}
        except (TypeError, ValueError):
            logger.warning(f"Manual trigger rejected: invalid brandIds {requested_brand_ids!r}")
            return jsonify({"status": "error", "message": "brandIds must be a list of brand indices"}), 400

        active_set = set(active_brand_indices)
        target_indices = sorted(requested_indices & active_set)
        skipped = sorted(requested_indices - active_set)
        if skipped:
            logger.warning(f"Manual trigger: ignoring inactive/unknown brandIds {skipped}")
        if not target_indices:
            logger.warning("Manual trigger rejected: none of the requested brandIds are active")
            return (
                jsonify({"status": "error", "message": "None of the requested brandIds are active"}),
                400,
            )
        logger.info(f"Manual trigger scoped to brandIds: {target_indices}")
    else:
        target_indices = list(active_brand_indices)

    # A dedicated one-off job per manual trigger (rather than reusing the
    # shared cron job id) so run_id/target_indices are bound directly as its
    # arguments - unambiguous even with several per-brand-group jobs
    # registered, unlike the previous shared-queue handoff onto a single
    # "pipeline_job".
    try:
        job_id = f"manual_trigger_{uuid.uuid4().hex[:8]}"
        scheduler.add_job(
            _execute_pipeline_run,
            trigger=None,
            next_run_time=datetime.now(IST),
            args=[target_indices],
            kwargs={"run_id": run_id},
            coalesce=True,
            max_instances=1,
            misfire_grace_time=120,
            replace_existing=True,
            id=job_id,
        )
        return (
            jsonify(
                {"status": "success", "message": "Pipeline triggered immediately", "runId": run_id}
            ),
            200,
        )
    except Exception as e:
        logger.error(f"❌ Error in manual trigger: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "healthy", "timestamp": datetime.now(IST).isoformat()}), 200
