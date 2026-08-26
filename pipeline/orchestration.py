"""
Per-brand pipeline orchestration: session/web-vitals refresh
(update_sessions_summary), the full per-brand order ETL pass
(process_single_brand), the top-level job runner invoked by APScheduler
and manual triggers (run_data_pipeline), and the Flask routes
(/trigger, /health).
"""

import json
import os
import queue
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Dict, Optional, Set
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
    MANUAL_TRIGGER_QUEUE,
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
    trigger_pipeline_completion_webhook,
    trigger_utm_pipeline,
)
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
)
from pipeline.summary_incremental import (
    execute_summary_queries_incremental,
    execute_discount_dashboard_backfill_only,
    update_daily_citywise_summary_incremental,
    update_hour_wise_sales_incremental,
    update_hourly_citywise_summary_incremental,
    push_web_vitals_metrics_to_qstash,
)
from pipeline.mongo_events import (
    sync_buy_now_events,
    sync_buy_now_variant_events,
    sync_checkout_initiated_ci_events,
    sync_checkout_initiated_variant_events,
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
    """
    try:
        with get_db_cursor(brand_index) as (cursor, connection):
            with timed("Ensure session tables"):
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

            # --- DAILY & HOURLY: Use ShopifyQL as source of truth ---
            update_sessions_summary_from_shopifyql(
                brand_index=brand_index,
                brand_name=brand_name,
                shop_name=shop_name,
                api_version=api_version,
                access_token=access_token,
                tz=tz,
                cursor=cursor,
                connection=connection,
            )

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

            ci_start_dt = datetime.now(tz).date() - timedelta(days=1)
            ci_end_dt = datetime.now(tz).date()
            if aws_background.is_backfill_active_for(brand_index):
                backfill_start_dt, backfill_end_dt = aws_background.get_backfill_window_for_brand(brand_index)
                if backfill_start_dt and backfill_end_dt:
                    ci_start_dt = backfill_start_dt.date()
                    ci_end_dt = backfill_end_dt.date()

            sync_checkout_initiated_ci_events(
                brand_index=brand_index,
                brand_name=brand_name,
                brand_key=os.environ.get(f"BRAND_TAG_{brand_index}"),
                start_date=ci_start_dt,
                end_date=ci_end_dt,
                cursor=cursor,
                connection=connection,
            )

            sync_checkout_initiated_variant_events(
                brand_index=brand_index,
                brand_name=brand_name,
                brand_key=os.environ.get(f"BRAND_TAG_{brand_index}"),
                start_date=ci_start_dt,
                end_date=ci_end_dt,
                cursor=cursor,
                connection=connection,
            )

            sync_buy_now_events(
                brand_index=brand_index,
                brand_name=brand_name,
                brand_key=os.environ.get(f"BRAND_TAG_{brand_index}"),
                start_date=ci_start_dt,
                end_date=ci_end_dt,
                cursor=cursor,
                connection=connection,
            )

            sync_buy_now_variant_events(
                brand_index=brand_index,
                brand_name=brand_name,
                brand_key=os.environ.get(f"BRAND_TAG_{brand_index}"),
                start_date=ci_start_dt,
                end_date=ci_end_dt,
                cursor=cursor,
                connection=connection,
            )

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


# ---------------------------
# Per-brand processing
# ---------------------------
def process_single_brand(brand_index: int, job_id: Optional[str] = None):
    if brand_index not in brand_db_configs:
        logger.error(f"❌ Skipping brand {brand_index}: no database config")
        return
    _db_context.job_id = job_id
    brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
    brand_key = os.environ.get(
        f"BRAND_TAG_{brand_index}"
    )  # used for session_adjustment_buckets & alerts

    logger.info(f"\n{'=' * 50}\nSTARTING PROCESS FOR SHOP: {brand_name}\n{'=' * 50}")
    log_db_connection_metrics(f"DB_CONNECTION_METRICS_BRAND_START brand={brand_name}")

    if aws_background.DISCOUNT_DASHBOARD_ONLY_MODE:
        execute_discount_dashboard_backfill_only(brand_index, brand_name)
        log_db_connection_metrics(f"DB_CONNECTION_METRICS_BRAND_COMPLETE brand={brand_name}")
        _db_context.job_id = None
        return

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
    update_sessions_summary(
        brand_index,
        brand_name,
        session_url,
        x_brand_name,
        x_api_key,
        shop_name,
        api_version,
        access_token,
        tz,
    )

    # Track affected dates across all order processing (sparse set, not a range)
    affected_dates: Set[str] = set()
    rto_affected_dates: Set[str] = set()

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
        ensure_summary_tables(cursor, connection)

        for process in process_types:
            logger.info(
                f"\n--- Processing {process['type']} orders for {brand_name} ---"
            )
            timestamp_col = "created_at" if process["type"] == "NEW" else "updated_at"
            last_order = get_last_order(brand_index, process["table"], cursor=cursor)
            existing_ids = set()

            if aws_background.is_backfill_active_for(brand_index):
                # Use the configured backfill window in the brand's store timezone.
                start_dt, end_dt = aws_background.get_backfill_window_for_brand(brand_index)
                if not start_dt or not end_dt:
                    logger.warning(
                        f"🛑 Missing localized backfill window for {brand_name}. Skipping."
                    )
                    continue
                start_date = convert_to_desired_format(start_dt, tz)
                end_date = convert_to_desired_format(end_dt, tz)
                logger.info(
                    f"🔁 BACKFILL MODE: {process['type']} from {start_dt} to {end_dt} "
                    f"({tz.key})"
                )
            else:
                if not last_order:
                    logger.warning(
                        f"🛑 No initial data in '{process['table']}' for {brand_name}. Skipping."
                    )
                    continue
                last_ts = last_order[timestamp_col]
                if last_ts is None:
                    logger.warning(
                        f"🛑 Last {timestamp_col} watermark is missing for {brand_name}. Skipping."
                    )
                    continue
                # Advance by one second because the Shopify start boundary is inclusive.
                next_ts = last_ts + timedelta(seconds=1)
                start_date = convert_to_desired_format(next_ts, tz)
                end_date = convert_to_desired_format(datetime.now(tz), tz)
                logger.info(f"Fetching orders after: {last_ts} (API start: {next_ts})")

            orders_list = fetch_orders(
                api_base_url, access_token, start_date, end_date, process["date_field"]
            )

            if not orders_list:
                logger.info(
                    f"No new {process['type']} orders to process for {brand_name}"
                )
                continue

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
                continue

            logger.info(
                f"Fetched {original_count}, removed {original_count - len(filtered)} duplicates. Processing {len(filtered)} orders."
            )

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

            # Load immediately for this process type
            batch_size = int(os.environ.get("BATCH_SIZE", 1000))
            load_data_to_sql_optimized(
                df, brand_index, brand_name, process["table"], batch_size
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
                upsert_products_sold_units_daily_from_orders(
                    brand_index=brand_index,
                    orders_list=filtered,
                    brand_name=brand_name,
                    shop_name=shop_name,
                    access_token=access_token,
                    cursor=cursor,
                    connection=connection,
                )

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

        # 3. Referrer
        try:
            process_and_upload_referrer_summary(
                brand_index, brand_name, shop_name, access_token, str(u_min), str(u_max)
            )
        except Exception as e:
            logger.error(f"❌ Error updating Referrer summary for {brand_name}: {e}")

    except Exception as e:
        logger.error(
            f"❌ Critical error in ShopifyQL summary block for {brand_name}: {e}"
        )
        traceback.print_exc()

    logger.info(f"✅ COMPLETED PROCESSING FOR {brand_name}")
    log_db_connection_metrics(f"DB_CONNECTION_METRICS_BRAND_COMPLETE brand={brand_name}")
    _db_context.job_id = None


# ---------------------------
# Job runner
# ---------------------------
def run_data_pipeline():
    manual_run_id = None
    succeeded = False
    failure_exception: Optional[Exception] = None
    try:
        manual_run_id = MANUAL_TRIGGER_QUEUE.get_nowait()
    except queue.Empty:
        pass

    job_start_time = aws_background.now_ist()
    job_id = f"job-{uuid.uuid4().hex[:8]}"
    logger.info(
        f"\n{'=' * 60}\nJOB TRIGGERED AT: {job_start_time.strftime('%Y-%m-%d %I:%M:%S %p')}\nJOB ID: {job_id}\nMANUAL TRIGGER: {bool(manual_run_id)}\n{'=' * 60}"
    )

    if manual_run_id and aws_background.MANAGER_STARTED_ENDPOINT:
        try:
            started_payload = {
                "runId": manual_run_id,
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
        if not active_brand_indices:
            logger.warning("No active brands with valid database configs. Nothing to do.")
            return

        # Filter indices if in backfill mode to avoid processing unrelated brands
        target_indices = active_brand_indices
        if aws_background.BACKFILL_MODE and aws_background.BACKFILL_BRAND_INDICES:
            target_indices = [
                i for i in active_brand_indices if i in aws_background.BACKFILL_BRAND_INDICES
            ]
            logger.info(
                f"🎯 BACKFILL_MODE is active. Filtering brands to: {target_indices}"
            )

        if not target_indices:
            logger.warning("No active brands match the current filters. Nothing to do.")
            return

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
        max_concurrent_brands = int(os.environ.get("MAX_CONCURRENT_BRANDS", "3"))
        max_workers = max(1, min(len(target_indices), max_concurrent_brands))
        logger.info(
            f"Processing {len(target_indices)} brands with {max_workers} parallel workers"
            f" (MAX_CONCURRENT_BRANDS={max_concurrent_brands})"
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            logger.info(f"🚀 Starting {len(target_indices)} brand worker threads...")
            futures = {
                executor.submit(process_single_brand, i, job_id): i for i in target_indices
            }
            for fut in as_completed(futures):
                idx = futures[fut]
                name = os.environ.get(f"BRAND_NAME_{idx}", f"Brand_{idx}")
                try:
                    fut.result()
                    logger.info(f"✅ Successfully completed brand: {name}")
                except Exception as e:
                    logger.error(f"❌ Failed processing brand {name}: {e}")
                    traceback.print_exc()
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

        # Trigger completion webhook
        trigger_pipeline_completion_webhook()

        # --- NEW: UTM Pipeline API Trigger ---
        trigger_utm_pipeline("ETL_COMPLETED", job_id)

        succeeded = True

    except Exception as e:
        logger.error(f"❌ PIPELINE FAILED with error: {e}")
        traceback.print_exc()
        failure_exception = e
    finally:
        if manual_run_id and succeeded and aws_background.MANAGER_COMPLETED_ENDPOINT:
            try:
                completed_payload = {
                    "runId": manual_run_id,
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

        if manual_run_id and (not succeeded) and aws_background.MANAGER_FAILED_ENDPOINT:
            try:
                recent_logs = _last_logs_handler.get_recent()
                error_body = "\n".join(recent_logs) if recent_logs else "No logs captured."
                error_code = _derive_error_code(failure_exception)
                failed_payload = {
                    "runId": manual_run_id,
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


# ---------------------------
# Flask API Endpoints
# ---------------------------
@app.route("/trigger", methods=["POST"])
def manual_trigger():
    """
    Manual trigger endpoint to run the pipeline immediately.
    Expects 'x-pipeline-key' header for authentication and a JSON body with
    'runId' provided by the pipeline manager.
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

    # We use the scheduler to trigger the job immediately.
    # This ensures that APScheduler's concurrency control (max_instances=1) is respected.
    try:
        job = scheduler.get_job("pipeline_job")
        if not job:
            logger.info("Pipeline job not found in scheduler (likely finished previous manual run). Re-adding job.")
            if aws_background.SELF_RUN:
                job = scheduler.add_job(
                    run_data_pipeline,
                    "cron",
                    minute="0,10,20,30,40,50",
                    second=0,
                    coalesce=True,
                    max_instances=1,
                    misfire_grace_time=120,
                    replace_existing=True,
                    id="pipeline_job",
                )
            else:
                job = scheduler.add_job(
                    run_data_pipeline,
                    trigger=None,
                    next_run_time=None,
                    coalesce=True,
                    max_instances=1,
                    misfire_grace_time=120,
                    replace_existing=True,
                    id="pipeline_job",
                )

        MANUAL_TRIGGER_QUEUE.put(run_id)
        # Trigger the job to run now
        job.modify(next_run_time=datetime.now(IST))
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
