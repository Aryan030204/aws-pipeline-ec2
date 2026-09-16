# -*- coding: utf-8 -*-
"""
Optimized & Hardened Shopify ETL Pipeline with Incremental Summary Updates
(With returns_fact + corrected overall returns logic + optional backfill window)

KEY OPTIMIZATION: Summary tables are now updated INCREMENTALLY only for affected dates,
reducing processing time by 90%+ compared to full table rebuilds.

Scope of improvements:
- Keep `shopify_orders_update` exactly as is (still fetched with updated_at_min).
- Introduce `returns_fact` table.
- Populate `returns_fact` directly from order snapshots (cancelled_at, refunds[].transactions).
- Incremental summary updates: Only recalculate rows for dates that changed
- Track affected date ranges from fetched orders
- Add ENV-based backfill window override for orders fetch:
    BACKFILL_MODE=true|false
    BACKFILL_START_IST=YYYY-MM-DD[THH:MM:SS]
    BACKFILL_END_IST=YYYY-MM-DD[THH:MM:SS]

Everything else stays the same to avoid performance regressions.

CHANGE IN THIS REVISION ONLY:
- In sales_summary, `overall_returns` and `actual_overall_sales` now use ONLY REFUND amounts
  from `returns_fact` (SUM(amount) WHERE event_type='REFUND' GROUP BY event_date).

ADDED IN THIS VERSION:
- overall_summary.adjusted_total_sessions column
- adjusted_total_sessions is computed from master.session_adjustment_buckets using:
  - brand_key
  - active = 1
  - effective_from <= date <= effective_to
  - total_sessions BETWEEN lower_bound_sessions AND upper_bound_sessions
  - offset_pct applied as percentage change

ADDED NOW:
- QStash producer: for each affected date in overall_summary, publish an event via
  QStash to the alerting system.
"""

import sys

# When this file is run directly (`python aws_background.py`), Python registers
# it as sys.modules['__main__'], NOT sys.modules['aws_background']. Several
# pipeline/*.py modules do `import aws_background` to reach names that stay in
# this facade (TEST_MODE, get_brand_timezone, etc.), relying on Python finding
# an already-registered 'aws_background' module rather than re-executing this
# file from scratch. Without this alias, direct execution triggers a second,
# independent import of this same file partway through the first one's pipeline
# imports, which fails with a circular-import ImportError. Must run before any
# `from pipeline.X import (...)` that could reach a submodule's `import aws_background`.
sys.modules.setdefault("aws_background", sys.modules[__name__])

from dotenv import load_dotenv

load_dotenv()

import asyncio
import aiohttp
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlparse, parse_qs

import pandas as pd
import numpy as np
import os
import json
import time
import logging
import re
import atexit
import random
import signal
import threading
from collections import deque
import traceback
from typing import Dict, Iterable, List, Optional, Tuple, Any, Set
from functools import lru_cache
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
import base64
import uuid
import queue
import hashlib
# from kafka import KafkaProducer (REMOVED)

from datetime import datetime, timedelta, date, timezone
from zoneinfo import ZoneInfo

import mysql.connector
from pymongo import MongoClient

from sqlalchemy import (
    create_engine,
    event,
    Table,
    Column,
    MetaData,
    String,
    Integer,
    Float,
    DateTime,
    Text,
    text,
)
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, jsonify
from pathlib import Path
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import requests

from bundle_rollups import execute_bundle_rollups_incremental, is_bundle_rollup_brand

from pipeline.state import (
    IST,
    logger,
    _LastLogsHandler,
    _last_logs_handler,
    brand_timezones,
    brand_tag_to_index_map,
    brand_id_from_config,
    brand_db_configs,
    sqlalchemy_engines,
    active_brand_indices,
    brand_rto_tags_by_index,
    scheduler,
    app,
    MANUAL_TRIGGER_QUEUE,
    _db_metrics_lock,
    _db_shutdown_lock,
    _db_context,
    _db_connection_metrics,
    http_session,
)
from pipeline.schema_migrations import (
    ensure_user_agent_column,
    ensure_customer_tag_column,
    ensure_bundle_property_columns,
    ensure_device_summary_columns,
    ensure_utm_names_column,
    ensure_ci_events_column,
    ensure_buy_now_events_column,
    ensure_overall_summary_event_columns,
    ensure_hour_wise_ppcod_column,
    ensure_hour_wise_payment_sales_columns,
    ensure_overall_summary_payment_sales_columns,
    run_brand_preflight_schema_migrations,
    ensure_summary_tables,
)


# ---- Globals ----
def parse_store_timezone(tz_str: Optional[str]) -> ZoneInfo:
    """
    Parses timezone strings of the form "(GMT+05:30) Asia/Kolkata" or "Asia/Kolkata".
    Falls back to IST (Asia/Kolkata) if missing or invalid.
    """
    if not tz_str:
        return ZoneInfo("Asia/Kolkata")
    tz_str = tz_str.strip()
    if ") " in tz_str:
        tz_name = tz_str.split(") ", 1)[1].strip()
    else:
        tz_name = tz_str
    try:
        return ZoneInfo(tz_name)
    except Exception as e:
        logger.warning(
            f"⚠️ Could not load timezone '{tz_name}' from raw value '{tz_str}' (error: {e}). "
            "Falling back to Asia/Kolkata."
        )
        return ZoneInfo("Asia/Kolkata")

def get_brand_timezone(brand_index: int) -> ZoneInfo:
    return brand_timezones.get(brand_index, ZoneInfo("Asia/Kolkata"))

# --- TEST_MODE ---
TEST_MODE = os.environ.get("TEST_MODE", "false").strip().lower() == "true"
if TEST_MODE:
    load_dotenv(".env.local", override=True)
    logger.info(
        "⚠️ TEST_MODE enabled: QStash and DB SSL will be DISABLED, loading config from local env (.env.local)."
    )

MONGO_URI = os.environ.get("MONGO_URI", "").strip()
SPEED_MONGO_URI = os.environ.get("SPEED_MONGO_URI", "").strip()

PIPELINE_AUTH_HEADER = os.environ.get("PIPELINE_AUTH_HEADER", "pipeline@nosecret123")
SELF_RUN = os.environ.get("SELF_RUN", "true").strip().lower() == "true"
MANAGER_STARTED_ENDPOINT = os.environ.get("MANAGER_STARTED_ENDPOINT")
MANAGER_COMPLETED_ENDPOINT = os.environ.get("MANAGER_COMPLETED_ENDPOINT")
MANAGER_FAILED_ENDPOINT = os.environ.get("MANAGER_FAILED_ENDPOINT")
PIPELINE_ID = os.environ.get("PIPELINE_ID", "shopify-pipeline-etl")


class RDSProxyValidationError(RuntimeError):
    pass


DB_TEST_ON_STARTUP = os.environ.get("DB_TEST_ON_STARTUP", "false").strip().lower() == "true"
REQUIRE_RDS_PROXY = os.environ.get("REQUIRE_RDS_PROXY", "true").strip().lower() == "true"
EXPECTED_RDS_PROXY_HOST = os.environ.get("EXPECTED_RDS_PROXY_HOST", "").strip()
try:
    RTO_ASSUMPTION_PERIOD = int(os.environ.get("RTO_ASSUMPTION_PERIOD", "7").strip())
except ValueError:
    logger.warning(
        "Invalid RTO_ASSUMPTION_PERIOD value %r; defaulting to 7 days.",
        os.environ.get("RTO_ASSUMPTION_PERIOD"),
    )
    RTO_ASSUMPTION_PERIOD = 7

from pipeline.db import (
    DB_RETRY_ATTEMPTS,
    DB_RETRY_BASE_DELAY_S,
    _metric_inc,
    _metric_dec,
    get_db_connection_metrics_snapshot,
    log_db_connection_metrics,
    close_all_database_resources,
    _handle_shutdown_signal,
    _ensure_ca_file_from_env,
    _sqlalchemy_connect_args_for_tls,
    _is_ssl_connection_error,
    timed,
    EXECUTEMANY_CHUNK_SIZE,
    executemany_chunked,
    _connector_config_without_ssl,
    _open_mysql_connection_once,
    get_db_connection,
    get_db_cursor,
    get_last_fetch_timestamp,
    update_last_fetch_timestamp,
    ensure_pipeline_metadata_table,
    get_pipeline_metadata_timestamp,
    update_pipeline_metadata_timestamp,
)
from pipeline.web_vitals import (
    WEB_VITALS_METADATA_KEY,
    rebuild_daily_web_vitals_summary_for_keys,
    rebuild_daily_web_vitals_summary_for_window,
    sync_web_vitals_summary,
)
from pipeline.shopifyql_sessions import (
    _make_shopifyql_session,
    _format_shopifyql_table,
    fetch_shopify_sessions_via_shopifyql,
    fetch_shopify_hourly_sessions_via_shopifyql,
    fetch_shopify_daily_city_sessions_via_shopifyql,
    fetch_shopify_hourly_city_sessions_via_shopifyql,
    update_hourly_sessions_summary_from_shopifyql,
    update_daily_city_sessions_summary_from_shopifyql,
    update_hourly_city_sessions_summary_from_shopifyql,
    update_sessions_summary_from_shopifyql,
)
from pipeline.notifications import (
    _derive_error_code,
    QSTASH_TOKEN,
    ALERTS_WEBHOOK_URL,
    get_qstash_client,
    CACHE_PIPELINE_TRIGGER_URL,
    trigger_cache_pipeline,
    UTM_PIPELINE_TRIGGER_API,
    UTM_PIPELINE_SWITCH,
    trigger_utm_pipeline,
)
from pipeline.shopify_orders import (
    fetch_orders_async,
    fetch_orders,
    convert_to_desired_format,
    format_datetime,
    _parse_iso_to_store_date_time,
    transform_orders_to_df_optimized,
    RETURN_EVENT_CANCEL,
    RETURN_EVENT_CANCEL_RTO,
    RETURN_EVENT_REFUND,
    _ensure_returns_fact,
    _parse_iso_to_store_date,
    upsert_returns_fact_from_orders,
    _classify_payment_mode,
    _normalize_city_name,
    _normalized_city_sql_expr,
    _date_range_iso_set,
    _parse_iso_to_store_date_hour,
    _normalize_discount_code_rows_from_orders,
    upsert_dashboard_order_discount_fact_from_orders,
    backfill_dashboard_order_discount_fact_from_shopify_orders,
    _ensure_products_sold_units_daily,
    _collect_product_variant_window_units,
    _extract_inventory_units_sold,
    _fetch_product_sold_units_from_shopifyql,
    upsert_products_sold_units_daily_from_orders,
    _dates_in_clause,
    _normalize_rto_key,
    _normalize_rto_tag_value,
    _build_rto_db_database_map,
    get_rto_tag_for_brand,
    _build_mysql_regex_for_exact_tag,
    _tags_csv_contains_exact_tag,
    _is_order_rto_cancel,
    get_rto_affected_dates_updates,
    get_affected_dates_new_orders,
    get_affected_dates_updates,
    load_data_to_sql_optimized,
)
from pipeline.summary_incremental import (
    merge_staging_to_main_by_date,
    update_sales_summary_incremental,
    update_order_summary_incremental,
    update_discount_summary_incremental,
    update_gross_summary_incremental,
    update_hour_wise_sales_incremental,
    backfill_payment_mode_breakdown_columns,
    update_daily_citywise_summary_incremental,
    update_hourly_citywise_summary_incremental,
    update_dashboard_discount_daily_incremental,
    update_dashboard_discount_hourly_incremental,
    update_dashboard_discount_payment_daily_incremental,
    update_dashboard_discount_payment_hourly_incremental,
    update_overall_summary_incremental,
    update_rto_summary_incremental,
    update_shopify_orders_utm_daily_incremental,
    push_overall_summary_events_to_qstash,
    push_web_vitals_metrics_to_qstash,
    execute_summary_queries_incremental,
    execute_discount_dashboard_backfill_only,
)
from pipeline.mongo_events import (
    _normalize_brand_token,
    CI_EVENTS_BRANDS,
    CI_EVENTS_MONGO_SHOP_NAME_MAP,
    is_ci_events_brand,
    get_ci_events_mongo_shop_name,
    _resolve_mongo_sessions_collection,
    _ist_date_range_to_utc_bounds,
    _extract_session_id,
    _extract_cart_token,
    _normalize_variant_ids,
    _iter_counted_pair_events_from_mongo,
    _iter_unique_daily_pair_events_from_mongo,
    _fetch_daily_unique_pair_event_counts_from_mongo,
    _upsert_overall_summary_event_totals,
    update_overall_summary_event_columns,
    fetch_checkout_initiated_hourly_counts_from_mongo,
    fetch_buy_now_hourly_counts_from_mongo,
    sync_checkout_initiated_ci_events,
    sync_buy_now_events,
    fetch_checkout_initiated_variant_counts_from_mongo,
    fetch_buy_now_variant_counts_from_mongo,
    sync_checkout_initiated_variant_events,
    sync_buy_now_variant_events,
    sync_variant_wise_events_summary,
)
from pipeline.checkout_login_status import (
    sync_daily_checkout_login_status,
)
from pipeline.intent_events import (
    _parse_intent_db_map,
    sync_intent_events_for_brand,
    sync_click_events_for_brand,
)
from pipeline.utm_traffic import (
    fetch_shopify_utm_sessions_daily,
    process_and_upload_utm_summary,
    _normalize_product_traffic_source,
    update_product_traffic_split_incremental,
    refresh_product_traffic_split_for_window,
    fetch_shopify_referrer_sessions_daily,
    process_and_upload_referrer_summary,
)
from pipeline.orchestration import (
    _get_last_order_with_cursor,
    get_last_order,
    update_sessions_summary,
    process_single_brand,
    run_data_pipeline,
    manual_trigger,
    health_check,
)
# Registers GET /workers, POST /workers/<name>/trigger, GET /workers/<name>/status
# on the same Flask app (manual knob for the standalone workers).
import pipeline.worker_triggers  # noqa: E402,F401


INVENTORY_PIPELINE_SWITCH = (
    os.environ.get("INVENTORY_PIPELINE_SWITCH", "false").strip().lower() == "true"
)


# ---------------------------
# Optional Backfill Config (ENV-driven; no effect unless enabled)
# ---------------------------
def _parse_backfill_dt(val: Optional[str]) -> Optional[datetime]:
    if not val:
        return None
    # Accept "YYYY-MM-DD" or "YYYY-MM-DDTHH:MM:SS".
    # These env values are interpreted later in each brand's store timezone.
    try:
        if "T" in val:
            return datetime.fromisoformat(val)
        else:
            return datetime.strptime(val, "%Y-%m-%d")
    except Exception:
        logger.warning(f"BACKFILL_* value '{val}' not parseable; ignoring.")
        return None


def _localize_backfill_dt(dt_obj: Optional[datetime], tz: ZoneInfo) -> Optional[datetime]:
    if dt_obj is None:
        return None
    if dt_obj.tzinfo is None:
        return dt_obj.replace(tzinfo=tz)
    return dt_obj.astimezone(tz)


def get_backfill_window_for_brand(
    brand_index: int,
) -> Tuple[Optional[datetime], Optional[datetime]]:
    tz = get_brand_timezone(brand_index)
    return (
        _localize_backfill_dt(BACKFILL_START_IST, tz),
        _localize_backfill_dt(BACKFILL_END_IST, tz),
    )


BACKFILL_MODE = os.environ.get("BACKFILL_MODE", "false").strip().lower() == "true"
BACKFILL_START_IST = _parse_backfill_dt(os.environ.get("BACKFILL_START_IST"))
BACKFILL_END_IST = _parse_backfill_dt(os.environ.get("BACKFILL_END_IST"))

if BACKFILL_MODE:
    if not BACKFILL_START_IST or not BACKFILL_END_IST:
        logger.warning(
            "BACKFILL_MODE enabled but BACKFILL_START_IST/BACKFILL_END_IST missing or invalid. Backfill will be ignored for this run."
        )
        BACKFILL_MODE = False
    elif BACKFILL_START_IST >= BACKFILL_END_IST:
        logger.warning("BACKFILL_* start >= end. Disabling backfill for safety.")
        BACKFILL_MODE = False
    else:
        logger.info(
            "🔁 BACKFILL_MODE ON :: "
            f"{BACKFILL_START_IST} → {BACKFILL_END_IST} "
            "(interpreted in each brand's store timezone)"
        )

# Backfill Filter
BACKFILL_BRAND_INDICES_STR = os.environ.get("BACKFILL_BRAND_INDICES", "").strip()
BACKFILL_BRAND_INDICES = set()
if BACKFILL_BRAND_INDICES_STR:
    try:
        BACKFILL_BRAND_INDICES = {
            int(x.strip()) for x in BACKFILL_BRAND_INDICES_STR.split(",") if x.strip()
        }
        logger.info(
            f"🎯 Backfill restricted to brand indices: {BACKFILL_BRAND_INDICES}"
        )
    except ValueError:
        logger.warning(
            f"⚠️ Invalid BACKFILL_BRAND_INDICES '{BACKFILL_BRAND_INDICES_STR}'. Ignoring filter."
        )


def is_backfill_active_for(brand_idx: int) -> bool:
    if not BACKFILL_MODE:
        return False
    if not BACKFILL_BRAND_INDICES:
        return True
    return brand_idx in BACKFILL_BRAND_INDICES


DISCOUNT_DASHBOARD_BACKFILL = (
    os.environ.get("DISCOUNT_DASHBOARD_BACKFILL", "false").strip().lower() == "true"
)
DISCOUNT_DASHBOARD_ONLY_MODE = (
    os.environ.get("DISCOUNT_DASHBOARD_ONLY_MODE", "false").strip().lower() == "true"
)
DISCOUNT_BACKFILL_START_DATE = os.environ.get("DISCOUNT_BACKFILL_START_DATE", "").strip()
DISCOUNT_BACKFILL_END_DATE = os.environ.get("DISCOUNT_BACKFILL_END_DATE", "").strip()


def _parse_backfill_date(val: str) -> Optional[str]:
    if not val:
        return None
    try:
        return datetime.strptime(val, "%Y-%m-%d").date().isoformat()
    except Exception:
        logger.warning(f"DISCOUNT_BACKFILL_* value '{val}' not parseable; ignoring.")
        return None


DISCOUNT_BACKFILL_START_DATE_PARSED = _parse_backfill_date(
    DISCOUNT_BACKFILL_START_DATE
)
DISCOUNT_BACKFILL_END_DATE_PARSED = _parse_backfill_date(DISCOUNT_BACKFILL_END_DATE)

if DISCOUNT_DASHBOARD_BACKFILL:
    if (
        DISCOUNT_BACKFILL_START_DATE_PARSED
        and DISCOUNT_BACKFILL_END_DATE_PARSED
        and DISCOUNT_BACKFILL_START_DATE_PARSED > DISCOUNT_BACKFILL_END_DATE_PARSED
    ):
        logger.warning(
            "DISCOUNT_BACKFILL_START_DATE > DISCOUNT_BACKFILL_END_DATE. Disabling discount dashboard backfill."
        )
        DISCOUNT_DASHBOARD_BACKFILL = False
    else:
        date_label = "all dates"
        if DISCOUNT_BACKFILL_START_DATE_PARSED or DISCOUNT_BACKFILL_END_DATE_PARSED:
            date_label = (
                f"{DISCOUNT_BACKFILL_START_DATE_PARSED or 'beginning'}"
                f" to {DISCOUNT_BACKFILL_END_DATE_PARSED or 'today'}"
            )
        logger.info(f"🔁 DISCOUNT_DASHBOARD_BACKFILL ON :: {date_label}")

if DISCOUNT_DASHBOARD_ONLY_MODE and not DISCOUNT_DASHBOARD_BACKFILL:
    logger.warning(
        "DISCOUNT_DASHBOARD_ONLY_MODE requires DISCOUNT_DASHBOARD_BACKFILL=true. Disabling discount-only mode."
    )
    DISCOUNT_DASHBOARD_ONLY_MODE = False


# ---------------------------
# Utilities: time/profiling
# ---------------------------
def now_ist() -> datetime:
    return datetime.now(IST)


def extract_bundle_item_columns_from_props(props) -> Dict[str, Optional[str]]:
    item_columns = {}
    for n in range(1, 11):
        item_columns[f"_ITEM{n}_name"] = None
        item_columns[f"_ITEM{n}_value"] = None

    if not props:
        return item_columns

    props_map = {}
    for prop in props[:100]:
        if isinstance(prop, dict) and prop.get("name"):
            props_map[str(prop.get("name"))] = prop.get("value")

    extracted_items = []

    raw_uniware = props_map.get("uniware_items")
    if raw_uniware:
        try:
            data = json.loads(raw_uniware)
            for item in data.get("items", []):
                raw_item_props = item.get("properties", "{}")
                if isinstance(raw_item_props, str):
                    try:
                        item_props = json.loads(raw_item_props)
                    except Exception:
                        item_props = {}
                else:
                    item_props = raw_item_props or {}

                if item_props.get("_product") != "child":
                    continue

                name = (item.get("title") or item.get("name") or "").strip()
                price = item.get("price")
                if not name:
                    continue
                extracted_items.append(
                    {
                        "name": name,
                        "value": str(price).strip()
                        if price not in (None, "")
                        else None,
                    }
                )
        except Exception:
            pass

    if not extracted_items:
        i = 1
        while True:
            key = f"sub_item_{i}"
            if key not in props_map:
                break
            raw_value = str(props_map[key] or "").strip()
            before_sep = raw_value.split("::")[0].strip()
            match = re.match(r"^(.*?)\s+x\s+\d+\s*$", before_sep)
            name = match.group(1).strip() if match else before_sep
            if name:
                extracted_items.append({"name": name, "value": None})
            i += 1

    if not extracted_items:
        for idx, prop in enumerate(props[:10]):
            if prop and prop.get("name", "").startswith("_ITEM"):
                value = (prop.get("value") or "").strip()
                value_parts = value.split("SKU:")
                extracted_items.append(
                    {
                        "name": value_parts[0].strip() if len(value_parts) > 0 else None,
                        "value": value_parts[1].strip() if len(value_parts) > 1 else None,
                    }
                )

    for idx, item in enumerate(extracted_items[:10], start=1):
        item_columns[f"_ITEM{idx}_name"] = item.get("name") or None
        item_columns[f"_ITEM{idx}_value"] = item.get("value") or None

    return item_columns


# ---------------------------
# API-Driven Config & Decryption (NEW)
# ---------------------------
GET_BRANDS_API = os.environ.get("GET_BRANDS_API")
PIPELINE_AUTH_HEADER = os.environ.get("PIPELINE_AUTH_HEADER")
PASSWORD_AES_KEY = os.environ.get("PASSWORD_AES_KEY")
ACTIVE_BRAND_IDS = [
    b.strip() for b in os.environ.get("ACTIVE_BRAND_IDS", "").split(",") if b.strip()
]
TOTAL_CONFIG_COUNT = int(os.environ.get("TOTAL_CONFIG_COUNT", "0"))


def decrypt_value(encrypted_val: str) -> str:
    """
    Decrypts a value that was encrypted using AES-256-CBC.
    Expected format: 'iv_b64:ciphertext_b64'
    """
    if not encrypted_val or ":" not in encrypted_val:
        return encrypted_val

    if not PASSWORD_AES_KEY:
        logger.warning(
            "PASSWORD_AES_KEY not found in environment. Returning raw value."
        )
        return encrypted_val

    try:
        iv_b64, ciphertext_b64 = encrypted_val.split(":", 1)
        iv = base64.b64decode(iv_b64)
        ciphertext = base64.b64decode(ciphertext_b64)

        # AES-256 requires 32-byte key
        key_bytes = PASSWORD_AES_KEY.encode("utf-8")
        if len(key_bytes) < 32:
            key_bytes = key_bytes.ljust(32, b"\0")
        elif len(key_bytes) > 32:
            key_bytes = key_bytes[:32]

        cipher = Cipher(
            algorithms.AES(key_bytes), modes.CBC(iv), backend=default_backend()
        )
        decryptor = cipher.decryptor()
        padded_content = decryptor.update(ciphertext) + decryptor.finalize()

        # Unpadding (assuming standard PKCS7 style where the last byte is the padding length)
        padding_len = padded_content[-1]
        if padding_len < 1 or padding_len > 16:
            # Fallback if padding looks invalid
            return padded_content.decode("utf-8", errors="ignore")

        content = padded_content[:-padding_len]
        return content.decode("utf-8")
    except Exception as e:
        logger.error(f"❌ Decryption failed: {e}")
        return encrypted_val


def fetch_active_brands() -> Dict[str, str]:
    """
    Fetch active brand ID-to-name mapping from the brands API via GET.
    Returns dict like {"1": "PTS", "2": "BBB", ...} where keys are brand_ids.
    """
    if not GET_BRANDS_API or not PIPELINE_AUTH_HEADER:
        logger.error(
            "❌ GET_BRANDS_API or PIPELINE_AUTH_HEADER not set in environment."
        )
        return {}

    headers = {"x-pipeline-key": PIPELINE_AUTH_HEADER}
    try:
        resp = requests.get(GET_BRANDS_API, headers=headers, timeout=30)
        if resp.status_code == 200:
            active_brands = resp.json()
            if isinstance(active_brands, dict):
                return active_brands
            logger.error(
                f"❌ Active brands API returned unexpected type: {type(active_brands)}"
            )
            return {}
        logger.error(f"❌ Failed to fetch active brands: {resp.status_code}")
        return {}
    except Exception as e:
        logger.error(f"❌ Brand discovery API error: {e}")
        return {}


def fetch_brand_config(brand_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch brand configuration from the tenants API via GET /{brand_id}.
    """
    if not GET_BRANDS_API or not PIPELINE_AUTH_HEADER:
        logger.error(
            "❌ GET_BRANDS_API or PIPELINE_AUTH_HEADER not set in environment."
        )
        return None

    headers = {"x-pipeline-key": PIPELINE_AUTH_HEADER}
    url = f"{GET_BRANDS_API.rstrip('/')}/{brand_id.lower()}"

    logger.info(f"📡 Fetching config from: {url}")
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        logger.error(f"❌ Failed to fetch config for {brand_id}: {resp.status_code}")
        logger.error(f"   Response: {resp.text[:500]}")
        return None
    except Exception as e:
        logger.error(f"❌ Config fetch error for {brand_id}: {e}")
        return None


def is_brand_config_active(config: Dict[str, Any]) -> bool:
    return config.get("is_active") is True

def _build_db_config(
    db_host: str,
    db_user: str,
    db_password: str,
    db_port: int,
    db_name: str,
    ca_path: Optional[str],
    verify_cert: bool,
    verify_identity: bool,
) -> Dict[str, Any]:
    effective_verify_identity = verify_identity
    if verify_identity and (
        "elb.amazonaws.com" in db_host
        or ("amazonaws.com" in db_host and "rds.amazonaws.com" not in db_host)
    ):
        effective_verify_identity = False

    config: Dict[str, Any] = {
        "host": db_host,
        "port": db_port,
        "user": db_user,
        "password": db_password,
        "database": db_name,
        "connection_timeout": int(os.environ.get("DB_CONNECT_TIMEOUT_S", "10")),
        "read_timeout": int(os.environ.get("DB_READ_TIMEOUT_S", "120")),
        "write_timeout": int(os.environ.get("DB_WRITE_TIMEOUT_S", "120")),
    }

    effective_ca_path = ca_path
    if db_host.lower() in ["localhost", "127.0.0.1"]:
        effective_ca_path = None

    if effective_ca_path:
        config.update(
            {
                "ssl_ca": effective_ca_path,
                "ssl_verify_cert": verify_cert,
                "ssl_verify_identity": effective_verify_identity,
            }
        )

    return config


def _validate_rds_proxy_host(db_host: str, brand_name: str) -> None:
    if TEST_MODE:
        return
    if not REQUIRE_RDS_PROXY:
        return

    host = (db_host or "").strip().lower()
    expected = EXPECTED_RDS_PROXY_HOST.lower()
    if expected:
        if host != expected:
            raise RDSProxyValidationError(
                f"DB host rejected for {brand_name}: expected configured RDS Proxy host"
            )
        return

    if ".proxy-" not in host and "proxy-" not in host:
        raise RDSProxyValidationError(
            f"DB host rejected for {brand_name}: host does not look like an RDS Proxy endpoint"
        )


def _create_sqlalchemy_engine_for_brand(
    brand_idx: int,
    brand_name: str,
    db_config: Dict[str, Any],
) -> Any:
    connect_args = {
        "connection_timeout": db_config.get("connection_timeout", 10),
        "read_timeout": db_config.get("read_timeout", 120),
        "write_timeout": db_config.get("write_timeout", 120),
    }
    for key in ("ssl_ca", "ssl_verify_cert", "ssl_verify_identity"):
        if key in db_config:
            connect_args[key] = db_config[key]

    engine = create_engine(
        URL.create(
            "mysql+mysqlconnector",
            username=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=int(db_config.get("port", 3306)),
            database=db_config["database"],
            query={"charset": "utf8mb4"},
        ),
        connect_args=connect_args,
        poolclass=NullPool,
        pool_pre_ping=True,
        echo=False,
        future=True,
    )

    @event.listens_for(engine, "connect")
    def _on_sqlalchemy_connect(dbapi_connection, connection_record):
        _metric_inc("active_sqlalchemy_connections")
        logger.debug(
            "SQLALCHEMY_CONNECTION_OPENED brand=%s brand_index=%s",
            brand_name,
            brand_idx,
        )

    @event.listens_for(engine, "close")
    def _on_sqlalchemy_close(dbapi_connection, connection_record):
        _metric_dec("active_sqlalchemy_connections")
        logger.debug(
            "SQLALCHEMY_CONNECTION_CLOSED brand=%s brand_index=%s",
            brand_name,
            brand_idx,
        )

    return engine


def _replace_sqlalchemy_engine_for_brand(
    brand_idx: int, brand_name: str, db_config: Dict[str, Any]
) -> None:
    old_engine = sqlalchemy_engines.get(brand_idx)
    if old_engine is not None:
        try:
            old_engine.dispose()
        except Exception:
            pass
    sqlalchemy_engines[brand_idx] = _create_sqlalchemy_engine_for_brand(
        brand_idx, brand_name, db_config
    )


def _test_brand_database_connection(brand_idx: int, brand_name: str) -> None:
    with get_db_connection(brand_idx) as cnx:
        cursor = cnx.cursor()
        try:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        finally:
            cursor.close()
    engine = sqlalchemy_engines.get(brand_idx)
    if engine is not None:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))


def initialize_brand_configs():
    import tempfile
    import certifi

    def _trunc(val: Any, length: int = 4) -> str:
        s = str(val) if val is not None else ""
        if len(s) <= length:
            return s
        return f"{s[:length]}..."

    logger.info("============================================================")
    logger.info("🔍 INITIALIZING PIPELINE ENVIRONMENT")
    logger.info(f"   TEST_MODE: {TEST_MODE}")
    logger.info("============================================================")

    # --------------------------
    # Infrastructure Setup Helper
    # --------------------------
    def _setup_brand_infra(
        brand_idx,
        brand_name,
        brand_tag,
        rto_tags,
        db_host,
        db_user,
        db_password,
        db_port,
        db_name,
        shop_name,
        access_token,
        api_version,
        speed_key,
        app_map_str,
        ca_path,
        verify_cert,
        verify_identity,
        store_timezone,
    ):
        logger.info(
            f"🔑 Config for {brand_name} ({'LOCAL' if TEST_MODE else 'API'}): "
            f"host={_trunc(db_host, 12)}, user={_trunc(db_user)}, db={db_name}"
        )

        brand_timezones[brand_idx] = parse_store_timezone(store_timezone)

        _validate_rds_proxy_host(db_host, brand_name)

        db_config = _build_db_config(
            db_host=db_host,
            db_user=db_user,
            db_password=db_password,
            db_port=db_port,
            db_name=db_name,
            ca_path=ca_path,
            verify_cert=verify_cert,
            verify_identity=verify_identity,
        )
        brand_db_configs[brand_idx] = db_config

        try:
            sqlalchemy_engines[brand_idx] = _create_sqlalchemy_engine_for_brand(
                brand_idx, brand_name, db_config
            )
        except Exception as e:
            brand_db_configs.pop(brand_idx, None)
            logger.error(f"❌ Engine setup error for {brand_name}: {e}")
            return

        if DB_TEST_ON_STARTUP:
            try:
                _test_brand_database_connection(brand_idx, brand_name)
            except Exception as e:
                sqlalchemy_engines.pop(brand_idx, None)
                brand_db_configs.pop(brand_idx, None)
                logger.error(f"❌ Startup DB test failed for {brand_name}: {e}")
                return

        os.environ[f"BRAND_NAME_{brand_idx}"] = brand_name
        os.environ[f"SHOP_NAME_{brand_idx}"] = shop_name
        os.environ[f"ACCESS_TOKEN_{brand_idx}"] = access_token
        os.environ[f"API_VERSION_{brand_idx}"] = api_version
        os.environ[f"BRAND_TAG_{brand_idx}"] = brand_tag
        os.environ[f"DB_DATABASE_{brand_idx}"] = db_name
        os.environ[f"SPEED_KEY_{brand_idx}"] = speed_key
        os.environ[f"APP_ID_MAPPING_{brand_idx}"] = app_map_str

        normalized_rto_tag = _normalize_rto_tag_value(rto_tags)
        if normalized_rto_tag:
            brand_rto_tags_by_index[brand_idx] = normalized_rto_tag
        else:
            brand_rto_tags_by_index.pop(brand_idx, None)

        brand_id_from_config[brand_idx] = (
            brand_idx  # store brand_id from pipelinecreds document
        )
        active_brand_indices.append(brand_idx)

    # --------------------------
    # TLS / CA resolution helpers
    # --------------------------
    def _write_b64_to_tempfile(b64_str: str, prefix: str) -> str:
        data = base64.b64decode(b64_str.encode("utf-8"))
        fd, path = tempfile.mkstemp(prefix=prefix, suffix=".pem")
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        return path

    def _resolve_ca_bundle_path() -> str | None:
        if TEST_MODE:
            return None  # No SSL in TEST_MODE
        mode = os.environ.get("DB_TLS_CA_MODE", "certifi").strip().lower()
        if mode == "certifi":
            return certifi.where()
        if mode == "rds":
            p = os.environ.get("RDS_CA_PATH")
            if p and os.path.exists(p):
                return p
            b64v = os.environ.get("RDS_CA_BUNDLE_B64")
            if b64v:
                return _write_b64_to_tempfile(b64v, prefix="rds-ca-")
            return None
        if mode == "custom":
            p = os.environ.get("CUSTOM_CA_PATH")
            if p and os.path.exists(p):
                return p
            b64v = os.environ.get("CUSTOM_CA_BUNDLE_B64")
            if b64v:
                return _write_b64_to_tempfile(b64v, prefix="custom-ca-")
            return None
        if mode == "none":
            return None
        return certifi.where()

    ca_path = _resolve_ca_bundle_path()
    verify_cert = os.environ.get("DB_SSL_VERIFY_CERT", "true").strip().lower() == "true"
    verify_identity = (
        os.environ.get("DB_SSL_VERIFY_IDENTITY", "false").strip().lower() == "true"
    )

    if TEST_MODE:
        logger.info("🧪 [TEST_MODE] Loading local config from .env...")

        for idx_str in ACTIVE_BRAND_IDS:
            try:
                i = int(idx_str)
                brand_name = os.environ.get(f"BRAND_NAME_{i}")
                if not brand_name:
                    continue

                brand_idx = i
                brand_tag = os.environ.get(f"BRAND_TAG_{i}", brand_name.lower())
                brand_tag_to_index_map[brand_tag] = brand_idx

                db_host = os.environ.get(f"DB_HOST_{i}")
                db_user = os.environ.get(f"DB_USER_{i}")
                db_password = os.environ.get(f"DB_PASSWORD_{i}")
                db_port = int(os.environ.get(f"DB_PORT_{i}", 3306))
                db_name = os.environ.get(f"DB_DATABASE_{i}")

                shop_name = os.environ.get(f"SHOP_NAME_{i}")
                access_token = os.environ.get(f"ACCESS_TOKEN_{i}")
                api_version = os.environ.get(f"API_VERSION_{i}", "2024-04")
                speed_key = os.environ.get(f"SPEED_KEY_{i}", "")
                app_map_str = os.environ.get(f"APP_ID_MAPPING_{i}", "{}")

                if not all(
                    [db_host, db_user, db_password, db_name, shop_name, access_token]
                ):
                    logger.error(f"❌ Missing local env vars for brand index {i}")
                    continue

                _setup_brand_infra(
                    brand_idx,
                    brand_name,
                    brand_tag,
                    os.environ.get(f"RTO_TAGS_{i}"),
                    db_host,
                    db_user,
                    db_password,
                    db_port,
                    db_name,
                    shop_name,
                    access_token,
                    api_version,
                    speed_key,
                    app_map_str,
                    ca_path,
                    verify_cert,
                    verify_identity,
                    os.environ.get(f"STORE_TIMEZONE_{i}"),
                )
            except Exception as e:
                logger.error(f"❌ Local config error for {idx_str}: {e}")
                continue
    else:
        logger.info("📡 [PROD_MODE] Discovering active brands via API...")
        active_brands = fetch_active_brands()
        logger.info(f"🔍 Active brands: {active_brands}")

        for brand_id_key, brand_tag_value in active_brands.items():
            logger.info(
                f"🔍 Fetching API config for brand: {brand_tag_value} (brand_id={brand_id_key})"
            )
            config = fetch_brand_config(brand_id_key)
            if not config:
                continue
            if not is_brand_config_active(config):
                logger.info(
                    "⏭️ Skipping brand %s (brand_id=%s): tenant config is not active",
                    config.get("brand_name", brand_tag_value),
                    brand_id_key,
                )
                continue

            try:
                # Use brand_id from the API response key (authoritative source)
                brand_idx = int(brand_id_key)
                brand_name = config.get("brand_name", brand_tag_value)
                brand_tag = config.get("brand_tag", brand_tag_value.lower())
                brand_tag_to_index_map[brand_tag] = brand_idx

                db_host = config["db_host"]
                db_user = config["db_user"]
                db_password = decrypt_value(config["db_password"])
                db_port = int(config.get("port", 3306))
                db_name = config.get("db_database", brand_tag_value)

                shop_name = config["shop_name"]
                access_token = decrypt_value(config["access_token"])
                api_version = config.get("api_version", "2024-04")
                speed_key = decrypt_value(config.get("speed_key", ""))
                app_map_val = config.get("app_id_mapping", "{}")
                app_map_str = (
                    app_map_val
                    if isinstance(app_map_val, str)
                    else json.dumps(app_map_val)
                )

                _setup_brand_infra(
                    brand_idx,
                    brand_name,
                    brand_tag,
                    config.get("rto_tags"),
                    db_host,
                    db_user,
                    db_password,
                    db_port,
                    db_name,
                    shop_name,
                    access_token,
                    api_version,
                    speed_key,
                    app_map_str,
                    ca_path,
                    verify_cert,
                    verify_identity,
                    config.get("store_timezone"),
                )
            except RDSProxyValidationError:
                raise
            except Exception as e:
                logger.error(f"❌ API config error for {brand_tag_value}: {e}")
                continue

    logger.info(f"✅ Active brands initialized: {active_brand_indices}")
    logger.info("Database connection mode: lazy")
    logger.info("SQLAlchemy pool mode: NullPool")
    logger.info(f"Configured brands: {len(active_brand_indices)}")
    logger.info(f"Active brand concurrency: {os.environ.get('MAX_CONCURRENT_BRANDS', '3')}")
    logger.info(f"RDS Proxy enforcement: {'enabled' if REQUIRE_RDS_PROXY and not TEST_MODE else 'disabled'}")
    logger.info(f"Startup connection test: {'enabled' if DB_TEST_ON_STARTUP else 'disabled'}")
    log_db_connection_metrics("DB_CONNECTION_METRICS_STARTUP")


# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    initialize_brand_configs()

    # Conditionally schedule the pipeline
    if SELF_RUN:
        # Schedule at fixed 10-minute wall-clock slots (:00, :10, :20, :30, :40, :50)
        scheduler.add_job(
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
        # Add the job without a trigger so it ONLY runs via manual trigger
        scheduler.add_job(
            run_data_pipeline,
            trigger=None,
            next_run_time=None,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=120,
            replace_existing=True,
            id="pipeline_job",
        )

    scheduler.start()

    logger.info("✅ Optimized Shopify ETL worker started (WITH MANUAL TRIGGER API)")
    
    if SELF_RUN:
        job = scheduler.get_job("pipeline_job")
        next_run = job.next_run_time if job else None
        if next_run:
            logger.info(f"   - Schedule: Every 10 mins (Next: {next_run.strftime('%Y-%m-%d %I:%M:%S %p %Z')})")
        else:
            logger.info("   - Schedule: Every 10 mins")
    else:
        logger.info("   - Schedule: MANUAL ONLY (SELF_RUN=false)")

    if DISCOUNT_DASHBOARD_ONLY_MODE:
        logger.info("   - Mode: DISCOUNT DASHBOARD BACKFILL ONLY")
    elif BACKFILL_MODE:
        msg = "   - Mode: BACKFILL (orders window overridden by BACKFILL_* env)"
        if BACKFILL_BRAND_INDICES:
            msg += f" [Restricted to brands: {BACKFILL_BRAND_INDICES}]"
        logger.info(msg)
    else:
        logger.info("   - Mode: Regular incremental")

    if DISCOUNT_DASHBOARD_BACKFILL:
        date_label = "all dates"
        if DISCOUNT_BACKFILL_START_DATE_PARSED or DISCOUNT_BACKFILL_END_DATE_PARSED:
            date_label = (
                f"{DISCOUNT_BACKFILL_START_DATE_PARSED or 'beginning'}"
                f" to {DISCOUNT_BACKFILL_END_DATE_PARSED or 'today'}"
            )
        logger.info(f"   - Discount dashboard backfill: ENABLED ({date_label})")

    logger.info(
        f"   - Parallel workers: {min(len(active_brand_indices), int(os.environ.get('MAX_CONCURRENT_BRANDS', '3')))}"
    )
    logger.info("   - Database connections: Lazy short-lived mysql.connector + SQLAlchemy NullPool")
    logger.info("   - Async API fetching: Enabled")
    logger.info("   - Summary updates: INCREMENTAL (only affected dates) 🚀")
    logger.info("   - Manual Trigger API: POST /trigger (port 5000)")
    logger.info("   - Worker Trigger API: POST /workers/<name>/trigger, GET /workers (port 5000)")

    try:
        # Run the Flask app (replaces the sleep loop)
        # Using 0.0.0.0 to listen on all interfaces
        app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down scheduler...")
        try:
            scheduler.shutdown()
        except Exception:
            pass
        close_all_database_resources()
        logger.info("✅ Shutdown complete")
