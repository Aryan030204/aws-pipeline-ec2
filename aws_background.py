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
    QSTASH_COMPLETION_TOKEN,
    get_qstash_client,
    trigger_pipeline_completion_webhook,
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
    execute_summary_queries_incremental,
    execute_discount_dashboard_backfill_only,
)


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
# DB reads/writes (accept cursor)
# ---------------------------
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


def _normalize_brand_token(value: Optional[str]) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").strip().lower())


CI_EVENTS_BRANDS = {
    "ajmal",
    "tmc",
    "pts",
    "bbb",
    "vama",
    "3tenx",
    "shylenew",
    "ajmalksa",
    "ajmaluk",
    "ajmalus",
    "ajmalbh"
}

CI_EVENTS_MONGO_SHOP_NAME_MAP = {
    "ajmal": "Ajmal_IN",
    "ajmalksa": "ajmal_ksa",
    "ajmaluk": "ajmal_uk",
    "ajmalus": "ajmal_us",
    "ajmalbh": "ajmal_bh",
    "tmc": "tmc",
    "pts": "pts",
    "bbb": "bbb",
    "vama": "vama",
    "3tenx": "3tenx",
    "shylenew": "shyle",
}


def is_ci_events_brand(
    brand_name: Optional[str], brand_key: Optional[str] = None
) -> bool:
    normalized_tokens = {
        _normalize_brand_token(brand_name),
        _normalize_brand_token(brand_key),
    }
    return any(token in CI_EVENTS_BRANDS for token in normalized_tokens)


def get_ci_events_mongo_shop_name(
    brand_name: Optional[str], brand_key: Optional[str] = None
) -> Optional[str]:
    normalized_tokens = (
        _normalize_brand_token(brand_key),
        _normalize_brand_token(brand_name),
    )
    for token in normalized_tokens:
        if token in CI_EVENTS_MONGO_SHOP_NAME_MAP:
            return CI_EVENTS_MONGO_SHOP_NAME_MAP[token]
    return None


def _resolve_mongo_sessions_collection():
    if not MONGO_URI:
        raise ValueError("MONGO_URI is not configured")

    client = MongoClient(MONGO_URI, tz_aware=True)
    return client, client["alerts"]["sessions"]


def _ist_date_range_to_utc_bounds(
    start_date: date, end_date: date, tz: ZoneInfo = IST
) -> Tuple[datetime, datetime]:
    start_tz = datetime.combine(start_date, datetime.min.time(), tzinfo=tz)
    end_tz_exclusive = datetime.combine(
        end_date + timedelta(days=1), datetime.min.time(), tzinfo=tz
    )
    return (
        start_tz.astimezone(timezone.utc),
        end_tz_exclusive.astimezone(timezone.utc),
    )


def _extract_session_id(doc: Dict[str, Any]) -> Optional[str]:
    session_id = str(doc.get("session_id") or "").strip()
    return session_id or None


def _extract_cart_token(doc: Dict[str, Any]) -> Optional[str]:
    cart_token = str(doc.get("cart_token") or "").strip()
    return cart_token or None


def _normalize_variant_ids(variant_val: Any) -> List[str]:
    variant_ids: List[str] = []
    if isinstance(variant_val, list):
        variant_ids = [str(v).strip() for v in variant_val if v is not None]
    elif isinstance(variant_val, (int, float)):
        variant_ids = [str(int(variant_val)).strip()]
    elif variant_val is not None:
        val_str = str(variant_val).strip()
        if val_str:
            variant_ids = [v.strip() for v in val_str.split(",") if v.strip()]
    return sorted(set(variant_ids))


def _iter_counted_pair_events_from_mongo(
    start_date: date,
    end_date: date,
    mongo_shop_name: str,
    event_type: str,
    tz: ZoneInfo = IST,
    dedupe_window_minutes: int = 30,
) -> List[Dict[str, Any]]:
    start_utc, end_utc = _ist_date_range_to_utc_bounds(start_date, end_date, tz)
    dedupe_window = timedelta(minutes=dedupe_window_minutes)
    client = None
    try:
        client, collection = _resolve_mongo_sessions_collection()
        cursor = (
            collection.find(
                {
                    "event_type": event_type,
                    "shop_name": mongo_shop_name,
                    "createdAt": {"$gte": start_utc, "$lt": end_utc},
                },
                {"createdAt": 1, "session_id": 1, "cart_token": 1, "variantId": 1},
            )
            .sort("createdAt", 1)
        )

        counted_events: List[Dict[str, Any]] = []
        last_counted_at_by_pair: Dict[Tuple[date, str, str], datetime] = {}
        for doc in cursor:
            session_id = _extract_session_id(doc)
            cart_token = _extract_cart_token(doc)
            if not session_id or not cart_token:
                continue

            created_at = doc.get("createdAt")
            if not isinstance(created_at, datetime):
                continue
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)

            local_created_at = created_at.astimezone(tz)
            local_date = local_created_at.date()
            pair_key = (local_date, session_id, cart_token)

            last_counted_at = last_counted_at_by_pair.get(pair_key)
            if last_counted_at is not None and created_at - last_counted_at <= dedupe_window:
                continue

            if local_date < start_date or local_date > end_date:
                continue

            counted_events.append(
                {
                    "date": local_date,
                    "hour": local_created_at.hour,
                    "variant_ids": _normalize_variant_ids(doc.get("variantId")),
                }
            )
            last_counted_at_by_pair[pair_key] = created_at

        return counted_events
    finally:
        if client is not None:
            client.close()


def _iter_unique_daily_pair_events_from_mongo(
    start_date: date,
    end_date: date,
    mongo_shop_name: str,
    event_type: str,
    tz: ZoneInfo = IST,
) -> List[Dict[str, Any]]:
    start_utc, end_utc = _ist_date_range_to_utc_bounds(start_date, end_date, tz)
    client = None
    try:
        client, collection = _resolve_mongo_sessions_collection()
        cursor = collection.find(
            {
                "event_type": event_type,
                "shop_name": mongo_shop_name,
                "createdAt": {"$gte": start_utc, "$lt": end_utc},
            },
            {"createdAt": 1, "session_id": 1, "cart_token": 1, "variantId": 1},
        )

        pair_events: Dict[Tuple[date, str, str], List[Dict[str, Any]]] = {}
        for doc in cursor:
            session_id = _extract_session_id(doc)
            cart_token = _extract_cart_token(doc)
            if not session_id or not cart_token:
                continue

            created_at = doc.get("createdAt")
            if not isinstance(created_at, datetime):
                continue
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)

            local_created_at = created_at.astimezone(tz)
            local_date = local_created_at.date()
            if local_date < start_date or local_date > end_date:
                continue

            pair_key = (local_date, session_id, cart_token)
            pair_events.setdefault(pair_key, []).append(
                {
                    "date": local_date,
                    "variant_ids": _normalize_variant_ids(doc.get("variantId")),
                }
            )

        unique_events: List[Dict[str, Any]] = []
        for events in pair_events.values():
            if len(events) == 1:
                unique_events.append(events[0])

        return unique_events
    finally:
        if client is not None:
            client.close()


def _fetch_daily_unique_pair_event_counts_from_mongo(
    start_date: date,
    end_date: date,
    mongo_shop_name: str,
    event_type: str,
    tz: ZoneInfo = IST,
) -> Dict[date, int]:
    counts: Dict[date, int] = {}
    unique_events = _iter_unique_daily_pair_events_from_mongo(
        start_date=start_date,
        end_date=end_date,
        mongo_shop_name=mongo_shop_name,
        event_type=event_type,
        tz=tz,
    )
    for event in unique_events:
        event_date = event["date"]
        counts[event_date] = counts.get(event_date, 0) + 1
    return counts


def _upsert_overall_summary_event_totals(
    cursor,
    connection,
    start_date: date,
    end_date: date,
    ci_counts: Dict[date, int],
    buy_now_counts: Dict[date, int],
):
    all_dates = []
    current = start_date
    while current <= end_date:
        all_dates.append(current)
        current += timedelta(days=1)

    for bucket_date in all_dates:
        cursor.execute(
            """
            INSERT INTO overall_summary (date, ci_events, buy_now_events)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                ci_events = VALUES(ci_events),
                buy_now_events = VALUES(buy_now_events)
            """,
            (
                bucket_date,
                int(ci_counts.get(bucket_date, 0)),
                int(buy_now_counts.get(bucket_date, 0)),
            ),
        )

    connection.commit()


def update_overall_summary_event_columns(
    brand_index: int,
    brand_name: str,
    start_date: date,
    end_date: date,
    brand_key: Optional[str] = None,
    cursor=None,
    connection=None,
):
    if not is_ci_events_brand(brand_name, brand_key):
        logger.info(
            f"Skipping overall_summary event totals for {brand_name}: brand not enabled for Mongo event sync"
        )
        return

    mongo_shop_name = get_ci_events_mongo_shop_name(brand_name, brand_key)
    if not mongo_shop_name:
        logger.info(
            f"Skipping overall_summary event totals for {brand_name}: no Mongo shop_name mapping"
        )
        return

    tz = get_brand_timezone(brand_index)
    try:
        ci_counts = _fetch_daily_unique_pair_event_counts_from_mongo(
            start_date=start_date,
            end_date=end_date,
            mongo_shop_name=mongo_shop_name,
            event_type="checkout_initiated",
            tz=tz,
        )
        buy_now_counts = _fetch_daily_unique_pair_event_counts_from_mongo(
            start_date=start_date,
            end_date=end_date,
            mongo_shop_name=mongo_shop_name,
            event_type="buy_now",
            tz=tz,
        )
    except Exception as e:
        logger.error(
            f"Error fetching overall_summary event totals from Mongo for {brand_name}: {e}"
        )
        return

    if cursor is not None:
        _upsert_overall_summary_event_totals(
            cursor, connection, start_date, end_date, ci_counts, buy_now_counts
        )
        logger.info(
            f"Updated overall_summary event totals for {brand_name} ({start_date}..{end_date})"
        )
        return

    try:
        with get_db_cursor(brand_index) as (c, conn):
            _upsert_overall_summary_event_totals(
                c, conn, start_date, end_date, ci_counts, buy_now_counts
            )
        logger.info(
            f"Updated overall_summary event totals for {brand_name} ({start_date}..{end_date})"
        )
    except Exception as e:
        logger.error(f"Error updating overall_summary event totals for {brand_name}: {e}")


def _fetch_event_hourly_counts_from_mongo(
    start_date: date,
    end_date: date,
    mongo_shop_name: str,
    event_type: str,
    metric_key: str,
    tz: ZoneInfo = IST,
) -> List[Dict[str, Any]]:
    counts: Dict[Tuple[date, int], int] = {}
    counted_events = _iter_counted_pair_events_from_mongo(
        start_date=start_date,
        end_date=end_date,
        mongo_shop_name=mongo_shop_name,
        event_type=event_type,
        tz=tz,
    )

    for event in counted_events:
        bucket_key = (event["date"], event["hour"])
        counts[bucket_key] = counts.get(bucket_key, 0) + 1

    return [
        {"date": bucket_date, "hour": bucket_hour, metric_key: count}
        for (bucket_date, bucket_hour), count in sorted(counts.items())
    ]


def fetch_checkout_initiated_hourly_counts_from_mongo(
    start_date: date,
    end_date: date,
    mongo_shop_name: str,
    tz: ZoneInfo = IST,
) -> List[Dict[str, Any]]:
    return _fetch_event_hourly_counts_from_mongo(
        start_date=start_date,
        end_date=end_date,
        mongo_shop_name=mongo_shop_name,
        event_type="checkout_initiated",
        metric_key="ci_events",
        tz=tz,
    )


def fetch_buy_now_hourly_counts_from_mongo(
    start_date: date,
    end_date: date,
    mongo_shop_name: str,
    tz: ZoneInfo = IST,
) -> List[Dict[str, Any]]:
    return _fetch_event_hourly_counts_from_mongo(
        start_date=start_date,
        end_date=end_date,
        mongo_shop_name=mongo_shop_name,
        event_type="buy_now",
        metric_key="buy_now_events",
        tz=tz,
    )


def _do_upsert_hourly_event_counts(
    results: List[Dict[str, Any]],
    cursor,
    connection,
    brand_name: str,
    start_date: date,
    end_date: date,
    column_name: str,
    success_label: str,
):
    cursor.execute(
        f"""
        UPDATE hourly_sessions_summary_shopify
        SET {column_name} = 0
        WHERE date BETWEEN %s AND %s
        """,
        (start_date, end_date),
    )

    for row in results:
        cursor.execute(
            f"""
            INSERT INTO hourly_sessions_summary_shopify (date, hour, {column_name})
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                {column_name} = VALUES({column_name})
            """,
            (row["date"], row["hour"], row[column_name]),
        )

    connection.commit()
    logger.info(
        f"Updated {success_label} for {brand_name} "
        f"({len(results)} hour buckets, {start_date}..{end_date})"
    )


def sync_checkout_initiated_ci_events(
    brand_index: int,
    brand_name: str,
    start_date: date,
    end_date: date,
    brand_key: Optional[str] = None,
    cursor=None,
    connection=None,
):
    if not is_ci_events_brand(brand_name, brand_key):
        logger.info(
            f"Skipping checkout-initiated Mongo sync for {brand_name}: brand not enabled for ci_events"
        )
        return

    mongo_shop_name = get_ci_events_mongo_shop_name(brand_name, brand_key)
    if not mongo_shop_name:
        logger.info(
            f"Skipping checkout-initiated Mongo sync for {brand_name}: no Mongo shop_name mapping"
        )
        return

    tz = get_brand_timezone(brand_index)
    try:
        results = fetch_checkout_initiated_hourly_counts_from_mongo(
            start_date=start_date,
            end_date=end_date,
            mongo_shop_name=mongo_shop_name,
            tz=tz,
        )
    except Exception as e:
        logger.error(
            f"Error fetching checkout-initiated Mongo events for {brand_name}: {e}"
        )
        return

    if cursor is not None:
        _do_upsert_hourly_event_counts(
            results,
            cursor,
            connection,
            brand_name,
            start_date,
            end_date,
            "ci_events",
            "checkout-initiated ci_events",
        )
        return

    try:
        with get_db_cursor(brand_index) as (c, conn):
            _do_upsert_hourly_event_counts(
                results,
                c,
                conn,
                brand_name,
                start_date,
                end_date,
                "ci_events",
                "checkout-initiated ci_events",
            )
    except Exception as e:
        logger.error(f"Error updating checkout-initiated ci_events for {brand_name}: {e}")


def sync_buy_now_events(
    brand_index: int,
    brand_name: str,
    start_date: date,
    end_date: date,
    brand_key: Optional[str] = None,
    cursor=None,
    connection=None,
):
    if not is_ci_events_brand(brand_name, brand_key):
        logger.info(
            f"Skipping buy-now Mongo sync for {brand_name}: brand not enabled for buy_now_events"
        )
        return

    mongo_shop_name = get_ci_events_mongo_shop_name(brand_name, brand_key)
    if not mongo_shop_name:
        logger.info(
            f"Skipping buy-now Mongo sync for {brand_name}: no Mongo shop_name mapping"
        )
        return

    tz = get_brand_timezone(brand_index)
    try:
        results = fetch_buy_now_hourly_counts_from_mongo(
            start_date=start_date,
            end_date=end_date,
            mongo_shop_name=mongo_shop_name,
            tz=tz,
        )
    except Exception as e:
        logger.error(f"Error fetching buy-now Mongo events for {brand_name}: {e}")
        return

    if cursor is not None:
        _do_upsert_hourly_event_counts(
            results,
            cursor,
            connection,
            brand_name,
            start_date,
            end_date,
            "buy_now_events",
            "buy-now events",
        )
        return

    try:
        with get_db_cursor(brand_index) as (c, conn):
            _do_upsert_hourly_event_counts(
                results,
                c,
                conn,
                brand_name,
                start_date,
                end_date,
                "buy_now_events",
                "buy-now events",
            )
    except Exception as e:
        logger.error(f"Error updating buy-now events for {brand_name}: {e}")


def _fetch_event_variant_counts_from_mongo(
    start_date: date,
    end_date: date,
    mongo_shop_name: str,
    event_type: str,
    metric_key: str,
    tz: ZoneInfo = IST,
) -> List[Dict[str, Any]]:
    counts: Dict[Tuple[date, str], int] = {}
    counted_events = _iter_unique_daily_pair_events_from_mongo(
        start_date=start_date,
        end_date=end_date,
        mongo_shop_name=mongo_shop_name,
        event_type=event_type,
        tz=tz,
    )

    for event in counted_events:
        event_date = event["date"]
        for vid in event["variant_ids"]:
            key = (event_date, vid)
            counts[key] = counts.get(key, 0) + 1

    return [
        {
            "date": bucket_date,
            "variant_id": vid,
            metric_key: count,
        }
        for (bucket_date, vid), count in sorted(counts.items())
    ]


def fetch_checkout_initiated_variant_counts_from_mongo(
    start_date: date,
    end_date: date,
    mongo_shop_name: str,
    tz: ZoneInfo = IST,
) -> List[Dict[str, Any]]:
    return _fetch_event_variant_counts_from_mongo(
        start_date=start_date,
        end_date=end_date,
        mongo_shop_name=mongo_shop_name,
        event_type="checkout_initiated",
        metric_key="checkout_initiated_count",
        tz=tz,
    )


def fetch_buy_now_variant_counts_from_mongo(
    start_date: date,
    end_date: date,
    mongo_shop_name: str,
    tz: ZoneInfo = IST,
) -> List[Dict[str, Any]]:
    return _fetch_event_variant_counts_from_mongo(
        start_date=start_date,
        end_date=end_date,
        mongo_shop_name=mongo_shop_name,
        event_type="buy_now",
        metric_key="buy_now_count",
        tz=tz,
    )


def _do_upsert_variant_event_counts(
    results: List[Dict[str, Any]],
    cursor,
    connection,
    brand_name: str,
    start_date: date,
    end_date: date,
    table_name: str,
    column_name: str,
    success_label: str,
):
    cursor.execute(
        f"""
        DELETE FROM {table_name}
        WHERE date BETWEEN %s AND %s
        """,
        (start_date, end_date),
    )

    for row in results:
        cursor.execute(
            f"""
            INSERT INTO {table_name} (date, variant_id, {column_name})
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                {column_name} = VALUES({column_name})
            """,
            (row["date"], row["variant_id"], row[column_name]),
        )

    connection.commit()
    logger.info(
        f"Updated {success_label} for {brand_name} "
        f"({len(results)} variant-date rows, {start_date}..{end_date})"
    )


def sync_checkout_initiated_variant_events(
    brand_index: int,
    brand_name: str,
    start_date: date,
    end_date: date,
    brand_key: Optional[str] = None,
    cursor=None,
    connection=None,
):
    if not is_ci_events_brand(brand_name, brand_key):
        logger.info(
            f"Skipping checkout-initiated variant Mongo sync for {brand_name}: brand not enabled for ci_events"
        )
        return

    mongo_shop_name = get_ci_events_mongo_shop_name(brand_name, brand_key)
    if not mongo_shop_name:
        logger.info(
            f"Skipping checkout-initiated variant Mongo sync for {brand_name}: no Mongo shop_name mapping"
        )
        return

    tz = get_brand_timezone(brand_index)
    try:
        results = fetch_checkout_initiated_variant_counts_from_mongo(
            start_date=start_date,
            end_date=end_date,
            mongo_shop_name=mongo_shop_name,
            tz=tz,
        )
    except Exception as e:
        logger.error(
            f"Error fetching checkout-initiated variant Mongo events for {brand_name}: {e}"
        )
        return

    if cursor is not None:
        _do_upsert_variant_event_counts(
            results,
            cursor,
            connection,
            brand_name,
            start_date,
            end_date,
            "daily_ci_events_summary",
            "checkout_initiated_count",
            "checkout-initiated variant events",
        )
        return

    try:
        with get_db_cursor(brand_index) as (c, conn):
            _do_upsert_variant_event_counts(
                results,
                c,
                conn,
                brand_name,
                start_date,
                end_date,
                "daily_ci_events_summary",
                "checkout_initiated_count",
                "checkout-initiated variant events",
            )
    except Exception as e:
        logger.error(f"Error updating checkout-initiated variant events for {brand_name}: {e}")


def sync_buy_now_variant_events(
    brand_index: int,
    brand_name: str,
    start_date: date,
    end_date: date,
    brand_key: Optional[str] = None,
    cursor=None,
    connection=None,
):
    if not is_ci_events_brand(brand_name, brand_key):
        logger.info(
            f"Skipping buy-now variant Mongo sync for {brand_name}: brand not enabled for buy_now_events"
        )
        return

    mongo_shop_name = get_ci_events_mongo_shop_name(brand_name, brand_key)
    if not mongo_shop_name:
        logger.info(
            f"Skipping buy-now variant Mongo sync for {brand_name}: no Mongo shop_name mapping"
        )
        return

    tz = get_brand_timezone(brand_index)
    try:
        results = fetch_buy_now_variant_counts_from_mongo(
            start_date=start_date,
            end_date=end_date,
            mongo_shop_name=mongo_shop_name,
            tz=tz,
        )
    except Exception as e:
        logger.error(f"Error fetching buy-now variant Mongo events for {brand_name}: {e}")
        return

    if cursor is not None:
        _do_upsert_variant_event_counts(
            results,
            cursor,
            connection,
            brand_name,
            start_date,
            end_date,
            "daily_buy_now_events_summary",
            "buy_now_count",
            "buy-now variant events",
        )
        return

    try:
        with get_db_cursor(brand_index) as (c, conn):
            _do_upsert_variant_event_counts(
                results,
                c,
                conn,
                brand_name,
                start_date,
                end_date,
                "daily_buy_now_events_summary",
                "buy_now_count",
                "buy-now variant events",
            )
    except Exception as e:
        logger.error(f"Error updating buy-now variant events for {brand_name}: {e}")


# ---------------------------
# UTM Summary (ShopifyQL - NEW)
# ---------------------------


# Reworking the function to include day dimension
def fetch_shopify_utm_sessions_daily(
    shop_name: str,
    access_token: str,
    start_date: str,
    end_date: str,
) -> List[Dict[str, Any]]:
    if not shop_name or not access_token:
        return []

    # Increment end_date by 1 day because UNTIL is exclusive in ShopifyQL
    next_day = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )

    shopify_ql = f"""
    FROM sessions
      SHOW sessions, sessions_with_cart_additions, utm_source, day
      WHERE human_or_bot_session IN ('human', 'bot')
      GROUP BY day, utm_source
      SINCE {start_date} UNTIL {next_day}
      ORDER BY day DESC, sessions DESC
      LIMIT 10000
    VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values
    """

    ql_flat = shopify_ql.replace("\n", " ").strip().replace('"', '\\"')
    graphql_query = f'''{{ shopifyqlQuery(query: "{ql_flat}") {{ tableData {{ rows columns {{ name dataType }} }} }} }}'''

    # ... (boilerplate request logic same as above) ...
    # Simplified for brevity in this thought trace,
    # but in actual code I will use the full request logic.

    payload = {"query": graphql_query}
    url = f"https://{shop_name}.myshopify.com/admin/api/2025-10/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token,
    }

    session = _make_shopifyql_session()
    try:
        resp = session.post(url, headers=headers, data=json.dumps(payload), timeout=60)
        if resp.status_code != 200:
            return []
        body = resp.json()
        table_data = body.get("data", {}).get("shopifyqlQuery", {}).get("tableData")
        return _format_shopifyql_table(table_data)
    except:
        return []
    finally:
        try:
            session.close()
        except:
            pass


def process_and_upload_utm_summary(
    brand_index: int,
    brand_name: str,
    shop_name: str,
    access_token: str,
    min_date: str,
    max_date: str,
):
    # Fetch with DAY dimension
    raw_rows = fetch_shopify_utm_sessions_daily(
        shop_name, access_token, min_date, max_date
    )

    if not raw_rows:
        return

    # Aggregation buckets
    # Key: (date_str, category) -> {sessions, atc, source_details: {src_name: {sessions, atc}}}
    agg = {}
    
    # New: Daily bucket for overall traffic split (all categories combined)
    # Key: date_str -> {src_name: {sessions, atc}}
    daily_traffic = {}

    for r in raw_rows:
        d_val = r.get("day")
        if not d_val:
            continue
        date_str = str(d_val)

        raw_src = (r.get("utm_source") or "").strip()
        src = raw_src.lower()
        sessions = int(r.get("sessions") or 0)
        atc = int(r.get("sessions_with_cart_additions") or 0)

        category = "others"
        if not src or src == "none" or src == "null":
            category = "direct"
        elif (
            "instagram" in src
            or "facebook" in src
            or src == "ig"
            or src == "fb"
            or "insta" in src
            or "meta" in src
        ):
            category = "Meta"
        elif "google" in src:
            category = "Google"

        key = (date_str, category)
        if key not in agg:
            agg[key] = {"sessions": 0, "atc": 0, "source_details": {}}

        agg[key]["sessions"] += sessions
        agg[key]["atc"] += atc

        # Store individual source details for accurately tracking "others" etc.
        src_name_key = (
            raw_src if raw_src and raw_src.lower() not in ["none", "null"] else category
        )
        if src_name_key not in agg[key]["source_details"]:
            agg[key]["source_details"][src_name_key] = {
                "utm_name": src_name_key,
                "sessions": 0,
                "atc_sessions": 0,
            }

        agg[key]["source_details"][src_name_key]["sessions"] += sessions
        agg[key]["source_details"][src_name_key]["atc_sessions"] += atc

        # New: Track for overall_traffic_split
        if date_str not in daily_traffic:
            daily_traffic[date_str] = {}
        
        if src_name_key not in daily_traffic[date_str]:
            daily_traffic[date_str][src_name_key] = {
                "utm_name": src_name_key,
                "sessions": 0,
                "atc_sessions": 0,
            }
        daily_traffic[date_str][src_name_key]["sessions"] += sessions
        daily_traffic[date_str][src_name_key]["atc_sessions"] += atc

    # Convert to list for DB insert
    db_rows = []
    traffic_split_rows = []
    
    all_dates = sorted(list(set(k[0] for k in agg.keys())))
    categories = ["Meta", "Google", "direct", "others"]

    for d in all_dates:
        # Prepare category-wise rows
        for cat in categories:
            stats = agg.get((d, cat), {"sessions": 0, "atc": 0, "source_details": {}})
            utm_names_list = list(stats.get("source_details", {}).values())
            row_tuple = (
                d,
                cat,
                stats["sessions"],
                stats["atc"],
                json.dumps(utm_names_list),
            )
            db_rows.append(row_tuple)
        
        # Prepare daily traffic split row
        if d in daily_traffic:
            total_utm_list = list(daily_traffic[d].values())
            traffic_split_rows.append((d, json.dumps(total_utm_list)))

    if not db_rows:
        return

    with get_db_cursor(brand_index) as (cursor, connection):
        # 1. Ensure tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS overall_utm_summary (
                date DATE NOT NULL,
                utm_source VARCHAR(255),
                utm_source_sessions INT DEFAULT 0,
                utm_source_atc_sessions INT DEFAULT 0,
                utm_names JSON,
                PRIMARY KEY (date, utm_source)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS overall_traffic_split (
                date DATE PRIMARY KEY,
                utm_source JSON
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # 2. Delete existing for dates
        if all_dates:
            format_strings = ",".join(["%s"] * len(all_dates))
            cursor.execute(
                f"DELETE FROM overall_utm_summary WHERE date IN ({format_strings})",
                tuple(all_dates),
            )
            cursor.execute(
                f"DELETE FROM overall_traffic_split WHERE date IN ({format_strings})",
                tuple(all_dates),
            )

        # 3. Insert new
        sql_summary = """
            INSERT INTO overall_utm_summary
            (date, utm_source, utm_source_sessions, utm_source_atc_sessions, utm_names)
            VALUES (%s, %s, %s, %s, %s)
        """
        executemany_chunked(cursor, connection, sql_summary, db_rows)

        if traffic_split_rows:
            sql_split = """
                INSERT INTO overall_traffic_split (date, utm_source)
                VALUES (%s, %s)
            """
            executemany_chunked(cursor, connection, sql_split, traffic_split_rows)
        logger.info(
            f"✅ Updated overall_utm_summary & overall_traffic_split for {brand_name} ({len(db_rows)} categories, {len(traffic_split_rows)} traffic split rows, {min_date}..{max_date})"
        )


def _normalize_product_traffic_source(raw_source: Any) -> str:
    value = (str(raw_source).strip() if raw_source is not None else "")
    if not value or value.lower() in {"none", "null"}:
        return "direct"
    return value


def update_product_traffic_split_incremental(
    cursor,
    connection,
    brand_name: str,
    target_dates: Iterable[str],
):
    placeholders, params = _dates_in_clause(target_dates)
    if not params:
        logger.debug(f"[{brand_name}] product_traffic_split: no dates to rebuild")
        return

    with timed(
        f"[{brand_name}] product_traffic_split incremental ({len(params)} date(s))"
    ):
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS product_traffic_split_stage LIKE product_traffic_split"
        )
        cursor.execute("TRUNCATE TABLE product_traffic_split_stage")

        cursor.execute(
            f"""
            SELECT
                `date`,
                product_id,
                utm_source,
                SUM(COALESCE(sessions, 0)) AS sessions,
                SUM(COALESCE(sessions_with_cart_additions, 0)) AS atc_sessions
            FROM product_sessions_snapshot
            WHERE `date` IN ({placeholders})
              AND product_id IS NOT NULL
              AND TRIM(product_id) <> ''
            GROUP BY `date`, product_id, utm_source
            """,
            params,
        )
        source_rows = cursor.fetchall()

        aggregated: Dict[Tuple[str, str], Dict[str, Dict[str, int]]] = {}
        for row in source_rows:
            if isinstance(row, dict):
                date_val = row.get("date")
                product_id = row.get("product_id")
                utm_source = row.get("utm_source")
                sessions = row.get("sessions")
                atc_sessions = row.get("atc_sessions")
            else:
                date_val, product_id, utm_source, sessions, atc_sessions = row

            if not date_val or not product_id:
                continue

            key = (str(date_val), str(product_id).strip())
            normalized_source = _normalize_product_traffic_source(utm_source)
            product_sources = aggregated.setdefault(key, {})
            stats = product_sources.setdefault(
                normalized_source,
                {
                    "utm_name": normalized_source,
                    "sessions": 0,
                    "atc_sessions": 0,
                },
            )
            stats["sessions"] += int(sessions or 0)
            stats["atc_sessions"] += int(atc_sessions or 0)

        stage_rows = []
        for (date_str, product_id), source_map in aggregated.items():
            sorted_sources = sorted(
                source_map.values(),
                key=lambda item: (-item["sessions"], item["utm_name"]),
            )
            stage_rows.append((date_str, product_id, json.dumps(sorted_sources)))

        if stage_rows:
            executemany_chunked(
                cursor,
                connection,
                """
                INSERT INTO product_traffic_split_stage (date, product_id, utm_source)
                VALUES (%s, %s, %s)
                """,
                stage_rows,
                commit_between_chunks=False,
            )
        else:
            connection.commit()

        cursor.execute("START TRANSACTION")
        cursor.execute(
            f"DELETE FROM product_traffic_split WHERE date IN ({placeholders})",
            params,
        )
        if stage_rows:
            cursor.execute(
                f"""
                INSERT INTO product_traffic_split (date, product_id, utm_source)
                SELECT date, product_id, utm_source
                FROM product_traffic_split_stage
                WHERE date IN ({placeholders})
                """,
                params,
            )
        connection.commit()
        logger.info(
            f"âœ… Updated product_traffic_split for {brand_name} ({len(stage_rows)} rows across {len(params)} date(s))"
        )


def refresh_product_traffic_split_for_window(
    brand_index: int,
    brand_name: str,
    min_date: str,
    max_date: str,
):
    with get_db_cursor(brand_index) as (cursor, connection):
        ensure_summary_tables(cursor, connection)
        cursor.execute(
            """
            SELECT DISTINCT `date`
            FROM product_sessions_snapshot
            WHERE `date` BETWEEN %s AND %s
            ORDER BY `date`
            """,
            (min_date, max_date),
        )
        rows = cursor.fetchall()
        target_dates = set()
        for row in rows:
            value = row.get("date") if isinstance(row, dict) else row[0]
            if value:
                target_dates.add(str(value))

        update_product_traffic_split_incremental(
            cursor,
            connection,
            brand_name,
            target_dates,
        )


def fetch_shopify_referrer_sessions_daily(
    shop_name: str,
    access_token: str,
    start_date: str,
    end_date: str,
) -> List[Dict[str, Any]]:
    if not shop_name or not access_token:
        return []

    # Increment end_date by 1 day because UNTIL is exclusive in ShopifyQL
    next_day = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )

    shopify_ql = f"""
    FROM sessions
      SHOW sessions, sessions_with_cart_additions, referrer_name, day
      WHERE human_or_bot_session IN ('human', 'bot')
      GROUP BY day, referrer_name
      SINCE {start_date} UNTIL {next_day}
      ORDER BY day DESC, sessions DESC
      LIMIT 10000
    VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values
    """

    ql_flat = shopify_ql.replace("\n", " ").strip().replace('"', '\\"')
    graphql_query = f'''{{ shopifyqlQuery(query: "{ql_flat}") {{ tableData {{ rows columns {{ name dataType }} }} }} }}'''

    payload = {"query": graphql_query}
    url = f"https://{shop_name}.myshopify.com/admin/api/2025-10/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token,
    }

    session = _make_shopifyql_session()
    try:
        resp = session.post(url, headers=headers, data=json.dumps(payload), timeout=60)
        if resp.status_code != 200:
            return []
        body = resp.json()
        table_data = body.get("data", {}).get("shopifyqlQuery", {}).get("tableData")
        return _format_shopifyql_table(table_data)
    except:
        return []
    finally:
        try:
            session.close()
        except:
            pass


def process_and_upload_referrer_summary(
    brand_index: int,
    brand_name: str,
    shop_name: str,
    access_token: str,
    min_date: str,
    max_date: str,
):
    raw_rows = fetch_shopify_referrer_sessions_daily(
        shop_name, access_token, min_date, max_date
    )

    if not raw_rows:
        return

    # Aggregation buckets
    # Key: (date_str, category) -> {sessions, atc, source_details: {src_name: {sessions, atc}}}
    agg = {}

    for r in raw_rows:
        d_val = r.get("day")
        if not d_val:
            continue
        date_str = str(d_val)

        raw_ref = (r.get("referrer_name") or "").strip()
        ref = raw_ref.lower()
        sessions = int(r.get("sessions") or 0)
        atc = int(r.get("sessions_with_cart_additions") or 0)

        category = "others"
        if not ref or ref == "none" or ref == "null":
            category = "direct"
        elif (
            "instagram" in ref
            or "facebook" in ref
            or ref == "ig"
            or ref == "fb"
            or "meta" in ref
        ):
            category = "Meta"
        elif "google" in ref:
            category = "Google"

        key = (date_str, category)
        if key not in agg:
            agg[key] = {"sessions": 0, "atc": 0, "source_details": {}}

        agg[key]["sessions"] += sessions
        agg[key]["atc"] += atc

        # Store individual source details
        ref_name_key = (
            raw_ref if raw_ref and raw_ref.lower() not in ["none", "null"] else category
        )
        if ref_name_key not in agg[key]["source_details"]:
            agg[key]["source_details"][ref_name_key] = {
                "referrer_name": ref_name_key,
                "sessions": 0,
                "atc_sessions": 0,
            }

        agg[key]["source_details"][ref_name_key]["sessions"] += sessions
        agg[key]["source_details"][ref_name_key]["atc_sessions"] += atc

    # Convert to list for DB insert
    db_rows = []
    all_dates = sorted(list(set(k[0] for k in agg.keys())))
    categories = ["Meta", "Google", "direct", "others"]

    for d in all_dates:
        for cat in categories:
            stats = agg.get((d, cat), {"sessions": 0, "atc": 0, "source_details": {}})
            referrer_names_list = list(stats.get("source_details", {}).values())

            row_tuple = (
                d,
                cat,
                stats["sessions"],
                stats["atc"],
                json.dumps(referrer_names_list),
            )
            db_rows.append(row_tuple)

    if not db_rows:
        return

    with get_db_cursor(brand_index) as (cursor, connection):
        # 1. Ensure table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS overall_referrer_summary (
                date DATE NOT NULL,
                referrer_name VARCHAR(255),
                referrer_sessions INT DEFAULT 0,
                referrer_atc_sessions INT DEFAULT 0,
                referrer_names JSON,
                PRIMARY KEY (date, referrer_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # 2. Delete existing for dates
        if all_dates:
            format_strings = ",".join(["%s"] * len(all_dates))
            cursor.execute(
                f"DELETE FROM overall_referrer_summary WHERE date IN ({format_strings})",
                tuple(all_dates),
            )

        # 3. Insert new
        sql = """
            INSERT INTO overall_referrer_summary
            (date, referrer_name, referrer_sessions, referrer_atc_sessions, referrer_names)
            VALUES (%s, %s, %s, %s, %s)
        """
        executemany_chunked(cursor, connection, sql, db_rows)
        logger.info(
            f"✅ Updated overall_referrer_summary for {brand_name} ({len(db_rows)} rows, {min_date}..{max_date})"
        )


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
            if is_backfill_active_for(brand_index):
                backfill_start_dt, backfill_end_dt = get_backfill_window_for_brand(brand_index)
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
            if is_backfill_active_for(brand_index):
                backfill_start_dt, backfill_end_dt = get_backfill_window_for_brand(brand_index)
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

    if DISCOUNT_DASHBOARD_ONLY_MODE:
        execute_discount_dashboard_backfill_only(brand_index, brand_name)
        log_db_connection_metrics(f"DB_CONNECTION_METRICS_BRAND_COMPLETE brand={brand_name}")
        _db_context.job_id = None
        return

    tz = get_brand_timezone(brand_index)
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

            if is_backfill_active_for(brand_index):
                # Use the configured backfill window in the brand's store timezone.
                start_dt, end_dt = get_backfill_window_for_brand(brand_index)
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

            if process["type"] == "NEW" and INVENTORY_PIPELINE_SWITCH:
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

        if DISCOUNT_DASHBOARD_BACKFILL:
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
                (now_ist(),),
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
        today_iso = now_ist().date().isoformat()
        u_min = min(affected_dates) if affected_dates else today_iso
        u_max = max(affected_dates) if affected_dates else today_iso

        if is_backfill_active_for(brand_index):
            backfill_start_dt, backfill_end_dt = get_backfill_window_for_brand(brand_index)
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

    job_start_time = now_ist()
    job_id = f"job-{uuid.uuid4().hex[:8]}"
    logger.info(
        f"\n{'=' * 60}\nJOB TRIGGERED AT: {job_start_time.strftime('%Y-%m-%d %I:%M:%S %p')}\nJOB ID: {job_id}\nMANUAL TRIGGER: {bool(manual_run_id)}\n{'=' * 60}"
    )

    if manual_run_id and MANAGER_STARTED_ENDPOINT:
        try:
            started_payload = {
                "runId": manual_run_id,
                "pipelineId": PIPELINE_ID,
                "status": "running"
            }
            logger.info(f"📤 Sending manual trigger STARTED webhook to {MANAGER_STARTED_ENDPOINT}")
            http_session.post(
                MANAGER_STARTED_ENDPOINT,
                json=started_payload,
                headers={"x-pipeline-key": PIPELINE_AUTH_HEADER},
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
        if BACKFILL_MODE and BACKFILL_BRAND_INDICES:
            target_indices = [
                i for i in active_brand_indices if i in BACKFILL_BRAND_INDICES
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

        end = now_ist()
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
        if manual_run_id and succeeded and MANAGER_COMPLETED_ENDPOINT:
            try:
                completed_payload = {
                    "runId": manual_run_id,
                    "pipelineId": PIPELINE_ID,
                    "status": "completed"
                }
                logger.info(f"📤 Sending manual trigger COMPLETED webhook to {MANAGER_COMPLETED_ENDPOINT}")
                http_session.post(
                    MANAGER_COMPLETED_ENDPOINT,
                    json=completed_payload,
                    headers={"x-pipeline-key": PIPELINE_AUTH_HEADER},
                    timeout=10
                )
            except Exception as e:
                logger.error(f"❌ Failed to send COMPLETED webhook: {e}")

        if manual_run_id and (not succeeded) and MANAGER_FAILED_ENDPOINT:
            try:
                recent_logs = _last_logs_handler.get_recent()
                error_body = "\n".join(recent_logs) if recent_logs else "No logs captured."
                error_code = _derive_error_code(failure_exception)
                failed_payload = {
                    "runId": manual_run_id,
                    "pipelineId": PIPELINE_ID,
                    "errorCode": error_code,
                    "errorBody": error_body,
                }
                logger.info(f"📤 Sending manual trigger FAILED webhook to {MANAGER_FAILED_ENDPOINT}")
                http_session.post(
                    MANAGER_FAILED_ENDPOINT,
                    json=failed_payload,
                    headers={"x-pipeline-key": PIPELINE_AUTH_HEADER},
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
    if not auth_key or auth_key != PIPELINE_AUTH_HEADER:
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
            if SELF_RUN:
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
