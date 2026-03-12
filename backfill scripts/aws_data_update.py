# -*- coding: utf-8 -*-
"""
Optimized & Hardened Shopify ETL Pipeline
(With returns_fact + corrected overall returns logic + optional backfill window)

Scope of change for this version:
- Keep `shopify_orders_update` exactly as is (still fetched with updated_at_min).
- Introduce `returns_fact` table.
- Populate `returns_fact` directly from order snapshots (cancelled_at, refunds[].transactions).
- Change ONLY the summary SQL to compute `overall_returns` and `actual_overall_sales`
  from `returns_fact` (leave per-app return splits as before).
- Add ENV-based backfill window override for orders fetch:
    BACKFILL_MODE=true|false
    BACKFILL_START_IST=YYYY-MM-DD[THH:MM:SS]
    BACKFILL_END_IST=YYYY-MM-DD[THH:MM:SS]

Performance optimizations (no business logic change):
- Summaries are recomputed only for affected dates in the current run (delete+insert upsert),
  not rebuilt for entire history.
- All summaries use generated+indexed columns: created_dt, created_hr, updated_dt.
"""

import asyncio
import aiohttp
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import pandas as pd
import numpy as np
import os
import json
import time
import logging
import traceback
from typing import Dict, List, Optional, Tuple, Any, Set
from functools import lru_cache
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

import mysql.connector
from mysql.connector import pooling

from sqlalchemy import (
    create_engine, Table, Column, MetaData, String, Integer, Float, DateTime, Text
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import QueuePool

from apscheduler.schedulers.background import BackgroundScheduler
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import base64

# ---- Logging ----
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

# ---- Globals ----
IST = ZoneInfo("Asia/Kolkata")
CPU_COUNT = multiprocessing.cpu_count()

brand_tag_to_index_map: Dict[str, int] = {}
db_connection_pools: Dict[int, pooling.MySQLConnectionPool] = {}
sqlalchemy_engines: Dict[int, Any] = {}
active_brand_indices: List[int] = []  # only brands with valid pools/engines

# HTTP Session (requests) with pooling/retries (used for sessions API)
http_session = requests.Session()
retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(pool_connections=20, pool_maxsize=50, max_retries=retry_strategy, pool_block=False)
http_session.mount("https://", adapter)
http_session.mount("http://", adapter)

# ---------------------------
# Optional Backfill Config (ENV-driven; no effect unless enabled)
# ---------------------------
def _parse_backfill_dt(val: Optional[str]) -> Optional[datetime]:
    if not val:
        return None
    try:
        if "T" in val:
            dt = datetime.fromisoformat(val)
        else:
            dt = datetime.strptime(val, "%Y-%m-%d")
        return dt.replace(tzinfo=IST)
    except Exception:
        logger.warning(f"BACKFILL_* value '{val}' not parseable; ignoring.")
        return None

BACKFILL_MODE = os.environ.get("BACKFILL_MODE", "false").strip().lower() == "true"
BACKFILL_START_IST = _parse_backfill_dt(os.environ.get("BACKFILL_START_IST"))
BACKFILL_END_IST = _parse_backfill_dt(os.environ.get("BACKFILL_END_IST"))

if BACKFILL_MODE:
    if not BACKFILL_START_IST or not BACKFILL_END_IST:
        logger.warning("BACKFILL_MODE enabled but BACKFILL_START_IST/BACKFILL_END_IST missing or invalid. Backfill will be ignored for this run.")
        BACKFILL_MODE = False
    elif BACKFILL_START_IST >= BACKFILL_END_IST:
        logger.warning("BACKFILL_* start >= end. Disabling backfill for safety.")
        BACKFILL_MODE = False
    else:
        logger.info(f"🔁 BACKFILL_MODE ON :: {BACKFILL_START_IST} → {BACKFILL_END_IST} (IST)")

# --- TEST_MODE ---
TEST_MODE = os.environ.get("TEST_MODE", "false").strip().lower() == "true"
if TEST_MODE:
    from dotenv import load_dotenv
    load_dotenv(".env.local", override=True)
    logger.info("⚠️ TEST_MODE enabled: QStash and DB SSL will be DISABLED, loading config from local env (.env.local).")
def now_ist() -> datetime:
    return datetime.now(IST)

def ts() -> str:
    return now_ist().strftime('%Y-%m-%d %H:%M:%S %Z')

@contextmanager
def timed(label: str):
    t0 = time.perf_counter()
    try:
        yield
    finally:
        dt = time.perf_counter() - t0
        logger.info(f"⏱️ {label} took {dt:.2f}s")

def enable_session_profiling(cursor) -> bool:
    try:
        cursor.execute("SET SESSION profiling = 1")
        return True
    except mysql.connector.Error:
        return False

def print_session_profiles(cursor, top_n=5):
    try:
        cursor.execute("SHOW PROFILES")
        rows = cursor.fetchall()
        if not rows:
            return
        logger.info("── SHOW PROFILES (last few) ──")
        for qid, time_taken, qtext in rows[-top_n:]:
            logger.info(f"  #{qid} {time_taken:.4f}s  {qtext[:160]}")
    except mysql.connector.Error:
        pass

def _make_shopifyql_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["POST"])
    ad = HTTPAdapter(pool_connections=5, pool_maxsize=10, max_retries=retry)
    s.mount("https://", ad)
    s.mount("http://", ad)
    return s

def _format_shopifyql_table(table_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not table_data: return []
    columns = table_data.get("columns") or []
    rows = table_data.get("rows") or []
    if not rows or not columns: return []
    formatted = []
    for row in rows:
        if isinstance(row, dict):
            obj = {}
            for col in columns:
                name = col.get("name")
                dtype = (col.get("dataType") or "").upper()
                val = row.get(name)
                if dtype in ("INTEGER", "NUMERIC", "DECIMAL"):
                    try: obj[name] = int(val) if dtype == "INTEGER" else float(val)
                    except: obj[name] = 0
                else: obj[name] = val
            formatted.append(obj)
        elif isinstance(row, (list, tuple)):
            obj = {}
            for idx, val in enumerate(row):
                if idx < len(columns):
                    col = columns[idx]
                    name = col.get("name")
                    dtype = (col.get("dataType") or "").upper()
                    if dtype in ("INTEGER", "NUMERIC", "DECIMAL"):
                        try: obj[name] = int(val) if dtype == "INTEGER" else float(val)
                        except: obj[name] = 0
                    else: obj[name] = val
            formatted.append(obj)
    return formatted

def fetch_shopify_sessions_via_shopifyql(shop_name: str, access_token: str, start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
    date_clause = f"SINCE {start_date} UNTIL {(datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')}" if start_date and end_date else "DURING today"
    ql = f"FROM sessions SHOW sessions, sessions_with_cart_additions, day GROUP BY day WITH CURRENCY 'INR' {date_clause} ORDER BY day ASC LIMIT 10000 VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values"
    payload = {"query": f'query {{ shopifyqlQuery(query: "{ql}") {{ tableData {{ rows columns {{ name dataType displayName }} }} parseErrors }} }}'}
    session = _make_shopifyql_session()
    try:
        resp = session.post(f"https://{shop_name}.myshopify.com/admin/api/2025-10/graphql.json", headers={"Content-Type": "application/json", "X-Shopify-Access-Token": access_token}, json=payload, timeout=60)
        if resp.status_code != 200: return []
        data = resp.json().get("data", {}).get("shopifyqlQuery", {})
        if data.get("parseErrors") or not data.get("tableData"): return []
        rows = _format_shopifyql_table(data["tableData"])
        return [{"date": str(r.get("day")), "sessions": int(r.get("sessions", 0)), "atc_sessions": int(r.get("sessions_with_cart_additions", 0))} for r in rows if r.get("day")]
    except: return []
    finally: session.close()

def fetch_shopify_hourly_sessions_via_shopifyql(shop_name: str, access_token: str, start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
    date_clause = f"SINCE {start_date} UNTIL {(datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')}" if start_date and end_date else "DURING today"
    ql = f"FROM sessions SHOW sessions, sessions_with_cart_additions, hour, day, session_device_type WHERE landing_page_path IS NOT NULL AND human_or_bot_session IN ('human', 'bot') GROUP BY day, hour, session_device_type WITH CURRENCY 'INR' {date_clause} ORDER BY day ASC, hour ASC LIMIT 10000 VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values"
    ql_flat = ql.replace("\n", " ").strip().replace('"', '\\"')
    payload = {"query": f'query {{ shopifyqlQuery(query: "{ql_flat}") {{ tableData {{ rows columns {{ name dataType displayName }} }} parseErrors }} }}'}
    session = _make_shopifyql_session()
    try:
        resp = session.post(f"https://{shop_name}.myshopify.com/admin/api/2025-10/graphql.json", headers={"Content-Type": "application/json", "X-Shopify-Access-Token": access_token}, json=payload, timeout=60)
        if resp.status_code != 200: return []
        data = resp.json().get("data", {}).get("shopifyqlQuery", {})
        if data.get("parseErrors") or not data.get("tableData"): return []
        rows = _format_shopifyql_table(data["tableData"])
        agg = {}
        for r in rows:
            h = r.get("hour")
            if not h: continue
            try:
                if "T" in str(h):
                    dt_ist = datetime.fromisoformat(str(h).replace("Z", "+00:00")).astimezone(IST)
                    d, hr = dt_ist.date().isoformat(), dt_ist.hour
                else: d, hr = str(r.get("day")), int(h)
                key = (d, hr)
                if key not in agg: agg[key] = {"date": d, "hour": hr, "mobile_sessions": 0, "mobile_atc_sessions": 0, "desktop_sessions": 0, "desktop_atc_sessions": 0, "tablet_sessions": 0, "tablet_atc_sessions": 0, "other_sessions": 0, "other_atc_sessions": 0}
                dev = (r.get("session_device_type") or "other").lower()
                s, a = int(r.get("sessions", 0)), int(r.get("sessions_with_cart_additions", 0))
                if dev == "mobile": agg[key]["mobile_sessions"] += s; agg[key]["mobile_atc_sessions"] += a
                elif dev == "desktop": agg[key]["desktop_sessions"] += s; agg[key]["desktop_atc_sessions"] += a
                elif dev == "tablet": agg[key]["tablet_sessions"] += s; agg[key]["tablet_atc_sessions"] += a
                else: agg[key]["other_sessions"] += s; agg[key]["other_atc_sessions"] += a
            except: continue
        return list(agg.values())
    except: return []
    finally: session.close()

def exec_timed(cursor, sql, params=None, label=None, show_profile=False):
    if label is None:
        label = sql.splitlines()[0][:90]
    with timed(label):
        cursor.execute(sql) if params is None else cursor.execute(sql, params)
    if show_profile:
        print_session_profiles(cursor)

# ---------------------------
# API-Driven Config & Decryption (NEW)
# ---------------------------
GET_BRANDS_API = os.environ.get("GET_BRANDS_API")
PIPELINE_AUTH_HEADER = os.environ.get("PIPELINE_AUTH_HEADER")
PASSWORD_AES_KEY = os.environ.get("PASSWORD_AES_KEY")
ACTIVE_BRAND_IDS = [b.strip() for b in os.environ.get("ACTIVE_BRAND_IDS", "").split(",") if b.strip()]
TOTAL_CONFIG_COUNT = int(os.environ.get("TOTAL_CONFIG_COUNT", "0"))

def decrypt_value(encrypted_val: str) -> str:
    """
    Decrypts a value that was encrypted using AES-256-CBC.
    Expected format: 'iv_b64:ciphertext_b64'
    """
    if not encrypted_val or ":" not in encrypted_val:
        return encrypted_val

    if not PASSWORD_AES_KEY:
        logger.warning("PASSWORD_AES_KEY not found in environment. Returning raw value.")
        return encrypted_val

    try:
        iv_b64, ciphertext_b64 = encrypted_val.split(":", 1)
        iv = base64.b64decode(iv_b64)
        ciphertext = base64.b64decode(ciphertext_b64)

        # AES-256 requires 32-byte key
        key_bytes = PASSWORD_AES_KEY.encode("utf-8")
        if len(key_bytes) < 32:
            key_bytes = key_bytes.ljust(32, b'\0')
        elif len(key_bytes) > 32:
            key_bytes = key_bytes[:32]

        cipher = Cipher(algorithms.AES(key_bytes), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        padded_content = decryptor.update(ciphertext) + decryptor.finalize()

        # Unpadding (assuming standard PKCS7 style)
        padding_len = padded_content[-1]
        if padding_len < 1 or padding_len > 16:
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
        logger.error("❌ GET_BRANDS_API or PIPELINE_AUTH_HEADER not set in environment.")
        return {}

    headers = {"x-pipeline-key": PIPELINE_AUTH_HEADER}
    try:
        resp = requests.get(GET_BRANDS_API, headers=headers, timeout=30)
        if resp.status_code == 200:
            active_brands = resp.json()
            if isinstance(active_brands, dict):
                return active_brands
            logger.error(f"❌ Active brands API returned unexpected type: {type(active_brands)}")
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
        logger.error("❌ GET_BRANDS_API or PIPELINE_AUTH_HEADER not set in environment.")
        return None

    headers = {"x-pipeline-key": PIPELINE_AUTH_HEADER}
    # Lowercase brand_id because API discovery labels (Uppercase) might not match route expectations
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

def initialize_brand_configs():
    import tempfile
    import certifi
    from sqlalchemy import create_engine
    from sqlalchemy.pool import QueuePool

    def _trunc(val: Any, length: int = 4) -> str:
        s = str(val) if val is not None else ""
        if len(s) <= length: return s
        return f"{s[:length]}..."

    logger.info("============================================================")
    logger.info("🔍 INITIALIZING PIPELINE ENVIRONMENT")
    logger.info(f"   TEST_MODE: {TEST_MODE}")
    logger.info("============================================================")

    # --------------------------
    # Infrastructure Setup Helper
    # --------------------------
    def _setup_brand_infra(brand_idx, brand_name, brand_tag, db_host, db_user, db_password, db_port, db_name, shop_name, access_token, api_version, speed_key, app_map_str, ca_path, verify_cert, verify_identity):
        def _trunc(val: Any, length: int = 4) -> str:
            s = str(val) if val is not None else ""
            if len(s) <= length: return s
            return f"{s[:length]}..."

        logger.info(f"🔑 Config for {brand_name} ({'LOCAL' if TEST_MODE else 'API'}): "
                    f"host={_trunc(db_host, 12)}, user={_trunc(db_user)}, pass={_trunc(db_password)}, "
                    f"db={db_name}, token={_trunc(access_token, 8)}")

        if verify_identity and ("elb.amazonaws.com" in db_host or ("amazonaws.com" in db_host and "rds.amazonaws.com" not in db_host)):
            effective_verify_identity = False
        else:
            effective_verify_identity = verify_identity

        db_config = {
            "host": db_host, "port": db_port, "user": db_user, "password": db_password, "database": db_name,
            "connection_timeout": int(os.environ.get("DB_CONNECT_TIMEOUT_S", "10")),
        }
        
        # Local development fix: Disable SSL if host is localhost/127.0.0.1
        effective_ca_path = ca_path
        if db_host.lower() in ["localhost", "127.0.0.1"]:
            effective_ca_path = None

        if effective_ca_path:
            db_config.update({"ssl_ca": effective_ca_path, "ssl_verify_cert": verify_cert, "ssl_verify_identity": effective_verify_identity})

        # --------------------------
        # 1. Connection Pool (Retry with SSL fallback)
        # --------------------------
        try:
            pool = pooling.MySQLConnectionPool(
                pool_name=f"pool_{brand_idx}",
                pool_size=int(os.environ.get(f"DB_POOL_SIZE_{brand_idx}", "5")),
                pool_reset_session=True, **db_config
            )
            db_connection_pools[brand_idx] = pool
        except Exception as e:
            err_msg = str(e).lower()
            if effective_ca_path and ("ssl" in err_msg or "certificate verify failed" in err_msg):
                logger.warning(f"⚠️ SSL connection failed for {brand_name}. Retrying WITHOUT SSL fallback...")
                # Remove SSL config and retry
                db_config.pop("ssl_ca", None)
                db_config.pop("ssl_verify_cert", None)
                db_config.pop("ssl_verify_identity", None)
                try:
                    pool = pooling.MySQLConnectionPool(
                        pool_name=f"pool_{brand_idx}",
                        pool_size=int(os.environ.get(f"DB_POOL_SIZE_{brand_idx}", "5")),
                        pool_reset_session=True, **db_config
                    )
                    db_connection_pools[brand_idx] = pool
                    effective_ca_path = None # Mark as disabled for engine too
                except Exception as retry_e:
                    logger.error(f"❌ Pool error for {brand_name} (even without SSL): {retry_e}")
                    return
            else:
                logger.error(f"❌ Pool error for {brand_name}: {e}")
                return

        # --------------------------
        # 2. SQLAlchemy Engine (Retry with SSL fallback)
        # --------------------------
        try:
            mysql_connect_str = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?charset=utf8mb4"
            connect_args = {"connection_timeout": int(os.environ.get("DB_CONNECT_TIMEOUT_S", "10"))}
            if effective_ca_path:
                connect_args.update({"ssl_ca": effective_ca_path, "ssl_verify_cert": verify_cert, "ssl_verify_identity": effective_verify_identity})

            engine = create_engine(
                mysql_connect_str, connect_args=connect_args,
                poolclass=QueuePool, pool_size=int(os.environ.get(f"SA_POOL_SIZE_{brand_idx}", "1")),
                max_overflow=int(os.environ.get(f"SA_MAX_OVERFLOW_{brand_idx}", "0")),
                pool_pre_ping=True, pool_recycle=int(os.environ.get("SA_POOL_RECYCLE_S", "1800")), echo=False, future=True
            )
            # Test connectivity immediately for the engine
            from sqlalchemy import text
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            sqlalchemy_engines[brand_idx] = engine
        except Exception as e:
            err_msg = str(e).lower()
            if effective_ca_path and ("ssl" in err_msg or "certificate verify failed" in err_msg):
                logger.warning(f"⚠️ Engine SSL failed for {brand_name}. Retrying WITHOUT SSL fallback...")
                try:
                    engine = create_engine(
                        mysql_connect_str, connect_args={"connection_timeout": int(os.environ.get("DB_CONNECT_TIMEOUT_S", "10"))},
                        poolclass=QueuePool, pool_size=int(os.environ.get(f"SA_POOL_SIZE_{brand_idx}", "1")),
                        max_overflow=int(os.environ.get(f"SA_MAX_OVERFLOW_{brand_idx}", "0")),
                        pool_pre_ping=True, pool_recycle=int(os.environ.get("SA_POOL_RECYCLE_S", "1800")), echo=False, future=True
                    )
                    from sqlalchemy import text
                    with engine.connect() as conn:
                        conn.execute(text("SELECT 1"))
                    sqlalchemy_engines[brand_idx] = engine
                except Exception as retry_e:
                    logger.error(f"❌ Engine error for {brand_name} (even without SSL): {retry_e}")
                    db_connection_pools.pop(brand_idx, None)
                    return
            else:
                logger.error(f"❌ Engine error for {brand_name}: {e}")
                db_connection_pools.pop(brand_idx, None)
                return

        os.environ[f"BRAND_NAME_{brand_idx}"] = brand_name
        os.environ[f"SHOP_NAME_{brand_idx}"] = shop_name
        os.environ[f"ACCESS_TOKEN_{brand_idx}"] = access_token
        os.environ[f"API_VERSION_{brand_idx}"] = api_version
        os.environ[f"BRAND_TAG_{brand_idx}"] = brand_tag
        os.environ[f"DB_DATABASE_{brand_idx}"] = db_name
        os.environ[f"SPEED_KEY_{brand_idx}"] = speed_key
        os.environ[f"APP_ID_MAPPING_{brand_idx}"] = app_map_str

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
        if TEST_MODE: return None # No SSL in TEST_MODE
        mode = os.environ.get("DB_TLS_CA_MODE", "certifi").strip().lower()
        if mode == "certifi": return certifi.where()
        if mode == "rds":
            p = os.environ.get("RDS_CA_PATH")
            if p and os.path.exists(p): return p
            b64v = os.environ.get("RDS_CA_BUNDLE_B64")
            if b64v: return _write_b64_to_tempfile(b64v, prefix="rds-ca-")
            return None
        if mode == "custom":
            p = os.environ.get("CUSTOM_CA_PATH")
            if p and os.path.exists(p): return p
            b64v = os.environ.get("CUSTOM_CA_BUNDLE_B64")
            if b64v: return _write_b64_to_tempfile(b64v, prefix="custom-ca-")
            return None
        if mode == "none": return None
        return certifi.where()

    ca_path = _resolve_ca_bundle_path()
    verify_cert = os.environ.get("DB_SSL_VERIFY_CERT", "true").strip().lower() == "true"
    verify_identity = os.environ.get("DB_SSL_VERIFY_IDENTITY", "false").strip().lower() == "true"

    if TEST_MODE:
        logger.info("🧪 [TEST_MODE] Loading local config from .env...")

        for idx_str in ACTIVE_BRAND_IDS:
            try:
                i = int(idx_str)
                brand_name = os.environ.get(f"BRAND_NAME_{i}")
                if not brand_name: continue
                
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
                
                if not all([db_host, db_user, db_password, db_name, shop_name, access_token]):
                    logger.error(f"❌ Missing local env vars for brand index {i}")
                    continue
                
                _setup_brand_infra(brand_idx, brand_name, brand_tag, db_host, db_user, db_password, db_port, db_name, shop_name, access_token, api_version, speed_key, app_map_str, ca_path, verify_cert, verify_identity)
            except Exception as e:
                logger.error(f"❌ Local config error for {idx_str}: {e}")
                continue
    else:
        logger.info("📡 [PROD_MODE] Discovering active brands via API...")
        active_brands = fetch_active_brands()
        logger.info(f"🔍 Active brands: {active_brands}")

        for brand_id_key, brand_tag_value in active_brands.items():
            logger.info(f"🔍 Fetching API config for brand: {brand_tag_value} (brand_id={brand_id_key})")
            config = fetch_brand_config(brand_id_key)
            if not config: continue

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
                app_map_str = app_map_val if isinstance(app_map_val, str) else json.dumps(app_map_val)

                _setup_brand_infra(brand_idx, brand_name, brand_tag, db_host, db_user, db_password, db_port, db_name, shop_name, access_token, api_version, speed_key, app_map_str, ca_path, verify_cert, verify_identity)
            except Exception as e:
                logger.error(f"❌ API config error for {brand_tag_value}: {e}")
                continue

    logger.info(f"✅ Active brands initialized: {active_brand_indices}")

# ---------------------------
# Pooled DB helpers (with retry)
# ---------------------------
@contextmanager
def get_db_connection(brand_index: int, attempts: int = 10, sleep_s: float = 0.2):
    pool = db_connection_pools.get(brand_index)
    if not pool:
        raise ValueError(f"No connection pool for brand {brand_index}")

    last_err = None
    for _ in range(attempts):
        try:
            cnx = pool.get_connection()
            break
        except mysql.connector.errors.PoolError as e:
            last_err = e
            time.sleep(sleep_s)
    else:
        raise last_err or mysql.connector.errors.PoolError("Failed getting connection; pool exhausted")

    try:
        yield cnx
    finally:
        try:
            cnx.close()  # returns to pool
        except Exception:
            pass

@contextmanager
def get_db_cursor(brand_index: int, dictionary=True):
    with get_db_connection(brand_index) as connection:
        cursor = connection.cursor(dictionary=dictionary)
        try:
            yield cursor, connection
        finally:
            try:
                cursor.close()
            except Exception:
                pass

# ---------------------------
# Session metadata helpers
# ---------------------------
def get_last_fetch_timestamp(cursor, default_minutes_ago=60):
    cursor.execute("SELECT key_value FROM pipeline_metadata WHERE key_name = 'last_session_fetch_timestamp'")
    result = cursor.fetchone()
    if result and (result.get('key_value') if isinstance(result, dict) else result[0]):
        val = result['key_value'] if isinstance(result, dict) else result[0]
        return val.replace(tzinfo=IST)
    return now_ist() - timedelta(minutes=default_minutes_ago)

def update_last_fetch_timestamp(cursor, connection, new_timestamp):
    cursor.execute("""
        INSERT INTO pipeline_metadata (key_name, key_value)
        VALUES ('last_session_fetch_timestamp', %s)
        ON DUPLICATE KEY UPDATE key_value = VALUES(key_value);
    """, (new_timestamp,))
    connection.commit()

# ---------------------------
# Shopify orders fetching (async)
# ---------------------------
async def fetch_orders_async(api_base_url: str, access_token: str, start_date: str,
                             end_date: str, date_filter_field: str) -> List[Dict]:
    headers = {"X-Shopify-Access-Token": access_token}
    order_list: List[Dict] = []

    # Ensure refunds/cancel snapshots are present
    fields = (
        "id,name,created_at,updated_at,cancelled_at,total_price,financial_status,"
        "payment_gateway_names,app_id,refunds,refunds/created_at,refunds/transactions,"
        "refunds/transactions/amount,refunds/transactions/kind,refunds/transactions/status"
    )

    date_filter_field_max = date_filter_field.replace('_min', '_max')
    url = (
        f"{api_base_url}/orders.json?status=any&limit=250"
        f"&{date_filter_field}={start_date}&{date_filter_field_max}={end_date}"
        f"&fields={fields}"
    )

    connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
    timeout = aiohttp.ClientTimeout(total=300)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        while url:
            try:
                async with session.get(url, headers=headers) as response:
                    if response.status == 429:
                        retry_after = int(response.headers.get('Retry-After', 2))
                        logger.warning(f"Rate limited, waiting {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue

                    if response.status != 200:
                        logger.error(f"Failed to fetch data: {response.status}")
                        break

                    data = await response.json()
                    orders = data.get('orders', [])
                    if not orders:
                        break

                    order_list.extend(orders)

                    # parse Link header for next
                    link_header = response.headers.get('Link', '')
                    url = None
                    if 'rel="next"' in link_header:
                        for part in link_header.split(','):
                            if 'rel="next"' in part:
                                url = part.split(';')[0].strip('<> ')
                                break

                    await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Error fetching orders: {e}")
                break

    return order_list

def fetch_orders(api_base_url: str, access_token: str, start_date: str,
                 end_date: str, date_filter_field: str) -> List[Dict]:
    with timed(f"Shopify fetch ({date_filter_field} window)"):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(
                fetch_orders_async(api_base_url, access_token, start_date, end_date, date_filter_field)
            )
        finally:
            loop.close()

# ---------------------------
# Transform (optimized; logic preserved)
# ---------------------------
def convert_to_desired_format(dt_obj: datetime) -> str:
    iso_str = dt_obj.strftime('%Y-%m-%dT%H:%M:%S%z')
    return f"{iso_str[:22]}%2B05:30"

def convert_to_desired_format_session(dt_obj: datetime) -> str:
    iso_str = dt_obj.strftime('%Y-%m-%dT%H:%M:%S%z')
    return f"{iso_str[:19]}%2B05:30"

def extract_date_time(datetime_str: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not datetime_str:
        return None, None
    date_part, time_part = datetime_str.split('T')
    time_s = time_part.split('+')[0]
    return date_part, time_s

def format_datetime(datetime_str: Optional[str]) -> Optional[str]:
    if not datetime_str:
        return None
    return datetime_str.replace('Z', '+00:00') and datetime.fromisoformat(
        datetime_str.replace('Z', '+00:00')
    ).strftime('%Y-%m-%d %H:%M:%S')

@lru_cache(maxsize=10000)
def cached_format_datetime(datetime_str):
    return format_datetime(datetime_str)

def _iso_to_ist_date(iso_ts: Optional[str]) -> Optional[date]:
    if not iso_ts:
        return None
    dt = datetime.fromisoformat(iso_ts.replace('Z', '+00:00')).astimezone(IST)
    return dt.date()

def transform_orders_to_df_optimized(orders_list: List[Dict], app_mapping: Dict) -> pd.DataFrame:
    with timed("Transform orders → DataFrame (optimized)"):
        if not orders_list:
            return pd.DataFrame()

        order_data = []
        for order in orders_list:
            customer = order.get('customer') or {}
            shipping_address = order.get('shipping_address') or {}
            billing_address = order.get('billing_address') or {}

            disc_codes = order.get('discount_codes', []) or []
            discount_codes = ', '.join([c.get('code', '') for c in disc_codes if c]) or None
            total_discount_amount = sum(float(c.get('amount', '0')) for c in disc_codes if c)

            discount_apps = order.get('discount_applications', []) or []
            discount_app_titles = ', '.join([a.get('title', '') for a in discount_apps if a]) or None
            discount_app_values = ', '.join([str(a.get('value', '')) for a in discount_apps if a]) or None
            discount_app_types = ', '.join([app_mapping.get(str(a.get('app_id', '')), 'N/A') for a in discount_apps if a]) or None
            discount_app_ids = ', '.join([str(a.get('app_id', '')) for a in discount_apps if a]) or None

            order_app_id = order.get('app_id')
            order_app_name = app_mapping.get(str(order_app_id), str(order_app_id)) if order_app_id else None

            payment_gateway_names = ', '.join(order.get('payment_gateway_names', [])) or None

            created_at_str = order.get('created_at', '')
            updated_at_str = order.get('updated_at', '')
            created_date, created_time = extract_date_time(created_at_str)
            updated_date, updated_time = extract_date_time(updated_at_str)

            base = {
                "created_at": format_datetime(created_at_str),
                "created_date": created_date,
                "created_time": created_time,
                "order_id": str(order.get('id')) if order.get('id') else None,
                "order_name": order.get('name'),
                "customer_id": str(customer.get('id')) if customer.get('id') else None,
                "customer_email": customer.get('email'),
                "customer_first_name": customer.get('first_name'),
                "customer_last_name": customer.get('last_name'),
                "customer_phone": customer.get('phone'),
                "financial_status": order.get('financial_status'),
                "fulfillment_status": order.get('fulfillment_status') or 'Unfulfilled',
                "currency": order.get('currency'),
                "discount_codes": discount_codes,
                "discount_amount": total_discount_amount if total_discount_amount > 0 else None,
                "discount_application_titles": discount_app_titles,
                "discount_application_values": discount_app_values,
                "discount_application_types": discount_app_types,
                "discount_application_ids": discount_app_ids,
                "order_app_id": str(order_app_id) if order_app_id else None,
                "order_app_name": order_app_name,
                "total_price": float(order.get('total_price', 0)) if order.get('total_price') else None,
                "shipping_price": float(order.get('total_shipping_price_set', {}).get('shop_money', {}).get('amount', '0')) or None,
                "total_tax": float(order.get('current_total_tax', 0)) if order.get('current_total_tax') else None,
                "payment_gateway_names": payment_gateway_names,
                "total_discounts": float(order.get('total_discounts', 0)) if order.get('total_discounts') else None,
                "total_duties": float(order.get('total_duties', 0)) if order.get('total_duties') else None,
                "sku": None, "variant_title": None, "line_item": None, "line_item_price": None,
                "line_item_quantity": None, "line_item_total_discount": None, "product_id": None, "variant_id": None,
                "tags": order.get('tags') or None,
                "updated_at": format_datetime(updated_at_str),
                "updated_date": updated_date,
                "updated_time": updated_time,
                "orig_referrer": order.get('orig_referrer'),
                "full_url": order.get('full_url'),
                "customer_ip": order.get('customer_ip'),
                "pg_order_id": order.get('pg_order_id'),
                "shipping_address": shipping_address.get('address1'),
                "shipping_phone": shipping_address.get('phone'),
                "shipping_city": shipping_address.get('city'),
                "shipping_zip": shipping_address.get('zip'),
                "shipping_province": shipping_address.get('province'),
                "billing_address": billing_address.get('address1'),
                "billing_phone": billing_address.get('phone'),
                "billing_city": billing_address.get('city'),
                "billing_zip": billing_address.get('zip'),
                "billing_province": billing_address.get('province'),
                "customer_tag": customer.get('tags'),
                "appmaker_platform": order.get('appmaker_platform'),
                "app_version": order.get('app_version'),
            }

            line_items = order.get('line_items', [])
            if not line_items:
                order_data.append(base)
                continue

            for i, item in enumerate(line_items):
                if item is None:
                    continue

                row = base.copy() if i == 0 else {k: None for k in base.keys()}
                if i > 0:
                    row.update({
                        "created_date": created_date,
                        "created_time": created_time,
                        "order_name": order.get('name'),
                        "customer_id": str(customer.get('id')) if customer.get('id') else None,
                        "tags": order.get('tags') or None,
                        "customer_tag": customer.get('tags'),
                        "appmaker_platform": order.get('appmaker_platform'),
                        "app_version": order.get('app_version'),
                    })

                row.update({
                    "sku": item.get('sku'),
                    "variant_title": item.get('variant_title'),
                    "line_item": item.get('title'),
                    "line_item_price": float(item.get('price', 0)) if item.get('price') else None,
                    "line_item_quantity": int(item.get('quantity', 0)) if item.get('quantity') else None,
                    "line_item_total_discount": float(item.get('total_discount', 0)) if item.get('total_discount') else None,
                    "product_id": str(item.get('product_id')) if item.get('product_id') else None,
                    "variant_id": str(item.get('variant_id')) if item.get('variant_id') else None,
                })

                props = item.get('properties', []) or []
                for n in range(1, 10 + 1):
                    row.setdefault(f'_ITEM{n}_name', None)
                    row.setdefault(f'_ITEM{n}_value', None)
                for idx, prop in enumerate(props[:10]):
                    if prop and prop.get('name', '').startswith('_ITEM'):
                        value = (prop.get('value') or '').strip()
                        value_parts = value.split("SKU:")
                        row[f'_ITEM{idx + 1}_name'] = value_parts[0].strip() if len(value_parts) > 0 else None
                        row[f'_ITEM{idx + 1}_value'] = value_parts[1].strip() if len(value_parts) > 1 else None

                if i == 0:
                    for note in order.get('note_attributes', []):
                        if note and note.get('name') in row:
                            row[note['name']] = note.get('value')

                order_data.append(row)

        df = pd.DataFrame(order_data)
        df = df.replace({np.nan: None, 'N/A': None, '': None})
        return df

# ---------------------------
# Returns fact helpers (NEW)
# ---------------------------
def _ensure_returns_fact(cursor, connection):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS returns_fact (
          order_id     BIGINT        NOT NULL,
          event_date   DATE          NOT NULL,
          event_type   ENUM('CANCEL','REFUND') NOT NULL,
          amount       DECIMAL(12,2) NOT NULL,
          PRIMARY KEY (order_id, event_type, event_date),
          KEY idx_event_date (event_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)
    connection.commit()

def _parse_iso_to_ist_date(iso_ts: Optional[str]) -> Optional[str]:
    if not iso_ts:
        return None
    dt = datetime.fromisoformat(iso_ts.replace('Z', '+00:00'))
    return dt.astimezone(IST).date().isoformat()

def upsert_returns_fact_from_orders(brand_index: int, orders_list: List[Dict], cursor=None, connection=None) -> Set[date]:
    """Idempotently upsert CANCEL and REFUND events per order per day. Returns affected dates set."""
    affected_dates: Set[date] = set()
    if not orders_list:
        return affected_dates

    close_after = False
    if cursor is None or connection is None:
        close_after = True
        (cursor, connection) = next(get_db_cursor(brand_index))

    try:
        _ensure_returns_fact(cursor, connection)

        agg: Dict[Tuple[str, str, str], float] = {}

        for o in orders_list:
            oid = str(o.get('id')) if o.get('id') else None
            if not oid:
                continue

            cancelled_at = o.get('cancelled_at')
            if cancelled_at:
                d = _parse_iso_to_ist_date(cancelled_at)
                if d:
                    affected_dates.add(datetime.fromisoformat(d).date())
                    amt = float(o.get('total_price') or 0.0)
                    key = (oid, 'CANCEL', d)
                    agg[key] = max(agg.get(key, 0.0), amt)

            refunds = o.get('refunds') or []
            for rf in refunds:
                rdate = _parse_iso_to_ist_date(rf.get('created_at'))
                if not rdate:
                    continue
                affected_dates.add(datetime.fromisoformat(rdate).date())
                txns = rf.get('transactions') or []
                refund_sum = 0.0
                for t in txns:
                    kind = (t.get('kind') or '').lower()
                    if kind in ('refund', 'chargeback', 'return'):
                        try:
                            amt = float(t.get('amount') or 0.0)
                            refund_sum += abs(amt)
                        except Exception:
                            continue
                if refund_sum > 0:
                    key = (oid, 'REFUND', rdate)
                    agg[key] = agg.get(key, 0.0) + refund_sum

        if agg:
            rows = [(int(k[0]), k[2], k[1], round(v, 2)) for k, v in agg.items()]
            insert_sql = """
                INSERT INTO returns_fact (order_id, event_date, event_type, amount)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE amount = VALUES(amount);
            """
            cursor.executemany(insert_sql, rows)
            connection.commit()
            logger.info(f"✅ Upserted {len(rows)} rows into returns_fact")

        return affected_dates

    finally:
        if close_after:
            try:
                cursor.close()
            except Exception:
                pass

# ---------------------------
# DB reads/writes (accept cursor)
# ---------------------------
def get_orders_with_same_timestamp(brand_index: int, table_name: str, timestamp_value,
                                   timestamp_field='created_at', cursor=None) -> set:
    try:
        if cursor is None:
            with get_db_cursor(brand_index) as (c, _):
                c.execute(f"SELECT order_id FROM {table_name} WHERE {timestamp_field} = %s", (timestamp_value,))
                return {row['order_id'] for row in c.fetchall()}
        else:
            cursor.execute(f"SELECT order_id FROM {table_name} WHERE {timestamp_field} = %s", (timestamp_value,))
            return {row['order_id'] for row in cursor.fetchall()}
    except mysql.connector.Error as err:
        logger.error(f"Error checking existing orders in {table_name}: {err}")
        return set()

def _get_last_order_with_cursor(cursor, table_name: str) -> Optional[Dict]:
    order_by_col = 'updated_at' if 'update' in table_name else 'created_at'
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

def load_data_to_sql_optimized(df: pd.DataFrame, brand_index: int, table_name: str, batch_size: int = 1000):
    if df.empty:
        logger.info(f"DataFrame empty for {table_name}; nothing to load.")
        return

    engine = sqlalchemy_engines.get(brand_index)
    if not engine:
        logger.error(f"No SQLAlchemy engine for brand {brand_index}")
        return

    metadata = MetaData()
    columns = [
        Column('created_at', DateTime), Column('created_date', String(10)), Column('created_time', String(8)),
        Column('order_id', String(50)), Column('order_name', String(50)), Column('customer_id', String(50)),
        Column('customer_email', String(100)), Column('customer_first_name', String(100)), Column('customer_last_name', String(100)),
        Column('customer_phone', String(30)), Column('financial_status', String(50)), Column('fulfillment_status', String(50)),
        Column('currency', String(10)), Column('discount_codes', Text), Column('discount_amount', Float),
        Column('discount_application_titles', Text), Column('discount_application_values', Text),
        Column('discount_application_types', Text), Column('discount_application_ids', Text), Column('order_app_id', String(50)),
        Column('order_app_name', String(100)), Column('total_price', Float), Column('shipping_price', Float),
        Column('total_tax', Float), Column('payment_gateway_names', Text), Column('total_discounts', Float),
        Column('total_duties', Float), Column('sku', String(100)), Column('variant_title', String(100)),
        Column('line_item', String(255)), Column('line_item_price', Float), Column('line_item_quantity', Integer),
        Column('line_item_total_discount', Float), Column('product_id', String(50)),
        Column('variant_id', String(50)), Column('tags', Text), Column('updated_at', DateTime),
        Column('updated_date', String(10)), Column('updated_time', String(8)), Column('orig_referrer', Text),
        Column('full_url', Text), Column('customer_ip', String(50)), Column('pg_order_id', String(50)),
        Column('shipping_address', Text), Column('shipping_phone', String(30)), Column('shipping_city', String(100)),
        Column('shipping_zip', String(20)), Column('shipping_province', String(100)), Column('billing_address', Text),
        Column('billing_phone', String(30)), Column('billing_city', String(100)), Column('billing_zip', String(20)),
        Column('billing_province', String(100)), Column('customer_tag', Text), Column('appmaker_platform', String(50)),
        Column('app_version', String(50)),
    ]
    for n in range(1, 11):
        columns.append(Column(f'_ITEM{n}_name', String(255)))
        columns.append(Column(f'_ITEM{n}_value', String(255)))
    Table(table_name, metadata, *columns)

    with timed(f"DDL check/create for {table_name}"):
        try:
            metadata.create_all(engine, checkfirst=True)
        except SQLAlchemyError as e:
            logger.error(f"❌ Error creating table '{table_name}': {e}")
            return

    try:
        optimal_batch = max(500, min(batch_size, 2000))
        with timed(f"Insert {len(df)} rows into {table_name} (batch={optimal_batch})"):
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=optimal_batch,
            )
        logger.info(f"✅ Loaded {len(df)} rows to {table_name}")
    except Exception as e:
        logger.error(f"❌ Error loading data to '{table_name}': {e}")
        traceback.print_exc()

# ---------------------------
# Build→Upsert helpers (date-scoped)
# ---------------------------
def _delete_in(cursor, table: str, dates: List[date]):
    if not dates:
        return
    placeholders = ",".join(["%s"] * len(dates))
    sql = f"DELETE FROM {table} WHERE date IN ({placeholders})"
    exec_timed(cursor, sql, [d for d in dates], label=f"Delete {table} for affected dates")

# ---------------------------
# Email (unchanged behavior)
# ---------------------------
def send_email(subject: str, body: str, recipients: List[str], sender_email: str, sender_password: str):
    if not sender_email or not sender_password or not recipients:
        return
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, recipients, msg.as_string())
        server.quit()
        logger.info("✅ Email sent")
    except Exception as e:
        logger.error(f"❌ Email send failed: {e}")

# ---------------------------
# Sessions summary (logic preserved)
# ---------------------------
def update_sessions_summary(brand_index: int, brand_name: str, shop_name: str, api_version: str, access_token: str):
    try:
        with get_db_cursor(brand_index) as (cursor, connection):
            cursor.execute("CREATE TABLE IF NOT EXISTS sessions_summary (date DATE PRIMARY KEY, number_of_sessions INT DEFAULT 0, number_of_atc_sessions INT DEFAULT 0)")
            cursor.execute("CREATE TABLE IF NOT EXISTS hourly_sessions_summary (date DATE NOT NULL, hour TINYINT UNSIGNED NOT NULL, number_of_sessions INT DEFAULT 0, number_of_atc_sessions INT DEFAULT 0, mobile_sessions INT DEFAULT 0, mobile_atc_sessions INT DEFAULT 0, desktop_sessions INT DEFAULT 0, desktop_atc_sessions INT DEFAULT 0, tablet_sessions INT DEFAULT 0, tablet_atc_sessions INT DEFAULT 0, other_sessions INT DEFAULT 0, other_atc_sessions INT DEFAULT 0, PRIMARY KEY (date, hour))")
            cursor.execute("CREATE TABLE IF NOT EXISTS pipeline_metadata (key_name VARCHAR(50) PRIMARY KEY, key_value DATETIME)")
            connection.commit()

            start_dt = (now_ist().date() - timedelta(days=2)).isoformat()
            end_dt = now_ist().date().isoformat()
            if BACKFILL_MODE:
                start_dt = BACKFILL_START_IST.date().isoformat()
                end_dt = BACKFILL_END_IST.date().isoformat()

            # Daily
            daily = fetch_shopify_sessions_via_shopifyql(shop_name, access_token, start_dt, end_dt)
            for r in daily:
                cursor.execute("INSERT INTO sessions_summary (date, number_of_sessions, number_of_atc_sessions) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE number_of_sessions=VALUES(number_of_sessions), number_of_atc_sessions=VALUES(number_of_atc_sessions)", (r["date"], r["sessions"], r["atc_sessions"]))
            
            # Hourly
            hourly = fetch_shopify_hourly_sessions_via_shopifyql(shop_name, access_token, start_dt, end_dt)
            for r in hourly:
                tot_s = r["mobile_sessions"] + r["desktop_sessions"] + r["tablet_sessions"] + r["other_sessions"]
                tot_a = r["mobile_atc_sessions"] + r["desktop_atc_sessions"] + r["tablet_atc_sessions"] + r["other_atc_sessions"]
                cursor.execute("INSERT INTO hourly_sessions_summary (date, hour, number_of_sessions, number_of_atc_sessions, mobile_sessions, mobile_atc_sessions, desktop_sessions, desktop_atc_sessions, tablet_sessions, tablet_atc_sessions, other_sessions, other_atc_sessions) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE number_of_sessions=VALUES(number_of_sessions), number_of_atc_sessions=VALUES(number_of_atc_sessions), mobile_sessions=VALUES(mobile_sessions), mobile_atc_sessions=VALUES(mobile_atc_sessions), desktop_sessions=VALUES(desktop_sessions), desktop_atc_sessions=VALUES(desktop_atc_sessions), tablet_sessions=VALUES(tablet_sessions), tablet_atc_sessions=VALUES(tablet_atc_sessions), other_sessions=VALUES(other_sessions), other_atc_sessions=VALUES(other_atc_sessions)", (r["date"], r["hour"], tot_s, tot_a, r["mobile_sessions"], r["mobile_atc_sessions"], r["desktop_sessions"], r["desktop_atc_sessions"], r["tablet_sessions"], r["tablet_atc_sessions"], r["other_sessions"], r["other_atc_sessions"]))
            
            connection.commit()
            update_last_fetch_timestamp(cursor, connection, now_ist())
            logger.info(f"✅ Sessions updated (ShopifyQL) for {brand_name}")
    except Exception as e:
        logger.error(f"❌ Error updating sessions for {brand_name}: {e}")

# ---------------------------
# Summary queries (date-scoped upsert)  **UPDATED FOR returns_fact**
# ---------------------------
def execute_summary_queries(brand_index: int, brand_name: str, affected_dates: Set[date]):
    if not affected_dates:
        # Failsafe: recompute today to keep fresh even if no orders fetched
        affected_dates = {now_ist().date()}

    dates_list = sorted(list(affected_dates))
    logger.info(f"📅 Recomputing summaries for {brand_name} on dates: {dates_list}")

    try:
        with get_db_cursor(brand_index, dictionary=False) as (cursor, connection):
            connection.autocommit = True
            prof = enable_session_profiling(cursor)
            cursor.execute("SET SESSION lock_wait_timeout=60")
            cursor.execute("SET SESSION innodb_lock_wait_timeout=60")

            # Delete old rows for these dates (fast upsert pattern)
            for t in ("sales_summary", "order_summary", "discount_summary", "gross_summary", "hour_wise_sales", "overall_summary"):
                _delete_in(cursor, t, dates_list)

            # SALES SUMMARY (uses created_dt / updated_dt / returns_fact)
            sales_insert = """
            INSERT INTO sales_summary (
                date,
                gokwik_sales, gokwik_returns, actual_gokwik_sale,
                KwikEngageSales, KwikEngageReturns, actual_KwikEngage_sale,
                online_store_sales, online_store_returns, actual_online_store_sale,
                hypd_store_sales, hypd_store_returns, actual_hypd_store_sale,
                draft_order_sales, draft_order_returns, actual_draft_order_sale,
                dpanda_sales, dpanda_returns, actual_dpanda_sale,
                gkappbrew_sales, gkappbrew_returns, actual_gkappbrew_sale,
                buykaro_sales, buykaro_returns, actual_buykaro_sale,
                appbrewplus_sales, appbrewplus_returns, actual_appbrewplus_sale,
                shopflo_sales, shopflo_returns, actual_shopflo_sale,
                overall_sales_WO_hypd, overall_returns_WO_hypd, actual_overall_sales_WO_hypd,
                overall_sales, overall_returns, actual_overall_sales
            )
            WITH target_dates AS (
                SELECT %s AS d UNION ALL
                """ + " UNION ALL ".join(["SELECT %s"] * (len(dates_list)-1)) + """
            ),
            SalesData AS (
                SELECT
                    so.created_dt AS date,
                    SUM(CASE WHEN so.order_app_name = 'GoKwik' THEN so.total_price ELSE 0 END) AS gokwik_sales,
                    SUM(CASE WHEN so.order_app_name = 'KwikEngage' THEN so.total_price ELSE 0 END) AS kwik_engage_sales,
                    SUM(CASE WHEN so.order_app_name = 'Online Store' THEN so.total_price ELSE 0 END) AS online_store_sales,
                    SUM(CASE WHEN so.order_app_name = 'HYPD_store' THEN so.total_price ELSE 0 END) AS hypd_store_sales,
                    SUM(CASE WHEN so.order_app_name = 'Draft Order' THEN so.total_price ELSE 0 END) AS draft_order_sales,
                    SUM(CASE WHEN so.order_app_name = 'Dpanda' THEN so.total_price ELSE 0 END) AS dpanda_sales,
                    SUM(CASE WHEN so.order_app_name = 'GKAppbrew' THEN so.total_price ELSE 0 END) AS gkappbrew_sales,
                    SUM(CASE WHEN so.order_app_name = 'BuyKaro' THEN so.total_price ELSE 0 END) AS buykaro_sales,
                    SUM(CASE WHEN so.order_app_name = 'AppbrewPlus' THEN so.total_price ELSE 0 END) AS appbrewplus_sales,
                    SUM(CASE WHEN so.order_app_name = 'Shopflo' THEN so.total_price ELSE 0 END) AS shopflo_sales,
                    SUM(so.total_price) AS global_overall_sales,
                    SUM(CASE WHEN so.order_app_name != 'HYPD_store' THEN so.total_price ELSE 0 END) AS global_sales_WO_hypd
                FROM shopify_orders so
                JOIN target_dates td ON so.created_dt = td.d
                GROUP BY so.created_dt
            ),
            ReturnsData AS (
                SELECT
                    sou.updated_dt AS date,
                    SUM(CASE WHEN sou.order_app_name = 'GoKwik' THEN sou.total_price ELSE 0 END) AS gokwik_returns,
                    SUM(CASE WHEN sou.order_app_name = 'KwikEngage' THEN sou.total_price ELSE 0 END) AS kwik_engage_returns,
                    SUM(CASE WHEN sou.order_app_name = 'Online Store' THEN sou.total_price ELSE 0 END) AS online_store_returns,
                    SUM(CASE WHEN sou.order_app_name = 'HYPD_store' THEN sou.total_price ELSE 0 END) AS hypd_store_returns,
                    SUM(CASE WHEN sou.order_app_name = 'Draft Order' THEN sou.total_price ELSE 0 END) AS draft_order_returns,
                    SUM(CASE WHEN sou.order_app_name = 'Dpanda' THEN sou.total_price ELSE 0 END) AS dpanda_returns,
                    SUM(CASE WHEN sou.order_app_name = 'GKAppbrew' THEN sou.total_price ELSE 0 END) AS gkappbrew_returns,
                    SUM(CASE WHEN sou.order_app_name = 'BuyKaro' THEN sou.total_price ELSE 0 END) AS buykaro_returns,
                    SUM(CASE WHEN sou.order_app_name = 'AppbrewPlus' THEN sou.total_price ELSE 0 END) AS appbrewplus_returns,
                    SUM(CASE WHEN sou.order_app_name = 'Shopflo' THEN sou.total_price ELSE 0 END) AS shopflo_returns,
                    SUM(CASE WHEN sou.order_app_name != 'HYPD_store' THEN sou.total_price ELSE 0 END) AS global_returns_WO_hypd
                FROM shopify_orders_update sou
                JOIN target_dates td ON sou.updated_dt = td.d
                WHERE sou.financial_status NOT IN ('paid', 'pending')
                GROUP BY sou.updated_dt
            ),
            ReturnsPerOrder AS (
                SELECT
                    rf.order_id,
                    MAX(CASE WHEN rf.event_type = 'CANCEL' THEN rf.amount END) AS cancel_amt,
                    MAX(CASE WHEN rf.event_type = 'CANCEL' THEN rf.event_date END) AS cancel_date,
                    SUM(CASE WHEN rf.event_type = 'REFUND' THEN rf.amount END) AS refund_amt,
                    MAX(CASE WHEN rf.event_type = 'REFUND' THEN rf.event_date END) AS refund_last_date
                FROM returns_fact rf
                JOIN target_dates td ON rf.event_date = td.d
                GROUP BY rf.order_id
            ),
            ReturnsChosen AS (
                SELECT
                    CASE
                        WHEN cancel_amt IS NULL THEN refund_last_date
                        WHEN refund_amt IS NULL THEN cancel_date
                        WHEN refund_amt >= cancel_amt THEN refund_last_date
                        ELSE cancel_date
                    END AS date,
                    CASE
                        WHEN cancel_amt IS NULL THEN COALESCE(refund_amt, 0)
                        WHEN refund_amt IS NULL THEN COALESCE(cancel_amt, 0)
                        ELSE GREATEST(refund_amt, cancel_amt)
                    END AS amount
                FROM ReturnsPerOrder
            ),
            ReturnsOverallByDate AS (
                SELECT date, SUM(amount) AS overall_returns
                FROM ReturnsChosen
                GROUP BY date
            )
            SELECT
                d.d AS date,

                COALESCE(s.gokwik_sales, 0) AS gokwik_sales, COALESCE(r.gokwik_returns, 0) AS gokwik_returns,
                COALESCE(s.gokwik_sales, 0) - COALESCE(r.gokwik_returns, 0) AS actual_gokwik_sale,

                COALESCE(s.kwik_engage_sales, 0) AS KwikEngageSales, COALESCE(r.kwik_engage_returns, 0) AS KwikEngageReturns,
                COALESCE(s.kwik_engage_sales, 0) - COALESCE(r.kwik_engage_returns, 0) AS actual_KwikEngage_sale,

                COALESCE(s.online_store_sales, 0) AS online_store_sales, COALESCE(r.online_store_returns, 0) AS online_store_returns,
                COALESCE(s.online_store_sales, 0) - COALESCE(r.online_store_returns, 0) AS actual_online_store_sale,

                COALESCE(s.hypd_store_sales, 0) AS hypd_store_sales, COALESCE(r.hypd_store_returns, 0) AS hypd_store_returns,
                COALESCE(s.hypd_store_sales, 0) - COALESCE(r.hypd_store_returns, 0) AS actual_hypd_store_sale,

                COALESCE(s.draft_order_sales, 0) AS draft_order_sales, COALESCE(r.draft_order_returns, 0) AS draft_order_returns,
                COALESCE(s.draft_order_sales, 0) - COALESCE(r.draft_order_returns, 0) AS actual_draft_order_sale,

                COALESCE(s.dpanda_sales, 0) AS dpanda_sales, COALESCE(r.dpanda_returns, 0) AS dpanda_returns,
                COALESCE(s.dpanda_sales, 0) - COALESCE(r.dpanda_returns, 0) AS actual_dpanda_sale,

                COALESCE(s.gkappbrew_sales, 0) AS gkappbrew_sales, COALESCE(r.gkappbrew_returns, 0) AS gkappbrew_returns,
                COALESCE(s.gkappbrew_sales, 0) - COALESCE(r.gkappbrew_returns, 0) AS actual_gkappbrew_sale,

                COALESCE(s.buykaro_sales, 0) AS buykaro_sales, COALESCE(r.buykaro_returns, 0) AS buykaro_returns,
                COALESCE(s.buykaro_sales, 0) - COALESCE(r.buykaro_returns, 0) AS actual_buykaro_sale,

                COALESCE(s.appbrewplus_sales, 0) AS appbrewplus_sales, COALESCE(r.appbrewplus_returns, 0) AS appbrewplus_returns,
                COALESCE(s.appbrewplus_sales, 0) - COALESCE(r.appbrewplus_returns, 0) AS actual_appbrewplus_sale,
                
                COALESCE(s.shopflo_sales, 0) AS shopflo_sales, COALESCE(r.shopflo_returns, 0) AS shopflo_returns,
                COALESCE(s.shopflo_sales, 0) - COALESCE(r.shopflo_returns, 0) AS actual_shopflo_sale,

                COALESCE(s.global_sales_WO_hypd, 0) AS overall_sales_WO_hypd,
                COALESCE(r.global_returns_WO_hypd, 0) AS overall_returns_WO_hypd,
                COALESCE(s.global_sales_WO_hypd, 0) - COALESCE(r.global_returns_WO_hypd, 0) AS actual_overall_sales_WO_hypd,

                COALESCE(s.global_overall_sales, 0) AS overall_sales,
                COALESCE(robd.overall_returns, 0) AS overall_returns,
                COALESCE(s.global_overall_sales, 0) - COALESCE(robd.overall_returns, 0) AS actual_overall_sales

            FROM (SELECT d FROM target_dates) d
            LEFT JOIN SalesData s ON d.d = s.date
            LEFT JOIN ReturnsData r ON d.d = r.date
            LEFT JOIN ReturnsOverallByDate robd ON d.d = robd.date
            """
            params = [dates_list[0]] + dates_list[1:]
            exec_timed(cursor, sales_insert, params, label=f"{brand_name} sales_summary upsert", show_profile=prof)

            # ORDER SUMMARY (uses created_dt / updated_dt)
            order_insert = """
            INSERT INTO order_summary (
                date,
                number_of_orders_created, number_of_orders_returned, actual_number_of_orders,
                cod_orders, prepaid_orders, overall_cod_orders, overall_prepaid_orders
            )
            WITH target_dates AS (
                SELECT %s AS d UNION ALL
                """ + " UNION ALL ".join(["SELECT %s"] * (len(dates_list)-1)) + """
            ),
            created AS (
                SELECT so.created_dt AS date,
                       COUNT(DISTINCT so.order_id) AS orders_created,
                       COUNT(DISTINCT CASE WHEN so.payment_gateway_names LIKE '%Cash on Delivery (COD)%'
                                           OR so.payment_gateway_names LIKE '%cash_on_delivery%' THEN so.order_id END) AS cod_created,
                       COUNT(DISTINCT CASE WHEN (so.payment_gateway_names IS NOT NULL AND so.payment_gateway_names != '')
                                           AND NOT (so.payment_gateway_names LIKE '%Cash on Delivery (COD)%'
                                                    OR so.payment_gateway_names LIKE '%cash_on_delivery%') THEN so.order_id END) AS prepaid_created
                FROM shopify_orders so
                JOIN target_dates td ON so.created_dt = td.d
                GROUP BY so.created_dt
            ),
            returned AS (
                SELECT sou.updated_dt AS date,
                       COUNT(DISTINCT sou.order_id) AS orders_returned,
                       COUNT(DISTINCT CASE WHEN sou.payment_gateway_names LIKE '%Cash on Delivery (COD)%'
                                           OR sou.payment_gateway_names LIKE '%cash_on_delivery%' THEN sou.order_id END) AS cod_returned,
                       COUNT(DISTINCT CASE WHEN (sou.payment_gateway_names IS NOT NULL AND sou.payment_gateway_names != '')
                                           AND NOT (sou.payment_gateway_names LIKE '%Cash on Delivery (COD)%'
                                                    OR sou.payment_gateway_names LIKE '%cash_on_delivery%') THEN sou.order_id END) AS prepaid_returned
                FROM shopify_orders_update sou
                JOIN target_dates td ON sou.updated_dt = td.d
                WHERE sou.financial_status NOT IN ('paid', 'pending')
                GROUP BY sou.updated_dt
            )
            SELECT
                d.d AS date,
                COALESCE(c.orders_created, 0) AS number_of_orders_created,
                COALESCE(r.orders_returned, 0) AS number_of_orders_returned,
                COALESCE(c.orders_created, 0) - COALESCE(r.orders_returned, 0) AS actual_number_of_orders,
                COALESCE(c.cod_created, 0) - COALESCE(r.cod_returned, 0) AS cod_orders,
                COALESCE(c.prepaid_created, 0) - COALESCE(r.prepaid_returned, 0) AS prepaid_orders,
                COALESCE(c.cod_created, 0) AS overall_cod_orders,
                COALESCE(c.prepaid_created, 0) AS overall_prepaid_orders
            FROM (SELECT d FROM target_dates) d
            LEFT JOIN created c ON d.d = c.date
            LEFT JOIN returned r ON d.d = r.date
            """
            exec_timed(cursor, order_insert, params, label=f"{brand_name} order_summary upsert", show_profile=prof)

            # DISCOUNT SUMMARY (uses created_dt / updated_dt)
            discount_insert = """
            INSERT INTO discount_summary (
                date, total_discounts_given, total_discount_on_returns, actual_discounts
            )
            WITH target_dates AS (
                SELECT %s AS d UNION ALL
                """ + " UNION ALL ".join(["SELECT %s"] * (len(dates_list)-1)) + """
            ),
            DiscountsGiven AS (
                SELECT so.created_dt AS date, SUM(COALESCE(so.discount_amount, 0)) AS total_discounts_given
                FROM shopify_orders so
                JOIN target_dates td ON so.created_dt = td.d
                GROUP BY so.created_dt
            ),
            DiscountsReturned AS (
                SELECT sou.updated_dt AS date, SUM(COALESCE(sou.discount_amount, 0)) AS total_discount_on_returns
                FROM shopify_orders_update sou
                JOIN target_dates td ON sou.updated_dt = td.d
                WHERE sou.financial_status NOT IN ('paid', 'pending')
                GROUP BY sou.updated_dt
            )
            SELECT
                d.d AS date,
                COALESCE(dg.total_discounts_given, 0) AS total_discounts_given,
                COALESCE(dr.total_discount_on_returns, 0) AS total_discount_on_returns,
                (COALESCE(dg.total_discounts_given, 0) - COALESCE(dr.total_discount_on_returns, 0)) AS actual_discounts
            FROM (SELECT d FROM target_dates) d
            LEFT JOIN DiscountsGiven dg ON d.d = dg.date
            LEFT JOIN DiscountsReturned dr ON d.d = dr.date
            """
            exec_timed(cursor, discount_insert, params, label=f"{brand_name} discount_summary upsert", show_profile=prof)

            # GROSS SUMMARY (uses created_dt + joins discount_summary)
            gross_insert = """
            INSERT INTO gross_summary (
                date, overall_sale, shipping_total, discounts_total, tax_total, gross_sales, actual_discounts, net_sales
            )
            WITH target_dates AS (
                SELECT %s AS d UNION ALL
                """ + " UNION ALL ".join(["SELECT %s"] * (len(dates_list)-1)) + """
            ),
            ShopifyAggregates AS (
                SELECT
                    so.created_dt AS date,
                    SUM(COALESCE(so.line_item_quantity, 0) * COALESCE(so.line_item_price, 0)) AS overall_sale,
                    SUM(COALESCE(so.shipping_price, 0)) AS shipping_total,
                    SUM(COALESCE(so.total_tax, 0)) AS tax_total
                FROM shopify_orders so
                JOIN target_dates td ON so.created_dt = td.d
                GROUP BY so.created_dt
            )
            SELECT
                sa.date, sa.overall_sale, sa.shipping_total,
                COALESCE(ds.total_discounts_given, 0) AS discounts_total,
                sa.tax_total,
                (sa.overall_sale * 0.84) AS gross_sales,
                COALESCE(ds.actual_discounts, 0) AS actual_discounts,
                ((sa.overall_sale * 0.84) - COALESCE(ds.actual_discounts, 0)) AS net_sales
            FROM ShopifyAggregates sa
            LEFT JOIN discount_summary ds ON sa.date = ds.date
            """
            exec_timed(cursor, gross_insert, params, label=f"{brand_name} gross_summary upsert", show_profile=prof)

            # HOURLY SALES (uses created_dt, created_hr)
            hourly_insert = """
            INSERT INTO hour_wise_sales (
                date, hour, number_of_orders, total_sales, number_of_prepaid_orders, number_of_cod_orders,
                number_of_sessions, number_of_atc_sessions
            )
            WITH target_dates AS (
                SELECT %s AS d UNION ALL
                """ + " UNION ALL ".join(["SELECT %s"] * (len(dates_list)-1)) + """
            ),
            HourlySales AS (
                SELECT
                    so.created_dt AS date,
                    so.created_hr AS hour,
                    COUNT(DISTINCT so.order_id) AS number_of_orders,
                    SUM(COALESCE(so.total_price, 0)) AS total_sales,
                    COUNT(DISTINCT CASE WHEN (so.payment_gateway_names IS NOT NULL AND so.payment_gateway_names != '')
                        AND NOT (so.payment_gateway_names LIKE '%Cash on Delivery (COD)%' OR so.payment_gateway_names LIKE '%cash_on_delivery%')
                        THEN so.order_id END) AS number_of_prepaid_orders,
                    COUNT(DISTINCT CASE WHEN so.payment_gateway_names LIKE '%Cash on Delivery (COD)%'
                        OR so.payment_gateway_names LIKE '%cash_on_delivery%' THEN so.order_id END) AS number_of_cod_orders
                FROM shopify_orders so
                JOIN target_dates td ON so.created_dt = td.d
                GROUP BY so.created_dt, so.created_hr
            ),
            HourlySessions AS (
                SELECT
                    ss.date,
                    ss.hour,
                    ss.number_of_sessions,
                    ss.number_of_atc_sessions
                FROM hourly_sessions_summary_shopify ss
                JOIN target_dates td ON ss.date = td.d
            ),
            AllKeys AS (
                SELECT date, hour FROM HourlySales
                UNION
                SELECT date, hour FROM HourlySessions
            )
            SELECT
                ak.date, ak.hour, 
                COALESCE(hs.number_of_orders, 0) AS number_of_orders, 
                COALESCE(hs.total_sales, 0) AS total_sales,
                COALESCE(hs.number_of_prepaid_orders, 0) AS number_of_prepaid_orders, 
                COALESCE(hs.number_of_cod_orders, 0) AS number_of_cod_orders,
                COALESCE(ss.number_of_sessions, 0) AS number_of_sessions,
                COALESCE(ss.number_of_atc_sessions, 0) AS number_of_atc_sessions
            FROM AllKeys ak
            LEFT JOIN HourlySales hs ON ak.date = hs.date AND ak.hour = hs.hour
            LEFT JOIN HourlySessions ss ON ak.date = ss.date AND ak.hour = ss.hour
            """
            exec_timed(cursor, hourly_insert, params, label=f"{brand_name} hour_wise_sales upsert", show_profile=prof)

            # OVERALL SUMMARY (joins date-scoped rows)
            overall_insert = """
            INSERT INTO overall_summary (
                date, gross_sales, total_discount_amount, total_sales, net_sales,
                total_orders, cod_orders, prepaid_orders, total_sessions, total_atc_sessions
            )
            SELECT
                s.date,
                COALESCE(gs.gross_sales, 0) AS gross_sales,
                COALESCE(ds.actual_discounts, 0) AS total_discount_amount,
                COALESCE(s.actual_overall_sales, 0) AS total_sales,
                COALESCE(gs.net_sales, 0) AS net_sales,
                COALESCE(o.number_of_orders_created, 0) AS total_orders,
                COALESCE(o.overall_cod_orders, 0) AS cod_orders,
                COALESCE(o.overall_prepaid_orders, 0) AS prepaid_orders,
                COALESCE(sess.number_of_sessions, 0) AS total_sessions,
                COALESCE(sess.number_of_atc_sessions, 0) AS total_atc_sessions
            FROM sales_summary s
            LEFT JOIN order_summary o   ON s.date = o.date
            LEFT JOIN sessions_summary sess ON s.date = sess.date
            LEFT JOIN gross_summary gs  ON s.date = gs.date
            LEFT JOIN discount_summary ds ON s.date = ds.date
            WHERE s.date IN (""" + ",".join(["%s"] * len(dates_list)) + ")"
            exec_timed(cursor, overall_insert, dates_list, label=f"{brand_name} overall_summary upsert", show_profile=prof)

            if prof:
                print_session_profiles(cursor)

            logger.info(f"✅ Summary tables refreshed for {brand_name} (dates={dates_list})")

    except Exception as e:
        logger.error(f"❌ Error executing summary queries for {brand_name}: {e}")
        traceback.print_exc()

# ---------------------------
# Per-brand processing
# ---------------------------
def process_single_brand(brand_index: int):
    if brand_index not in db_connection_pools:
        logger.error(f"❌ Skipping brand {brand_index}: no connection pool")
        return
    brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")

    logger.info(f"\n{'='*50}\nSTARTING PROCESS FOR SHOP: {brand_name}\n{'='*50}")

    shop_name = os.environ.get(f"SHOP_NAME_{brand_index}")
    api_version = os.environ.get(f"API_VERSION_{brand_index}")
    access_token = os.environ.get(f"ACCESS_TOKEN_{brand_index}")
    session_url = os.environ.get(f"SESSION_URL_{brand_index}")
    x_brand_name = os.environ.get(f"X_BRAND_NAME_{brand_index}")
    x_api_key = os.environ.get(f"X_API_KEY_{brand_index}")
    api_base_url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}"

    # mapping
    app_id_mapping_str = os.environ.get(f"APP_ID_MAPPING_{brand_index}", "{}")
    try:
        app_id_mapping = json.loads(app_id_mapping_str)
    except json.JSONDecodeError:
        logger.warning(f"⚠️ Invalid JSON in APP_ID_MAPPING_{brand_index} for {brand_name}. Using empty mapping.")
        app_id_mapping = {}

    # Sessions (ShopifyQL)
    update_sessions_summary(brand_index, brand_name, shop_name, api_version, access_token)

    # Orders (NEW + UPDATED) with single connection/cursor reused
    process_types = [
        {'type': 'NEW', 'date_field': 'created_at_min', 'table': 'shopify_orders'},
        {'type': 'UPDATED', 'date_field': 'updated_at_min', 'table': 'shopify_orders_update'}
    ]

    affected_dates: Set[date] = set()

    with get_db_cursor(brand_index) as (cursor, connection):
        for process in process_types:
            logger.info(f"\n--- Processing {process['type']} orders for {brand_name} ---")
            timestamp_col = 'created_at' if process['type'] == 'NEW' else 'updated_at'
            last_order = get_last_order(brand_index, process['table'], cursor=cursor)
            existing_ids = set()

            if BACKFILL_MODE:
                # Use provided IST window exactly
                start_dt = BACKFILL_START_IST
                end_dt = BACKFILL_END_IST
                start_date = convert_to_desired_format(start_dt)
                end_date = convert_to_desired_format(end_dt)
                logger.info(f"🔁 BACKFILL MODE: {process['type']} from {start_dt} to {end_dt} (IST)")
            else:
                if not last_order:
                    logger.warning(f"🛑 No initial data in '{process['table']}' for {brand_name}. Skipping.")
                    continue
                last_ts = last_order[timestamp_col]
                start_date = convert_to_desired_format(last_ts)
                end_date = convert_to_desired_format(now_ist())
                logger.info(f"Checking for orders since: {last_ts}")

                existing_ids = get_orders_with_same_timestamp(
                    brand_index, process['table'], last_ts, timestamp_field=timestamp_col, cursor=cursor
                )
                logger.info(f"Found {len(existing_ids)} existing orders at this timestamp")

            orders_list = fetch_orders(api_base_url, access_token, start_date, end_date, process['date_field'])

            if not orders_list:
                logger.info(f"No new {process['type']} orders to process for {brand_name}")
                continue

            original_count = len(orders_list)
            filtered = [o for o in orders_list if str(o.get('id')) not in existing_ids] if existing_ids else orders_list
            if not filtered:
                logger.info(f"Fetched {original_count} orders, all duplicates. Nothing to insert.")
                continue

            logger.info(f"Fetched {original_count}, removed {original_count - len(filtered)} duplicates. Processing {len(filtered)} orders.")
            df = transform_orders_to_df_optimized(filtered, app_id_mapping)

            # Load immediately for this process type
            batch_size = int(os.environ.get('BATCH_SIZE', 1000))
            load_data_to_sql_optimized(df, brand_index, process['table'], batch_size)

            # Track affected dates (created and updated dates in IST)
            for o in filtered:
                cdt = _iso_to_ist_date(o.get('created_at'))
                if cdt:
                    affected_dates.add(cdt)
                udt = _iso_to_ist_date(o.get('updated_at'))
                if udt:
                    affected_dates.add(udt)

            # Upsert returns_fact; also collect affected return dates
            returned_dates = upsert_returns_fact_from_orders(brand_index, filtered, cursor=cursor, connection=connection)
            affected_dates.update(returned_dates)

    # Record completion
    with timed("Record pipeline completion timestamp"):
        with get_db_cursor(brand_index) as (cursor, connection):
            cursor.execute("""
                INSERT INTO pipeline_metadata (key_name, key_value)
                VALUES ('last_pipeline_completion_time', %s)
                ON DUPLICATE KEY UPDATE key_value = VALUES(key_value);
            """, (now_ist(),))
            connection.commit()
            logger.info(f"✅ Recorded pipeline completion for {brand_name}")

    # Summaries (date-scoped + on-index)
    logger.info(f"\n--- Creating summary reports for {brand_name} ---")
    execute_summary_queries(brand_index, brand_name, affected_dates)
    logger.info(f"✅ COMPLETED PROCESSING FOR {brand_name}")

# ---------------------------
# Job runner
# ---------------------------
def run_data_pipeline():
    job_start_time = now_ist()
    logger.info(f"\n{'='*60}\nJOB TRIGGERED AT: {job_start_time.strftime('%Y-%m-%d %I:%M:%S %p')}\n{'='*60}")

    try:
        if not active_brand_indices:
            logger.warning("No active brands with valid pools/engines. Nothing to do.")
            return

        max_workers = min(len(active_brand_indices), max(2, CPU_COUNT // 2))
        logger.info(f"Processing {len(active_brand_indices)} brands with {max_workers} parallel workers")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_single_brand, i): i for i in active_brand_indices}
            for fut in as_completed(futures):
                idx = futures[fut]
                name = os.environ.get(f"BRAND_NAME_{idx}", f"Brand_{idx}")
                try:
                    fut.result()
                    logger.info(f"✅ Successfully completed brand: {name}")
                except Exception as e:
                    logger.error(f"❌ Failed processing brand {name}: {e}")
                    traceback.print_exc()

        end = now_ist()
        dur = (end - job_start_time).total_seconds()
        logger.info(f"\n{'='*60}\nJOB COMPLETED AT: {end.strftime('%Y-%m-%d %I:%M:%S %p')}")
        logger.info(f"Total Duration: {dur:.2f}s ({dur/60:.2f} minutes)\n{'='*60}")

    except Exception as e:
        logger.error(f"❌ PIPELINE FAILED with error: {e}")
        traceback.print_exc()

# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    initialize_brand_configs()

    scheduler = BackgroundScheduler(timezone=IST)
    scheduler.add_job(
        run_data_pipeline,
        "interval",
        minutes=10,
        next_run_time=now_ist(),   # fire immediately on boot
        coalesce=True,
        max_instances=1,
        misfire_grace_time=120,
        replace_existing=True,
    )
    scheduler.start()

    logger.info("✅ Optimized Shopify ETL worker started")
    if BACKFILL_MODE:
        logger.info("   - Mode: BACKFILL (orders window overridden by BACKFILL_* env)")
    else:
        logger.info("   - Mode: Regular incremental")

    logger.info("   - First run: Immediate")
    logger.info("   - Interval: Every 10 minutes")
    logger.info(f"   - Parallel workers: {min(len(active_brand_indices), max(2, CPU_COUNT // 2))}")
    logger.info("   - Connection pooling: Enabled (retry on exhaustion)")
    logger.info("   - Async API fetching: Enabled")

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down scheduler...")
        scheduler.shutdown()
        for i, engine in sqlalchemy_engines.items():
            try:
                engine.dispose()
            except Exception:
                pass
        http_session.close()
        logger.info("✅ Shutdown complete")
