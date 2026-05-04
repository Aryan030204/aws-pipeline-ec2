import os
import logging
import base64
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Any, List

import requests
from dotenv import load_dotenv
import mysql.connector
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

# Load environment
load_dotenv()

# ---- Logging ----
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---- Globals ----
TEST_MODE = os.environ.get("TEST_MODE", "false").strip().lower() == "true"
if TEST_MODE:
    load_dotenv(".env.local", override=True)
    logger.info("⚠️ TEST_MODE enabled: loading config from local env.")

GET_BRANDS_API = os.environ.get("GET_BRANDS_API")
PIPELINE_AUTH_HEADER = os.environ.get("PIPELINE_AUTH_HEADER")
PASSWORD_AES_KEY = os.environ.get("PASSWORD_AES_KEY")
ACTIVE_BRAND_IDS = [
    b.strip() for b in os.environ.get("ACTIVE_BRAND_IDS", "").split(",") if b.strip()
]
BACKFILL_BRAND_INDICES = [
    b.strip()
    for b in os.environ.get("BACKFILL_BRAND_INDICES", "").split(",")
    if b.strip()
]

IST = timezone(timedelta(hours=5, minutes=30))


# ---------------------------
# Backfill window
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
        logger.warning(
            "BACKFILL_MODE enabled but BACKFILL_START_IST/BACKFILL_END_IST missing or invalid."
        )
        BACKFILL_MODE = False
    elif BACKFILL_START_IST >= BACKFILL_END_IST:
        logger.warning("BACKFILL_* start >= end. Disabling backfill for safety.")
        BACKFILL_MODE = False
    else:
        logger.info(
            f"🔁 BACKFILL_MODE ON :: {BACKFILL_START_IST} → {BACKFILL_END_IST} (IST)"
        )


def convert_to_desired_format(dt_obj: datetime) -> str:
    dt_obj = dt_obj.astimezone(IST)
    iso_str = dt_obj.strftime("%Y-%m-%dT%H:%M:%S%z")
    return f"{iso_str[:19]}%2B05:30"


# ---------------------------
# Decryption Logic
# ---------------------------
def decrypt_value(encrypted_val: str) -> str:
    if not encrypted_val or ":" not in encrypted_val:
        return encrypted_val

    if not PASSWORD_AES_KEY:
        return encrypted_val

    try:
        iv_b64, ciphertext_b64 = encrypted_val.split(":", 1)
        iv = base64.b64decode(iv_b64)
        ciphertext = base64.b64decode(ciphertext_b64)

        key_bytes = PASSWORD_AES_KEY.encode("utf-8")
        if len(key_bytes) < 32:
            key_bytes = key_bytes.ljust(32, b"\0")
        elif len(key_bytes) > 32:
            key_bytes = key_bytes[:32]

        cipher = Cipher(algorithms.AES(key_bytes), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        padded_content = decryptor.update(ciphertext) + decryptor.finalize()

        padding_len = padded_content[-1]
        if padding_len < 1 or padding_len > 16:
            return padded_content.decode("utf-8", errors="ignore")

        content = padded_content[:-padding_len]
        return content.decode("utf-8")
    except Exception as e:
        logger.error(f"❌ Decryption failed: {e}")
        return encrypted_val


# ---------------------------
# Config Fetching
# ---------------------------
def fetch_active_brands() -> Dict[str, str]:
    target_ids = BACKFILL_BRAND_INDICES if BACKFILL_BRAND_INDICES else ACTIVE_BRAND_IDS

    if TEST_MODE:
        brands = {}
        for idx in target_ids:
            name = os.environ.get(f"BRAND_NAME_{idx}")
            if name:
                brands[idx] = name
        return brands

    if not GET_BRANDS_API or not PIPELINE_AUTH_HEADER:
        logger.error("❌ GET_BRANDS_API or PIPELINE_AUTH_HEADER not set.")
        return {}

    headers = {"x-pipeline-key": PIPELINE_AUTH_HEADER}
    try:
        resp = requests.get(GET_BRANDS_API, headers=headers, timeout=30)
        if resp.status_code == 200:
            all_brands = resp.json()
            if target_ids:
                return {k: v for k, v in all_brands.items() if k in target_ids}
            return all_brands
        logger.error(f"❌ Failed to fetch brands: {resp.status_code}")
        return {}
    except Exception as e:
        logger.error(f"❌ Brand API error: {e}")
        return {}


def fetch_brand_config(brand_id: str) -> Optional[Dict[str, Any]]:
    if TEST_MODE:
        return {
            "db_host": os.environ.get(f"DB_HOST_{brand_id}"),
            "db_user": os.environ.get(f"DB_USER_{brand_id}"),
            "db_password": os.environ.get(f"DB_PASSWORD_{brand_id}"),
            "port": int(os.environ.get(f"DB_PORT_{brand_id}", 3306)),
            "db_database": os.environ.get(f"DB_DATABASE_{brand_id}"),
            "shop_name": os.environ.get(f"SHOP_NAME_{brand_id}"),
            "access_token": os.environ.get(f"ACCESS_TOKEN_{brand_id}"),
            "api_version": os.environ.get(f"API_VERSION_{brand_id}", "2024-04"),
        }

    headers = {"x-pipeline-key": PIPELINE_AUTH_HEADER}
    url = f"{GET_BRANDS_API.rstrip('/')}/{brand_id.lower()}"
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        return None
    except Exception as e:
        logger.error(f"❌ Config fetch error: {e}")
        return None


# ---------------------------
# Shopify Orders Fetch
# ---------------------------
def fetch_orders_window(shop_name: str, access_token: str, api_version: str) -> List[Dict[str, Any]]:
    fields = (
        "id,created_at,"
        "shipping_address,shipping_address/address1,shipping_address/zip,"
        "billing_address,billing_address/address1,billing_address/zip"
    )

    start_date = convert_to_desired_format(BACKFILL_START_IST)
    end_date = convert_to_desired_format(BACKFILL_END_IST)

    url = (
        f"https://{shop_name}.myshopify.com/admin/api/{api_version}/orders.json"
        f"?status=any&limit=250&created_at_min={start_date}&created_at_max={end_date}"
        f"&fields={fields}"
    )

    headers = {"X-Shopify-Access-Token": access_token}
    orders: List[Dict[str, Any]] = []

    while url:
        resp = requests.get(url, headers=headers, timeout=60)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 2))
            logger.warning(f"Rate limited, waiting {retry_after}s")
            time.sleep(retry_after)
            continue
        if resp.status_code != 200:
            logger.error(f"Failed to fetch orders: {resp.status_code} - {resp.text[:200]}")
            break

        data = resp.json()
        batch = data.get("orders", [])
        if not batch:
            break
        orders.extend(batch)

        link_header = resp.headers.get("Link", "")
        url = None
        if 'rel="next"' in link_header:
            for part in link_header.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip("<> ")
                    break

        time.sleep(0.5)

    return orders


# ---------------------------
# Backfill Execution
# ---------------------------
def ensure_address_columns(cursor) -> None:
    cursor.execute("SHOW COLUMNS FROM shopify_orders")
    existing = {row[0] if not isinstance(row, dict) else row.get("Field") for row in cursor.fetchall()}

    missing = []
    for col, sql_type in (
        ("shipping_address", "TEXT"),
        ("shipping_zip", "VARCHAR(20)"),
        ("billing_address", "TEXT"),
        ("billing_zip", "VARCHAR(20)"),
    ):
        if col not in existing:
            missing.append((col, sql_type))

    for col, sql_type in missing:
        cursor.execute(f"ALTER TABLE shopify_orders ADD COLUMN {col} {sql_type}")


def backfill_brand(brand_id: str, brand_name: str) -> None:
    config = fetch_brand_config(brand_id)
    if not config:
        return

    db_host = config.get("db_host")
    db_user = config.get("db_user")
    db_pass_raw = config.get("db_password")
    db_pass = db_pass_raw if TEST_MODE else decrypt_value(db_pass_raw)
    db_name = config.get("db_database")
    db_port = int(config.get("port", 3306))

    shop_name = config.get("shop_name")
    access_token_raw = config.get("access_token")
    access_token = access_token_raw if TEST_MODE else decrypt_value(access_token_raw)
    api_version = config.get("api_version", "2024-04")

    if not all([db_host, db_user, db_pass, db_name]):
        logger.error(
            f"❌ Missing DB config for {brand_name}. Host={db_host}, User={db_user}, DB={db_name}"
        )
        return

    if not all([shop_name, access_token, api_version]):
        logger.error(
            f"❌ Missing Shopify config for {brand_name}. Shop={shop_name}, Token={bool(access_token)}, API={api_version}"
        )
        return

    logger.info(f"🚀 Backfill start for {brand_name} ({brand_id})")

    orders = fetch_orders_window(shop_name, access_token, api_version)
    if not orders:
        logger.info(f"✅ [{brand_name}] No orders in backfill window.")
        return

    updates = []
    for order in orders:
        order_id = order.get("id")
        if not order_id:
            continue
        shipping = order.get("shipping_address") or {}
        billing = order.get("billing_address") or {}
        updates.append(
            (
                shipping.get("address1"),
                shipping.get("zip"),
                billing.get("address1"),
                billing.get("zip"),
                str(order_id),
            )
        )

    if not updates:
        logger.info(f"✅ [{brand_name}] No address updates to apply.")
        return

    try:
        conn = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_pass,
            database=db_name,
            port=db_port,
        )
        cursor = conn.cursor()

        ensure_address_columns(cursor)

        update_sql = (
            "UPDATE shopify_orders "
            "SET shipping_address = COALESCE(%s, shipping_address), "
            "shipping_zip = COALESCE(%s, shipping_zip), "
            "billing_address = COALESCE(%s, billing_address), "
            "billing_zip = COALESCE(%s, billing_zip) "
            "WHERE order_id = %s"
        )
        cursor.executemany(update_sql, updates)
        conn.commit()

        logger.info(f"✅ [{brand_name}] Updated {cursor.rowcount} rows (order_id matches).")
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"❌ Database error for {brand_name}: {e}")


def main() -> None:
    if not BACKFILL_MODE:
        logger.error("BACKFILL_MODE is false or invalid. Exiting.")
        return

    brands = fetch_active_brands()
    if not brands:
        logger.warning("No active brands found.")
        return

    logger.info(f"Found {len(brands)} active brands: {list(brands.values())}")

    for brand_id, brand_name in brands.items():
        backfill_brand(brand_id, brand_name)

    logger.info("🏁 Address backfill completed.")


if __name__ == "__main__":
    main()
