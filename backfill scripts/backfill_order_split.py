# this script backfills the cod, partial paid and prepaid orders columns in overall summary





import os
import logging
import base64
import requests
from typing import Dict, Optional, Any
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

# ---- Globals (from aws_background.py) ----
# --- TEST_MODE ---
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
# Support for BACKFILL_BRAND_INDICES to filter brands
BACKFILL_BRAND_INDICES = [
    b.strip() for b in os.environ.get("BACKFILL_BRAND_INDICES", "").split(",") if b.strip()
]

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

        cipher = Cipher(
            algorithms.AES(key_bytes), modes.CBC(iv), backend=default_backend()
        )
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
    # Prioritize BACKFILL_BRAND_INDICES if set
    target_ids = BACKFILL_BRAND_INDICES if BACKFILL_BRAND_INDICES else ACTIVE_BRAND_IDS

    if TEST_MODE:
        # In TEST_MODE, we use brand names from env vars BRAND_NAME_{idx}
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
            # Filter by target_ids if any were specified
            if target_ids:
                return {k: v for k, v in all_brands.items() if k in target_ids}
            return all_brands
        logger.error(f"❌ Failed to fetch brands: {resp.status_code}")
        return {}
    except Exception as e:
        logger.error(f"❌ Brand API error: {e}")
        return {}

def fetch_brand_config(brand_id: str, brand_tag_value: str = "") -> Optional[Dict[str, Any]]:
    if TEST_MODE:
        # Load from env vars directly
        return {
            "db_host": os.environ.get(f"DB_HOST_{brand_id}"),
            "db_user": os.environ.get(f"DB_USER_{brand_id}"),
            "db_password": os.environ.get(f"DB_PASSWORD_{brand_id}"),
            "port": int(os.environ.get(f"DB_PORT_{brand_id}", 3306)),
            "db_database": os.environ.get(f"DB_DATABASE_{brand_id}"),
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
# Backfill Execution
# ---------------------------
def backfill_brand(brand_id: str, brand_name: str):
    config = fetch_brand_config(brand_id, brand_name)
    if not config:
        return

    # Extract DB settings using keys aligned with aws_background.py
    db_host = config.get("db_host")
    db_user = config.get("db_user")
    db_pass_raw = config.get("db_password")
    
    # Only decrypt if it looks like an encrypted string (test mode might have raw pass)
    if TEST_MODE:
        db_pass = db_pass_raw
    else:
        db_pass = decrypt_value(db_pass_raw)
        
    db_name = config.get("db_database")
    db_port = int(config.get("port", 3306))

    if not all([db_host, db_user, db_pass, db_name]):
        logger.error(f"❌ Missing DB config for {brand_name}. Host={db_host}, User={db_user}, DB={db_name}")
        return


    logger.info(f"🚀 Starting backfill for {brand_name} ({brand_id})")

    try:
        conn = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_pass,
            database=db_name,
            port=db_port
        )
        cursor = conn.cursor(dictionary=True)

        # Refined Logic SQL Update
        # Based on aws_background.py logic for order split
        update_sql = """
        UPDATE overall_summary T1
        JOIN (
            SELECT 
                STR_TO_DATE(created_date, '%Y-%m-%d') AS date,
                COUNT(DISTINCT CASE 
                    WHEN (payment_gateway_names IS NULL 
                      OR payment_gateway_names = '' 
                      OR payment_gateway_names LIKE '%Cash on Delivery (COD)%' 
                      OR payment_gateway_names LIKE '%cash_on_delivery%')
                      AND (payment_gateway_names NOT LIKE '%Gokwik PPCOD%' OR payment_gateway_names IS NULL)
                    THEN order_id END) AS new_cod,
                COUNT(DISTINCT CASE 
                    WHEN payment_gateway_names IS NOT NULL 
                      AND payment_gateway_names != '' 
                      AND NOT (payment_gateway_names LIKE '%Cash on Delivery (COD)%'
                          OR payment_gateway_names LIKE '%cash_on_delivery%'
                          OR payment_gateway_names LIKE '%Gokwik PPCOD%')
                    THEN order_id END) AS new_prepaid,
                COUNT(DISTINCT CASE 
                    WHEN payment_gateway_names LIKE '%Gokwik PPCOD%' 
                    THEN order_id END) AS new_partial
            FROM shopify_orders
            GROUP BY date
        ) T2 ON T1.date = T2.date
        SET 
            T1.cod_orders = T2.new_cod,
            T1.prepaid_orders = T2.new_prepaid,
            T1.partially_paid_orders = T2.new_partial;
        """

        cursor.execute(update_sql)
        updated_rows = cursor.rowcount
        conn.commit()
        
        logger.info(f"✅ [{brand_name}] Corrected counts for {updated_rows} dates in overall_summary.")

        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"❌ Database error for {brand_name}: {e}")

def main():
    logger.info("Initializing Order Split Backfill...")
    brands = fetch_active_brands()
    
    if not brands:
        logger.warning("No active brands found.")
        return

    logger.info(f"Found {len(brands)} active brands: {list(brands.values())}")

    for brand_id, brand_name in brands.items():
        backfill_brand(brand_id, brand_name)

    logger.info("🏁 Backfill process completed.")

if __name__ == "__main__":
    main()
