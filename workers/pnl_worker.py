"""
Standalone worker: builds the daily brand-level P&L (overall_pnl) in each
brand's own database - overall only, not per product.

The tables this worker writes are `overall_pnl` (migrations/003_overall_pnl.sql)
and the two ad-spend rollups `pnl_meta_ad_spend_rollup` /
`pnl_google_ad_spend_rollup` (migrations/004_pnl_ad_spend_rollups.sql), all
created once per brand DB by those migrations; the worker runs no DDL of its
own. Every other table it touches (gross_summary, discount_summary,
overall_summary, rto_summary, returns_fact) is read-only here and owned by
the main pipeline.

Reads and writes both use the brand's own database connection (the
db_host/db_user/... the brand-config API returns for that tenant, loaded by
aws_background.initialize_brand_configs()), exactly like the other
per-brand workers. Nothing about the target is hard-coded or env-driven.
Rows still carry `brand_key` so the table is self-describing.

Revenue-side figures come from the summary tables the main pipeline already
maintains in that brand DB; cost-side figures come from ONE Mongo document
per brand in the `total_config` collection (arch-auth), the same document
the dashboard's "Brand Cost Configuration" page edits:

    {
      "brand_id": "BBB",
      "gst_pct": 18,
      "costs": {
        "cogs":  {"value_type": "percentage", "value": 32},
        "rent":  {"value_type": "flat",       "value": 90000},
        "meta":  null,          # not configured -> DEFAULT_COST_PERCENTAGES
        ...one key per COST_COLUMNS entry
      }
    }

A brand with no document at all is costed entirely from
DEFAULT_COST_PERCENTAGES (and GST at DEFAULT_GST_PCT); its rows are tagged
cost_config_source='default'. A brand with a document is tagged 'mongo',
and any individual line that is null/missing in it falls back to that
line's default.

Paid-ads spend (the `meta` and `google` lines) is synced from the ad
platforms themselves, per brand, right before the P&L is built - but only
when the brand has pasted its credentials on the dashboard's P&L page. Those
land in the same arch-auth Mongo database as total_config, one document per
brand, keyed by the same brand_id:

    meta_ads_credentials   {brand_id, ad_account_id, access_token_encrypted, ...}
    google_ads_credentials {brand_id, customer_id, login_customer_id, token_encrypted, ...}

Tokens are AES-256-CBC encrypted with PASSWORD_AES_KEY (same helper the
dashboard uses). For each channel with a document the worker pulls spend
for exactly the target dates (never more), upserts one row per date into
that channel's rollup table, and records any API failure on the document's
`last_error` so the dashboard can show it. A brand without a document for a
channel is simply skipped for that channel - nothing is fetched. Meta
credentials are additionally only synced when `token_type` is
"system_user": a System User token (generated in Business Settings) doesn't
expire, so it's the only kind this unattended nightly job trusts - a brand
still on the OAuth "user" token is skipped (with `last_error` pointing at
reconnecting with a System User token) until it reconnects. The P&L row
then takes the rollup figure when the day has one (meta_spend_synced /
google_spend_synced = 1), else the configured/default cost for that line.
The dashboard reads the rollup tables directly for those two lines, so an
unsynced day shows "-" there rather than the estimate written here.

Per date, for each brand:

    gross_sales   = gross_summary.overall_sale          (line items, before discounts)
    discounts     = discount_summary.actual_discounts   (net of cancelled-order discounts)
    cancellations = returns_fact CANCEL                 (full order value, by order date)
                  + returns_fact REFUND                 (by refund date, only for orders
                                                         that have no CANCEL / CANCEL (RTO)
                                                         row, so a cancel+refund isn't
                                                         counted twice)
    rto_returns   = returns_fact CANCEL (RTO)           (by order date; informational)
    gst           = (gross - discounts - cancellations) * gst_pct/(100+gst_pct)
    net_sales     = gross - discounts - cancellations - gst
    gross_margin  = net_sales - cogs - freight_inwards
    cm1           = gross_margin - shipping - rto - payment_gateway - packaging
    cm2           = cm1 - meta - google - other_paid
    cm3           = cm2 - influencers - content - sponsorships - other_brand
    ebitda        = cm3 - salaries - rent - technology - agency_fees - other_overheads

RTO follows the same convention as sales_summary: an RTO'd order is not a
cancellation (its revenue stays in gross/net), it's surfaced separately as
rto_orders / rto_returns, and the `rto` line is the configured RTO *cost*.
CANCEL and CANCEL (RTO) are mutually exclusive event types in returns_fact
(a cancelled order gets exactly one of them), so they are never combined.

A configured cost line resolves to a daily amount as:
    percentage -> value% of that day's net_sales
    flat       -> a MONTHLY amount, prorated per calendar day

Same shape as the other per-brand-DB workers (checkout_login_status_worker
etc): brand discovery and DB access come from aws_background / pipeline.db.
"""

import base64
import calendar
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Dict, List, Optional, Sequence, Set, Tuple
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# When run directly (`python workers/pnl_worker.py`, which is how the
# Dockerfile/CMD invokes it), Python sets sys.path[0] to workers/, not the
# repo root - same fix every other worker in this repo uses.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

PNL_TABLE = "overall_pnl"  # DDL in migrations/003_overall_pnl.sql
# Daily ad spend per channel, one row per date; DDL in migrations/004_pnl_ad_spend_rollups.sql.
META_SPEND_TABLE = "pnl_meta_ad_spend_rollup"
GOOGLE_SPEND_TABLE = "pnl_google_ad_spend_rollup"
SPEND_TABLES = {"meta": META_SPEND_TABLE, "google": GOOGLE_SPEND_TABLE}
# Which account the rollup row was pulled for, per channel's own id column.
SPEND_ACCOUNT_COLUMNS = {"meta": "ad_account_id", "google": "customer_id"}
AD_CHANNELS = ("meta", "google")
# Meta credentials can be a regular OAuth "user" token or a Business-Settings
# "system_user" token (see analytics/shared/db/models/MetaAdsCredential.mongo.js
# on the dashboard side). Only System User tokens are used to drive this
# unattended nightly sync - they don't expire, so a brand that reconnects
# with the OAuth login won't have its rollup silently go stale weeks later.
# Google has no such distinction (it always refreshes via OAuth refresh_token).
META_REQUIRED_TOKEN_TYPE = "system_user"

# The cost configs live in the arch-auth database, which is not necessarily
# the cluster the pipeline's MONGO_URI (events) points at.
PNL_CONFIG_MONGO_URI = (
    os.environ.get("PNL_CONFIG_MONGO_URI", "").strip()
    or os.environ.get("MONGO_URI", "").strip()
)
PNL_CONFIG_MONGO_DB = os.environ.get("PNL_CONFIG_MONGO_DB", "arch-auth").strip() or "arch-auth"
PNL_CONFIG_COLLECTION = (
    os.environ.get("PNL_CONFIG_COLLECTION", "total_config").strip() or "total_config"
)
# Per-brand ad credentials pasted on the dashboard's P&L page, same database.
PNL_META_CREDENTIALS_COLLECTION = (
    os.environ.get("PNL_META_CREDENTIALS_COLLECTION", "meta_ads_credentials").strip() or "meta_ads_credentials"
)
PNL_GOOGLE_CREDENTIALS_COLLECTION = (
    os.environ.get("PNL_GOOGLE_CREDENTIALS_COLLECTION", "google_ads_credentials").strip()
    or "google_ads_credentials"
)
CREDENTIAL_COLLECTIONS = {
    "meta": PNL_META_CREDENTIALS_COLLECTION,
    "google": PNL_GOOGLE_CREDENTIALS_COLLECTION,
}

# Shared secret the dashboard encrypts pasted tokens with (analytics/shared/utils/crypto.js).
PASSWORD_AES_KEY = os.environ.get("PASSWORD_AES_KEY", "")

# Ad platform APIs. Meta only needs the brand's own token; Google Ads also
# needs this deployment's OAuth client (to turn the brand's refresh token into
# an access token) and a developer token, none of which are per brand.
META_GRAPH_URL = "https://graph.facebook.com"
META_API_VERSION = os.environ.get("META_API_VERSION", "v21.0").strip() or "v21.0"
GOOGLE_OAUTH_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_ADS_BASE_URL = "https://googleads.googleapis.com"
GOOGLE_ADS_API_VERSION = os.environ.get("GOOGLE_ADS_API_VERSION", "v25").strip() or "v25"
GOOGLE_ADS_CLIENT_ID = os.environ.get("GOOGLE_ADS_CLIENT_ID", "").strip()
GOOGLE_ADS_CLIENT_SECRET = os.environ.get("GOOGLE_ADS_CLIENT_SECRET", "").strip()
GOOGLE_ADS_DEVELOPER_TOKEN = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN", "").strip()
AD_API_TIMEOUT_S = int(os.environ.get("PNL_AD_API_TIMEOUT_S", "30"))
AD_API_MAX_PAGES = 50

# Rolling rebuild window (yesterday and the N-1 days before it), or an explicit
# range for backfills.
PNL_WORKER_LOOKBACK_DAYS = int(os.environ.get("PNL_WORKER_LOOKBACK_DAYS", "2"))
PNL_WORKER_START_DATE = os.environ.get("PNL_WORKER_START_DATE", "").strip()
PNL_WORKER_END_DATE = os.environ.get("PNL_WORKER_END_DATE", "").strip()

DEFAULT_GST_PCT = Decimal("18")
CENT = Decimal("0.01")
ZERO = Decimal("0")

VALUE_TYPES = {"flat", "percentage"}

CONFIG_SOURCE_MONGO = "mongo"
CONFIG_SOURCE_DEFAULT = "default"

# Cost lines a Mongo config category can populate, in P&L order. Keys are
# both the Mongo `category` and the overall_pnl column.
COST_COLUMNS = (
    "cogs",
    "freight_inwards",
    "shipping",
    "rto",
    "payment_gateway",
    "packaging",
    "meta",
    "google",
    "other_paid",
    "influencers",
    "content",
    "sponsorships",
    "other_brand",
    "salaries",
    "rent",
    "technology",
    "agency_fees",
    "other_overheads",
)

# Fallback for a brand with no total_config document, and for any single
# cost line that is null/missing in its document: a percentage of that day's
# net sales. Every COST_COLUMNS entry has an entry here so a row is always
# fully populated.
DEFAULT_COST_PERCENTAGES: Dict[str, str] = {
    "cogs": "32",
    "freight_inwards": "2",
    "shipping": "6",
    "rto": "3",
    "payment_gateway": "2",
    "packaging": "1.5",
    "meta": "8",
    "google": "5",
    "other_paid": "1",
    "influencers": "2",
    "content": "1",
    "sponsorships": "0.5",
    "other_brand": "0.5",
    "salaries": "6",
    "rent": "1",
    "technology": "1",
    "agency_fees": "1",
    "other_overheads": "1",
}

MONEY_COLUMNS = (
    "gross_sales",
    "discounts",
    "cancellations",
    "gst",
    "net_sales",
    "cogs",
    "freight_inwards",
    "gross_margin",
    "shipping",
    "rto",
    "payment_gateway",
    "packaging",
    "cm1",
    "meta",
    "google",
    "other_paid",
    "cm2",
    "influencers",
    "content",
    "sponsorships",
    "other_brand",
    "cm3",
    "salaries",
    "rent",
    "technology",
    "agency_fees",
    "other_overheads",
    "ebitda",
    "rto_returns",
    "cod_sales",
    "prepaid_sales",
    "partially_prepaid_sales",
)
INT_COLUMNS = ("total_orders", "rto_orders", "meta_spend_synced", "google_spend_synced")
RATE_COLUMNS = ("gst_pct",)
TEXT_COLUMNS = ("cost_config_source",)
KEY_COLUMNS = ("brand_key", "date")
ALL_COLUMNS = (*KEY_COLUMNS, *MONEY_COLUMNS, *RATE_COLUMNS, *INT_COLUMNS, *TEXT_COLUMNS)


def money(value) -> Decimal:
    return Decimal(str(value if value is not None else 0)).quantize(CENT, rounding=ROUND_HALF_UP)


def compute_target_dates(
    tz: ZoneInfo,
    current_dt: Optional[datetime] = None,
    lookback_days: int = PNL_WORKER_LOOKBACK_DAYS,
    start_date: str = PNL_WORKER_START_DATE,
    end_date: str = PNL_WORKER_END_DATE,
) -> List[date]:
    if start_date and end_date:
        start, end = date.fromisoformat(start_date), date.fromisoformat(end_date)
        return [start + timedelta(days=i) for i in range((end - start).days + 1)]

    local_now = current_dt.astimezone(tz) if current_dt else datetime.now(tz)
    yesterday = local_now.date() - timedelta(days=1)
    return [yesterday - timedelta(days=i) for i in range(max(1, lookback_days) - 1, -1, -1)]


def resolve_brand_key(brand_index: int) -> str:
    """The dashboard keys configs by the tenant's brand_id ("BBB"), which is
    this pipeline's brand tag upper-cased."""
    tag = os.environ.get(f"BRAND_TAG_{brand_index}") or os.environ.get(f"BRAND_NAME_{brand_index}", "")
    return tag.strip().upper()


# ---------------------------
# Cost config (Mongo: one total_config document per brand)
# ---------------------------
class BrandPnlConfig:
    """Fully-resolved cost configuration for one brand: every COST_COLUMNS
    line has a {"value_type", "value"} entry (from Mongo or the default), plus
    the GST rate. `defaulted` names the lines that came from defaults."""

    def __init__(self, brand_key: str, gst_pct: Decimal, costs: Dict[str, Dict], source: str, defaulted: Set[str]):
        self.brand_key = brand_key
        self.gst_pct = gst_pct
        self.costs = costs
        self.source = source
        self.defaulted = defaulted

    @property
    def gst_rate(self) -> Decimal:
        """Inclusive GST share of a gross figure: pct / (100 + pct)."""
        return self.gst_pct / (Decimal(100) + self.gst_pct)


def default_cost_line(column: str) -> Dict:
    return {"value_type": "percentage", "value": Decimal(DEFAULT_COST_PERCENTAGES[column])}


def _normalize_cost_line(column: str, raw) -> Optional[Dict]:
    """A total_config `costs.<column>` value -> {"value_type","value"} or None
    when it's null/missing/unusable (the caller then applies the default)."""
    if not isinstance(raw, dict):
        return None
    value_type = str(raw.get("value_type") or "").strip()
    if value_type not in VALUE_TYPES:
        logger.warning("total_config %s: unknown value_type %r; using default", column, value_type)
        return None
    try:
        value = Decimal(str(raw.get("value")))
    except Exception:
        logger.warning("total_config %s: non-numeric value %r; using default", column, raw.get("value"))
        return None
    if value < 0:
        logger.warning("total_config %s: negative value %s; using default", column, value)
        return None
    return {"value_type": value_type, "value": value}


def _normalize_gst_pct(raw) -> Decimal:
    if raw is None or raw == "":
        return DEFAULT_GST_PCT
    try:
        pct = Decimal(str(raw))
    except Exception:
        logger.warning("total_config gst_pct %r is not numeric; using %s", raw, DEFAULT_GST_PCT)
        return DEFAULT_GST_PCT
    if pct < 0 or pct > 100:
        logger.warning("total_config gst_pct %s out of range; using %s", pct, DEFAULT_GST_PCT)
        return DEFAULT_GST_PCT
    return pct


def build_brand_config(brand_key: str, doc: Optional[Dict]) -> BrandPnlConfig:
    """Resolves a total_config document (or None) into a BrandPnlConfig."""
    if not doc:
        return BrandPnlConfig(
            brand_key,
            DEFAULT_GST_PCT,
            {column: default_cost_line(column) for column in COST_COLUMNS},
            CONFIG_SOURCE_DEFAULT,
            set(COST_COLUMNS),
        )
    raw_costs = doc.get("costs") if isinstance(doc.get("costs"), dict) else {}
    costs: Dict[str, Dict] = {}
    defaulted: Set[str] = set()
    for column in COST_COLUMNS:
        line = _normalize_cost_line(column, raw_costs.get(column))
        if line is None:
            line = default_cost_line(column)
            defaulted.add(column)
        costs[column] = line
    return BrandPnlConfig(brand_key, _normalize_gst_pct(doc.get("gst_pct")), costs, CONFIG_SOURCE_MONGO, defaulted)


def fetch_total_config_doc(brand_key: str) -> Optional[Dict]:
    """The brand's total_config document, or None when it has none (or the
    collection doesn't exist yet). Raises on a real connectivity/auth failure
    so a brand isn't silently re-costed with defaults because Mongo hiccupped."""
    from pymongo import MongoClient

    if not PNL_CONFIG_MONGO_URI:
        logger.warning(
            "PNL_CONFIG_MONGO_URI / MONGO_URI not configured; using default cost config for %s",
            brand_key,
        )
        return None

    client = MongoClient(PNL_CONFIG_MONGO_URI, tz_aware=True)
    try:
        collection = client[PNL_CONFIG_MONGO_DB][PNL_CONFIG_COLLECTION]
        return collection.find_one({"brand_id": brand_key}, {"gst_pct": 1, "costs": 1})
    finally:
        client.close()


def resolve_brand_config(brand_key: str) -> BrandPnlConfig:
    config = build_brand_config(brand_key, fetch_total_config_doc(brand_key))
    if config.source == CONFIG_SOURCE_DEFAULT:
        logger.info(
            "No %s.%s document for brand_id=%s; using DEFAULT_COST_PERCENTAGES and GST %s%%",
            PNL_CONFIG_MONGO_DB,
            PNL_CONFIG_COLLECTION,
            brand_key,
            DEFAULT_GST_PCT,
        )
    elif config.defaulted:
        logger.info(
            "total_config for brand_id=%s leaves %s line(s) unset; defaults applied to: %s",
            brand_key,
            len(config.defaulted),
            ", ".join(sorted(config.defaulted)),
        )
    return config


def cost_line_amount(line: Dict, day: date, net_sales: Decimal) -> Decimal:
    """percentage -> share of the day's net sales; flat -> monthly amount
    prorated per calendar day of that month."""
    value = Decimal(str(line.get("value") or 0))
    if line.get("value_type") == "percentage":
        return money(net_sales * value / Decimal(100))
    return money(value / Decimal(calendar.monthrange(day.year, day.month)[1]))


# ---------------------------
# Revenue-side sources (brand DB)
# ---------------------------
def fetch_sales_rows(cursor, start: date, end: date) -> Dict[date, Dict]:
    cursor.execute(
        """
        SELECT
            gs.date,
            COALESCE(gs.overall_sale, 0)            AS gross_sales,
            COALESCE(ds.actual_discounts, 0)        AS discounts,
            COALESCE(os.total_orders, 0)            AS total_orders,
            COALESCE(os.cod_sales, 0)               AS cod_sales,
            COALESCE(os.prepaid_sales, 0)           AS prepaid_sales,
            COALESCE(os.partially_prepaid_sales, 0) AS partially_prepaid_sales,
            COALESCE(rs.rto_orders, 0)              AS rto_orders
        FROM gross_summary gs
        LEFT JOIN discount_summary ds ON ds.date = gs.date
        LEFT JOIN overall_summary  os ON os.date = gs.date
        LEFT JOIN rto_summary      rs ON rs.summary_date = gs.date
        WHERE gs.date BETWEEN %s AND %s
        """,
        (start, end),
    )
    return {_as_date(row["date"]): row for row in cursor.fetchall() or []}


def fetch_returns_rows(cursor, start: date, end: date) -> Dict[date, Dict[str, Decimal]]:
    rows: Dict[date, Dict[str, Decimal]] = {}

    # CANCEL and CANCEL (RTO) are mutually exclusive per order, so each is
    # its own bucket: plain cancels are a revenue deduction, RTO cancels are
    # reported alongside (their revenue stays in gross/net).
    cursor.execute(
        """
        SELECT
            order_created_date AS d,
            SUM(CASE WHEN event_type = 'CANCEL'       THEN amount ELSE 0 END) AS cancel_amount,
            SUM(CASE WHEN event_type = 'CANCEL (RTO)' THEN amount ELSE 0 END) AS rto_amount
        FROM returns_fact
        WHERE order_created_date BETWEEN %s AND %s
          AND event_type IN ('CANCEL', 'CANCEL (RTO)')
        GROUP BY order_created_date
        """,
        (start, end),
    )
    for row in cursor.fetchall() or []:
        day = _as_date(row["d"])
        rows[day] = {
            "cancellations": money(row["cancel_amount"]),
            "rto_returns": money(row["rto_amount"]),
        }

    # Refunds on orders that were never cancelled (a cancel with a refund is
    # already counted in full through its CANCEL row).
    cursor.execute(
        """
        SELECT rf.event_date AS d, SUM(rf.amount) AS refund_amount
        FROM returns_fact rf
        WHERE rf.event_type = 'REFUND'
          AND rf.event_date BETWEEN %s AND %s
          AND NOT EXISTS (
              SELECT 1 FROM returns_fact c
              WHERE c.order_id = rf.order_id
                AND c.event_type IN ('CANCEL', 'CANCEL (RTO)')
          )
        GROUP BY rf.event_date
        """,
        (start, end),
    )
    for row in cursor.fetchall() or []:
        day = _as_date(row["d"])
        entry = rows.setdefault(day, {"cancellations": ZERO, "rto_returns": ZERO})
        entry["cancellations"] += money(row["refund_amount"])

    return rows


def table_exists(cursor, table: str) -> bool:
    cursor.execute("SHOW TABLES LIKE %s", (table,))
    return bool(cursor.fetchone())


def fetch_spend_rows(cursor, table: str, start: date, end: date) -> Dict[date, Decimal]:
    """Synced daily spend from one ad-spend rollup table. Empty when the
    table hasn't been migrated into this brand DB yet."""
    if not table_exists(cursor, table):
        return {}
    cursor.execute(
        f"SELECT `date`, spend FROM `{table}` WHERE `date` BETWEEN %s AND %s",
        (start, end),
    )
    return {_as_date(row["date"]): money(row["spend"]) for row in cursor.fetchall() or []}


def _as_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


# ---------------------------
# Ad credentials (Mongo) + token decryption
# ---------------------------
class AdSpendSyncError(Exception):
    """A channel's spend couldn't be pulled; the message is safe to store on
    the brand's credential document and show on the dashboard."""


def _aes_key(raw: str) -> bytes:
    """Same key convention as the dashboard's crypto.js: utf-8 bytes, cut to
    32 or zero-padded up to 32."""
    return raw.encode("utf-8")[:32].ljust(32, b"\0")


def decrypt_text(enc: str, key: str = "") -> str:
    """Inverse of the dashboard's encryptText: "<iv b64>:<AES-256-CBC b64>"."""
    from cryptography.hazmat.primitives import padding
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    secret = key or PASSWORD_AES_KEY
    if not secret:
        raise AdSpendSyncError("PASSWORD_AES_KEY is not configured on the worker; cannot decrypt the token")
    if not isinstance(enc, str) or enc.count(":") != 1:
        raise AdSpendSyncError("Stored token has an invalid encrypted format")
    iv_b64, body_b64 = enc.split(":")
    iv, body = base64.b64decode(iv_b64), base64.b64decode(body_b64)
    decryptor = Cipher(algorithms.AES(_aes_key(secret)), modes.CBC(iv)).decryptor()
    padded = decryptor.update(body) + decryptor.finalize()
    unpadder = padding.PKCS7(128).unpadder()
    return (unpadder.update(padded) + unpadder.finalize()).decode("utf-8")


def fetch_ad_credential_docs(brand_key: str) -> Dict[str, Optional[Dict]]:
    """{"meta": doc | None, "google": doc | None} for the brand, from the
    arch-auth Mongo database. A channel with no document is one the brand
    never connected: nothing is fetched for it."""
    from pymongo import MongoClient

    empty: Dict[str, Optional[Dict]] = {channel: None for channel in AD_CHANNELS}
    if not PNL_CONFIG_MONGO_URI:
        return empty

    client = MongoClient(PNL_CONFIG_MONGO_URI, tz_aware=True)
    try:
        db = client[PNL_CONFIG_MONGO_DB]
        return {
            channel: db[CREDENTIAL_COLLECTIONS[channel]].find_one({"brand_id": brand_key})
            for channel in AD_CHANNELS
        }
    finally:
        client.close()


def record_credential_error(brand_key: str, channel: str, message: Optional[str]) -> None:
    """Best-effort: stores (or clears, with None) the last sync failure on the
    brand's credential document so the dashboard can surface it."""
    from pymongo import MongoClient

    if not PNL_CONFIG_MONGO_URI:
        return
    try:
        client = MongoClient(PNL_CONFIG_MONGO_URI, tz_aware=True)
        try:
            client[PNL_CONFIG_MONGO_DB][CREDENTIAL_COLLECTIONS[channel]].update_one(
                {"brand_id": brand_key},
                {"$set": {"last_error": (message or "")[:500] or None}},
            )
        finally:
            client.close()
    except Exception as e:  # noqa: BLE001 - never let bookkeeping break the sync
        logger.warning("Could not record %s credential status for %s: %s", channel, brand_key, e)


# ---------------------------
# Ad platform APIs (spend for exactly the target dates)
# ---------------------------
def _api_error_message(payload) -> str:
    body = payload[0] if isinstance(payload, list) and payload else payload
    if not isinstance(body, dict):
        return "request failed"
    err = body.get("error")
    if isinstance(err, dict):
        details = err.get("details") or []
        first = details[0] if details and isinstance(details[0], dict) else {}
        errors = first.get("errors") or []
        nested = errors[0].get("message") if errors and isinstance(errors[0], dict) else None
        return str(nested or err.get("message") or err)
    return str(body.get("error_description") or err or "request failed")


def _json_or_raise(response, label: str) -> Dict:
    try:
        payload = response.json()
    except ValueError:
        payload = {}
    if response.status_code >= 400 or (isinstance(payload, dict) and payload.get("error")):
        raise AdSpendSyncError(f"{label}: {_api_error_message(payload) or response.status_code}")
    return payload if isinstance(payload, dict) else {}


def normalize_meta_ad_account_id(raw) -> str:
    value = str(raw or "").strip()
    return value if value.startswith("act_") else f"act_{value}"


def normalize_google_customer_id(raw) -> str:
    return "".join(ch for ch in str(raw or "") if ch.isdigit())


def fetch_meta_spend_from_api(access_token: str, ad_account_id: str, start: date, end: date) -> Dict[date, Dict]:
    """Account-level daily spend from the Marketing API insights edge, for
    [start, end] only. {date: {"spend", "currency", "account_id"}}; days Meta
    returns nothing for are absent (the caller writes them as 0)."""
    import requests

    account = normalize_meta_ad_account_id(ad_account_id)
    url = f"{META_GRAPH_URL}/{META_API_VERSION}/{account}/insights"
    params: Optional[Dict] = {
        "fields": "spend,account_currency",
        "level": "account",
        "time_increment": 1,
        "time_range": json.dumps({"since": start.isoformat(), "until": end.isoformat()}),
        "limit": 500,
        "access_token": access_token,
    }
    rows: Dict[date, Dict] = {}
    for _ in range(AD_API_MAX_PAGES):
        try:
            response = requests.get(url, params=params, timeout=AD_API_TIMEOUT_S)
        except requests.RequestException as e:
            raise AdSpendSyncError(f"Meta insights request failed: {e.__class__.__name__}") from e
        payload = _json_or_raise(response, "Meta insights")
        for item in payload.get("data") or []:
            if not item.get("date_start"):
                continue
            day = _as_date(item["date_start"])
            entry = rows.setdefault(day, {"spend": ZERO, "currency": None, "account_id": account})
            entry["spend"] = money(entry["spend"] + money(item.get("spend")))
            entry["currency"] = entry["currency"] or item.get("account_currency") or None
        next_url = (payload.get("paging") or {}).get("next")
        if not next_url:
            break
        url, params = next_url, None  # paging.next already carries the token + params
    return rows


def google_access_token(refresh_token: str) -> str:
    import requests

    if not GOOGLE_ADS_CLIENT_ID or not GOOGLE_ADS_CLIENT_SECRET:
        raise AdSpendSyncError("GOOGLE_ADS_CLIENT_ID / GOOGLE_ADS_CLIENT_SECRET are not configured on the worker")
    try:
        response = requests.post(
            GOOGLE_OAUTH_TOKEN_URL,
            data={
                "client_id": GOOGLE_ADS_CLIENT_ID,
                "client_secret": GOOGLE_ADS_CLIENT_SECRET,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token",
            },
            timeout=AD_API_TIMEOUT_S,
        )
    except requests.RequestException as e:
        raise AdSpendSyncError(f"Google token refresh failed: {e.__class__.__name__}") from e
    payload = _json_or_raise(response, "Google token refresh")
    token = payload.get("access_token")
    if not token:
        raise AdSpendSyncError("Google token refresh returned no access token")
    return str(token)


def fetch_google_spend_from_api(
    refresh_token: str,
    customer_id: str,
    login_customer_id: Optional[str],
    start: date,
    end: date,
) -> Dict[date, Dict]:
    """Daily cost for one Google Ads customer via googleAds:search, for
    [start, end] only. Same shape as fetch_meta_spend_from_api; cost_micros
    / 1e6 = currency units."""
    import requests

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise AdSpendSyncError("GOOGLE_ADS_DEVELOPER_TOKEN is not configured on the worker")
    cid = normalize_google_customer_id(customer_id)
    if not cid:
        raise AdSpendSyncError("Google Ads credential has no customer_id")

    headers = {
        "Authorization": f"Bearer {google_access_token(refresh_token)}",
        "developer-token": GOOGLE_ADS_DEVELOPER_TOKEN,
    }
    login = normalize_google_customer_id(login_customer_id)
    if login:
        headers["login-customer-id"] = login

    url = f"{GOOGLE_ADS_BASE_URL}/{GOOGLE_ADS_API_VERSION}/customers/{cid}/googleAds:search"
    query = (
        "SELECT segments.date, metrics.cost_micros, customer.currency_code FROM customer "
        f"WHERE segments.date BETWEEN '{start.isoformat()}' AND '{end.isoformat()}' ORDER BY segments.date"
    )
    rows: Dict[date, Dict] = {}
    page_token = None
    for _ in range(AD_API_MAX_PAGES):
        body = {"query": query, "pageToken": page_token} if page_token else {"query": query}
        try:
            response = requests.post(url, json=body, headers=headers, timeout=AD_API_TIMEOUT_S)
        except requests.RequestException as e:
            raise AdSpendSyncError(f"Google Ads request failed: {e.__class__.__name__}") from e
        payload = _json_or_raise(response, "Google Ads")
        for result in payload.get("results") or []:
            day_str = (result.get("segments") or {}).get("date")
            if not day_str:
                continue
            day = _as_date(day_str)
            micros = Decimal(str((result.get("metrics") or {}).get("costMicros") or 0))
            entry = rows.setdefault(day, {"spend": ZERO, "currency": None, "account_id": cid})
            entry["spend"] += micros / Decimal(1_000_000)
            entry["currency"] = entry["currency"] or (result.get("customer") or {}).get("currencyCode") or None
        page_token = payload.get("nextPageToken")
        if not page_token:
            break
    for entry in rows.values():
        entry["spend"] = money(entry["spend"])
    return rows


def fetch_channel_spend(channel: str, doc: Dict, start: date, end: date) -> Dict[date, Dict]:
    """Decrypts the brand's token and pulls that channel's spend for the range."""
    if channel == "meta":
        return fetch_meta_spend_from_api(
            decrypt_text(doc.get("access_token_encrypted")),
            doc.get("ad_account_id"),
            start,
            end,
        )
    if channel == "google":
        return fetch_google_spend_from_api(
            decrypt_text(doc.get("token_encrypted")),
            doc.get("customer_id"),
            doc.get("login_customer_id"),
            start,
            end,
        )
    raise AdSpendSyncError(f"Unknown ad channel {channel!r}")


# ---------------------------
# Ad-spend rollups (brand DB)
# ---------------------------
def build_spend_rollup_rows(api_rows: Dict[date, Dict], target_dates: Sequence[date], account_id: str) -> List[Tuple]:
    """One (date, account_id, spend, currency) per target date. A day the API
    returned nothing for was still fetched, so it's written as 0 spend rather
    than left missing - the sync did cover it."""
    currency = next((entry.get("currency") for entry in api_rows.values() if entry.get("currency")), None)
    rows = []
    for day in target_dates:
        entry = api_rows.get(day) or {}
        rows.append((day, entry.get("account_id") or account_id, money(entry.get("spend")), entry.get("currency") or currency))
    return rows


def upsert_spend_rollup_rows(cursor, connection, table: str, account_column: str, rows: Sequence[Tuple]) -> int:
    if not rows:
        return 0
    from pipeline.db import executemany_chunked

    sql = (
        f"INSERT INTO `{table}` (`date`, `{account_column}`, `spend`, `currency`) VALUES (%s, %s, %s, %s) "
        f"ON DUPLICATE KEY UPDATE `{account_column}` = VALUES(`{account_column}`), "
        "`spend` = VALUES(`spend`), `currency` = VALUES(`currency`)"
    )
    return executemany_chunked(cursor, connection, sql, list(rows))


def sync_ad_spend(
    cursor,
    connection,
    brand_key: str,
    brand_name: str,
    credentials: Dict[str, Optional[Dict]],
    target_dates: Sequence[date],
) -> Dict[str, str]:
    """For every channel the brand has credentials for, pulls spend for the
    target dates and upserts it into that channel's rollup table. Never
    raises: a failed channel is logged, recorded on the credential document,
    and the P&L still builds (that line falls back to the configured cost).
    Returns {channel: "synced" | "no-credentials" | "not-system-user" | "no-table" | "failed"}."""
    start, end = min(target_dates), max(target_dates)
    outcomes: Dict[str, str] = {}
    for channel in AD_CHANNELS:
        doc = credentials.get(channel)
        if not doc:
            outcomes[channel] = "no-credentials"
            logger.info("[%s] no %s credentials in Mongo; %s spend not fetched", brand_name, channel, channel)
            continue
        if channel == "meta" and doc.get("token_type") != META_REQUIRED_TOKEN_TYPE:
            outcomes[channel] = "not-system-user"
            message = (
                "Automated P&L spend sync only uses a Meta System User token (it doesn't expire); "
                "reconnect this brand's Meta Ads account with a System User token to resume syncing."
            )
            logger.warning(
                "[%s] meta credentials are a %r token, not %r; skipping automated sync",
                brand_name,
                doc.get("token_type") or "user",
                META_REQUIRED_TOKEN_TYPE,
            )
            record_credential_error(brand_key, channel, message)
            continue
        table = SPEND_TABLES[channel]
        if not table_exists(cursor, table):
            outcomes[channel] = "no-table"
            logger.warning(
                "[%s] %s credentials found but `%s` is missing in the brand DB; run migrations/004_pnl_ad_spend_rollups.sql",
                brand_name,
                channel,
                table,
            )
            continue
        try:
            api_rows = fetch_channel_spend(channel, doc, start, end)
            account_id = (
                normalize_meta_ad_account_id(doc.get("ad_account_id"))
                if channel == "meta"
                else normalize_google_customer_id(doc.get("customer_id"))
            )
            rows = build_spend_rollup_rows(api_rows, target_dates, account_id)
            written = upsert_spend_rollup_rows(cursor, connection, table, SPEND_ACCOUNT_COLUMNS[channel], rows)
            record_credential_error(brand_key, channel, None)
            outcomes[channel] = "synced"
            logger.info(
                "[%s] %s spend synced for %s..%s: %s day(s) written, total %s",
                brand_name,
                channel,
                start,
                end,
                written,
                sum(row[2] for row in rows),
            )
        except Exception as e:  # noqa: BLE001 - one channel failing must not sink the P&L
            message = str(e) if isinstance(e, AdSpendSyncError) else f"{e.__class__.__name__}: {e}"
            outcomes[channel] = "failed"
            logger.error("[%s] %s spend sync failed: %s", brand_name, channel, message)
            record_credential_error(brand_key, channel, message)
    return outcomes


# ---------------------------
# P&L math
# ---------------------------
def compute_pnl_row(
    day: date,
    sales: Optional[Dict],
    returns: Optional[Dict[str, Decimal]],
    meta_spend: Optional[Decimal],
    config: BrandPnlConfig,
    brand_key: str = "",
    google_spend: Optional[Decimal] = None,
) -> Dict[str, object]:
    sales = sales or {}
    returns = returns or {}

    gross_sales = money(sales.get("gross_sales"))
    discounts = money(sales.get("discounts"))
    cancellations = money(returns.get("cancellations"))
    gst = money((gross_sales - discounts - cancellations) * config.gst_rate)
    net_sales = gross_sales - discounts - cancellations - gst

    cost = {column: cost_line_amount(config.costs[column], day, net_sales) for column in COST_COLUMNS}
    meta_spend_synced = meta_spend is not None
    if meta_spend_synced:
        cost["meta"] = money(meta_spend)
    google_spend_synced = google_spend is not None
    if google_spend_synced:
        cost["google"] = money(google_spend)

    gross_margin = net_sales - cost["cogs"] - cost["freight_inwards"]
    cm1 = gross_margin - cost["shipping"] - cost["rto"] - cost["payment_gateway"] - cost["packaging"]
    cm2 = cm1 - cost["meta"] - cost["google"] - cost["other_paid"]
    cm3 = cm2 - cost["influencers"] - cost["content"] - cost["sponsorships"] - cost["other_brand"]
    ebitda = (
        cm3
        - cost["salaries"]
        - cost["rent"]
        - cost["technology"]
        - cost["agency_fees"]
        - cost["other_overheads"]
    )

    return {
        "brand_key": brand_key,
        "date": day,
        "gross_sales": gross_sales,
        "discounts": discounts,
        "cancellations": cancellations,
        "gst": gst,
        "net_sales": net_sales,
        **cost,
        "gross_margin": gross_margin,
        "cm1": cm1,
        "cm2": cm2,
        "cm3": cm3,
        "ebitda": ebitda,
        "rto_returns": money(returns.get("rto_returns")),
        "cod_sales": money(sales.get("cod_sales")),
        "prepaid_sales": money(sales.get("prepaid_sales")),
        "partially_prepaid_sales": money(sales.get("partially_prepaid_sales")),
        "total_orders": int(sales.get("total_orders") or 0),
        "rto_orders": int(sales.get("rto_orders") or 0),
        "gst_pct": config.gst_pct.quantize(CENT, rounding=ROUND_HALF_UP),
        "meta_spend_synced": int(meta_spend_synced),
        "google_spend_synced": int(google_spend_synced),
        "cost_config_source": config.source,
    }


# ---------------------------
# Target table (brand DB)
# ---------------------------
def upsert_pnl_rows(cursor, connection, rows: Sequence[Dict[str, object]]) -> int:
    if not rows:
        return 0
    from pipeline.db import executemany_chunked

    columns = ", ".join(f"`{col}`" for col in ALL_COLUMNS)
    placeholders = ", ".join(["%s"] * len(ALL_COLUMNS))
    updates = ", ".join(f"`{col}` = VALUES(`{col}`)" for col in ALL_COLUMNS if col not in KEY_COLUMNS)
    sql = (
        f"INSERT INTO `{PNL_TABLE}` ({columns}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {updates}"
    )
    values = [tuple(row[col] for col in ALL_COLUMNS) for row in rows]
    return executemany_chunked(cursor, connection, sql, values)


# ---------------------------
# Orchestration
# ---------------------------
def rebuild_brand_pnl(
    brand_index: int,
    brand_name: str,
    brand_key: str,
    target_dates: Sequence[date],
) -> int:
    from pipeline.db import get_db_cursor

    start, end = min(target_dates), max(target_dates)
    config = resolve_brand_config(brand_key)
    logger.info(
        "[%s] cost config for brand_id=%s: source=%s gst=%s%% defaulted_lines=%s",
        brand_name,
        brand_key,
        config.source,
        config.gst_pct,
        len(config.defaulted),
    )
    credentials = fetch_ad_credential_docs(brand_key)

    with get_db_cursor(brand_index, dictionary=True) as (cursor, connection):
        # Ad spend first, so the rollups already hold today's target dates
        # when the P&L rows below read them. Only runs for channels the
        # brand has credentials for; failures are logged and skipped.
        outcomes = sync_ad_spend(cursor, connection, brand_key, brand_name, credentials, target_dates)

        # Reads: the source tables are only ever SELECTed here.
        sales_rows = fetch_sales_rows(cursor, start, end)
        returns_rows = fetch_returns_rows(cursor, start, end)
        meta_rows = fetch_spend_rows(cursor, META_SPEND_TABLE, start, end)
        google_rows = fetch_spend_rows(cursor, GOOGLE_SPEND_TABLE, start, end)

        rows = [
            compute_pnl_row(
                day,
                sales_rows.get(day),
                returns_rows.get(day),
                meta_rows.get(day),
                config,
                brand_key=brand_key,
                google_spend=google_rows.get(day),
            )
            for day in target_dates
        ]

        # Writes: overall_pnl (created by migrations/003_overall_pnl.sql).
        written = upsert_pnl_rows(cursor, connection, rows)

    logger.info("[%s] ad spend sync: %s", brand_name, ", ".join(f"{k}={v}" for k, v in outcomes.items()))
    for row in rows:
        logger.info(
            "[%s] %s gross=%s cancel=%s net=%s cogs=%s meta=%s%s google=%s%s ebitda=%s costs=%s",
            brand_name,
            row["date"].isoformat(),
            row["gross_sales"],
            row["cancellations"],
            row["net_sales"],
            row["cogs"],
            row["meta"],
            "" if row["meta_spend_synced"] else " (not synced)",
            row["google"],
            "" if row["google_spend_synced"] else " (not synced)",
            row["ebitda"],
            row["cost_config_source"],
        )
    return written


def run_worker(current_dt: Optional[datetime] = None) -> None:
    import aws_background

    aws_background.initialize_brand_configs()
    if not aws_background.active_brand_indices:
        logger.warning("No active brands with valid database configs. Nothing to do.")
        return

    for brand_index in aws_background.active_brand_indices:
        brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
        brand_key = resolve_brand_key(brand_index)
        tz = aws_background.get_brand_timezone(brand_index)
        target_dates = compute_target_dates(tz, current_dt=current_dt)

        logger.info(
            "Rebuilding P&L for %s (brand_id=%s): %s..%s (%s)",
            brand_name,
            brand_key,
            target_dates[0],
            target_dates[-1],
            tz,
        )
        try:
            written = rebuild_brand_pnl(brand_index, brand_name, brand_key, target_dates)
            logger.info("[%s] Upserted %s %s row(s)", brand_name, written, PNL_TABLE)
        except Exception as e:
            logger.error("Error rebuilding P&L for %s: %s", brand_name, e)

    logger.info("P&L worker complete.")


def _run_scheduled() -> None:
    """
    Long-running mode for the standalone container: daily at 00:10 IST
    (40 18 * * * UTC), after the other daily workers have rolled up the
    tables this one reads. Set PNL_WORKER_SELF_RUN=false to run once and
    exit (e.g. for a one-off/manual invocation or a backfill).
    """
    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        run_worker,
        trigger="cron",
        hour=18,
        minute=40,
        timezone="UTC",
        id="pnl_worker_job",
        coalesce=True,
        max_instances=1,
        misfire_grace_time=3600,
    )
    logger.info("P&L worker scheduler started (cron 40 18 * * * UTC / 00:10 IST)")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("P&L worker scheduler stopped")


if __name__ == "__main__":
    self_run = os.environ.get("PNL_WORKER_SELF_RUN", "true").strip().lower() == "true"
    if self_run:
        _run_scheduled()
    else:
        run_worker()
