"""
Campaign / ad / product-level paid-ads performance for the dashboard's Ads
page, synced by the P&L worker right after its account-level spend rollups
(workers/pnl_worker.py::rebuild_brand_pnl calls sync_ads_performance), so it
reuses the exact same Mongo credentials, token rules (Meta needs a
"system_user" token) and target dates - including the dashboard's
"connect -> backfill the last 30 days" trigger.

Writes three per-brand tables (DDL in migrations/007_ads_performance.sql,
the worker runs none of its own):

    ads_campaign_daily  one row per (channel, date, campaign_id)
    ads_ad_daily        one row per (channel, date, ad_id)  - Google: ad group
    ads_product_daily   one row per (channel, date, product_id), 0 = unattributed

Per channel:

    Meta    insights edge, level=ad, time_increment=1: spend, impressions,
            clicks, inline_link_clicks (LC), landing_page_view action (LPV),
            purchases + purchase value (omni_purchase, else the pixel
            purchase). Each ad's creative destination URL is looked up once
            (?ids= batch) to get its landing path and, for /products/<handle>,
            the product (product_landing_page_map). Campaign rows are the sum
            of their ads.
    Google  googleAds:search over `campaign` and `ad_group` segmented by date,
            plus shopping_performance_view by product_item_id
            ("shopify_<cc>_<product_id>_<variant_id>") for per-product spend.
            Whatever campaign spend Shopping can't place goes to product 0.

Both channels also get the dashboard's own attribution: Shopify orders from
shopify_orders_utm_daily whose utm_content / utm_term / utm_medium /
utm_campaign carry one of the synced ad, ad set / ad group or campaign ids
(or the campaign's exact name) - most specific match wins - land in
utm_orders / utm_sales on that campaign and ad.

After a sync that reaches yesterday, the brand's alert rules (arch-auth Mongo
`ads_alert_rules`, edited on the Ads page) are evaluated per campaign. A
breach is recorded once per (rule, campaign, window end) in `ads_alert_events`
and pushed to the alerts-service's /push/receive (ADS_ALERTS_PUSH_URL +
ADS_ALERTS_PUSH_TOKEN) when configured. evaluate_rule mirrors the dashboard's
analytics/services/adsAlertRules.service.js::evaluateRule - keep them in sync.

Nothing here raises into the P&L: every failure is logged and the P&L build
carries on.
"""

import json
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

from workers import pnl_worker as pw

logger = logging.getLogger(__name__)

ADS_CAMPAIGN_TABLE = "ads_campaign_daily"
ADS_AD_TABLE = "ads_ad_daily"
ADS_PRODUCT_TABLE = "ads_product_daily"
ADS_TABLES = (ADS_CAMPAIGN_TABLE, ADS_AD_TABLE, ADS_PRODUCT_TABLE)
PRODUCT_MAP_TABLE = "product_landing_page_map"
UTM_ORDERS_TABLE = "shopify_orders_utm_daily"
UNATTRIBUTED_PRODUCT_ID = 0

# utm_source values (lower-cased) that can carry each channel's ids/names.
CHANNEL_UTM_SOURCES = {
    "meta": ("facebook", "fb", "instagram", "ig", "meta", "an", "messenger"),
    "google": ("google", "youtube", "gdn", "adwords"),
}

# Overlapping action types: omni_purchase already includes the pixel's
# purchases, so the first one present wins - never summed.
META_PURCHASE_ACTIONS = ("omni_purchase", "purchase", "offsite_conversion.fb_pixel_purchase")
META_LPV_ACTIONS = ("landing_page_view", "omni_landing_page_view")
META_INSIGHT_FIELDS = (
    "campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,spend,impressions,clicks,"
    "inline_link_clicks,actions,action_values,account_currency"
)
META_AD_FIELDS = (
    "effective_status,campaign{effective_status,objective},"
    "creative{link_url,object_story_spec,asset_feed_spec,template_url_spec}"
)
META_IDS_BATCH = 50

ALERT_RULES_COLLECTION = os.environ.get("ADS_ALERT_RULES_COLLECTION", "ads_alert_rules").strip() or "ads_alert_rules"
ALERT_EVENTS_COLLECTION = os.environ.get("ADS_ALERT_EVENTS_COLLECTION", "ads_alert_events").strip() or "ads_alert_events"
ALERTS_PUSH_URL = os.environ.get("ADS_ALERTS_PUSH_URL", "").strip()
ALERTS_PUSH_TOKEN = os.environ.get("ADS_ALERTS_PUSH_TOKEN", "").strip()
# Undelivered events older than this are left alone when pushing is (re)enabled.
PENDING_PUSH_DAYS = int(os.environ.get("ADS_ALERTS_PENDING_PUSH_DAYS", "3"))

ALERT_METRICS = ("roas", "shopify_roas", "lpv_rate", "ctr", "cpc", "cpa")
ALERT_METRIC_LABELS = {
    "roas": "ROAS",
    "shopify_roas": "Shopify ROAS",
    "lpv_rate": "LC to LPV",
    "ctr": "CTR",
    "cpc": "CPC",
    "cpa": "CPA",
}
CHANNEL_LABELS = {"meta": "Meta", "google": "Google"}

ZERO = Decimal("0")
PRODUCT_PATH_RE = re.compile(r"/products/([^/?#]+)", re.IGNORECASE)
SHOPPING_ITEM_RE = re.compile(r"^shopify_[a-z]{2}_(\d+)_\d+$", re.IGNORECASE)


# ---------------------------
# Small helpers
# ---------------------------
def _dec(value) -> Decimal:
    try:
        return Decimal(str(value)) if value not in (None, "") else ZERO
    except Exception:  # noqa: BLE001 - API sent something non-numeric
        return ZERO


def _int(value) -> int:
    return int(_dec(value))


def _first_action(actions, types: Sequence[str]) -> Decimal:
    """Value of the first of `types` present in a Meta actions/action_values
    list ([{"action_type", "value"}]), else 0."""
    by_type = {a.get("action_type"): a.get("value") for a in actions or [] if isinstance(a, dict)}
    for action_type in types:
        if action_type in by_type:
            return _dec(by_type[action_type])
    return ZERO


def landing_path(url: Optional[str]) -> Optional[str]:
    """Lower-cased URL path without a trailing slash ("/" for the home page),
    so it joins the paths the rest of the pipeline stores."""
    if not url:
        return None
    try:
        path = urlparse(str(url).strip()).path or "/"
    except ValueError:
        return None
    path = path.rstrip("/") or "/"
    return path.lower()[:512]


def product_handle(path: Optional[str]) -> Optional[str]:
    match = PRODUCT_PATH_RE.search(path or "")
    return match.group(1).lower() if match else None


def shopping_product_id(item_id: Optional[str]) -> int:
    """Shopify's Google channel item ids are shopify_<country>_<product>_<variant>."""
    match = SHOPPING_ITEM_RE.match(str(item_id or "").strip())
    return int(match.group(1)) if match else UNATTRIBUTED_PRODUCT_ID


def creative_url(creative: Optional[Dict]) -> Optional[str]:
    """Destination URL of a Meta ad creative, from wherever this creative
    type keeps it. None for catalog ads with only a template URL etc."""
    if not isinstance(creative, dict):
        return None
    story = creative.get("object_story_spec") or {}
    link_data = story.get("link_data") or {}
    video_data = story.get("video_data") or {}
    template_data = story.get("template_data") or {}
    feed_links = (creative.get("asset_feed_spec") or {}).get("link_urls") or []
    candidates = [
        link_data.get("link"),
        ((link_data.get("call_to_action") or {}).get("value") or {}).get("link"),
        ((video_data.get("call_to_action") or {}).get("value") or {}).get("link"),
        template_data.get("link"),
        feed_links[0].get("website_url") if feed_links and isinstance(feed_links[0], dict) else None,
        creative.get("link_url"),
    ]
    for url in candidates:
        if url and str(url).startswith("http"):
            return str(url)[:1024]
    return None


def _new_metrics() -> Dict:
    return {
        "spend": ZERO,
        "impressions": 0,
        "clicks": 0,
        "link_clicks": None,
        "landing_page_views": None,
        "platform_purchases": ZERO,
        "platform_purchase_value": ZERO,
        "utm_orders": 0,
        "utm_sales": ZERO,
    }


def _add_metrics(target: Dict, source: Dict) -> None:
    for key in ("spend", "impressions", "clicks", "platform_purchases", "platform_purchase_value", "utm_orders", "utm_sales"):
        target[key] = target[key] + source.get(key, 0)
    for key in ("link_clicks", "landing_page_views"):
        if source.get(key) is not None:
            target[key] = (target[key] or 0) + source[key]


# ---------------------------
# Meta
# ---------------------------
def _meta_get(url: str, params: Optional[Dict], label: str) -> Dict:
    import requests

    try:
        response = requests.get(url, params=params, timeout=pw.AD_API_TIMEOUT_S)
    except requests.RequestException as e:
        raise pw.AdSpendSyncError(f"{label} request failed: {e.__class__.__name__}") from e
    return pw._json_or_raise(response, label)


def fetch_meta_ad_insights(access_token: str, ad_account_id: str, start: date, end: date) -> List[Dict]:
    """Daily ad-level insights for one account, normalized to ads_ad_daily
    rows (without landing/product, filled in later)."""
    account = pw.normalize_meta_ad_account_id(ad_account_id)
    url = f"{pw.META_GRAPH_URL}/{pw.META_API_VERSION}/{account}/insights"
    params: Optional[Dict] = {
        "fields": META_INSIGHT_FIELDS,
        "level": "ad",
        "time_increment": 1,
        "time_range": json.dumps({"since": start.isoformat(), "until": end.isoformat()}),
        "limit": 500,
        "access_token": access_token,
    }
    rows: List[Dict] = []
    for _ in range(pw.AD_API_MAX_PAGES):
        payload = _meta_get(url, params, "Meta ad insights")
        for item in payload.get("data") or []:
            if not item.get("date_start") or not item.get("ad_id"):
                continue
            row = {
                "date": pw._as_date(item["date_start"]),
                "channel": "meta",
                "account_id": account,
                "campaign_id": str(item.get("campaign_id") or ""),
                "campaign_name": item.get("campaign_name"),
                "adset_id": str(item.get("adset_id") or "") or None,
                "adset_name": item.get("adset_name"),
                "ad_id": str(item["ad_id"]),
                "ad_name": item.get("ad_name"),
                "currency": item.get("account_currency"),
                **_new_metrics(),
            }
            row["spend"] = _dec(item.get("spend"))
            row["impressions"] = _int(item.get("impressions"))
            row["clicks"] = _int(item.get("clicks"))
            row["link_clicks"] = _int(item.get("inline_link_clicks"))
            row["landing_page_views"] = int(_first_action(item.get("actions"), META_LPV_ACTIONS))
            row["platform_purchases"] = _first_action(item.get("actions"), META_PURCHASE_ACTIONS)
            row["platform_purchase_value"] = _first_action(item.get("action_values"), META_PURCHASE_ACTIONS)
            rows.append(row)
        next_url = (payload.get("paging") or {}).get("next")
        if not next_url:
            break
        url, params = next_url, None
    return rows


def fetch_meta_ad_metadata(access_token: str, ad_ids: Iterable[str]) -> Dict[str, Dict]:
    """{ad_id: {"landing_url", "campaign_status", "campaign_type"}} via the
    Graph API's ?ids= batch read. Best-effort: a failed batch is logged and
    those ads just have no landing page / status."""
    ids = sorted({a for a in ad_ids if a})
    meta: Dict[str, Dict] = {}
    url = f"{pw.META_GRAPH_URL}/{pw.META_API_VERSION}/"
    for i in range(0, len(ids), META_IDS_BATCH):
        batch = ids[i : i + META_IDS_BATCH]
        try:
            payload = _meta_get(
                url,
                {"ids": ",".join(batch), "fields": META_AD_FIELDS, "access_token": access_token},
                "Meta ad metadata",
            )
        except pw.AdSpendSyncError as e:
            logger.warning("Meta ad metadata batch failed (%s ads): %s", len(batch), e)
            continue
        for ad_id, ad in payload.items():
            if not isinstance(ad, dict):
                continue
            campaign = ad.get("campaign") or {}
            meta[str(ad_id)] = {
                "landing_url": creative_url(ad.get("creative")),
                "campaign_status": campaign.get("effective_status") or ad.get("effective_status"),
                "campaign_type": campaign.get("objective"),
            }
    return meta


def fetch_meta_performance(doc: Dict, start: date, end: date) -> Dict[str, List[Dict]]:
    """{"ads": [...], "campaign_meta": {campaign_id: {status, type}}} across
    every authorized account; one failing account is skipped unless all fail
    (same rule as the spend sync)."""
    token = pw.decrypt_text(doc.get("access_token_encrypted"))
    accounts = pw.normalize_meta_ad_account_ids(doc)
    if not accounts:
        raise pw.AdSpendSyncError("Meta credential has no ad_account_id")
    ads: List[Dict] = []
    failures: List[str] = []
    for account in accounts:
        try:
            ads.extend(fetch_meta_ad_insights(token, account, start, end))
        except Exception as e:  # noqa: BLE001
            failures.append(f"{account}: {e}")
    if len(failures) == len(accounts):
        raise pw.AdSpendSyncError("; ".join(failures))
    if failures:
        logger.warning("Some Meta ad accounts failed and were skipped: %s", "; ".join(failures))

    metadata = fetch_meta_ad_metadata(token, (row["ad_id"] for row in ads))
    campaign_meta: Dict[str, Dict] = {}
    for row in ads:
        info = metadata.get(row["ad_id"]) or {}
        row["landing_url"] = info.get("landing_url")
        if info.get("campaign_status") or info.get("campaign_type"):
            campaign_meta.setdefault(row["campaign_id"], {
                "campaign_status": info.get("campaign_status"),
                "campaign_type": info.get("campaign_type"),
            })
    return {"ads": ads, "campaign_meta": campaign_meta}


# ---------------------------
# Google
# ---------------------------
def _google_search(access_token: str, customer_id: str, login_customer_id: Optional[str], query: str) -> List[Dict]:
    import requests

    if not pw.GOOGLE_ADS_DEVELOPER_TOKEN:
        raise pw.AdSpendSyncError("GOOGLE_ADS_DEVELOPER_TOKEN is not configured on the worker")
    headers = {"Authorization": f"Bearer {access_token}", "developer-token": pw.GOOGLE_ADS_DEVELOPER_TOKEN}
    login = pw.normalize_google_customer_id(login_customer_id)
    if login:
        headers["login-customer-id"] = login
    url = f"{pw.GOOGLE_ADS_BASE_URL}/{pw.GOOGLE_ADS_API_VERSION}/customers/{customer_id}/googleAds:search"
    results: List[Dict] = []
    page_token = None
    for _ in range(pw.AD_API_MAX_PAGES):
        body = {"query": query, "pageToken": page_token} if page_token else {"query": query}
        try:
            response = requests.post(url, json=body, headers=headers, timeout=pw.AD_API_TIMEOUT_S)
        except requests.RequestException as e:
            raise pw.AdSpendSyncError(f"Google Ads request failed: {e.__class__.__name__}") from e
        payload = pw._json_or_raise(response, "Google Ads")
        results.extend(payload.get("results") or [])
        page_token = payload.get("nextPageToken")
        if not page_token:
            break
    return results


def _google_metrics(result: Dict) -> Dict:
    metrics = result.get("metrics") or {}
    out = _new_metrics()
    out["spend"] = _dec(metrics.get("costMicros")) / Decimal(1_000_000)
    out["impressions"] = _int(metrics.get("impressions"))
    out["clicks"] = _int(metrics.get("clicks"))
    out["platform_purchases"] = _dec(metrics.get("conversions"))
    out["platform_purchase_value"] = _dec(metrics.get("conversionsValue"))
    return out


def _google_date_clause(start: date, end: date) -> str:
    return f"segments.date BETWEEN '{start.isoformat()}' AND '{end.isoformat()}'"


def fetch_google_customer_performance(
    access_token: str, customer_id: str, login_customer_id: Optional[str], start: date, end: date
) -> Dict[str, List[Dict]]:
    cid = pw.normalize_google_customer_id(customer_id)
    where = _google_date_clause(start, end)
    campaigns: List[Dict] = []
    for result in _google_search(
        access_token,
        cid,
        login_customer_id,
        "SELECT segments.date, campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, "
        "customer.currency_code, metrics.cost_micros, metrics.impressions, metrics.clicks, metrics.conversions, "
        f"metrics.conversions_value FROM campaign WHERE {where} AND metrics.impressions > 0",
    ):
        campaign = result.get("campaign") or {}
        campaigns.append({
            "date": pw._as_date((result.get("segments") or {}).get("date")),
            "channel": "google",
            "account_id": cid,
            "campaign_id": str(campaign.get("id") or ""),
            "campaign_name": campaign.get("name"),
            "campaign_status": campaign.get("status"),
            "campaign_type": campaign.get("advertisingChannelType"),
            "currency": (result.get("customer") or {}).get("currencyCode"),
            **_google_metrics(result),
        })

    ad_groups: List[Dict] = []
    for result in _google_search(
        access_token,
        cid,
        login_customer_id,
        "SELECT segments.date, campaign.id, ad_group.id, ad_group.name, metrics.cost_micros, metrics.impressions, "
        f"metrics.clicks, metrics.conversions, metrics.conversions_value FROM ad_group WHERE {where} "
        "AND metrics.impressions > 0",
    ):
        group = result.get("adGroup") or {}
        ad_groups.append({
            "date": pw._as_date((result.get("segments") or {}).get("date")),
            "channel": "google",
            "account_id": cid,
            "campaign_id": str((result.get("campaign") or {}).get("id") or ""),
            "adset_id": None,
            "adset_name": None,
            "ad_id": str(group.get("id") or ""),
            "ad_name": group.get("name"),
            "landing_url": None,
            **_google_metrics(result),
        })

    shopping: List[Dict] = []
    try:
        shopping_results = _google_search(
            access_token,
            cid,
            login_customer_id,
            "SELECT segments.date, segments.product_item_id, metrics.cost_micros, metrics.impressions, "
            f"metrics.clicks, metrics.conversions_value FROM shopping_performance_view WHERE {where} "
            "AND metrics.impressions > 0",
        )
    except pw.AdSpendSyncError as e:  # no Shopping/PMax on this account is not a failure
        logger.info("Google shopping_performance_view unavailable for %s: %s", cid, e)
        shopping_results = []
    for result in shopping_results:
        segments = result.get("segments") or {}
        metrics = _google_metrics(result)
        shopping.append({
            "date": pw._as_date(segments.get("date")),
            "product_id": shopping_product_id(segments.get("productItemId")),
            "spend": metrics["spend"],
            "impressions": metrics["impressions"],
            "clicks": metrics["clicks"],
            "platform_purchase_value": metrics["platform_purchase_value"],
        })
    return {"campaigns": campaigns, "ads": ad_groups, "shopping": shopping}


def fetch_google_performance(doc: Dict, start: date, end: date) -> Dict[str, List[Dict]]:
    customer_ids = pw.normalize_google_customer_ids(doc)
    if not customer_ids:
        raise pw.AdSpendSyncError("Google Ads credential has no customer_id")
    access_token = pw.google_access_token(pw.decrypt_text(doc.get("token_encrypted")))
    combined: Dict[str, List[Dict]] = {"campaigns": [], "ads": [], "shopping": []}
    failures: List[str] = []
    for customer_id in customer_ids:
        try:
            part = fetch_google_customer_performance(access_token, customer_id, doc.get("login_customer_id"), start, end)
        except Exception as e:  # noqa: BLE001
            failures.append(f"{customer_id}: {e}")
            continue
        for key in combined:
            combined[key].extend(part[key])
    if failures and len(failures) == len(customer_ids):
        raise pw.AdSpendSyncError("; ".join(failures))
    if failures:
        logger.warning("Some Google Ads accounts failed and were skipped: %s", "; ".join(failures))
    return combined


# ---------------------------
# Brand DB reads
# ---------------------------
def fetch_product_handle_map(cursor) -> Dict[str, int]:
    if not pw.table_exists(cursor, PRODUCT_MAP_TABLE):
        return {}
    cursor.execute(f"SELECT handle, product_id FROM `{PRODUCT_MAP_TABLE}` WHERE handle IS NOT NULL")
    out: Dict[str, int] = {}
    for row in cursor.fetchall() or []:
        try:
            out[str(row["handle"]).strip().lower()] = int(row["product_id"])
        except (TypeError, ValueError):
            continue
    return out


def fetch_utm_order_rows(cursor, channel: str, start: date, end: date) -> List[Dict]:
    if not pw.table_exists(cursor, UTM_ORDERS_TABLE):
        return []
    sources = CHANNEL_UTM_SOURCES[channel]
    placeholders = ", ".join(["%s"] * len(sources))
    cursor.execute(
        f"""
        SELECT `date`, utm_medium, utm_campaign, utm_content, utm_term,
               COALESCE(total_orders, 0) AS total_orders, COALESCE(total_sales, 0) AS total_sales
        FROM `{UTM_ORDERS_TABLE}`
        WHERE `date` BETWEEN %s AND %s AND LOWER(utm_source) IN ({placeholders})
        """,
        (start, end, *sources),
    )
    return list(cursor.fetchall() or [])


# ---------------------------
# Shaping (pure)
# ---------------------------
def _norm(value) -> str:
    return str(value or "").strip().lower()


def build_utm_index(ads: Sequence[Dict], campaigns: Sequence[Dict]) -> Dict[str, Dict[str, Tuple[str, Optional[str]]]]:
    """Lookup tables from a UTM value to (campaign_id, ad_id | None).
    A campaign name shared by two campaigns is dropped (ambiguous)."""
    by_ad: Dict[str, Tuple[str, Optional[str]]] = {}
    by_adset: Dict[str, Tuple[str, Optional[str]]] = {}
    by_campaign: Dict[str, Tuple[str, Optional[str]]] = {}
    names: Dict[str, set] = {}
    for row in list(ads) + list(campaigns):
        cid = row.get("campaign_id")
        if not cid:
            continue
        by_campaign[_norm(cid)] = (cid, None)
        if row.get("campaign_name"):
            names.setdefault(_norm(row["campaign_name"]), set()).add(cid)
        if row.get("ad_id"):
            by_ad[_norm(row["ad_id"])] = (cid, row["ad_id"])
        if row.get("adset_id"):
            by_adset[_norm(row["adset_id"])] = (cid, None)
    for name, ids in names.items():
        if len(ids) == 1 and name not in by_campaign:
            by_campaign[name] = (next(iter(ids)), None)
    return {"ad": by_ad, "adset": by_adset, "campaign": by_campaign}


def match_utm_row(row: Dict, index: Dict) -> Optional[Tuple[str, Optional[str]]]:
    """Most specific id wins: an ad (or Google ad group) id in utm_content /
    utm_term, then an ad set id in utm_medium / utm_content, then the
    campaign id or exact name in utm_campaign."""
    for field in ("utm_content", "utm_term"):
        hit = index["ad"].get(_norm(row.get(field)))
        if hit:
            return hit
    for field in ("utm_medium", "utm_content"):
        hit = index["adset"].get(_norm(row.get(field)))
        if hit:
            return hit
    return index["campaign"].get(_norm(row.get("utm_campaign")))


def apply_utm_attribution(
    ads: List[Dict], campaigns: Dict[Tuple[date, str], Dict], utm_rows: Sequence[Dict], index: Dict, channel: str
) -> Tuple[int, Decimal]:
    """Adds matched Shopify orders onto the campaign (and ad) rows of the
    same date. An order on a day its campaign didn't deliver still counts:
    that campaign gets a zero-spend row for the day. Returns the matched
    (orders, sales) totals for logging."""
    ads_by_key = {(row["date"], row["ad_id"]): row for row in ads}
    names = {row["campaign_id"]: row.get("campaign_name") for row in list(ads) + list(campaigns.values())}
    matched_orders, matched_sales = 0, ZERO
    for utm in utm_rows:
        hit = match_utm_row(utm, index)
        if not hit:
            continue
        campaign_id, ad_id = hit
        day = pw._as_date(utm["date"])
        orders, sales = int(utm.get("total_orders") or 0), _dec(utm.get("total_sales"))
        camp = campaigns.get((day, campaign_id))
        if camp is None:
            camp = campaigns[(day, campaign_id)] = {
                "date": day,
                "channel": channel,
                "account_id": None,
                "campaign_id": campaign_id,
                "campaign_name": names.get(campaign_id),
                "campaign_status": None,
                "campaign_type": None,
                "currency": None,
                **_new_metrics(),
            }
        camp["utm_orders"] += orders
        camp["utm_sales"] += sales
        ad = ads_by_key.get((day, ad_id)) if ad_id else None
        if ad is not None:
            ad["utm_orders"] += orders
            ad["utm_sales"] += sales
        matched_orders += orders
        matched_sales += sales
    return matched_orders, matched_sales


def roll_up_meta_campaigns(ads: Sequence[Dict], campaign_meta: Dict[str, Dict]) -> Dict[Tuple[date, str], Dict]:
    campaigns: Dict[Tuple[date, str], Dict] = {}
    for ad in ads:
        key = (ad["date"], ad["campaign_id"])
        camp = campaigns.get(key)
        if camp is None:
            info = campaign_meta.get(ad["campaign_id"]) or {}
            camp = campaigns[key] = {
                "date": ad["date"],
                "channel": "meta",
                "account_id": ad.get("account_id"),
                "campaign_id": ad["campaign_id"],
                "campaign_name": ad.get("campaign_name"),
                "campaign_status": info.get("campaign_status"),
                "campaign_type": info.get("campaign_type"),
                "currency": ad.get("currency"),
                **_new_metrics(),
            }
        _add_metrics(camp, {k: v for k, v in ad.items() if k not in ("utm_orders", "utm_sales")})
    return campaigns


def attach_landing(ads: Sequence[Dict], handle_map: Dict[str, int]) -> None:
    for ad in ads:
        ad["landing_path"] = landing_path(ad.get("landing_url"))
        handle = product_handle(ad["landing_path"])
        ad["product_id"] = handle_map.get(handle) if handle else None


def build_product_rows(channel: str, ads: Sequence[Dict], campaigns: Sequence[Dict], shopping: Sequence[Dict]) -> List[Dict]:
    """Per (date, product) spend. Meta: each ad's spend goes to its landing
    product. Google: Shopping item spend per product; the rest of the day's
    campaign spend is product 0. Either way the rows sum to the channel's
    spend for the day."""
    rows: Dict[Tuple[date, int], Dict] = {}

    def bucket(day: date, product_id: int, source: str) -> Dict:
        key = (day, product_id)
        if key not in rows:
            rows[key] = {
                "date": day,
                "channel": channel,
                "product_id": product_id,
                "source": source,
                "spend": ZERO,
                "impressions": 0,
                "clicks": 0,
                "platform_purchase_value": ZERO,
            }
        return rows[key]

    def add(target: Dict, source: Dict) -> None:
        for key in ("spend", "impressions", "clicks", "platform_purchase_value"):
            target[key] += source.get(key) or 0

    if channel == "meta":
        for ad in ads:
            product_id = ad.get("product_id") or UNATTRIBUTED_PRODUCT_ID
            add(bucket(ad["date"], product_id, "creative" if product_id else "unattributed"), ad)
        return list(rows.values())

    def zeros() -> Dict:
        return {"spend": ZERO, "impressions": 0, "clicks": 0, "platform_purchase_value": ZERO}

    placed: Dict[date, Dict] = {}
    for item in shopping:
        if item["product_id"]:
            add(bucket(item["date"], item["product_id"], "shopping"), item)
            add(placed.setdefault(item["date"], zeros()), item)
    totals: Dict[date, Dict] = {}
    for camp in campaigns:
        add(totals.setdefault(camp["date"], zeros()), camp)
    # Product 0 is whatever of the day's campaign totals Shopping didn't
    # place on a product (Search/Display, unparseable item ids, ...).
    for day, total in totals.items():
        done = placed.get(day, zeros())
        remainder = {key: max(total[key] - done[key], 0) for key in total}
        if remainder["spend"] > 0 or remainder["impressions"] > 0:
            add(bucket(day, UNATTRIBUTED_PRODUCT_ID, "unattributed"), remainder)
    return list(rows.values())


# ---------------------------
# Brand DB writes
# ---------------------------
CAMPAIGN_COLUMNS = (
    "date", "channel", "account_id", "campaign_id", "campaign_name", "campaign_status", "campaign_type",
    "currency", "spend", "impressions", "clicks", "link_clicks", "landing_page_views",
    "platform_purchases", "platform_purchase_value", "utm_orders", "utm_sales",
)
AD_COLUMNS = (
    "date", "channel", "account_id", "campaign_id", "adset_id", "adset_name", "ad_id", "ad_name",
    "landing_url", "landing_path", "product_id", "spend", "impressions", "clicks", "link_clicks",
    "landing_page_views", "platform_purchases", "platform_purchase_value", "utm_orders", "utm_sales",
)
PRODUCT_COLUMNS = (
    "date", "channel", "product_id", "source", "spend", "impressions", "clicks", "platform_purchase_value",
)
TEXT_LIMITS = {"campaign_name": 512, "adset_name": 512, "ad_name": 512, "landing_url": 1024, "landing_path": 512}


def _row_values(row: Dict, columns: Sequence[str]) -> Tuple:
    values = []
    for column in columns:
        value = row.get(column)
        if column in TEXT_LIMITS and value is not None:
            value = str(value)[: TEXT_LIMITS[column]]
        if isinstance(value, Decimal):
            value = value.quantize(Decimal("0.0001"))
        values.append(value)
    return tuple(values)


def replace_window(cursor, connection, table: str, columns: Sequence[str], channel: str, start: date, end: date, rows: Sequence[Dict]) -> int:
    """DELETE the channel's rows for [start, end] then INSERT the fresh ones,
    so entities that stopped delivering don't linger. ON DUPLICATE KEY
    covers two accounts reporting the same campaign id."""
    from pipeline.db import executemany_chunked

    cursor.execute(f"DELETE FROM `{table}` WHERE `channel` = %s AND `date` BETWEEN %s AND %s", (channel, start, end))
    connection.commit()
    if not rows:
        return 0
    key_columns = {"date", "channel", "campaign_id", "ad_id", "product_id"}
    column_sql = ", ".join(f"`{c}`" for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    updates = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in columns if c not in key_columns)
    sql = f"INSERT INTO `{table}` ({column_sql}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
    return executemany_chunked(cursor, connection, sql, [_row_values(row, columns) for row in rows])


# ---------------------------
# Orchestration
# ---------------------------
def shape_channel(channel: str, fetched: Dict, handle_map: Dict[str, int], utm_rows: Sequence[Dict]) -> Dict[str, List[Dict]]:
    """API output -> rows for the three tables (pure, no I/O)."""
    ads = fetched["ads"]
    if channel == "meta":
        attach_landing(ads, handle_map)
        campaigns = roll_up_meta_campaigns(ads, fetched.get("campaign_meta") or {})
    else:
        for ad in ads:
            ad.setdefault("landing_path", None)
            ad.setdefault("product_id", None)
        campaigns = {(row["date"], row["campaign_id"]): row for row in fetched["campaigns"]}
    index = build_utm_index(ads, list(campaigns.values()))
    apply_utm_attribution(ads, campaigns, utm_rows, index, channel)
    campaign_rows = list(campaigns.values())
    products = build_product_rows(channel, ads, campaign_rows, fetched.get("shopping") or [])
    return {"campaigns": campaign_rows, "ads": ads, "products": products}


def sync_ads_performance(
    cursor,
    connection,
    brand_key: str,
    brand_name: str,
    credentials: Dict[str, Optional[Dict]],
    target_dates: Sequence[date],
) -> Dict[str, str]:
    """Never raises. {channel: "synced" | "no-credentials" | "not-system-user" | "no-table" | "failed"}."""
    start, end = min(target_dates), max(target_dates)
    outcomes: Dict[str, str] = {}
    missing = [table for table in ADS_TABLES if not pw.table_exists(cursor, table)]
    handle_map: Optional[Dict[str, int]] = None
    for channel in pw.AD_CHANNELS:
        doc = credentials.get(channel)
        if not doc:
            outcomes[channel] = "no-credentials"
            continue
        if channel == "meta" and doc.get("token_type") != pw.META_REQUIRED_TOKEN_TYPE:
            outcomes[channel] = "not-system-user"  # sync_ad_spend already recorded last_error
            continue
        if missing:
            outcomes[channel] = "no-table"
            logger.warning(
                "[%s] %s connected but %s missing in the brand DB; run migrations/007_ads_performance.sql",
                brand_name,
                channel,
                ", ".join(missing),
            )
            continue
        try:
            fetched = fetch_meta_performance(doc, start, end) if channel == "meta" else fetch_google_performance(doc, start, end)
            if handle_map is None:
                handle_map = fetch_product_handle_map(cursor)
            shaped = shape_channel(channel, fetched, handle_map, fetch_utm_order_rows(cursor, channel, start, end))
            written = {
                "campaigns": replace_window(cursor, connection, ADS_CAMPAIGN_TABLE, CAMPAIGN_COLUMNS, channel, start, end, shaped["campaigns"]),
                "ads": replace_window(cursor, connection, ADS_AD_TABLE, AD_COLUMNS, channel, start, end, shaped["ads"]),
                "products": replace_window(cursor, connection, ADS_PRODUCT_TABLE, PRODUCT_COLUMNS, channel, start, end, shaped["products"]),
            }
            outcomes[channel] = "synced"
            logger.info(
                "[%s] %s ads performance synced for %s..%s: %s",
                brand_name,
                channel,
                start,
                end,
                ", ".join(f"{k}={v}" for k, v in written.items()),
            )
        except Exception as e:  # noqa: BLE001 - never sink the P&L
            outcomes[channel] = "failed"
            logger.error("[%s] %s ads performance sync failed: %s", brand_name, channel, e)

    if any(v == "synced" for v in outcomes.values()) and end >= date.today() - timedelta(days=2):
        try:
            evaluate_brand_alerts(cursor, brand_key, brand_name, end)
        except Exception as e:  # noqa: BLE001
            logger.error("[%s] ads alert evaluation failed: %s", brand_name, e)
    return outcomes


# ---------------------------
# Alerts
# ---------------------------
def metric_value(metric: str, row: Dict) -> Optional[float]:
    """Same definitions as the dashboard (adsAlertRules.service.js::metricValue)."""
    spend = float(row.get("spend") or 0)
    if metric == "roas":
        return float(row.get("platform_purchase_value") or 0) / spend if spend > 0 else None
    if metric == "shopify_roas":
        return float(row.get("utm_sales") or 0) / spend if spend > 0 else None
    if metric == "lpv_rate":
        link_clicks = float(row.get("link_clicks") or 0)
        return float(row.get("landing_page_views") or 0) / link_clicks * 100 if link_clicks > 0 else None
    if metric == "ctr":
        impressions = float(row.get("impressions") or 0)
        return float(row.get("clicks") or 0) / impressions * 100 if impressions > 0 else None
    if metric == "cpc":
        clicks = float(row.get("clicks") or 0)
        return spend / clicks if clicks > 0 else None
    if metric == "cpa":
        purchases = float(row.get("platform_purchases") or 0)
        return spend / purchases if purchases > 0 else None
    return None


def evaluate_rule(rule: Dict, row: Dict) -> Optional[float]:
    """The metric value when `row` (one campaign summed over the rule's
    window) breaches `rule`, else None."""
    if not rule.get("is_active", True) or rule.get("metric") not in ALERT_METRICS:
        return None
    channel = rule.get("channel") or "all"
    if channel != "all" and channel != row.get("channel"):
        return None
    if float(row.get("spend") or 0) < float(rule.get("min_spend") or 0):
        return None
    clicks = row.get("link_clicks") if rule["metric"] == "lpv_rate" else row.get("clicks")
    if float(clicks or 0) < float(rule.get("min_clicks") or 0):
        return None
    value = metric_value(rule["metric"], row)
    if value is None:
        return None
    threshold = float(rule.get("threshold") or 0)
    breached = value < threshold if rule.get("operator", "below") == "below" else value > threshold
    return value if breached else None


def fetch_campaign_window(cursor, start: date, end: date) -> List[Dict]:
    cursor.execute(
        f"""
        SELECT channel, campaign_id, MAX(campaign_name) AS campaign_name,
               SUM(spend) AS spend, SUM(impressions) AS impressions, SUM(clicks) AS clicks,
               SUM(link_clicks) AS link_clicks, SUM(landing_page_views) AS landing_page_views,
               SUM(platform_purchases) AS platform_purchases,
               SUM(platform_purchase_value) AS platform_purchase_value,
               SUM(utm_orders) AS utm_orders, SUM(utm_sales) AS utm_sales
        FROM `{ADS_CAMPAIGN_TABLE}`
        WHERE `date` BETWEEN %s AND %s
        GROUP BY channel, campaign_id
        """,
        (start, end),
    )
    return list(cursor.fetchall() or [])


def _mongo_db():
    from pymongo import MongoClient

    client = MongoClient(pw.PNL_CONFIG_MONGO_URI, tz_aware=True)
    return client, client[pw.PNL_CONFIG_MONGO_DB]


def push_alert(event: Dict, brand_key: str) -> bool:
    """Hands a breach to the alerts-service so it lands in the bell menu +
    FCM like every other alert. True only when the alerts-service accepted
    it (2xx); False when unconfigured or the call failed, so the caller
    leaves the event unsent and retries it next run."""
    if not ALERTS_PUSH_URL or not ALERTS_PUSH_TOKEN:
        return False
    import requests

    threshold = float(event["threshold"])
    value = float(event["value"])
    delta = ((value - threshold) / threshold * 100) if threshold else 0
    channel_label = CHANNEL_LABELS.get(event["channel"], event["channel"])
    metric_label = ALERT_METRIC_LABELS.get(event["metric"], event["metric"])
    payload = {
        "source": "ads",
        "event": {
            "event_id": event["event_key"],
            "brand": brand_key,
            "metric": f"{channel_label} {metric_label} - {event.get('campaign_name') or event['campaign_id']}",
            "current_value": round(value, 2),
            "threshold_value": threshold,
            "threshold_type": "less_than" if event["operator"] == "below" else "greater_than",
            "delta_percent": round(delta, 2),
            "severity": event.get("severity") or "medium",
            "condition": f"{metric_label} {event['operator']} {threshold:g} over {event['lookback_days']}d",
            "rule_name": event.get("rule_name"),
            "campaign_id": event["campaign_id"],
            "window_start": event["window_start"],
            "window_end": event["window_end"],
        },
    }
    try:
        response = requests.post(ALERTS_PUSH_URL, json=payload, headers={"x-push-token": ALERTS_PUSH_TOKEN}, timeout=10)
    except requests.RequestException as e:
        logger.warning("ads alert push failed for %s: %s", event["event_key"], e)
        return False
    if response.status_code >= 300:
        logger.warning("ads alert push rejected for %s: HTTP %s", event["event_key"], response.status_code)
        return False
    return True


def deliver_pending_alerts(db, brand_key: str, brand_name: str) -> int:
    """Pushes every recent event of the brand that hasn't been delivered yet
    (no `pushed_at`) and stamps the ones the alerts-service accepted. An
    event recorded while pushing was unconfigured or alerts-service was down
    therefore still goes out on a later run instead of being lost. Only the
    last PENDING_PUSH_DAYS of events are retried, so turning pushing on
    doesn't flood the bell with old breaches."""
    if not ALERTS_PUSH_URL or not ALERTS_PUSH_TOKEN:
        return 0
    since = datetime.now(timezone.utc) - timedelta(days=PENDING_PUSH_DAYS)
    events = db[ALERT_EVENTS_COLLECTION].find(
        {"brand_id": brand_key, "pushed_at": None, "created_at": {"$gte": since}}
    ).sort("created_at", 1)
    sent = 0
    for event in events:
        if push_alert(event, brand_key):
            db[ALERT_EVENTS_COLLECTION].update_one(
                {"_id": event["_id"]}, {"$set": {"pushed_at": datetime.now(timezone.utc)}}
            )
            sent += 1
    if sent:
        logger.info("[%s] ads alerts: pushed %s event(s) to the alerts-service", brand_name, sent)
    return sent


def evaluate_brand_alerts(cursor, brand_key: str, brand_name: str, end: date) -> int:
    """Checks every active rule against each campaign's window ending `end`.
    Each breach is stored once (unique event_key, `pushed_at` unset), then
    deliver_pending_alerts pushes whatever isn't delivered yet - so
    re-running the same night never re-notifies, but an event whose push
    failed is retried. Returns new events."""
    if not pw.PNL_CONFIG_MONGO_URI:
        return 0
    client, db = _mongo_db()
    try:
        doc = db[ALERT_RULES_COLLECTION].find_one({"brand_id": brand_key}) or {}
        rules = [r for r in doc.get("rules") or [] if r.get("is_active", True)]
        if not rules:
            deliver_pending_alerts(db, brand_key, brand_name)
            return 0
        windows: Dict[int, List[Dict]] = {}
        created = 0
        for rule in rules:
            lookback = max(1, min(30, int(rule.get("lookback_days") or 1)))
            if lookback not in windows:
                windows[lookback] = fetch_campaign_window(cursor, end - timedelta(days=lookback - 1), end)
            for row in windows[lookback]:
                value = evaluate_rule(rule, row)
                if value is None:
                    continue
                event_key = f"{brand_key}:{rule.get('rule_id')}:{row['channel']}:{row['campaign_id']}:{end.isoformat()}"
                event = {
                    "event_key": event_key,
                    "brand_id": brand_key,
                    "rule_id": rule.get("rule_id"),
                    "rule_name": rule.get("name"),
                    "severity": rule.get("severity") or "medium",
                    "channel": row["channel"],
                    "campaign_id": row["campaign_id"],
                    "campaign_name": row.get("campaign_name"),
                    "metric": rule["metric"],
                    "operator": rule.get("operator", "below"),
                    "threshold": float(rule.get("threshold") or 0),
                    "value": round(value, 4),
                    "spend": float(row.get("spend") or 0),
                    "lookback_days": lookback,
                    "window_start": (end - timedelta(days=lookback - 1)).isoformat(),
                    "window_end": end.isoformat(),
                }
                result = db[ALERT_EVENTS_COLLECTION].update_one(
                    {"event_key": event_key},
                    {"$setOnInsert": {**event, "created_at": datetime.now(timezone.utc), "pushed_at": None}},
                    upsert=True,
                )
                if result.upserted_id is not None:
                    created += 1
        logger.info("[%s] ads alerts: %s new breach(es) for window ending %s", brand_name, created, end)
        deliver_pending_alerts(db, brand_key, brand_name)
        return created
    finally:
        client.close()
