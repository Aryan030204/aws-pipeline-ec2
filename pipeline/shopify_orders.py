"""
Shopify order extract/transform/load: async order fetching from the
Shopify Admin REST API, the order-to-DataFrame transform (bundle item
columns, UTM parsing, user agent/customer tag), returns_fact upsert,
discount-dashboard order-line fact upsert/backfill, products_sold_units_daily
sync, affected-dates tracking, RTO tag matching, and the bulk
shopify_orders/shopify_orders_update loader.
"""

import asyncio
import json
import os
import re
import time
import traceback
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import aiohttp
import numpy as np
import pandas as pd
import requests
from sqlalchemy import Column, DateTime, Float, Integer, MetaData, String, Table, Text, text
from sqlalchemy.exc import SQLAlchemyError

from pipeline.state import (
    logger,
    active_brand_indices,
    brand_db_configs,
    brand_rto_tags_by_index,
    sqlalchemy_engines,
)
from pipeline.db import get_db_cursor, executemany_chunked, timed
from pipeline.shopifyql_sessions import _format_shopifyql_table, _make_shopifyql_session

# aws_background still owns extract_bundle_item_columns_from_props (misc_utils),
# get_brand_timezone/is_backfill_active_for/get_backfill_window_for_brand
# (brand_config_init/backfill-config, used broadly across categories), and the
# BACKFILL_START_IST/BACKFILL_END_IST/RTO_ASSUMPTION_PERIOD/
# DISCOUNT_BACKFILL_START_DATE_PARSED/DISCOUNT_BACKFILL_END_DATE_PARSED module
# constants. Imported at module level here is safe: attribute access only
# happens when these functions are actually CALLED, during real pipeline
# execution, long after aws_background.py has fully finished loading (see
# pipeline/shopifyql_sessions.py and pipeline/notifications.py for the same
# pattern).
import aws_background


async def fetch_orders_async(
    api_base_url: str,
    access_token: str,
    start_date: str,
    end_date: str,
    date_filter_field: str,
) -> List[Dict]:
    headers = {"X-Shopify-Access-Token": access_token}
    order_list: List[Dict] = []

    # Ensure refunds/cancel snapshots are present
    fields = (
        "id,name,tags,created_at,updated_at,cancelled_at,total_price,financial_status,"
        "payment_gateway_names,app_id,currency,discount_codes,discount_applications,landing_site,"
        "refunds,refunds/created_at,refunds/transactions,"
        "refunds/transactions/amount,refunds/transactions/kind,refunds/transactions/status,"
        "line_items,line_items/sku,line_items/title,line_items/variant_title,"
        "line_items/price,line_items/quantity,line_items/total_discount,"
        "line_items/product_id,line_items/variant_id,line_items/properties,"
        "line_items/properties/name,line_items/properties/value,"
        "customer,customer/id,customer/email,customer/first_name,"
        "customer/last_name,customer/phone,customer/tags,"
        "shipping_address,shipping_address/address1,shipping_address/phone,"
        "shipping_address/city,shipping_address/zip,shipping_address/province,"
        "billing_address,billing_address/address1,billing_address/phone,"
        "billing_address/city,billing_address/zip,billing_address/province,"
        "note_attributes,note_attributes/name,note_attributes/value"
    )

    date_filter_field_max = date_filter_field.replace("_min", "_max")
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
                        retry_after = int(response.headers.get("Retry-After", 2))
                        logger.warning(f"Rate limited, waiting {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue

                    if response.status != 200:
                        logger.error(f"Failed to fetch data: {response.status}")
                        break

                    data = await response.json()
                    orders = data.get("orders", [])
                    if not orders:
                        break

                    order_list.extend(orders)

                    # parse Link header for next
                    link_header = response.headers.get("Link", "")
                    url = None
                    if 'rel="next"' in link_header:
                        for part in link_header.split(","):
                            if 'rel="next"' in part:
                                url = part.split(";")[0].strip("<> ")
                                break

                    await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Error fetching orders: {e}")
                break

    return order_list


def fetch_orders(
    api_base_url: str,
    access_token: str,
    start_date: str,
    end_date: str,
    date_filter_field: str,
) -> List[Dict]:
    with timed(f"Shopify fetch ({date_filter_field} window)"):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(
                fetch_orders_async(
                    api_base_url, access_token, start_date, end_date, date_filter_field
                )
            )
        finally:
            loop.close()


# ---------------------------
# Transform (optimized; logic preserved)
# ---------------------------
def convert_to_desired_format(dt_obj: datetime, tz: ZoneInfo) -> str:
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=tz)
    else:
        dt_obj = dt_obj.astimezone(tz)
    iso_str = dt_obj.isoformat()
    return iso_str[:19] + iso_str[19:].replace("+", "%2B")


def format_datetime(datetime_str: Optional[str]) -> Optional[str]:
    if not datetime_str:
        return None
    return datetime_str.replace("Z", "+00:00") and datetime.fromisoformat(
        datetime_str.replace("Z", "+00:00")
    ).strftime("%Y-%m-%d %H:%M:%S")


def _parse_iso_to_store_date_time(
    iso_ts: Optional[str], tz: ZoneInfo
) -> Tuple[Optional[str], Optional[str]]:
    if not iso_ts:
        return None, None
    try:
        dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00")).astimezone(tz)
        return dt.date().isoformat(), dt.strftime("%H:%M:%S")
    except Exception:
        return None, None


def transform_orders_to_df_optimized(
    orders_list: List[Dict], app_mapping: Dict, tz: ZoneInfo
) -> pd.DataFrame:
    with timed("Transform orders → DataFrame (optimized)"):
        if not orders_list:
            return pd.DataFrame()

        order_data = []
        for order in orders_list:
            customer = order.get("customer") or {}
            shipping_address = order.get("shipping_address") or {}
            billing_address = order.get("billing_address") or {}

            disc_codes = order.get("discount_codes", []) or []
            discount_codes = (
                ", ".join([c.get("code", "") for c in disc_codes if c]) or None
            )
            total_discount_amount = sum(
                float(c.get("amount", "0")) for c in disc_codes if c
            )

            discount_apps = order.get("discount_applications", []) or []
            discount_app_titles = (
                ", ".join([a.get("title", "") for a in discount_apps if a]) or None
            )
            discount_app_values = (
                ", ".join([str(a.get("value", "")) for a in discount_apps if a]) or None
            )
            discount_app_types = (
                ", ".join(
                    [
                        app_mapping.get(str(a.get("app_id", "")), "N/A")
                        for a in discount_apps
                        if a
                    ]
                )
                or None
            )
            discount_app_ids = (
                ", ".join([str(a.get("app_id", "")) for a in discount_apps if a])
                or None
            )

            order_app_id = order.get("app_id")
            order_app_name = (
                app_mapping.get(str(order_app_id), str(order_app_id))
                if order_app_id
                else None
            )

            payment_gateway_names = (
                ", ".join(order.get("payment_gateway_names", [])) or None
            )

            created_at_str = order.get("created_at", "")
            updated_at_str = order.get("updated_at", "")
            created_date, created_time = _parse_iso_to_store_date_time(created_at_str, tz)
            updated_date, updated_time = _parse_iso_to_store_date_time(updated_at_str, tz)

            # --- UTM Parsing (Updated) ---
            # 1. Try landing_site first (most reliable for Shopify)
            # 2. Fallback to full_url (rarely populated but historically used)
            # 3. Fallback to note_attributes (if any)

            landing_site = order.get("landing_site")
            full_url = order.get("full_url")

            utm_data = {
                "utm_source": None,
                "utm_medium": None,
                "utm_campaign": None,
                "utm_content": None,
                "utm_term": None,
            }

            # Helper to parse from a url string
            def parse_utms_from_url(url_str):
                try:
                    if not url_str:
                        return
                    # Handle "landing_site": "/?utm_source=..." case
                    if url_str.startswith("/"):
                        url_str = "http://dummy.com" + url_str

                    parsed = urlparse(url_str)
                    qs = parse_qs(parsed.query)
                    for k in utm_data.keys():
                        if not utm_data[k]:  # only fill if empty
                            val_list = qs.get(k)
                            if val_list:
                                utm_data[k] = val_list[0]
                except Exception:
                    pass

            # 1. Landing Site
            parse_utms_from_url(landing_site)

            # 2. Full URL (fallback)
            if not any(utm_data.values()):
                parse_utms_from_url(full_url)

            # 3. Note attributes (fallback)
            if not any(utm_data.values()):
                for note in order.get("note_attributes", []):
                    if not note:
                        continue
                    name = (note.get("name") or "").lower()
                    val = note.get("value")
                    if name in utm_data and not utm_data[name]:
                        utm_data[name] = val

            base = {
                "created_at": format_datetime(created_at_str),
                "created_date": created_date,
                "created_time": created_time,
                "order_id": str(order.get("id")) if order.get("id") else None,
                "order_name": order.get("name"),
                "customer_id": str(customer.get("id")) if customer.get("id") else None,
                "customer_email": customer.get("email"),
                "customer_first_name": customer.get("first_name"),
                "customer_last_name": customer.get("last_name"),
                "customer_phone": customer.get("phone"),
                "financial_status": order.get("financial_status"),
                "fulfillment_status": order.get("fulfillment_status") or "Unfulfilled",
                "currency": order.get("currency"),
                "discount_codes": discount_codes,
                "discount_amount": total_discount_amount
                if total_discount_amount > 0
                else None,
                "discount_application_titles": discount_app_titles,
                "discount_application_values": discount_app_values,
                "discount_application_types": discount_app_types,
                "discount_application_ids": discount_app_ids,
                "order_app_id": str(order_app_id) if order_app_id else None,
                "order_app_name": order_app_name,
                "total_price": float(order.get("total_price", 0))
                if order.get("total_price")
                else None,
                "shipping_price": float(
                    order.get("total_shipping_price_set", {})
                    .get("shop_money", {})
                    .get("amount", "0")
                )
                or None,
                "total_tax": float(order.get("current_total_tax", 0))
                if order.get("current_total_tax")
                else None,
                "payment_gateway_names": payment_gateway_names,
                "total_discounts": float(order.get("total_discounts", 0))
                if order.get("total_discounts")
                else None,
                "total_duties": float(order.get("total_duties", 0))
                if order.get("total_duties")
                else None,
                "sku": None,
                "variant_title": None,
                "line_item": None,
                "line_item_price": None,
                "line_item_quantity": None,
                "line_item_total_discount": None,
                "product_id": None,
                "variant_id": None,
                "line_item_properties_json": None,
                "tags": order.get("tags") or None,
                "updated_at": format_datetime(updated_at_str),
                "updated_date": updated_date,
                "updated_time": updated_time,
                "orig_referrer": order.get("orig_referrer"),
                "full_url": order.get("full_url"),
                "customer_ip": order.get("customer_ip"),
                "pg_order_id": order.get("pg_order_id"),
                "shipping_address": shipping_address.get("address1"),
                "shipping_phone": shipping_address.get("phone"),
                "shipping_city": shipping_address.get("city"),
                "shipping_zip": shipping_address.get("zip"),
                "shipping_province": shipping_address.get("province"),
                "billing_address": billing_address.get("address1"),
                "billing_phone": billing_address.get("phone"),
                "billing_city": billing_address.get("city"),
                "billing_zip": billing_address.get("zip"),
                "billing_province": billing_address.get("province"),
                "customer_tag": customer.get("tags"),
                "appmaker_platform": order.get("appmaker_platform"),
                "app_version": order.get("app_version"),
                # Add user_agent to base dictionary, defaulting to "unknown"
                "user_agent": "unknown",
            }

            # Extract user_agent from note_attributes if present
            found_ua = False
            for note in order.get("note_attributes", []) or []:
                if note and note.get("name") == "user_agent":
                    base["user_agent"] = note.get("value") or "unknown"
                    found_ua = True
                    break

            if found_ua:
                logger.debug(
                    f"found user_agent for order {base['order_name']}: {base['user_agent']}"
                )
            else:
                logger.debug(
                    f"no user_agent found for order {base['order_name']}, setting to unknown"
                )

            line_items = order.get("line_items", [])
            if not line_items:
                order_data.append(base)
                continue

            for i, item in enumerate(line_items):
                if item is None:
                    continue

                row = base.copy() if i == 0 else {k: None for k in base.keys()}

                allocs = item.get("discount_allocations") or []
                discount_amount_per_line_item = 0.0
                for a in allocs:
                    try:
                        discount_amount_per_line_item += float(a.get("amount") or 0.0)
                    except Exception:
                        pass

                # if you prefer NULL instead of 0 when no allocations:
                discount_amount_per_line_item = (
                    discount_amount_per_line_item
                    if discount_amount_per_line_item > 0
                    else None
                )
                if i > 0:
                    row.update(
                        {
                            "created_date": created_date,
                            "created_time": created_time,
                            "order_name": order.get("name"),
                            "order_id": str(order.get("id"))
                            if order.get("id")
                            else None,
                            "customer_id": str(customer.get("id"))
                            if customer.get("id")
                            else None,
                            "tags": order.get("tags") or None,
                            "customer_tag": customer.get("tags"),
                            "appmaker_platform": order.get("appmaker_platform"),
                            "app_version": order.get("app_version"),
                            "payment_gateway_names": payment_gateway_names,
                            "discount_amount_per_line_item": discount_amount_per_line_item,
                            "full_url": order.get("full_url"),
                            # Add UTMs for subsequent lines explicitly (though they are in base, good to be safe if base is not copied fully for props)
                            "utm_source": utm_data["utm_source"],
                            "utm_medium": utm_data["utm_medium"],
                            "utm_campaign": utm_data["utm_campaign"],
                            "utm_content": utm_data["utm_content"],
                            "utm_term": utm_data["utm_term"],
                            "user_agent": base.get("user_agent"),
                        }
                    )

                row.update(
                    {
                        "sku": item.get("sku"),
                        "variant_title": item.get("variant_title"),
                        "line_item": item.get("title"),
                        "order_id": str(order.get("id")) if order.get("id") else None,
                        "full_url": order.get("full_url"),
                        "utm_source": utm_data["utm_source"],
                        "utm_medium": utm_data["utm_medium"],
                        "utm_campaign": utm_data["utm_campaign"],
                        "utm_content": utm_data["utm_content"],
                        "utm_term": utm_data["utm_term"],
                        "line_item_price": float(item.get("price", 0))
                        if item.get("price")
                        else None,
                        "line_item_quantity": int(item.get("quantity", 0))
                        if item.get("quantity")
                        else None,
                        "line_item_total_discount": float(item.get("total_discount", 0))
                        if item.get("total_discount")
                        else None,
                        "discount_amount_per_line_item": discount_amount_per_line_item,
                        "product_id": str(item.get("product_id"))
                        if item.get("product_id")
                        else None,
                        "variant_id": str(item.get("variant_id"))
                        if item.get("variant_id")
                        else None,
                        "line_item_properties_json": None,
                        "user_agent": base.get("user_agent"),
                    }
                )

                props = item.get("properties", []) or []
                row["line_item_properties_json"] = (
                    json.dumps(props, ensure_ascii=False) if props else None
                )
                row.update(aws_background.extract_bundle_item_columns_from_props(props))

                for note in order.get("note_attributes", []) or []:
                    if note and note.get("name") in row:
                        row[note["name"]] = note.get("value")

                order_data.append(row)

        df = pd.DataFrame(order_data)
        df = df.replace({np.nan: None, "N/A": None, "": None})
        return df


# ---------------------------
# Returns fact helpers
# ---------------------------
RETURN_EVENT_CANCEL = "CANCEL"
RETURN_EVENT_CANCEL_RTO = "CANCEL (RTO)"
RETURN_EVENT_REFUND = "REFUND"


def _ensure_returns_fact(cursor, connection):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS returns_fact (
          order_id     BIGINT        NOT NULL,
          event_date   DATE          NOT NULL,
          order_created_date DATE    NULL,
          event_type   ENUM('CANCEL','CANCEL (RTO)','REFUND') NOT NULL,
          amount       DECIMAL(12,2) NOT NULL,
          PRIMARY KEY (order_id, event_type, event_date),
          KEY idx_event_date (event_date),
          KEY idx_order_created_date_event_type (order_created_date, event_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)
    try:
        cursor.execute("SHOW COLUMNS FROM returns_fact LIKE 'event_type'")
        row = cursor.fetchone()
        if row:
            type_value = row.get("Type") if isinstance(row, dict) else row[1]
            if "CANCEL (RTO)" not in str(type_value):
                cursor.execute("""
                    ALTER TABLE returns_fact
                    MODIFY COLUMN event_type ENUM('CANCEL','CANCEL (RTO)','REFUND') NOT NULL
                """)
        cursor.execute("SHOW COLUMNS FROM returns_fact LIKE 'order_created_date'")
        created_date_row = cursor.fetchone()
        if not created_date_row:
            cursor.execute("""
                ALTER TABLE returns_fact
                ADD COLUMN order_created_date DATE NULL AFTER event_date
            """)
        cursor.execute("""
            SELECT 1
            FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = 'returns_fact'
              AND index_name = 'idx_order_created_date_event_type'
            LIMIT 1
        """)
        index_row = cursor.fetchone()
        if not index_row:
            cursor.execute("""
                ALTER TABLE returns_fact
                ADD INDEX idx_order_created_date_event_type (order_created_date, event_type)
            """)
    except Exception as e:
        logger.warning(f"⚠️ returns_fact event_type migration check failed: {e}")
    connection.commit()


def _parse_iso_to_store_date(iso_ts: Optional[str], tz: ZoneInfo) -> Optional[str]:
    if not iso_ts:
        return None
    try:
        dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        return dt.astimezone(tz).date().isoformat()
    except Exception:
        return None


def upsert_returns_fact_from_orders(
    brand_index: int, orders_list: List[Dict], cursor=None, connection=None
) -> Set[str]:
    """Idempotently upsert CANCEL and REFUND events per order per day.

    Returns:
      Set of IST event_date strings that were actually inserted/changed.
    """
    if not orders_list:
        return set()

    if cursor is None or connection is None:
        with get_db_cursor(brand_index) as (cursor, connection):
            return upsert_returns_fact_from_orders(
                brand_index, orders_list, cursor=cursor, connection=connection
            )

    try:
        _ensure_returns_fact(cursor, connection)
        tz = aws_background.get_brand_timezone(brand_index)

        agg: Dict[Tuple[str, str, str], Tuple[float, Optional[str]]] = {}

        for o in orders_list:
            oid = str(o.get("id")) if o.get("id") else None
            if not oid:
                continue

            cancelled_at = o.get("cancelled_at")
            if cancelled_at:
                d = _parse_iso_to_store_date(cancelled_at, tz)
                created_date = _parse_iso_to_store_date(o.get("created_at"), tz)
                if d:
                    amt = float(o.get("total_price") or 0.0)
                    cancel_event_type = (
                        RETURN_EVENT_CANCEL_RTO
                        if _is_order_rto_cancel(
                            brand_index=brand_index,
                            created_at=o.get("created_at"),
                            cancelled_at=cancelled_at,
                            tags=o.get("tags"),
                            tz=tz,
                        )
                        else RETURN_EVENT_CANCEL
                    )
                    key = (oid, cancel_event_type, d)
                    old_amount, _ = agg.get(key, (0.0, created_date))
                    agg[key] = (max(old_amount, amt), created_date)

            refunds = o.get("refunds") or []
            for rf in refunds:
                rdate = _parse_iso_to_store_date(rf.get("created_at"), tz)
                if not rdate:
                    continue
                txns = rf.get("transactions") or []
                refund_sum = 0.0
                for t in txns:
                    kind = (t.get("kind") or "").lower()
                    if kind in ("refund", "chargeback", "return"):
                        try:
                            amt = float(t.get("amount") or 0.0)
                            refund_sum += abs(amt)
                        except Exception:
                            continue
                if refund_sum > 0:
                    key = (oid, RETURN_EVENT_REFUND, rdate)
                    old_amount, _ = agg.get(key, (0.0, None))
                    agg[key] = (old_amount + refund_sum, None)

        if not agg:
            return set()

        # Compare against existing values first so callers can use only true deltas
        # for affected range expansion.
        changed_dates: Set[str] = set()
        existing_values: Dict[Tuple[str, str, str], Tuple[float, Optional[str]]] = {}
        order_ids = sorted({int(k[0]) for k in agg.keys()})

        def _chunked(values: List[int], size: int = 500):
            for i in range(0, len(values), size):
                yield values[i : i + size]

        for chunk in _chunked(order_ids):
            placeholders = ",".join(["%s"] * len(chunk))
            cursor.execute(
                f"""
                SELECT order_id, event_type, event_date, amount, order_created_date
                FROM returns_fact
                WHERE order_id IN ({placeholders})
                """,
                tuple(chunk),
            )
            for row in cursor.fetchall():
                if isinstance(row, dict):
                    r_order_id = str(row.get("order_id"))
                    r_event_type = row.get("event_type")
                    r_event_date = row.get("event_date")
                    r_amount = float(row.get("amount") or 0.0)
                    r_order_created_date = row.get("order_created_date")
                else:
                    r_order_id = str(row[0])
                    r_event_type = row[1]
                    r_event_date = row[2]
                    r_amount = float(row[3] or 0.0)
                    r_order_created_date = row[4]

                r_event_date_str = (
                    r_event_date.isoformat()
                    if hasattr(r_event_date, "isoformat")
                    else str(r_event_date)
                )
                r_order_created_date_str = (
                    r_order_created_date.isoformat()
                    if hasattr(r_order_created_date, "isoformat")
                    else (str(r_order_created_date) if r_order_created_date else None)
                )
                existing_values[(r_order_id, r_event_type, r_event_date_str)] = (
                    r_amount,
                    r_order_created_date_str,
                )

        def _summary_attribution_date(
            event_type: str,
            event_date: str,
            order_created_date: Optional[str],
        ) -> Optional[str]:
            if event_type in (RETURN_EVENT_CANCEL, RETURN_EVENT_CANCEL_RTO):
                return order_created_date or event_date
            return event_date

        for key, (new_amount, new_order_created_date) in agg.items():
            old_value = existing_values.get(key)
            old_amount = old_value[0] if old_value else None
            old_order_created_date = old_value[1] if old_value else None
            if (
                old_value is None
                or abs(float(old_amount) - float(new_amount)) > 0.009
                or old_order_created_date != new_order_created_date
            ):
                attribution_date = _summary_attribution_date(
                    key[1], key[2], new_order_created_date
                )
                if attribution_date:
                    changed_dates.add(attribution_date)

        stale_keys = [key for key in existing_values.keys() if key not in agg]
        for key in stale_keys:
            _, old_order_created_date = existing_values[key]
            attribution_date = _summary_attribution_date(
                key[1], key[2], old_order_created_date
            )
            if attribution_date:
                changed_dates.add(attribution_date)

        if stale_keys:
            delete_sql = """
                DELETE FROM returns_fact
                WHERE order_id = %s AND event_type = %s AND event_date = %s
            """
            delete_rows = [(int(k[0]), k[1], k[2]) for k in stale_keys]
            executemany_chunked(cursor, connection, delete_sql, delete_rows)

        rows = [
            (int(k[0]), k[2], created_date, k[1], round(value[0], 2))
            for k, value in agg.items()
            for created_date in [value[1]]
        ]
        insert_sql = """
            INSERT INTO returns_fact (order_id, event_date, order_created_date, event_type, amount)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                order_created_date = VALUES(order_created_date),
                amount = VALUES(amount);
        """
        executemany_chunked(cursor, connection, insert_sql, rows)

        logger.info(
            f"✅ Upserted {len(rows)} rows into returns_fact ({len(changed_dates)} changed event dates)"
        )
        return changed_dates

    finally:
        pass


# ---------------------------
# Discount dashboard fact helpers
# ---------------------------
def _classify_payment_mode(payment_gateway_names: Optional[str]) -> str:
    pg = payment_gateway_names or ""
    if "Gokwik PPCOD" in pg:
        return "partially_paid"
    if (
        not pg
        or "Cash on Delivery (COD)" in pg
        or "cash_on_delivery" in pg
    ):
        return "cod"
    return "prepaid"


def _normalize_city_name(raw_city: Optional[str]) -> str:
    city = (raw_city or "").strip().lower()
    return city or "Unknown"


def _normalized_city_sql_expr(column_name: str) -> str:
    return (
        f"CASE "
        f"WHEN {column_name} IS NULL OR TRIM({column_name}) = '' THEN 'Unknown' "
        f"ELSE LOWER(TRIM({column_name})) "
        f"END"
    )


def _date_range_iso_set(start_date: date, end_date: date) -> Set[str]:
    current = start_date
    values: Set[str] = set()
    while current <= end_date:
        values.add(current.isoformat())
        current += timedelta(days=1)
    return values


def _parse_iso_to_store_date_hour(
    iso_ts: Optional[str],
    tz: ZoneInfo,
) -> Tuple[Optional[str], Optional[int]]:
    if not iso_ts:
        return None, None
    try:
        dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00")).astimezone(tz)
        return dt.date().isoformat(), dt.hour
    except Exception:
        return None, None


def _normalize_discount_code_rows_from_orders(
    orders_list: List[Dict],
    tz: ZoneInfo,
) -> List[Tuple[Any, ...]]:
    rows: List[Tuple[Any, ...]] = []
    seen: Set[Tuple[str, str]] = set()

    for order in orders_list or []:
        order_id = str(order.get("id")) if order.get("id") else None
        if not order_id:
            continue

        created_date, created_hour = _parse_iso_to_store_date_hour(order.get("created_at"), tz)
        if not created_date or created_hour is None:
            continue

        discount_codes = order.get("discount_codes") or []
        if not discount_codes:
            continue

        payment_gateway_names = (
            ", ".join(order.get("payment_gateway_names") or []) or None
        )
        payment_mode = _classify_payment_mode(payment_gateway_names)

        try:
            total_price = float(order.get("total_price") or 0)
        except Exception:
            total_price = 0.0

        for discount in discount_codes:
            if not discount:
                continue
            code = (discount.get("code") or "").strip()
            if not code:
                continue

            key = (order_id, code)
            if key in seen:
                continue
            seen.add(key)

            try:
                discount_amount = float(discount.get("amount") or 0)
            except Exception:
                discount_amount = 0.0

            rows.append(
                (
                    order_id,
                    order.get("name"),
                    format_datetime(order.get("created_at")),
                    created_date,
                    created_hour,
                    code,
                    discount_amount,
                    payment_gateway_names,
                    payment_mode,
                    total_price,
                )
            )

    return rows


def upsert_dashboard_order_discount_fact_from_orders(
    orders_list: List[Dict], cursor, connection, brand_name: str, tz: ZoneInfo
) -> Set[str]:
    rows = _normalize_discount_code_rows_from_orders(orders_list, tz)
    if not rows:
        logger.info(f"✔️ [{brand_name}] No discount-code orders to upsert")
        return set()

    sql = """
        INSERT INTO dashboard_order_discount_fact (
            order_id, order_name, created_at, created_date, created_hour,
            discount_code, discount_amount, payment_gateway_names, payment_mode,
            total_price
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            order_name = VALUES(order_name),
            created_at = VALUES(created_at),
            created_date = VALUES(created_date),
            created_hour = VALUES(created_hour),
            discount_amount = VALUES(discount_amount),
            payment_gateway_names = VALUES(payment_gateway_names),
            payment_mode = VALUES(payment_mode),
            total_price = VALUES(total_price)
    """
    cursor.executemany(sql, rows)
    connection.commit()

    affected_dates = {str(row[3]) for row in rows if row[3]}
    logger.info(
        f"✅ [{brand_name}] Upserted {len(rows)} discount fact rows "
        f"for {len(affected_dates)} date(s)"
    )
    return affected_dates


def backfill_dashboard_order_discount_fact_from_shopify_orders(
    cursor, connection, brand_name: str, batch_size: int = 5000
) -> Set[str]:
    where_clauses = ["discount_codes IS NOT NULL", "discount_codes != ''"]
    params: List[Any] = []

    if aws_background.DISCOUNT_BACKFILL_START_DATE_PARSED:
        where_clauses.append("created_date >= %s")
        params.append(aws_background.DISCOUNT_BACKFILL_START_DATE_PARSED)
    if aws_background.DISCOUNT_BACKFILL_END_DATE_PARSED:
        where_clauses.append("created_date <= %s")
        params.append(aws_background.DISCOUNT_BACKFILL_END_DATE_PARSED)

    sql = f"""
        SELECT
            order_id,
            order_name,
            created_at,
            created_date,
            created_time,
            discount_codes,
            discount_amount,
            payment_gateway_names,
            total_price
        FROM shopify_orders
        WHERE {' AND '.join(where_clauses)}
        ORDER BY created_date, order_id
        LIMIT %s OFFSET %s
    """

    affected_dates: Set[str] = set()
    total_rows = 0
    offset = 0

    while True:
        cursor.execute(sql, tuple(params + [batch_size, offset]))
        source_rows = cursor.fetchall()
        if not source_rows:
            break
        offset += len(source_rows)

        upsert_rows: List[Tuple[Any, ...]] = []
        seen: Set[Tuple[str, str]] = set()
        for row in source_rows:
            order_id = str(row.get("order_id") or "")
            if not order_id:
                continue

            created_date = row.get("created_date")
            if created_date is not None:
                created_date = str(created_date)

            created_time = str(row.get("created_time") or "")
            try:
                created_hour = int(created_time.split(":")[0])
            except Exception:
                created_hour = None

            if not created_date or created_hour is None:
                continue

            discount_codes = [
                c.strip()
                for c in str(row.get("discount_codes") or "").split(",")
                if c and c.strip()
            ]
            if not discount_codes:
                continue

            payment_gateway_names = row.get("payment_gateway_names")
            payment_mode = _classify_payment_mode(payment_gateway_names)

            try:
                total_price = float(row.get("total_price") or 0)
            except Exception:
                total_price = 0.0
            # Historical shopify_orders rows only have order-level discount_amount,
            # not per-code amounts after splitting comma-separated codes.
            discount_amount = 0.0

            for code in discount_codes:
                key = (order_id, code)
                if key in seen:
                    continue
                seen.add(key)
                upsert_rows.append(
                    (
                        order_id,
                        row.get("order_name"),
                        row.get("created_at"),
                        created_date,
                        created_hour,
                        code,
                        discount_amount,
                        payment_gateway_names,
                        payment_mode,
                        total_price,
                    )
                )

        if not upsert_rows:
            continue

        cursor.executemany(
            """
            INSERT INTO dashboard_order_discount_fact (
                order_id, order_name, created_at, created_date, created_hour,
                discount_code, discount_amount, payment_gateway_names, payment_mode,
                total_price
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                order_name = VALUES(order_name),
                created_at = VALUES(created_at),
                created_date = VALUES(created_date),
                created_hour = VALUES(created_hour),
                discount_amount = VALUES(discount_amount),
                payment_gateway_names = VALUES(payment_gateway_names),
                payment_mode = VALUES(payment_mode),
                total_price = VALUES(total_price)
            """,
            upsert_rows,
        )
        connection.commit()

        total_rows += len(upsert_rows)
        affected_dates.update(str(row[3]) for row in upsert_rows if row[3])

    logger.info(
        f"✅ [{brand_name}] Discount dashboard backfill upserted {total_rows} fact rows "
        f"for {len(affected_dates)} date(s)"
    )
    return affected_dates


def _ensure_products_sold_units_daily(cursor, connection):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS products_sold_units_daily (
            product_id BIGINT NOT NULL,
            variant_id BIGINT NOT NULL,
            sold_units_7d INT NOT NULL DEFAULT 0,
            sold_units_30d INT NOT NULL DEFAULT 0,
            sold_units_90d INT NOT NULL DEFAULT 0,
            last_processed_at DATETIME NOT NULL,
            PRIMARY KEY (product_id, variant_id),
            KEY idx_product_id (product_id),
            KEY idx_last_processed_at (last_processed_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    connection.commit()


def _collect_product_variant_window_units(
    orders_list: List[Dict],
    tz: ZoneInfo,
) -> Tuple[Dict[Tuple[int, int], Dict[str, int]], Dict[int, Dict[str, int]]]:
    """
    Build sold-unit deltas from fetched orders (today-inclusive windows).

    Returns:
      pair_window_qty: {(product_id, variant_id): {"7d": x, "30d": y, "90d": z}}
      product_window_qty: {product_id: {"7d": x, "30d": y, "90d": z}}
    """
    pair_window_qty: Dict[Tuple[int, int], Dict[str, int]] = {}
    product_window_qty: Dict[int, Dict[str, int]] = {}

    today_ist = datetime.now(tz).date()

    for order in orders_list:
        created_ist = _parse_iso_to_store_date(order.get("created_at"), tz)
        if not created_ist:
            continue

        try:
            created_date = datetime.strptime(created_ist, "%Y-%m-%d").date()
        except Exception:
            continue

        age_days = (today_ist - created_date).days
        if age_days < 0:
            continue

        in_7d = age_days <= 6
        in_30d = age_days <= 29
        in_90d = age_days <= 89

        if not (in_7d or in_30d or in_90d):
            continue

        for item in order.get("line_items") or []:
            if not item:
                continue

            try:
                pid = int(item.get("product_id") or 0)
                vid = int(item.get("variant_id") or 0)
                qty = int(item.get("quantity") or 0)
            except Exception:
                continue

            if pid <= 0 or vid <= 0 or qty <= 0:
                continue

            pair_key = (pid, vid)
            if pair_key not in pair_window_qty:
                pair_window_qty[pair_key] = {"7d": 0, "30d": 0, "90d": 0}
            if pid not in product_window_qty:
                product_window_qty[pid] = {"7d": 0, "30d": 0, "90d": 0}

            if in_7d:
                pair_window_qty[pair_key]["7d"] += qty
                product_window_qty[pid]["7d"] += qty
            if in_30d:
                pair_window_qty[pair_key]["30d"] += qty
                product_window_qty[pid]["30d"] += qty
            if in_90d:
                pair_window_qty[pair_key]["90d"] += qty
                product_window_qty[pid]["90d"] += qty

    return pair_window_qty, product_window_qty


def _extract_inventory_units_sold(table_data: Dict[str, Any]) -> int:
    rows = _format_shopifyql_table(table_data)
    if not rows:
        return 0

    total = 0
    found_named_col = False
    for row in rows:
        if "inventory_units_sold" in row:
            try:
                total += int(float(row.get("inventory_units_sold") or 0))
                found_named_col = True
            except Exception:
                pass

    if found_named_col:
        return max(0, total)

    # Fallback: pick first numeric cell from each row if schema naming differs.
    for row in rows:
        for value in row.values():
            try:
                total += int(float(value))
                break
            except Exception:
                continue

    return max(0, total)


def _fetch_product_sold_units_from_shopifyql(
    shop_name: str,
    access_token: str,
    product_id: int,
    days: int,
    session: requests.Session,
) -> int:
    shopify_ql = (
        "FROM inventory "
        "SHOW inventory_units_sold "
        f"WHERE product_id = {product_id} "
        "WITH PERCENT_CHANGE "
        f"SINCE startOfDay(-{days}d) UNTIL today "
        "COMPARE TO previous_period "
        "ORDER BY inventory_units_sold DESC "
        "LIMIT 1000 "
        "VISUALIZE inventory_units_sold"
    )

    ql_flat = shopify_ql.replace("\n", " ").strip().replace('"', '\\"')
    graphql_query = f'''query InventoryUnitsSoldPercentChangeQuery {{ shopifyqlQuery(query: "{ql_flat}") {{ tableData {{ rows columns {{ name dataType displayName }} }} parseErrors }} }}'''

    payload = {"query": graphql_query}
    shopifyql_api_version = "2025-10"
    url = f"https://{shop_name}.myshopify.com/admin/api/{shopifyql_api_version}/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token,
    }

    last_exc: Optional[Exception] = None
    for attempt in range(1, 4):
        try:
            resp = session.post(url, headers=headers, data=json.dumps(payload), timeout=60)
            if resp.status_code != 200:
                logger.warning(
                    f"⚠️ Product sold-units ShopifyQL HTTP {resp.status_code} for product {product_id}, {days}d"
                )
                continue

            body = resp.json()
            if body.get("errors"):
                logger.warning(
                    f"⚠️ Product sold-units GraphQL errors for product {product_id}, {days}d: {body.get('errors')}"
                )
                continue

            ql_result = (body.get("data") or {}).get("shopifyqlQuery") or {}
            parse_errors = ql_result.get("parseErrors") or []
            if parse_errors:
                logger.warning(
                    f"⚠️ Product sold-units parse errors for product {product_id}, {days}d: {parse_errors}"
                )
                continue

            table_data = ql_result.get("tableData")
            if not table_data:
                return 0

            return _extract_inventory_units_sold(table_data)
        except Exception as e:
            last_exc = e
            logger.warning(
                f"⚠️ Product sold-units fetch failed for product {product_id}, {days}d, attempt {attempt}/3: {e}"
            )
            time.sleep(2**attempt)

    if last_exc:
        logger.error(
            f"❌ Product sold-units fetch failed after retries for product {product_id}, {days}d: {last_exc}"
        )
    return 0


def upsert_products_sold_units_daily_from_orders(
    brand_index: int,
    orders_list: List[Dict],
    brand_name: str,
    shop_name: str,
    access_token: str,
    cursor=None,
    connection=None,
):
    """
    Maintain products_sold_units_daily from fetched orders.

    Rules:
    - First time product_id: fetch 7d/30d/90d sold units via ShopifyQL and upsert.
    - Existing product_id: update sold units using new-order deltas from this run.
    """
    if not orders_list:
        return

    if cursor is None or connection is None:
        with get_db_cursor(brand_index) as (cursor, connection):
            return upsert_products_sold_units_daily_from_orders(
                brand_index,
                orders_list,
                brand_name,
                shop_name,
                access_token,
                cursor=cursor,
                connection=connection,
            )

    try:
        _ensure_products_sold_units_daily(cursor, connection)
        tz = aws_background.get_brand_timezone(brand_index)

        pair_window_qty, product_window_qty = _collect_product_variant_window_units(
            orders_list, tz
        )
        if not pair_window_qty:
            logger.info(
                f"[{brand_name}] No valid product/variant quantities found for products_sold_units_daily update"
            )
            return

        product_ids = sorted(product_window_qty.keys())
        placeholders = ",".join(["%s"] * len(product_ids))

        existing_product_ids: Set[int] = set()
        cursor.execute(
            f"SELECT DISTINCT product_id FROM products_sold_units_daily WHERE product_id IN ({placeholders})",
            tuple(product_ids),
        )
        for row in cursor.fetchall():
            existing_product_ids.add(
                int(row["product_id"] if isinstance(row, dict) else row[0])
            )

        new_product_ids = [pid for pid in product_ids if pid not in existing_product_ids]

        baseline_by_product: Dict[int, Dict[str, int]] = {}
        if new_product_ids:
            session = _make_shopifyql_session()
            try:
                for pid in new_product_ids:
                    sold_7d = _fetch_product_sold_units_from_shopifyql(
                        shop_name, access_token, pid, 7, session
                    )
                    sold_30d = _fetch_product_sold_units_from_shopifyql(
                        shop_name, access_token, pid, 30, session
                    )
                    sold_90d = _fetch_product_sold_units_from_shopifyql(
                        shop_name, access_token, pid, 90, session
                    )
                    baseline_by_product[pid] = {
                        "7d": sold_7d,
                        "30d": sold_30d,
                        "90d": sold_90d,
                    }
            finally:
                try:
                    session.close()
                except Exception:
                    pass

        existing_pair_values: Dict[Tuple[int, int], Dict[str, int]] = {}
        if existing_product_ids:
            placeholders_existing = ",".join(["%s"] * len(existing_product_ids))
            cursor.execute(
                f"""
                SELECT product_id, variant_id, sold_units_7d, sold_units_30d, sold_units_90d
                FROM products_sold_units_daily
                WHERE product_id IN ({placeholders_existing})
                """,
                tuple(sorted(existing_product_ids)),
            )
            for row in cursor.fetchall():
                if isinstance(row, dict):
                    pid = int(row["product_id"])
                    vid = int(row["variant_id"])
                    s7 = int(row.get("sold_units_7d") or 0)
                    s30 = int(row.get("sold_units_30d") or 0)
                    s90 = int(row.get("sold_units_90d") or 0)
                else:
                    pid = int(row[0])
                    vid = int(row[1])
                    s7 = int(row[2] or 0)
                    s30 = int(row[3] or 0)
                    s90 = int(row[4] or 0)

                existing_pair_values[(pid, vid)] = {"7d": s7, "30d": s30, "90d": s90}

        now_dt = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
        upsert_rows = []

        for (pid, vid), delta in pair_window_qty.items():
            if pid in baseline_by_product:
                # First-time product: use ShopifyQL baseline windows.
                target_7d = baseline_by_product[pid]["7d"]
                target_30d = baseline_by_product[pid]["30d"]
                target_90d = baseline_by_product[pid]["90d"]
            else:
                # Existing product: increment from new-order deltas.
                old_vals = existing_pair_values.get((pid, vid), {"7d": 0, "30d": 0, "90d": 0})
                target_7d = int(old_vals["7d"]) + int(delta["7d"])
                target_30d = int(old_vals["30d"]) + int(delta["30d"])
                target_90d = int(old_vals["90d"]) + int(delta["90d"])

            upsert_rows.append((pid, vid, target_7d, target_30d, target_90d, now_dt))

        if not upsert_rows:
            logger.info(f"[{brand_name}] No rows to upsert in products_sold_units_daily")
            return

        executemany_chunked(
            cursor,
            connection,
            """
            INSERT INTO products_sold_units_daily (
                product_id,
                variant_id,
                sold_units_7d,
                sold_units_30d,
                sold_units_90d,
                last_processed_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                sold_units_7d = VALUES(sold_units_7d),
                sold_units_30d = VALUES(sold_units_30d),
                sold_units_90d = VALUES(sold_units_90d),
                last_processed_at = VALUES(last_processed_at)
            """,
            upsert_rows,
        )

        logger.info(
            f"✅ [{brand_name}] Upserted {len(upsert_rows)} rows into products_sold_units_daily "
            f"(new products fetched via ShopifyQL: {len(new_product_ids)})"
        )

    except Exception as e:
        logger.error(
            f"❌ [{brand_name}] Error updating products_sold_units_daily: {e}"
        )
        traceback.print_exc()


# ---------------------------
# Affected date tracking
# ---------------------------
def _dates_in_clause(dates: Iterable[str]) -> Tuple[str, Tuple[str, ...]]:
    """Build the body of a SQL IN-clause for a set of dates.

    Returns (placeholders_string, params_tuple). Callers splice the
    placeholders into an f-string SQL fragment and pass the params tuple
    to cursor.execute (repeated per BETWEEN-replaced clause).
    """
    ordered = sorted({d for d in dates if d})
    placeholders = ", ".join(["%s"] * len(ordered))
    return placeholders, tuple(ordered)


def _normalize_rto_key(value: Optional[str]) -> str:
    return str(value or "").strip().lower()


def _normalize_rto_tag_value(value: Any) -> Optional[str]:
    if value is None:
        return None

    if isinstance(value, (list, tuple, set)):
        for item in value:
            normalized = _normalize_rto_tag_value(item)
            if normalized:
                return normalized
        return None

    tag = str(value).strip()
    return tag or None


def _build_rto_db_database_map() -> Dict[str, str]:
    resolved: Dict[str, str] = {}
    candidate_indices = sorted(set(active_brand_indices) | set(brand_db_configs.keys()))
    for brand_index in candidate_indices:
        db_name = os.environ.get(f"DB_DATABASE_{brand_index}") or (
            brand_db_configs.get(brand_index) or {}
        ).get("database")
        if not db_name:
            continue

        mapped_tag = brand_rto_tags_by_index.get(brand_index)
        if mapped_tag:
            resolved[_normalize_rto_key(db_name)] = mapped_tag

    return resolved


def get_rto_tag_for_brand(brand_index: int, brand_name: Optional[str] = None) -> Optional[str]:
    db_name = os.environ.get(f"DB_DATABASE_{brand_index}") or (
        brand_db_configs.get(brand_index) or {}
    ).get("database")
    if db_name:
        mapped_tag = _build_rto_db_database_map().get(_normalize_rto_key(db_name))
        if mapped_tag:
            return mapped_tag

    return brand_rto_tags_by_index.get(brand_index)


def _build_mysql_regex_for_exact_tag(tag_value: str) -> str:
    escaped_tag = re.escape(tag_value.strip().lower()).replace(r"\ ", " ")
    return rf"(^|,)[[:space:]]*{escaped_tag}[[:space:]]*(,|$)"


def _tags_csv_contains_exact_tag(raw_tags: Optional[str], tag_value: Optional[str]) -> bool:
    if not raw_tags or not tag_value:
        return False

    target = tag_value.strip().lower()
    if not target:
        return False

    return any(tag.strip().lower() == target for tag in str(raw_tags).split(","))


def _is_order_rto_cancel(
    brand_index: int,
    created_at: Optional[str],
    cancelled_at: Optional[str],
    tags: Optional[str],
    tz: ZoneInfo,
    brand_name: Optional[str] = None,
) -> bool:
    if not cancelled_at:
        return False

    rto_tag = get_rto_tag_for_brand(brand_index, brand_name)
    if rto_tag:
        return _tags_csv_contains_exact_tag(tags, rto_tag)

    created_date = _parse_iso_to_store_date(created_at, tz)
    cancelled_date = _parse_iso_to_store_date(cancelled_at, tz)
    if not created_date or not cancelled_date:
        return False

    try:
        created_day = date.fromisoformat(created_date)
        cancelled_day = date.fromisoformat(cancelled_date)
    except ValueError:
        return False

    return (cancelled_day - created_day).days > aws_background.RTO_ASSUMPTION_PERIOD


def get_rto_affected_dates_updates(orders_list: List[Dict], tz: ZoneInfo) -> Set[str]:
    """Return created_date values for cancelled orders that can affect RTO metrics."""
    dates: Set[str] = set()
    for order in orders_list or []:
        if order.get("cancelled_at") and order.get("created_at"):
            created_date = _parse_iso_to_store_date(order["created_at"], tz)
            if created_date:
                dates.add(created_date)
    return dates


def get_affected_dates_new_orders(orders_list: List[Dict], tz: ZoneInfo) -> Set[str]:
    """Return the set of store dates whose summaries are affected by NEW orders.
    Summaries for NEW orders depend only on created_date.
    """
    dates: Set[str] = set()
    for o in orders_list or []:
        if o.get("created_at"):
            d = _parse_iso_to_store_date(o["created_at"], tz)
            if d:
                dates.add(d)
    return dates


def get_affected_dates_updates(
    orders_list: List[Dict],
    tz: ZoneInfo,
    extra_event_dates: Optional[Set[str]] = None,
) -> Set[str]:
    """Return the set of store dates whose summaries are affected by UPDATED orders.

    Sources:
      - updated_at date for orders that can affect summaries
        (not-paid/pending, or cancelled, or has refunds).
      - extra_event_dates from returns_fact upsert deltas (only event_dates
        whose amount actually changed or were newly inserted).

    Passing a *set* (not a min/max range) downstream is what keeps the
    summary rebuild sparse: a 2016 refund correction only rebuilds the
    2016-04-08 summary row, not the entire 2016→today span.
    """
    dates: Set[str] = set(extra_event_dates or set())
    for o in orders_list or []:
        financial_status = (o.get("financial_status") or "").lower()
        refunds = o.get("refunds") or []
        cancelled_at = o.get("cancelled_at")

        affects_summaries = (
            financial_status not in ("paid", "pending")
            or cancelled_at is not None
            or len(refunds) > 0
        )
        if not affects_summaries:
            continue

        # updated_at is the updated_date driver for summary refreshes.
        if o.get("updated_at"):
            d = _parse_iso_to_store_date(o["updated_at"], tz)
            if d:
                dates.add(d)

    return dates


def load_data_to_sql_optimized(
    df: pd.DataFrame,
    brand_index: int,
    brand_name: str,
    table_name: str,
    batch_size: int = 1000,
):
    engine = sqlalchemy_engines.get(brand_index)
    if not engine:
        logger.error(f"No SQLAlchemy engine for brand {brand_index}")
        return

    metadata = MetaData()
    columns = [
        Column("created_at", DateTime),
        Column("created_date", String(10)),
        Column("created_time", String(8)),
        Column("order_id", String(50)),
        Column("order_name", String(50)),
        Column("customer_id", String(50)),
        Column("customer_email", String(100)),
        Column("customer_first_name", String(100)),
        Column("customer_last_name", String(100)),
        Column("customer_phone", String(30)),
        Column("financial_status", String(50)),
        Column("fulfillment_status", String(50)),
        Column("currency", String(10)),
        Column("discount_codes", Text),
        Column("discount_amount", Float),
        Column("discount_application_titles", Text),
        Column("discount_application_values", Text),
        Column("discount_application_types", Text),
        Column("discount_application_ids", Text),
        Column("order_app_id", String(50)),
        Column("order_app_name", String(100)),
        Column("total_price", Float),
        Column("shipping_price", Float),
        Column("total_tax", Float),
        Column("payment_gateway_names", Text),
        Column("total_discounts", Float),
        Column("total_duties", Float),
        Column("sku", String(100)),
        Column("variant_title", String(100)),
        Column("line_item", String(255)),
        Column("line_item_price", Float),
        Column("line_item_quantity", Integer),
        Column("line_item_total_discount", Float),
        Column("product_id", String(50)),
        Column("variant_id", String(50)),
        Column("line_item_properties_json", Text),
        Column("tags", Text),
        Column("updated_at", DateTime),
        Column("updated_date", String(10)),
        Column("updated_time", String(8)),
        Column("orig_referrer", Text),
        Column("full_url", Text),
        Column("customer_ip", String(50)),
        Column("pg_order_id", String(50)),
        Column("shipping_address", Text),
        Column("shipping_phone", String(30)),
        Column("shipping_city", String(100)),
        Column("shipping_zip", String(20)),
        Column("shipping_province", String(100)),
        Column("billing_address", Text),
        Column("billing_phone", String(30)),
        Column("billing_city", String(100)),
        Column("billing_zip", String(20)),
        Column("billing_province", String(100)),
        Column("customer_tag", Text),
        Column("appmaker_platform", String(50)),
        Column("app_version", String(50)),
        # UTM Columns
        Column("utm_source", Text),
        Column("utm_medium", Text),
        Column("utm_campaign", Text),
        Column("utm_content", Text),
        Column("utm_term", Text),
        Column("user_agent", Text),
    ]
    for n in range(1, 11):
        columns.append(Column(f"_ITEM{n}_name", String(255)))
        columns.append(Column(f"_ITEM{n}_value", String(255)))
    Table(table_name, metadata, *columns)

    try:
        metadata.create_all(engine, checkfirst=True)
    except SQLAlchemyError as e:
        logger.error(f"❌ Error creating table '{table_name}': {e}")
        return

    def _replace_backfill_window_if_needed() -> None:
        if not (
            aws_background.is_backfill_active_for(brand_index)
            and aws_background.BACKFILL_START_IST
            and aws_background.BACKFILL_END_IST
            and table_name in ("shopify_orders", "shopify_orders_update")
        ):
            return

        window_start_dt, window_end_dt = aws_background.get_backfill_window_for_brand(brand_index)
        if not window_start_dt or not window_end_dt:
            return
        window_start = window_start_dt.date().isoformat()
        window_end = window_end_dt.date().isoformat()
        date_column = "created_date" if table_name == "shopify_orders" else "updated_date"

        with engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    DELETE FROM {table_name}
                    WHERE {date_column} >= :window_start
                      AND {date_column} <= :window_end
                    """
                ),
                {"window_start": window_start, "window_end": window_end},
            )

        logger.info(
            f"🔁 [{brand_name}] Backfill replace-mode cleared {table_name} "
            f"for {date_column} window {window_start}..{window_end}"
        )

    try:
        _replace_backfill_window_if_needed()
    except Exception as e:
        logger.error(
            f"❌ Error clearing backfill window in '{table_name}' for {brand_name}: {e}"
        )
        traceback.print_exc()
        return

    if df.empty:
        logger.info(f"DataFrame empty for {table_name}; nothing to load.")
        return

    try:
        optimal_batch = max(500, min(batch_size, 2000))
        with timed(f"Insert {len(df)} rows into {table_name} (batch={optimal_batch})"):
            with engine.begin() as conn:
                df.to_sql(
                    name=table_name,
                    con=conn,  # ✅ use the held connection (NOT the engine)
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=optimal_batch,
                )

        logger.info(f"✅ [{brand_name}] Loaded {len(df)} rows to {table_name}")
    except Exception as e:
        logger.error(f"❌ Error loading data to '{table_name}': {e}")
        traceback.print_exc()
