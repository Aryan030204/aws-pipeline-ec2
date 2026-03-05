#!/usr/bin/env python3
import os, json, time, argparse
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import requests
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, Table, Column, MetaData, String, Integer, Float, DateTime, Text
from sqlalchemy.exc import SQLAlchemyError
import mysql.connector

# -----------------------------
# Helpers (same logic as pipeline)
# -----------------------------

def to_shopify_iso(dt):
    """UTC -> 'YYYY-MM-DDTHH:MM:SSZ' (Shopify wants Z)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def fetch_orders(api_base_url, access_token, start_z_quoted, end_z_quoted, date_filter_field):
    """
    Fetch orders between [start, end] using created_at_min/_max or updated_at_min/_max.
    start_z_quoted / end_z_quoted must already be URL-quoted UTC Z strings.
    """
    headers = {"X-Shopify-Access-Token": access_token}
    lst, url = [], f"{api_base_url}/orders.json?status=any&limit=250&{date_filter_field}={start_z_quoted}&{date_filter_field.replace('_min','_max')}={end_z_quoted}"

    while url:
        r = requests.get(url, headers=headers, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"Shopify {r.status_code}: {r.text}")
        data = r.json()
        orders = data.get("orders", [])
        if not orders:
            break
        lst.extend(orders)
        url = r.links.get("next", {}).get("url")
        time.sleep(0.5)  # be nice
    return lst

def transform_orders_to_df(orders, app_mapping):
    rows = []
    for order in orders:
        # customer
        cust = order.get('customer') or {}
        customer_first_name = cust.get('first_name')
        customer_last_name  = cust.get('last_name')
        customer_email      = cust.get('email')
        customer_phone      = cust.get('phone')
        customer_tags       = cust.get('tags')

        # discounts (order-level)
        discount_codes = ', '.join([c.get('code','') for c in order.get('discount_codes',[]) if c]) or None
        total_discount_amount = sum(float(c.get('amount','0') or 0) for c in order.get('discount_codes',[]) if c)

        apps = order.get('discount_applications',[]) or []
        discount_application_titles = ', '.join([a.get('title','') for a in apps if a]) or None
        discount_application_values = ', '.join([str(a.get('value','')) for a in apps if a]) or None
        discount_application_types  = ', '.join([app_mapping.get(str(a.get('app_id','')), 'N/A') for a in apps if a]) or None
        discount_application_ids    = ', '.join([str(a.get('app_id','')) for a in apps if a]) or None

        order_app_id   = order.get('app_id')
        order_app_name = app_mapping.get(str(order_app_id), str(order_app_id)) if order_app_id is not None else None

        # addresses
        ship = order.get('shipping_address') or {}
        bill = order.get('billing_address') or {}

        # times: keep as UTC naive string for DB (YYYY-mm-dd HH:MM:SS)
        def fmt_dt(s):
            if not s: return None
            return datetime.fromisoformat(s.replace('Z','+00:00')).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        created_at = fmt_dt(order.get('created_at'))
        updated_at = fmt_dt(order.get('updated_at'))

        # line items
        for i, item in enumerate(order.get('line_items', [])):
            if not item: 
                continue

            # _ITEMn
            props = item.get('properties') or []
            item_names  = {f'_ITEM{n}_name': None for n in range(1,11)}
            item_values = {f'_ITEM{n}_value': None for n in range(1,11)}
            for idx, p in enumerate(props):
                if isinstance(p, dict) and p.get('name','').startswith('_ITEM') and idx+1 <= 10:
                    parts = (p.get('value') or '').strip().split('SKU:')
                    item_names[f'_ITEM{idx+1}_name']  = (parts[0].strip() if len(parts)>0 and parts[0].strip() else None)
                    item_values[f'_ITEM{idx+1}_value']= (parts[1].strip() if len(parts)>1 and parts[1].strip() else None)

            total_discount_li = item.get('total_discount') or 0

            rows.append({
                "created_at": created_at,
                "created_date": created_at[:10] if created_at else None,
                "created_time": created_at[11:19] if created_at else None,
                "order_id": str(order.get('id')).split('.')[0] if order.get('id') is not None else None,
                "order_name": order.get('name'),
                "customer_id": cust.get('id'),
                "customer_email": customer_email,
                "customer_first_name": customer_first_name,
                "customer_last_name": customer_last_name,
                "customer_phone": customer_phone,
                "financial_status": order.get('financial_status'),
                "fulfillment_status": (order.get('fulfillment_status') or 'Unfulfilled'),
                "currency": order.get('currency'),
                "discount_codes": discount_codes,
                "discount_amount": float(total_discount_amount or 0),
                "discount_application_titles": discount_application_titles,
                "discount_application_values": discount_application_values,
                "discount_application_types": discount_application_types,
                "discount_application_ids": discount_application_ids,
                "order_app_id": order_app_id,
                "order_app_name": order_app_name,
                "total_price": float(order.get('total_price') or 0) if order.get('total_price') is not None else None,
                "shipping_price": float(order.get('total_shipping_price_set',{}).get('shop_money',{}).get('amount') or 0),
                "total_tax": float(order.get('current_total_tax') or 0),
                "payment_gateway_names": ', '.join(order.get('payment_gateway_names',[]) or []) or None,
                "total_discounts": float(order.get('total_discounts') or 0),
                "total_duties": float(order.get('total_duties') or 0),
                "sku": item.get('sku'),
                "variant_title": item.get('variant_title'),
                "line_item": item.get('title'),
                "line_item_price": float(item.get('price') or 0),
                "line_item_quantity": int(item.get('quantity') or 0),
                "line_item_total_discount": float(total_discount_li or 0),
                "tags": order.get('tags') or None,
                "updated_at": updated_at,
                "updated_date": updated_at[:10] if updated_at else None,
                "updated_time": updated_at[11:19] if updated_at else None,
                "orig_referrer": order.get('orig_referrer'),
                "full_url": order.get('full_url'),
                "customer_ip": order.get('customer_ip'),
                "pg_order_id": order.get('pg_order_id'),
                "shipping_address": ship.get('address1'),
                "shipping_phone": ship.get('phone'),
                "shipping_city": ship.get('city'),
                "shipping_zip": ship.get('zip'),
                "shipping_province": ship.get('province'),
                "billing_address": bill.get('address1'),
                "billing_phone": bill.get('phone'),
                "billing_city": bill.get('city'),
                "billing_zip": bill.get('zip'),
                "billing_province": bill.get('province'),
                "customer_tag": customer_tags,
                "appmaker_platform": order.get('appmaker_platform'),
                "app_version": order.get('app_version'),
                **item_names, **item_values
            })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # normalize & de-dup (same identity as pipeline)
    df = df.replace({np.nan: None})
    df["order_id"] = df["order_id"].apply(lambda v: None if v is None else str(v).split('.')[0])
    dedupe_cols = ["order_id", "line_item", "sku", "line_item_price", "line_item_quantity"]
    df = df.drop_duplicates(subset=dedupe_cols, keep="last")
    return df

def ensure_table_and_unique_key(engine, table_name):
    metadata = MetaData()
    columns = [
        Column('created_at', DateTime), Column('created_date', String(10)), Column('created_time', String(8)),
        Column('order_id', String(50)), Column('order_name', String(50)), Column('customer_id', String(50)),
        Column('customer_email', String(100)), Column('customer_first_name', String(100)), Column('customer_last_name', String(100)),
        Column('customer_phone', String(30)), Column('financial_status', String(50)), Column('fulfillment_status', String(50)),
        Column('currency', String(10)), Column('discount_codes', Text), Column('discount_amount', Float),
        Column('discount_application_titles', Text), Column('discount_application_values', Text),
        Column('discount_application_types', Text), Column('discount_application_ids', Text),
        Column('order_app_id', String(50)), Column('order_app_name', String(100)),
        Column('total_price', Float), Column('shipping_price', Float), Column('total_tax', Float),
        Column('payment_gateway_names', Text), Column('total_discounts', Float), Column('total_duties', Float),
        Column('sku', String(100)), Column('variant_title', String(100)), Column('line_item', String(255)),
        Column('line_item_price', Float), Column('line_item_quantity', Integer), Column('line_item_total_discount', Float),
        Column('tags', Text), Column('updated_at', DateTime), Column('updated_date', String(10)), Column('updated_time', String(8)),
        Column('orig_referrer', Text), Column('full_url', Text), Column('customer_ip', String(50)), Column('pg_order_id', String(50)),
        Column('shipping_address', Text), Column('shipping_phone', String(30)), Column('shipping_city', String(100)),
        Column('shipping_zip', String(20)), Column('shipping_province', String(100)), Column('billing_address', Text),
        Column('billing_phone', String(30)), Column('billing_city', String(100)), Column('billing_zip', String(20)),
        Column('billing_province', String(100)), Column('customer_tag', Text),
        Column('appmaker_platform', String(50)), Column('app_version', String(50)),
    ]
    for n in range(1, 11):
        columns.append(Column(f'_ITEM{n}_name', String(255)))
        columns.append(Column(f'_ITEM{n}_value', String(255)))

    Table(table_name, metadata, *columns)
    try:
        metadata.create_all(engine, checkfirst=True)
    except SQLAlchemyError as e:
        raise RuntimeError(f"Error creating table {table_name}: {e}")

    # add unique key if missing
    try:
        with engine.begin() as conn:
            idx = list(conn.exec_driver_sql(f"SHOW INDEX FROM {table_name} WHERE Key_name = %s", ("uq_order_line",)))
            if not idx:
                conn.exec_driver_sql(
                    f"ALTER TABLE {table_name} "
                    f"ADD UNIQUE KEY uq_order_line (order_id, line_item, sku, line_item_price, line_item_quantity)"
                )
    except Exception as e:
        print(f"⚠️ Could not ensure unique index on {table_name}: {e}")

def save_df(df, engine, table_name, batch_size=250):
    if df.empty:
        print(f"→ No rows to insert for {table_name}")
        return
    ensure_table_and_unique_key(engine, table_name)
    for start in range(0, len(df), batch_size):
        end = start + batch_size
        df.iloc[start:end].to_sql(name=table_name, con=engine, if_exists='append', index=False)
    print(f"✅ Inserted {len(df)} rows into {table_name}")

# -----------------------------
# Backfill driver
# -----------------------------

def backfill_range(brand_idx, start_date_str, end_date_str, mode):
    """
    brand_idx: integer index of the brand in env (e.g., 0)
    start_date_str / end_date_str: 'YYYY-MM-DD' in IST calendar
    mode: 'created' | 'updated' | 'both'
    """
    # read env
    shop_name     = os.environ.get(f"SHOP_NAME_{brand_idx}")
    api_version   = os.environ.get(f"API_VERSION_{brand_idx}")
    access_token  = os.environ.get(f"ACCESS_TOKEN_{brand_idx}")
    mysql_connect = os.environ.get(f"MYSQL_CONNECT_{brand_idx}")
    app_map_json  = os.environ.get(f"APP_ID_MAPPING_{brand_idx}", "{}")

    if not (shop_name and api_version and access_token and mysql_connect):
        raise SystemExit("Missing required env for brand index {}. Check SHOP_NAME_i, API_VERSION_i, ACCESS_TOKEN_i, MYSQL_CONNECT_i".format(brand_idx))

    try:
        app_mapping = json.loads(app_map_json)
    except json.JSONDecodeError:
        app_mapping = {}

    api_base_url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}"
    engine = create_engine(mysql_connect)

    # build UTC range from IST date strings
    # IST start at 00:00, end at 23:59:59 → convert to UTC Z for Shopify
    ist_offset = timedelta(hours=5, minutes=30)
    ist_tz = timezone(ist_offset)

    start_ist = datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=ist_tz)
    end_ist   = datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=ist_tz) + timedelta(hours=23, minutes=59, seconds=59)

    # chunk in days to avoid massive responses
    step = timedelta(days=2)  # adjust if needed

    def run_one_window(win_start_ist, win_end_ist, date_field, dest_table):
        s = quote(to_shopify_iso(win_start_ist.astimezone(timezone.utc)))
        e = quote(to_shopify_iso(win_end_ist.astimezone(timezone.utc)))
        print(f"Fetching {date_field} between {win_start_ist} (IST) and {win_end_ist} (IST)")
        orders = fetch_orders(api_base_url, access_token, s, e, date_field)
        print(f"  → Retrieved {len(orders)} orders")
        if orders:
            df = transform_orders_to_df(orders, app_mapping)
            save_df(df, engine, dest_table)

    cur = start_ist
    while cur <= end_ist:
        win_start = cur
        win_end = min(cur + step - timedelta(seconds=1), end_ist)

        if mode in ("created", "both"):
            run_one_window(win_start, win_end, "created_at_min", "shopify_orders")
        if mode in ("updated", "both"):
            run_one_window(win_start, win_end, "updated_at_min", "shopify_orders_update")

        cur = cur + step

    engine.dispose()
    print("✅ Backfill complete.")

# -----------------------------
# CLI
# -----------------------------
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="One-off Shopify backfill loader (RDS).")
    p.add_argument("--brand-index", type=int, default=0, help="Which brand slot in env to use (default: 0)")
    p.add_argument("--start", required=True, help="Start date in IST (YYYY-MM-DD)")
    p.add_argument("--end", required=True, help="End date in IST (YYYY-MM-DD)")
    p.add_argument("--mode", choices=["created", "updated", "both"], default="both", help="Which stream to backfill (default: both)")
    args = p.parse_args()

    backfill_range(args.brand_index, args.start, args.end, args.mode)
