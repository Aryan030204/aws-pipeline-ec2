import time
import re
import json
import os
import requests
import pandas as pd
import mysql.connector
from collections import defaultdict
from dotenv import load_dotenv

# ===================== CONFIG ===================== #

load_dotenv(".env.bundle")


def get_required_env(name):
    value = os.getenv(name)
    if value is None or not value.strip():
        raise ValueError(f"Missing required environment variable: {name}")
    return value.strip()


DB_CONFIG = {
    "host": get_required_env("BUNDLE_DB_HOST"),
    "user": get_required_env("BUNDLE_DB_USER"),
    "password": get_required_env("BUNDLE_DB_PASSWORD"),
    "database": get_required_env("BUNDLE_DB_NAME"),
    "port": int(os.getenv("BUNDLE_DB_PORT", "3306")),
    "ssl_disabled": os.getenv("BUNDLE_DB_SSL_DISABLED", "false").strip().lower() == "true"
}

SHOP_DOMAIN = get_required_env("BUNDLE_SHOP_DOMAIN")           # e.g. "your-store.myshopify.com"
SHOPIFY_TOKEN = get_required_env("BUNDLE_SHOPIFY_TOKEN")         # shpat_xxxxx
API_VERSION = os.getenv("BUNDLE_API_VERSION", "2025-07").strip()

HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json"
}

START_DATE = "2026-01-18"
END_DATE = "2026-01-30"

OUTPUT_FILE = f"TMC_Bundle_Analysiss_{START_DATE}_to_{END_DATE}.xlsx"

BUNDLE_MAP = {
      8417496367300: "3 Products at 499",
      8374355493060: "Mini Perfume Box",
      8384621805764: "3 fragrances at 549",
       8376454316228: "Luxury Scent Box",
       8384622067908: "4 products at 699",
       8376464376004: "Fab Five Kit",
       8376464048324: "Self Grooming Kit",
       8417496137924: "3 Products at 349",
       8458140745924: "2 Products at 399",
       8382929600708: "Daily Deo Combo",
       8395644993732: "Fab four Kit"
}

BUNDLE_IDS = tuple(BUNDLE_MAP.keys())

# ===================== STEP 1 ===================== #

def fetch_bundle_orders():
    conn = mysql.connector.connect(**DB_CONFIG)

    query = f"""
        SELECT DISTINCT order_id, created_date
        FROM shopify_orders
        WHERE created_date BETWEEN '{START_DATE}' AND '{END_DATE}'
        AND product_id IN {BUNDLE_IDS}
    """

    df = pd.read_sql(query, conn)
    conn.close()

    print(f"Orders fetched from DB: {len(df)}")
    return df


# ===================== STEP 2 ===================== #

def fetch_shopify_orders_graphql(order_ids):
    if not order_ids:
        return {}

    gid_ids = [f"gid://shopify/Order/{oid}" for oid in order_ids]

    query = """
    query ($ids: [ID!]!) {
      nodes(ids: $ids) {
        ... on Order {
          id
          lineItems(first: 250) {
            nodes {
              product {
                id
              }
              quantity
              originalUnitPriceSet {
                shopMoney {
                  amount
                }
              }
              customAttributes {
                key
                value
              }
            }
          }
        }
      }
    }
    """

    url = f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/graphql.json"
    payload = {"query": query, "variables": {"ids": gid_ids}}

    r = requests.post(url, headers=HEADERS, json=payload)
    time.sleep(0.5)

    resp = r.json()
    if "errors" in resp:
        print(f"  [GraphQL ERROR] {resp['errors']}")

    nodes_raw = resp.get("data", {}).get("nodes", [])
    if not nodes_raw:
        print(f"  [DEBUG] Empty nodes. Response: {str(resp)[:500]}")

    result = {}

    for node in nodes_raw:
        if not node:
            continue

        order_id = int(node["id"].rsplit("/", 1)[-1])
        order = {"line_items": []}

        for li in node["lineItems"]["nodes"]:
            pid = li.get("product", {})
            pid_val = int(pid["id"].rsplit("/", 1)[-1]) if pid and pid.get("id") else None

            props = {
                ca["key"]: ca["value"]
                for ca in li.get("customAttributes", [])
            }

            price = float(li.get("originalUnitPriceSet", {}).get("shopMoney", {}).get("amount") or 0)
            quantity = int(li.get("quantity") or 1)

            order["line_items"].append({
                "product_id": pid_val,
                "props": props,
                "price": price,
                "quantity": quantity
            })

        result[order_id] = order

    return result


# ===================== STEP 3 ===================== #

def extract_item_names_tmc(props):
    """
    Try uniware_items first (richer — has title, quantity, _product=child).
    Fallback to sub_item_N (format: "Product Name x QTY :: image.png").
    """
    items = []

    # Primary: uniware_items
    if "uniware_items" in props:
        try:
            data = json.loads(props["uniware_items"])
            for item in data.get("items", []):
                raw_props = item.get("properties", "{}")
                if isinstance(raw_props, str):
                    try:
                        item_props = json.loads(raw_props)
                    except:
                        item_props = {}
                else:
                    item_props = raw_props

                if item_props.get("_product") == "child":
                    name = item.get("title", "").strip()
                    per_qty = int(item.get("quantity") or 1)
                    if name:
                        items.append((name, per_qty))
        except Exception as e:
            print(f"  [WARN] uniware_items parse error: {e}")

    # Fallback: sub_item_N
    if not items:
        i = 1
        while True:
            key = f"sub_item_{i}"
            if key in props:
                val = props[key].strip()
                match = re.match(r'^(.+?)\s+x\s+(\d+)\s*::', val)
                if match:
                    name = match.group(1).strip()
                    per_qty = int(match.group(2))
                else:
                    name = val.split("::")[0].strip()
                    per_qty = 1
                items.append((name, per_qty))
                i += 1
            else:
                break

    return items


# ===================== MAIN ===================== #

def run_analysis():
    orders_df = fetch_bundle_orders()
    order_ids = orders_df["order_id"].unique().tolist()

    print(f"Unique orders to process: {len(order_ids)}")

    sheet_data = defaultdict(lambda: {"orders": 0, "sales": 0})

    for i in range(0, len(order_ids), 50):
        chunk = order_ids[i:i + 50]
        orders_map = fetch_shopify_orders_graphql(chunk)

        for order_id, order in orders_map.items():
            for item in order["line_items"]:
                pid = item["product_id"]

                if pid not in BUNDLE_MAP:
                    continue

                bundle_name = BUNDLE_MAP[pid]
                quantity = item["quantity"]
                price = item["price"]
                props = item["props"]

                product_names = extract_item_names_tmc(props)

                if not product_names:
                    print(f"  [WARN] No items found for order {order_id}, bundle {bundle_name}")
                    continue

                total_slots = sum(per_qty for _, per_qty in product_names)
                for product_name, per_qty in product_names:
                    key = (bundle_name, product_name)
                    sheet_data[key]["orders"] += quantity * per_qty
                    sheet_data[key]["sales"] += (price * quantity / total_slots) * per_qty

        print(f"  Processed chunk {i+1}–{min(i+50, len(order_ids))}")

    if not sheet_data:
        print("⚠️  No bundle data found. Check product_ids in BUNDLE_MAP.")
        return

    df_out = pd.DataFrame([
        {
            "Bundle Name": b,
            "Product Title": p,
            "Orders": v["orders"],
            "Sales": round(v["sales"], 2)
        }
        for (b, p), v in sheet_data.items()
    ]).sort_values(["Bundle Name", "Orders"], ascending=[True, False])

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        df_out.to_excel(writer, sheet_name="Bundle Product Performance", index=False)

    print(f"\n✅ Done. Excel saved: {OUTPUT_FILE}")
    print(f"   Total (bundle, product) combinations: {len(df_out)}")


if __name__ == "__main__":
    run_analysis() 
