import os
import time
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
    "host": get_required_env("PTS_BUNDLE_DB_HOST"),
    "user": get_required_env("PTS_BUNDLE_DB_USER"),
    "password": get_required_env("PTS_BUNDLE_DB_PASSWORD"),
    "database": get_required_env("PTS_BUNDLE_DB_NAME"),
    "port": int(os.getenv("PTS_BUNDLE_DB_PORT", "3306")),
    "ssl_disabled": os.getenv("PTS_BUNDLE_DB_SSL_DISABLED", "false").strip().lower() == "true"
}

SHOP_DOMAIN = get_required_env("PTS_BUNDLE_SHOP_DOMAIN")
SHOPIFY_TOKEN = get_required_env("PTS_BUNDLE_SHOPIFY_TOKEN")
API_VERSION = os.getenv("PTS_BUNDLE_API_VERSION", "2025-07").strip()

HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json"
}

START_DATE = "2026-08-01"
END_DATE = "2026-08-10"

OUTPUT_FILE = f"PTS_Bundle_Analysis_{START_DATE}_to_{END_DATE}--.xlsx"

BUNDLE_MAP = {
    10207672992019: "Buy any 3 at 999",
    10386295914771: "Buy any 3 at 1299",
    10219967578387: "Buy any 2 at 1199",
    10169674989843: "Buy any 5 at 2599",
    10207675154707: "Buy any 3 at 1499",
    10199892361491: "Buy any 4 at 1999",
    10304722731283: "PT Vanity Kit (S) At ₹ 999",
    10305748271379: "Buy any 4 at 1499",
    10360719573267: "BUY ANY 3 BATH TOWEL AT 1999",
    10304723616019: "PT Vanity Kit (M) At ₹1999",
    10360727634195: "BUY ANY 3 FACE TOWEL AT 999"
}   

BUNDLE_IDS = tuple(BUNDLE_MAP.keys())
ONLY_SPECIFIED_BUNDLES = True


# ===================== STEP 1 ===================== #

def fetch_bundle_orders():
    conn = mysql.connector.connect(**DB_CONFIG)

    inner_filter = f"product_id IN {BUNDLE_IDS}"

    query = f"""
        SELECT DISTINCT order_id, order_name, created_date 
        FROM shopify_orders
        WHERE created_date BETWEEN '{START_DATE}' AND '{END_DATE}'
        GROUP BY order_id, product_id
        HAVING {inner_filter}
    """

    df = pd.read_sql(query, conn)
    conn.close()

    df["month"] = pd.to_datetime(df["created_date"]).dt.to_period("M").astype(str)
    return df


# ===================== STEP 2 ===================== #

def fetch_shopify_order(order_id):
    url = f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/orders/{order_id}.json"
    r = requests.get(url, headers=HEADERS)
    time.sleep(0.4)

    if r.status_code != 200:
        print(f"Failed to fetch order {order_id} | {r.status_code}")
        return {}

    return r.json().get("order", {})


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
              productId
              title
              quantity
              currentQuantity
              customAttributes {
                key
                value
              }
              originalUnitPrice {
                amount
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

    nodes = r.json().get("data", {}).get("nodes", [])
    result = {}

    for node in nodes:
        if not node:
            continue

        try:
            order_id = int(node["id"].rsplit("/", 1)[-1])
        except Exception:
            continue

        order = {"id": order_id, "line_items": []}

        for li in node.get("lineItems", {}).get("nodes", []):
            pid = li.get("productId")
            try:
                pid_val = int(pid.rsplit("/", 1)[-1]) if isinstance(pid, str) else int(pid)
            except Exception:
                pid_val = None

            props = [
                {"name": ca.get("key"), "value": ca.get("value")}
                for ca in li.get("customAttributes", [])
            ]

            price = li.get("originalUnitPrice", {}).get("amount")

            order["line_items"].append({
                "product_id": pid_val,
                "title": li.get("title"),
                "properties": props,
                "price": price,
                "quantity": li.get("currentQuantity") or li.get("quantity") or 1
            })

        result[order_id] = order

    return result


# ===================== STEP 3 ===================== #

def extract_picked_products(line_item):
    products = []
    for prop in line_item.get("properties", []):
        if prop.get("name", "").startswith("_bundle_product"):
            products.append(prop.get("value").split("/")[0].strip())
    return list(set(products))


# ===================== MAIN ===================== #

def run_analysis():
    orders_df = fetch_bundle_orders()
    print(f"Bundle orders found: {orders_df}")

    sheet3 = defaultdict(lambda: {"orders": 0, "sales": 0})

    order_month_map = dict(zip(orders_df["order_id"], orders_df["month"]))
    order_ids = list(order_month_map.keys())

    print("Total orders to fetch from Shopify:", len(order_ids))

    for chunk_start in range(0, len(order_ids), 50):
        chunk = order_ids[chunk_start:chunk_start + 50]
        orders_map = fetch_shopify_orders_graphql(chunk)

        for order_id in chunk:
            order = orders_map.get(order_id) or fetch_shopify_order(order_id)
            if not order:
                continue

            for item in order.get("line_items", []):
                pid = item.get("product_id")

                if pid not in BUNDLE_MAP:
                    continue

                bundle_name = BUNDLE_MAP[pid]

                picked_products = extract_picked_products(item)
                if not picked_products:
                    continue

                bundle_price = float(item.get("price") or 0)
                per_product_price = bundle_price / len(picked_products)

                bundle_multiplier = item.get("quantity", 1)

                for _ in range(bundle_multiplier):
                    for product in picked_products:
                        key = (bundle_name, product)
                        sheet3[key]["orders"] += 1
                        sheet3[key]["sales"] += per_product_price

    df3 = pd.DataFrame([
        {
            "Bundle Name": b,
            "Product Title": p,
            "Orders": v["orders"],
            "Sales": round(v["sales"], 2)
        }
        for (b, p), v in sheet3.items()
    ])

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        df3.to_excel(writer, sheet_name="Bundle Product Performance", index=False)

    print(f"✅ Excel generated: {OUTPUT_FILE}")


if __name__ == "__main__":
    run_analysis()
