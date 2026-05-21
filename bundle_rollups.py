import json
import logging
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import requests


logger = logging.getLogger(__name__)

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

GRAPHQL_CHUNK_SIZE = int(os.environ.get("BUNDLE_GRAPHQL_CHUNK_SIZE", "50"))
GRAPHQL_SLEEP_SECONDS = float(os.environ.get("BUNDLE_GRAPHQL_SLEEP_SECONDS", "0.5"))
BUNDLE_TARGET_BRAND_TAG = os.environ.get("BUNDLE_TARGET_BRAND_TAG", "tmc").strip().lower()
BUNDLE_TARGET_SHOP_NAME = os.environ.get(
    "BUNDLE_TARGET_SHOP_NAME", "the-man-company"
).strip().lower()
EXECUTEMANY_CHUNK_SIZE = int(os.environ.get("EXECUTEMANY_CHUNK_SIZE", "1000"))


def is_bundle_rollup_brand(
    brand_name: Optional[str], brand_key: Optional[str], shop_name: Optional[str]
) -> bool:
    logger.info(
        "Bundle brand check: brand_name=%s, brand_key=%s, shop_name=%s, target_brand_tag=%s, target_shop=%s",
        brand_name,
        brand_key,
        shop_name,
        BUNDLE_TARGET_BRAND_TAG,
        BUNDLE_TARGET_SHOP_NAME,
    )
    if (brand_key or "").strip().lower() == BUNDLE_TARGET_BRAND_TAG:
        logger.info("Bundle brand check matched on brand_key=%s", brand_key)
        return True
    if (brand_name or "").strip().lower() == "tmc":
        logger.info("Bundle brand check matched on brand_name=%s", brand_name)
        return True
    if (shop_name or "").strip().lower() == BUNDLE_TARGET_SHOP_NAME:
        logger.info("Bundle brand check matched on shop_name=%s", shop_name)
        return True
    logger.info("Bundle brand check did not match; bundle rollups will be skipped")
    return False


def _dates_in_clause(target_dates: Iterable[str]) -> Tuple[str, List[str]]:
    dates = sorted({str(d) for d in target_dates if d})
    return ",".join(["%s"] * len(dates)), dates


def _bundle_ids_in_scope(bundle_ids: Optional[Iterable[int]] = None) -> List[int]:
    scoped = sorted(
        {int(pid) for pid in (bundle_ids if bundle_ids is not None else BUNDLE_MAP.keys())}
    )
    return [pid for pid in scoped if pid in BUNDLE_MAP]


def _chunked(items: Sequence[int], size: int) -> Iterable[Sequence[int]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _executemany_chunked(cursor, connection, sql: str, rows: List[Tuple]) -> int:
    if not rows:
        return 0
    total = len(rows)
    for i in range(0, total, EXECUTEMANY_CHUNK_SIZE):
        cursor.executemany(sql, rows[i : i + EXECUTEMANY_CHUNK_SIZE])
        connection.commit()
    return total


def ensure_bundle_rollup_tables(cursor, connection) -> None:
    logger.info("Ensuring bundle rollup tables exist")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS bundle_master (
            bundle_product_id BIGINT PRIMARY KEY,
            bundle_name VARCHAR(255) NOT NULL,
            is_active TINYINT(1) NOT NULL DEFAULT 1,
            sort_order INT NOT NULL DEFAULT 0,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS bundle_daily_rollup (
            date DATE NOT NULL,
            bundle_product_id BIGINT NOT NULL,
            bundle_name VARCHAR(255) NOT NULL,
            bundle_order_count INT NOT NULL DEFAULT 0,
            bundle_units_sold INT NOT NULL DEFAULT 0,
            bundle_sales DECIMAL(12,2) NOT NULL DEFAULT 0,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (date, bundle_product_id),
            KEY idx_bundle_date (bundle_product_id, date),
            KEY idx_date (date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS bundle_product_daily_rollup (
            date DATE NOT NULL,
            bundle_product_id BIGINT NOT NULL,
            bundle_name VARCHAR(255) NOT NULL,
            child_product_title VARCHAR(255) NOT NULL,
            child_product_sku VARCHAR(255) NULL,
            allocated_orders INT NOT NULL DEFAULT 0,
            allocated_sales DECIMAL(12,2) NOT NULL DEFAULT 0,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (date, bundle_product_id, child_product_title),
            KEY idx_bundle_date (bundle_product_id, date),
            KEY idx_date (date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    connection.commit()
    logger.info("Bundle rollup tables ensured successfully")


def sync_bundle_master(
    cursor, connection, bundle_ids: Optional[Iterable[int]] = None
) -> None:
    scoped_ids = _bundle_ids_in_scope(bundle_ids)
    logger.info(
        "Syncing bundle_master with %s configured bundle(s): %s",
        len(scoped_ids),
        scoped_ids,
    )
    rows = [
        (bundle_id, BUNDLE_MAP[bundle_id], 1, index)
        for index, bundle_id in enumerate(scoped_ids, start=1)
    ]

    logger.info("Marking existing bundle_master rows inactive before sync")
    cursor.execute("UPDATE bundle_master SET is_active = 0")
    if rows:
        logger.info("Upserting %s bundle_master row(s)", len(rows))
        _executemany_chunked(
            cursor,
            connection,
            """
            INSERT INTO bundle_master (
                bundle_product_id, bundle_name, is_active, sort_order
            )
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                bundle_name = VALUES(bundle_name),
                is_active = VALUES(is_active),
                sort_order = VALUES(sort_order)
            """,
            rows,
        )
    else:
        connection.commit()
    logger.info("bundle_master sync completed")


def fetch_bundle_orders_for_dates(
    cursor, target_dates: Iterable[str], bundle_ids: Optional[Iterable[int]] = None
) -> List[Dict]:
    placeholders, params = _dates_in_clause(target_dates)
    scoped_ids = _bundle_ids_in_scope(bundle_ids)
    if not params or not scoped_ids:
        logger.info(
            "Skipping bundle order fetch: target_dates=%s, scoped_bundle_ids=%s",
            params,
            scoped_ids,
        )
        return []

    logger.info(
        "Fetching bundle orders for %s target date(s) and %s bundle(s): dates=%s bundles=%s",
        len(params),
        len(scoped_ids),
        params,
        scoped_ids,
    )
    bundle_placeholders = ",".join(["%s"] * len(scoped_ids))
    sql = f"""
        SELECT DISTINCT order_id, created_date
        FROM shopify_orders
        WHERE created_date IN ({placeholders})
          AND product_id IN ({bundle_placeholders})
    """
    cursor.execute(sql, tuple(params + scoped_ids))
    rows = cursor.fetchall() or []
    logger.info("Fetched %s distinct bundle order row(s) from shopify_orders", len(rows))
    return rows


def fetch_shopify_orders_graphql(
    order_ids: Sequence[int], shop_name: str, access_token: str, api_version: str
) -> Dict[int, Dict]:
    if not order_ids:
        logger.info("Skipping GraphQL fetch: no order_ids provided")
        return {}

    logger.info(
        "Calling Shopify GraphQL for %s order(s); shop=%s api_version=%s first_order=%s last_order=%s",
        len(order_ids),
        shop_name,
        api_version,
        order_ids[0],
        order_ids[-1],
    )
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

    url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/graphql.json"
    payload = {"query": query, "variables": {"ids": gid_ids}}
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }

    response = requests.post(url, headers=headers, json=payload, timeout=60)
    response.raise_for_status()
    logger.info(
        "GraphQL response received successfully for %s order(s); status=%s",
        len(order_ids),
        response.status_code,
    )
    if GRAPHQL_SLEEP_SECONDS > 0:
        logger.info("Sleeping for %s second(s) after GraphQL call", GRAPHQL_SLEEP_SECONDS)
        time.sleep(GRAPHQL_SLEEP_SECONDS)

    payload_json = response.json()
    if "errors" in payload_json:
        logger.warning("GraphQL bundle fetch errors: %s", payload_json["errors"])

    result: Dict[int, Dict] = {}
    for node in payload_json.get("data", {}).get("nodes", []):
        if not node:
            continue

        order_id = int(str(node["id"]).rsplit("/", 1)[-1])
        line_items = []
        for item in node.get("lineItems", {}).get("nodes", []):
            product = item.get("product") or {}
            product_gid = product.get("id")
            product_id = int(str(product_gid).rsplit("/", 1)[-1]) if product_gid else None
            props = {
                attr.get("key"): attr.get("value")
                for attr in item.get("customAttributes", [])
                if attr.get("key")
            }
            line_items.append(
                {
                    "product_id": product_id,
                    "quantity": int(item.get("quantity") or 1),
                    "price": float(
                        item.get("originalUnitPriceSet", {})
                        .get("shopMoney", {})
                        .get("amount")
                        or 0
                    ),
                    "props": props,
                }
            )

        result[order_id] = {"line_items": line_items}

    logger.info(
        "Parsed GraphQL payload into %s order node(s) for requested %s order(s)",
        len(result),
        len(order_ids),
    )
    return result


def extract_item_names_tmc(props: Dict[str, str]) -> List[Tuple[str, Optional[str], int]]:
    items: List[Tuple[str, Optional[str], int]] = []

    if "uniware_items" in props:
        logger.info("Attempting child extraction via uniware_items")
        try:
            data = json.loads(props["uniware_items"])
            for item in data.get("items", []):
                raw_props = item.get("properties", "{}")
                if isinstance(raw_props, str):
                    try:
                        item_props = json.loads(raw_props)
                    except Exception:
                        item_props = {}
                else:
                    item_props = raw_props or {}

                if item_props.get("_product") == "child":
                    name = (item.get("title") or "").strip()
                    sku = (
                        item.get("sku")
                        or item_props.get("sku")
                        or item_props.get("_sku")
                        or item_props.get("product_sku")
                    )
                    sku_clean = str(sku).strip() if sku else None
                    per_qty = int(item.get("quantity") or 1)
                    if name:
                        items.append((name, sku_clean, per_qty))
        except Exception as exc:
            logger.warning("uniware_items parse error: %s", exc)

    if not items:
        logger.info("Falling back to sub_item_N child extraction")
        i = 1
        while True:
            key = f"sub_item_{i}"
            if key not in props:
                break

            value = str(props[key]).strip()
            match = re.match(r"^(.+?)\s+x\s+(\d+)\s*::", value)
            if match:
                name = match.group(1).strip()
                per_qty = int(match.group(2))
            else:
                name = value.split("::")[0].strip()
                per_qty = 1
            if name:
                items.append((name, None, per_qty))
            i += 1

    logger.info("Extracted %s child item(s) from bundle props", len(items))
    return items


def _delete_rollup_dates(
    cursor,
    connection,
    table_name: str,
    target_dates: Iterable[str],
    bundle_ids: Optional[Iterable[int]] = None,
) -> None:
    placeholders, params = _dates_in_clause(target_dates)
    scoped_ids = _bundle_ids_in_scope(bundle_ids)
    if not params:
        logger.info("Skipping delete for %s: no target dates", table_name)
        return

    if scoped_ids:
        logger.info(
            "Deleting existing rows from %s for %s date(s) and %s bundle(s)",
            table_name,
            len(params),
            len(scoped_ids),
        )
        bundle_placeholders = ",".join(["%s"] * len(scoped_ids))
        sql = (
            f"DELETE FROM {table_name} WHERE date IN ({placeholders}) "
            f"AND bundle_product_id IN ({bundle_placeholders})"
        )
        cursor.execute(sql, tuple(params + scoped_ids))
    else:
        logger.info(
            "Deleting existing rows from %s for %s date(s) without bundle filter",
            table_name,
            len(params),
        )
        cursor.execute(f"DELETE FROM {table_name} WHERE date IN ({placeholders})", tuple(params))
    connection.commit()
    logger.info("Delete phase completed for %s", table_name)


def _daterange(start_date: str, end_date: str) -> Set[str]:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    dates = set()
    current = start
    while current <= end:
        dates.add(current.isoformat())
        current += timedelta(days=1)
    return dates


def build_target_dates(start_date: str, end_date: str) -> Set[str]:
    dates = _daterange(start_date, end_date)
    logger.info(
        "Built %s target date(s) for bundle rollups from %s to %s",
        len(dates),
        start_date,
        end_date,
    )
    return dates


def _build_rollup_rows(
    cursor,
    target_dates: Iterable[str],
    shop_name: str,
    access_token: str,
    api_version: str,
    bundle_ids: Optional[Iterable[int]] = None,
) -> Tuple[List[Tuple], List[Tuple]]:
    scoped_ids = _bundle_ids_in_scope(bundle_ids)
    logger.info(
        "Starting bundle rollup row build: target_dates=%s scoped_bundle_ids=%s shop=%s",
        sorted({d for d in target_dates if d}),
        scoped_ids,
        shop_name,
    )
    if not scoped_ids:
        logger.info("No scoped bundle IDs available for row build")
        return [], []

    order_rows = fetch_bundle_orders_for_dates(cursor, target_dates, scoped_ids)
    if not order_rows:
        logger.info("No bundle orders found for requested dates; rollup rows will be empty")
        return [], []

    order_dates: Dict[int, str] = {}
    skipped_source_rows = 0
    for row in order_rows:
        order_id_raw = row["order_id"] if isinstance(row, dict) else row[0]
        created_date_raw = row["created_date"] if isinstance(row, dict) else row[1]
        if order_id_raw in (None, "") or created_date_raw in (None, ""):
            skipped_source_rows += 1
            logger.warning(
                "Skipping bundle source row with missing order_id/created_date: order_id=%s created_date=%s",
                order_id_raw,
                created_date_raw,
            )
            continue
        try:
            order_dates[int(order_id_raw)] = str(created_date_raw)
        except (TypeError, ValueError) as exc:
            skipped_source_rows += 1
            logger.warning(
                "Skipping bundle source row with invalid order_id=%s created_date=%s error=%s",
                order_id_raw,
                created_date_raw,
                exc,
            )

    if skipped_source_rows:
        logger.info("Skipped %s invalid bundle source row(s) from shopify_orders", skipped_source_rows)

    if not order_dates:
        logger.info("No valid bundle source rows remained after filtering invalid records")
        return [], []

    order_ids = sorted(order_dates.keys())
    logger.info(
        "Preparing bundle aggregation for %s unique order(s) across %s created date(s)",
        len(order_ids),
        len(set(order_dates.values())),
    )

    daily_agg = defaultdict(
        lambda: {"order_ids": set(), "bundle_units_sold": 0, "bundle_sales": 0.0}
    )
    product_agg = defaultdict(
        lambda: {"child_product_sku": None, "allocated_orders": 0, "allocated_sales": 0.0}
    )

    chunk_count = 0
    for chunk in _chunked(order_ids, GRAPHQL_CHUNK_SIZE):
        chunk_count += 1
        logger.info(
            "Processing GraphQL chunk %s with %s order(s): first=%s last=%s",
            chunk_count,
            len(chunk),
            chunk[0],
            chunk[-1],
        )
        orders_map = fetch_shopify_orders_graphql(
            chunk, shop_name=shop_name, access_token=access_token, api_version=api_version
        )
        logger.info(
            "Chunk %s returned %s order payload(s)",
            chunk_count,
            len(orders_map),
        )
        for order_id, order in orders_map.items():
            created_date = order_dates.get(order_id)
            if not created_date:
                logger.warning(
                    "Skipping order %s from GraphQL payload because created_date was not found in source order map",
                    order_id,
                )
                continue

            for item in order.get("line_items", []):
                bundle_id = item.get("product_id")
                if bundle_id not in scoped_ids:
                    continue

                bundle_name = BUNDLE_MAP[bundle_id]
                quantity = int(item.get("quantity") or 1)
                bundle_sales = float(item.get("price") or 0) * quantity
                logger.info(
                    "Aggregating bundle line item: order_id=%s date=%s bundle_id=%s bundle_name=%s quantity=%s bundle_sales=%s",
                    order_id,
                    created_date,
                    bundle_id,
                    bundle_name,
                    quantity,
                    bundle_sales,
                )

                daily_key = (created_date, bundle_id)
                daily_agg[daily_key]["order_ids"].add(order_id)
                daily_agg[daily_key]["bundle_units_sold"] += quantity
                daily_agg[daily_key]["bundle_sales"] += bundle_sales

                child_items = extract_item_names_tmc(item.get("props") or {})
                if not child_items:
                    logger.warning(
                        "No child items found for order %s, bundle %s",
                        order_id,
                        bundle_name,
                    )
                    continue

                total_slots = sum(child_qty for _, _, child_qty in child_items)
                if total_slots <= 0:
                    logger.warning(
                        "Invalid total slots for order %s, bundle %s", order_id, bundle_name
                    )
                    continue

                for child_title, child_sku, child_qty in child_items:
                    product_key = (created_date, bundle_id, child_title)
                    row = product_agg[product_key]
                    if child_sku and not row["child_product_sku"]:
                        row["child_product_sku"] = child_sku
                    row["allocated_orders"] += quantity * child_qty
                    row["allocated_sales"] += (bundle_sales / total_slots) * child_qty
                    logger.info(
                        "Allocated child row: order_id=%s date=%s bundle_id=%s child_title=%s child_sku=%s child_qty=%s allocated_orders_increment=%s allocated_sales_increment=%.2f",
                        order_id,
                        created_date,
                        bundle_id,
                        child_title,
                        child_sku,
                        child_qty,
                        quantity * child_qty,
                        (bundle_sales / total_slots) * child_qty,
                    )

    daily_rows = [
        (
            date_str,
            bundle_id,
            BUNDLE_MAP[bundle_id],
            len(values["order_ids"]),
            int(values["bundle_units_sold"]),
            round(values["bundle_sales"], 2),
        )
        for (date_str, bundle_id), values in sorted(daily_agg.items())
    ]

    product_rows = [
        (
            date_str,
            bundle_id,
            BUNDLE_MAP[bundle_id],
            child_title,
            values["child_product_sku"],
            int(values["allocated_orders"]),
            round(values["allocated_sales"], 2),
        )
        for (date_str, bundle_id, child_title), values in sorted(product_agg.items())
    ]

    logger.info(
        "Completed bundle row build: daily_rows=%s product_rows=%s",
        len(daily_rows),
        len(product_rows),
    )
    return daily_rows, product_rows


def update_bundle_daily_rollup_incremental(
    cursor,
    connection,
    brand_name: str,
    target_dates: Iterable[str],
    shop_name: str,
    access_token: str,
    api_version: str,
    bundle_ids: Optional[Iterable[int]] = None,
    prebuilt_rows: Optional[List[Tuple]] = None,
) -> int:
    dates = sorted({d for d in target_dates if d})
    if not dates:
        logger.info("[%s] bundle_daily_rollup: no dates to rebuild", brand_name)
        return 0

    logger.info(
        "[%s] Rebuilding bundle_daily_rollup for %s date(s): %s",
        brand_name,
        len(dates),
        dates,
    )
    rows = prebuilt_rows if prebuilt_rows is not None else _build_rollup_rows(
        cursor, dates, shop_name, access_token, api_version, bundle_ids
    )[0]
    logger.info("[%s] bundle_daily_rollup prepared %s row(s) for insert", brand_name, len(rows))
    _delete_rollup_dates(cursor, connection, "bundle_daily_rollup", dates, bundle_ids)

    inserted = _executemany_chunked(
        cursor,
        connection,
        """
        INSERT INTO bundle_daily_rollup (
            date, bundle_product_id, bundle_name,
            bundle_order_count, bundle_units_sold, bundle_sales
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        rows,
    )
    logger.info(
        "✅ Updated bundle_daily_rollup for %s (%s row(s), %s..%s)",
        brand_name,
        inserted,
        dates[0],
        dates[-1],
    )
    return inserted


def update_bundle_product_daily_rollup_incremental(
    cursor,
    connection,
    brand_name: str,
    target_dates: Iterable[str],
    shop_name: str,
    access_token: str,
    api_version: str,
    bundle_ids: Optional[Iterable[int]] = None,
    prebuilt_rows: Optional[List[Tuple]] = None,
) -> int:
    dates = sorted({d for d in target_dates if d})
    if not dates:
        logger.info("[%s] bundle_product_daily_rollup: no dates to rebuild", brand_name)
        return 0

    logger.info(
        "[%s] Rebuilding bundle_product_daily_rollup for %s date(s): %s",
        brand_name,
        len(dates),
        dates,
    )
    rows = prebuilt_rows if prebuilt_rows is not None else _build_rollup_rows(
        cursor, dates, shop_name, access_token, api_version, bundle_ids
    )[1]
    logger.info(
        "[%s] bundle_product_daily_rollup prepared %s row(s) for insert",
        brand_name,
        len(rows),
    )
    _delete_rollup_dates(
        cursor, connection, "bundle_product_daily_rollup", dates, bundle_ids
    )

    inserted = _executemany_chunked(
        cursor,
        connection,
        """
        INSERT INTO bundle_product_daily_rollup (
            date, bundle_product_id, bundle_name, child_product_title,
            child_product_sku, allocated_orders, allocated_sales
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        rows,
    )
    logger.info(
        "✅ Updated bundle_product_daily_rollup for %s (%s row(s), %s..%s)",
        brand_name,
        inserted,
        dates[0],
        dates[-1],
    )
    return inserted


def execute_bundle_rollups_incremental(
    cursor,
    connection,
    brand_name: str,
    target_dates: Iterable[str],
    shop_name: str,
    access_token: str,
    api_version: str,
    bundle_ids: Optional[Iterable[int]] = None,
) -> Tuple[int, int]:
    logger.info(
        "Starting incremental bundle rollup execution for %s; target_dates=%s shop=%s api_version=%s bundle_filter=%s",
        brand_name,
        sorted({d for d in target_dates if d}),
        shop_name,
        api_version,
        list(bundle_ids) if bundle_ids is not None else None,
    )
    ensure_bundle_rollup_tables(cursor, connection)
    sync_bundle_master(cursor, connection, bundle_ids=bundle_ids)

    dates = sorted({d for d in target_dates if d})
    if not dates:
        logger.info("✔️ No bundle rollup refresh needed for %s (no affected dates)", brand_name)
        return 0, 0

    daily_rows, product_rows = _build_rollup_rows(
        cursor, dates, shop_name, access_token, api_version, bundle_ids
    )
    daily_count = update_bundle_daily_rollup_incremental(
        cursor,
        connection,
        brand_name,
        dates,
        shop_name,
        access_token,
        api_version,
        bundle_ids=bundle_ids,
        prebuilt_rows=daily_rows,
    )
    product_count = update_bundle_product_daily_rollup_incremental(
        cursor,
        connection,
        brand_name,
        dates,
        shop_name,
        access_token,
        api_version,
        bundle_ids=bundle_ids,
        prebuilt_rows=product_rows,
    )
    logger.info(
        "Finished incremental bundle rollup execution for %s: daily_count=%s product_count=%s",
        brand_name,
        daily_count,
        product_count,
    )
    return daily_count, product_count
