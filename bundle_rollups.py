import json
import logging
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import requests


logger = logging.getLogger(__name__)

def _parse_csv_set(env_name: str, default: str) -> Set[str]:
    raw = os.environ.get(env_name, default)
    return {v.strip().lower() for v in raw.split(",") if v.strip()}


BUNDLE_TARGET_BRAND_TAGS = _parse_csv_set("BUNDLE_TARGET_BRAND_TAG", "tmc,pts")
BUNDLE_TARGET_SHOP_NAMES = _parse_csv_set(
    "BUNDLE_TARGET_SHOP_NAME", "the-man-company,skincarepersonaltouch"
)
EXECUTEMANY_CHUNK_SIZE = int(os.environ.get("EXECUTEMANY_CHUNK_SIZE", "1000"))
ITEM_COLUMN_COUNT = 10
# GraphQL Admin API connections cap `first` at 250 regardless of what's requested.
BUNDLE_DISCOVERY_PAGE_SIZE = min(int(os.environ.get("BUNDLE_DISCOVERY_PAGE_SIZE", "250")), 250)
BUNDLE_PRODUCT_TYPES = ("CYOB", "BUNDLE")

_BUNDLE_DISCOVERY_GRAPHQL = """
query BundleDiscovery($first: Int!, $after: String, $searchQuery: String!) {
  products(first: $first, after: $after, query: $searchQuery) {
    edges {
      cursor
      node {
        id
        title
      }
    }
    pageInfo {
      hasNextPage
    }
  }
}
"""


def _make_admin_api_session() -> requests.Session:
    return requests.Session()


def _build_bundle_discovery_query_filter() -> str:
    """
    Active products only, of any of the configured bundle product types.
    Draft/archived products are deliberately excluded.
    """
    type_clause = " OR ".join(f"product_type:'{value}'" for value in BUNDLE_PRODUCT_TYPES)
    return f"status:active AND ({type_clause})"


def _extract_numeric_id_from_gid(gid: Optional[str]) -> Optional[int]:
    """Shopify GraphQL ids look like 'gid://shopify/Product/1234567890'."""
    if not gid:
        return None
    tail = str(gid).rsplit("/", 1)[-1]
    return _coerce_int(tail)


def _parse_bundle_product_nodes(nodes: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    discovered: List[Dict[str, Any]] = []
    seen_product_ids: Set[int] = set()

    for node in nodes:
        product_id = _extract_numeric_id_from_gid((node or {}).get("id"))
        product_title = str((node or {}).get("title") or "").strip()
        if product_id is None or not product_title:
            continue
        if product_id in seen_product_ids:
            continue
        seen_product_ids.add(product_id)
        discovered.append(
            {
                "product_id": product_id,
                "product_title": product_title,
            }
        )

    return discovered


def fetch_dynamic_bundle_products(
    shop_name: str,
    access_token: str,
    api_version: str,
    *,
    page_size: int = BUNDLE_DISCOVERY_PAGE_SIZE,
) -> List[Dict[str, Any]]:
    """
    Discover bundle products directly from the Shopify Admin API product
    catalog (active products only), instead of a sales report. This finds a
    bundle the moment it's published, rather than only after it has
    accumulated sales history within a lookback window.
    """
    search_query = _build_bundle_discovery_query_filter()
    session = _make_admin_api_session()
    discovered_by_id: Dict[int, Dict[str, Any]] = {}
    after_cursor: Optional[str] = None
    url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token,
    }

    try:
        while True:
            payload = {
                "query": _BUNDLE_DISCOVERY_GRAPHQL,
                "variables": {
                    "first": page_size,
                    "after": after_cursor,
                    "searchQuery": search_query,
                },
            }
            response = session.post(url, headers=headers, data=json.dumps(payload), timeout=60)
            response.raise_for_status()
            body = response.json()
            if body.get("errors"):
                raise RuntimeError(f"Bundle discovery GraphQL errors: {body['errors']}")

            products_conn = (body.get("data") or {}).get("products") or {}
            edges = products_conn.get("edges") or []
            nodes = [edge.get("node") or {} for edge in edges]
            parsed_page = _parse_bundle_product_nodes(nodes)
            for product in parsed_page:
                discovered_by_id.setdefault(product["product_id"], product)

            logger.info(
                "Bundle discovery fetched %s product(s), %s unique product(s) total, query=%s",
                len(edges),
                len(discovered_by_id),
                search_query,
            )

            page_info = products_conn.get("pageInfo") or {}
            if not page_info.get("hasNextPage") or not edges:
                break
            after_cursor = edges[-1].get("cursor")
    finally:
        try:
            session.close()
        except Exception:
            pass

    return list(discovered_by_id.values())


def is_bundle_rollup_brand(
    brand_name: Optional[str], brand_key: Optional[str], shop_name: Optional[str]
) -> bool:
    logger.info(
        "Bundle brand check: brand_name=%s, brand_key=%s, shop_name=%s, target_brand_tags=%s, target_shops=%s",
        brand_name,
        brand_key,
        shop_name,
        BUNDLE_TARGET_BRAND_TAGS,
        BUNDLE_TARGET_SHOP_NAMES,
    )
    if (brand_key or "").strip().lower() in BUNDLE_TARGET_BRAND_TAGS:
        logger.info("Bundle brand check matched on brand_key=%s", brand_key)
        return True
    if (shop_name or "").strip().lower() in BUNDLE_TARGET_SHOP_NAMES:
        logger.info("Bundle brand check matched on shop_name=%s", shop_name)
        return True
    logger.info("Bundle brand check did not match; bundle rollups will be skipped")
    return False


def _dates_in_clause(target_dates: Iterable[str]) -> Tuple[str, List[str]]:
    dates = sorted({str(d) for d in target_dates if d})
    return ",".join(["%s"] * len(dates)), dates


def _products_by_id(
    bundle_products: Optional[Iterable[Dict[str, Any]]],
) -> Dict[int, Dict[str, Any]]:
    if not bundle_products:
        return {}
    products: Dict[int, Dict[str, Any]] = {}
    for product in bundle_products:
        product_id = _coerce_int(product.get("product_id"))
        product_title = str(product.get("product_title") or "").strip()
        if product_id is None or not product_title:
            continue
        products.setdefault(
            product_id,
            {
                "product_id": product_id,
                "product_title": product_title,
            },
        )
    return products


def _bundle_ids_in_scope(
    bundle_ids: Optional[Iterable[int]] = None,
    bundle_products: Optional[Iterable[Dict[str, Any]]] = None,
) -> List[int]:
    dynamic_products = _products_by_id(bundle_products)
    if bundle_ids is not None:
        scoped = sorted({int(pid) for pid in bundle_ids})
        return [pid for pid in scoped if pid in dynamic_products]
    return list(dynamic_products.keys())


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
    cursor,
    connection,
    bundle_products: Optional[Iterable[Dict[str, Any]]] = None,
    bundle_ids: Optional[Iterable[int]] = None,
) -> None:
    products_by_id = _products_by_id(bundle_products)
    scoped_ids = _bundle_ids_in_scope(bundle_ids, bundle_products)
    logger.info(
        "Syncing bundle_master with %s configured bundle(s): %s",
        len(scoped_ids),
        scoped_ids,
    )
    rows = [
        (bundle_id, products_by_id[bundle_id]["product_title"], 1, index)
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


def fetch_bundle_parent_rows_for_dates(
    cursor,
    target_dates: Iterable[str],
    bundle_products: Optional[Iterable[Dict[str, Any]]] = None,
    bundle_ids: Optional[Iterable[int]] = None,
) -> List[Dict]:
    placeholders, params = _dates_in_clause(target_dates)
    scoped_ids = _bundle_ids_in_scope(bundle_ids, bundle_products)
    if not params or not scoped_ids:
        logger.info(
            "Skipping bundle order fetch: target_dates=%s, scoped_bundle_ids=%s",
            params,
            scoped_ids,
        )
        return []

    logger.info(
        "Fetching bundle parent rows for %s target date(s) and %s bundle(s): dates=%s bundles=%s",
        len(params),
        len(scoped_ids),
        params,
        scoped_ids,
    )
    bundle_placeholders = ",".join(["%s"] * len(scoped_ids))
    item_columns = []
    for n in range(1, ITEM_COLUMN_COUNT + 1):
        item_columns.append(f"_ITEM{n}_name")
        item_columns.append(f"_ITEM{n}_value")
    item_columns_sql = ", ".join(item_columns)
    sql = f"""
        SELECT
            order_id,
            created_date,
            product_id,
            line_item,
            line_item_price,
            line_item_quantity,
            line_item_properties_json,
            {item_columns_sql}
        FROM shopify_orders
        WHERE created_date IN ({placeholders})
          AND product_id IN ({bundle_placeholders})
    """
    cursor.execute(sql, tuple(params + scoped_ids))
    rows = cursor.fetchall() or []
    logger.info("Fetched %s bundle parent row(s) from shopify_orders", len(rows))
    return rows


def _coerce_int(value) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _coerce_float(value) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _normalize_row_value(row, key: str, index: int = 0):
    if isinstance(row, dict):
        return row.get(key)
    return row[index]


def _parse_line_item_properties(row: Dict) -> Dict[str, str]:
    props_map: Dict[str, str] = {}
    raw_json = row.get("line_item_properties_json")
    if raw_json:
        try:
            parsed = json.loads(raw_json)
            if isinstance(parsed, list):
                for prop in parsed:
                    if isinstance(prop, dict) and prop.get("name"):
                        props_map[str(prop["name"])] = str(prop.get("value") or "")
        except Exception as exc:
            logger.warning(
                "line_item_properties_json parse error for order_id=%s product_id=%s: %s",
                row.get("order_id"),
                row.get("product_id"),
                exc,
            )
    return props_map


def _extract_children_from_uniware_items(
    props_map: Dict[str, str], order_id, bundle_id, bundle_name
) -> Tuple[List[Dict], bool]:
    items: List[Dict] = []
    raw_uniware = props_map.get("uniware_items")
    if not raw_uniware:
        return items, False

    logger.info(
        "Attempting child extraction via uniware_items for order_id=%s bundle_id=%s bundle_name=%s",
        order_id,
        bundle_id,
        bundle_name,
    )
    try:
        data = json.loads(raw_uniware)
    except Exception as exc:
        logger.warning(
            "uniware_items parse error for order_id=%s bundle_id=%s bundle_name=%s: %s",
            order_id,
            bundle_id,
            bundle_name,
            exc,
        )
        return items, True

    for item in data.get("items", []):
        raw_props = item.get("properties", "{}")
        if isinstance(raw_props, str):
            try:
                item_props = json.loads(raw_props)
            except Exception:
                item_props = {}
        else:
            item_props = raw_props or {}

        if item_props.get("_product") != "child":
            continue

        title = (item.get("title") or item.get("name") or "").strip()
        if not title:
            continue

        items.append(
            {
                "title": title,
                "sku": (
                    str(item.get("sku")).strip()
                    if item.get("sku") not in (None, "")
                    else None
                ),
                "quantity": _coerce_int(item.get("quantity")) or 1,
                "source": "uniware_items",
            }
        )
    return items, True


def _extract_children_from_sub_items(props_map: Dict[str, str]) -> List[Dict]:
    items: List[Dict] = []
    logger.info("Falling back to sub_item_N child extraction")
    i = 1
    while True:
        key = f"sub_item_{i}"
        if key not in props_map:
            break
        raw_value = str(props_map[key]).strip()
        before_sep = raw_value.split("::")[0].strip()
        match = re.match(r"^(.*?)\s+x\s+(\d+)\s*$", before_sep)
        if match:
            title = match.group(1).strip()
            quantity = _coerce_int(match.group(2)) or 1
        else:
            title = before_sep
            quantity = 1
        if title:
            items.append(
                {
                    "title": title,
                    "sku": None,
                    "quantity": quantity,
                    "source": "sub_item",
                }
            )
        i += 1
    return items


def _extract_children_from_bundle_product_properties(
    props_map: Dict[str, str],
) -> List[Dict]:
    items: List[Dict] = []
    logger.info("Falling back to _bundle_product* child extraction")
    for key, raw_value in props_map.items():
        if not key.startswith("_bundle_product"):
            continue
        value = str(raw_value or "").strip()
        if not value:
            continue
        parts = value.split("/")
        title = parts[0].strip()
        sku = parts[1].strip() if len(parts) > 1 and parts[1].strip() else None
        if not title:
            continue
        items.append(
            {
                "title": title,
                "sku": sku,
                "quantity": 1,
                "source": "bundle_product_property",
            }
        )
    return items


def extract_bundle_children(row: Dict) -> Tuple[List[Dict], str]:
    order_id = row.get("order_id")
    bundle_id = row.get("product_id")
    bundle_name = row.get("line_item") or str(bundle_id)
    props_map = _parse_line_item_properties(row)

    uniware_items, uniware_present = _extract_children_from_uniware_items(
        props_map, order_id, bundle_id, bundle_name
    )
    if uniware_items:
        logger.info("Extracted %s child item(s) from uniware_items", len(uniware_items))
        return uniware_items, "uniware_items"

    sub_items = _extract_children_from_sub_items(props_map)
    if sub_items:
        logger.info("Extracted %s child item(s) from sub_item_N", len(sub_items))
        return sub_items, "sub_item"

    bundle_product_items = _extract_children_from_bundle_product_properties(props_map)
    if bundle_product_items:
        logger.info(
            "Extracted %s child item(s) from _bundle_product* properties",
            len(bundle_product_items),
        )
        return bundle_product_items, "bundle_product_property"

    if uniware_present:
        logger.warning(
            "uniware_items was present but yielded no usable child rows for order_id=%s bundle_id=%s bundle_name=%s",
            order_id,
            bundle_id,
            bundle_name,
        )
    logger.info("No child item metadata could be extracted for order_id=%s bundle_id=%s", order_id, bundle_id)
    return [], "none"


def _delete_rollup_dates(
    cursor,
    connection,
    table_name: str,
    target_dates: Iterable[str],
    bundle_products: Optional[Iterable[Dict[str, Any]]] = None,
    bundle_ids: Optional[Iterable[int]] = None,
) -> None:
    placeholders, params = _dates_in_clause(target_dates)
    scoped_ids = _bundle_ids_in_scope(bundle_ids, bundle_products)
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
    bundle_products: Optional[Iterable[Dict[str, Any]]] = None,
    bundle_ids: Optional[Iterable[int]] = None,
) -> Tuple[List[Tuple], List[Tuple]]:
    products_by_id = _products_by_id(bundle_products)
    scoped_ids = _bundle_ids_in_scope(bundle_ids, bundle_products)
    logger.info(
        "Starting bundle rollup row build: target_dates=%s scoped_bundle_ids=%s shop=%s",
        sorted({d for d in target_dates if d}),
        scoped_ids,
        shop_name,
    )
    if not scoped_ids:
        logger.info("No scoped bundle IDs available for row build")
        return [], []

    source_rows = fetch_bundle_parent_rows_for_dates(
        cursor, target_dates, bundle_products=bundle_products, bundle_ids=scoped_ids
    )
    if not source_rows:
        logger.info("No bundle parent rows found for requested dates; rollup rows will be empty")
        return [], []

    skipped_source_rows = 0
    valid_rows: List[Dict] = []
    for raw_row in source_rows:
        row = raw_row if isinstance(raw_row, dict) else {}
        order_id_raw = _normalize_row_value(raw_row, "order_id", 0)
        created_date_raw = _normalize_row_value(raw_row, "created_date", 1)
        product_id_raw = _normalize_row_value(raw_row, "product_id", 2)
        if order_id_raw in (None, "") or created_date_raw in (None, ""):
            skipped_source_rows += 1
            logger.warning(
                "Skipping bundle source row with missing order_id/created_date: order_id=%s created_date=%s",
                order_id_raw,
                created_date_raw,
            )
            continue
        try:
            normalized_order_id = int(order_id_raw)
        except (TypeError, ValueError) as exc:
            skipped_source_rows += 1
            logger.warning(
                "Skipping bundle source row with invalid order_id=%s created_date=%s error=%s",
                order_id_raw,
                created_date_raw,
                exc,
            )
            continue
        row["order_id"] = normalized_order_id
        row["created_date"] = str(created_date_raw)
        row["product_id"] = _coerce_int(product_id_raw)
        valid_rows.append(row)
    if skipped_source_rows:
        logger.info("Skipped %s invalid bundle source row(s) from shopify_orders", skipped_source_rows)

    if not valid_rows:
        logger.info("No valid bundle source rows remained after filtering invalid records")
        return [], []

    order_ids = sorted({_coerce_int(row.get("order_id")) for row in valid_rows if _coerce_int(row.get("order_id")) is not None})
    logger.info(
        "Preparing bundle aggregation for %s bundle parent row(s), %s unique order(s), across %s created date(s)",
        len(valid_rows),
        len(order_ids),
        len({str(row.get("created_date")) for row in valid_rows}),
    )

    daily_agg = defaultdict(
        lambda: {"order_ids": set(), "bundle_units_sold": 0, "bundle_sales": 0.0}
    )
    product_agg = defaultdict(
        lambda: {"child_product_sku": None, "allocated_orders": 0, "allocated_sales": 0.0}
    )
    for row in valid_rows:
        order_id = _coerce_int(row.get("order_id"))
        created_date = str(row.get("created_date"))
        bundle_id = _coerce_int(row.get("product_id"))
        if bundle_id not in scoped_ids:
            logger.info(
                "Skipping source row because bundle product_id=%s is not in scoped bundle IDs",
                bundle_id,
            )
            continue

        bundle_name = row.get("line_item") or products_by_id[bundle_id]["product_title"]
        quantity = _coerce_int(row.get("line_item_quantity")) or 1
        bundle_unit_price = _coerce_float(row.get("line_item_price")) or 0.0
        bundle_sales = bundle_unit_price * quantity
        logger.info(
            "Aggregating bundle parent row: order_id=%s date=%s bundle_id=%s bundle_name=%s quantity=%s bundle_unit_price=%s bundle_sales=%s",
            order_id,
            created_date,
            bundle_id,
            bundle_name,
            quantity,
            bundle_unit_price,
            bundle_sales,
        )

        daily_key = (created_date, bundle_id)
        if order_id is not None:
            daily_agg[daily_key]["order_ids"].add(order_id)
        daily_agg[daily_key]["bundle_units_sold"] += quantity
        daily_agg[daily_key]["bundle_sales"] += bundle_sales

        child_items, child_source = extract_bundle_children(row)
        if not child_items:
            logger.warning(
                "No child items found for order_id=%s bundle_id=%s bundle_name=%s source=%s",
                order_id,
                bundle_id,
                bundle_name,
                child_source,
            )
            continue

        logger.info(
            "Child sales allocation mode for order_id=%s bundle_id=%s bundle_name=%s: proportional_parent_price_split",
            order_id,
            bundle_id,
            bundle_name,
        )

        total_slots = sum(max(_coerce_int(child.get("quantity")) or 1, 1) for child in child_items)
        if total_slots <= 0:
            logger.warning(
                "Invalid total child slots for order_id=%s bundle_id=%s bundle_name=%s",
                order_id,
                bundle_id,
                bundle_name,
            )
            continue

        for child in child_items:
            child_qty = max(_coerce_int(child.get("quantity")) or 1, 1)
            allocated_sales_increment = (bundle_sales / total_slots) * child_qty
            product_key = (created_date, bundle_id, child["title"])
            agg_row = product_agg[product_key]
            if child.get("sku"):
                if agg_row["child_product_sku"] and agg_row["child_product_sku"] != child["sku"]:
                    logger.warning(
                        "SKU mismatch for same child title: date=%s bundle_id=%s child_title=%s existing_sku=%s new_sku=%s",
                        created_date,
                        bundle_id,
                        child["title"],
                        agg_row["child_product_sku"],
                        child["sku"],
                    )
                elif not agg_row["child_product_sku"]:
                    agg_row["child_product_sku"] = child["sku"]
            agg_row["allocated_orders"] += quantity * child_qty
            agg_row["allocated_sales"] += allocated_sales_increment
            logger.info(
                "Allocated child row: order_id=%s date=%s bundle_id=%s child_title=%s child_sku=%s child_qty=%s allocated_orders_increment=%s allocated_sales_increment=%.2f",
                order_id,
                created_date,
                bundle_id,
                child["title"],
                child.get("sku"),
                child_qty,
                quantity * child_qty,
                allocated_sales_increment,
            )

    daily_rows = [
        (
            date_str,
            bundle_id,
            products_by_id[bundle_id]["product_title"],
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
            products_by_id[bundle_id]["product_title"],
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
    bundle_products: Optional[Iterable[Dict[str, Any]]] = None,
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
        cursor, dates, shop_name, access_token, api_version, bundle_products, bundle_ids
    )[0]
    logger.info("[%s] bundle_daily_rollup prepared %s row(s) for insert", brand_name, len(rows))
    _delete_rollup_dates(
        cursor,
        connection,
        "bundle_daily_rollup",
        dates,
        bundle_products=bundle_products,
        bundle_ids=bundle_ids,
    )

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
    bundle_products: Optional[Iterable[Dict[str, Any]]] = None,
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
        cursor, dates, shop_name, access_token, api_version, bundle_products, bundle_ids
    )[1]
    logger.info(
        "[%s] bundle_product_daily_rollup prepared %s row(s) for insert",
        brand_name,
        len(rows),
    )
    _delete_rollup_dates(
        cursor,
        connection,
        "bundle_product_daily_rollup",
        dates,
        bundle_products=bundle_products,
        bundle_ids=bundle_ids,
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
    bundle_products: Optional[Iterable[Dict[str, Any]]] = None,
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
    discovered_products = (
        list(bundle_products)
        if bundle_products is not None
        else fetch_dynamic_bundle_products(shop_name, access_token, api_version)
    )
    sync_bundle_master(
        cursor,
        connection,
        bundle_products=discovered_products,
        bundle_ids=bundle_ids,
    )

    dates = sorted({d for d in target_dates if d})
    if not dates:
        logger.info("✔️ No bundle rollup refresh needed for %s (no affected dates)", brand_name)
        return 0, 0

    daily_rows, product_rows = _build_rollup_rows(
        cursor,
        dates,
        shop_name,
        access_token,
        api_version,
        bundle_products=discovered_products,
        bundle_ids=bundle_ids,
    )
    daily_count = update_bundle_daily_rollup_incremental(
        cursor,
        connection,
        brand_name,
        dates,
        shop_name,
        access_token,
        api_version,
        bundle_products=discovered_products,
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
        bundle_products=discovered_products,
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
