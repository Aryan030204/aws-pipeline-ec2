"""
UTM and referrer attribution, and product traffic split: ShopifyQL-based
daily session fetchers categorized into Meta/Google/direct/others, plus
product-level traffic-source rollups, written to overall_utm_summary /
overall_traffic_split / product_traffic_split / overall_referrer_summary.
"""

import json
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Tuple

from pipeline.state import logger
from pipeline.db import get_db_cursor, executemany_chunked, timed
from pipeline.schema_migrations import ensure_summary_tables
from pipeline.shopifyql_sessions import _format_shopifyql_table, _make_shopifyql_session
from pipeline.shopify_orders import _dates_in_clause


def fetch_shopify_utm_sessions_daily(
    shop_name: str,
    access_token: str,
    start_date: str,
    end_date: str,
) -> List[Dict[str, Any]]:
    if not shop_name or not access_token:
        return []

    # Increment end_date by 1 day because UNTIL is exclusive in ShopifyQL
    next_day = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )

    shopify_ql = f"""
    FROM sessions
      SHOW sessions, sessions_with_cart_additions, utm_source, day
      WHERE human_or_bot_session IN ('human', 'bot')
      GROUP BY day, utm_source
      SINCE {start_date} UNTIL {next_day}
      ORDER BY day DESC, sessions DESC
      LIMIT 10000
    VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values
    """

    ql_flat = shopify_ql.replace("\n", " ").strip().replace('"', '\\"')
    graphql_query = f'''{{ shopifyqlQuery(query: "{ql_flat}") {{ tableData {{ rows columns {{ name dataType }} }} }} }}'''

    # ... (boilerplate request logic same as above) ...
    # Simplified for brevity in this thought trace,
    # but in actual code I will use the full request logic.

    payload = {"query": graphql_query}
    url = f"https://{shop_name}.myshopify.com/admin/api/2025-10/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token,
    }

    session = _make_shopifyql_session()
    try:
        resp = session.post(url, headers=headers, data=json.dumps(payload), timeout=60)
        if resp.status_code != 200:
            return []
        body = resp.json()
        table_data = body.get("data", {}).get("shopifyqlQuery", {}).get("tableData")
        return _format_shopifyql_table(table_data)
    except:
        return []
    finally:
        try:
            session.close()
        except:
            pass


def process_and_upload_utm_summary(
    brand_index: int,
    brand_name: str,
    shop_name: str,
    access_token: str,
    min_date: str,
    max_date: str,
):
    # Fetch with DAY dimension
    raw_rows = fetch_shopify_utm_sessions_daily(
        shop_name, access_token, min_date, max_date
    )

    if not raw_rows:
        return

    # Aggregation buckets
    # Key: (date_str, category) -> {sessions, atc, source_details: {src_name: {sessions, atc}}}
    agg = {}
    
    # New: Daily bucket for overall traffic split (all categories combined)
    # Key: date_str -> {src_name: {sessions, atc}}
    daily_traffic = {}

    for r in raw_rows:
        d_val = r.get("day")
        if not d_val:
            continue
        date_str = str(d_val)

        raw_src = (r.get("utm_source") or "").strip()
        src = raw_src.lower()
        sessions = int(r.get("sessions") or 0)
        atc = int(r.get("sessions_with_cart_additions") or 0)

        category = "others"
        if not src or src == "none" or src == "null":
            category = "direct"
        elif (
            "instagram" in src
            or "facebook" in src
            or src == "ig"
            or src == "fb"
            or "insta" in src
            or "meta" in src
        ):
            category = "Meta"
        elif "google" in src:
            category = "Google"

        key = (date_str, category)
        if key not in agg:
            agg[key] = {"sessions": 0, "atc": 0, "source_details": {}}

        agg[key]["sessions"] += sessions
        agg[key]["atc"] += atc

        # Store individual source details for accurately tracking "others" etc.
        src_name_key = (
            raw_src if raw_src and raw_src.lower() not in ["none", "null"] else category
        )
        if src_name_key not in agg[key]["source_details"]:
            agg[key]["source_details"][src_name_key] = {
                "utm_name": src_name_key,
                "sessions": 0,
                "atc_sessions": 0,
            }

        agg[key]["source_details"][src_name_key]["sessions"] += sessions
        agg[key]["source_details"][src_name_key]["atc_sessions"] += atc

        # New: Track for overall_traffic_split
        if date_str not in daily_traffic:
            daily_traffic[date_str] = {}
        
        if src_name_key not in daily_traffic[date_str]:
            daily_traffic[date_str][src_name_key] = {
                "utm_name": src_name_key,
                "sessions": 0,
                "atc_sessions": 0,
            }
        daily_traffic[date_str][src_name_key]["sessions"] += sessions
        daily_traffic[date_str][src_name_key]["atc_sessions"] += atc

    # Convert to list for DB insert
    db_rows = []
    traffic_split_rows = []
    
    all_dates = sorted(list(set(k[0] for k in agg.keys())))
    categories = ["Meta", "Google", "direct", "others"]

    for d in all_dates:
        # Prepare category-wise rows
        for cat in categories:
            stats = agg.get((d, cat), {"sessions": 0, "atc": 0, "source_details": {}})
            utm_names_list = list(stats.get("source_details", {}).values())
            row_tuple = (
                d,
                cat,
                stats["sessions"],
                stats["atc"],
                json.dumps(utm_names_list),
            )
            db_rows.append(row_tuple)
        
        # Prepare daily traffic split row
        if d in daily_traffic:
            total_utm_list = list(daily_traffic[d].values())
            traffic_split_rows.append((d, json.dumps(total_utm_list)))

    if not db_rows:
        return

    with get_db_cursor(brand_index) as (cursor, connection):
        # 1. Ensure tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS overall_utm_summary (
                date DATE NOT NULL,
                utm_source VARCHAR(255),
                utm_source_sessions INT DEFAULT 0,
                utm_source_atc_sessions INT DEFAULT 0,
                utm_names JSON,
                PRIMARY KEY (date, utm_source)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS overall_traffic_split (
                date DATE PRIMARY KEY,
                utm_source JSON
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # 2. Delete existing for dates
        if all_dates:
            format_strings = ",".join(["%s"] * len(all_dates))
            cursor.execute(
                f"DELETE FROM overall_utm_summary WHERE date IN ({format_strings})",
                tuple(all_dates),
            )
            cursor.execute(
                f"DELETE FROM overall_traffic_split WHERE date IN ({format_strings})",
                tuple(all_dates),
            )

        # 3. Insert new
        sql_summary = """
            INSERT INTO overall_utm_summary
            (date, utm_source, utm_source_sessions, utm_source_atc_sessions, utm_names)
            VALUES (%s, %s, %s, %s, %s)
        """
        executemany_chunked(cursor, connection, sql_summary, db_rows)

        if traffic_split_rows:
            sql_split = """
                INSERT INTO overall_traffic_split (date, utm_source)
                VALUES (%s, %s)
            """
            executemany_chunked(cursor, connection, sql_split, traffic_split_rows)
        logger.info(
            f"✅ Updated overall_utm_summary & overall_traffic_split for {brand_name} ({len(db_rows)} categories, {len(traffic_split_rows)} traffic split rows, {min_date}..{max_date})"
        )


def _normalize_product_traffic_source(raw_source: Any) -> str:
    value = (str(raw_source).strip() if raw_source is not None else "")
    if not value or value.lower() in {"none", "null"}:
        return "direct"
    return value


def update_product_traffic_split_incremental(
    cursor,
    connection,
    brand_name: str,
    target_dates: Iterable[str],
):
    placeholders, params = _dates_in_clause(target_dates)
    if not params:
        logger.debug(f"[{brand_name}] product_traffic_split: no dates to rebuild")
        return

    with timed(
        f"[{brand_name}] product_traffic_split incremental ({len(params)} date(s))"
    ):
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS product_traffic_split_stage LIKE product_traffic_split"
        )
        cursor.execute("TRUNCATE TABLE product_traffic_split_stage")

        cursor.execute(
            f"""
            SELECT
                `date`,
                product_id,
                utm_source,
                SUM(COALESCE(sessions, 0)) AS sessions,
                SUM(COALESCE(sessions_with_cart_additions, 0)) AS atc_sessions
            FROM product_sessions_snapshot
            WHERE `date` IN ({placeholders})
              AND product_id IS NOT NULL
              AND TRIM(product_id) <> ''
            GROUP BY `date`, product_id, utm_source
            """,
            params,
        )
        source_rows = cursor.fetchall()

        aggregated: Dict[Tuple[str, str], Dict[str, Dict[str, int]]] = {}
        for row in source_rows:
            if isinstance(row, dict):
                date_val = row.get("date")
                product_id = row.get("product_id")
                utm_source = row.get("utm_source")
                sessions = row.get("sessions")
                atc_sessions = row.get("atc_sessions")
            else:
                date_val, product_id, utm_source, sessions, atc_sessions = row

            if not date_val or not product_id:
                continue

            key = (str(date_val), str(product_id).strip())
            normalized_source = _normalize_product_traffic_source(utm_source)
            product_sources = aggregated.setdefault(key, {})
            stats = product_sources.setdefault(
                normalized_source,
                {
                    "utm_name": normalized_source,
                    "sessions": 0,
                    "atc_sessions": 0,
                },
            )
            stats["sessions"] += int(sessions or 0)
            stats["atc_sessions"] += int(atc_sessions or 0)

        stage_rows = []
        for (date_str, product_id), source_map in aggregated.items():
            sorted_sources = sorted(
                source_map.values(),
                key=lambda item: (-item["sessions"], item["utm_name"]),
            )
            stage_rows.append((date_str, product_id, json.dumps(sorted_sources)))

        if stage_rows:
            executemany_chunked(
                cursor,
                connection,
                """
                INSERT INTO product_traffic_split_stage (date, product_id, utm_source)
                VALUES (%s, %s, %s)
                """,
                stage_rows,
                commit_between_chunks=False,
            )
        else:
            connection.commit()

        cursor.execute("START TRANSACTION")
        cursor.execute(
            f"DELETE FROM product_traffic_split WHERE date IN ({placeholders})",
            params,
        )
        if stage_rows:
            cursor.execute(
                f"""
                INSERT INTO product_traffic_split (date, product_id, utm_source)
                SELECT date, product_id, utm_source
                FROM product_traffic_split_stage
                WHERE date IN ({placeholders})
                """,
                params,
            )
        connection.commit()
        logger.info(
            f"âœ… Updated product_traffic_split for {brand_name} ({len(stage_rows)} rows across {len(params)} date(s))"
        )


def refresh_product_traffic_split_for_window(
    brand_index: int,
    brand_name: str,
    min_date: str,
    max_date: str,
):
    with get_db_cursor(brand_index) as (cursor, connection):
        ensure_summary_tables(cursor, connection)
        cursor.execute(
            """
            SELECT DISTINCT `date`
            FROM product_sessions_snapshot
            WHERE `date` BETWEEN %s AND %s
            ORDER BY `date`
            """,
            (min_date, max_date),
        )
        rows = cursor.fetchall()
        target_dates = set()
        for row in rows:
            value = row.get("date") if isinstance(row, dict) else row[0]
            if value:
                target_dates.add(str(value))

        update_product_traffic_split_incremental(
            cursor,
            connection,
            brand_name,
            target_dates,
        )


def fetch_shopify_referrer_sessions_daily(
    shop_name: str,
    access_token: str,
    start_date: str,
    end_date: str,
) -> List[Dict[str, Any]]:
    if not shop_name or not access_token:
        return []

    # Increment end_date by 1 day because UNTIL is exclusive in ShopifyQL
    next_day = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )

    shopify_ql = f"""
    FROM sessions
      SHOW sessions, sessions_with_cart_additions, referrer_name, day
      WHERE human_or_bot_session IN ('human', 'bot')
      GROUP BY day, referrer_name
      SINCE {start_date} UNTIL {next_day}
      ORDER BY day DESC, sessions DESC
      LIMIT 10000
    VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values
    """

    ql_flat = shopify_ql.replace("\n", " ").strip().replace('"', '\\"')
    graphql_query = f'''{{ shopifyqlQuery(query: "{ql_flat}") {{ tableData {{ rows columns {{ name dataType }} }} }} }}'''

    payload = {"query": graphql_query}
    url = f"https://{shop_name}.myshopify.com/admin/api/2025-10/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token,
    }

    session = _make_shopifyql_session()
    try:
        resp = session.post(url, headers=headers, data=json.dumps(payload), timeout=60)
        if resp.status_code != 200:
            return []
        body = resp.json()
        table_data = body.get("data", {}).get("shopifyqlQuery", {}).get("tableData")
        return _format_shopifyql_table(table_data)
    except:
        return []
    finally:
        try:
            session.close()
        except:
            pass


def process_and_upload_referrer_summary(
    brand_index: int,
    brand_name: str,
    shop_name: str,
    access_token: str,
    min_date: str,
    max_date: str,
):
    raw_rows = fetch_shopify_referrer_sessions_daily(
        shop_name, access_token, min_date, max_date
    )

    if not raw_rows:
        return

    # Aggregation buckets
    # Key: (date_str, category) -> {sessions, atc, source_details: {src_name: {sessions, atc}}}
    agg = {}

    for r in raw_rows:
        d_val = r.get("day")
        if not d_val:
            continue
        date_str = str(d_val)

        raw_ref = (r.get("referrer_name") or "").strip()
        ref = raw_ref.lower()
        sessions = int(r.get("sessions") or 0)
        atc = int(r.get("sessions_with_cart_additions") or 0)

        category = "others"
        if not ref or ref == "none" or ref == "null":
            category = "direct"
        elif (
            "instagram" in ref
            or "facebook" in ref
            or ref == "ig"
            or ref == "fb"
            or "meta" in ref
        ):
            category = "Meta"
        elif "google" in ref:
            category = "Google"

        key = (date_str, category)
        if key not in agg:
            agg[key] = {"sessions": 0, "atc": 0, "source_details": {}}

        agg[key]["sessions"] += sessions
        agg[key]["atc"] += atc

        # Store individual source details
        ref_name_key = (
            raw_ref if raw_ref and raw_ref.lower() not in ["none", "null"] else category
        )
        if ref_name_key not in agg[key]["source_details"]:
            agg[key]["source_details"][ref_name_key] = {
                "referrer_name": ref_name_key,
                "sessions": 0,
                "atc_sessions": 0,
            }

        agg[key]["source_details"][ref_name_key]["sessions"] += sessions
        agg[key]["source_details"][ref_name_key]["atc_sessions"] += atc

    # Convert to list for DB insert
    db_rows = []
    all_dates = sorted(list(set(k[0] for k in agg.keys())))
    categories = ["Meta", "Google", "direct", "others"]

    for d in all_dates:
        for cat in categories:
            stats = agg.get((d, cat), {"sessions": 0, "atc": 0, "source_details": {}})
            referrer_names_list = list(stats.get("source_details", {}).values())

            row_tuple = (
                d,
                cat,
                stats["sessions"],
                stats["atc"],
                json.dumps(referrer_names_list),
            )
            db_rows.append(row_tuple)

    if not db_rows:
        return

    with get_db_cursor(brand_index) as (cursor, connection):
        # 1. Ensure table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS overall_referrer_summary (
                date DATE NOT NULL,
                referrer_name VARCHAR(255),
                referrer_sessions INT DEFAULT 0,
                referrer_atc_sessions INT DEFAULT 0,
                referrer_names JSON,
                PRIMARY KEY (date, referrer_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # 2. Delete existing for dates
        if all_dates:
            format_strings = ",".join(["%s"] * len(all_dates))
            cursor.execute(
                f"DELETE FROM overall_referrer_summary WHERE date IN ({format_strings})",
                tuple(all_dates),
            )

        # 3. Insert new
        sql = """
            INSERT INTO overall_referrer_summary
            (date, referrer_name, referrer_sessions, referrer_atc_sessions, referrer_names)
            VALUES (%s, %s, %s, %s, %s)
        """
        executemany_chunked(cursor, connection, sql, db_rows)
        logger.info(
            f"✅ Updated overall_referrer_summary for {brand_name} ({len(db_rows)} rows, {min_date}..{max_date})"
        )
