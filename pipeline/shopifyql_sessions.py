"""
ShopifyQL session/traffic fetchers: daily/hourly and daily/hourly-city
session counts pulled via the ShopifyQL GraphQL endpoint, normalized,
and upserted into hourly_sessions_summary_shopify / daily_citywise_summary /
hourly_citywise_summary. Also owns _make_shopifyql_session, the short-lived
HTTP session factory used for all ShopifyQL calls.
"""

import json
import time
import traceback
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipeline.state import IST, logger
from pipeline.db import get_db_cursor, timed

# aws_background still owns is_backfill_active_for/get_backfill_window_for_brand
# (backfill-window config, used across many categories) and _normalize_city_name
# (discount_dashboard category, not yet extracted). Imported at module level here
# is safe even though aws_background imports this module in turn: attribute
# access only happens when these functions are actually CALLED, which is during
# real pipeline execution, long after aws_background has finished loading.
import aws_background

def _make_shopifyql_session() -> requests.Session:
    """
    Create a short-lived session just for ShopifyQL calls.

    This avoids reusing a potentially stale keep-alive connection from the global
    http_session, which is what tends to cause RemoteDisconnected on later runs.
    """
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"],
        raise_on_status=False,
    )
    ad = HTTPAdapter(
        pool_connections=5, pool_maxsize=10, max_retries=retry, pool_block=False
    )
    s.mount("https://", ad)
    s.mount("http://", ad)
    return s


def _format_shopifyql_table(table_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Normalize ShopifyQL tableData into a list of dict rows.

    Handles both:
      - Legacy array rows: ["value1", "value2", ...]
      - New object rows: { "col_name": "value", ... }
    """
    if not table_data:
        return []

    columns = table_data.get("columns") or []
    rows = table_data.get("rows") or []
    if not rows or not columns:
        return []

    formatted: List[Dict[str, Any]] = []

    for row in rows:
        # Case 1: 2025-10+ object rows
        if isinstance(row, dict):
            obj: Dict[str, Any] = {}
            for col in columns:
                name = col.get("name")
                dtype = (col.get("dataType") or "").upper()
                val = row.get(name)

                if dtype in ("INTEGER", "NUMERIC", "DECIMAL"):
                    try:
                        obj[name] = int(val) if dtype == "INTEGER" else float(val)
                    except (TypeError, ValueError):
                        obj[name] = 0
                else:
                    obj[name] = val
            formatted.append(obj)

        # Case 2: legacy array rows
        elif isinstance(row, (list, tuple)):
            obj = {}
            for idx, val in enumerate(row):
                if idx >= len(columns):
                    continue
                col = columns[idx]
                name = col.get("name")
                dtype = (col.get("dataType") or "").upper()

                if dtype in ("INTEGER", "NUMERIC", "DECIMAL"):
                    try:
                        obj[name] = int(val) if dtype == "INTEGER" else float(val)
                    except (TypeError, ValueError):
                        obj[name] = 0
                else:
                    obj[name] = val
            formatted.append(obj)

    return formatted


def fetch_shopify_sessions_via_shopifyql(
    shop_name: str,
    api_version: str,  # kept for signature compatibility
    access_token: str,
    start_date: str = None,  # YYYY-MM-DD
    end_date: str = None,  # YYYY-MM-DD
) -> List[Dict[str, Any]]:
    """
    Call Shopify Admin GraphQL -> shopifyqlQuery to get daily sessions rows.

    Uses ShopifyQL:

      FROM sessions
        SHOW sessions, sessions_with_cart_additions
        WITH CURRENCY 'INR'
        DURING today
        ORDER BY sessions DESC
        LIMIT 1000
      VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values

    Returns:
      A list of rows shaped like
      {"date": "YYYY-MM-DD", "sessions": int, "atc_sessions": int}.
      Falls back to [] on errors or empty responses.
    """

    if not shop_name or not api_version or not access_token:
        logger.warning(
            "ShopifyQL sessions fetch skipped: missing shop_name/api_version/access_token"
        )
        return []

    # Construct Date Range
    if start_date and end_date:
        # Increment end_date by 1 day because UNTIL is exclusive in ShopifyQL
        next_day = (
            datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        ).strftime("%Y-%m-%d")
        date_clause = f"SINCE {start_date} UNTIL {next_day}"
    else:
        date_clause = "DURING today"

    shopify_ql = f"""
    FROM sessions
      SHOW sessions, sessions_with_cart_additions, day
      GROUP BY day
      WITH CURRENCY 'INR'
      {date_clause}
      ORDER BY day ASC
      LIMIT 10000
    VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values
    """

    # Flatten & escape for GraphQL
    ql_flat = shopify_ql.replace("\n", " ").strip()
    ql_flat = ql_flat.replace('"', '\\"')

    graphql_query = f'''
    query {{
      shopifyqlQuery(query: "{ql_flat}") {{
        tableData {{
          rows
          columns {{
            name
            dataType
            displayName
          }}
        }}
        parseErrors
      }}
    }}
    '''

    payload = {"query": graphql_query}
    # Force the ShopifyQL-compatible version explicitly
    shopifyql_api_version = "2025-10"
    url = f"https://{shop_name}.myshopify.com/admin/api/{shopifyql_api_version}/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token,
    }

    # --- NEW: short-lived session + manual retry on connection errors ---
    session = _make_shopifyql_session()
    try:
        resp = None
        last_exc: Optional[Exception] = None

        for attempt in range(1, 4):  # up to 3 tries
            try:
                with timed(
                    f"ShopifyQL fetch (sessions DURING today) [attempt {attempt}/3]"
                ):
                    resp = session.post(
                        url, headers=headers, data=json.dumps(payload), timeout=60
                    )
                last_exc = None
                break  # success
            except Exception as e:
                last_exc = e
                logger.warning(
                    f"⚠️ ShopifyQL sessions call failed on attempt {attempt}/3: {e}"
                )
                # recreate the session in case the connection pool is in a bad state
                try:
                    session.close()
                except Exception:
                    pass
                session = _make_shopifyql_session()
                time.sleep(2**attempt)  # simple backoff: 2s, 4s, 8s

        if last_exc is not None:
            logger.error(f"❌ ShopifyQL sessions failed after 3 attempts: {last_exc}")
            return []

        if resp.status_code != 200:
            logger.error(
                f"❌ ShopifyQL sessions API error: HTTP {resp.status_code} - {resp.text[:500]}"
            )
            return []

        body = resp.json()

        # Top-level GraphQL errors
        if body.get("errors"):
            logger.error(f"❌ ShopifyQL GraphQL errors: {json.dumps(body['errors'])}")
            return []

        ql_result = (body.get("data") or {}).get("shopifyqlQuery") or {}

        # Parse errors
        parse_errors = ql_result.get("parseErrors") or []
        if parse_errors:
            logger.error(f"❌ ShopifyQL parse errors: {parse_errors}")
            return []

        table_data = ql_result.get("tableData")
        if not table_data:
            logger.warning("ShopifyQL sessions: no tableData in response")
            return []

        rows = _format_shopifyql_table(table_data)
        if not rows:
            logger.info("ShopifyQL sessions: 0 rows returned for today")
            return []

        results = []
        for row in rows:
            d_val = row.get("day")
            sess_val = row.get("sessions")
            atc_val = row.get("sessions_with_cart_additions")
            if not d_val:
                continue
            try:
                s_int = int(sess_val)
            except:
                s_int = 0
            try:
                a_int = int(atc_val)
            except:
                a_int = 0
            results.append(
                {"date": str(d_val), "sessions": s_int, "atc_sessions": a_int}
            )

        logger.info(f"✅ ShopifyQL sessions received {len(results)} daily rows")
        return results

    except Exception as e:
        logger.error(f"❌ Exception while calling ShopifyQL sessions: {e}")
        traceback.print_exc()
        return []
    finally:
        try:
            session.close()
        except Exception:
            pass


def fetch_shopify_hourly_sessions_via_shopifyql(
    shop_name: str,
    api_version: str,
    access_token: str,
    tz: ZoneInfo = IST,
    start_date: str = None,  # YYYY-MM-DD
    end_date: str = None,  # YYYY-MM-DD
) -> List[Dict[str, Any]]:
    """
    Fetch hourly sessions from ShopifyQL.
    """
    if not shop_name or not access_token:
        logger.warning("ShopifyQL hourly sessions fetch skipped: missing credentials")
        return []

    if start_date and end_date:
        # Increment end_date by 1 day because UNTIL is exclusive in ShopifyQL
        next_day = (
            datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        ).strftime("%Y-%m-%d")
        date_clause = f"SINCE {start_date} UNTIL {next_day}"
    else:
        date_clause = "DURING today"

    shopify_ql = f"""
    FROM sessions
      SHOW sessions, sessions_with_cart_additions, hour, day, session_device_type
      WHERE landing_page_path IS NOT NULL
        AND human_or_bot_session IN ('human', 'bot')
      GROUP BY day, hour, session_device_type
      WITH CURRENCY 'INR'
      {date_clause}
      ORDER BY day ASC, hour ASC
      LIMIT 10000
    VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values
    """

    ql_flat = shopify_ql.replace("\n", " ").strip().replace('"', '\\"')

    graphql_query = f'''
    query {{
      shopifyqlQuery(query: "{ql_flat}") {{
        tableData {{
          rows
          columns {{
            name
            dataType
            displayName
          }}
        }}
        parseErrors
      }}
    }}
    '''

    payload = {"query": graphql_query}
    shopifyql_api_version = "2025-10"
    url = f"https://{shop_name}.myshopify.com/admin/api/{shopifyql_api_version}/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token,
    }

    session = _make_shopifyql_session()
    try:
        resp = None
        for attempt in range(1, 4):
            try:
                with timed(
                    f"ShopifyQL hourly fetch ({date_clause}) [attempt {attempt}/3]"
                ):
                    resp = session.post(
                        url, headers=headers, data=json.dumps(payload), timeout=60
                    )
                break
            except Exception as e:
                logger.warning(f"⚠️ Attempt {attempt}/3 failed: {e}")
                try:
                    session.close()
                except:
                    pass
                session = _make_shopifyql_session()
                time.sleep(2**attempt)

        if not resp or resp.status_code != 200:
            logger.error(
                f"❌ ShopifyQL hourly failed: {resp.status_code if resp else 'No response'}"
            )
            return []

        body = resp.json()
        if body.get("errors"):
            logger.error(f"❌ GraphQL errors: {json.dumps(body['errors'])}")
            return []

        ql_result = body.get("data", {}).get("shopifyqlQuery", {})
        if ql_result.get("parseErrors"):
            logger.error(f"❌ Parse errors: {ql_result['parseErrors']}")
            return []

        table_data = ql_result.get("tableData")
        if not table_data:
            return []

        rows = _format_shopifyql_table(table_data)
        if not rows:
            return []

        # Aggregate rows by (date, hour)
        aggregated = {}

        for row in rows:
            h_val = row.get("hour")
            device_type = (row.get("session_device_type") or "other").lower()
            s_val = row.get("sessions")
            atc_val = row.get("sessions_with_cart_additions")

            if not h_val:
                continue

            try:
                if "T" in str(h_val):
                    dt_utc = datetime.fromisoformat(str(h_val).replace("Z", "+00:00"))
                    dt_store = dt_utc.astimezone(tz)
                    d_dt = dt_store.date().isoformat()
                    h_int = dt_store.hour
                else:
                    d_dt = str(row.get("day"))
                    h_int = int(h_val)

                key = (d_dt, h_int)
                if key not in aggregated:
                    aggregated[key] = {
                        "date": d_dt,
                        "hour": h_int,
                        "mobile_sessions": 0,
                        "mobile_atc_sessions": 0,
                        "desktop_sessions": 0,
                        "desktop_atc_sessions": 0,
                        "tablet_sessions": 0,
                        "tablet_atc_sessions": 0,
                        "other_sessions": 0,
                        "other_atc_sessions": 0,
                    }

                s_int = int(s_val) if s_val is not None else 0
                a_int = int(atc_val) if atc_val is not None else 0

                if device_type == "mobile":
                    aggregated[key]["mobile_sessions"] += s_int
                    aggregated[key]["mobile_atc_sessions"] += a_int
                elif device_type == "desktop":
                    aggregated[key]["desktop_sessions"] += s_int
                    aggregated[key]["desktop_atc_sessions"] += a_int
                elif device_type == "tablet":
                    aggregated[key]["tablet_sessions"] += s_int
                    aggregated[key]["tablet_atc_sessions"] += a_int
                else:
                    aggregated[key]["other_sessions"] += s_int
                    aggregated[key]["other_atc_sessions"] += a_int

            except Exception:
                continue

        results = list(aggregated.values())
        logger.info(
            f"✅ ShopifyQL hourly received {len(rows)} raw rows, aggregated into {len(results)} hourly records"
        )
        return results
    except Exception as e:
        logger.error(f"❌ ShopifyQL hourly exception: {e}")
        return []
    finally:
        try:
            session.close()
        except Exception:
            pass


def fetch_shopify_daily_city_sessions_via_shopifyql(
    shop_name: str,
    api_version: str,
    access_token: str,
    start_date: str = None,
    end_date: str = None,
) -> List[Dict[str, Any]]:
    """Fetch daily city sessions from ShopifyQL."""
    if not shop_name or not access_token:
        logger.warning(
            "ShopifyQL daily city sessions fetch skipped: missing credentials"
        )
        return []

    if start_date and end_date:
        next_day = (
            datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        ).strftime("%Y-%m-%d")
        date_clause = f"SINCE {start_date} UNTIL {next_day}"
    else:
        date_clause = "DURING today"

    shopify_ql = f"""
    FROM sessions
      SHOW sessions, sessions_with_cart_additions, day, session_city
      WHERE session_country IS NOT NULL
        AND human_or_bot_session IN ('human', 'bot')
      GROUP BY day, session_city
      {date_clause}
      ORDER BY day ASC, sessions DESC
      LIMIT 10000
    VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values
    """

    ql_flat = shopify_ql.replace("\n", " ").strip().replace('"', '\\"')
    graphql_query = f'''
    query {{
      shopifyqlQuery(query: "{ql_flat}") {{
        tableData {{
          rows
          columns {{
            name
            dataType
            displayName
          }}
        }}
        parseErrors
      }}
    }}
    '''

    payload = {"query": graphql_query}
    shopifyql_api_version = "2025-10"
    url = f"https://{shop_name}.myshopify.com/admin/api/{shopifyql_api_version}/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token,
    }

    session = _make_shopifyql_session()
    try:
        resp = None
        for attempt in range(1, 4):
            try:
                with timed(
                    f"ShopifyQL daily city sessions fetch ({date_clause}) [attempt {attempt}/3]"
                ):
                    resp = session.post(
                        url, headers=headers, data=json.dumps(payload), timeout=60
                    )
                break
            except Exception as e:
                logger.warning(f"⚠️ Attempt {attempt}/3 failed: {e}")
                try:
                    session.close()
                except Exception:
                    pass
                session = _make_shopifyql_session()
                time.sleep(2**attempt)

        if not resp or resp.status_code != 200:
            logger.error(
                f"❌ ShopifyQL daily city sessions failed: {resp.status_code if resp else 'No response'}"
            )
            return []

        body = resp.json()
        if body.get("errors"):
            logger.error(f"❌ GraphQL errors: {json.dumps(body['errors'])}")
            return []

        ql_result = body.get("data", {}).get("shopifyqlQuery", {})
        if ql_result.get("parseErrors"):
            logger.error(f"❌ Parse errors: {ql_result['parseErrors']}")
            return []

        table_data = ql_result.get("tableData")
        if not table_data:
            return []

        rows = _format_shopifyql_table(table_data)
        if not rows:
            return []

        aggregated: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for row in rows:
            d_val = row.get("day")
            if not d_val:
                continue

            city = aws_background._normalize_city_name(row.get("session_city"))
            key = (str(d_val), city)
            if key not in aggregated:
                aggregated[key] = {
                    "date": str(d_val),
                    "city": city,
                    "sessions": 0,
                    "atc_sessions": 0,
                }

            aggregated[key]["sessions"] += int(row.get("sessions") or 0)
            aggregated[key]["atc_sessions"] += int(
                row.get("sessions_with_cart_additions") or 0
            )

        results = list(aggregated.values())
        logger.info(
            f"✅ ShopifyQL daily city sessions received {len(rows)} raw rows, aggregated into {len(results)} records"
        )
        return results
    except Exception as e:
        logger.error(f"❌ ShopifyQL daily city sessions exception: {e}")
        return []
    finally:
        try:
            session.close()
        except Exception:
            pass


def fetch_shopify_hourly_city_sessions_via_shopifyql(
    shop_name: str,
    api_version: str,
    access_token: str,
    tz: ZoneInfo = IST,
    start_date: str = None,
    end_date: str = None,
) -> List[Dict[str, Any]]:
    """Fetch hourly city sessions from ShopifyQL."""
    if not shop_name or not access_token:
        logger.warning(
            "ShopifyQL hourly city sessions fetch skipped: missing credentials"
        )
        return []

    if start_date and end_date:
        next_day = (
            datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        ).strftime("%Y-%m-%d")
        date_clause = f"SINCE {start_date} UNTIL {next_day}"
    else:
        date_clause = "DURING today"

    shopify_ql = f"""
    FROM sessions
      SHOW sessions, sessions_with_cart_additions, hour, day, session_city
      WHERE session_country IS NOT NULL
        AND human_or_bot_session IN ('human', 'bot')
      GROUP BY day, hour, session_city
      {date_clause}
      ORDER BY day ASC, hour ASC, sessions DESC
      LIMIT 10000
    VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values
    """

    ql_flat = shopify_ql.replace("\n", " ").strip().replace('"', '\\"')
    graphql_query = f'''
    query {{
      shopifyqlQuery(query: "{ql_flat}") {{
        tableData {{
          rows
          columns {{
            name
            dataType
            displayName
          }}
        }}
        parseErrors
      }}
    }}
    '''

    payload = {"query": graphql_query}
    shopifyql_api_version = "2025-10"
    url = f"https://{shop_name}.myshopify.com/admin/api/{shopifyql_api_version}/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token,
    }

    session = _make_shopifyql_session()
    try:
        resp = None
        for attempt in range(1, 4):
            try:
                with timed(
                    f"ShopifyQL hourly city sessions fetch ({date_clause}) [attempt {attempt}/3]"
                ):
                    resp = session.post(
                        url, headers=headers, data=json.dumps(payload), timeout=60
                    )
                break
            except Exception as e:
                logger.warning(f"⚠️ Attempt {attempt}/3 failed: {e}")
                try:
                    session.close()
                except Exception:
                    pass
                session = _make_shopifyql_session()
                time.sleep(2**attempt)

        if not resp or resp.status_code != 200:
            logger.error(
                f"❌ ShopifyQL hourly city sessions failed: {resp.status_code if resp else 'No response'}"
            )
            return []

        body = resp.json()
        if body.get("errors"):
            logger.error(f"❌ GraphQL errors: {json.dumps(body['errors'])}")
            return []

        ql_result = body.get("data", {}).get("shopifyqlQuery", {})
        if ql_result.get("parseErrors"):
            logger.error(f"❌ Parse errors: {ql_result['parseErrors']}")
            return []

        table_data = ql_result.get("tableData")
        if not table_data:
            return []

        rows = _format_shopifyql_table(table_data)
        if not rows:
            return []

        aggregated: Dict[Tuple[str, int, str], Dict[str, Any]] = {}
        for row in rows:
            h_val = row.get("hour")
            if h_val is None:
                continue

            city = aws_background._normalize_city_name(row.get("session_city"))
            try:
                if "T" in str(h_val):
                    dt_utc = datetime.fromisoformat(str(h_val).replace("Z", "+00:00"))
                    dt_store = dt_utc.astimezone(tz)
                    d_dt = dt_store.date().isoformat()
                    h_int = dt_store.hour
                else:
                    d_dt = str(row.get("day"))
                    h_int = int(h_val)
            except Exception:
                continue

            key = (d_dt, h_int, city)
            if key not in aggregated:
                aggregated[key] = {
                    "date": d_dt,
                    "hour": h_int,
                    "city": city,
                    "sessions": 0,
                    "atc_sessions": 0,
                }

            aggregated[key]["sessions"] += int(row.get("sessions") or 0)
            aggregated[key]["atc_sessions"] += int(
                row.get("sessions_with_cart_additions") or 0
            )

        results = list(aggregated.values())
        logger.info(
            f"✅ ShopifyQL hourly city sessions received {len(rows)} raw rows, aggregated into {len(results)} records"
        )
        return results
    except Exception as e:
        logger.error(f"❌ ShopifyQL hourly city sessions exception: {e}")
        return []
    finally:
        try:
            session.close()
        except Exception:
            pass


def update_hourly_sessions_summary_from_shopifyql(
    brand_index: int,
    brand_name: str,
    shop_name: str,
    api_version: str,
    access_token: str,
    tz: ZoneInfo,
    cursor=None,
    connection=None,
):
    """
    Update hourly_sessions_summary_shopify from ShopifyQL.
    """
    today_dt = datetime.now(tz).date()
    start_dt = today_dt - timedelta(days=1)
    end_dt = today_dt

    if aws_background.is_backfill_active_for(brand_index):
        backfill_start_dt, backfill_end_dt = aws_background.get_backfill_window_for_brand(brand_index)
        if backfill_start_dt and backfill_end_dt:
            start_dt = backfill_start_dt.date()
            end_dt = backfill_end_dt.date()

    results = fetch_shopify_hourly_sessions_via_shopifyql(
        shop_name=shop_name,
        api_version=api_version,
        access_token=access_token,
        tz=tz,
        start_date=start_dt.isoformat(),
        end_date=end_dt.isoformat(),
    )

    if not results:
        return

    if cursor is not None:
        _do_upsert_hourly_sessions_shopify(results, cursor, connection, brand_name)
    else:
        try:
            with get_db_cursor(brand_index) as (c, conn):
                _do_upsert_hourly_sessions_shopify(results, c, conn, brand_name)
        except Exception as e:
            logger.error(
                f"❌ Error updating hourly Shopify sessions for {brand_name}: {e}"
            )


def _do_upsert_hourly_sessions_shopify(results, cursor, connection, brand_name):
    for r in results:
        # 1. Update hourly_sessions_summary_shopify (Shopify Truth)
        # We also update the main number_of_sessions/atc columns as they represent the shopify truth
        total_sessions = (
            r["mobile_sessions"]
            + r["desktop_sessions"]
            + r["tablet_sessions"]
            + r["other_sessions"]
        )
        total_atc = (
            r["mobile_atc_sessions"]
            + r["desktop_atc_sessions"]
            + r["tablet_atc_sessions"]
            + r["other_atc_sessions"]
        )

        cursor.execute(
            """
            INSERT INTO hourly_sessions_summary_shopify 
            (date, hour, 
             number_of_sessions, number_of_atc_sessions,
             mobile_sessions, mobile_atc_sessions,
             desktop_sessions, desktop_atc_sessions,
             tablet_sessions, tablet_atc_sessions,
             other_sessions, other_atc_sessions)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                number_of_sessions = VALUES(number_of_sessions),
                number_of_atc_sessions = VALUES(number_of_atc_sessions),
                mobile_sessions = VALUES(mobile_sessions),
                mobile_atc_sessions = VALUES(mobile_atc_sessions),
                desktop_sessions = VALUES(desktop_sessions),
                desktop_atc_sessions = VALUES(desktop_atc_sessions),
                tablet_sessions = VALUES(tablet_sessions),
                tablet_atc_sessions = VALUES(tablet_atc_sessions),
                other_sessions = VALUES(other_sessions),
                other_atc_sessions = VALUES(other_atc_sessions);
        """,
            (
                r["date"],
                r["hour"],
                total_sessions,
                total_atc,
                r["mobile_sessions"],
                r["mobile_atc_sessions"],
                r["desktop_sessions"],
                r["desktop_atc_sessions"],
                r["tablet_sessions"],
                r["tablet_atc_sessions"],
                r["other_sessions"],
                r["other_atc_sessions"],
            ),
        )

        # 2. Update hourly_sessions_summary (Internal Telemetry)
        # We also update the total columns here to keep them in sync with Shopify truth
        cursor.execute(
            """
            INSERT INTO hourly_sessions_summary 
            (date, hour, 
             number_of_sessions, number_of_atc_sessions,
             mobile_sessions, mobile_atc_sessions,
             desktop_sessions, desktop_atc_sessions,
             tablet_sessions, tablet_atc_sessions,
             other_sessions, other_atc_sessions)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                number_of_sessions = VALUES(number_of_sessions),
                number_of_atc_sessions = VALUES(number_of_atc_sessions),
                mobile_sessions = VALUES(mobile_sessions),
                mobile_atc_sessions = VALUES(mobile_atc_sessions),
                desktop_sessions = VALUES(desktop_sessions),
                desktop_atc_sessions = VALUES(desktop_atc_sessions),
                tablet_sessions = VALUES(tablet_sessions),
                tablet_atc_sessions = VALUES(tablet_atc_sessions),
                other_sessions = VALUES(other_sessions),
                other_atc_sessions = VALUES(other_atc_sessions);
        """,
            (
                r["date"],
                r["hour"],
                total_sessions,
                total_atc,
                r["mobile_sessions"],
                r["mobile_atc_sessions"],
                r["desktop_sessions"],
                r["desktop_atc_sessions"],
                r["tablet_sessions"],
                r["tablet_atc_sessions"],
                r["other_sessions"],
                r["other_atc_sessions"],
            ),
        )

    connection.commit()
    logger.info(
        f"✅ ShopifyQL hourly sessions and device breakdown updated for {brand_name} ({len(results)} rows)"
    )


def update_daily_city_sessions_summary_from_shopifyql(
    brand_index: int,
    brand_name: str,
    shop_name: str,
    api_version: str,
    access_token: str,
    tz: ZoneInfo,
    cursor=None,
    connection=None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Set[str]:
    """Update daily city session source table from ShopifyQL."""
    today_dt = datetime.now(tz).date()
    start_dt = start_date or (today_dt - timedelta(days=2))
    end_dt = end_date or today_dt

    if start_date is None and end_date is None and aws_background.is_backfill_active_for(brand_index):
        backfill_start_dt, backfill_end_dt = aws_background.get_backfill_window_for_brand(brand_index)
        if backfill_start_dt and backfill_end_dt:
            start_dt = backfill_start_dt.date()
            end_dt = backfill_end_dt.date()

    results = fetch_shopify_daily_city_sessions_via_shopifyql(
        shop_name=shop_name,
        api_version=api_version,
        access_token=access_token,
        start_date=start_dt.isoformat(),
        end_date=end_dt.isoformat(),
    )

    if not results:
        return set()

    if cursor is not None:
        _do_upsert_daily_city_sessions_shopify(results, cursor, connection, brand_name)
    else:
        try:
            with get_db_cursor(brand_index) as (c, conn):
                _do_upsert_daily_city_sessions_shopify(results, c, conn, brand_name)
        except Exception as e:
            logger.error(
                f"❌ Error updating daily city sessions summary for {brand_name}: {e}"
            )
            return set()

    return {str(r['date']) for r in results if r.get('date')}


def _do_upsert_daily_city_sessions_shopify(results, cursor, connection, brand_name):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_city_sessions_summary_shopify (
            date DATE NOT NULL,
            city VARCHAR(100) NOT NULL,
            sessions INT DEFAULT 0,
            atc_sessions INT DEFAULT 0,
            PRIMARY KEY (date, city)
        );
    """
    )
    for r in results:
        cursor.execute(
            """
            INSERT INTO daily_city_sessions_summary_shopify (date, city, sessions, atc_sessions)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                sessions = VALUES(sessions),
                atc_sessions = VALUES(atc_sessions);
        """,
            (r["date"], r["city"], r["sessions"], r["atc_sessions"]),
        )
    connection.commit()
    logger.info(
        f"✅ ShopifyQL daily city sessions updated for {brand_name} ({len(results)} rows)"
    )


def update_hourly_city_sessions_summary_from_shopifyql(
    brand_index: int,
    brand_name: str,
    shop_name: str,
    api_version: str,
    access_token: str,
    tz: ZoneInfo,
    cursor=None,
    connection=None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Set[str]:
    """Update hourly city session source table from ShopifyQL."""
    today_dt = datetime.now(tz).date()
    start_dt = start_date or (today_dt - timedelta(days=1))
    end_dt = end_date or today_dt

    if start_date is None and end_date is None and aws_background.is_backfill_active_for(brand_index):
        backfill_start_dt, backfill_end_dt = aws_background.get_backfill_window_for_brand(brand_index)
        if backfill_start_dt and backfill_end_dt:
            start_dt = backfill_start_dt.date()
            end_dt = backfill_end_dt.date()

    results = fetch_shopify_hourly_city_sessions_via_shopifyql(
        shop_name=shop_name,
        api_version=api_version,
        access_token=access_token,
        tz=tz,
        start_date=start_dt.isoformat(),
        end_date=end_dt.isoformat(),
    )

    if not results:
        return set()

    if cursor is not None:
        _do_upsert_hourly_city_sessions_shopify(results, cursor, connection, brand_name)
    else:
        try:
            with get_db_cursor(brand_index) as (c, conn):
                _do_upsert_hourly_city_sessions_shopify(results, c, conn, brand_name)
        except Exception as e:
            logger.error(
                f"❌ Error updating hourly city sessions summary for {brand_name}: {e}"
            )
            return set()

    return {str(r['date']) for r in results if r.get('date')}


def _do_upsert_hourly_city_sessions_shopify(results, cursor, connection, brand_name):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS hourly_city_sessions_summary_shopify (
            date DATE NOT NULL,
            hour TINYINT UNSIGNED NOT NULL,
            city VARCHAR(100) NOT NULL,
            sessions INT DEFAULT 0,
            atc_sessions INT DEFAULT 0,
            PRIMARY KEY (date, hour, city)
        );
    """
    )
    for r in results:
        cursor.execute(
            """
            INSERT INTO hourly_city_sessions_summary_shopify (date, hour, city, sessions, atc_sessions)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                sessions = VALUES(sessions),
                atc_sessions = VALUES(atc_sessions);
        """,
            (r["date"], r["hour"], r["city"], r["sessions"], r["atc_sessions"]),
        )
    connection.commit()
    logger.info(
        f"✅ ShopifyQL hourly city sessions updated for {brand_name} ({len(results)} rows)"
    )


def update_sessions_summary_from_shopifyql(
    brand_index: int,
    brand_name: str,
    shop_name: str,
    api_version: str,
    access_token: str,
    tz: ZoneInfo,
    cursor=None,
    connection=None,
):
    """
    Use ShopifyQL to update the daily sessions_summary table.
    """
    today_dt = datetime.now(tz).date()
    start_dt = today_dt - timedelta(days=2)  # default lookback
    end_dt = today_dt

    # 1. Determine dates
    if aws_background.is_backfill_active_for(brand_index):
        backfill_start_dt, backfill_end_dt = aws_background.get_backfill_window_for_brand(brand_index)
        if backfill_start_dt and backfill_end_dt:
            start_dt = backfill_start_dt.date()
            end_dt = backfill_end_dt.date()
            logger.info(f"📆 Sessions Backfill Mode: {start_dt} -> {end_dt}")
    else:
        # Try to find the last date in DB to optimize
        try:
            # We need a cursor here. If not provided, we use a temporary one.
            if cursor:
                cursor.execute("SELECT MAX(date) FROM sessions_summary")
                last_db_date = cursor.fetchone()[0]
            else:
                with get_db_cursor(brand_index, dictionary=False) as (c, _):
                    c.execute("SELECT MAX(date) FROM sessions_summary")
                    last_db_date = c.fetchone()[0]

            if last_db_date:
                if isinstance(last_db_date, str):
                    last_db_date = datetime.strptime(last_db_date, "%Y-%m-%d").date()
                start_dt = max(start_dt, last_db_date)
        except Exception:
            pass

    if not aws_background.is_backfill_active_for(brand_index) and end_dt > today_dt:
        end_dt = today_dt
    if start_dt > end_dt:
        start_dt = end_dt

    start_str = start_dt.isoformat()
    end_str = end_dt.isoformat()

    logger.info(f"🚀 Fetching ShopifyQL sessions from {start_str} to {end_str}")

    results = fetch_shopify_sessions_via_shopifyql(
        shop_name=shop_name,
        api_version=api_version,
        access_token=access_token,
        start_date=start_str,
        end_date=end_str,
    )

    if not results:
        logger.info("No session data returned from ShopifyQL.")
        return

    if cursor is not None:
        # Reusing existing connection
        _do_upsert_sessions_from_shopifyql(results, cursor, connection, brand_name)
    else:
        # Create new connection
        try:
            with get_db_cursor(brand_index) as (c, conn):
                _do_upsert_sessions_from_shopifyql(results, c, conn, brand_name)
        except Exception as e:
            logger.error(f"❌ Error updating sessions summary for {brand_name}: {e}")


def _do_upsert_sessions_from_shopifyql(results, cursor, connection, brand_name):
    normalized_results = [r for r in results if isinstance(r, dict)]
    if len(normalized_results) != len(results):
        logger.warning(
            f"⚠️ ShopifyQL daily sessions returned unexpected row shapes for {brand_name}; "
            f"using {len(normalized_results)}/{len(results)} valid rows"
        )

    if not normalized_results:
        logger.info(f"No valid ShopifyQL daily sessions rows to upsert for {brand_name}")
        return

    # Ensure table exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sessions_summary (
            date DATE PRIMARY KEY,
            number_of_sessions INT DEFAULT 0,
            number_of_atc_sessions INT DEFAULT 0
        );
    """)
    for r in normalized_results:
        cursor.execute(
            """
            INSERT INTO sessions_summary (date, number_of_sessions, number_of_atc_sessions)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                number_of_sessions = VALUES(number_of_sessions),
                number_of_atc_sessions = VALUES(number_of_atc_sessions);
        """,
            (r["date"], r["sessions"], r["atc_sessions"]),
        )
    connection.commit()
    logger.info(
        f"✅ ShopifyQL daily sessions updated for {brand_name} ({len(normalized_results)} rows)"
    )
