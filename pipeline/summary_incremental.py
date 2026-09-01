"""
Incremental summary rebuilds: the stage-then-merge helper
(merge_staging_to_main_by_date) and every update_*_incremental function
that recomputes a summary table for only the affected dates, plus the
orchestrator (execute_summary_queries_incremental) that runs them all in
order and the QStash sales-alert push that fires once overall_summary is
up to date.
"""

import traceback
from datetime import date, datetime
from typing import Iterable, Optional
from zoneinfo import ZoneInfo

from pipeline.state import IST, logger, brand_id_from_config
from pipeline.db import get_db_cursor, timed
from pipeline.schema_migrations import ensure_summary_tables
from pipeline.notifications import get_qstash_client, ALERTS_WEBHOOK_URL
from pipeline.shopify_orders import (
    _dates_in_clause,
    _normalized_city_sql_expr,
    backfill_dashboard_order_discount_fact_from_shopify_orders,
)

# aws_background still owns update_overall_summary_event_columns (mongo_events
# category, not yet extracted), now_ist (misc_utils), and the
# DISCOUNT_DASHBOARD_BACKFILL constant. Imported at module level here is safe:
# attribute access only happens when these functions are actually CALLED,
# during real pipeline execution, long after aws_background.py has fully
# finished loading (see pipeline/shopifyql_sessions.py and others for the
# same pattern).
import aws_background


# ---------------------------
# INCREMENTAL Summary Updates (NEW APPROACH)
# ---------------------------


def merge_staging_to_main_by_date(
    cursor,
    connection,
    table_name,
    staging_table_name,
    brand_name,
    batch_size=5,
    date_column="date",
):
    """Merge staging into main using range DELETE + range INSERT per chunk.

    Previously this did per-date DELETE + per-date INSERT in a tight loop, which
    means 2N round-trips and N index-scan deletes for N dates. With a PK on
    `date` (see migrations/001_rds_perf_pass.sql) the DELETE is a key lookup
    and the INSERT can be done in one pass per chunk.
    """
    logger.debug(
        f"🔍 [{brand_name}] Starting staging merge from {staging_table_name} to {table_name}"
    )

    cursor.execute(
        f"SELECT DISTINCT {date_column} FROM {staging_table_name} ORDER BY {date_column}"
    )
    rows = cursor.fetchall()

    affected_dates = []
    for r in rows:
        val = r["date"] if isinstance(r, dict) else r[0]
        if val:
            affected_dates.append(val)

    if not affected_dates:
        logger.info(f"✔️ No dates found in {staging_table_name} for {brand_name}")
        return

    logger.info(
        f"🔄 [{brand_name}] Found {len(affected_dates)} dates to merge into {table_name}"
    )

    for i in range(0, len(affected_dates), batch_size):
        chunk = affected_dates[i : i + batch_size]
        placeholders = ",".join(["%s"] * len(chunk))
        chunk_params = [str(d) for d in chunk]

        cursor.execute(
            f"DELETE FROM {table_name} WHERE {date_column} IN ({placeholders})",
            chunk_params,
        )
        deleted_count = cursor.rowcount

        cursor.execute(
            f"INSERT INTO {table_name} "
            f"SELECT * FROM {staging_table_name} WHERE {date_column} IN ({placeholders})",
            chunk_params,
        )
        inserted_count = cursor.rowcount

        connection.commit()
        logger.info(
            f"   🚀 [{brand_name}] Merged {min(i + batch_size, len(affected_dates))}/{len(affected_dates)} "
            f"dates into {table_name} (-{deleted_count} +{inserted_count})"
        )

    logger.info(f"🏁 [{brand_name}] Completed merge into {table_name}")


def update_sales_summary_incremental(
    cursor, connection, brand_index: int, brand_name: str, target_dates: Iterable[str]
):
    """Update sales_summary for only the specific affected dates."""
    placeholders, params = _dates_in_clause(target_dates)
    if not params:
        logger.debug(f"[{brand_name}] sales_summary: no dates to rebuild")
        return
    with timed(
        f"[{brand_name}] sales_summary incremental ({len(params)} date(s))"
    ):
        # STEP 1: Create staging table
        cursor.execute("DROP TABLE IF EXISTS sales_summary_stage")
        cursor.execute("CREATE TABLE sales_summary_stage LIKE sales_summary")

        # STEP 3: Populate staging table
        sql = f"""
        INSERT INTO sales_summary_stage (
            date, gokwik_sales, gokwik_returns, actual_gokwik_sale,
            KwikEngageSales, KwikEngageReturns, actual_KwikEngage_sale,
            online_store_sales, online_store_returns, actual_online_store_sale,
            hypd_store_sales, hypd_store_returns, actual_hypd_store_sale,
            draft_order_sales, draft_order_returns, actual_draft_order_sale,
            dpanda_sales, dpanda_returns, actual_dpanda_sale,
            gkappbrew_sales, gkappbrew_returns, actual_gkappbrew_sale,
            buykaro_sales, buykaro_returns, actual_buykaro_sale,
            appbrewplus_sales, appbrewplus_returns, actual_appbrewplus_sale,
            shopflo_sales, shopflo_returns, actual_shopflo_sale,
            overall_sales_WO_hypd, overall_returns_WO_hypd, actual_overall_sales_WO_hypd,
            overall_sales, overall_returns,
            rto_gokwik_returns, rto_kwik_engage_returns, rto_online_store_returns,
            rto_hypd_store_returns, rto_draft_order_returns, rto_dpanda_returns,
            rto_gkappbrew_returns, rto_buykaro_returns, rto_appbrewplus_returns,
            rto_shopflo_returns, rto_overall_returns_WO_hypd,
            actual_overall_sales
        )
        WITH SalesData AS (
            SELECT
                STR_TO_DATE(created_date, '%Y-%m-%d') AS date,
                SUM(CASE WHEN order_app_name = 'GoKwik' THEN total_price ELSE 0 END) AS gokwik_sales,
                SUM(CASE WHEN order_app_name = 'KwikEngage' THEN total_price ELSE 0 END) AS kwik_engage_sales,
                SUM(CASE WHEN order_app_name = 'Online Store' THEN total_price ELSE 0 END) AS online_store_sales,
                SUM(CASE WHEN order_app_name = 'HYPD_store' THEN total_price ELSE 0 END) AS hypd_store_sales,
                SUM(CASE WHEN order_app_name = 'Draft Order' THEN total_price ELSE 0 END) AS draft_order_sales,
                SUM(CASE WHEN order_app_name = 'Dpanda' THEN total_price ELSE 0 END) AS dpanda_sales,
                SUM(CASE WHEN order_app_name = 'GKAppbrew' THEN total_price ELSE 0 END) AS gkappbrew_sales,
                SUM(CASE WHEN order_app_name = 'BuyKaro' THEN total_price ELSE 0 END) AS buykaro_sales,
                SUM(CASE WHEN order_app_name = 'AppbrewPlus' THEN total_price ELSE 0 END) AS appbrewplus_sales,
                SUM(CASE WHEN order_app_name = 'Shopflo' THEN total_price ELSE 0 END) AS shopflo_sales,
                SUM(total_price) AS global_overall_sales,
                SUM(CASE WHEN order_app_name != 'HYPD_store' THEN total_price ELSE 0 END) AS global_sales_WO_hypd
            FROM shopify_orders
            WHERE created_date IN ({placeholders})
            GROUP BY date
        ),
        OrderHeaders AS (
            SELECT
                order_id,
                created_date,
                MAX(order_app_name) AS order_app_name
            FROM shopify_orders
            WHERE created_date IN ({placeholders})
            GROUP BY order_id, created_date
        ),
        ReturnsData AS (
            SELECT
                rf.order_created_date AS date,
                SUM(CASE WHEN h.order_app_name = 'GoKwik' THEN COALESCE(rf.amount, 0) ELSE 0 END) AS gokwik_returns,
                SUM(CASE WHEN h.order_app_name = 'KwikEngage' THEN COALESCE(rf.amount, 0) ELSE 0 END) AS kwik_engage_returns,
                SUM(CASE WHEN h.order_app_name = 'Online Store' THEN COALESCE(rf.amount, 0) ELSE 0 END) AS online_store_returns,
                SUM(CASE WHEN h.order_app_name = 'HYPD_store' THEN COALESCE(rf.amount, 0) ELSE 0 END) AS hypd_store_returns,
                SUM(CASE WHEN h.order_app_name = 'Draft Order' THEN COALESCE(rf.amount, 0) ELSE 0 END) AS draft_order_returns,
                SUM(CASE WHEN h.order_app_name = 'Dpanda' THEN COALESCE(rf.amount, 0) ELSE 0 END) AS dpanda_returns,
                SUM(CASE WHEN h.order_app_name = 'GKAppbrew' THEN COALESCE(rf.amount, 0) ELSE 0 END) AS gkappbrew_returns,
                SUM(CASE WHEN h.order_app_name = 'BuyKaro' THEN COALESCE(rf.amount, 0) ELSE 0 END) AS buykaro_returns,
                SUM(CASE WHEN h.order_app_name = 'AppbrewPlus' THEN COALESCE(rf.amount, 0) ELSE 0 END) AS appbrewplus_returns,
                SUM(CASE WHEN h.order_app_name = 'Shopflo' THEN COALESCE(rf.amount, 0) ELSE 0 END) AS shopflo_returns,
                SUM(CASE WHEN h.order_app_name != 'HYPD_store' THEN COALESCE(rf.amount, 0) ELSE 0 END) AS global_returns_WO_hypd
            FROM returns_fact rf
            INNER JOIN OrderHeaders h
                ON h.order_id = CAST(rf.order_id AS CHAR)
               AND h.created_date = DATE_FORMAT(rf.order_created_date, '%Y-%m-%d')
            WHERE rf.event_type = 'CANCEL'
              AND rf.order_created_date IN ({placeholders})
            GROUP BY date
        ),
        RtoReturnsData AS (
            SELECT
                rf.order_created_date AS date,
                SUM(CASE WHEN h.order_app_name = 'GoKwik' THEN COALESCE(rf.amount, 0) ELSE 0 END) AS gokwik_returns,
                SUM(CASE WHEN h.order_app_name = 'KwikEngage' THEN COALESCE(rf.amount, 0) ELSE 0 END) AS kwik_engage_returns,
                SUM(CASE WHEN h.order_app_name = 'Online Store' THEN COALESCE(rf.amount, 0) ELSE 0 END) AS online_store_returns,
                SUM(CASE WHEN h.order_app_name = 'HYPD_store' THEN COALESCE(rf.amount, 0) ELSE 0 END) AS hypd_store_returns,
                SUM(CASE WHEN h.order_app_name = 'Draft Order' THEN COALESCE(rf.amount, 0) ELSE 0 END) AS draft_order_returns,
                SUM(CASE WHEN h.order_app_name = 'Dpanda' THEN COALESCE(rf.amount, 0) ELSE 0 END) AS dpanda_returns,
                SUM(CASE WHEN h.order_app_name = 'GKAppbrew' THEN COALESCE(rf.amount, 0) ELSE 0 END) AS gkappbrew_returns,
                SUM(CASE WHEN h.order_app_name = 'BuyKaro' THEN COALESCE(rf.amount, 0) ELSE 0 END) AS buykaro_returns,
                SUM(CASE WHEN h.order_app_name = 'AppbrewPlus' THEN COALESCE(rf.amount, 0) ELSE 0 END) AS appbrewplus_returns,
                SUM(CASE WHEN h.order_app_name = 'Shopflo' THEN COALESCE(rf.amount, 0) ELSE 0 END) AS shopflo_returns,
                SUM(CASE WHEN h.order_app_name != 'HYPD_store' THEN COALESCE(rf.amount, 0) ELSE 0 END) AS global_returns_WO_hypd
            FROM returns_fact rf
            INNER JOIN OrderHeaders h
                ON h.order_id = CAST(rf.order_id AS CHAR)
               AND h.created_date = DATE_FORMAT(rf.order_created_date, '%Y-%m-%d')
            WHERE rf.event_type = 'CANCEL (RTO)'
              AND rf.order_created_date IN ({placeholders})
            GROUP BY date
        ),
        RefundsByDate AS (
            SELECT event_date AS date, SUM(amount) AS overall_returns
            FROM returns_fact
            WHERE event_type = 'REFUND' AND event_date IN ({placeholders})
            GROUP BY event_date
        ),
        AllDates AS (
            SELECT DISTINCT date FROM SalesData
            UNION SELECT DISTINCT date FROM ReturnsData
            UNION SELECT DISTINCT date FROM RtoReturnsData
            UNION SELECT DISTINCT date FROM RefundsByDate
        )
        SELECT
            d.date,
            COALESCE(s.gokwik_sales, 0), COALESCE(ar.gokwik_returns, 0) - COALESCE(rr.gokwik_returns, 0),
            COALESCE(s.gokwik_sales, 0) - (COALESCE(ar.gokwik_returns, 0) - COALESCE(rr.gokwik_returns, 0)),
            
            COALESCE(s.kwik_engage_sales, 0), COALESCE(ar.kwik_engage_returns, 0) - COALESCE(rr.kwik_engage_returns, 0),
            COALESCE(s.kwik_engage_sales, 0) - (COALESCE(ar.kwik_engage_returns, 0) - COALESCE(rr.kwik_engage_returns, 0)),
            
            COALESCE(s.online_store_sales, 0), COALESCE(ar.online_store_returns, 0) - COALESCE(rr.online_store_returns, 0),
            COALESCE(s.online_store_sales, 0) - (COALESCE(ar.online_store_returns, 0) - COALESCE(rr.online_store_returns, 0)),
            
            COALESCE(s.hypd_store_sales, 0), COALESCE(ar.hypd_store_returns, 0) - COALESCE(rr.hypd_store_returns, 0),
            COALESCE(s.hypd_store_sales, 0) - (COALESCE(ar.hypd_store_returns, 0) - COALESCE(rr.hypd_store_returns, 0)),
            
            COALESCE(s.draft_order_sales, 0), COALESCE(ar.draft_order_returns, 0) - COALESCE(rr.draft_order_returns, 0),
            COALESCE(s.draft_order_sales, 0) - (COALESCE(ar.draft_order_returns, 0) - COALESCE(rr.draft_order_returns, 0)),
            
            COALESCE(s.dpanda_sales, 0), COALESCE(ar.dpanda_returns, 0) - COALESCE(rr.dpanda_returns, 0),
            COALESCE(s.dpanda_sales, 0) - (COALESCE(ar.dpanda_returns, 0) - COALESCE(rr.dpanda_returns, 0)),
            
            COALESCE(s.gkappbrew_sales, 0), COALESCE(ar.gkappbrew_returns, 0) - COALESCE(rr.gkappbrew_returns, 0),
            COALESCE(s.gkappbrew_sales, 0) - (COALESCE(ar.gkappbrew_returns, 0) - COALESCE(rr.gkappbrew_returns, 0)),
            
            COALESCE(s.buykaro_sales, 0), COALESCE(ar.buykaro_returns, 0) - COALESCE(rr.buykaro_returns, 0),
            COALESCE(s.buykaro_sales, 0) - (COALESCE(ar.buykaro_returns, 0) - COALESCE(rr.buykaro_returns, 0)),
            
            COALESCE(s.appbrewplus_sales, 0), COALESCE(ar.appbrewplus_returns, 0) - COALESCE(rr.appbrewplus_returns, 0),
            COALESCE(s.appbrewplus_sales, 0) - (COALESCE(ar.appbrewplus_returns, 0) - COALESCE(rr.appbrewplus_returns, 0)),
            
            COALESCE(s.shopflo_sales, 0), COALESCE(ar.shopflo_returns, 0) - COALESCE(rr.shopflo_returns, 0),
            COALESCE(s.shopflo_sales, 0) - (COALESCE(ar.shopflo_returns, 0) - COALESCE(rr.shopflo_returns, 0)),
            
            COALESCE(s.global_sales_WO_hypd, 0), 
            COALESCE(ar.global_returns_WO_hypd, 0) - COALESCE(rr.global_returns_WO_hypd, 0), 
            COALESCE(s.global_sales_WO_hypd, 0) - (COALESCE(ar.global_returns_WO_hypd, 0) - COALESCE(rr.global_returns_WO_hypd, 0)),
            
            COALESCE(s.global_overall_sales, 0),
            COALESCE(rfd.overall_returns, 0),
            COALESCE(rr.gokwik_returns, 0),
            COALESCE(rr.kwik_engage_returns, 0),
            COALESCE(rr.online_store_returns, 0),
            COALESCE(rr.hypd_store_returns, 0),
            COALESCE(rr.draft_order_returns, 0),
            COALESCE(rr.dpanda_returns, 0),
            COALESCE(rr.gkappbrew_returns, 0),
            COALESCE(rr.buykaro_returns, 0),
            COALESCE(rr.appbrewplus_returns, 0),
            COALESCE(rr.shopflo_returns, 0),
            COALESCE(rr.global_returns_WO_hypd, 0),
            COALESCE(s.global_overall_sales, 0) - COALESCE(rfd.overall_returns, 0)
            
        FROM AllDates d
        LEFT JOIN SalesData s ON d.date = s.date
        LEFT JOIN ReturnsData ar ON d.date = ar.date
        LEFT JOIN RtoReturnsData rr ON d.date = rr.date
        LEFT JOIN RefundsByDate rfd ON d.date = rfd.date
        """
        cursor.execute(
            sql,
            params + params + params + params + params,
        )
        connection.commit()

        # STEP 4 & 5: Fetch affected dates and merge
        merge_staging_to_main_by_date(
            cursor, connection, "sales_summary", "sales_summary_stage", brand_name
        )


def update_order_summary_incremental(
    cursor, connection, brand_index: int, brand_name: str, target_dates: Iterable[str]
):
    """Update order_summary for only the specific affected dates."""
    placeholders, params = _dates_in_clause(target_dates)
    if not params:
        logger.debug(f"[{brand_name}] order_summary: no dates to rebuild")
        return
    with timed(f"[{brand_name}] order_summary incremental ({len(params)} date(s))"):
        # STEP 1: Create staging table
        cursor.execute("DROP TABLE IF EXISTS order_summary_stage")
        cursor.execute("CREATE TABLE order_summary_stage LIKE order_summary")

        # STEP 3: Populate staging table
        sql = f"""
        INSERT INTO order_summary_stage (
            date, number_of_orders_created, number_of_orders_returned, actual_number_of_orders,
            cod_orders, prepaid_orders, partially_paid_orders,
            overall_cod_orders, overall_prepaid_orders, overall_partially_paid_orders,
            rto_number_of_orders_returned, rto_cod_orders_returned,
            rto_prepaid_orders_returned, rto_partially_paid_orders_returned,
            overall_cod_sales, overall_prepaid_sales, overall_partially_paid_sales
        )
        WITH OrderHeaders AS (
            SELECT
                order_id,
                created_date,
                MAX(payment_gateway_names) AS payment_gateway_names
            FROM shopify_orders
            WHERE created_date IN ({placeholders})
            GROUP BY order_id, created_date
        )
        SELECT
            date,
            SUM(orders_created) AS number_of_orders_created,
            SUM(all_orders_returned) - SUM(rto_orders_returned) AS number_of_orders_returned,
            SUM(orders_created) - (SUM(all_orders_returned) - SUM(rto_orders_returned)) AS actual_number_of_orders,
            SUM(cod_created) - (SUM(all_cod_returned) - SUM(rto_cod_returned)) AS cod_orders,
            SUM(prepaid_created) - (SUM(all_prepaid_returned) - SUM(rto_prepaid_returned)) AS prepaid_orders,
            SUM(partially_paid_created) - (SUM(all_partially_paid_returned) - SUM(rto_partially_paid_returned)) AS partially_paid_orders,
            SUM(cod_created) AS overall_cod_orders,
            SUM(prepaid_created) AS overall_prepaid_orders,
            SUM(partially_paid_created) AS overall_partially_paid_orders,
            SUM(rto_orders_returned) AS rto_number_of_orders_returned,
            SUM(rto_cod_returned) AS rto_cod_orders_returned,
            SUM(rto_prepaid_returned) AS rto_prepaid_orders_returned,
            SUM(rto_partially_paid_returned) AS rto_partially_paid_orders_returned,
            SUM(cod_sales_created) AS overall_cod_sales,
            SUM(prepaid_sales_created) AS overall_prepaid_sales,
            SUM(partially_paid_sales_created) AS overall_partially_paid_sales
        FROM (
            SELECT 
                STR_TO_DATE(created_date, '%Y-%m-%d') AS date,
                COUNT(DISTINCT order_id) AS orders_created,
                0 AS all_orders_returned,

                -- COD created: includes NULL, empty, and COD keywords
                COUNT(DISTINCT CASE 
                    WHEN (payment_gateway_names IS NULL 
                      OR payment_gateway_names = '' 
                      OR payment_gateway_names LIKE '%Cash on Delivery (COD)%' 
                      OR payment_gateway_names LIKE '%cash_on_delivery%')
                      AND (payment_gateway_names NOT LIKE '%Gokwik PPCOD%' OR payment_gateway_names IS NULL)
                    THEN order_id END) AS cod_created,

                0 AS all_cod_returned,

                -- Prepaid created: exclude COD, blank, and Gokwik PPCOD
                COUNT(DISTINCT CASE 
                    WHEN payment_gateway_names IS NOT NULL 
                      AND payment_gateway_names != '' 
                      AND NOT (payment_gateway_names LIKE '%Cash on Delivery (COD)%'
                          OR payment_gateway_names LIKE '%cash_on_delivery%'
                          OR payment_gateway_names LIKE '%Gokwik PPCOD%')
                    THEN order_id END) AS prepaid_created,

                0 AS all_prepaid_returned,

                -- Partially paid created (Gokwik PPCOD)
                COUNT(DISTINCT CASE
                    WHEN payment_gateway_names LIKE '%Gokwik PPCOD%'
                    THEN order_id END) AS partially_paid_created,

                0 AS all_partially_paid_returned,
                0 AS rto_orders_returned,
                0 AS rto_cod_returned,
                0 AS rto_prepaid_returned,
                0 AS rto_partially_paid_returned,

                SUM(CASE
                    WHEN (payment_gateway_names IS NULL
                      OR payment_gateway_names = ''
                      OR payment_gateway_names LIKE '%Cash on Delivery (COD)%'
                      OR payment_gateway_names LIKE '%cash_on_delivery%')
                      AND (payment_gateway_names NOT LIKE '%Gokwik PPCOD%' OR payment_gateway_names IS NULL)
                    THEN COALESCE(total_price, 0) ELSE 0 END) AS cod_sales_created,
                SUM(CASE
                    WHEN payment_gateway_names IS NOT NULL
                      AND payment_gateway_names != ''
                      AND NOT (payment_gateway_names LIKE '%Cash on Delivery (COD)%'
                          OR payment_gateway_names LIKE '%cash_on_delivery%'
                          OR payment_gateway_names LIKE '%Gokwik PPCOD%')
                    THEN COALESCE(total_price, 0) ELSE 0 END) AS prepaid_sales_created,
                SUM(CASE
                    WHEN payment_gateway_names LIKE '%Gokwik PPCOD%'
                    THEN COALESCE(total_price, 0) ELSE 0 END) AS partially_paid_sales_created

            FROM shopify_orders
            WHERE created_date IN ({placeholders})
            GROUP BY created_date

            UNION ALL

            SELECT 
                rf.order_created_date AS date,
                0 AS orders_created,
                COUNT(*) AS all_orders_returned,
                0 AS cod_created,
                COUNT(CASE
                    WHEN (h.payment_gateway_names IS NULL
                      OR h.payment_gateway_names = ''
                      OR h.payment_gateway_names LIKE '%Cash on Delivery (COD)%'
                      OR h.payment_gateway_names LIKE '%cash_on_delivery%')
                      AND (h.payment_gateway_names NOT LIKE '%Gokwik PPCOD%' OR h.payment_gateway_names IS NULL)
                    THEN 1 END) AS all_cod_returned,
                0 AS prepaid_created,
                COUNT(CASE
                    WHEN h.payment_gateway_names IS NOT NULL
                      AND h.payment_gateway_names != ''
                      AND NOT (h.payment_gateway_names LIKE '%Cash on Delivery (COD)%'
                          OR h.payment_gateway_names LIKE '%cash_on_delivery%'
                          OR h.payment_gateway_names LIKE '%Gokwik PPCOD%')
                    THEN 1 END) AS all_prepaid_returned,
                0 AS partially_paid_created,
                COUNT(CASE
                    WHEN h.payment_gateway_names LIKE '%Gokwik PPCOD%'
                    THEN 1 END) AS all_partially_paid_returned,
                0 AS rto_orders_returned,
                0 AS rto_cod_returned,
                0 AS rto_prepaid_returned,
                0 AS rto_partially_paid_returned,
                0 AS cod_sales_created,
                0 AS prepaid_sales_created,
                0 AS partially_paid_sales_created
            FROM returns_fact rf
            INNER JOIN OrderHeaders h
                ON h.order_id = CAST(rf.order_id AS CHAR)
               AND h.created_date = DATE_FORMAT(rf.order_created_date, '%Y-%m-%d')
            WHERE rf.event_type = 'CANCEL'
              AND rf.order_created_date IN ({placeholders})
            GROUP BY rf.order_created_date

            UNION ALL

            SELECT 
                rf.order_created_date AS date,
                0 AS orders_created,
                0 AS all_orders_returned,
                0 AS cod_created,
                0 AS all_cod_returned,
                0 AS prepaid_created,
                0 AS all_prepaid_returned,
                0 AS partially_paid_created,
                0 AS all_partially_paid_returned,
                COUNT(*) AS rto_orders_returned,
                COUNT(CASE
                    WHEN (h.payment_gateway_names IS NULL
                      OR h.payment_gateway_names = ''
                      OR h.payment_gateway_names LIKE '%Cash on Delivery (COD)%'
                      OR h.payment_gateway_names LIKE '%cash_on_delivery%')
                      AND (h.payment_gateway_names NOT LIKE '%Gokwik PPCOD%' OR h.payment_gateway_names IS NULL)
                    THEN 1 END) AS rto_cod_returned,
                COUNT(CASE
                    WHEN h.payment_gateway_names IS NOT NULL
                      AND h.payment_gateway_names != ''
                      AND NOT (h.payment_gateway_names LIKE '%Cash on Delivery (COD)%'
                          OR h.payment_gateway_names LIKE '%cash_on_delivery%'
                          OR h.payment_gateway_names LIKE '%Gokwik PPCOD%')
                    THEN 1 END) AS rto_prepaid_returned,
                COUNT(CASE
                    WHEN h.payment_gateway_names LIKE '%Gokwik PPCOD%'
                    THEN 1 END) AS rto_partially_paid_returned,
                0 AS cod_sales_created,
                0 AS prepaid_sales_created,
                0 AS partially_paid_sales_created
            FROM returns_fact rf
            INNER JOIN OrderHeaders h
                ON h.order_id = CAST(rf.order_id AS CHAR)
               AND h.created_date = DATE_FORMAT(rf.order_created_date, '%Y-%m-%d')
            WHERE rf.event_type = 'CANCEL (RTO)'
              AND rf.order_created_date IN ({placeholders})
            GROUP BY rf.order_created_date
        ) AS combined
        WHERE date IS NOT NULL
        GROUP BY date
        """
        cursor.execute(
            sql,
            params + params + params + params,
        )
        connection.commit()

        # STEP 4 & 5: Fetch affected dates and merge
        merge_staging_to_main_by_date(
            cursor, connection, "order_summary", "order_summary_stage", brand_name
        )


def update_discount_summary_incremental(
    cursor, connection, brand_index: int, brand_name: str, target_dates: Iterable[str]
):
    """Update discount_summary for only the specific affected dates."""
    placeholders, params = _dates_in_clause(target_dates)
    if not params:
        logger.debug(f"[{brand_name}] discount_summary: no dates to rebuild")
        return
    with timed(
        f"[{brand_name}] discount_summary incremental ({len(params)} date(s))"
    ):
        # STEP 1: Create staging table
        cursor.execute("DROP TABLE IF EXISTS discount_summary_stage")
        cursor.execute("CREATE TABLE discount_summary_stage LIKE discount_summary")

        # STEP 3: Populate staging table
        sql = f"""
        INSERT INTO discount_summary_stage (
            date, total_discounts_given, total_discount_on_returns,
            rto_total_discount_on_returns, actual_discounts
        )
        WITH DiscountsGiven AS (
            SELECT
                STR_TO_DATE(created_date, '%Y-%m-%d') AS date,
                SUM(COALESCE(discount_amount, 0)) AS total_discounts_given
            FROM shopify_orders
            WHERE created_date IN ({placeholders})
            GROUP BY date
        ),
        OrderHeaders AS (
            SELECT
                order_id,
                created_date,
                MAX(discount_amount) AS discount_amount
            FROM shopify_orders
            WHERE created_date IN ({placeholders})
            GROUP BY order_id, created_date
        ),
        DiscountsReturned AS (
            SELECT
                rf.order_created_date AS date,
                SUM(COALESCE(h.discount_amount, 0)) AS total_discount_on_returns
            FROM returns_fact rf
            INNER JOIN OrderHeaders h
                ON h.order_id = CAST(rf.order_id AS CHAR)
               AND h.created_date = DATE_FORMAT(rf.order_created_date, '%Y-%m-%d')
            WHERE rf.event_type = 'CANCEL'
              AND rf.order_created_date IN ({placeholders})
            GROUP BY date
        ),
        RtoDiscountsReturned AS (
            SELECT
                rf.order_created_date AS date,
                SUM(COALESCE(h.discount_amount, 0)) AS total_discount_on_returns
            FROM returns_fact rf
            INNER JOIN OrderHeaders h
                ON h.order_id = CAST(rf.order_id AS CHAR)
               AND h.created_date = DATE_FORMAT(rf.order_created_date, '%Y-%m-%d')
            WHERE rf.event_type = 'CANCEL (RTO)'
              AND rf.order_created_date IN ({placeholders})
            GROUP BY date
        ),
        AllDates AS (
            SELECT DISTINCT date FROM DiscountsGiven
            UNION
            SELECT DISTINCT date FROM DiscountsReturned
            UNION
            SELECT DISTINCT date FROM RtoDiscountsReturned
        )
        SELECT
            d.date, 
            COALESCE(dg.total_discounts_given, 0) AS total_discounts_given,
            (COALESCE(dr.total_discount_on_returns, 0) - COALESCE(rdr.total_discount_on_returns, 0)) AS total_discount_on_returns,
            COALESCE(rdr.total_discount_on_returns, 0) AS rto_total_discount_on_returns,
            (COALESCE(dg.total_discounts_given, 0) - (COALESCE(dr.total_discount_on_returns, 0) - COALESCE(rdr.total_discount_on_returns, 0))) AS actual_discounts
        FROM AllDates d
        LEFT JOIN DiscountsGiven dg ON d.date = dg.date
        LEFT JOIN DiscountsReturned dr ON d.date = dr.date
        LEFT JOIN RtoDiscountsReturned rdr ON d.date = rdr.date
        """
        cursor.execute(
            sql,
            params + params + params + params,
        )
        connection.commit()

        # STEP 4 & 5: Fetch affected dates and merge
        merge_staging_to_main_by_date(
            cursor, connection, "discount_summary", "discount_summary_stage", brand_name
        )


def update_gross_summary_incremental(
    cursor, connection, brand_name: str, target_dates: Iterable[str]
):
    """Update gross_summary for only the specific affected dates."""
    placeholders, params = _dates_in_clause(target_dates)
    if not params:
        logger.debug(f"[{brand_name}] gross_summary: no dates to rebuild")
        return
    with timed(f"[{brand_name}] gross_summary incremental ({len(params)} date(s))"):
        # STEP 1: Create staging table
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS gross_summary_stage LIKE gross_summary"
        )
        # STEP 2: Clear staging table
        cursor.execute("TRUNCATE TABLE gross_summary_stage")

        # STEP 3: Populate staging table
        sql = f"""
        INSERT INTO gross_summary_stage (
            date, overall_sale, shipping_total, discounts_total, tax_total, 
            gross_sales, actual_discounts, net_sales
        )
        WITH ShopifyAggregates AS (
            SELECT
                STR_TO_DATE(created_date, '%Y-%m-%d') AS date,
                SUM(COALESCE(line_item_quantity, 0) * COALESCE(line_item_price, 0)) AS overall_sale,
                SUM(COALESCE(shipping_price, 0)) AS shipping_total,
                SUM(COALESCE(total_tax, 0)) AS tax_total
            FROM shopify_orders
            WHERE created_date IN ({placeholders})
            GROUP BY date
        )
        SELECT
            sa.date,
            sa.overall_sale,
            sa.shipping_total,
            COALESCE(ds.total_discounts_given, 0) AS discounts_total,
            sa.tax_total,
            (sa.overall_sale * 0.84) AS gross_sales,
            COALESCE(ds.actual_discounts, 0) AS actual_discounts,
            ((sa.overall_sale * 0.84) - COALESCE(ds.actual_discounts, 0)) AS net_sales
        FROM ShopifyAggregates sa
        LEFT JOIN discount_summary ds ON sa.date = ds.date
        """
        cursor.execute(sql, params)
        connection.commit()

        # STEP 4 & 5: Fetch affected dates and merge
        merge_staging_to_main_by_date(
            cursor, connection, "gross_summary", "gross_summary_stage", brand_name
        )


def update_hour_wise_sales_incremental(
    cursor, connection, brand_name: str, target_dates: Iterable[str]
):
    """Update hour_wise_sales for only the specific affected dates."""
    placeholders, params = _dates_in_clause(target_dates)
    if not params:
        logger.debug(f"[{brand_name}] hour_wise_sales: no dates to rebuild")
        return
    with timed(
        f"[{brand_name}] hour_wise_sales incremental ({len(params)} date(s))"
    ):
        # STEP 1: Create staging table
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS hour_wise_sales_stage LIKE hour_wise_sales"
        )
        # STEP 2: Clear staging table
        cursor.execute("TRUNCATE TABLE hour_wise_sales_stage")

        # STEP 3: Populate staging table
        sql = f"""
        INSERT INTO hour_wise_sales_stage (
            date, hour, number_of_orders, total_sales, number_of_prepaid_orders,
            number_of_cod_orders, number_of_ppcod_orders, cod_sales, prepaid_sales,
            partially_prepaid_sales, number_of_sessions, number_of_atc_sessions
        )
        WITH HourlySales AS (
            SELECT
                STR_TO_DATE(created_date, '%Y-%m-%d') AS date,
                HOUR(created_time) AS hour,
                COUNT(DISTINCT order_id) AS number_of_orders,
                SUM(COALESCE(total_price, 0)) AS total_sales,
                COUNT(DISTINCT CASE WHEN (payment_gateway_names IS NOT NULL AND payment_gateway_names != '')
                    AND NOT (payment_gateway_names LIKE '%Cash on Delivery (COD)%'
                    OR payment_gateway_names LIKE '%cash_on_delivery%'
                    OR payment_gateway_names LIKE '%Gokwik PPCOD%') THEN order_id END) AS number_of_prepaid_orders,
                COUNT(DISTINCT CASE WHEN (payment_gateway_names is NULL OR payment_gateway_names LIKE '%Cash on Delivery (COD)%'
                    OR payment_gateway_names LIKE '%cash_on_delivery%')
                    AND (payment_gateway_names NOT LIKE '%Gokwik PPCOD%' OR payment_gateway_names is NULL) THEN order_id END) AS number_of_cod_orders,
                COUNT(DISTINCT CASE WHEN payment_gateway_names LIKE '%Gokwik PPCOD%'
                    THEN order_id END) AS number_of_ppcod_orders,
                SUM(CASE WHEN (payment_gateway_names is NULL OR payment_gateway_names LIKE '%Cash on Delivery (COD)%'
                    OR payment_gateway_names LIKE '%cash_on_delivery%')
                    AND (payment_gateway_names NOT LIKE '%Gokwik PPCOD%' OR payment_gateway_names is NULL)
                    THEN COALESCE(total_price, 0) ELSE 0 END) AS cod_sales,
                SUM(CASE WHEN (payment_gateway_names IS NOT NULL AND payment_gateway_names != '')
                    AND NOT (payment_gateway_names LIKE '%Cash on Delivery (COD)%'
                    OR payment_gateway_names LIKE '%cash_on_delivery%'
                    OR payment_gateway_names LIKE '%Gokwik PPCOD%')
                    THEN COALESCE(total_price, 0) ELSE 0 END) AS prepaid_sales,
                SUM(CASE WHEN payment_gateway_names LIKE '%Gokwik PPCOD%'
                    THEN COALESCE(total_price, 0) ELSE 0 END) AS partially_prepaid_sales
            FROM shopify_orders
            WHERE created_date IN ({placeholders})
                AND created_time IS NOT NULL
            GROUP BY date, hour
        ),
        HourlySessions AS (
            SELECT
                date,
                hour,
                (mobile_sessions + desktop_sessions + tablet_sessions + other_sessions) AS number_of_sessions,
                (mobile_atc_sessions + desktop_atc_sessions + tablet_atc_sessions + other_atc_sessions) AS number_of_atc_sessions
            FROM hourly_sessions_summary_shopify
            WHERE date IN ({placeholders})
        ),
        AllKeys AS (
            SELECT date, hour FROM HourlySales
            UNION
            SELECT date, hour FROM HourlySessions
        )
        SELECT
            ak.date,
            ak.hour,
            COALESCE(hs.number_of_orders, 0) AS number_of_orders,
            COALESCE(hs.total_sales, 0) AS total_sales,
            COALESCE(hs.number_of_prepaid_orders, 0) AS number_of_prepaid_orders,
            COALESCE(hs.number_of_cod_orders, 0) AS number_of_cod_orders,
            COALESCE(hs.number_of_ppcod_orders, 0) AS number_of_ppcod_orders,
            COALESCE(hs.cod_sales, 0) AS cod_sales,
            COALESCE(hs.prepaid_sales, 0) AS prepaid_sales,
            COALESCE(hs.partially_prepaid_sales, 0) AS partially_prepaid_sales,
            COALESCE(ss.number_of_sessions, 0) AS number_of_sessions,
            COALESCE(ss.number_of_atc_sessions, 0) AS number_of_atc_sessions
        FROM AllKeys ak
        LEFT JOIN HourlySales hs ON ak.date = hs.date AND ak.hour = hs.hour
        LEFT JOIN HourlySessions ss ON ak.date = ss.date AND ak.hour = ss.hour
        """
        cursor.execute(sql, params + params)
        connection.commit()

        # STEP 4 & 5: Fetch affected dates and merge
        merge_staging_to_main_by_date(
            cursor, connection, "hour_wise_sales", "hour_wise_sales_stage", brand_name
        )


_PAYMENT_MODE_BREAKDOWN_CASE_SQL = """
        COUNT(DISTINCT CASE WHEN (payment_gateway_names IS NOT NULL AND payment_gateway_names != '')
            AND NOT (payment_gateway_names LIKE '%Cash on Delivery (COD)%'
            OR payment_gateway_names LIKE '%cash_on_delivery%'
            OR payment_gateway_names LIKE '%Gokwik PPCOD%') THEN order_id END) AS prepaid_orders,
        COUNT(DISTINCT CASE WHEN (payment_gateway_names is NULL OR payment_gateway_names LIKE '%Cash on Delivery (COD)%'
            OR payment_gateway_names LIKE '%cash_on_delivery%')
            AND (payment_gateway_names NOT LIKE '%Gokwik PPCOD%' OR payment_gateway_names is NULL) THEN order_id END) AS cod_orders,
        COUNT(DISTINCT CASE WHEN payment_gateway_names LIKE '%Gokwik PPCOD%'
            THEN order_id END) AS ppcod_orders,
        SUM(CASE WHEN (payment_gateway_names is NULL OR payment_gateway_names LIKE '%Cash on Delivery (COD)%'
            OR payment_gateway_names LIKE '%cash_on_delivery%')
            AND (payment_gateway_names NOT LIKE '%Gokwik PPCOD%' OR payment_gateway_names is NULL)
            THEN COALESCE(total_price, 0) ELSE 0 END) AS cod_sales,
        SUM(CASE WHEN (payment_gateway_names IS NOT NULL AND payment_gateway_names != '')
            AND NOT (payment_gateway_names LIKE '%Cash on Delivery (COD)%'
            OR payment_gateway_names LIKE '%cash_on_delivery%'
            OR payment_gateway_names LIKE '%Gokwik PPCOD%')
            THEN COALESCE(total_price, 0) ELSE 0 END) AS prepaid_sales,
        SUM(CASE WHEN payment_gateway_names LIKE '%Gokwik PPCOD%'
            THEN COALESCE(total_price, 0) ELSE 0 END) AS partially_prepaid_sales
"""


def backfill_payment_mode_breakdown_columns(
    brand_index: int, brand_name: str, target_dates: Iterable[str]
) -> None:
    """
    Focused backfill: recomputes and upserts ONLY the cod/prepaid/ppcod
    order-count and sales-amount columns on hour_wise_sales
    (number_of_cod_orders, number_of_prepaid_orders, number_of_ppcod_orders,
    cod_sales, prepaid_sales, partially_prepaid_sales) and overall_summary
    (cod_orders, prepaid_orders, partially_paid_orders, cod_sales,
    prepaid_sales, partially_prepaid_sales), computed straight from
    shopify_orders.

    Every other column on both tables (session counts, total_sales,
    ci_events, etc.) is left completely untouched - this intentionally does
    NOT go through the full stage-table/merge_staging_to_main_by_date
    rebuild the regular incremental functions use, and upserts via an
    explicit ON DUPLICATE KEY UPDATE column list instead.
    """
    placeholders, params = _dates_in_clause(target_dates)
    if not params:
        logger.debug(f"[{brand_name}] payment mode breakdown backfill: no dates to process")
        return

    with get_db_cursor(brand_index, dictionary=True) as (cursor, connection):
        hourly_sql = f"""
            SELECT
                STR_TO_DATE(created_date, '%Y-%m-%d') AS date,
                HOUR(created_time) AS hour,
                {_PAYMENT_MODE_BREAKDOWN_CASE_SQL}
            FROM shopify_orders
            WHERE created_date IN ({placeholders})
                AND created_time IS NOT NULL
            GROUP BY date, hour
        """
        cursor.execute(hourly_sql, params)
        hourly_rows = cursor.fetchall()

        if hourly_rows:
            cursor.executemany(
                """
                INSERT INTO hour_wise_sales (
                    date, hour, number_of_prepaid_orders, number_of_cod_orders,
                    number_of_ppcod_orders, cod_sales, prepaid_sales, partially_prepaid_sales
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    number_of_prepaid_orders = VALUES(number_of_prepaid_orders),
                    number_of_cod_orders = VALUES(number_of_cod_orders),
                    number_of_ppcod_orders = VALUES(number_of_ppcod_orders),
                    cod_sales = VALUES(cod_sales),
                    prepaid_sales = VALUES(prepaid_sales),
                    partially_prepaid_sales = VALUES(partially_prepaid_sales)
                """,
                [
                    (
                        row["date"],
                        row["hour"],
                        row["prepaid_orders"],
                        row["cod_orders"],
                        row["ppcod_orders"],
                        row["cod_sales"],
                        row["prepaid_sales"],
                        row["partially_prepaid_sales"],
                    )
                    for row in hourly_rows
                ],
            )
            connection.commit()
        logger.info(
            f"[{brand_name}] hour_wise_sales payment columns backfilled: {len(hourly_rows)} hour bucket(s)"
        )

        daily_sql = f"""
            SELECT
                STR_TO_DATE(created_date, '%Y-%m-%d') AS date,
                {_PAYMENT_MODE_BREAKDOWN_CASE_SQL}
            FROM shopify_orders
            WHERE created_date IN ({placeholders})
            GROUP BY date
        """
        cursor.execute(daily_sql, params)
        daily_rows = cursor.fetchall()

        if daily_rows:
            cursor.executemany(
                """
                INSERT INTO overall_summary (
                    date, prepaid_orders, cod_orders, partially_paid_orders,
                    cod_sales, prepaid_sales, partially_prepaid_sales
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    prepaid_orders = VALUES(prepaid_orders),
                    cod_orders = VALUES(cod_orders),
                    partially_paid_orders = VALUES(partially_paid_orders),
                    cod_sales = VALUES(cod_sales),
                    prepaid_sales = VALUES(prepaid_sales),
                    partially_prepaid_sales = VALUES(partially_prepaid_sales)
                """,
                [
                    (
                        row["date"],
                        row["prepaid_orders"],
                        row["cod_orders"],
                        row["ppcod_orders"],
                        row["cod_sales"],
                        row["prepaid_sales"],
                        row["partially_prepaid_sales"],
                    )
                    for row in daily_rows
                ],
            )
            connection.commit()
        logger.info(
            f"[{brand_name}] overall_summary payment columns backfilled: {len(daily_rows)} date(s)"
        )


def update_daily_citywise_summary_incremental(
    cursor, connection, brand_name: str, target_dates: Iterable[str]
):
    """Update daily_citywise_summary for only the specific affected dates."""
    placeholders, params = _dates_in_clause(target_dates)
    if not params:
        logger.debug(f"[{brand_name}] daily_citywise_summary: no dates to rebuild")
        return

    city_expr = _normalized_city_sql_expr("shipping_city")
    with timed(
        f"[{brand_name}] daily_citywise_summary incremental ({len(params)} date(s))"
    ):
        cursor.execute("DROP TABLE IF EXISTS daily_citywise_summary_stage")
        cursor.execute(
            "CREATE TABLE daily_citywise_summary_stage LIKE daily_citywise_summary"
        )

        sql = f"""
        INSERT INTO daily_citywise_summary_stage (
            date, city, total_orders, total_revenue, cod_orders, prepaid_orders, partially_paid_orders,
            sessions, atc_sessions
        )
        WITH OrderAgg AS (
            SELECT
                STR_TO_DATE(created_date, '%Y-%m-%d') AS date,
                {city_expr} AS city,
                COUNT(DISTINCT order_id) AS total_orders,
                SUM(COALESCE(total_price, 0)) AS total_revenue,
                COUNT(DISTINCT CASE
                    WHEN (payment_gateway_names IS NULL
                      OR payment_gateway_names = ''
                      OR payment_gateway_names LIKE '%Cash on Delivery (COD)%'
                      OR payment_gateway_names LIKE '%cash_on_delivery%')
                      AND (payment_gateway_names NOT LIKE '%Gokwik PPCOD%' OR payment_gateway_names IS NULL)
                    THEN order_id END) AS cod_orders,
                COUNT(DISTINCT CASE
                    WHEN payment_gateway_names IS NOT NULL
                      AND payment_gateway_names != ''
                      AND NOT (payment_gateway_names LIKE '%Cash on Delivery (COD)%'
                          OR payment_gateway_names LIKE '%cash_on_delivery%'
                          OR payment_gateway_names LIKE '%Gokwik PPCOD%')
                    THEN order_id END) AS prepaid_orders,
                COUNT(DISTINCT CASE
                    WHEN payment_gateway_names LIKE '%Gokwik PPCOD%'
                    THEN order_id END) AS partially_paid_orders
            FROM shopify_orders
            WHERE created_date IN ({placeholders})
            GROUP BY created_date, city
        ),
        SessionAgg AS (
            SELECT
                date,
                city,
                SUM(sessions) AS sessions,
                SUM(atc_sessions) AS atc_sessions
            FROM daily_city_sessions_summary_shopify
            WHERE date IN ({placeholders})
            GROUP BY date, city
        ),
        AllKeys AS (
            SELECT date, city FROM OrderAgg
            UNION
            SELECT date, city FROM SessionAgg
        )
        SELECT
            ak.date,
            ak.city,
            COALESCE(oa.total_orders, 0) AS total_orders,
            COALESCE(oa.total_revenue, 0) AS total_revenue,
            COALESCE(oa.cod_orders, 0) AS cod_orders,
            COALESCE(oa.prepaid_orders, 0) AS prepaid_orders,
            COALESCE(oa.partially_paid_orders, 0) AS partially_paid_orders,
            COALESCE(sa.sessions, 0) AS sessions,
            COALESCE(sa.atc_sessions, 0) AS atc_sessions
        FROM AllKeys ak
        LEFT JOIN OrderAgg oa ON ak.date = oa.date AND ak.city = oa.city
        LEFT JOIN SessionAgg sa ON ak.date = sa.date AND ak.city = sa.city
        """
        cursor.execute(sql, params + params)
        connection.commit()

        merge_staging_to_main_by_date(
            cursor,
            connection,
            "daily_citywise_summary",
            "daily_citywise_summary_stage",
            brand_name,
        )


def update_hourly_citywise_summary_incremental(
    cursor, connection, brand_name: str, target_dates: Iterable[str]
):
    """Update hourly_citywise_summary for only the specific affected dates."""
    placeholders, params = _dates_in_clause(target_dates)
    if not params:
        logger.debug(f"[{brand_name}] hourly_citywise_summary: no dates to rebuild")
        return

    city_expr = _normalized_city_sql_expr("shipping_city")
    with timed(
        f"[{brand_name}] hourly_citywise_summary incremental ({len(params)} date(s))"
    ):
        cursor.execute("DROP TABLE IF EXISTS hourly_citywise_summary_stage")
        cursor.execute(
            "CREATE TABLE hourly_citywise_summary_stage LIKE hourly_citywise_summary"
        )

        sql = f"""
        INSERT INTO hourly_citywise_summary_stage (
            date, hour, city, total_orders, total_revenue, cod_orders, prepaid_orders, partially_paid_orders,
            sessions, atc_sessions
        )
        WITH OrderAgg AS (
            SELECT
                STR_TO_DATE(created_date, '%Y-%m-%d') AS date,
                HOUR(created_time) AS hour,
                {city_expr} AS city,
                COUNT(DISTINCT order_id) AS total_orders,
                SUM(COALESCE(total_price, 0)) AS total_revenue,
                COUNT(DISTINCT CASE
                    WHEN (payment_gateway_names IS NULL
                      OR payment_gateway_names = ''
                      OR payment_gateway_names LIKE '%Cash on Delivery (COD)%'
                      OR payment_gateway_names LIKE '%cash_on_delivery%')
                      AND (payment_gateway_names NOT LIKE '%Gokwik PPCOD%' OR payment_gateway_names IS NULL)
                    THEN order_id END) AS cod_orders,
                COUNT(DISTINCT CASE
                    WHEN payment_gateway_names IS NOT NULL
                      AND payment_gateway_names != ''
                      AND NOT (payment_gateway_names LIKE '%Cash on Delivery (COD)%'
                          OR payment_gateway_names LIKE '%cash_on_delivery%'
                          OR payment_gateway_names LIKE '%Gokwik PPCOD%')
                    THEN order_id END) AS prepaid_orders,
                COUNT(DISTINCT CASE
                    WHEN payment_gateway_names LIKE '%Gokwik PPCOD%'
                    THEN order_id END) AS partially_paid_orders
            FROM shopify_orders
            WHERE created_date IN ({placeholders})
              AND created_time IS NOT NULL
            GROUP BY created_date, hour, city
        ),
        SessionAgg AS (
            SELECT
                date,
                hour,
                city,
                SUM(sessions) AS sessions,
                SUM(atc_sessions) AS atc_sessions
            FROM hourly_city_sessions_summary_shopify
            WHERE date IN ({placeholders})
            GROUP BY date, hour, city
        ),
        AllKeys AS (
            SELECT date, hour, city FROM OrderAgg
            UNION
            SELECT date, hour, city FROM SessionAgg
        )
        SELECT
            ak.date,
            ak.hour,
            ak.city,
            COALESCE(oa.total_orders, 0) AS total_orders,
            COALESCE(oa.total_revenue, 0) AS total_revenue,
            COALESCE(oa.cod_orders, 0) AS cod_orders,
            COALESCE(oa.prepaid_orders, 0) AS prepaid_orders,
            COALESCE(oa.partially_paid_orders, 0) AS partially_paid_orders,
            COALESCE(sa.sessions, 0) AS sessions,
            COALESCE(sa.atc_sessions, 0) AS atc_sessions
        FROM AllKeys ak
        LEFT JOIN OrderAgg oa
            ON ak.date = oa.date AND ak.hour = oa.hour AND ak.city = oa.city
        LEFT JOIN SessionAgg sa
            ON ak.date = sa.date AND ak.hour = sa.hour AND ak.city = sa.city
        """
        cursor.execute(sql, params + params)
        connection.commit()

        merge_staging_to_main_by_date(
            cursor,
            connection,
            "hourly_citywise_summary",
            "hourly_citywise_summary_stage",
            brand_name,
        )


def update_dashboard_discount_daily_incremental(
    cursor, connection, brand_name: str, target_dates: Iterable[str]
):
    """Update dashboard_discount_daily for only the specific affected dates."""
    placeholders, params = _dates_in_clause(target_dates)
    if not params:
        logger.debug(f"[{brand_name}] dashboard_discount_daily: no dates to rebuild")
        return

    with timed(
        f"[{brand_name}] dashboard_discount_daily incremental ({len(params)} date(s))"
    ):
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS dashboard_discount_daily_stage LIKE dashboard_discount_daily"
        )
        cursor.execute("TRUNCATE TABLE dashboard_discount_daily_stage")

        sql = f"""
        INSERT INTO dashboard_discount_daily_stage (
            date, discount_code, total_orders, gross_revenue, aov
        )
        SELECT
            created_date AS date,
            discount_code,
            COUNT(DISTINCT order_id) AS total_orders,
            SUM(COALESCE(total_price, 0)) AS gross_revenue,
            CASE
                WHEN COUNT(DISTINCT order_id) > 0
                THEN SUM(COALESCE(total_price, 0)) / COUNT(DISTINCT order_id)
                ELSE 0
            END AS aov
        FROM dashboard_order_discount_fact
        WHERE created_date IN ({placeholders})
        GROUP BY created_date, discount_code
        """
        cursor.execute(sql, params)
        connection.commit()

        merge_staging_to_main_by_date(
            cursor,
            connection,
            "dashboard_discount_daily",
            "dashboard_discount_daily_stage",
            brand_name,
        )


def update_dashboard_discount_hourly_incremental(
    cursor, connection, brand_name: str, target_dates: Iterable[str]
):
    """Update dashboard_discount_hourly for only the specific affected dates."""
    placeholders, params = _dates_in_clause(target_dates)
    if not params:
        logger.debug(f"[{brand_name}] dashboard_discount_hourly: no dates to rebuild")
        return

    with timed(
        f"[{brand_name}] dashboard_discount_hourly incremental ({len(params)} date(s))"
    ):
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS dashboard_discount_hourly_stage LIKE dashboard_discount_hourly"
        )
        cursor.execute("TRUNCATE TABLE dashboard_discount_hourly_stage")

        sql = f"""
        INSERT INTO dashboard_discount_hourly_stage (
            date, hour, discount_code, total_orders, gross_revenue, aov
        )
        SELECT
            created_date AS date,
            created_hour AS hour,
            discount_code,
            COUNT(DISTINCT order_id) AS total_orders,
            SUM(COALESCE(total_price, 0)) AS gross_revenue,
            CASE
                WHEN COUNT(DISTINCT order_id) > 0
                THEN SUM(COALESCE(total_price, 0)) / COUNT(DISTINCT order_id)
                ELSE 0
            END AS aov
        FROM dashboard_order_discount_fact
        WHERE created_date IN ({placeholders})
        GROUP BY created_date, created_hour, discount_code
        """
        cursor.execute(sql, params)
        connection.commit()

        merge_staging_to_main_by_date(
            cursor,
            connection,
            "dashboard_discount_hourly",
            "dashboard_discount_hourly_stage",
            brand_name,
        )


def update_dashboard_discount_payment_daily_incremental(
    cursor, connection, brand_name: str, target_dates: Iterable[str]
):
    """Update dashboard_discount_payment_daily for only the specific affected dates."""
    placeholders, params = _dates_in_clause(target_dates)
    if not params:
        logger.debug(
            f"[{brand_name}] dashboard_discount_payment_daily: no dates to rebuild"
        )
        return

    with timed(
        f"[{brand_name}] dashboard_discount_payment_daily incremental ({len(params)} date(s))"
    ):
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS dashboard_discount_payment_daily_stage LIKE dashboard_discount_payment_daily"
        )
        cursor.execute("TRUNCATE TABLE dashboard_discount_payment_daily_stage")

        sql = f"""
        INSERT INTO dashboard_discount_payment_daily_stage (
            date, discount_code, payment_mode, total_orders, gross_revenue
        )
        SELECT
            created_date AS date,
            discount_code,
            payment_mode,
            COUNT(DISTINCT order_id) AS total_orders,
            SUM(COALESCE(total_price, 0)) AS gross_revenue
        FROM dashboard_order_discount_fact
        WHERE created_date IN ({placeholders})
        GROUP BY created_date, discount_code, payment_mode
        """
        cursor.execute(sql, params)
        connection.commit()

        merge_staging_to_main_by_date(
            cursor,
            connection,
            "dashboard_discount_payment_daily",
            "dashboard_discount_payment_daily_stage",
            brand_name,
        )


def update_dashboard_discount_payment_hourly_incremental(
    cursor, connection, brand_name: str, target_dates: Iterable[str]
):
    """Update dashboard_discount_payment_hourly for only the specific affected dates."""
    placeholders, params = _dates_in_clause(target_dates)
    if not params:
        logger.debug(
            f"[{brand_name}] dashboard_discount_payment_hourly: no dates to rebuild"
        )
        return

    with timed(
        f"[{brand_name}] dashboard_discount_payment_hourly incremental ({len(params)} date(s))"
    ):
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS dashboard_discount_payment_hourly_stage LIKE dashboard_discount_payment_hourly"
        )
        cursor.execute("TRUNCATE TABLE dashboard_discount_payment_hourly_stage")

        sql = f"""
        INSERT INTO dashboard_discount_payment_hourly_stage (
            date, hour, discount_code, payment_mode, total_orders, gross_revenue, aov
        )
        SELECT
            created_date AS date,
            created_hour AS hour,
            discount_code,
            payment_mode,
            COUNT(DISTINCT order_id) AS total_orders,
            SUM(COALESCE(total_price, 0)) AS gross_revenue,
            CASE
                WHEN COUNT(DISTINCT order_id) > 0
                THEN SUM(COALESCE(total_price, 0)) / COUNT(DISTINCT order_id)
                ELSE 0
            END AS aov
        FROM dashboard_order_discount_fact
        WHERE created_date IN ({placeholders})
        GROUP BY created_date, created_hour, discount_code, payment_mode
        """
        cursor.execute(sql, params)
        connection.commit()

        merge_staging_to_main_by_date(
            cursor,
            connection,
            "dashboard_discount_payment_hourly",
            "dashboard_discount_payment_hourly_stage",
            brand_name,
        )


def update_overall_summary_incremental(
    brand_index: int,
    cursor,
    connection,
    brand_key: Optional[str],
    brand_name: str,
    target_dates: Iterable[str],
):
    """Update overall_summary for only the specific affected dates."""
    placeholders, params = _dates_in_clause(target_dates)
    if not params:
        logger.debug(f"[{brand_name}] overall_summary: no dates to rebuild")
        return
    with timed(
        f"[{brand_name}] overall_summary incremental ({len(params)} date(s))"
    ):
        # Recreate the stage table so its schema always matches overall_summary.
        cursor.execute("DROP TABLE IF EXISTS overall_summary_stage")
        cursor.execute("CREATE TABLE overall_summary_stage LIKE overall_summary")

        # STEP 3: Populate staging table
        sql = f"""
        INSERT INTO overall_summary_stage (
            date, gross_sales, total_discount_amount, total_sales, net_sales,
            total_orders, cod_orders, prepaid_orders, partially_paid_orders,
            cod_sales, prepaid_sales, partially_prepaid_sales,
            total_sessions, total_atc_sessions, adjusted_total_sessions
        )
        SELECT
            s.date,
            COALESCE(gs.gross_sales, 0)           AS gross_sales,
            COALESCE(ds.actual_discounts, 0)      AS total_discount_amount,
            COALESCE(s.actual_overall_sales, 0)   AS total_sales,
            COALESCE(gs.net_sales, 0)             AS net_sales,
            COALESCE(o.number_of_orders_created, 0)       AS total_orders,
            COALESCE(o.overall_cod_orders, 0)             AS cod_orders,
            COALESCE(o.overall_prepaid_orders, 0)         AS prepaid_orders,
            COALESCE(o.overall_partially_paid_orders, 0)  AS partially_paid_orders,
            COALESCE(o.overall_cod_sales, 0)              AS cod_sales,
            COALESCE(o.overall_prepaid_sales, 0)          AS prepaid_sales,
            COALESCE(o.overall_partially_paid_sales, 0)   AS partially_prepaid_sales,
            COALESCE(sess.number_of_sessions, 0)          AS total_sessions,
            COALESCE(sess.number_of_atc_sessions, 0)      AS total_atc_sessions,
            ROUND(
                COALESCE(sess.number_of_sessions, 0) * (
                    1 + COALESCE(
                        (
                            SELECT sab.offset_pct / 100.0
                            FROM master.session_adjustment_buckets sab
                            WHERE sab.brand_key = %s
                              AND sab.active = 1
                              AND s.date BETWEEN sab.effective_from AND sab.effective_to
                              AND COALESCE(sess.number_of_sessions, 0)
                                    BETWEEN sab.lower_bound_sessions AND sab.upper_bound_sessions
                            ORDER BY sab.priority ASC, sab.id DESC
                            LIMIT 1
                        ),
                        0
                    )
                )
            ) AS adjusted_total_sessions
        FROM sales_summary s
        LEFT JOIN order_summary      o    ON s.date = o.date
        LEFT JOIN sessions_summary   sess ON s.date = sess.date
        LEFT JOIN gross_summary      gs   ON s.date = gs.date
        LEFT JOIN discount_summary   ds   ON s.date = ds.date
        WHERE s.date IN ({placeholders})
        """
        cursor.execute(sql, (brand_key,) + params)
        connection.commit()

        # STEP 4 & 5: Fetch affected dates and merge
        merge_staging_to_main_by_date(
            cursor, connection, "overall_summary", "overall_summary_stage", brand_name
        )

        parsed_dates = sorted(date.fromisoformat(str(d)) for d in params)
        aws_background.update_overall_summary_event_columns(
            brand_index=brand_index,
            brand_name=brand_name,
            brand_key=brand_key,
            start_date=parsed_dates[0],
            end_date=parsed_dates[-1],
            cursor=cursor,
            connection=connection,
        )


def update_rto_summary_incremental(
    cursor, connection, brand_index: int, brand_name: str, target_dates: Iterable[str]
):
    """Update rto_summary for only the specific affected dates."""
    placeholders, params = _dates_in_clause(target_dates)
    if not params:
        logger.debug(f"[{brand_name}] rto_summary: no dates to rebuild")
        return

    with timed(f"[{brand_name}] rto_summary incremental ({len(params)} date(s))"):
        cursor.execute("CREATE TABLE IF NOT EXISTS rto_summary_stage LIKE rto_summary")
        cursor.execute("TRUNCATE TABLE rto_summary_stage")

        sql = f"""
        INSERT INTO rto_summary_stage (
            summary_date, total_orders, rto_orders, rto_percentage
        )
        SELECT
            d.summary_date,
            COALESCE(t.total_orders, 0) AS total_orders,
            COALESCE(r.rto_orders, 0) AS rto_orders,
            ROUND(
                COALESCE(
                    COALESCE(r.rto_orders, 0) * 100.0 / NULLIF(COALESCE(t.total_orders, 0), 0),
                    0
                ),
                2
            ) AS rto_percentage
        FROM (
            SELECT STR_TO_DATE(created_date, '%Y-%m-%d') AS summary_date
            FROM shopify_orders
            WHERE created_date IN ({placeholders})
            GROUP BY created_date
        ) AS d
        LEFT JOIN (
            SELECT
                STR_TO_DATE(s.created_date, '%Y-%m-%d') AS summary_date,
                COUNT(DISTINCT s.order_id) AS total_orders
            FROM (
                SELECT
                    order_id,
                    created_date
                FROM shopify_orders
                WHERE created_date IN ({placeholders})
                GROUP BY order_id, created_date
            ) AS s
            GROUP BY s.created_date
        ) AS t
            ON d.summary_date = t.summary_date
        LEFT JOIN (
            SELECT
                order_created_date AS summary_date,
                COUNT(DISTINCT order_id) AS rto_orders
            FROM returns_fact
            WHERE event_type = 'CANCEL (RTO)'
              AND order_created_date IN ({placeholders})
            GROUP BY order_created_date
        ) AS r
            ON d.summary_date = r.summary_date
        WHERE d.summary_date IS NOT NULL
        """

        cursor.execute(sql, params + params + params)
        connection.commit()

        merge_staging_to_main_by_date(
            cursor,
            connection,
            "rto_summary",
            "rto_summary_stage",
            brand_name,
            date_column="summary_date",
        )


def update_shopify_orders_utm_daily_incremental(
    cursor, connection, brand_name: str, target_dates: Iterable[str]
):
    """Update shopify_orders_utm_daily for only the specific affected dates.

    Note: the OrderAgg CTE buckets by DATE(created_at) (server tz). To keep
    that bucketing consistent with the filter, the WHERE uses OR'd per-date
    created_at ranges rather than created_date — this preserves the original
    UTC-day semantics exactly and stays index-friendly on created_at.
    """
    placeholders, params = _dates_in_clause(target_dates)
    if not params:
        logger.debug(f"[{brand_name}] shopify_orders_utm_daily: no dates to rebuild")
        return
    # Per-date created_at ranges, OR'd together, to match DATE(created_at) bucketing.
    order_range_clauses = " OR ".join(
        ["(created_at >= %s AND created_at < DATE_ADD(%s, INTERVAL 1 DAY))"] * len(params)
    )
    order_range_params = tuple(p for d in params for p in (d, d))
    with timed(
        f"[{brand_name}] shopify_orders_utm_daily incremental ({len(params)} date(s))"
    ):
        sql = f"""
        INSERT INTO shopify_orders_utm_daily (
            date, utm_source, utm_medium, utm_campaign, utm_content, utm_term,
            total_orders, total_sales, total_discounts, shipping_total, tax_total, net_sales, aov,
            utm_key, number_of_sessions, number_of_atc_sessions
        )
        WITH OrderAgg AS (
            SELECT 
                DATE(created_at) AS `date`,
                IF(IFNULL(utm_source, '')='', '(none)', utm_source) AS utm_source,
                IF(IFNULL(utm_medium, '')='', '(none)', utm_medium) AS utm_medium,
                IF(IFNULL(utm_campaign, '')='', '(none)', utm_campaign) AS utm_campaign,
                IF(IFNULL(utm_content, '')='', '(none)', utm_content) AS utm_content,
                IF(IFNULL(utm_term, '')='', '(none)', utm_term) AS utm_term,
                COUNT(order_id) AS total_orders,
                SUM(total_price) AS total_sales,
                SUM(total_discounts) AS total_discounts,
                SUM(IFNULL(shipping_price, 0)) AS shipping_total,
                SUM(total_tax) AS tax_total,
                0 AS number_of_sessions,
                0 AS number_of_atc_sessions
            FROM shopify_orders
            WHERE {order_range_clauses}
            GROUP BY 1, 2, 3, 4, 5, 6
        ),
        SessionAgg AS (
            SELECT
                `date`,
                IF(IFNULL(utm_source, '')='', '(none)', utm_source) AS utm_source,
                IF(IFNULL(utm_medium, '')='', '(none)', utm_medium) AS utm_medium,
                IF(IFNULL(utm_campaign, '')='', '(none)', utm_campaign) AS utm_campaign,
                IF(IFNULL(utm_content, '')='', '(none)', utm_content) AS utm_content,
                IF(IFNULL(utm_term, '')='', '(none)', utm_term) AS utm_term,
                0 AS total_orders,
                0.00 AS total_sales,
                0.00 AS total_discounts,
                0.00 AS shipping_total,
                0.00 AS tax_total,
                SUM(sessions) AS number_of_sessions,
                SUM(sessions_with_cart_additions) AS number_of_atc_sessions
            FROM product_sessions_snapshot
            WHERE `date` IN ({placeholders})
            GROUP BY 1, 2, 3, 4, 5, 6
        ),
        Combined AS (
            SELECT * FROM OrderAgg
            UNION ALL
            SELECT * FROM SessionAgg
        )
        SELECT 
            `date`, utm_source, utm_medium, utm_campaign, utm_content, utm_term,
            SUM(total_orders) AS total_orders,
            SUM(total_sales) AS total_sales,
            SUM(total_discounts) AS total_discounts,
            SUM(shipping_total) AS shipping_total,
            SUM(tax_total) AS tax_total,
            IFNULL((SUM(total_sales) * 0.84) - SUM(total_discounts), 0.00) AS net_sales,
            IF(SUM(total_orders) > 0, IFNULL((SUM(total_sales) * 0.84) - SUM(total_discounts), 0.00) / SUM(total_orders), 0.00) AS aov,
            MD5(CONCAT_WS('|', `date`, utm_source, utm_medium, utm_campaign, utm_content, utm_term)) AS utm_key,
            SUM(number_of_sessions) AS number_of_sessions,
            SUM(number_of_atc_sessions) AS number_of_atc_sessions
        FROM Combined
        GROUP BY 1, 2, 3, 4, 5, 6
        ON DUPLICATE KEY UPDATE
            total_orders = VALUES(total_orders),
            total_sales = VALUES(total_sales),
            total_discounts = VALUES(total_discounts),
            shipping_total = VALUES(shipping_total),
            tax_total = VALUES(tax_total),
            net_sales = VALUES(net_sales),
            aov = VALUES(aov),
            number_of_sessions = VALUES(number_of_sessions),
            number_of_atc_sessions = VALUES(number_of_atc_sessions);
        """
        cursor.execute(sql, order_range_params + params)
        connection.commit()


# ---------------------------
# NEW: Publish overall_summary rows to QStash for alerts
# ---------------------------
def push_overall_summary_events_to_qstash(
    brand_index: int,
    brand_name: str,
    brand_key: Optional[str],
) -> None:
    """
    Read hour_wise_sales for *today up to the current hour (inclusive)* and publish
    one cumulative event via QStash to the alerting system.

    Event shape (unchanged):
      {
        brand_id,
        brand,
        total_sales,
        total_orders,
        aov,
        total_sessions,
        total_atc_sessions,
        gross_sales,
      }

    - Metrics come from hour_wise_sales aggregated over hour <= current_hour.
    - total_sessions uses SUM(adjusted_number_of_sessions) if present, else SUM(number_of_sessions).
    - brand_id is taken from BRAND_ID_{i} env, else falls back to brand_index.
    - brand is BRAND_TAG_{i} (brand_key) if available, else BRAND_NAME_{i}.
    """
    client = get_qstash_client()
    if client is None:
        return  # alerts disabled, nothing to do

    if not ALERTS_WEBHOOK_URL:
        logger.info("ALERTS_WEBHOOK_URL not set; skipping QStash alerts.")
        return

    now = aws_background.now_ist()
    today_str = now.date().isoformat()
    current_hour = now.hour
    end_hour = current_hour - 1

    # Aggregate all hours up to current_hour for today
    with get_db_cursor(brand_index) as (cursor, _connection):
        cursor.execute(
            """
            SELECT
                date,
                SUM(number_of_orders)              AS number_of_orders,
                SUM(total_sales)                   AS total_sales,
                SUM(number_of_prepaid_orders)      AS number_of_prepaid_orders,
                SUM(number_of_cod_orders)          AS number_of_cod_orders,
                SUM(number_of_sessions)            AS number_of_sessions,
                SUM(number_of_atc_sessions)        AS number_of_atc_sessions,
                SUM(
                    CASE
                        WHEN adjusted_number_of_sessions IS NULL THEN 0
                        ELSE adjusted_number_of_sessions
                    END
                ) AS adjusted_number_of_sessions
            FROM hour_wise_sales
            WHERE date = %s
              AND hour <= %s
            GROUP BY date
            """,
            (today_str, end_hour),
        )
        rows = cursor.fetchall()

    if not rows:
        logger.info(
            f"No cumulative hour_wise_sales rows found for {brand_name} on "
            f"{today_str} up to hour={end_hour}; skipping QStash alert event."
        )
        return

    brand_id = brand_id_from_config.get(brand_index, brand_index)

    brand_label = brand_key or brand_name or f"Brand_{brand_index}"

    pushed = 0
    for row in rows:
        # row is dict because get_db_cursor dictionary=True by default
        total_sales = float(row.get("total_sales") or 0.0)
        total_orders = (
            int(row.get("number_of_orders") or 0)
            if row.get("number_of_orders") is not None
            else 0
        )
        gross_sales = total_sales  # cumulative gross = cumulative total_sales

        raw_sessions = (
            int(row.get("number_of_sessions") or 0)
            if row.get("number_of_sessions") is not None
            else 0
        )
        adjusted_sessions_val = row.get("adjusted_number_of_sessions")
        # If any adjusted sessions were stored, use their sum; otherwise fall back to raw_sessions
        if adjusted_sessions_val is not None and float(adjusted_sessions_val) > 0:
            try:
                total_sessions = int(adjusted_sessions_val)
            except Exception:
                total_sessions = raw_sessions
        else:
            total_sessions = raw_sessions

        total_atc_sessions = (
            int(row.get("number_of_atc_sessions") or 0)
            if row.get("number_of_atc_sessions") is not None
            else 0
        )

        aov = float(total_sales / total_orders) if total_orders > 0 else 0.0

        event = {
            "brand_id": brand_id,
            "brand": brand_label,
            "total_sales": total_sales,
            "total_orders": total_orders,
            "aov": aov,
            "total_sessions": total_sessions,
            "total_atc_sessions": total_atc_sessions,
            "gross_sales": gross_sales,
        }

        try:
            res = client.message.publish_json(
                url=ALERTS_WEBHOOK_URL,
                body=event,
                headers={
                    "Content-Type": "application/json",
                    "X-Brand": brand_label,
                },
            )
            pushed += 1
            try:
                msg_id = getattr(res, "message_id", None)
            except Exception:
                msg_id = None
            if msg_id:
                logger.debug(
                    f"QStash message_id={msg_id} for {brand_label} on {row.get('date')} "
                    f"(cumulative up to hour={current_hour})"
                )
        except Exception as e:
            logger.error(
                f"❌ Failed to publish QStash event for {brand_label} on "
                f"{row.get('date')} (cumulative up to hour={current_hour}): {e}"
            )

    if pushed:
        logger.info(
            f"📤 Published {pushed} cumulative (up to hour={current_hour}) events to QStash "
            f"for {brand_label} (date {today_str})"
        )


# ---------------------------
# NEW: Publish daily web vitals averages to QStash for alerts
# ---------------------------
def push_web_vitals_metrics_to_qstash(
    brand_index: int,
    brand_name: str,
    brand_key: Optional[str],
    cursor=None,
    connection=None,
    tz: ZoneInfo = IST,
) -> None:
    """
    Average today's per-page daily_web_vitals_summary rows for this brand into
    one brand-wide reading and publish it to QStash via ALERTS_WEBHOOK_URL.

    Event shape:
      {
        brand_key, brand_name,
        performance, cls, fcp, inp, lcp, ttfb,
        date, time,
      }
    """
    effective_brand_key = (brand_key or "").strip()
    if not effective_brand_key:
        return

    client = get_qstash_client()
    if client is None:
        return
    if not ALERTS_WEBHOOK_URL:
        return

    now = datetime.now(tz)
    date_str = now.date().isoformat()
    time_str = now.strftime("%H:%M:%S")

    query = """
        SELECT
            AVG(avg_performance) AS performance,
            AVG(avg_cls) AS cls,
            AVG(avg_fcp) AS fcp,
            AVG(avg_inp) AS inp,
            AVG(avg_lcp) AS lcp,
            AVG(avg_ttfb) AS ttfb
        FROM daily_web_vitals_summary
        WHERE brand_key = %s AND `date` = %s
    """
    if cursor is not None:
        cursor.execute(query, (effective_brand_key, date_str))
        row = cursor.fetchone()
    else:
        with get_db_cursor(brand_index) as (c, _conn):
            c.execute(query, (effective_brand_key, date_str))
            row = c.fetchone()

    if not row or row.get("performance") is None:
        logger.info(
            f"No daily_web_vitals_summary rows for {brand_name} on {date_str}; "
            "skipping web vitals QStash event."
        )
        return

    event = {
        "brand_key": effective_brand_key,
        "brand_name": brand_name,
        "performance": round(float(row["performance"])),
        "cls": round(float(row["cls"]), 3) if row["cls"] is not None else 0,
        "fcp": round(float(row["fcp"]), 2) if row["fcp"] is not None else 0,
        "inp": round(float(row["inp"]), 2) if row["inp"] is not None else 0,
        "lcp": round(float(row["lcp"]), 2) if row["lcp"] is not None else 0,
        "ttfb": round(float(row["ttfb"]), 2) if row["ttfb"] is not None else 0,
        "date": date_str,
        "time": time_str,
    }

    try:
        client.message.publish_json(
            url=ALERTS_WEBHOOK_URL,
            body=event,
            headers={"Content-Type": "application/json", "X-Brand": effective_brand_key},
        )
        logger.info(
            f"📤 Published web vitals event to QStash for {brand_name} (date {date_str})"
        )
    except Exception as e:
        logger.error(f"❌ Failed to publish web vitals QStash event for {brand_name}: {e}")


def execute_summary_queries_incremental(
    brand_index: int,
    brand_name: str,
    brand_key: Optional[str],
    target_dates: Iterable[str],
    rto_target_dates: Optional[Iterable[str]] = None,
):
    """Rebuild each summary table for only the dates in target_dates.

    Each summary function filters its source reads by `date IN (…)` rather
    than `date BETWEEN min AND max`, so a sparse set (e.g. {today, 2016-04-08})
    only touches those exact dates — not everything in between.
    """
    dates_set = {d for d in target_dates if d}
    rto_dates_set = {d for d in (rto_target_dates or []) if d}
    if not dates_set and not rto_dates_set:
        logger.info(f"✔️ No dates to update summaries for {brand_name}")
        return

    ordered = sorted(dates_set)
    rto_ordered = sorted(rto_dates_set)
    if ordered:
        range_label = (
            f"{len(ordered)} date(s): {ordered[0]}..{ordered[-1]}"
            if len(ordered) > 1
            else f"1 date: {ordered[0]}"
        )
    else:
        range_label = "0 dates"

    try:
        with get_db_cursor(brand_index, dictionary=False) as (cursor, connection):
            cursor.execute("SET SESSION autocommit=1")
            logger.info(f"📊 Updating summaries for {brand_name}: {range_label}")

            # Ensure all tables exist
            ensure_summary_tables(cursor, connection)

            # Update each summary incrementally
            if dates_set:
                update_sales_summary_incremental(
                    cursor, connection, brand_index, brand_name, dates_set
                )
                update_order_summary_incremental(
                    cursor, connection, brand_index, brand_name, dates_set
                )
                update_discount_summary_incremental(
                    cursor, connection, brand_index, brand_name, dates_set
                )
                update_gross_summary_incremental(
                    cursor, connection, brand_name, dates_set
                )
                update_hour_wise_sales_incremental(
                    cursor, connection, brand_name, dates_set
                )
                update_daily_citywise_summary_incremental(
                    cursor, connection, brand_name, dates_set
                )
                update_hourly_citywise_summary_incremental(
                    cursor, connection, brand_name, dates_set
                )
                update_dashboard_discount_daily_incremental(
                    cursor, connection, brand_name, dates_set
                )
                update_dashboard_discount_hourly_incremental(
                    cursor, connection, brand_name, dates_set
                )
                update_dashboard_discount_payment_daily_incremental(
                    cursor, connection, brand_name, dates_set
                )
                update_dashboard_discount_payment_hourly_incremental(
                    cursor, connection, brand_name, dates_set
                )
                update_overall_summary_incremental(
                    brand_index, cursor, connection, brand_key, brand_name, dates_set
                )
                update_shopify_orders_utm_daily_incremental(
                    cursor, connection, brand_name, dates_set
                )

            update_rto_summary_incremental(
                cursor, connection, brand_index, brand_name, rto_dates_set or dates_set
            )

            logger.info(
                f"✅ Incremental summaries updated for {brand_name} ({range_label})"
            )

        # Now that overall_summary is updated, publish events via QStash
        try:
            push_overall_summary_events_to_qstash(
                brand_index, brand_name, brand_key
            )
        except Exception as e:
            logger.error(
                f"❌ Error while publishing overall_summary events to QStash for {brand_name}: {e}"
            )

    except Exception as e:
        logger.error(f"❌ Error executing incremental summaries for {brand_name}: {e}")
        traceback.print_exc()


def execute_discount_dashboard_backfill_only(brand_index: int, brand_name: str):
    """Backfill only discount dashboard fact and aggregates from shopify_orders."""
    if not aws_background.DISCOUNT_DASHBOARD_BACKFILL:
        logger.info(
            f"✔️ Discount dashboard backfill disabled for {brand_name}; nothing to do"
        )
        return

    logger.info(f"🔁 Starting DISCOUNT-ONLY dashboard backfill for {brand_name}")
    try:
        with get_db_cursor(brand_index, dictionary=True) as (cursor, connection):
            ensure_summary_tables(cursor, connection)
            affected_dates = backfill_dashboard_order_discount_fact_from_shopify_orders(
                cursor, connection, brand_name
            )

            if not affected_dates:
                logger.info(
                    f"✔️ Discount-only backfill found no discount rows for {brand_name}"
                )
                return

            ordered = sorted(affected_dates)
            logger.info(
                f"📊 Rebuilding discount dashboard aggregates for {brand_name}: "
                f"{len(ordered)} date(s), {ordered[0]}..{ordered[-1]}"
            )
            update_dashboard_discount_daily_incremental(
                cursor, connection, brand_name, affected_dates
            )
            update_dashboard_discount_hourly_incremental(
                cursor, connection, brand_name, affected_dates
            )
            update_dashboard_discount_payment_daily_incremental(
                cursor, connection, brand_name, affected_dates
            )
            update_dashboard_discount_payment_hourly_incremental(
                cursor, connection, brand_name, affected_dates
            )

        logger.info(f"✅ Discount-only dashboard backfill complete for {brand_name}")
    except Exception as e:
        logger.error(f"❌ Discount-only dashboard backfill failed for {brand_name}: {e}")
        traceback.print_exc()


