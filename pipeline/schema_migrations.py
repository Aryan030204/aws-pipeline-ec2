"""
Schema migration helpers: idempotent DDL to create/extend the summary
tables used by the pipeline. Each ensure_* function is safe to call
repeatedly (it either creates a table with IF NOT EXISTS or attempts an
ALTER TABLE and swallows the duplicate-column error).
"""

import os
from typing import Optional

import mysql.connector
from sqlalchemy import text

from pipeline.state import logger, sqlalchemy_engines


def ensure_user_agent_column(brand_index: int, table_name: str):
    """
    Ensure user_agent column exists in the specific table.
    Run this in a separate transaction BEFORE opening long-lived cursors to avoid metadata locks.
    """
    engine = sqlalchemy_engines.get(brand_index)
    if not engine:
        return

    try:
        with engine.connect() as conn:
            # Check/Add user_agent column
            # We blindly attempt ADD COLUMN; if it exists, it fails safely or throws error we catch.
            try:
                conn.execute(
                    text(f"ALTER TABLE {table_name} ADD COLUMN user_agent TEXT")
                )
                conn.commit()
            except Exception as e:
                # If it fails, likely because it exists.
                if "Duplicate column name" in str(e):
                    pass
                else:
                    pass
    except Exception:
        pass


def ensure_customer_tag_column(brand_index: int, table_name: str):
    """
    Ensure customer_tag exists in the specific table.
    """
    engine = sqlalchemy_engines.get(brand_index)
    if not engine:
        return

    try:
        with engine.connect() as conn:
            try:
                conn.execute(
                    text(f"ALTER TABLE {table_name} ADD COLUMN customer_tag TEXT")
                )
                conn.commit()
            except Exception as e:
                if "Duplicate column name" in str(e):
                    pass
    except Exception:
        pass


def ensure_bundle_property_columns(brand_index: int, table_name: str):
    """
    Ensure line_item_properties_json exists in the specific table so bundle
    rollups can be rebuilt directly from shopify_orders rows.
    """
    engine = sqlalchemy_engines.get(brand_index)
    if not engine:
        return

    try:
        with engine.connect() as conn:
            try:
                conn.execute(
                    text(
                        f"ALTER TABLE {table_name} ADD COLUMN line_item_properties_json TEXT"
                    )
                )
                conn.commit()
            except Exception as e:
                if "Duplicate column name" in str(e):
                    pass
    except Exception:
        pass


def ensure_device_summary_columns(brand_index: int, table_name: str):
    """
    Ensure device-wise session and ATC columns exist in the specific table.
    """
    engine = sqlalchemy_engines.get(brand_index)
    if not engine:
        return

    columns = [
        "desktop_sessions",
        "desktop_atc_sessions",
        "mobile_sessions",
        "mobile_atc_sessions",
        "tablet_sessions",
        "tablet_atc_sessions",
        "other_sessions",
        "other_atc_sessions",
    ]

    try:
        with engine.connect() as conn:
            for col in columns:
                try:
                    conn.execute(
                        text(f"ALTER TABLE {table_name} ADD COLUMN {col} INT DEFAULT 0")
                    )
                    conn.commit()
                except Exception as e:
                    if "Duplicate column name" in str(e):
                        pass
                    else:
                        pass
    except Exception:
        pass


def ensure_utm_names_column(brand_index: int, table_name: str = "overall_utm_summary"):
    """
    Ensure 'utm_names' JSON column exists in existing table.
    """
    engine = sqlalchemy_engines.get(brand_index)
    if not engine:
        return

    try:
        with engine.connect() as conn:
            try:
                conn.execute(
                    text(f"ALTER TABLE {table_name} ADD COLUMN utm_names JSON")
                )
                conn.commit()
            except Exception as e:
                if "Duplicate column name" in str(e):
                    pass
                else:
                    pass
    except Exception:
        pass


def ensure_ci_events_column(
    brand_index: int, table_name: str = "hourly_sessions_summary_shopify"
):
    """
    Ensure ci_events exists in the specific table.
    """
    engine = sqlalchemy_engines.get(brand_index)
    if not engine:
        return

    try:
        with engine.connect() as conn:
            try:
                conn.execute(
                    text(f"ALTER TABLE {table_name} ADD COLUMN ci_events INT DEFAULT 0")
                )
                conn.commit()
            except Exception as e:
                if "Duplicate column name" in str(e):
                    pass
    except Exception:
        pass


def ensure_buy_now_events_column(
    brand_index: int, table_name: str = "hourly_sessions_summary_shopify"
):
    """
    Ensure buy_now_events exists in the specific table.
    """
    engine = sqlalchemy_engines.get(brand_index)
    if not engine:
        return

    try:
        with engine.connect() as conn:
            try:
                conn.execute(
                    text(
                        f"ALTER TABLE {table_name} ADD COLUMN buy_now_events INT DEFAULT 0"
                    )
                )
                conn.commit()
            except Exception as e:
                if "Duplicate column name" in str(e):
                    pass
    except Exception:
        pass


def ensure_overall_summary_event_columns(brand_index: int) -> None:
    """
    Ensure ci_events and buy_now_events exist in overall_summary.
    """
    engine = sqlalchemy_engines.get(brand_index)
    if not engine:
        return

    statements = (
        "ALTER TABLE overall_summary ADD COLUMN ci_events INT DEFAULT 0",
        "ALTER TABLE overall_summary ADD COLUMN buy_now_events INT DEFAULT 0",
    )
    try:
        with engine.connect() as conn:
            for sql in statements:
                try:
                    conn.execute(text(sql))
                    conn.commit()
                except Exception as e:
                    if "Duplicate column name" in str(e):
                        continue
    except Exception:
        pass


def ensure_hour_wise_ppcod_column(brand_index: int) -> None:
    """
    Ensure number_of_ppcod_orders exists in hour_wise_sales. PPCOD
    (Gokwik "Prepaid Partial COD") orders are excluded from BOTH
    number_of_prepaid_orders and number_of_cod_orders today, so this is a
    genuinely new bucket, not a split out of an existing one.
    """
    engine = sqlalchemy_engines.get(brand_index)
    if not engine:
        return

    try:
        with engine.connect() as conn:
            try:
                conn.execute(
                    text(
                        "ALTER TABLE hour_wise_sales ADD COLUMN number_of_ppcod_orders INT DEFAULT 0"
                    )
                )
                conn.commit()
            except Exception as e:
                if "Duplicate column name" in str(e):
                    pass
    except Exception:
        pass


def ensure_hour_wise_payment_sales_columns(brand_index: int) -> None:
    """
    Ensure cod_sales, prepaid_sales, partially_prepaid_sales exist in
    hour_wise_sales - the sales-amount counterparts of the existing
    number_of_cod_orders/number_of_prepaid_orders/number_of_ppcod_orders
    order-count columns.
    """
    engine = sqlalchemy_engines.get(brand_index)
    if not engine:
        return

    statements = (
        "ALTER TABLE hour_wise_sales ADD COLUMN cod_sales DECIMAL(12,2) DEFAULT 0",
        "ALTER TABLE hour_wise_sales ADD COLUMN prepaid_sales DECIMAL(12,2) DEFAULT 0",
        "ALTER TABLE hour_wise_sales ADD COLUMN partially_prepaid_sales DECIMAL(12,2) DEFAULT 0",
    )
    try:
        with engine.connect() as conn:
            for sql in statements:
                try:
                    conn.execute(text(sql))
                    conn.commit()
                except Exception as e:
                    if "Duplicate column name" in str(e):
                        continue
    except Exception:
        pass


def ensure_overall_summary_payment_sales_columns(brand_index: int) -> None:
    """
    Ensure cod_sales, prepaid_sales, partially_prepaid_sales exist in
    overall_summary - the sales-amount counterparts of the existing
    cod_orders/prepaid_orders/partially_paid_orders order-count columns.
    """
    engine = sqlalchemy_engines.get(brand_index)
    if not engine:
        return

    statements = (
        "ALTER TABLE overall_summary ADD COLUMN cod_sales DECIMAL(12,2) DEFAULT 0",
        "ALTER TABLE overall_summary ADD COLUMN prepaid_sales DECIMAL(12,2) DEFAULT 0",
        "ALTER TABLE overall_summary ADD COLUMN partially_prepaid_sales DECIMAL(12,2) DEFAULT 0",
    )
    try:
        with engine.connect() as conn:
            for sql in statements:
                try:
                    conn.execute(text(sql))
                    conn.commit()
                except Exception as e:
                    if "Duplicate column name" in str(e):
                        continue
    except Exception:
        pass


def run_brand_preflight_schema_migrations(brand_index: int) -> None:
    brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
    logger.info(f"🧱 Running preflight schema migrations for {brand_name}")
    ensure_user_agent_column(brand_index, "shopify_orders")
    ensure_user_agent_column(brand_index, "shopify_orders_update")
    ensure_customer_tag_column(brand_index, "shopify_orders")
    ensure_bundle_property_columns(brand_index, "shopify_orders")
    ensure_bundle_property_columns(brand_index, "shopify_orders_update")
    ensure_device_summary_columns(brand_index, "hourly_sessions_summary")
    ensure_device_summary_columns(brand_index, "hourly_sessions_summary_shopify")
    ensure_ci_events_column(brand_index, "hourly_sessions_summary_shopify")
    ensure_buy_now_events_column(brand_index, "hourly_sessions_summary_shopify")
    ensure_overall_summary_event_columns(brand_index)
    ensure_hour_wise_ppcod_column(brand_index)
    ensure_hour_wise_payment_sales_columns(brand_index)
    ensure_overall_summary_payment_sales_columns(brand_index)
    ensure_utm_names_column(brand_index, "overall_utm_summary")
    logger.info(f"✅ Preflight schema migrations complete for {brand_name}")


def ensure_summary_tables(cursor, connection):
    """Create all summary tables if they don't exist."""
    # Keep summary schema setup silent (no console timing log).
    if True:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sales_summary (
                date DATE PRIMARY KEY,
                gokwik_sales DECIMAL(12,2) DEFAULT 0,
                gokwik_returns DECIMAL(12,2) DEFAULT 0,
                actual_gokwik_sale DECIMAL(12,2) DEFAULT 0,
                KwikEngageSales DECIMAL(12,2) DEFAULT 0,
                KwikEngageReturns DECIMAL(12,2) DEFAULT 0,
                actual_KwikEngage_sale DECIMAL(12,2) DEFAULT 0,
                online_store_sales DECIMAL(12,2) DEFAULT 0,
                online_store_returns DECIMAL(12,2) DEFAULT 0,
                actual_online_store_sale DECIMAL(12,2) DEFAULT 0,
                hypd_store_sales DECIMAL(12,2) DEFAULT 0,
                hypd_store_returns DECIMAL(12,2) DEFAULT 0,
                actual_hypd_store_sale DECIMAL(12,2) DEFAULT 0,
                draft_order_sales DECIMAL(12,2) DEFAULT 0,
                draft_order_returns DECIMAL(12,2) DEFAULT 0,
                actual_draft_order_sale DECIMAL(12,2) DEFAULT 0,
                dpanda_sales DECIMAL(12,2) DEFAULT 0,
                dpanda_returns DECIMAL(12,2) DEFAULT 0,
                actual_dpanda_sale DECIMAL(12,2) DEFAULT 0,
                gkappbrew_sales DECIMAL(12,2) DEFAULT 0,
                gkappbrew_returns DECIMAL(12,2) DEFAULT 0,
                actual_gkappbrew_sale DECIMAL(12,2) DEFAULT 0,
                buykaro_sales DECIMAL(12,2) DEFAULT 0,
                buykaro_returns DECIMAL(12,2) DEFAULT 0,
                actual_buykaro_sale DECIMAL(12,2) DEFAULT 0,
                appbrewplus_sales DECIMAL(12,2) DEFAULT 0,
                appbrewplus_returns DECIMAL(12,2) DEFAULT 0,
                actual_appbrewplus_sale DECIMAL(12,2) DEFAULT 0,
                shopflo_sales DECIMAL(12,2) DEFAULT 0,
                shopflo_returns DECIMAL(12,2) DEFAULT 0,
                actual_shopflo_sale DECIMAL(12,2) DEFAULT 0,
                overall_sales_WO_hypd DECIMAL(12,2) DEFAULT 0,
                overall_returns_WO_hypd DECIMAL(12,2) DEFAULT 0,
                actual_overall_sales_WO_hypd DECIMAL(12,2) DEFAULT 0,
                overall_sales DECIMAL(12,2) DEFAULT 0,
                overall_returns DECIMAL(12,2) DEFAULT 0,
                rto_gokwik_returns DECIMAL(12,2) DEFAULT 0,
                rto_kwik_engage_returns DECIMAL(12,2) DEFAULT 0,
                rto_online_store_returns DECIMAL(12,2) DEFAULT 0,
                rto_hypd_store_returns DECIMAL(12,2) DEFAULT 0,
                rto_draft_order_returns DECIMAL(12,2) DEFAULT 0,
                rto_dpanda_returns DECIMAL(12,2) DEFAULT 0,
                rto_gkappbrew_returns DECIMAL(12,2) DEFAULT 0,
                rto_buykaro_returns DECIMAL(12,2) DEFAULT 0,
                rto_appbrewplus_returns DECIMAL(12,2) DEFAULT 0,
                rto_shopflo_returns DECIMAL(12,2) DEFAULT 0,
                rto_overall_returns_WO_hypd DECIMAL(12,2) DEFAULT 0,
                actual_overall_sales DECIMAL(12,2) DEFAULT 0,
                KEY idx_date (date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # --- Migration: Add/align sales_summary columns if they are missing from an existing table ---
        try:
            cursor.execute("SHOW COLUMNS FROM sales_summary")
            rows = cursor.fetchall()
            existing_cols = {
                row["Field"] if isinstance(row, dict) else row[0] for row in rows
            }

            migrations = {
                "shopflo_sales": "DECIMAL(12,2) DEFAULT 0 AFTER actual_appbrewplus_sale",
                "shopflo_returns": "DECIMAL(12,2) DEFAULT 0 AFTER shopflo_sales",
                "actual_shopflo_sale": "DECIMAL(12,2) DEFAULT 0 AFTER shopflo_returns",
                "rto_gokwik_returns": "DECIMAL(12,2) DEFAULT 0 AFTER overall_returns",
                "rto_kwik_engage_returns": "DECIMAL(12,2) DEFAULT 0 AFTER rto_gokwik_returns",
                "rto_online_store_returns": "DECIMAL(12,2) DEFAULT 0 AFTER rto_kwik_engage_returns",
                "rto_hypd_store_returns": "DECIMAL(12,2) DEFAULT 0 AFTER rto_online_store_returns",
                "rto_draft_order_returns": "DECIMAL(12,2) DEFAULT 0 AFTER rto_hypd_store_returns",
                "rto_dpanda_returns": "DECIMAL(12,2) DEFAULT 0 AFTER rto_draft_order_returns",
                "rto_gkappbrew_returns": "DECIMAL(12,2) DEFAULT 0 AFTER rto_dpanda_returns",
                "rto_buykaro_returns": "DECIMAL(12,2) DEFAULT 0 AFTER rto_gkappbrew_returns",
                "rto_appbrewplus_returns": "DECIMAL(12,2) DEFAULT 0 AFTER rto_buykaro_returns",
                "rto_shopflo_returns": "DECIMAL(12,2) DEFAULT 0 AFTER rto_appbrewplus_returns",
                "rto_overall_returns_WO_hypd": "DECIMAL(12,2) DEFAULT 0 AFTER rto_shopflo_returns",
            }

            for col, definition in migrations.items():
                if col not in existing_cols:
                    cursor.execute(
                        f"ALTER TABLE sales_summary ADD COLUMN {col} {definition}"
                    )

            connection.commit()
        except Exception as e:
            logger.warning(f"⚠️ Migration check for sales_summary failed: {e}")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS order_summary (
                date DATE PRIMARY KEY,
                number_of_orders_created INT DEFAULT 0,
                number_of_orders_returned INT DEFAULT 0,
                actual_number_of_orders INT DEFAULT 0,
                cod_orders INT DEFAULT 0,
                prepaid_orders INT DEFAULT 0,
                partially_paid_orders INT DEFAULT 0,
                overall_cod_orders INT DEFAULT 0,
                overall_prepaid_orders INT DEFAULT 0,
                overall_partially_paid_orders INT DEFAULT 0,
                rto_number_of_orders_returned INT DEFAULT 0,
                rto_cod_orders_returned INT DEFAULT 0,
                rto_prepaid_orders_returned INT DEFAULT 0,
                rto_partially_paid_orders_returned INT DEFAULT 0,
                overall_cod_sales DECIMAL(12,2) DEFAULT 0,
                overall_prepaid_sales DECIMAL(12,2) DEFAULT 0,
                overall_partially_paid_sales DECIMAL(12,2) DEFAULT 0,
                KEY idx_date (date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        try:
            cursor.execute("SHOW COLUMNS FROM order_summary")
            rows = cursor.fetchall()
            existing_cols = {
                row["Field"] if isinstance(row, dict) else row[0] for row in rows
            }
            migrations = {
                "rto_number_of_orders_returned": "INT DEFAULT 0 AFTER overall_partially_paid_orders",
                "rto_cod_orders_returned": "INT DEFAULT 0 AFTER rto_number_of_orders_returned",
                "rto_prepaid_orders_returned": "INT DEFAULT 0 AFTER rto_cod_orders_returned",
                "rto_partially_paid_orders_returned": "INT DEFAULT 0 AFTER rto_prepaid_orders_returned",
                "overall_cod_sales": "DECIMAL(12,2) DEFAULT 0 AFTER rto_partially_paid_orders_returned",
                "overall_prepaid_sales": "DECIMAL(12,2) DEFAULT 0 AFTER overall_cod_sales",
                "overall_partially_paid_sales": "DECIMAL(12,2) DEFAULT 0 AFTER overall_prepaid_sales",
            }
            for col, definition in migrations.items():
                if col not in existing_cols:
                    cursor.execute(
                        f"ALTER TABLE order_summary ADD COLUMN {col} {definition}"
                    )
            connection.commit()
        except Exception as e:
            logger.warning(f"⚠️ Migration check for order_summary failed: {e}")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS discount_summary (
                date DATE PRIMARY KEY,
                total_discounts_given DECIMAL(12,2) DEFAULT 0,
                total_discount_on_returns DECIMAL(12,2) DEFAULT 0,
                rto_total_discount_on_returns DECIMAL(12,2) DEFAULT 0,
                actual_discounts DECIMAL(12,2) DEFAULT 0,
                KEY idx_date (date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        try:
            cursor.execute("SHOW COLUMNS FROM discount_summary")
            rows = cursor.fetchall()
            existing_cols = {
                row["Field"] if isinstance(row, dict) else row[0] for row in rows
            }
            if "rto_total_discount_on_returns" not in existing_cols:
                cursor.execute(
                    "ALTER TABLE discount_summary ADD COLUMN rto_total_discount_on_returns DECIMAL(12,2) DEFAULT 0 AFTER total_discount_on_returns"
                )
            connection.commit()
        except Exception as e:
            logger.warning(f"⚠️ Migration check for discount_summary failed: {e}")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gross_summary (
                date DATE PRIMARY KEY,
                overall_sale DECIMAL(12,2) DEFAULT 0,
                shipping_total DECIMAL(12,2) DEFAULT 0,
                discounts_total DECIMAL(12,2) DEFAULT 0,
                tax_total DECIMAL(12,2) DEFAULT 0,
                gross_sales DECIMAL(12,2) DEFAULT 0,
                actual_discounts DECIMAL(12,2) DEFAULT 0,
                net_sales DECIMAL(12,2) DEFAULT 0,
                KEY idx_date (date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hour_wise_sales (
                date DATE NOT NULL,
                hour TINYINT UNSIGNED NOT NULL,
                number_of_orders INT DEFAULT 0,
                total_sales DECIMAL(12,2) DEFAULT 0,
                number_of_prepaid_orders INT DEFAULT 0,
                number_of_cod_orders INT DEFAULT 0,
                number_of_ppcod_orders INT DEFAULT 0,
                cod_sales DECIMAL(12,2) DEFAULT 0,
                prepaid_sales DECIMAL(12,2) DEFAULT 0,
                partially_prepaid_sales DECIMAL(12,2) DEFAULT 0,
                number_of_sessions INT DEFAULT 0,
                number_of_atc_sessions INT DEFAULT 0,
                PRIMARY KEY (date, hour),
                KEY idx_date (date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_city_sessions_summary_shopify (
                date DATE NOT NULL,
                city VARCHAR(100) NOT NULL,
                sessions INT DEFAULT 0,
                atc_sessions INT DEFAULT 0,
                PRIMARY KEY (date, city),
                KEY idx_date (date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hourly_city_sessions_summary_shopify (
                date DATE NOT NULL,
                hour TINYINT UNSIGNED NOT NULL,
                city VARCHAR(100) NOT NULL,
                sessions INT DEFAULT 0,
                atc_sessions INT DEFAULT 0,
                PRIMARY KEY (date, hour, city),
                KEY idx_date (date),
                KEY idx_date_hour (date, hour)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_citywise_summary (
                date DATE NOT NULL,
                city VARCHAR(100) NOT NULL,
                total_orders INT DEFAULT 0,
                total_revenue DECIMAL(12,2) DEFAULT 0,
                cod_orders INT DEFAULT 0,
                prepaid_orders INT DEFAULT 0,
                partially_paid_orders INT DEFAULT 0,
                sessions INT DEFAULT 0,
                atc_sessions INT DEFAULT 0,
                PRIMARY KEY (date, city),
                KEY idx_date (date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hourly_citywise_summary (
                date DATE NOT NULL,
                hour TINYINT UNSIGNED NOT NULL,
                city VARCHAR(100) NOT NULL,
                total_orders INT DEFAULT 0,
                total_revenue DECIMAL(12,2) DEFAULT 0,
                cod_orders INT DEFAULT 0,
                prepaid_orders INT DEFAULT 0,
                partially_paid_orders INT DEFAULT 0,
                sessions INT DEFAULT 0,
                atc_sessions INT DEFAULT 0,
                PRIMARY KEY (date, hour, city),
                KEY idx_date (date),
                KEY idx_date_hour (date, hour)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS product_traffic_split (
                date DATE NOT NULL,
                product_id VARCHAR(50) NOT NULL,
                utm_source JSON NOT NULL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (date, product_id),
                KEY idx_product_date (product_id, date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        try:
            citywise_migrations = {
                "daily_citywise_summary": {
                    "total_revenue": "DECIMAL(12,2) DEFAULT 0 AFTER total_orders",
                },
                "hourly_citywise_summary": {
                    "total_revenue": "DECIMAL(12,2) DEFAULT 0 AFTER total_orders",
                },
            }

            for table_name, columns in citywise_migrations.items():
                cursor.execute(f"SHOW COLUMNS FROM {table_name}")
                rows = cursor.fetchall()
                existing_cols = {
                    row["Field"] if isinstance(row, dict) else row[0] for row in rows
                }

                for col, definition in columns.items():
                    if col not in existing_cols:
                        cursor.execute(
                            f"ALTER TABLE {table_name} ADD COLUMN {col} {definition}"
                        )

            connection.commit()
        except Exception as e:
            logger.warning(f"⚠️ Migration check for citywise summary tables failed: {e}")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS overall_summary (
                date DATE PRIMARY KEY,
                gross_sales DECIMAL(12,2) DEFAULT 0,
                total_discount_amount DECIMAL(12,2) DEFAULT 0,
                total_sales DECIMAL(12,2) DEFAULT 0,
                net_sales DECIMAL(12,2) DEFAULT 0,
                total_orders INT DEFAULT 0,
                cod_orders INT DEFAULT 0,
                prepaid_orders INT DEFAULT 0,
                partially_paid_orders INT DEFAULT 0,
                cod_sales DECIMAL(12,2) DEFAULT 0,
                prepaid_sales DECIMAL(12,2) DEFAULT 0,
                partially_prepaid_sales DECIMAL(12,2) DEFAULT 0,
                total_sessions INT DEFAULT 0,
                total_atc_sessions INT DEFAULT 0,
                adjusted_total_sessions INT DEFAULT 0,
                ci_events INT DEFAULT 0,
                buy_now_events INT DEFAULT 0,
                KEY idx_date (date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rto_summary (
                summary_date DATE PRIMARY KEY,
                total_orders INT NOT NULL DEFAULT 0,
                rto_orders INT NOT NULL DEFAULT 0,
                rto_percentage DECIMAL(8,2) NOT NULL DEFAULT 0,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                KEY idx_summary_date (summary_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dashboard_order_discount_fact (
                order_id VARCHAR(50) NOT NULL,
                order_name VARCHAR(50),
                created_at DATETIME,
                created_date DATE NOT NULL,
                created_hour TINYINT UNSIGNED NOT NULL,
                discount_code VARCHAR(255) NOT NULL,
                discount_amount DECIMAL(12,2) DEFAULT 0,
                payment_gateway_names TEXT,
                payment_mode ENUM('prepaid', 'cod', 'partially_paid') NOT NULL,
                total_price DECIMAL(12,2) NOT NULL DEFAULT 0,
                PRIMARY KEY (order_id, discount_code),
                KEY idx_discount_date (discount_code, created_date),
                KEY idx_date_discount (created_date, discount_code),
                KEY idx_discount_hour (discount_code, created_date, created_hour),
                KEY idx_payment_discount_date (payment_mode, discount_code, created_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dashboard_discount_daily (
                date DATE NOT NULL,
                discount_code VARCHAR(255) NOT NULL,
                total_orders INT NOT NULL DEFAULT 0,
                gross_revenue DECIMAL(12,2) NOT NULL DEFAULT 0,
                aov DECIMAL(12,2) NOT NULL DEFAULT 0,
                PRIMARY KEY (date, discount_code),
                KEY idx_discount_date (discount_code, date),
                KEY idx_date (date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dashboard_discount_hourly (
                date DATE NOT NULL,
                hour TINYINT UNSIGNED NOT NULL,
                discount_code VARCHAR(255) NOT NULL,
                total_orders INT NOT NULL DEFAULT 0,
                gross_revenue DECIMAL(12,2) NOT NULL DEFAULT 0,
                aov DECIMAL(12,2) NOT NULL DEFAULT 0,
                PRIMARY KEY (date, hour, discount_code),
                KEY idx_discount_date_hour (discount_code, date, hour),
                KEY idx_date_hour (date, hour)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dashboard_discount_payment_daily (
                date DATE NOT NULL,
                discount_code VARCHAR(255) NOT NULL,
                payment_mode ENUM('prepaid', 'cod', 'partially_paid') NOT NULL,
                total_orders INT NOT NULL DEFAULT 0,
                gross_revenue DECIMAL(12,2) NOT NULL DEFAULT 0,
                PRIMARY KEY (date, discount_code, payment_mode),
                KEY idx_discount_date (discount_code, date),
                KEY idx_payment_discount_date (payment_mode, discount_code, date),
                KEY idx_date_payment (date, payment_mode)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dashboard_discount_payment_hourly (
                date DATE NOT NULL,
                hour TINYINT UNSIGNED NOT NULL,
                discount_code VARCHAR(255) NOT NULL,
                payment_mode ENUM('prepaid', 'cod', 'partially_paid') NOT NULL,
                total_orders INT NOT NULL DEFAULT 0,
                gross_revenue DECIMAL(12,2) NOT NULL DEFAULT 0,
                aov DECIMAL(12,2) NOT NULL DEFAULT 0,
                PRIMARY KEY (date, hour, discount_code, payment_mode),
                KEY idx_discount_date_hour (discount_code, date, hour),
                KEY idx_payment_discount_date_hour (payment_mode, discount_code, date, hour),
                KEY idx_date_hour_payment (date, hour, payment_mode)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # Ensure adjusted_total_sessions exists even if table was created earlier
        try:
            cursor.execute("""
                ALTER TABLE overall_summary
                ADD COLUMN adjusted_total_sessions INT DEFAULT 0
            """)
        except mysql.connector.Error:
            # Column may already exist; ignore error
            pass

        try:
            cursor.execute("""
                ALTER TABLE overall_summary
                ADD COLUMN ci_events INT DEFAULT 0
            """)
        except mysql.connector.Error:
            pass

        try:
            cursor.execute("""
                ALTER TABLE overall_summary
                ADD COLUMN buy_now_events INT DEFAULT 0
            """)
        except mysql.connector.Error:
            pass

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_ci_events_summary (
                id INT AUTO_INCREMENT PRIMARY KEY,
                date DATE NOT NULL,
                variant_id VARCHAR(255) NOT NULL,
                checkout_initiated_count INT DEFAULT 0,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY idx_date_variant (date, variant_id),
                KEY idx_date (date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_buy_now_events_summary (
                id INT AUTO_INCREMENT PRIMARY KEY,
                date DATE NOT NULL,
                variant_id VARCHAR(255) NOT NULL,
                buy_now_count INT DEFAULT 0,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY idx_date_variant (date, variant_id),
                KEY idx_date (date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS web_vitals_summary (
                mongo_object_id VARCHAR(24) PRIMARY KEY,
                brand_name VARCHAR(255) NOT NULL,
                brand_key VARCHAR(255) NOT NULL,
                page_name VARCHAR(512) NULL,
                url TEXT NOT NULL,
                `rank` INT NULL,
                sessions INT NULL,
                `date` DATE NULL,
                `time` TIME NULL,
                performance INT NULL,
                lcp DECIMAL(10,3) NULL,
                inp DECIMAL(10,3) NULL,
                cls DECIMAL(10,3) NULL,
                fcp DECIMAL(10,3) NULL,
                ttfb DECIMAL(10,3) NULL,
                created_at DATETIME(6) NOT NULL,
                updated_at DATETIME(6) NULL,
                KEY idx_brand_date (brand_key, date),
                KEY idx_created_at (created_at),
                KEY idx_url_date (url(255), date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_web_vitals_summary (
                `date` DATE NOT NULL,
                url_hash CHAR(64) NOT NULL,
                url VARCHAR(1024) NOT NULL,
                page_name VARCHAR(512) NULL,
                brand_name VARCHAR(255) NOT NULL,
                brand_key VARCHAR(255) NOT NULL,
                avg_performance DECIMAL(10,3) NULL,
                avg_fcp DECIMAL(10,3) NULL,
                avg_lcp DECIMAL(10,3) NULL,
                avg_inp DECIMAL(10,3) NULL,
                avg_ttfb DECIMAL(10,3) NULL,
                avg_cls DECIMAL(10,3) NULL,
                sample_count INT NOT NULL DEFAULT 0,
                PRIMARY KEY (`date`, url_hash),
                KEY idx_brand_date (brand_key, `date`),
                KEY idx_url (`url`(255))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        connection.commit()
