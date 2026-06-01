import os
import traceback
from datetime import timedelta
from typing import List

from workers import fillrate_common as common


def zero_metrics_for_fast_movers(cursor, connection, product_ids: List[int]):
    for values in common.chunked(product_ids, common.db_read_chunk_size()):
        placeholders = ", ".join(["%s"] * len(values))
        cursor.execute(
            f"""
            UPDATE product_fill_rate_metrics
            SET ordered_units_30d = 0,
                fulfilled_units_30d = 0,
                fill_rate_30d = 0,
                updated_at = CURRENT_TIMESTAMP
            WHERE product_id IN ({placeholders})
            """,
            tuple(values),
        )
        connection.commit()


def calculate_for_brand(core, brand_index: int, brand_name: str, brand_key: str, cursor=None, connection=None):
    if not common.is_fillrate_enabled() or not common.is_calculator_enabled() or not brand_key:
        return

    fast_movers = common.get_fast_movers(
        core, brand_index, brand_name, brand_key, cursor, connection
    )
    if not fast_movers:
        common.fillrate_log(core, "info", f"[{brand_name}] No fast movers found. Skipping fill-rate calculator.")
        return

    if cursor is None or connection is None:
        with core.get_db_cursor(brand_index) as (c, conn):
            return calculate_for_brand(core, brand_index, brand_name, brand_key, cursor=c, connection=conn)

    start_date = (core.now_ist().date() - timedelta(days=29)).isoformat()
    end_date = (core.now_ist().date() + timedelta(days=1)).isoformat()

    try:
        common.ensure_tables(cursor, connection)
        zero_metrics_for_fast_movers(cursor, connection, sorted(fast_movers))

        upsert_rows = []
        for values in common.chunked(sorted(fast_movers), common.db_read_chunk_size()):
            placeholders = ", ".join(["%s"] * len(values))
            cursor.execute(
                f"""
                SELECT
                    product_id,
                    variant_id,
                    SUM(ordered_units) AS ordered_units_30d,
                    SUM(fulfilled_units) AS fulfilled_units_30d
                FROM product_fill_rate_daily
                WHERE metric_date >= %s
                  AND metric_date < %s
                  AND product_id IN ({placeholders})
                GROUP BY product_id, variant_id
                """,
                (start_date, end_date, *values),
            )
            for row in cursor.fetchall():
                if isinstance(row, dict):
                    product_id = int(row["product_id"])
                    variant_id = common.normalize_variant_id(row.get("variant_id"))
                    ordered_units = int(row.get("ordered_units_30d") or 0)
                    fulfilled_units = int(row.get("fulfilled_units_30d") or 0)
                else:
                    product_id = int(row[0])
                    variant_id = common.normalize_variant_id(row[1])
                    ordered_units = int(row[2] or 0)
                    fulfilled_units = int(row[3] or 0)

                fill_rate = round((fulfilled_units / ordered_units) * 100, 2) if ordered_units > 0 else 0
                upsert_rows.append((product_id, variant_id, ordered_units, fulfilled_units, fill_rate))

        if upsert_rows:
            core.executemany_chunked(
                cursor,
                connection,
                """
                INSERT INTO product_fill_rate_metrics (
                    product_id, variant_id,
                    ordered_units_30d, fulfilled_units_30d, fill_rate_30d
                )
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    ordered_units_30d = VALUES(ordered_units_30d),
                    fulfilled_units_30d = VALUES(fulfilled_units_30d),
                    fill_rate_30d = VALUES(fill_rate_30d),
                    updated_at = CURRENT_TIMESTAMP
                """,
                upsert_rows,
            )

        common.fillrate_log(core, "info", f"✅ [{brand_name}] Materialized {len(upsert_rows)} fill-rate metric rows")
    except Exception as e:
        core.logger.error(f"❌ [{brand_name}] Fill-rate calculator failed: {e}")
        traceback.print_exc()


def run_job(core):
    if not common.is_fillrate_enabled() or not common.is_calculator_enabled():
        return

    for brand_index in core.active_brand_indices:
        brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
        brand_key = os.environ.get(f"BRAND_TAG_{brand_index}", "")
        try:
            calculate_for_brand(core, brand_index, brand_name, brand_key)
        except Exception as e:
            core.logger.error(f"❌ [{brand_name}] Fill-rate calculator job failed: {e}")
            traceback.print_exc()
