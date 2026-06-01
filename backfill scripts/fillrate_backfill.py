from dotenv import load_dotenv

load_dotenv()

import os
import sys
import traceback
from datetime import timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import aws_background as core
from workers import fillrate_common
from workers.fillrate_calculator import calculate_for_brand


def _brand_created_date_column(cursor) -> str:
    cursor.execute("SHOW COLUMNS FROM shopify_orders LIKE 'created_dt'")
    if cursor.fetchone():
        return "created_dt"
    return "created_date"


def _backfill_brand(brand_index: int):
    brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
    brand_key = os.environ.get(f"BRAND_TAG_{brand_index}", "")
    days = fillrate_common.backfill_days()

    with core.get_db_cursor(brand_index) as (cursor, connection):
        fillrate_common.ensure_tables(cursor, connection)
        fast_movers = fillrate_common.fetch_fast_movers_from_db(
            core, brand_index, brand_name, cursor, connection
        )

        if not fast_movers:
            print(f"[{brand_name}] No fast movers found. Skipping.")
            return

        fillrate_common.replace_fast_movers_cache(core, brand_key, fast_movers)

        created_date_col = _brand_created_date_column(cursor)
        start_date = (core.now_ist().date() - timedelta(days=days - 1)).isoformat()
        end_date = (core.now_ist().date() + timedelta(days=1)).isoformat()

        rows = []
        state_rows = []
        for chunk in fillrate_common.chunked(sorted(fast_movers), fillrate_common.db_read_chunk_size()):
            placeholders = ", ".join(["%s"] * len(chunk))
            cursor.execute(
                f"""
                SELECT
                    order_id,
                    {created_date_col} AS metric_date,
                    CAST(product_id AS UNSIGNED) AS product_id,
                    CAST(COALESCE(NULLIF(variant_id, ''), '0') AS UNSIGNED) AS variant_id,
                    COALESCE(line_item_quantity, 0) AS ordered_units,
                    fulfillment_status
                FROM shopify_orders
                WHERE {created_date_col} >= %s
                  AND {created_date_col} < %s
                  AND CAST(product_id AS UNSIGNED) IN ({placeholders})
                """,
                (start_date, end_date, *chunk),
            )
            order_rows = cursor.fetchall()
            order_ids = []
            normalized_rows = []
            for row in order_rows:
                if isinstance(row, dict):
                    order_id = str(row["order_id"])
                    metric_date = str(row["metric_date"])
                    product_id = int(row["product_id"])
                    variant_id = fillrate_common.normalize_variant_id(row.get("variant_id"))
                    ordered_units = int(row.get("ordered_units") or 0)
                    base_status = fillrate_common.normalize_fulfillment_status(row.get("fulfillment_status"))
                else:
                    order_id = str(row[0])
                    metric_date = str(row[1])
                    product_id = int(row[2])
                    variant_id = fillrate_common.normalize_variant_id(row[3])
                    ordered_units = int(row[4] or 0)
                    base_status = fillrate_common.normalize_fulfillment_status(row[5])
                order_ids.append(order_id)
                normalized_rows.append(
                    (order_id, metric_date, product_id, variant_id, ordered_units, base_status)
                )

            latest_status_by_order = fillrate_common.fetch_latest_fulfillment_statuses_for_order_ids(
                cursor,
                order_ids,
            )

            aggregated = {}
            for order_id, metric_date, product_id, variant_id, ordered_units, base_status in normalized_rows:
                final_status = latest_status_by_order.get(order_id, base_status)
                fulfilled_units = ordered_units if final_status == "fulfilled" else 0
                key = (metric_date, product_id, variant_id)
                if key not in aggregated:
                    aggregated[key] = {"ordered_units": 0, "fulfilled_units": 0}
                aggregated[key]["ordered_units"] += ordered_units
                aggregated[key]["fulfilled_units"] += fulfilled_units
                state_rows.append(
                    (
                        order_id,
                        metric_date,
                        product_id,
                        variant_id,
                        ordered_units,
                        1,
                        1 if final_status == "fulfilled" else 0,
                        final_status,
                    )
                )

            for (metric_date, product_id, variant_id), values in aggregated.items():
                rows.append(
                    (
                        metric_date,
                        product_id,
                        variant_id,
                        values["ordered_units"],
                        values["fulfilled_units"],
                    )
                )

        if rows:
            core.executemany_chunked(
                cursor,
                connection,
                """
                INSERT INTO product_fill_rate_daily (
                    metric_date, product_id, variant_id, ordered_units, fulfilled_units
                )
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    ordered_units = VALUES(ordered_units),
                    fulfilled_units = VALUES(fulfilled_units),
                    updated_at = CURRENT_TIMESTAMP
                """,
                rows,
            )
            fillrate_common.upsert_order_state_rows(core, cursor, connection, state_rows, brand_name)
            print(f"[{brand_name}] Backfilled {len(rows)} daily fill-rate rows")
        else:
            print(f"[{brand_name}] No historical fast-mover rows found in {days}-day window")

        calculate_for_brand(core, brand_index, brand_name, brand_key, cursor=cursor, connection=connection)


if __name__ == "__main__":
    if not fillrate_common.backfill_enabled():
        raise SystemExit(
            "FILLRATE_BACKFILL_ENABLED is false. Set it to true explicitly before running."
        )

    core.initialize_brand_configs()
    if not core.active_brand_indices:
        raise SystemExit("No active brands configured.")

    for brand_index in core.active_brand_indices:
        try:
            _backfill_brand(brand_index)
        except Exception as exc:
            brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
            print(f"[{brand_name}] Fill-rate backfill failed: {exc}")
            traceback.print_exc()
