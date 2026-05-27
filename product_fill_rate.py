import logging
import os
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple
from zoneinfo import ZoneInfo


logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")
EXECUTEMANY_CHUNK_SIZE = int(os.environ.get("EXECUTEMANY_CHUNK_SIZE", "1000"))


def now_ist() -> datetime:
    return datetime.now(IST)


def _parse_threshold(name: str) -> float:
    raw = os.environ.get(name, "0").strip()
    if not raw:
        return 0.0
    return float(raw)


def _dates_in_clause(values: Iterable[str]) -> Tuple[str, List[str]]:
    ordered = sorted({str(value) for value in values if value})
    return ",".join(["%s"] * len(ordered)), ordered


def _product_ids_in_clause(product_ids: Iterable[int]) -> Tuple[str, List[int]]:
    ordered = sorted({int(product_id) for product_id in product_ids if product_id})
    return ",".join(["%s"] * len(ordered)), ordered


def _executemany_chunked(cursor, connection, sql: str, rows: Sequence[Tuple]) -> int:
    if not rows:
        return 0

    total = len(rows)
    for start in range(0, total, EXECUTEMANY_CHUNK_SIZE):
        cursor.executemany(sql, rows[start : start + EXECUTEMANY_CHUNK_SIZE])
        connection.commit()
    return total


def _parse_iso_to_ist_date(iso_ts: Optional[str]) -> Optional[str]:
    if not iso_ts:
        return None
    try:
        dt = datetime.fromisoformat(str(iso_ts).replace("Z", "+00:00"))
        return dt.astimezone(IST).date().isoformat()
    except Exception:
        return None


def ensure_product_fill_rate_tables(cursor, connection) -> None:
    logger.info("Ensuring product fill-rate tables exist")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS product_fill_rate_daily (
            date DATE NOT NULL,
            product_id BIGINT NOT NULL,
            ordered_units INT NOT NULL DEFAULT 0,
            fulfilled_units INT NOT NULL DEFAULT 0,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (date, product_id),
            KEY idx_product_id (product_id),
            KEY idx_date (date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS fill_rate_calculator_state (
            state_key VARCHAR(100) PRIMARY KEY,
            run_count BIGINT NOT NULL DEFAULT 0,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS product_fill_rate_metrics (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            product_id BIGINT NOT NULL,
            variant_id BIGINT NULL,
            ordered_units_7d INT NOT NULL DEFAULT 0,
            ordered_units_30d INT NOT NULL DEFAULT 0,
            ordered_units_90d INT NOT NULL DEFAULT 0,
            fulfilled_units_7d INT NOT NULL DEFAULT 0,
            fulfilled_units_30d INT NOT NULL DEFAULT 0,
            fulfilled_units_90d INT NOT NULL DEFAULT 0,
            fill_rate_7d DECIMAL(10,2) NOT NULL DEFAULT 0,
            fill_rate_30d DECIMAL(10,2) NOT NULL DEFAULT 0,
            fill_rate_90d DECIMAL(10,2) NOT NULL DEFAULT 0,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_product_id (product_id),
            KEY idx_product_id (product_id),
            KEY idx_variant_id (variant_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    connection.commit()
    logger.info("Product fill-rate tables ensured successfully")


def get_fill_rate_calculator_interval() -> int:
    raw = os.environ.get("FILL_RATE_CALCULATOR_EVERY_N_RUNS", "1").strip()
    try:
        interval = int(raw)
    except ValueError:
        interval = 1
    return max(1, interval)


def register_fill_rate_pipeline_run(
    cursor, connection, brand_name: str
) -> Tuple[bool, int, int]:
    ensure_product_fill_rate_tables(cursor, connection)

    interval = get_fill_rate_calculator_interval()
    state_key = "fill_rate_calculator_main_pipeline_runs"

    cursor.execute(
        """
        INSERT INTO fill_rate_calculator_state (state_key, run_count)
        VALUES (%s, 1)
        ON DUPLICATE KEY UPDATE run_count = run_count + 1
        """,
        (state_key,),
    )
    connection.commit()

    cursor.execute(
        "SELECT run_count FROM fill_rate_calculator_state WHERE state_key = %s",
        (state_key,),
    )
    row = cursor.fetchone()
    run_count = int((row["run_count"] if isinstance(row, dict) else row[0]) or 0)
    should_run = run_count % interval == 0
    logger.info(
        "[%s] Fill-rate calculator cadence check: run_count=%s, interval=%s, should_run=%s",
        brand_name,
        run_count,
        interval,
        should_run,
    )
    return should_run, run_count, interval


def fetch_fast_mover_product_ids(cursor, brand_name: str) -> Set[int]:
    threshold_7d = _parse_threshold("FILL_RATE_FAST_MOVER_DRR_7D_THRESHOLD")
    threshold_30d = _parse_threshold("FILL_RATE_FAST_MOVER_DRR_30D_THRESHOLD")
    threshold_90d = _parse_threshold("FILL_RATE_FAST_MOVER_DRR_90D_THRESHOLD")

    logger.info(
        "[%s] Fetching fast movers from top_products_inventory once for this run "
        "(thresholds: 7d=%s, 30d=%s, 90d=%s)",
        brand_name,
        threshold_7d,
        threshold_30d,
        threshold_90d,
    )
    cursor.execute(
        """
        SELECT DISTINCT product_id
        FROM top_products_inventory
        WHERE product_id IS NOT NULL
          AND (
                COALESCE(drr_7d, 0) >= %s
             OR COALESCE(drr_30d, 0) >= %s
             OR COALESCE(drr_90d, 0) >= %s
          )
        """,
        (threshold_7d, threshold_30d, threshold_90d),
    )

    product_ids: Set[int] = set()
    for row in cursor.fetchall() or []:
        try:
            product_id = int(row["product_id"] if isinstance(row, dict) else row[0])
        except Exception:
            continue
        if product_id > 0:
            product_ids.add(product_id)

    logger.info("[%s] Fast movers fetched: %s product(s)", brand_name, len(product_ids))
    return product_ids


def collect_fill_rate_affected_created_dates(
    orders_list: List[Dict], fast_mover_product_ids: Set[int]
) -> Set[str]:
    dates: Set[str] = set()
    if not orders_list or not fast_mover_product_ids:
        return dates

    for order in orders_list:
        created_date = _parse_iso_to_ist_date(order.get("created_at"))
        if not created_date:
            continue

        for item in order.get("line_items") or []:
            try:
                product_id = int(item.get("product_id") or 0)
            except Exception:
                product_id = 0

            if product_id in fast_mover_product_ids:
                dates.add(created_date)
                break

    return dates


def _fetch_base_order_product_states(
    cursor, target_dates: Iterable[str], fast_mover_product_ids: Set[int]
) -> Dict[Tuple[str, str, int], Dict[str, int]]:
    date_placeholders, date_params = _dates_in_clause(target_dates)
    product_placeholders, product_params = _product_ids_in_clause(fast_mover_product_ids)
    if not date_params or not product_params:
        return {}

    cursor.execute(
        f"""
        SELECT
            created_date,
            order_id,
            product_id,
            SUM(COALESCE(line_item_quantity, 0)) AS ordered_units,
            MAX(
                CASE
                    WHEN LOWER(COALESCE(fulfillment_status, '')) = 'fulfilled' THEN 1
                    ELSE 0
                END
            ) AS is_fulfilled
        FROM shopify_orders
        WHERE created_date IN ({date_placeholders})
          AND product_id IN ({product_placeholders})
          AND order_id IS NOT NULL
        GROUP BY created_date, order_id, product_id
        """,
        tuple(date_params + product_params),
    )

    states: Dict[Tuple[str, str, int], Dict[str, int]] = {}
    for row in cursor.fetchall() or []:
        created_date = str(row["created_date"] if isinstance(row, dict) else row[0])
        order_id = str(row["order_id"] if isinstance(row, dict) else row[1])
        product_id = int(row["product_id"] if isinstance(row, dict) else row[2])
        ordered_units = int(row["ordered_units"] if isinstance(row, dict) else row[3] or 0)
        is_fulfilled = int(row["is_fulfilled"] if isinstance(row, dict) else row[4] or 0)
        states[(created_date, order_id, product_id)] = {
            "ordered_units": ordered_units,
            "is_fulfilled": is_fulfilled,
        }
    return states


def _fetch_latest_update_order_product_states(
    cursor, target_dates: Iterable[str], fast_mover_product_ids: Set[int]
) -> Dict[Tuple[str, str, int], Dict[str, int]]:
    date_placeholders, date_params = _dates_in_clause(target_dates)
    product_placeholders, product_params = _product_ids_in_clause(fast_mover_product_ids)
    if not date_params or not product_params:
        return {}

    params = tuple(date_params + product_params + date_params + product_params)
    cursor.execute(
        f"""
        SELECT
            u.created_date,
            u.order_id,
            u.product_id,
            SUM(COALESCE(u.line_item_quantity, 0)) AS ordered_units,
            MAX(
                CASE
                    WHEN LOWER(COALESCE(u.fulfillment_status, '')) = 'fulfilled' THEN 1
                    ELSE 0
                END
            ) AS is_fulfilled
        FROM shopify_orders_update u
        INNER JOIN (
            SELECT
                created_date,
                order_id,
                product_id,
                MAX(updated_at) AS max_updated_at
            FROM shopify_orders_update
            WHERE created_date IN ({date_placeholders})
              AND product_id IN ({product_placeholders})
              AND order_id IS NOT NULL
            GROUP BY created_date, order_id, product_id
        ) latest
            ON latest.created_date = u.created_date
           AND latest.order_id = u.order_id
           AND latest.product_id = u.product_id
           AND (
                latest.max_updated_at = u.updated_at
                OR (latest.max_updated_at IS NULL AND u.updated_at IS NULL)
           )
        WHERE u.created_date IN ({date_placeholders})
          AND u.product_id IN ({product_placeholders})
          AND u.order_id IS NOT NULL
        GROUP BY u.created_date, u.order_id, u.product_id
        """,
        params,
    )

    states: Dict[Tuple[str, str, int], Dict[str, int]] = {}
    for row in cursor.fetchall() or []:
        created_date = str(row["created_date"] if isinstance(row, dict) else row[0])
        order_id = str(row["order_id"] if isinstance(row, dict) else row[1])
        product_id = int(row["product_id"] if isinstance(row, dict) else row[2])
        ordered_units = int(row["ordered_units"] if isinstance(row, dict) else row[3] or 0)
        is_fulfilled = int(row["is_fulfilled"] if isinstance(row, dict) else row[4] or 0)
        states[(created_date, order_id, product_id)] = {
            "ordered_units": ordered_units,
            "is_fulfilled": is_fulfilled,
        }
    return states


def rebuild_product_fill_rate_daily(
    cursor,
    connection,
    brand_name: str,
    target_dates: Iterable[str],
    fast_mover_product_ids: Set[int],
) -> int:
    ensure_product_fill_rate_tables(cursor, connection)

    date_placeholders, date_params = _dates_in_clause(target_dates)
    product_placeholders, product_params = _product_ids_in_clause(fast_mover_product_ids)
    if not date_params or not product_params:
        logger.info("[%s] Skipping daily fill-rate rebuild: no target dates or fast movers", brand_name)
        return 0

    logger.info(
        "[%s] Rebuilding product_fill_rate_daily for %s date(s) and %s fast-mover product(s)",
        brand_name,
        len(date_params),
        len(product_params),
    )

    base_states = _fetch_base_order_product_states(cursor, date_params, fast_mover_product_ids)
    update_states = _fetch_latest_update_order_product_states(cursor, date_params, fast_mover_product_ids)

    effective_states = dict(base_states)
    effective_states.update(update_states)

    aggregated: Dict[Tuple[str, int], Dict[str, int]] = {}
    for (created_date, _order_id, product_id), payload in effective_states.items():
        ordered_units = int(payload.get("ordered_units") or 0)
        if ordered_units <= 0:
            continue

        key = (created_date, product_id)
        bucket = aggregated.setdefault(
            key,
            {"ordered_units": 0, "fulfilled_units": 0},
        )
        bucket["ordered_units"] += ordered_units
        if int(payload.get("is_fulfilled") or 0) == 1:
            bucket["fulfilled_units"] += ordered_units

    cursor.execute(
        f"""
        DELETE FROM product_fill_rate_daily
        WHERE date IN ({date_placeholders})
          AND product_id IN ({product_placeholders})
        """,
        tuple(date_params + product_params),
    )
    connection.commit()

    upsert_rows = [
        (created_date, product_id, values["ordered_units"], values["fulfilled_units"])
        for (created_date, product_id), values in sorted(aggregated.items())
    ]
    inserted = _executemany_chunked(
        cursor,
        connection,
        """
        INSERT INTO product_fill_rate_daily (
            date,
            product_id,
            ordered_units,
            fulfilled_units
        )
        VALUES (%s, %s, %s, %s)
        """,
        upsert_rows,
    )
    logger.info(
        "[%s] product_fill_rate_daily rebuilt: deleted target rows and inserted %s row(s)",
        brand_name,
        inserted,
    )
    return inserted


def _cleanup_non_fast_mover_metrics(
    cursor, connection, brand_name: str, fast_mover_product_ids: Set[int]
) -> None:
    product_placeholders, product_params = _product_ids_in_clause(fast_mover_product_ids)
    if not product_params:
        logger.info("[%s] No fast movers found; clearing product_fill_rate_metrics", brand_name)
        cursor.execute("DELETE FROM product_fill_rate_metrics")
        connection.commit()
        return

    cursor.execute(
        f"""
        DELETE FROM product_fill_rate_metrics
        WHERE product_id NOT IN ({product_placeholders})
        """,
        tuple(product_params),
    )
    connection.commit()


def rebuild_product_fill_rate_metrics(
    cursor,
    connection,
    brand_name: str,
    fast_mover_product_ids: Set[int],
    anchor_date: Optional[date] = None,
) -> int:
    ensure_product_fill_rate_tables(cursor, connection)
    _cleanup_non_fast_mover_metrics(cursor, connection, brand_name, fast_mover_product_ids)

    if not fast_mover_product_ids:
        return 0

    today = anchor_date or now_ist().date()
    start_7d = (today - timedelta(days=6)).isoformat()
    start_30d = (today - timedelta(days=29)).isoformat()
    start_90d = (today - timedelta(days=89)).isoformat()

    product_placeholders, product_params = _product_ids_in_clause(fast_mover_product_ids)
    cursor.execute(
        f"""
        SELECT
            product_id,
            SUM(CASE WHEN date >= %s THEN ordered_units ELSE 0 END) AS ordered_units_7d,
            SUM(CASE WHEN date >= %s THEN ordered_units ELSE 0 END) AS ordered_units_30d,
            SUM(CASE WHEN date >= %s THEN ordered_units ELSE 0 END) AS ordered_units_90d,
            SUM(CASE WHEN date >= %s THEN fulfilled_units ELSE 0 END) AS fulfilled_units_7d,
            SUM(CASE WHEN date >= %s THEN fulfilled_units ELSE 0 END) AS fulfilled_units_30d,
            SUM(CASE WHEN date >= %s THEN fulfilled_units ELSE 0 END) AS fulfilled_units_90d
        FROM product_fill_rate_daily
        WHERE date >= %s
          AND product_id IN ({product_placeholders})
        GROUP BY product_id
        """,
        tuple(
            [
                start_7d,
                start_30d,
                start_90d,
                start_7d,
                start_30d,
                start_90d,
                start_90d,
            ]
            + product_params
        ),
    )

    metrics_by_product: Dict[int, Dict[str, int]] = {
        product_id: {
            "ordered_units_7d": 0,
            "ordered_units_30d": 0,
            "ordered_units_90d": 0,
            "fulfilled_units_7d": 0,
            "fulfilled_units_30d": 0,
            "fulfilled_units_90d": 0,
        }
        for product_id in fast_mover_product_ids
    }

    for row in cursor.fetchall() or []:
        product_id = int(row["product_id"] if isinstance(row, dict) else row[0])
        metrics_by_product[product_id] = {
            "ordered_units_7d": int((row["ordered_units_7d"] if isinstance(row, dict) else row[1]) or 0),
            "ordered_units_30d": int((row["ordered_units_30d"] if isinstance(row, dict) else row[2]) or 0),
            "ordered_units_90d": int((row["ordered_units_90d"] if isinstance(row, dict) else row[3]) or 0),
            "fulfilled_units_7d": int((row["fulfilled_units_7d"] if isinstance(row, dict) else row[4]) or 0),
            "fulfilled_units_30d": int((row["fulfilled_units_30d"] if isinstance(row, dict) else row[5]) or 0),
            "fulfilled_units_90d": int((row["fulfilled_units_90d"] if isinstance(row, dict) else row[6]) or 0),
        }

    upsert_rows = [
        (
            product_id,
            None,
            values["ordered_units_7d"],
            values["ordered_units_30d"],
            values["ordered_units_90d"],
            values["fulfilled_units_7d"],
            values["fulfilled_units_30d"],
            values["fulfilled_units_90d"],
        )
        for product_id, values in sorted(metrics_by_product.items())
    ]

    upserted = _executemany_chunked(
        cursor,
        connection,
        """
        INSERT INTO product_fill_rate_metrics (
            product_id,
            variant_id,
            ordered_units_7d,
            ordered_units_30d,
            ordered_units_90d,
            fulfilled_units_7d,
            fulfilled_units_30d,
            fulfilled_units_90d
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            variant_id = VALUES(variant_id),
            ordered_units_7d = VALUES(ordered_units_7d),
            ordered_units_30d = VALUES(ordered_units_30d),
            ordered_units_90d = VALUES(ordered_units_90d),
            fulfilled_units_7d = VALUES(fulfilled_units_7d),
            fulfilled_units_30d = VALUES(fulfilled_units_30d),
            fulfilled_units_90d = VALUES(fulfilled_units_90d)
        """,
        upsert_rows,
    )
    logger.info(
        "[%s] product_fill_rate_metrics rebuilt for %s fast-mover product(s)",
        brand_name,
        upserted,
    )
    return upserted


def _compute_fill_rate(ordered_units: int, fulfilled_units: int) -> Decimal:
    if ordered_units <= 0 or fulfilled_units <= 0:
        return Decimal("0.00")
    return (
        (Decimal(fulfilled_units) / Decimal(ordered_units)) * Decimal("100")
    ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def calculate_fill_rate_percentages(
    cursor,
    connection,
    brand_name: str,
    product_ids: Optional[Iterable[int]] = None,
) -> int:
    ensure_product_fill_rate_tables(cursor, connection)

    params: List[int] = []
    where_sql = ""
    if product_ids is not None:
        product_placeholders, product_params = _product_ids_in_clause(product_ids)
        if not product_params:
            logger.info("[%s] No product_ids provided for fill-rate calculation", brand_name)
            return 0
        where_sql = f" WHERE product_id IN ({product_placeholders})"
        params.extend(product_params)

    cursor.execute(
        f"""
        SELECT
            product_id,
            ordered_units_7d,
            ordered_units_30d,
            ordered_units_90d,
            fulfilled_units_7d,
            fulfilled_units_30d,
            fulfilled_units_90d
        FROM product_fill_rate_metrics
        {where_sql}
        """,
        tuple(params),
    )

    update_rows = []
    for row in cursor.fetchall() or []:
        product_id = int(row["product_id"] if isinstance(row, dict) else row[0])
        ordered_7d = int((row["ordered_units_7d"] if isinstance(row, dict) else row[1]) or 0)
        ordered_30d = int((row["ordered_units_30d"] if isinstance(row, dict) else row[2]) or 0)
        ordered_90d = int((row["ordered_units_90d"] if isinstance(row, dict) else row[3]) or 0)
        fulfilled_7d = int((row["fulfilled_units_7d"] if isinstance(row, dict) else row[4]) or 0)
        fulfilled_30d = int((row["fulfilled_units_30d"] if isinstance(row, dict) else row[5]) or 0)
        fulfilled_90d = int((row["fulfilled_units_90d"] if isinstance(row, dict) else row[6]) or 0)

        update_rows.append(
            (
                str(_compute_fill_rate(ordered_7d, fulfilled_7d)),
                str(_compute_fill_rate(ordered_30d, fulfilled_30d)),
                str(_compute_fill_rate(ordered_90d, fulfilled_90d)),
                product_id,
            )
        )

    updated = _executemany_chunked(
        cursor,
        connection,
        """
        UPDATE product_fill_rate_metrics
        SET
            fill_rate_7d = %s,
            fill_rate_30d = %s,
            fill_rate_90d = %s
        WHERE product_id = %s
        """,
        update_rows,
    )
    logger.info("[%s] Fill-rate percentages updated for %s product(s)", brand_name, updated)
    return updated


def execute_fill_rate_pipeline_incremental(
    cursor,
    connection,
    brand_name: str,
    target_dates: Iterable[str],
    fast_mover_product_ids: Set[int],
    anchor_date: Optional[date] = None,
) -> Tuple[int, int]:
    ensure_product_fill_rate_tables(cursor, connection)
    daily_rows = rebuild_product_fill_rate_daily(
        cursor,
        connection,
        brand_name,
        target_dates,
        fast_mover_product_ids,
    )
    metric_rows = rebuild_product_fill_rate_metrics(
        cursor,
        connection,
        brand_name,
        fast_mover_product_ids,
        anchor_date=anchor_date,
    )
    return daily_rows, metric_rows


def build_fill_rate_backfill_dates(
    range_name: str, anchor_date: Optional[date] = None
) -> List[str]:
    range_key = (range_name or "").strip().lower()
    days_by_range = {"7d": 7, "30d": 30, "90d": 90}
    if range_key not in days_by_range:
        raise ValueError("FILL_RATE_BACKFILL_RANGE must be one of: 7d, 30d, 90d")

    today = anchor_date or now_ist().date()
    days = days_by_range[range_key]
    start_date = today - timedelta(days=days - 1)
    return [
        (start_date + timedelta(days=offset)).isoformat()
        for offset in range(days)
    ]
