import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    from upstash_redis import Redis as UpstashRedis
except ImportError:
    UpstashRedis = None

try:
    import redis as redis_lib
except ImportError:
    redis_lib = None


_redis_client = None
_redis_init_attempted = False


def is_fillrate_enabled() -> bool:
    return os.environ.get("FILLRATE_ENABLED", "false").strip().lower() == "true"


def is_fast_mover_worker_enabled() -> bool:
    return os.environ.get("FILLRATE_FAST_MOVER_ENABLED", "true").strip().lower() == "true"


def is_calculator_enabled() -> bool:
    return os.environ.get("FILLRATE_CALCULATOR_ENABLED", "true").strip().lower() == "true"


def backfill_enabled() -> bool:
    return os.environ.get("FILLRATE_BACKFILL_ENABLED", "false").strip().lower() == "true"


def backfill_days() -> int:
    return int(os.environ.get("FILLRATE_BACKFILL_DAYS", "30"))


def fast_mover_cron() -> str:
    return os.environ.get("FILLRATE_FAST_MOVER_CRON", "0 1 * * *")


def calculator_cron() -> str:
    return os.environ.get("FILLRATE_CALCULATOR_CRON", "0 */1 * * *")


def redis_ttl_seconds() -> int:
    return int(os.environ.get("FILLRATE_FAST_MOVER_REDIS_TTL_SECONDS", "86400"))


def drr_30_threshold() -> float:
    return float(os.environ.get("DRR_30_THRESHOLD", "0"))


def redis_chunk_size() -> int:
    return int(os.environ.get("FILLRATE_REDIS_CHUNK_SIZE", "1000"))


def db_read_chunk_size() -> int:
    return int(os.environ.get("FILLRATE_DB_READ_CHUNK_SIZE", "500"))


def log_level() -> str:
    return os.environ.get("FILLRATE_LOG_LEVEL", "info").strip().lower()


def fillrate_log(core, level: str, message: str):
    order = {"debug": 10, "info": 20, "warning": 30, "error": 40}
    current = order.get(log_level(), 20)
    if order.get(level, 20) < current:
        return
    getattr(core.logger, level, core.logger.info)(message)


def chunked(values: List[Any], size: int):
    for i in range(0, len(values), size):
        yield values[i : i + size]


def get_redis_client(core):
    global _redis_client, _redis_init_attempted

    if _redis_client is not None:
        return _redis_client
    if _redis_init_attempted:
        return None

    _redis_init_attempted = True

    redis_url = os.environ.get("REDIS_URL", "").strip()
    if redis_lib is not None and redis_url:
        try:
            _redis_client = redis_lib.Redis.from_url(redis_url, decode_responses=True)
            _redis_client.ping()
            core.logger.info("✅ Fill-rate Redis client initialized via REDIS_URL")
            return _redis_client
        except Exception as e:
            core.logger.warning(f"Fill-rate Redis URL fallback failed: {e}")

    if UpstashRedis is not None:
        try:
            _redis_client = UpstashRedis.from_env()
            try:
                _redis_client.ping()
            except Exception:
                pass
            core.logger.info("✅ Fill-rate Redis client initialized via Upstash REST")
            return _redis_client
        except Exception as e:
            core.logger.warning(f"Fill-rate Upstash Redis init failed: {e}")

    if UpstashRedis is None and redis_lib is None:
        core.logger.warning("Fill-rate Redis disabled: no supported Redis client installed.")
    else:
        core.logger.warning("Fill-rate Redis disabled: no usable Redis connection could be established.")
    _redis_client = None
    return None


def fastmovers_key(brand_key: str) -> str:
    return f"fillrate:fastmovers:{brand_key}"


def normalize_variant_id(value: Any) -> int:
    try:
        parsed = int(value or 0)
        return parsed if parsed > 0 else 0
    except Exception:
        return 0


def normalize_fulfillment_status(value: Any) -> str:
    return str(value or "").strip().lower()


def ensure_tables(cursor, connection):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS product_fill_rate_daily (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            metric_date DATE NOT NULL,
            product_id BIGINT NOT NULL,
            variant_id BIGINT NULL,
            ordered_units INT NOT NULL DEFAULT 0,
            fulfilled_units INT NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_daily_product_variant (
                metric_date,
                product_id,
                variant_id
            ),
            KEY idx_product_date_variant (
                product_id,
                metric_date,
                variant_id
            ),
            KEY idx_metric_date (
                metric_date
            )
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS product_fill_rate_order_state (
            order_id VARCHAR(50) NOT NULL,
            metric_date DATE NOT NULL,
            product_id BIGINT NOT NULL,
            variant_id BIGINT NOT NULL DEFAULT 0,
            quantity INT NOT NULL DEFAULT 0,
            ordered_counted TINYINT(1) NOT NULL DEFAULT 0,
            fulfilled_counted TINYINT(1) NOT NULL DEFAULT 0,
            current_fulfillment_status VARCHAR(32) NOT NULL DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (order_id, metric_date, product_id, variant_id),
            KEY idx_product_metric_date (
                product_id,
                metric_date,
                variant_id
            )
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS product_fill_rate_metrics (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            product_id BIGINT NOT NULL,
            variant_id BIGINT NULL,
            ordered_units_30d INT NOT NULL DEFAULT 0,
            fulfilled_units_30d INT NOT NULL DEFAULT 0,
            fill_rate_30d DECIMAL(10,2) NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_product_variant (
                product_id,
                variant_id
            ),
            KEY idx_product (
                product_id
            )
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    connection.commit()


def fetch_fast_movers_from_db(core, brand_index: int, brand_name: str, cursor, connection) -> Set[int]:
    product_ids: Set[int] = set()
    try:
        cursor.execute(
            """
            SELECT DISTINCT product_id
            FROM top_products_inventory
            WHERE drr_30d >= %s
              AND product_id IS NOT NULL
            """,
            (drr_30_threshold(),),
        )
        for row in cursor.fetchall():
            raw = row["product_id"] if isinstance(row, dict) else row[0]
            try:
                pid = int(raw)
            except Exception:
                continue
            if pid > 0:
                product_ids.add(pid)

        fillrate_log(
            core,
            "info",
            f"[{brand_name}] Loaded {len(product_ids)} fast movers from top_products_inventory",
        )
    except Exception as e:
        core.logger.error(f"❌ [{brand_name}] Failed to fetch fast movers: {e}")
    return product_ids


def replace_fast_movers_cache(core, brand_key: str, product_ids: Set[int]) -> bool:
    redis_client = get_redis_client(core)
    if redis_client is None:
        return False

    key = fastmovers_key(brand_key)
    temp_key = f"{key}:tmp:{uuid.uuid4().hex[:8]}"
    product_values = [str(pid) for pid in sorted(product_ids)]

    try:
        redis_client.delete(temp_key)
        if product_values:
            for member_chunk in chunked(product_values, redis_chunk_size()):
                redis_client.sadd(temp_key, *member_chunk)
        else:
            redis_client.sadd(temp_key, "__empty__")
            redis_client.srem(temp_key, "__empty__")
        redis_client.rename(temp_key, key)
        redis_client.expire(key, redis_ttl_seconds())
        return True
    except Exception as e:
        core.logger.error(f"❌ Failed to refresh fill-rate fast mover cache for {brand_key}: {e}")
        try:
            redis_client.delete(temp_key)
        except Exception:
            pass
        return False


def load_fast_movers_from_redis(core, brand_key: str) -> Optional[Set[int]]:
    redis_client = get_redis_client(core)
    if redis_client is None:
        return None

    try:
        members = redis_client.smembers(fastmovers_key(brand_key)) or set()
        product_ids = set()
        for member in members:
            try:
                if isinstance(member, bytes):
                    member = member.decode("utf-8")
                product_ids.add(int(member))
            except Exception:
                continue
        return product_ids
    except Exception as e:
        core.logger.error(f"❌ Failed to load fill-rate fast mover cache for {brand_key}: {e}")
        return None


def get_fast_movers(
    core,
    brand_index: int,
    brand_name: str,
    brand_key: str,
    cursor,
    connection,
) -> Set[int]:
    cached = load_fast_movers_from_redis(core, brand_key)
    if cached:
        return cached

    fast_movers = fetch_fast_movers_from_db(core, brand_index, brand_name, cursor, connection)
    if fast_movers:
        replace_fast_movers_cache(core, brand_key, fast_movers)
        if cached is None:
            core.logger.warning(
                f"⚠️ [{brand_name}] Redis unavailable; used DB fallback for fast movers"
            )
        else:
            fillrate_log(
                core,
                "info",
                f"[{brand_name}] Redis cache empty; used DB fallback for fast movers"
            )
    return fast_movers


def fetch_previous_fulfillment_statuses(core, cursor, connection, order_ids: List[str]) -> Dict[str, str]:
    if not order_ids:
        return {}

    try:
        connection.ping(reconnect=True, attempts=3, delay=2)
    except Exception as e:
        core.logger.warning(f"⚠️ fill-rate status lookup ping/reconnect failed: {e}")

    previous_statuses: Dict[str, Tuple[datetime, str]] = {}

    for values in chunked(order_ids, db_read_chunk_size()):
        placeholders = ", ".join(["%s"] * len(values))
        cursor.execute(
            f"""
            SELECT order_id, fulfillment_status, updated_at
            FROM shopify_orders_update
            WHERE order_id IN ({placeholders})
            """,
            tuple(values),
        )
        for row in cursor.fetchall():
            if isinstance(row, dict):
                order_id = str(row.get("order_id") or "")
                status = row.get("fulfillment_status")
                ts_value = row.get("updated_at")
            else:
                order_id = str(row[0] or "")
                status = row[1]
                ts_value = row[2]
            if not order_id:
                continue
            prev = previous_statuses.get(order_id)
            compare_ts = ts_value or datetime.min
            if prev is None or compare_ts >= prev[0]:
                previous_statuses[order_id] = (compare_ts, normalize_fulfillment_status(status))

    missing_ids = [oid for oid in order_ids if oid not in previous_statuses]
    for values in chunked(missing_ids, db_read_chunk_size()):
        placeholders = ", ".join(["%s"] * len(values))
        cursor.execute(
            f"""
            SELECT order_id, fulfillment_status, created_at
            FROM shopify_orders
            WHERE order_id IN ({placeholders})
            """,
            tuple(values),
        )
        for row in cursor.fetchall():
            if isinstance(row, dict):
                order_id = str(row.get("order_id") or "")
                status = row.get("fulfillment_status")
            else:
                order_id = str(row[0] or "")
                status = row[1]
            if order_id:
                previous_statuses[order_id] = (datetime.min, normalize_fulfillment_status(status))

    return {order_id: status for order_id, (_ts, status) in previous_statuses.items()}


def load_order_state_rows(cursor, order_ids: List[str]) -> Dict[Tuple[str, str, int, int], Dict[str, Any]]:
    if not order_ids:
        return {}

    state_by_key: Dict[Tuple[str, str, int, int], Dict[str, Any]] = {}
    for values in chunked(order_ids, db_read_chunk_size()):
        placeholders = ", ".join(["%s"] * len(values))
        cursor.execute(
            f"""
            SELECT
                order_id,
                metric_date,
                product_id,
                variant_id,
                quantity,
                ordered_counted,
                fulfilled_counted,
                current_fulfillment_status
            FROM product_fill_rate_order_state
            WHERE order_id IN ({placeholders})
            """,
            tuple(values),
        )
        for row in cursor.fetchall():
            if isinstance(row, dict):
                order_id = str(row["order_id"])
                metric_date = str(row["metric_date"])
                product_id = int(row["product_id"])
                variant_id = normalize_variant_id(row.get("variant_id"))
                quantity = int(row.get("quantity") or 0)
                ordered_counted = int(row.get("ordered_counted") or 0)
                fulfilled_counted = int(row.get("fulfilled_counted") or 0)
                current_status = normalize_fulfillment_status(row.get("current_fulfillment_status"))
            else:
                order_id = str(row[0])
                metric_date = str(row[1])
                product_id = int(row[2])
                variant_id = normalize_variant_id(row[3])
                quantity = int(row[4] or 0)
                ordered_counted = int(row[5] or 0)
                fulfilled_counted = int(row[6] or 0)
                current_status = normalize_fulfillment_status(row[7])

            state_by_key[(order_id, metric_date, product_id, variant_id)] = {
                "quantity": quantity,
                "ordered_counted": ordered_counted,
                "fulfilled_counted": fulfilled_counted,
                "current_fulfillment_status": current_status,
            }

    return state_by_key


def upsert_order_state_rows(core, cursor, connection, rows: List[Tuple[Any, ...]], brand_name: str):
    if not rows:
        return
    try:
        core.executemany_chunked(
            cursor,
            connection,
            """
            INSERT INTO product_fill_rate_order_state (
                order_id,
                metric_date,
                product_id,
                variant_id,
                quantity,
                ordered_counted,
                fulfilled_counted,
                current_fulfillment_status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                quantity = VALUES(quantity),
                ordered_counted = VALUES(ordered_counted),
                fulfilled_counted = VALUES(fulfilled_counted),
                current_fulfillment_status = VALUES(current_fulfillment_status),
                updated_at = CURRENT_TIMESTAMP
            """,
            rows,
        )
    except Exception as e:
        core.logger.error(f"❌ [{brand_name}] Failed to upsert fill-rate order state: {e}")
        raise


def fetch_latest_fulfillment_statuses_for_order_ids(
    cursor,
    order_ids: List[str],
) -> Dict[str, str]:
    latest_status: Dict[str, Tuple[datetime, str]] = {}
    if not order_ids:
        return {}

    for values in chunked(order_ids, db_read_chunk_size()):
        placeholders = ", ".join(["%s"] * len(values))
        cursor.execute(
            f"""
            SELECT order_id, fulfillment_status, updated_at
            FROM shopify_orders_update
            WHERE order_id IN ({placeholders})
            """,
            tuple(values),
        )
        for row in cursor.fetchall():
            if isinstance(row, dict):
                order_id = str(row.get("order_id") or "")
                status = normalize_fulfillment_status(row.get("fulfillment_status"))
                ts_value = row.get("updated_at") or datetime.min
            else:
                order_id = str(row[0] or "")
                status = normalize_fulfillment_status(row[1])
                ts_value = row[2] or datetime.min
            if not order_id:
                continue
            prev = latest_status.get(order_id)
            if prev is None or ts_value >= prev[0]:
                latest_status[order_id] = (ts_value, status)

    missing_ids = [oid for oid in order_ids if oid not in latest_status]
    if missing_ids:
        for values in chunked(missing_ids, db_read_chunk_size()):
            placeholders = ", ".join(["%s"] * len(values))
            cursor.execute(
                f"""
                SELECT order_id, fulfillment_status, created_at
                FROM shopify_orders
                WHERE order_id IN ({placeholders})
                """,
                tuple(values),
            )
            for row in cursor.fetchall():
                if isinstance(row, dict):
                    order_id = str(row.get("order_id") or "")
                    status = normalize_fulfillment_status(row.get("fulfillment_status"))
                else:
                    order_id = str(row[0] or "")
                    status = normalize_fulfillment_status(row[1])
                if order_id and order_id not in latest_status:
                    latest_status[order_id] = (datetime.min, status)

    return {order_id: status for order_id, (_ts, status) in latest_status.items()}
