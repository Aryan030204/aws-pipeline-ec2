"""
Drops the dashboard's Redis cache entries for the dates a run actually
rewrote.

The dashboard's analytics service serves day-level metrics out of Redis
(`dashboard/analytics/services/metricsCacheService.js`) and only falls back
to MySQL on a miss. Those entries are written by the separate cache
pipeline, which rebuilds after a full ETL run - so a write that lands
OUTSIDE that rebuild's window (a backfill, a manual worker trigger for an
older range) leaves the dashboard serving the pre-write numbers for those
dates even though the tables are correct.

Deleting the affected keys is the fix: the next dashboard read misses and
reads MySQL, and the cache pipeline refills it on its next run. Deletes are
therefore always safe - the worst case is one uncached read.

Key shapes mirror metricsCacheService.js exactly (brand lower-cased):

    metrics:<brand>:<YYYY-MM-DD>          per-day snapshot
    hourly_metrics:<brand>:<YYYY-MM-DD>   per-day hourly breakdown
    date_presets:<brand>                  pre-computed 7/30/90-day, last-month
                                          and MTD totals - any changed date
                                          can fall inside one of those
                                          windows, so it goes with them

Never raises: a cache that can't be reached must not fail an ETL run that
has already written its rows.
"""

import os
from datetime import date
from typing import Iterable, Optional, Sequence, Union

from pipeline.state import logger

try:
    import redis
except ImportError:  # pragma: no cover - dependency is in requirements.txt
    redis = None

DateLike = Union[str, date]

REDIS_URL = os.environ.get("REDIS_URL")
# Deleted in chunks so one backfill can't build a single huge command.
DELETE_BATCH_SIZE = 200
SOCKET_TIMEOUT_S = float(os.environ.get("REDIS_SOCKET_TIMEOUT_S", "5"))

_client = None
_client_initialized = False


def get_redis_client():
    """Lazily builds the shared client. Returns None (and logs once) when
    Redis isn't configured or the dependency is missing, which disables
    invalidation without disabling the pipeline."""
    global _client, _client_initialized

    if _client_initialized:
        return _client

    _client_initialized = True

    if redis is None:
        logger.warning("Cache invalidation disabled: redis package not installed.")
        return None

    if not REDIS_URL:
        logger.info("Cache invalidation disabled: REDIS_URL not set.")
        return None

    try:
        _client = redis.Redis.from_url(
            REDIS_URL,
            socket_timeout=SOCKET_TIMEOUT_S,
            socket_connect_timeout=SOCKET_TIMEOUT_S,
            decode_responses=True,
        )
        logger.info("✅ Redis client initialized for dashboard cache invalidation")
    except Exception as e:
        logger.error(f"❌ Failed to initialize Redis client for cache invalidation: {e}")
        _client = None

    return _client


def _as_iso(value: Optional[DateLike]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    return text or None


def build_cache_keys(brand_key: str, dates: Iterable[DateLike]) -> list:
    """The keys the dashboard would serve for these dates, in delete order.
    Pure - no Redis contact - so it can be asserted on directly."""
    brand = (brand_key or "").strip().lower()
    if not brand:
        return []

    iso_dates = sorted({iso for iso in (_as_iso(d) for d in dates) if iso})
    if not iso_dates:
        return []

    keys = []
    for iso in iso_dates:
        keys.append(f"metrics:{brand}:{iso}")
        keys.append(f"hourly_metrics:{brand}:{iso}")
    keys.append(f"date_presets:{brand}")
    return keys


def invalidate_dates(brand_key: str, dates: Sequence[DateLike], reason: str = "") -> int:
    """Deletes the dashboard cache entries for `dates` of one brand and
    returns how many keys were actually removed. Returns 0 - never raises -
    when Redis is unavailable, TEST_MODE is on, or there is nothing to do."""
    import aws_background

    if aws_background.TEST_MODE:
        logger.info("Skipping dashboard cache invalidation (TEST_MODE)")
        return 0

    keys = build_cache_keys(brand_key, dates)
    if not keys:
        return 0

    client = get_redis_client()
    if client is None:
        return 0

    label = f" ({reason})" if reason else ""
    removed = 0
    try:
        for i in range(0, len(keys), DELETE_BATCH_SIZE):
            removed += client.delete(*keys[i : i + DELETE_BATCH_SIZE])
    except Exception as e:
        logger.error(
            f"❌ Dashboard cache invalidation failed for {brand_key}{label}: {e}"
        )
        return removed

    logger.info(
        f"🧹 Invalidated dashboard cache for {brand_key}{label}: "
        f"{removed}/{len(keys)} key(s) removed"
    )
    return removed
