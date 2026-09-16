"""
Standalone worker: builds the daily brand-level P&L (overall_pnl) in each
brand's own database - overall only, not per product.

The only table this worker ever creates or writes is `overall_pnl`. Every
other table it touches (gross_summary, discount_summary, overall_summary,
rto_summary, returns_fact, pnl_meta_ad_spend_rollup) is read-only here and
owned by the main pipeline / the Meta sync.

Reads and writes both use the brand's own database connection (the
db_host/db_user/... the brand-config API returns for that tenant, loaded by
aws_background.initialize_brand_configs()), exactly like the other
per-brand workers. Nothing about the target is hard-coded or env-driven.
Rows still carry `brand_key` so the table is self-describing.

Revenue-side figures come from the summary tables the main pipeline already
maintains in that brand DB; cost-side figures come from the Mongo
`pnl_config` collection (one document per brand+category+effective window,
same shape the dashboard's "Brand Cost Configuration" page writes). A brand
with no documents in that collection is costed with DEFAULT_COST_CONFIGS
(percent-of-net-sales assumptions) and its rows are tagged
cost_config_source='default' so the dashboard can tell. Meta ad spend is
taken from pnl_meta_ad_spend_rollup when that table has a synced row for
the day, else from the configured/default `meta` cost.

Per date, for each brand:

    gross_sales   = gross_summary.overall_sale          (line items, before discounts)
    discounts     = discount_summary.actual_discounts   (net of cancelled-order discounts)
    cancellations = returns_fact CANCEL                 (full order value, by order date)
                  + returns_fact REFUND                 (by refund date, only for orders
                                                         that have no CANCEL / CANCEL (RTO)
                                                         row, so a cancel+refund isn't
                                                         counted twice)
    rto_returns   = returns_fact CANCEL (RTO)           (by order date; informational)
    gst           = (gross - discounts - cancellations) * 18/118
    net_sales     = gross - discounts - cancellations - gst
    gross_margin  = net_sales - cogs - freight_inwards
    cm1           = gross_margin - shipping - rto - payment_gateway - packaging
    cm2           = cm1 - meta - google - other_paid
    cm3           = cm2 - influencers - content - sponsorships - other_brand
    ebitda        = cm3 - salaries - rent - technology - agency_fees - other_overheads

RTO follows the same convention as sales_summary: an RTO'd order is not a
cancellation (its revenue stays in gross/net), it's surfaced separately as
rto_orders / rto_returns, and the `rto` line is the configured RTO *cost*.
CANCEL and CANCEL (RTO) are mutually exclusive event types in returns_fact
(a cancelled order gets exactly one of them), so they are never combined.

A configured cost resolves to a daily amount as:
    percentage          -> value% of that day's net_sales
    flat + recurring    -> a monthly amount, prorated per calendar day
    flat + one_time     -> the full value on effective_from only

Same shape as the other per-brand-DB workers (checkout_login_status_worker
etc): brand discovery and DB access come from aws_background / pipeline.db.
"""

import calendar
import logging
import os
import sys
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# When run directly (`python workers/pnl_worker.py`, which is how the
# Dockerfile/CMD invokes it), Python sets sys.path[0] to workers/, not the
# repo root - same fix every other worker in this repo uses.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

PNL_TABLE = "overall_pnl"  # the only table this worker creates / writes
META_SPEND_TABLE = "pnl_meta_ad_spend_rollup"  # read-only

# The cost configs live in the arch-auth database, which is not necessarily
# the cluster the pipeline's MONGO_URI (events) points at.
PNL_CONFIG_MONGO_URI = (
    os.environ.get("PNL_CONFIG_MONGO_URI", "").strip()
    or os.environ.get("MONGO_URI", "").strip()
)
PNL_CONFIG_MONGO_DB = os.environ.get("PNL_CONFIG_MONGO_DB", "arch-auth").strip() or "arch-auth"
PNL_CONFIG_COLLECTION = (
    os.environ.get("PNL_CONFIG_COLLECTION", "pnl_config").strip() or "pnl_config"
)

# Rolling rebuild window (yesterday and the N-1 days before it), or an explicit
# range for backfills.
PNL_WORKER_LOOKBACK_DAYS = int(os.environ.get("PNL_WORKER_LOOKBACK_DAYS", "2"))
PNL_WORKER_START_DATE = os.environ.get("PNL_WORKER_START_DATE", "").strip()
PNL_WORKER_END_DATE = os.environ.get("PNL_WORKER_END_DATE", "").strip()

GST_RATE = Decimal(18) / Decimal(118)
CENT = Decimal("0.01")
ZERO = Decimal("0")

VALUE_TYPES = {"flat", "percentage"}

CONFIG_SOURCE_MONGO = "mongo"
CONFIG_SOURCE_DEFAULT = "default"

# Cost lines a Mongo config category can populate, in P&L order. Keys are
# both the Mongo `category` and the overall_pnl column.
COST_COLUMNS = (
    "cogs",
    "freight_inwards",
    "shipping",
    "rto",
    "payment_gateway",
    "packaging",
    "meta",
    "google",
    "other_paid",
    "influencers",
    "content",
    "sponsorships",
    "other_brand",
    "salaries",
    "rent",
    "technology",
    "agency_fees",
    "other_overheads",
)

# Used for a brand that has no documents in the pnl_config collection: each
# cost is a percentage of that day's net sales. Every COST_COLUMNS entry has
# an entry here so a defaulted row is fully populated.
DEFAULT_COST_PERCENTAGES: Dict[str, str] = {
    "cogs": "32",
    "freight_inwards": "2",
    "shipping": "6",
    "rto": "3",
    "payment_gateway": "2",
    "packaging": "1.5",
    "meta": "8",
    "google": "5",
    "other_paid": "1",
    "influencers": "2",
    "content": "1",
    "sponsorships": "0.5",
    "other_brand": "0.5",
    "salaries": "6",
    "rent": "1",
    "technology": "1",
    "agency_fees": "1",
    "other_overheads": "1",
}

MONEY_COLUMNS = (
    "gross_sales",
    "discounts",
    "cancellations",
    "gst",
    "net_sales",
    "cogs",
    "freight_inwards",
    "gross_margin",
    "shipping",
    "rto",
    "payment_gateway",
    "packaging",
    "cm1",
    "meta",
    "google",
    "other_paid",
    "cm2",
    "influencers",
    "content",
    "sponsorships",
    "other_brand",
    "cm3",
    "salaries",
    "rent",
    "technology",
    "agency_fees",
    "other_overheads",
    "ebitda",
    "rto_returns",
    "cod_sales",
    "prepaid_sales",
    "partially_prepaid_sales",
)
INT_COLUMNS = ("total_orders", "rto_orders", "meta_spend_synced")
TEXT_COLUMNS = ("cost_config_source",)
KEY_COLUMNS = ("brand_key", "date")
ALL_COLUMNS = (*KEY_COLUMNS, *MONEY_COLUMNS, *INT_COLUMNS, *TEXT_COLUMNS)


def money(value) -> Decimal:
    return Decimal(str(value if value is not None else 0)).quantize(CENT, rounding=ROUND_HALF_UP)


def compute_target_dates(
    tz: ZoneInfo,
    current_dt: Optional[datetime] = None,
    lookback_days: int = PNL_WORKER_LOOKBACK_DAYS,
    start_date: str = PNL_WORKER_START_DATE,
    end_date: str = PNL_WORKER_END_DATE,
) -> List[date]:
    if start_date and end_date:
        start, end = date.fromisoformat(start_date), date.fromisoformat(end_date)
        return [start + timedelta(days=i) for i in range((end - start).days + 1)]

    local_now = current_dt.astimezone(tz) if current_dt else datetime.now(tz)
    yesterday = local_now.date() - timedelta(days=1)
    return [yesterday - timedelta(days=i) for i in range(max(1, lookback_days) - 1, -1, -1)]


def resolve_brand_key(brand_index: int) -> str:
    """The dashboard keys configs by the tenant's brand_id ("BBB"), which is
    this pipeline's brand tag upper-cased."""
    tag = os.environ.get(f"BRAND_TAG_{brand_index}") or os.environ.get(f"BRAND_NAME_{brand_index}", "")
    return tag.strip().upper()


# ---------------------------
# Cost configs (Mongo)
# ---------------------------
def default_cost_configs() -> List[Dict]:
    """One percentage-of-net-sales config per cost line, effective forever."""
    return [
        {
            "category": category,
            "value_type": "percentage",
            "value": pct,
            "frequency": "recurring",
            "effective_from": "1970-01-01",
            "effective_to": None,
        }
        for category, pct in DEFAULT_COST_PERCENTAGES.items()
    ]


def fetch_brand_cost_configs(brand_key: str) -> List[Dict]:
    """Active pnl_config documents for the brand. Empty when the brand has
    none (or the collection doesn't exist yet - Mongo treats both the same).
    Raises on a real connectivity/auth failure so a brand isn't silently
    re-costed with defaults because Mongo hiccupped."""
    from pymongo import MongoClient

    if not PNL_CONFIG_MONGO_URI:
        logger.warning(
            "PNL_CONFIG_MONGO_URI / MONGO_URI not configured; using default cost configs for %s",
            brand_key,
        )
        return []

    client = MongoClient(PNL_CONFIG_MONGO_URI, tz_aware=True)
    try:
        collection = client[PNL_CONFIG_MONGO_DB][PNL_CONFIG_COLLECTION]
        return list(
            collection.find(
                {"brand_id": brand_key, "is_active": True},
                {
                    "category": 1,
                    "value_type": 1,
                    "value": 1,
                    "frequency": 1,
                    "effective_from": 1,
                    "effective_to": 1,
                },
            )
        )
    finally:
        client.close()


def resolve_brand_cost_configs(brand_key: str) -> Tuple[List[Dict], str]:
    """(configs, source): the brand's Mongo configs, or the defaults when it
    has none in pnl_config."""
    configs = fetch_brand_cost_configs(brand_key)
    if configs:
        return configs, CONFIG_SOURCE_MONGO
    logger.info(
        "No %s.%s documents for brand_id=%s; using DEFAULT_COST_PERCENTAGES",
        PNL_CONFIG_MONGO_DB,
        PNL_CONFIG_COLLECTION,
        brand_key,
    )
    return default_cost_configs(), CONFIG_SOURCE_DEFAULT


def _config_date(value) -> Optional[date]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def configs_effective_on(configs: Iterable[Dict], day: date) -> Dict[str, Dict]:
    """One config per category for the given day - where windows overlap, the
    latest effective_from wins (same rule as the dashboard's summary)."""
    chosen: Dict[str, Dict] = {}
    for config in configs:
        category = str(config.get("category") or "").strip()
        if category not in COST_COLUMNS or config.get("value_type") not in VALUE_TYPES:
            continue
        effective_from = _config_date(config.get("effective_from"))
        effective_to = _config_date(config.get("effective_to"))
        if effective_from is None or effective_from > day:
            continue
        if effective_to is not None and effective_to < day:
            continue
        current = chosen.get(category)
        if current is None or effective_from > _config_date(current.get("effective_from")):
            chosen[category] = config
    return chosen


def configured_amount(config: Dict, day: date, net_sales: Decimal) -> Decimal:
    value = Decimal(str(config.get("value") or 0))
    if config.get("value_type") == "percentage":
        return money(net_sales * value / Decimal(100))
    if config.get("frequency") == "one_time":
        return money(value) if _config_date(config.get("effective_from")) == day else ZERO
    return money(value / Decimal(calendar.monthrange(day.year, day.month)[1]))


# ---------------------------
# Revenue-side sources (brand DB)
# ---------------------------
def fetch_sales_rows(cursor, start: date, end: date) -> Dict[date, Dict]:
    cursor.execute(
        """
        SELECT
            gs.date,
            COALESCE(gs.overall_sale, 0)            AS gross_sales,
            COALESCE(ds.actual_discounts, 0)        AS discounts,
            COALESCE(os.total_orders, 0)            AS total_orders,
            COALESCE(os.cod_sales, 0)               AS cod_sales,
            COALESCE(os.prepaid_sales, 0)           AS prepaid_sales,
            COALESCE(os.partially_prepaid_sales, 0) AS partially_prepaid_sales,
            COALESCE(rs.rto_orders, 0)              AS rto_orders
        FROM gross_summary gs
        LEFT JOIN discount_summary ds ON ds.date = gs.date
        LEFT JOIN overall_summary  os ON os.date = gs.date
        LEFT JOIN rto_summary      rs ON rs.summary_date = gs.date
        WHERE gs.date BETWEEN %s AND %s
        """,
        (start, end),
    )
    return {_as_date(row["date"]): row for row in cursor.fetchall() or []}


def fetch_returns_rows(cursor, start: date, end: date) -> Dict[date, Dict[str, Decimal]]:
    rows: Dict[date, Dict[str, Decimal]] = {}

    # CANCEL and CANCEL (RTO) are mutually exclusive per order, so each is
    # its own bucket: plain cancels are a revenue deduction, RTO cancels are
    # reported alongside (their revenue stays in gross/net).
    cursor.execute(
        """
        SELECT
            order_created_date AS d,
            SUM(CASE WHEN event_type = 'CANCEL'       THEN amount ELSE 0 END) AS cancel_amount,
            SUM(CASE WHEN event_type = 'CANCEL (RTO)' THEN amount ELSE 0 END) AS rto_amount
        FROM returns_fact
        WHERE order_created_date BETWEEN %s AND %s
          AND event_type IN ('CANCEL', 'CANCEL (RTO)')
        GROUP BY order_created_date
        """,
        (start, end),
    )
    for row in cursor.fetchall() or []:
        day = _as_date(row["d"])
        rows[day] = {
            "cancellations": money(row["cancel_amount"]),
            "rto_returns": money(row["rto_amount"]),
        }

    # Refunds on orders that were never cancelled (a cancel with a refund is
    # already counted in full through its CANCEL row).
    cursor.execute(
        """
        SELECT rf.event_date AS d, SUM(rf.amount) AS refund_amount
        FROM returns_fact rf
        WHERE rf.event_type = 'REFUND'
          AND rf.event_date BETWEEN %s AND %s
          AND NOT EXISTS (
              SELECT 1 FROM returns_fact c
              WHERE c.order_id = rf.order_id
                AND c.event_type IN ('CANCEL', 'CANCEL (RTO)')
          )
        GROUP BY rf.event_date
        """,
        (start, end),
    )
    for row in cursor.fetchall() or []:
        day = _as_date(row["d"])
        entry = rows.setdefault(day, {"cancellations": ZERO, "rto_returns": ZERO})
        entry["cancellations"] += money(row["refund_amount"])

    return rows


def fetch_meta_spend_rows(cursor, start: date, end: date) -> Dict[date, Decimal]:
    """Empty when the rollup table hasn't been created for this brand yet."""
    cursor.execute("SHOW TABLES LIKE %s", (META_SPEND_TABLE,))
    if not cursor.fetchone():
        return {}
    cursor.execute(
        f"SELECT `date`, spend FROM `{META_SPEND_TABLE}` WHERE `date` BETWEEN %s AND %s",
        (start, end),
    )
    return {_as_date(row["date"]): money(row["spend"]) for row in cursor.fetchall() or []}


def _as_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


# ---------------------------
# P&L math
# ---------------------------
def compute_pnl_row(
    day: date,
    sales: Optional[Dict],
    returns: Optional[Dict[str, Decimal]],
    meta_spend: Optional[Decimal],
    configs: Iterable[Dict],
    config_source: str = CONFIG_SOURCE_MONGO,
    brand_key: str = "",
) -> Dict[str, object]:
    sales = sales or {}
    returns = returns or {}

    gross_sales = money(sales.get("gross_sales"))
    discounts = money(sales.get("discounts"))
    cancellations = money(returns.get("cancellations"))
    gst = money((gross_sales - discounts - cancellations) * GST_RATE)
    net_sales = gross_sales - discounts - cancellations - gst

    active = configs_effective_on(configs, day)
    cost = {
        column: (configured_amount(active[column], day, net_sales) if column in active else ZERO)
        for column in COST_COLUMNS
    }
    meta_spend_synced = meta_spend is not None
    if meta_spend_synced:
        cost["meta"] = money(meta_spend)

    gross_margin = net_sales - cost["cogs"] - cost["freight_inwards"]
    cm1 = gross_margin - cost["shipping"] - cost["rto"] - cost["payment_gateway"] - cost["packaging"]
    cm2 = cm1 - cost["meta"] - cost["google"] - cost["other_paid"]
    cm3 = cm2 - cost["influencers"] - cost["content"] - cost["sponsorships"] - cost["other_brand"]
    ebitda = (
        cm3
        - cost["salaries"]
        - cost["rent"]
        - cost["technology"]
        - cost["agency_fees"]
        - cost["other_overheads"]
    )

    return {
        "brand_key": brand_key,
        "date": day,
        "gross_sales": gross_sales,
        "discounts": discounts,
        "cancellations": cancellations,
        "gst": gst,
        "net_sales": net_sales,
        **cost,
        "gross_margin": gross_margin,
        "cm1": cm1,
        "cm2": cm2,
        "cm3": cm3,
        "ebitda": ebitda,
        "rto_returns": money(returns.get("rto_returns")),
        "cod_sales": money(sales.get("cod_sales")),
        "prepaid_sales": money(sales.get("prepaid_sales")),
        "partially_prepaid_sales": money(sales.get("partially_prepaid_sales")),
        "total_orders": int(sales.get("total_orders") or 0),
        "rto_orders": int(sales.get("rto_orders") or 0),
        "meta_spend_synced": int(meta_spend_synced),
        "cost_config_source": config_source,
    }


# ---------------------------
# Target table (brand DB)
# ---------------------------
def ensure_overall_pnl_table(cursor, connection) -> None:
    """Creates overall_pnl if missing. This is the only DDL the worker runs."""
    money_defs = ",\n".join(f"    `{col}` DECIMAL(14,2) NOT NULL DEFAULT 0" for col in MONEY_COLUMNS)
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{PNL_TABLE}` (
            `brand_key` VARCHAR(64) NOT NULL,
            `date` DATE NOT NULL,
        {money_defs},
            `total_orders` INT NOT NULL DEFAULT 0,
            `rto_orders` INT NOT NULL DEFAULT 0,
            `meta_spend_synced` TINYINT(1) NOT NULL DEFAULT 0,
            `cost_config_source` VARCHAR(16) NOT NULL DEFAULT 'mongo',
            `computed_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`brand_key`, `date`),
            KEY `idx_overall_pnl_date` (`date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    connection.commit()


def upsert_pnl_rows(cursor, connection, rows: Sequence[Dict[str, object]]) -> int:
    if not rows:
        return 0
    from pipeline.db import executemany_chunked

    columns = ", ".join(f"`{col}`" for col in ALL_COLUMNS)
    placeholders = ", ".join(["%s"] * len(ALL_COLUMNS))
    updates = ", ".join(f"`{col}` = VALUES(`{col}`)" for col in ALL_COLUMNS if col not in KEY_COLUMNS)
    sql = (
        f"INSERT INTO `{PNL_TABLE}` ({columns}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {updates}"
    )
    values = [tuple(row[col] for col in ALL_COLUMNS) for row in rows]
    return executemany_chunked(cursor, connection, sql, values)


# ---------------------------
# Orchestration
# ---------------------------
def rebuild_brand_pnl(
    brand_index: int,
    brand_name: str,
    brand_key: str,
    target_dates: Sequence[date],
) -> int:
    from pipeline.db import get_db_cursor

    start, end = min(target_dates), max(target_dates)
    configs, config_source = resolve_brand_cost_configs(brand_key)
    logger.info(
        "[%s] %s cost config(s) for brand_id=%s (source=%s)",
        brand_name,
        len(configs),
        brand_key,
        config_source,
    )

    with get_db_cursor(brand_index, dictionary=True) as (cursor, connection):
        # Reads: the source tables are only ever SELECTed here.
        sales_rows = fetch_sales_rows(cursor, start, end)
        returns_rows = fetch_returns_rows(cursor, start, end)
        meta_rows = fetch_meta_spend_rows(cursor, start, end)

        rows = [
            compute_pnl_row(
                day,
                sales_rows.get(day),
                returns_rows.get(day),
                meta_rows.get(day),
                configs,
                config_source,
                brand_key=brand_key,
            )
            for day in target_dates
        ]

        # Writes: overall_pnl only.
        ensure_overall_pnl_table(cursor, connection)
        written = upsert_pnl_rows(cursor, connection, rows)

    for row in rows:
        logger.info(
            "[%s] %s gross=%s cancel=%s net=%s cogs=%s meta=%s%s ebitda=%s costs=%s",
            brand_name,
            row["date"].isoformat(),
            row["gross_sales"],
            row["cancellations"],
            row["net_sales"],
            row["cogs"],
            row["meta"],
            "" if row["meta_spend_synced"] else " (not synced)",
            row["ebitda"],
            row["cost_config_source"],
        )
    return written


def run_worker(current_dt: Optional[datetime] = None) -> None:
    import aws_background

    aws_background.initialize_brand_configs()
    if not aws_background.active_brand_indices:
        logger.warning("No active brands with valid database configs. Nothing to do.")
        return

    for brand_index in aws_background.active_brand_indices:
        brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
        brand_key = resolve_brand_key(brand_index)
        tz = aws_background.get_brand_timezone(brand_index)
        target_dates = compute_target_dates(tz, current_dt=current_dt)

        logger.info(
            "Rebuilding P&L for %s (brand_id=%s): %s..%s (%s)",
            brand_name,
            brand_key,
            target_dates[0],
            target_dates[-1],
            tz,
        )
        try:
            written = rebuild_brand_pnl(brand_index, brand_name, brand_key, target_dates)
            logger.info("[%s] Upserted %s %s row(s)", brand_name, written, PNL_TABLE)
        except Exception as e:
            logger.error("Error rebuilding P&L for %s: %s", brand_name, e)

    logger.info("P&L worker complete.")


def _run_scheduled() -> None:
    """
    Long-running mode for the standalone container: daily at 00:10 IST
    (40 18 * * * UTC), after the other daily workers have rolled up the
    tables this one reads. Set PNL_WORKER_SELF_RUN=false to run once and
    exit (e.g. for a one-off/manual invocation or a backfill).
    """
    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        run_worker,
        trigger="cron",
        hour=18,
        minute=40,
        timezone="UTC",
        id="pnl_worker_job",
        coalesce=True,
        max_instances=1,
        misfire_grace_time=3600,
    )
    logger.info("P&L worker scheduler started (cron 40 18 * * * UTC / 00:10 IST)")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("P&L worker scheduler stopped")


if __name__ == "__main__":
    self_run = os.environ.get("PNL_WORKER_SELF_RUN", "true").strip().lower() == "true"
    if self_run:
        _run_scheduled()
    else:
        run_worker()
