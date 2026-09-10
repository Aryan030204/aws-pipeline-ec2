"""
Standalone worker: rebuilds daily_checkout_user_summary and
daily_checkout_user_variant_summary (checkout counts split by logged-in vs
non-logged-in, using login-event cart_token matching) for every
CI-events-enabled brand, in that brand's own database.

Same shape as workers/variant_wise_events_worker.py: writes into each
brand's own database, so it reuses the main pipeline's existing brand
infrastructure (aws_background) rather than reimplementing per-brand DB
config loading. The actual Mongo fetch/dedup/classify/upsert logic lives in
pipeline/checkout_login_status.py (sync_daily_checkout_login_status) and is
shared with the rest of the pipeline; this file is just the
scheduling/orchestration wrapper that runs it, per brand, on its own
schedule.

Business logic (checkout dedup, login classification, idempotent upserts)
is not duplicated here.
"""

import logging
import os
import sys
from datetime import date, datetime, timedelta
from typing import List, Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# When run directly (`python workers/checkout_login_status_worker.py`, which
# is how the Dockerfile/CMD invokes it), Python sets sys.path[0] to this
# script's own directory (workers/), not the repo root - so aws_background.py
# (at the repo root) isn't importable without this. Same pattern every
# script under backfill scripts/ and workers/variant_wise_events_worker.py
# already use to reach aws_background.py.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

import aws_background


def compute_rebuild_dates(tz: ZoneInfo, current_dt: Optional[datetime] = None) -> List[date]:
    """
    "Yesterday + day-before-yesterday" 2-day rolling rebuild window,
    computed in the given brand's own store timezone - the mandated D-1/D-2
    processing window (today is deliberately excluded; today's data is
    still being accumulated).
    """
    local_now = current_dt.astimezone(tz) if current_dt else datetime.now(tz)
    today = local_now.date()
    return [today - timedelta(days=2), today - timedelta(days=1)]


def run_worker(current_dt: Optional[datetime] = None) -> None:
    aws_background.initialize_brand_configs()

    if not aws_background.active_brand_indices:
        logger.warning("No active brands with valid database configs. Nothing to do.")
        return

    for brand_index in aws_background.active_brand_indices:
        brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
        brand_key = os.environ.get(f"BRAND_TAG_{brand_index}")

        if not aws_background.is_ci_events_brand(brand_name, brand_key):
            logger.info(
                "Skipping checkout login-status rollup for %s: brand not enabled for ci_events",
                brand_name,
            )
            continue

        tz = aws_background.get_brand_timezone(brand_index)
        target_dates = compute_rebuild_dates(tz, current_dt=current_dt)
        start_date, end_date = min(target_dates), max(target_dates)

        logger.info(
            "Rebuilding checkout login-status rollup for %s: %s..%s (%s)",
            brand_name,
            start_date,
            end_date,
            tz,
        )
        try:
            aws_background.sync_daily_checkout_login_status(
                brand_index=brand_index,
                brand_name=brand_name,
                start_date=start_date,
                end_date=end_date,
                brand_key=brand_key,
            )
        except Exception as e:
            logger.error("Error rebuilding checkout login-status rollup for %s: %s", brand_name, e)

    logger.info("Checkout login-status worker complete.")


def _run_scheduled() -> None:
    """
    Long-running mode for the standalone container: schedules run_worker
    daily at 00:05 IST (30 18 * * * UTC is 00:00 IST for the other two
    workers; this one runs 5 minutes later - 35 18 * * * UTC - independent
    containers, no real conflict), then blocks. Set
    CHECKOUT_LOGIN_STATUS_WORKER_SELF_RUN=false to instead run once and exit
    (e.g. for a one-off/manual invocation).
    """
    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        run_worker,
        trigger="cron",
        hour=18,
        minute=35,
        timezone="UTC",
        id="checkout_login_status_worker_job",
        coalesce=True,
        max_instances=1,
        misfire_grace_time=3600,
    )
    logger.info(
        "Checkout login-status worker scheduler started (cron 35 18 * * * UTC / 00:05 IST)"
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Checkout login-status worker scheduler stopped")


if __name__ == "__main__":
    self_run = (
        os.environ.get("CHECKOUT_LOGIN_STATUS_WORKER_SELF_RUN", "true").strip().lower() == "true"
    )
    if self_run:
        _run_scheduled()
    else:
        run_worker()
