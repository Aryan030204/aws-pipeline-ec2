"""
Standalone worker: rebuilds variant_wise_events_summary (dynamic per-event-type
counts, keyed by (date, variant_id)) for every CI-events-enabled brand, in that
brand's own database.

Unlike workers/events_summary_worker.py (a single master-DB table keyed by
(date, shop_name), fully self-contained with its own Mongo/DB config), this
worker writes into each brand's own database - the same place
daily_ci_events_summary/daily_buy_now_events_summary already live - so it
reuses the main pipeline's existing brand infrastructure (aws_background)
rather than reimplementing per-brand DB config loading. Reads from that
brand's own mongo_events_summary table (MySQL), not Mongo directly -
pipeline/mongo_events_raw.py is the only thing that still queries
alerts.sessions. The fetch/dedup/aggregate/upsert logic lives in
pipeline/mongo_events_sql.py (sync_variant_wise_events_summary_sql) and is
shared with the rest of the pipeline; this file is just the
scheduling/orchestration wrapper that runs it, per brand, on its own
schedule.

Business logic (dedup rule, dynamic column discovery) is not duplicated here.
"""

import logging
import os
import sys
import tempfile
from contextlib import contextmanager
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

# When run directly (`python workers/variant_wise_events_worker.py`, which is
# how the Dockerfile/CMD invokes it), Python sets sys.path[0] to this script's
# own directory (workers/), not the repo root - so aws_background.py (at the
# repo root) isn't importable without this. Same pattern every script under
# backfill scripts/ already uses to reach aws_background.py.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

import aws_background

LOCK_PATH = os.environ.get(
    "VARIANT_WISE_LOCK_PATH",
    os.path.join(tempfile.gettempdir(), "variant_wise_events_worker.lock"),
)


@contextmanager
def _single_instance_lock(path: Optional[str] = None):
    """Non-blocking exclusive OS file lock; yields True if acquired, False if
    another process already holds it. The kernel drops the lock if the process
    dies, so a crash can't leave it stuck."""
    handle = open(path or LOCK_PATH, "a+")
    try:
        try:
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            yield False
            return
        try:
            yield True
        finally:
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    finally:
        handle.close()


def compute_rebuild_dates(tz: ZoneInfo, current_dt: Optional[datetime] = None) -> List[date]:
    """
    Same "yesterday + day-before-yesterday" 2-day rolling rebuild window as
    events_summary_worker.compute_rebuild_dates, computed in the given
    brand's own store timezone (catches late-arriving Mongo events).
    """
    local_now = current_dt.astimezone(tz) if current_dt else datetime.now(tz)
    today = local_now.date()
    return [today - timedelta(days=2), today - timedelta(days=1)]


def run_worker(current_dt: Optional[datetime] = None) -> None:
    with _single_instance_lock() as acquired:
        if not acquired:
            logger.warning(
                "Variant-wise events worker already running (lock %s held); exiting without scanning.",
                LOCK_PATH,
            )
            return
        _run_rebuild(current_dt)


def _run_rebuild(current_dt: Optional[datetime] = None) -> None:
    aws_background.initialize_brand_configs()

    if not aws_background.active_brand_indices:
        logger.warning("No active brands with valid database configs. Nothing to do.")
        return

    for brand_index in aws_background.active_brand_indices:
        brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
        brand_key = os.environ.get(f"BRAND_TAG_{brand_index}")

        if not aws_background.is_ci_events_brand(brand_name, brand_key):
            logger.info(
                "Skipping variant-wise events summary for %s: brand not enabled for ci_events",
                brand_name,
            )
            continue

        tz = aws_background.get_brand_timezone(brand_index)
        target_dates = compute_rebuild_dates(tz, current_dt=current_dt)
        start_date, end_date = min(target_dates), max(target_dates)

        logger.info(
            "Rebuilding variant-wise events summary for %s: %s..%s (%s)",
            brand_name,
            start_date,
            end_date,
            tz,
        )
        try:
            aws_background.sync_variant_wise_events_summary_sql(
                brand_index=brand_index,
                brand_name=brand_name,
                start_date=start_date,
                end_date=end_date,
                brand_key=brand_key,
            )
        except Exception as e:
            logger.error("Error rebuilding variant-wise events summary for %s: %s", brand_name, e)

    logger.info("Variant-wise events worker complete.")


def _run_scheduled() -> None:
    """
    Long-running mode for the standalone container: runs run_worker once a
    day at 00:17 IST (the window it rebuilds is the two closed days D-2..D-1,
    so more frequent runs only re-scan identical data). No run at startup, so
    a container restart can't trigger the full-table scan. Set
    VARIANT_WISE_EVENTS_WORKER_SELF_RUN=false to instead run once and exit
    (e.g. for a one-off/manual invocation).
    """
    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler(timezone="Asia/Kolkata")
    scheduler.add_job(
        run_worker,
        trigger="cron",
        hour=0,
        minute=17,
        timezone="Asia/Kolkata",
        id="variant_wise_events_worker_job",
        coalesce=True,
        max_instances=1,
        misfire_grace_time=6 * 3600,
    )
    logger.info("Variant-wise events worker scheduler started (daily at 00:17 IST)")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Variant-wise events worker scheduler stopped")


if __name__ == "__main__":
    self_run = (
        os.environ.get("VARIANT_WISE_EVENTS_WORKER_SELF_RUN", "true").strip().lower() == "true"
    )
    if self_run:
        _run_scheduled()
    else:
        run_worker()
