"""
Standalone worker: monthly data purge. For every active brand database it archives rows older than the
6-calendar-month cutoff to S3 (CSV + manifest per brand/table), verifies the archive, and - only when
explicitly allowed - deletes the archived rows in batches.

All logic (cutoff, config modes, live schema validation, streaming export, S3 manifest protocol, batched
capped deletes) lives in pipeline/data_purge.py; this file is the scheduling/orchestration wrapper in the
same shape as the other workers.

SAFE BY DEFAULT: DATA_PURGE_ENABLED=false, DATA_PURGE_ARCHIVE_ONLY=true, DATA_PURGE_DELETE_TABLES empty.
Deploying this worker does nothing destructive. See the README ("Data purge worker") for the modes, the
dry-run, the rollout procedure and the operational invariant (old rows must not be mutated while it runs).
"""

import functools
import logging
import os
import sys
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime
from typing import List, Optional

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Run directly (`python workers/data_purge_worker.py`), sys.path[0] is workers/, not the repo root.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

import aws_background
from pipeline import data_purge

LOCK_PATH = os.environ.get(
    "DATA_PURGE_LOCK_PATH",
    os.path.join(tempfile.gettempdir(), "data_purge_worker.lock"),
)


@contextmanager
def _single_instance_lock(path: Optional[str] = None):
    """Non-blocking exclusive OS file lock; yields True if acquired, False if another process holds it.
    The kernel drops the lock if the process dies, so a crash can't leave it stuck. (Per container: the
    per-table MySQL GET_LOCK in pipeline/data_purge.py covers the cross-container case.)"""
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


def run_worker(current_dt: Optional[datetime] = None, brand_key: Optional[str] = None) -> None:
    """current_dt: pretend to run at this time (selects the purge month/cutoff). brand_key: restrict to one
    brand (matches the brand index, BRAND_TAG, BRAND_NAME or BRAND_ID, case-insensitive)."""
    with _single_instance_lock() as acquired:
        if not acquired:
            logger.warning("Data purge worker already running (lock %s held); exiting.", LOCK_PATH)
            return
        _run_purge(current_dt, brand_key)


def _select_brands(brand_key: Optional[str]) -> List[data_purge.BrandContext]:
    aws_background.initialize_brand_configs()
    brands: List[data_purge.BrandContext] = []
    wanted = brand_key.strip().upper() if brand_key else None
    for index in aws_background.active_brand_indices:
        name = os.environ.get(f"BRAND_NAME_{index}", f"Brand_{index}")
        tag = os.environ.get(f"BRAND_TAG_{index}") or ""
        brand_id = os.environ.get(f"BRAND_ID_{index}") or ""
        if wanted and wanted not in {str(index), name.upper(), tag.upper(), brand_id.upper()}:
            continue
        brands.append(
            data_purge.BrandContext(
                index=index,
                name=tag or name,  # stable archive folder / manifest brand
                tz=aws_background.get_brand_timezone(index),
                cursor_ctx=functools.partial(aws_background.get_db_cursor, index),
            )
        )
    return brands


def _run_purge(current_dt: Optional[datetime], brand_key: Optional[str]) -> None:
    try:
        cfg = data_purge.load_config()
    except data_purge.ConfigError as exc:
        logger.error("Data purge refused to run - unsafe or invalid configuration: %s", exc)
        raise

    if cfg.mode is data_purge.Mode.DISABLED:
        logger.info(
            "Data purge is disabled (DATA_PURGE_ENABLED is not 'true'); nothing to do. "
            "Set DATA_PURGE_DRY_RUN=true for a read-only report."
        )
        return

    run_dt = current_dt or datetime.now(data_purge.SCHEDULER_TZ)
    cutoff = data_purge.compute_cutoff(run_dt)
    store = None
    if cfg.mode in (data_purge.Mode.ARCHIVE_ONLY, data_purge.Mode.FULL):
        store = data_purge.S3ArchiveStore(cfg.bucket, sse=cfg.sse, kms_key_id=cfg.kms_key_id)

    brands = _select_brands(brand_key)
    if not brands:
        logger.warning("No active brands%s. Nothing to do.", f" matching '{brand_key}'" if brand_key else "")
        return

    run = data_purge.RunContext(
        cfg=cfg,
        run_dt=run_dt,
        cutoff=cutoff,
        month=data_purge.archive_month_label(run_dt),
        store=store,
        env=os.environ,
    )
    started = time.monotonic()
    results = data_purge.run_purge(brands, run)
    summary = data_purge.format_summary(results, run, time.monotonic() - started)
    logger.info("\n%s", summary)

    if data_purge.any_failure(results):
        raise RuntimeError("Data purge finished with failures; see the summary above (nothing is deleted without a verified archive).")
    logger.info("Data purge worker complete.")


def _purge_test_enabled() -> bool:
    """True only for an explicit PURGE_TEST=true (same strict parsing as the config; anything else is False here
    and is rejected by the config when a run is attempted)."""
    return os.environ.get("PURGE_TEST", "").strip().lower() == "true"


def _run_startup_test_pass() -> None:
    """PURGE_TEST=true: one run right after every container (re)start, so an export test does not have to wait for
    the 1st of the month. It can never delete (PURGE_TEST caps the mode at archive-only) and it still needs
    DATA_PURGE_ENABLED=true and a bucket. Any failure is logged and swallowed: the container must stay up (it is
    restart: always, so an exception here would otherwise re-run the export on every crash-restart)."""
    logger.info("PURGE_TEST=true: running one export-only pass now (startup); the monthly schedule stays active")
    try:
        run_worker()
    except Exception as exc:
        logger.error("PURGE_TEST startup pass failed (the monthly schedule is still started): %s", exc)


def _run_scheduled() -> None:
    """
    Long-running mode for the standalone container: runs run_worker on the 1st of every month at 03:23 IST.
    Nothing runs at startup - EXCEPT when PURGE_TEST=true, which runs one export-only pass (never a delete) right
    away on every container (re)start, before the scheduler starts. misfire_grace_time is a full day: if the
    container was down at the trigger instant the (coalesced) run still happens after it starts - the cutoff
    depends only on the month, so a late run is identical. Set DATA_PURGE_WORKER_SELF_RUN=false to run once and
    exit instead (what `python -m workers.run data-purge` does).
    """
    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler(timezone="Asia/Kolkata")
    scheduler.add_job(
        run_worker,
        trigger="cron",
        day=1,
        hour=3,
        minute=23,
        timezone="Asia/Kolkata",
        id="data_purge_worker_job",
        coalesce=True,
        max_instances=1,
        misfire_grace_time=24 * 3600,
    )
    logger.info("Data purge worker scheduler started (1st of every month at 03:23 IST)")
    if _purge_test_enabled():
        _run_startup_test_pass()
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Data purge worker scheduler stopped")


if __name__ == "__main__":
    self_run = os.environ.get("DATA_PURGE_WORKER_SELF_RUN", "true").strip().lower() == "true"
    if self_run:
        _run_scheduled()
    else:
        run_worker()
