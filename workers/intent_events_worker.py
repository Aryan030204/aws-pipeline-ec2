"""
Standalone worker: ingests tracking events from the Intent MongoDB cluster
(INTENT_MONGO_URI) into each mapped brand's own database - behavioral events
(intent_sessions.events -> behavioral_events table) and click events
(intent_sessions.click_events -> click_events table), two independent
streams ingested per brand on every run.

Same shape as the other per-brand-DB workers (workers/variant_wise_events_worker.py,
workers/checkout_login_status_worker.py): writes into each brand's own
database, so it reuses the main pipeline's existing brand infrastructure
(aws_background) rather than reimplementing per-brand DB config loading.
The actual Mongo fetch/upsert/watermark logic lives in
pipeline/intent_events.py (sync_intent_events_for_brand,
sync_click_events_for_brand) and is shared with the rest of the pipeline;
this file is just the scheduling/orchestration wrapper that runs both, per
mapped brand, on its own schedule.

Unlike the other workers, brand applicability here is NOT driven by
CI_EVENTS_BRANDS/is_ci_events_brand - it's driven entirely by INTENT_DB_MAP
(a JSON mapping of the Mongo document's brand_id to this pipeline's own
DB_DATABASE_<i> values - the actual per-brand MySQL database name, matched
case-insensitively), a separate and independent gate.
"""

import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# When run directly (`python workers/intent_events_worker.py`, which is how
# the Dockerfile/CMD invokes it), Python sets sys.path[0] to this script's
# own directory (workers/), not the repo root - so aws_background.py (at the
# repo root) isn't importable without this. Same pattern every other worker
# in this repo already uses to reach aws_background.py.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

import aws_background


def _resolve_brand_index_by_db_database(db_database_value: str):
    """
    Case-insensitive match against DB_DATABASE_<i> for every active brand -
    the actual per-brand MySQL database name (set from the brand-config
    source's db_database field during initialize_brand_configs()).
    INTENT_DB_MAP maps a Mongo brand_id straight to that database name, not
    the short brand_tag - so this doesn't reuse brand_tag_to_index_map,
    which is keyed differently.
    """
    target = (db_database_value or "").strip().lower()
    if not target:
        return None
    for brand_index in aws_background.active_brand_indices:
        candidate = os.environ.get(f"DB_DATABASE_{brand_index}", "")
        if candidate.strip().lower() == target:
            return brand_index
    return None


def run_worker() -> None:
    aws_background.initialize_brand_configs()

    intent_db_map = aws_background._parse_intent_db_map()
    if not intent_db_map:
        logger.warning("INTENT_DB_MAP is empty. Nothing to do.")
        return

    for mongo_brand_id, db_database_value in intent_db_map.items():
        brand_index = _resolve_brand_index_by_db_database(db_database_value)
        if brand_index is None:
            logger.warning(
                "Skipping intent events for mongo_brand_id=%s: db_database=%s does not "
                "match any active brand's DB_DATABASE_<i>",
                mongo_brand_id,
                db_database_value,
            )
            continue

        try:
            aws_background.sync_intent_events_for_brand(
                brand_index=brand_index,
                mongo_brand_id=mongo_brand_id,
                brand_label=db_database_value,
            )
        except Exception as e:
            logger.error(
                "Error running intent events sync for mongo_brand_id=%s db_database=%s: %s",
                mongo_brand_id,
                db_database_value,
                e,
            )

        # Independent try/except: a failure ingesting click events must not
        # block behavioral-event ingestion for this brand, or vice versa.
        try:
            aws_background.sync_click_events_for_brand(
                brand_index=brand_index,
                mongo_brand_id=mongo_brand_id,
                brand_label=db_database_value,
            )
        except Exception as e:
            logger.error(
                "Error running click events sync for mongo_brand_id=%s db_database=%s: %s",
                mongo_brand_id,
                db_database_value,
                e,
            )

    logger.info("Intent events worker complete.")


def _run_scheduled() -> None:
    """
    Long-running mode for the standalone container: schedules run_worker
    every 15 minutes via an IntervalTrigger (uniform cadence, anchored to
    when the scheduler process starts - not fixed clock times like the
    other daily workers). Set INTENT_EVENTS_WORKER_SELF_RUN=false to
    instead run once and exit (e.g. for a one-off/manual invocation).
    """
    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        run_worker,
        trigger="interval",
        minutes=15,
        id="intent_events_worker_job",
        coalesce=True,
        max_instances=1,
        misfire_grace_time=600,
    )
    logger.info("Intent events worker scheduler started (every 15 minutes)")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Intent events worker scheduler stopped")


if __name__ == "__main__":
    self_run = (
        os.environ.get("INTENT_EVENTS_WORKER_SELF_RUN", "true").strip().lower() == "true"
    )
    if self_run:
        _run_scheduled()
    else:
        run_worker()
