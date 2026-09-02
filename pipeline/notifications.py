"""
Outbound notifications: QStash pipeline-completion/alert webhooks, the
UTM aggregation pipeline trigger, and exception-to-error-code
classification used for the manager-failure webhook payload.
"""

import asyncio
import os
import threading
from datetime import datetime
from typing import Optional

import aiohttp
import mysql.connector
import requests
from sqlalchemy.exc import SQLAlchemyError

from pipeline.state import IST, logger, http_session

try:
    from qstash import QStash
except ImportError:
    QStash = None

# aws_background still owns TEST_MODE (brand_config_init category). Imported
# at module level here is safe: attribute access only happens when these
# functions are actually CALLED, during real pipeline execution, long after
# aws_background.py has fully finished loading (see pipeline/shopifyql_sessions.py
# for the same pattern).
import aws_background


def _derive_error_code(exc: Optional[BaseException]) -> str:
    if exc is None:
        return "PIPELINE_FAILED"

    if isinstance(exc, (mysql.connector.Error, SQLAlchemyError)):
        return "DB_ERROR"

    if isinstance(
        exc,
        (
            requests.exceptions.Timeout,
            asyncio.TimeoutError,
            TimeoutError,
        ),
    ):
        return "TIMEOUT"

    if isinstance(exc, (requests.exceptions.RequestException, aiohttp.ClientError)):
        return "HTTP_ERROR"

    if isinstance(exc, KeyError):
        return "MISSING_ENV"

    if isinstance(exc, (ValueError, TypeError)):
        return "VALIDATION_ERROR"

    if isinstance(exc, (FileNotFoundError, PermissionError, OSError)):
        return "IO_ERROR"

    return exc.__class__.__name__.upper()


# ---------------------------
# QStash / Alerts config (NEW)
# ---------------------------
QSTASH_TOKEN = os.environ.get("QSTASH_TOKEN")
ALERTS_WEBHOOK_URL = os.environ.get("ALERTS_WEBHOOK_URL")

_qstash_client: Optional["QStash"] = None


def get_qstash_client() -> Optional["QStash"]:
    """
    Lazily initialize a shared QStash client.

    If env vars or dependency are missing, this returns None and silently
    disables QStash-based alerts (pipeline still works normally).
    """
    global _qstash_client

    if _qstash_client is not None:
        return _qstash_client

    if QStash is None:
        logger.warning("QStash alerts disabled: qstash package not installed.")
        return None

    if aws_background.TEST_MODE:
        logger.info("Skipping QStash client init (TEST_MODE)")
        return None

    if not QSTASH_TOKEN:
        logger.info("QStash alerts disabled: QSTASH_TOKEN not set.")
        return None

    try:
        _qstash_client = QStash(QSTASH_TOKEN)
        logger.info("✅ QStash client initialized for alerts producer")
    except Exception as e:
        logger.error(f"❌ Failed to initialize QStash client: {e}")
        _qstash_client = None

    return _qstash_client


# ---------------------------
# Cache pipeline trigger
# ---------------------------
# Cache pipeline now runs as a container on the same Docker network as this
# pipeline, so it's triggered with a direct GET instead of routing through
# QStash.
CACHE_PIPELINE_TRIGGER_URL = os.environ.get(
    "CACHE_PIPELINE_TRIGGER_URL", "http://cache-pipeline:8080/trigger-pipeline"
)


def _do_trigger_cache_pipeline():
    """Runs in a background thread - see trigger_cache_pipeline() below."""
    try:
        resp = http_session.get(CACHE_PIPELINE_TRIGGER_URL, timeout=600)
        try:
            body = resp.json()
            status = body.get("status", "unknown")
            brand_count = len(body.get("results") or {})
            logger.info(
                f"✅ Cache pipeline trigger completed: status={status}, brands={brand_count}"
            )
        except ValueError:
            logger.info(
                f"✅ Cache pipeline trigger completed: HTTP {resp.status_code}"
            )
    except Exception as e:
        logger.error(f"❌ Cache pipeline trigger failed: {e}")


def trigger_cache_pipeline():
    """
    Fires a GET request to the cache pipeline's trigger endpoint. The
    request runs in a background thread so the ETL run doesn't block
    waiting for the cache rebuild to finish (it can take a while across all
    brands/dates) - this function returns immediately after starting the
    thread. Whenever the response does come back, only a compact summary
    is logged, not the full per-brand/per-date JSON body.
    """
    if aws_background.TEST_MODE:
        logger.info("Skipping cache pipeline trigger (TEST_MODE)")
        return

    logger.info(f"🚀 Triggering cache pipeline: {CACHE_PIPELINE_TRIGGER_URL}")
    threading.Thread(target=_do_trigger_cache_pipeline, daemon=True).start()


# ---------------------------
# UTM Pipeline Trigger (NEW: Replacing Kafka)
# ---------------------------
UTM_PIPELINE_TRIGGER_API = os.environ.get("UTM_PIPELINE_TRIGGER_API")
UTM_PIPELINE_SWITCH = (
    os.environ.get("UTM_PIPELINE_SWITCH", os.environ.get("UTM_PIPELINE_SWTICH", "false"))
    .strip()
    .lower()
    == "true"
)


def trigger_utm_pipeline(event_type: str, job_id: str, recompute_days: int = 1):
    """
    Triggers the UTM aggregation pipeline via a POST API call.
    Body is identical to the previous Kafka message schema.
    """
    if not UTM_PIPELINE_SWITCH:
        logger.info(
            "UTM pipeline switch is OFF (UTM_PIPELINE_SWITCH=false). Skipping trigger."
        )
        return

    if not UTM_PIPELINE_TRIGGER_API:
        logger.warning("UTM_PIPELINE_TRIGGER_API not set. Skipping trigger.")
        return

    # Use the same auth header as other pipeline APIs
    auth_header = os.environ.get("PIPELINE_AUTH_HEADER")
    headers = {
        "Content-Type": "application/json",
        "x-pipeline-key": auth_header if auth_header else ""
    }

    message = {
        "eventType": event_type,
        "jobId": job_id,
        "completedAt": datetime.now(IST).isoformat(),
        "recomputeDays": recompute_days,
    }

    logger.info(f"🚀 Triggering UTM pipeline API: {UTM_PIPELINE_TRIGGER_API}")

    try:
        # We reuse the global http_session for connection pooling
        resp = http_session.post(
            UTM_PIPELINE_TRIGGER_API,
            json=message,
            headers=headers,
            timeout=30
        )

        if resp.status_code in (200, 201, 202):
            logger.info(f"✅ UTM pipeline trigger successful: {resp.status_code}")
        else:
            logger.error(f"❌ UTM pipeline trigger failed: {resp.status_code} - {resp.text}")
    except Exception as e:
        logger.error(f"❌ Error triggering UTM pipeline: {e}")
