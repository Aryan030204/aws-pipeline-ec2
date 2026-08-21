"""
Outbound notifications: QStash pipeline-completion/alert webhooks, the
UTM aggregation pipeline trigger, and exception-to-error-code
classification used for the manager-failure webhook payload.
"""

import asyncio
import os
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

# New: QStash Completion Token
QSTASH_COMPLETION_TOKEN = os.environ.get("QSTASH_COMPLETION_TOKEN")

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


def trigger_pipeline_completion_webhook():
    """
    Triggers a QStash webhook via the QStash Publish API.
    Retries are handled by QStash.
    """
    if not QSTASH_TOKEN or not QSTASH_COMPLETION_TOKEN:
        logger.warning("Skipping QStash: Token(s) missing.")
        return

    if aws_background.TEST_MODE:
        logger.info("Skipping QStash completion webhook (TEST_MODE)")
        return

    # Target URL is the QStash Publish endpoint wrapping the destination
    destination_urls = ["https://etl-cache-pipeline.onrender.com/qstash"]

    headers = {
        "Authorization": f"Bearer {QSTASH_TOKEN}",
        "Upstash-Forward-Authorization": f"Bearer {QSTASH_COMPLETION_TOKEN}",
        "Content-Type": "application/json",
    }

    for destination_url in destination_urls:
        qstash_url = f"https://qstash.upstash.io/v2/publish/{destination_url}"
        try:
            # We can use a short timeout because QStash accepts the request immediately (millisecond latency)
            resp = requests.post(qstash_url, headers=headers, json={}, timeout=10)

            if resp.status_code in (200, 201):
                logger.info(
                    f"✅ QStash Publish API accepted the message for {destination_url} (Async delivery started)."
                )
            else:
                logger.error(
                    f"❌ QStash Publish failed for {destination_url}: {resp.status_code} {resp.text}"
                )
        except Exception as e:
            logger.error(f"❌ QStash Publish error for {destination_url}: {e}")


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
