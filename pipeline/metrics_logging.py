"""
Per-brand pipeline run metrics -> Google Sheets.

Every _execute_pipeline_run() call (pipeline/orchestration.py) already
collects a metrics dict per brand (see process_single_brand's return value:
new/updated order counts, affected-date count, sessions/ATC sessions synced,
success/error, duration) plus the run's overall duration. This module's only
job is to turn that into rows appended to a Google Sheet, one row per brand
per run plus one "ALL BRANDS" summary row - a compact, queryable record of
what each run did, without digging through container logs.

Entirely best-effort: a bad/missing service account, an unreachable Sheets
API, a quota error, or anything else here is logged and swallowed, never
raised - this must never fail or delay the pipeline run that's reporting on
itself. The actual write is dispatched to a background thread (same pattern
as pipeline/notifications.py's trigger_cache_pipeline) so a slow Sheets API
call never holds up the next scheduled run.
"""

import base64
import json
import os
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from pipeline.state import logger

IST = ZoneInfo("Asia/Kolkata")

PIPELINE_METRICS_SHEET_SWITCH = (
    os.environ.get("PIPELINE_METRICS_SHEET_SWITCH", "true").strip().lower() == "true"
)
PIPELINE_METRICS_SHEET_ID = os.environ.get("PIPELINE_METRICS_SHEET_ID", "").strip()
GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE = os.environ.get(
    "GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE", ""
).strip()
GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON_B64 = os.environ.get(
    "GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON_B64", ""
).strip()

HEADER_ROW = [
    "timestamp_ist",
    "job_id",
    "brand_name",
    "brand_index",
    "new_orders",
    "updated_orders",
    "affected_dates",
    "sessions_synced",
    "atc_sessions_synced",
    "success",
    "error_message",
    "duration_seconds",
]

_ALL_BRANDS_LABEL = "ALL BRANDS"


def _build_rows(
    job_id: str,
    brand_run_metrics: List[Dict[str, Any]],
    overall_duration_s: float,
) -> List[List[Any]]:
    timestamp = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    rows: List[List[Any]] = []

    for m in brand_run_metrics:
        rows.append(
            [
                timestamp,
                job_id,
                m.get("brand_name", ""),
                m.get("brand_index", ""),
                m.get("new_orders_count", 0),
                m.get("updated_orders_count", 0),
                m.get("affected_dates_count", 0),
                m.get("sessions_synced", 0),
                m.get("atc_sessions_synced", 0),
                bool(m.get("success", False)),
                m.get("error_message", "") or "",
                m.get("duration_seconds", 0),
            ]
        )

    rows.append(
        [
            timestamp,
            job_id,
            _ALL_BRANDS_LABEL,
            len(brand_run_metrics),
            sum(m.get("new_orders_count", 0) for m in brand_run_metrics),
            sum(m.get("updated_orders_count", 0) for m in brand_run_metrics),
            sum(m.get("affected_dates_count", 0) for m in brand_run_metrics),
            sum(m.get("sessions_synced", 0) for m in brand_run_metrics),
            sum(m.get("atc_sessions_synced", 0) for m in brand_run_metrics),
            all(m.get("success", False) for m in brand_run_metrics) if brand_run_metrics else True,
            "; ".join(
                f"{m.get('brand_name', '?')}: {m.get('error_message')}"
                for m in brand_run_metrics
                if m.get("error_message")
            ),
            round(overall_duration_s, 2),
        ]
    )
    return rows


def _resolve_service_account_client():
    """
    Returns an authorized gspread client, or raises - the caller (_do_log)
    is responsible for catching. Import of gspread/google-auth is deferred
    to here (not module top-level) so a container that never uses this
    feature (PIPELINE_METRICS_SHEET_SWITCH=false, the default-off path for
    anyone who hasn't set it up yet) never needs those packages importable.
    """
    if GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE:
        import gspread

        return gspread.service_account(filename=GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE)

    if GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON_B64:
        import gspread

        info = json.loads(base64.b64decode(GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON_B64))
        return gspread.service_account_from_dict(info)

    raise ValueError(
        "Neither GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE nor "
        "GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON_B64 is configured"
    )


def _monthly_worksheet_name(now: Optional[datetime] = None) -> str:
    now = now or datetime.now(IST)
    return f"Runs-{now.strftime('%Y-%m')}"


def _get_or_create_monthly_worksheet(spreadsheet):
    """
    One worksheet per calendar month (Runs-2026-09, ...) so a single tab
    doesn't grow unbounded across the pipeline's lifetime. Writes the header
    row once, only when the tab is newly created.
    """
    import gspread

    name = _monthly_worksheet_name()
    try:
        return spreadsheet.worksheet(name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=name, rows=1000, cols=len(HEADER_ROW)
        )
        worksheet.append_row(HEADER_ROW)
        return worksheet


def _do_log(job_id: str, brand_run_metrics: List[Dict[str, Any]], overall_duration_s: float) -> None:
    if not PIPELINE_METRICS_SHEET_ID:
        logger.warning(
            "PIPELINE_METRICS_SHEET_ID is not set; skipping pipeline run-metrics logging."
        )
        return

    try:
        client = _resolve_service_account_client()
        spreadsheet = client.open_by_key(PIPELINE_METRICS_SHEET_ID)
        worksheet = _get_or_create_monthly_worksheet(spreadsheet)
        rows = _build_rows(job_id, brand_run_metrics, overall_duration_s)
        worksheet.append_rows(rows, value_input_option="USER_ENTERED")
        logger.info(
            f"📊 Logged {len(rows)} pipeline run-metrics row(s) to Google Sheets "
            f"(job_id={job_id})"
        )
    except Exception as e:
        logger.error(f"❌ Failed to log pipeline run metrics to Google Sheets: {e}")


def log_pipeline_run_metrics_async(
    job_id: str,
    brand_run_metrics: List[Dict[str, Any]],
    overall_duration_s: float,
) -> None:
    """
    Fire-and-forget entry point called from pipeline/orchestration.py's
    _execute_pipeline_run, right after a run's ThreadPoolExecutor loop
    finishes. Never raises, never blocks - see module docstring.
    """
    if not PIPELINE_METRICS_SHEET_SWITCH:
        logger.debug("Pipeline run-metrics logging is disabled (PIPELINE_METRICS_SHEET_SWITCH=false).")
        return

    threading.Thread(
        target=_do_log,
        args=(job_id, brand_run_metrics, overall_duration_s),
        daemon=True,
    ).start()
