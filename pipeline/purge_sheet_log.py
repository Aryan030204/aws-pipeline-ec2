"""
Data purge run log -> one Google Sheets tab.

Per run it appends, in a single call:
  * a TABLE row per (brand, table)  - status, rows eligible/archived/deleted, table size before/after, timings, error
  * a BRAND row per brand           - per-brand duration, table counts, sums, last/previous run timestamp
  * one RUN row ("ALL BRANDS")      - total duration, totals, overall success, brand filter, mode

Same spreadsheet and service account as the pipeline run metrics (pipeline/metrics_logging.py); only the tab
differs. Entirely best-effort: nothing here may fail the purge or change its exit status, so every error is logged
and swallowed. The write is synchronous (the worker can be a one-shot process; a daemon thread could be killed
before it finished) and bounded by a client timeout. Row VALUES from the databases are never written - only
counts, sizes, statuses and the same sanitised error text the purge logs.

Env:
  PURGE_LOG_SHEET_SWITCH  true (default) / false
  PURGE_LOG_SHEET_ID      spreadsheet id; falls back to PIPELINE_METRICS_SHEET_ID
  PURGE_LOG_SHEET_TAB     tab name, default "Data Purge Logs"
Size note: sizes come from information_schema (approximate). InnoDB does not shrink a table after DELETE, so
size_after is usually ~ size_before; rows_after_approx and data_free_after_mb are the signals that rows were removed.
"""

import os
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence
from zoneinfo import ZoneInfo

from pipeline import data_purge as dp
from pipeline.state import logger

IST = ZoneInfo("Asia/Kolkata")
DEFAULT_TAB = "Data Purge Logs"
ALL_BRANDS_LABEL = "ALL BRANDS"
ERROR_MAX_CHARS = 500
TIMEOUT_S = 30
CAPACITY_PADDING_ROWS = 500

HEADER_ROW = [
    "timestamp_ist", "run_date", "run_id", "row_type", "mode", "brand", "table", "status", "reason", "cutoff",
    "retention_days", "eligible_rows", "archived_rows", "deleted_rows", "csv_bytes", "size_before_mb",
    "size_after_mb", "rows_before_approx", "rows_after_approx", "data_free_after_mb", "started_at_ist",
    "finished_at_ist", "duration_s", "last_run_ist", "previous_run_ist", "error",
]
_COL = {name: i for i, name in enumerate(HEADER_ROW)}

_OK_STATUSES = {dp.Status.SUCCESS, dp.Status.ARCHIVED_ONLY, dp.Status.NO_DATA, dp.Status.DRY_RUN}


# --------------------------------------------------------------------------------------
# Row building (pure; no gspread)
# --------------------------------------------------------------------------------------
def _ts(value: Optional[datetime]) -> str:
    return value.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S") if value else ""


def _mb(num_bytes: Optional[int]) -> Any:
    return round(num_bytes / 1048576, 2) if num_bytes is not None else ""


def _clip(text: Any) -> str:
    text = str(text or "")
    return text if len(text) <= ERROR_MAX_CHARS else text[: ERROR_MAX_CHARS - 3] + "..."


def _opt(value: Any) -> Any:
    return "" if value is None else value


def _mode_label(cfg: dp.PurgeConfig) -> str:
    return cfg.mode.value + (" (PURGE_TEST)" if cfg.purge_test else "")


def _filter_label(cfg: dp.PurgeConfig) -> str:
    return "PURGE_BRAND_IDS=" + ",".join(str(i) for i in cfg.purge_brand_indices) if cfg.purge_brand_indices else "all brands"


def run_id_for(run_dt: datetime) -> str:
    return run_dt.astimezone(IST).strftime("%Y%m%d-%H%M%S")


def _blank_row() -> List[Any]:
    return [""] * len(HEADER_ROW)


def _after_values(r: dp.TableResult):
    """size/rows after: re-read when rows were deleted; otherwise nothing changed, so 'after' == 'before'."""
    if r.deleted and (r.size_after_bytes is not None or r.rows_after is not None):
        return r.size_after_bytes, r.rows_after
    return r.size_before_bytes, r.rows_before


def _is_failed(r: dp.TableResult) -> bool:
    return r.status == dp.Status.FAILED or r.error


def build_rows(
    results: Sequence[dp.TableResult],
    run: dp.RunContext,
    total_s: float,
    previous_runs: Optional[Dict[str, str]] = None,
    error: Optional[str] = None,
) -> List[List[Any]]:
    previous_runs = previous_runs or {}
    cfg = run.cfg
    now = datetime.now(IST)
    run_date, run_id, mode = run.run_label, run_id_for(run.run_dt), _mode_label(cfg)

    def base(row_type: str, brand: str, table: str) -> List[Any]:
        row = _blank_row()
        row[_COL["timestamp_ist"]], row[_COL["run_date"]], row[_COL["run_id"]] = _ts(now), run_date, run_id
        row[_COL["row_type"]], row[_COL["mode"]], row[_COL["brand"]], row[_COL["table"]] = row_type, mode, brand, table
        row[_COL["cutoff"]], row[_COL["retention_days"]] = run.cutoff.isoformat(), cfg.retention_days
        return row

    rows: List[List[Any]] = []
    by_brand: Dict[str, List[dp.TableResult]] = {}
    for r in results:
        by_brand.setdefault(r.brand, []).append(r)
        row = base("TABLE", r.brand, r.table)
        size_after, rows_after = _after_values(r)
        row[_COL["status"]] = r.status
        row[_COL["reason"]] = _clip(r.reason)
        row[_COL["eligible_rows"]] = _opt(r.eligible)
        row[_COL["archived_rows"]] = r.exported if r.archived else 0
        row[_COL["deleted_rows"]] = r.deleted
        row[_COL["csv_bytes"]] = r.csv_bytes
        row[_COL["size_before_mb"]], row[_COL["size_after_mb"]] = _mb(r.size_before_bytes), _mb(size_after)
        row[_COL["rows_before_approx"]], row[_COL["rows_after_approx"]] = _opt(r.rows_before), _opt(rows_after)
        row[_COL["data_free_after_mb"]] = _mb(r.data_free_after)
        row[_COL["started_at_ist"]], row[_COL["finished_at_ist"]] = _ts(r.started_at), _ts(r.finished_at)
        row[_COL["duration_s"]] = round(r.duration_s, 2)
        row[_COL["error"]] = _clip(r.reason) if _is_failed(r) else ""
        rows.append(row)

    def totals(rs: Sequence[dp.TableResult]):
        before = sum(r.size_before_bytes or 0 for r in rs)
        after = sum((_after_values(r)[0] or 0) for r in rs)
        rb = sum(r.rows_before or 0 for r in rs)
        ra = sum((_after_values(r)[1] or 0) for r in rs)
        return {
            "eligible": sum(r.eligible or 0 for r in rs),
            "archived": sum(r.exported for r in rs if r.archived),
            "deleted": sum(r.deleted for r in rs),
            "csv": sum(r.csv_bytes for r in rs),
            "size_before": before, "size_after": after, "rows_before": rb, "rows_after": ra,
        }

    def fill_totals(row: List[Any], t: Dict[str, int]) -> None:
        row[_COL["eligible_rows"]], row[_COL["archived_rows"]], row[_COL["deleted_rows"]] = t["eligible"], t["archived"], t["deleted"]
        row[_COL["csv_bytes"]] = t["csv"]
        row[_COL["size_before_mb"]], row[_COL["size_after_mb"]] = _mb(t["size_before"]), _mb(t["size_after"])
        row[_COL["rows_before_approx"]], row[_COL["rows_after_approx"]] = t["rows_before"], t["rows_after"]

    for brand, rs in by_brand.items():
        timing = run.brand_timings.get(brand, {})
        failed = [r for r in rs if _is_failed(r)]
        skipped = [r for r in rs if r.status.startswith("SKIPPED")]
        row = base("BRAND", brand, "")
        row[_COL["status"]] = "FAILED" if failed else "OK"
        row[_COL["reason"]] = f"tables={len(rs)} ok={sum(1 for r in rs if r.status in _OK_STATUSES)} failed={len(failed)} skipped={len(skipped)}"
        fill_totals(row, totals(rs))
        row[_COL["started_at_ist"]], row[_COL["finished_at_ist"]] = _ts(timing.get("started_at")), _ts(timing.get("finished_at"))
        row[_COL["duration_s"]] = round(timing.get("duration_s", sum(r.duration_s for r in rs)), 2)
        row[_COL["last_run_ist"]] = _ts(timing.get("finished_at")) or _ts(now)
        row[_COL["previous_run_ist"]] = previous_runs.get(brand, "")
        row[_COL["error"]] = _clip("; ".join(f"{r.table}: {r.reason}" for r in failed))
        rows.append(row)

    all_failed = [r for r in results if _is_failed(r)]
    run_row = base("RUN", ALL_BRANDS_LABEL, "")
    run_row[_COL["status"]] = "FAILED" if (all_failed or error) else "OK"
    run_row[_COL["reason"]] = f"brands={len(by_brand)} tables={len(results)} failed={len(all_failed)}; {_filter_label(cfg)}"
    fill_totals(run_row, totals(results))
    run_row[_COL["started_at_ist"]], run_row[_COL["finished_at_ist"]] = _ts(run.run_dt), _ts(now)
    run_row[_COL["duration_s"]] = round(total_s, 2)
    run_row[_COL["last_run_ist"]] = _ts(now)
    errors = ([error] if error else []) + [f"{r.brand}/{r.table}: {r.reason}" for r in all_failed]
    run_row[_COL["error"]] = _clip("; ".join(errors))
    rows.append(run_row)
    return rows


def build_error_row(error: str, run_dt: datetime, total_s: float, cfg: Optional[dp.PurgeConfig] = None,
                    note: str = "") -> List[Any]:
    """A RUN row for a run that died before it produced any results (bad config, S3 client, no brands)."""
    now = datetime.now(IST)
    row = _blank_row()
    row[_COL["timestamp_ist"]], row[_COL["run_date"]] = _ts(now), dp.archive_run_label(run_dt)
    row[_COL["run_id"]], row[_COL["row_type"]], row[_COL["brand"]] = run_id_for(run_dt), "RUN", ALL_BRANDS_LABEL
    row[_COL["status"]], row[_COL["reason"]] = "FAILED", note or "run did not complete"
    if cfg is not None:
        row[_COL["mode"]], row[_COL["retention_days"]] = _mode_label(cfg), cfg.retention_days
    row[_COL["started_at_ist"]], row[_COL["finished_at_ist"]] = _ts(run_dt), _ts(now)
    row[_COL["duration_s"]] = round(total_s, 2)
    row[_COL["last_run_ist"]] = _ts(now)
    row[_COL["error"]] = _clip(error)
    return row


# --------------------------------------------------------------------------------------
# Google Sheets write (best effort; never raises)
# --------------------------------------------------------------------------------------
def _settings() -> Optional[Dict[str, str]]:
    if os.environ.get("PURGE_LOG_SHEET_SWITCH", "true").strip().lower() != "true":
        logger.info("Purge sheet logging is disabled (PURGE_LOG_SHEET_SWITCH=false).")
        return None
    sheet_id = (os.environ.get("PURGE_LOG_SHEET_ID") or os.environ.get("PIPELINE_METRICS_SHEET_ID") or "").strip()
    if not sheet_id:
        logger.warning("Neither PURGE_LOG_SHEET_ID nor PIPELINE_METRICS_SHEET_ID is set; skipping purge sheet logging.")
        return None
    return {"sheet_id": sheet_id, "tab": (os.environ.get("PURGE_LOG_SHEET_TAB") or DEFAULT_TAB).strip() or DEFAULT_TAB}


def _get_or_create_tab(spreadsheet, tab: str):
    import gspread

    try:
        return spreadsheet.worksheet(tab)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=tab, rows=1000, cols=len(HEADER_ROW))
        worksheet.append_row(HEADER_ROW)
        return worksheet


def _previous_runs(worksheet) -> Dict[str, str]:
    """brand -> its latest last_run_ist, from earlier BRAND rows. Best effort: {} on any problem."""
    try:
        types = worksheet.col_values(_COL["row_type"] + 1)
        brands = worksheet.col_values(_COL["brand"] + 1)
        last_runs = worksheet.col_values(_COL["last_run_ist"] + 1)
    except Exception as exc:
        logger.warning("could not read previous purge runs from the sheet: %s", dp._safe_error(exc))
        return {}
    previous: Dict[str, str] = {}
    for i, row_type in enumerate(types):
        if row_type == "BRAND" and i < len(brands) and i < len(last_runs) and last_runs[i]:
            previous[brands[i]] = last_runs[i]  # later rows overwrite earlier ones
    return previous


def write_rows(make_rows: Callable[[Dict[str, str]], List[List[Any]]]) -> bool:
    """Open the tab, build the rows (given the previous-run map) and append them in one call. Returns True when
    rows were written; never raises."""
    try:
        settings = _settings()
        if settings is None:
            return False
        from pipeline import metrics_logging

        client = metrics_logging._resolve_service_account_client()
        if hasattr(client, "set_timeout"):
            client.set_timeout(TIMEOUT_S)
        worksheet = _get_or_create_tab(client.open_by_key(settings["sheet_id"]), settings["tab"])
        rows = make_rows(_previous_runs(worksheet))
        used = len(worksheet.col_values(1))
        capacity = getattr(worksheet, "row_count", None)
        if isinstance(capacity, int) and used + len(rows) > capacity:
            worksheet.add_rows(used + len(rows) - capacity + CAPACITY_PADDING_ROWS)
        worksheet.append_rows(rows, value_input_option="USER_ENTERED")
        logger.info("Logged %s purge row(s) to Google Sheets tab '%s'", len(rows), settings["tab"])
        return True
    except Exception as exc:
        logger.error("Failed to log the purge run to Google Sheets (the purge itself is unaffected): %s", dp._safe_error(exc))
        return False


def log_purge_run(results: Sequence[dp.TableResult], run: dp.RunContext, total_s: float,
                  error: Optional[str] = None) -> bool:
    return write_rows(lambda previous: build_rows(results, run, total_s, previous, error))


def log_purge_failure(error: str, run_dt: datetime, total_s: float, cfg: Optional[dp.PurgeConfig] = None,
                      note: str = "") -> bool:
    return write_rows(lambda previous: [build_error_row(error, run_dt, total_s, cfg, note)])
