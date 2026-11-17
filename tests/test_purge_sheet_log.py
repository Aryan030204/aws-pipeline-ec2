"""Tests for pipeline/purge_sheet_log.py: row building (pure) and the best-effort Google Sheets write, with a fake
gspread. No network, no credentials."""
import os
import sys
import types
import unittest
from datetime import datetime
from unittest import mock
from zoneinfo import ZoneInfo

import aws_background  # noqa: F401  (import order: the pipeline modules import it circularly)
from pipeline import data_purge as dp
from pipeline import purge_sheet_log as psl

IST = ZoneInfo("Asia/Kolkata")
RUN_DT = datetime(2026, 11, 16, 3, 23, tzinfo=IST)
COL = psl._COL


def make_run(**env):
    base = {"DATA_PURGE_ENABLED": "true", "DATA_PURGE_S3_BUCKET": "bkt"}
    base.update(env)
    cfg = dp.load_config(base)
    run = dp.RunContext(cfg=cfg, run_dt=RUN_DT, cutoff=dp.compute_cutoff(RUN_DT, cfg.retention_days),
                        run_label=dp.archive_run_label(RUN_DT), store=None)
    run.brand_timings = {
        "pts": {"index": 1, "started_at": RUN_DT, "finished_at": RUN_DT.replace(minute=30), "duration_s": 420.0},
        "bbb": {"index": 2, "started_at": RUN_DT.replace(minute=30), "finished_at": RUN_DT.replace(minute=31), "duration_s": 60.0},
    }
    return run


def result(brand, table, status=dp.Status.ARCHIVED_ONLY, **kw):
    r = dp.TableResult(brand=brand, table=table, status=status)
    r.started_at, r.finished_at, r.duration_s = RUN_DT, RUN_DT.replace(minute=5), 300.0
    for key, value in kw.items():
        setattr(r, key, value)
    return r


def sample_results():
    return [
        result("pts", "shopify_orders", dp.Status.SUCCESS, eligible=100, exported=100, archived=True, deleted=100,
               csv_bytes=2_000_000, size_before_bytes=10 * 1048576, size_after_bytes=10 * 1048576, rows_before=500,
               rows_after=400, data_free_after=3 * 1048576, reason="archived, verified and deleted"),
        result("pts", "raw_mongo_events", dp.Status.FAILED, error=True, reason="RuntimeError: S3 is down",
               size_before_bytes=5 * 1048576, rows_before=50),
        result("bbb", "shopify_orders", dp.Status.SKIPPED, reason="not in DATA_PURGE_TABLES"),
        result("bbb", "raw_mongo_events", dp.Status.ARCHIVED_ONLY, eligible=7, exported=7, archived=True,
               csv_bytes=1000, size_before_bytes=2 * 1048576, rows_before=70),
    ]


class BuildRowsTests(unittest.TestCase):
    def setUp(self):
        self.run = make_run()
        self.rows = psl.build_rows(sample_results(), self.run, 500.0, {"pts": "2026-11-01 03:40:11"})
        self.by_type = lambda t: [r for r in self.rows if r[COL["row_type"]] == t]

    def test_every_row_has_exactly_the_header_width(self):
        self.assertTrue(all(len(r) == len(psl.HEADER_ROW) for r in self.rows))

    def test_one_table_row_per_result_then_one_row_per_brand_then_one_run_row(self):
        self.assertEqual([r[COL["row_type"]] for r in self.rows], ["TABLE"] * 4 + ["BRAND"] * 2 + ["RUN"])

    def test_table_row_carries_status_counts_sizes_and_timing(self):
        row = self.by_type("TABLE")[0]
        self.assertEqual(row[COL["run_date"]], "2026-11-16")
        self.assertEqual((row[COL["brand"]], row[COL["table"]], row[COL["status"]]), ("pts", "shopify_orders", "SUCCESS"))
        self.assertEqual((row[COL["eligible_rows"]], row[COL["archived_rows"]], row[COL["deleted_rows"]]), (100, 100, 100))
        self.assertEqual((row[COL["size_before_mb"]], row[COL["size_after_mb"]]), (10.0, 10.0))
        self.assertEqual((row[COL["rows_before_approx"]], row[COL["rows_after_approx"]]), (500, 400))
        self.assertEqual(row[COL["data_free_after_mb"]], 3.0)
        self.assertEqual(row[COL["cutoff"]], "2026-04-30")
        self.assertEqual(row[COL["retention_days"]], 200)
        self.assertEqual(row[COL["started_at_ist"]], "2026-11-16 03:23:00")
        self.assertEqual(row[COL["duration_s"]], 300.0)
        self.assertEqual(row[COL["error"]], "")

    def test_after_equals_before_when_nothing_was_deleted(self):
        row = self.by_type("TABLE")[3]  # archived only
        self.assertEqual((row[COL["size_before_mb"]], row[COL["size_after_mb"]]), (2.0, 2.0))
        self.assertEqual((row[COL["rows_before_approx"]], row[COL["rows_after_approx"]]), (70, 70))

    def test_tables_that_never_touched_the_database_have_blank_sizes(self):
        row = self.by_type("TABLE")[2]  # skipped
        self.assertEqual((row[COL["size_before_mb"]], row[COL["size_after_mb"]], row[COL["rows_before_approx"]]), ("", "", ""))
        self.assertEqual(row[COL["eligible_rows"]], "")

    def test_failed_tables_put_the_reason_in_the_error_column_only(self):
        failed = self.by_type("TABLE")[1]
        self.assertEqual(failed[COL["error"]], "RuntimeError: S3 is down")
        self.assertEqual(self.by_type("TABLE")[0][COL["error"]], "")
        self.assertEqual(self.by_type("TABLE")[2][COL["error"]], "")  # a skip is not an error

    def test_brand_rows_have_duration_counts_sums_and_last_and_previous_run(self):
        pts, bbb = self.by_type("BRAND")
        self.assertEqual((pts[COL["brand"]], pts[COL["status"]]), ("pts", "FAILED"))
        self.assertEqual(pts[COL["reason"]], "tables=2 ok=1 failed=1 skipped=0")
        self.assertEqual(pts[COL["duration_s"]], 420.0)
        self.assertEqual((pts[COL["archived_rows"]], pts[COL["deleted_rows"]], pts[COL["csv_bytes"]]), (100, 100, 2_000_000))
        self.assertEqual((pts[COL["size_before_mb"]], pts[COL["size_after_mb"]]), (15.0, 15.0))
        self.assertEqual(pts[COL["last_run_ist"]], "2026-11-16 03:30:00")
        self.assertEqual(pts[COL["previous_run_ist"]], "2026-11-01 03:40:11")
        self.assertIn("raw_mongo_events: RuntimeError: S3 is down", pts[COL["error"]])
        self.assertEqual((bbb[COL["status"]], bbb[COL["reason"]]), ("OK", "tables=2 ok=1 failed=0 skipped=1"))
        self.assertEqual(bbb[COL["previous_run_ist"]], "")  # first time this brand is logged
        self.assertEqual(bbb[COL["duration_s"]], 60.0)

    def test_run_row_has_total_time_totals_filter_and_errors(self):
        run_row = self.by_type("RUN")[0]
        self.assertEqual(run_row[COL["brand"]], "ALL BRANDS")
        self.assertEqual(run_row[COL["status"]], "FAILED")
        self.assertEqual(run_row[COL["duration_s"]], 500.0)
        self.assertEqual((run_row[COL["archived_rows"]], run_row[COL["deleted_rows"]]), (107, 100))
        self.assertIn("brands=2 tables=4 failed=1", run_row[COL["reason"]])
        self.assertIn("all brands", run_row[COL["reason"]])
        self.assertIn("pts/raw_mongo_events: RuntimeError: S3 is down", run_row[COL["error"]])
        self.assertEqual(run_row[COL["mode"]], "ARCHIVE_ONLY")

    def test_the_brand_filter_and_purge_test_are_visible(self):
        run = make_run(PURGE_BRAND_IDS="1,2", PURGE_TEST="true")
        run_row = psl.build_rows(sample_results(), run, 1.0)[-1]
        self.assertIn("PURGE_BRAND_IDS=1,2", run_row[COL["reason"]])
        self.assertEqual(run_row[COL["mode"]], "ARCHIVE_ONLY (PURGE_TEST)")

    def test_a_clean_run_is_ok(self):
        rows = psl.build_rows([result("pts", "t")], self.run, 1.0)
        self.assertEqual((rows[-1][COL["status"]], rows[-1][COL["error"]]), ("OK", ""))

    def test_long_errors_are_truncated(self):
        long = result("pts", "t", dp.Status.FAILED, error=True, reason="x" * 5000)
        rows = psl.build_rows([long], self.run, 1.0)
        for row in rows:
            self.assertLessEqual(len(row[COL["error"]]), psl.ERROR_MAX_CHARS)
            self.assertLessEqual(len(row[COL["reason"]]), psl.ERROR_MAX_CHARS + 60)

    def test_no_database_values_can_reach_the_sheet(self):
        # only counts/sizes/statuses/sanitised errors are ever passed in; row payloads live in TableResult.details
        # (never read here), and the cells hold nothing but scalars
        res = sample_results()
        res[0].details["secret_payload"] = "SECRET-XYZ"
        flat = " ".join(str(c) for row in psl.build_rows(res, self.run, 1.0) for c in row)
        self.assertNotIn("SECRET-XYZ", flat)
        self.assertTrue(all(isinstance(c, (str, int, float)) for row in self.rows for c in row))

    def test_error_row_for_a_run_that_never_started(self):
        row = psl.build_error_row("ConfigError: bad", RUN_DT, 0.4, None, "invalid configuration")
        self.assertEqual(len(row), len(psl.HEADER_ROW))
        self.assertEqual((row[COL["row_type"]], row[COL["status"]], row[COL["reason"]]), ("RUN", "FAILED", "invalid configuration"))
        self.assertEqual(row[COL["error"]], "ConfigError: bad")
        self.assertEqual(row[COL["run_date"]], "2026-11-16")

    def test_empty_run_builds_just_the_run_row(self):
        rows = psl.build_rows([], self.run, 1.0)
        self.assertEqual([r[COL["row_type"]] for r in rows], ["RUN"])
        self.assertEqual(rows[0][COL["status"]], "OK")


class FakeWorksheet:
    def __init__(self, existing=None, row_count=1000):
        self.rows = [list(r) for r in (existing or [])]
        self.row_count = row_count
        self.appended = []
        self.added_rows = 0
        self.fail_append = False

    def col_values(self, n):
        return [r[n - 1] if len(r) >= n else "" for r in self.rows]

    def append_row(self, row):
        self.rows.append(list(row))

    def append_rows(self, rows, value_input_option=None):
        if self.fail_append:
            raise RuntimeError("quota exceeded")
        self.appended.append((list(rows), value_input_option))
        self.rows.extend(rows)

    def add_rows(self, n):
        self.added_rows += n
        self.row_count += n


class FakeSpreadsheet:
    def __init__(self, worksheet=None):
        self.ws = worksheet
        self.created = []

    def worksheet(self, name):
        if self.ws is None:
            raise sys.modules["gspread"].WorksheetNotFound(name)
        return self.ws

    def add_worksheet(self, title, rows, cols):
        self.created.append((title, rows, cols))
        self.ws = FakeWorksheet(row_count=rows)
        return self.ws


class FakeClient:
    def __init__(self, spreadsheet):
        self.spreadsheet = spreadsheet
        self.timeout = None
        self.opened = None

    def set_timeout(self, t):
        self.timeout = t

    def open_by_key(self, key):
        self.opened = key
        return self.spreadsheet


def fake_gspread():
    module = types.ModuleType("gspread")

    class WorksheetNotFound(Exception):
        pass

    module.WorksheetNotFound = WorksheetNotFound
    return module


class WriterTests(unittest.TestCase):
    ENV = {"PURGE_LOG_SHEET_SWITCH": "true", "PIPELINE_METRICS_SHEET_ID": "sheet123"}

    def _env(self, **extra):
        clean = {k: v for k, v in os.environ.items() if not k.startswith("PURGE_LOG_") and k != "PIPELINE_METRICS_SHEET_ID"}
        clean.update(self.ENV)
        clean.update(extra)
        return mock.patch.dict(os.environ, clean, clear=True)

    def _run(self, spreadsheet, env=None, results=None, error=None):
        client = FakeClient(spreadsheet)
        with self._env(**(env or {})), mock.patch.dict(sys.modules, {"gspread": fake_gspread()}), \
             mock.patch("pipeline.metrics_logging._resolve_service_account_client", return_value=client):
            ok = psl.log_purge_run(results if results is not None else sample_results(), make_run(), 12.0, error)
        return ok, client

    def test_creates_the_tab_with_a_header_once_and_appends_everything_in_one_call(self):
        sheet = FakeSpreadsheet()
        ok, client = self._run(sheet)
        self.assertTrue(ok)
        self.assertEqual(client.opened, "sheet123")
        self.assertEqual(client.timeout, psl.TIMEOUT_S)
        self.assertEqual(sheet.created[0][0], "Data Purge Logs")
        self.assertEqual(sheet.ws.rows[0], psl.HEADER_ROW)
        self.assertEqual(len(sheet.ws.appended), 1)
        rows, option = sheet.ws.appended[0]
        self.assertEqual((len(rows), option), (7, "USER_ENTERED"))

    def test_an_existing_tab_is_reused_without_a_second_header(self):
        ws = FakeWorksheet(existing=[psl.HEADER_ROW])
        ok, _ = self._run(FakeSpreadsheet(ws))
        self.assertTrue(ok)
        self.assertEqual(sum(1 for r in ws.rows if r == psl.HEADER_ROW), 1)

    def test_previous_run_is_read_from_the_latest_earlier_brand_row(self):
        old = psl._blank_row()
        old[COL["row_type"]], old[COL["brand"]], old[COL["last_run_ist"]] = "BRAND", "pts", "2026-11-01 03:40:11"
        older = psl._blank_row()
        older[COL["row_type"]], older[COL["brand"]], older[COL["last_run_ist"]] = "BRAND", "pts", "2026-10-16 03:30:00"
        ws = FakeWorksheet(existing=[psl.HEADER_ROW, older, old])
        self._run(FakeSpreadsheet(ws))
        rows = ws.appended[0][0]
        pts = next(r for r in rows if r[COL["row_type"]] == "BRAND" and r[COL["brand"]] == "pts")
        self.assertEqual(pts[COL["previous_run_ist"]], "2026-11-01 03:40:11")

    def test_a_full_tab_is_grown_before_appending(self):
        ws = FakeWorksheet(existing=[psl.HEADER_ROW] * 998, row_count=1000)
        self._run(FakeSpreadsheet(ws))
        self.assertGreater(ws.added_rows, 0)
        self.assertGreaterEqual(ws.row_count, 998 + 7)

    def test_custom_tab_and_dedicated_sheet_id(self):
        sheet = FakeSpreadsheet()
        ok, client = self._run(sheet, env={"PURGE_LOG_SHEET_TAB": "Purge", "PURGE_LOG_SHEET_ID": "other"})
        self.assertEqual((client.opened, sheet.created[0][0]), ("other", "Purge"))

    def test_switch_off_missing_id_and_missing_credentials_are_all_skipped_quietly(self):
        sheet = FakeSpreadsheet()
        ok, client = self._run(sheet, env={"PURGE_LOG_SHEET_SWITCH": "false"})
        self.assertFalse(ok)
        self.assertIsNone(client.opened)
        with self._env(PIPELINE_METRICS_SHEET_ID=""), \
             mock.patch("pipeline.metrics_logging._resolve_service_account_client") as resolve:
            self.assertFalse(psl.log_purge_run(sample_results(), make_run(), 1.0))
        resolve.assert_not_called()
        with self._env(), mock.patch.dict(sys.modules, {"gspread": fake_gspread()}), \
             mock.patch("pipeline.metrics_logging._resolve_service_account_client", side_effect=ValueError("no credentials")):
            self.assertFalse(psl.log_purge_run(sample_results(), make_run(), 1.0))

    def test_every_failure_is_swallowed(self):
        ws = FakeWorksheet(existing=[psl.HEADER_ROW])
        ws.fail_append = True
        ok, _ = self._run(FakeSpreadsheet(ws))
        self.assertFalse(ok)  # quota error: logged, not raised
        with self._env(), mock.patch.dict(sys.modules, {"gspread": fake_gspread()}), \
             mock.patch("pipeline.metrics_logging._resolve_service_account_client", return_value=mock.Mock(
                 open_by_key=mock.Mock(side_effect=RuntimeError("sheet not shared")))):
            self.assertFalse(psl.log_purge_failure("boom", RUN_DT, 1.0))

    def test_a_failure_row_is_written_for_runs_that_never_started(self):
        sheet = FakeSpreadsheet()
        client = FakeClient(sheet)
        with self._env(), mock.patch.dict(sys.modules, {"gspread": fake_gspread()}), \
             mock.patch("pipeline.metrics_logging._resolve_service_account_client", return_value=client):
            self.assertTrue(psl.log_purge_failure("ConfigError: bad", RUN_DT, 0.2, None, "invalid configuration"))
        rows = sheet.ws.appended[0][0]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][COL["error"]], "ConfigError: bad")

    def test_unreadable_history_only_blanks_previous_run(self):
        ws = FakeWorksheet(existing=[psl.HEADER_ROW])
        ws.col_values = mock.Mock(side_effect=[RuntimeError("read quota"), [""]])
        ok, _ = self._run(FakeSpreadsheet(ws))
        self.assertTrue(ok)
        pts = next(r for r in ws.appended[0][0] if r[COL["row_type"]] == "BRAND")
        self.assertEqual(pts[COL["previous_run_ist"]], "")


if __name__ == "__main__":
    unittest.main()
