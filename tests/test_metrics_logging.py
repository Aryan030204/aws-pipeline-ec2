import base64
import json
import sys
import types
import unittest
from unittest import mock

from pipeline import metrics_logging as ml


def _sample_metrics(**overrides):
    base = {
        "brand_name": "PTS",
        "brand_index": 1,
        "new_orders_count": 360,
        "updated_orders_count": 12,
        "affected_dates_count": 1,
        "sessions_synced": 500,
        "atc_sessions_synced": 40,
        "success": True,
        "error_message": "",
        "duration_seconds": 12.5,
    }
    base.update(overrides)
    return base


class BuildRowsTests(unittest.TestCase):
    def test_one_row_per_brand_plus_one_summary_row(self):
        rows = ml._build_rows("job-1", [_sample_metrics(), _sample_metrics(brand_name="BBB", brand_index=2)], 30.0)
        self.assertEqual(len(rows), 3)  # 2 brands + ALL BRANDS

    def test_brand_row_fields_map_correctly(self):
        rows = ml._build_rows("job-1", [_sample_metrics()], 30.0)
        row = rows[0]
        # header order: timestamp, job_id, brand_name, brand_index, new_orders,
        # updated_orders, affected_dates, sessions_synced, atc_sessions_synced,
        # success, error_message, duration_seconds
        self.assertEqual(row[1], "job-1")
        self.assertEqual(row[2], "PTS")
        self.assertEqual(row[3], 1)
        self.assertEqual(row[4], 360)
        self.assertEqual(row[5], 12)
        self.assertEqual(row[6], 1)
        self.assertEqual(row[7], 500)
        self.assertEqual(row[8], 40)
        self.assertEqual(row[9], True)
        self.assertEqual(row[10], "")
        self.assertEqual(row[11], 12.5)

    def test_summary_row_aggregates_across_brands(self):
        rows = ml._build_rows(
            "job-1",
            [
                _sample_metrics(brand_name="PTS", new_orders_count=360, sessions_synced=500),
                _sample_metrics(brand_name="BBB", new_orders_count=100, sessions_synced=200, success=False, error_message="boom"),
            ],
            45.0,
        )
        summary = rows[-1]
        self.assertEqual(summary[2], "ALL BRANDS")
        self.assertEqual(summary[3], 2)  # brand count
        self.assertEqual(summary[4], 460)  # summed new orders
        self.assertEqual(summary[7], 700)  # summed sessions
        self.assertEqual(summary[9], False)  # not all brands succeeded
        self.assertIn("BBB: boom", summary[10])
        self.assertEqual(summary[11], 45.0)  # overall duration, not summed per-brand

    def test_summary_row_success_true_when_all_brands_succeed(self):
        rows = ml._build_rows("job-1", [_sample_metrics(), _sample_metrics(brand_name="BBB")], 10.0)
        self.assertEqual(rows[-1][9], True)

    def test_no_brands_still_produces_a_summary_row(self):
        rows = ml._build_rows("job-1", [], 5.0)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][2], "ALL BRANDS")
        self.assertEqual(rows[0][9], True)  # all([]) is True - no brands failed


class MonthlyWorksheetNameTests(unittest.TestCase):
    def test_name_format(self):
        import datetime

        name = ml._monthly_worksheet_name(datetime.datetime(2026, 9, 15))
        self.assertEqual(name, "Runs-2026-09")


class ServiceAccountResolutionTests(unittest.TestCase):
    def _fake_gspread_module(self):
        fake = types.ModuleType("gspread")
        fake.service_account = mock.Mock(return_value="client-from-file")
        fake.service_account_from_dict = mock.Mock(return_value="client-from-dict")

        class WorksheetNotFound(Exception):
            pass

        fake.WorksheetNotFound = WorksheetNotFound
        return fake

    def test_file_path_mode_used_when_set(self):
        fake_gspread = self._fake_gspread_module()
        with mock.patch.dict(sys.modules, {"gspread": fake_gspread}), \
             mock.patch.object(ml, "GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE", "/tmp/sa.json"), \
             mock.patch.object(ml, "GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON_B64", ""):
            client = ml._resolve_service_account_client()

        self.assertEqual(client, "client-from-file")
        fake_gspread.service_account.assert_called_once_with(filename="/tmp/sa.json")

    def test_base64_json_mode_used_when_file_not_set(self):
        fake_gspread = self._fake_gspread_module()
        info = {"type": "service_account", "client_email": "x@y.iam.gserviceaccount.com"}
        b64 = base64.b64encode(json.dumps(info).encode("utf-8")).decode("utf-8")
        with mock.patch.dict(sys.modules, {"gspread": fake_gspread}), \
             mock.patch.object(ml, "GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE", ""), \
             mock.patch.object(ml, "GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON_B64", b64):
            client = ml._resolve_service_account_client()

        self.assertEqual(client, "client-from-dict")
        fake_gspread.service_account_from_dict.assert_called_once_with(info)

    def test_raises_when_neither_configured(self):
        with mock.patch.object(ml, "GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE", ""), \
             mock.patch.object(ml, "GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON_B64", ""):
            with self.assertRaises(ValueError):
                ml._resolve_service_account_client()


class GetOrCreateMonthlyWorksheetTests(unittest.TestCase):
    def _fake_gspread_module(self):
        fake = types.ModuleType("gspread")

        class WorksheetNotFound(Exception):
            pass

        fake.WorksheetNotFound = WorksheetNotFound
        return fake

    def test_returns_existing_worksheet_without_creating(self):
        fake_gspread = self._fake_gspread_module()
        spreadsheet = mock.Mock()
        spreadsheet.worksheet.return_value = "existing-sheet"

        with mock.patch.dict(sys.modules, {"gspread": fake_gspread}):
            result = ml._get_or_create_monthly_worksheet(spreadsheet)

        self.assertEqual(result, "existing-sheet")
        spreadsheet.add_worksheet.assert_not_called()

    def test_creates_worksheet_and_writes_header_when_missing(self):
        fake_gspread = self._fake_gspread_module()
        spreadsheet = mock.Mock()
        spreadsheet.worksheet.side_effect = fake_gspread.WorksheetNotFound()
        new_ws = mock.Mock()
        spreadsheet.add_worksheet.return_value = new_ws

        with mock.patch.dict(sys.modules, {"gspread": fake_gspread}):
            result = ml._get_or_create_monthly_worksheet(spreadsheet)

        self.assertEqual(result, new_ws)
        new_ws.append_row.assert_called_once_with(ml.HEADER_ROW)


class DoLogTests(unittest.TestCase):
    def test_missing_sheet_id_skips_without_raising(self):
        with mock.patch.object(ml, "PIPELINE_METRICS_SHEET_ID", ""):
            ml._do_log("job-1", [_sample_metrics()], 10.0)  # must not raise

    def test_auth_failure_is_caught_and_logged_not_raised(self):
        with mock.patch.object(ml, "PIPELINE_METRICS_SHEET_ID", "sheet123"), \
             mock.patch.object(ml, "_resolve_service_account_client", side_effect=RuntimeError("bad creds")):
            ml._do_log("job-1", [_sample_metrics()], 10.0)  # must not raise

    def test_successful_path_appends_rows_to_the_monthly_worksheet(self):
        worksheet = mock.Mock()
        spreadsheet = mock.Mock()
        client = mock.Mock()
        client.open_by_key.return_value = spreadsheet

        with mock.patch.object(ml, "PIPELINE_METRICS_SHEET_ID", "sheet123"), \
             mock.patch.object(ml, "_resolve_service_account_client", return_value=client), \
             mock.patch.object(ml, "_get_or_create_monthly_worksheet", return_value=worksheet):
            ml._do_log("job-1", [_sample_metrics(), _sample_metrics(brand_name="BBB")], 10.0)

        client.open_by_key.assert_called_once_with("sheet123")
        worksheet.append_rows.assert_called_once()
        appended_rows = worksheet.append_rows.call_args.args[0]
        self.assertEqual(len(appended_rows), 3)  # 2 brands + summary


class LogPipelineRunMetricsAsyncTests(unittest.TestCase):
    def test_kill_switch_disables_logging_entirely(self):
        with mock.patch.object(ml, "PIPELINE_METRICS_SHEET_SWITCH", False), \
             mock.patch("threading.Thread") as thread_cls:
            ml.log_pipeline_run_metrics_async("job-1", [_sample_metrics()], 10.0)

        thread_cls.assert_not_called()

    def test_enabled_dispatches_a_background_thread(self):
        with mock.patch.object(ml, "PIPELINE_METRICS_SHEET_SWITCH", True), \
             mock.patch.object(ml.threading, "Thread") as thread_cls:
            fake_thread = mock.Mock()
            thread_cls.return_value = fake_thread

            ml.log_pipeline_run_metrics_async("job-1", [_sample_metrics()], 10.0)

        thread_cls.assert_called_once()
        kwargs = thread_cls.call_args.kwargs
        self.assertEqual(kwargs["target"], ml._do_log)
        self.assertEqual(kwargs["args"], ("job-1", [_sample_metrics()], 10.0))
        self.assertTrue(kwargs["daemon"])
        fake_thread.start.assert_called_once()


if __name__ == "__main__":
    unittest.main()
