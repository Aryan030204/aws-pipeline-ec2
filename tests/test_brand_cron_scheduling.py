import os
import unittest
from unittest import mock

# pipeline/orchestration.py does `import aws_background` at module level, and
# aws_background.py does `from pipeline.orchestration import (...)` - safe
# order is aws_background first (see other worker test files), otherwise
# pipeline.orchestration is only partially initialized when aws_background.py
# tries to import names from it (circular import).
import aws_background
import pipeline.orchestration as orchestration


class ParseBrandsCronMapTests(unittest.TestCase):
    def test_valid_json_is_parsed_with_int_keys(self):
        with mock.patch.dict(os.environ, {"BRANDS_CRON_MAP": '{"3": "15,45", "1": "0,30"}'}):
            self.assertEqual(orchestration._parse_brands_cron_map(), {3: "15,45", 1: "0,30"})

    def test_unset_env_var_gives_empty_mapping(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("BRANDS_CRON_MAP", None)
            self.assertEqual(orchestration._parse_brands_cron_map(), {})

    def test_invalid_json_gives_empty_mapping(self):
        with mock.patch.dict(os.environ, {"BRANDS_CRON_MAP": "not json"}):
            self.assertEqual(orchestration._parse_brands_cron_map(), {})

    def test_non_dict_json_gives_empty_mapping(self):
        with mock.patch.dict(os.environ, {"BRANDS_CRON_MAP": "[1, 2, 3]"}):
            self.assertEqual(orchestration._parse_brands_cron_map(), {})

    def test_non_numeric_keys_are_skipped(self):
        with mock.patch.dict(os.environ, {"BRANDS_CRON_MAP": '{"abc": "15,45", "2": "0,30"}'}):
            self.assertEqual(orchestration._parse_brands_cron_map(), {2: "0,30"})

    def test_values_are_stripped(self):
        with mock.patch.dict(os.environ, {"BRANDS_CRON_MAP": '{"1": "  0,30  "}'}):
            self.assertEqual(orchestration._parse_brands_cron_map(), {1: "0,30"})


class GroupBrandsByCronTests(unittest.TestCase):
    def test_brands_sharing_a_cron_value_land_in_one_group(self):
        groups = orchestration._group_brands_by_cron({1: "0,30", 2: "0,30", 3: "15,45"})
        self.assertEqual(groups, {"0,30": [1, 2], "15,45": [3]})

    def test_empty_map_gives_empty_groups(self):
        self.assertEqual(orchestration._group_brands_by_cron({}), {})

    def test_groups_are_sorted(self):
        groups = orchestration._group_brands_by_cron({8: "0,30", 1: "0,30", 6: "0,30"})
        self.assertEqual(groups["0,30"], [1, 6, 8])


class RunDataPipelineFallbackTests(unittest.TestCase):
    def test_only_unmapped_brands_are_included(self):
        with mock.patch.object(orchestration, "active_brand_indices", [1, 2, 3, 4]), \
             mock.patch.object(orchestration, "_parse_brands_cron_map", return_value={3: "15,45", 4: "15,45"}), \
             mock.patch.object(orchestration, "_execute_pipeline_run") as execute:
            orchestration.run_data_pipeline()

        execute.assert_called_once_with([1, 2], run_id=None)

    def test_empty_map_runs_every_active_brand(self):
        with mock.patch.object(orchestration, "active_brand_indices", [1, 2, 3]), \
             mock.patch.object(orchestration, "_parse_brands_cron_map", return_value={}), \
             mock.patch.object(orchestration, "_execute_pipeline_run") as execute:
            orchestration.run_data_pipeline()

        execute.assert_called_once_with([1, 2, 3], run_id=None)


class RunBrandGroupTests(unittest.TestCase):
    def test_only_captured_indices_are_included(self):
        with mock.patch.object(orchestration, "active_brand_indices", [1, 2, 3, 4]), \
             mock.patch.object(orchestration, "_execute_pipeline_run") as execute:
            orchestration._run_brand_group([3, 4])()

        execute.assert_called_once_with([3, 4], run_id=None)

    def test_deactivated_brand_is_dropped(self):
        with mock.patch.object(orchestration, "active_brand_indices", [1, 3]), \
             mock.patch.object(orchestration, "_execute_pipeline_run") as execute:
            orchestration._run_brand_group([3, 4])()  # 4 isn't active

        execute.assert_called_once_with([3], run_id=None)

    def test_two_groups_are_independent_closures(self):
        with mock.patch.object(orchestration, "active_brand_indices", [1, 2, 3, 4]), \
             mock.patch.object(orchestration, "_execute_pipeline_run") as execute:
            group_a = orchestration._run_brand_group([1, 2])
            group_b = orchestration._run_brand_group([3, 4])
            group_a()
            group_b()

        execute.assert_any_call([1, 2], run_id=None)
        execute.assert_any_call([3, 4], run_id=None)
        self.assertEqual(execute.call_count, 2)


class ExecutePipelineRunBackfillFilterTests(unittest.TestCase):
    """BACKFILL_MODE/BACKFILL_BRAND_INDICES filtering happens once inside
    _execute_pipeline_run, so it applies no matter which caller supplied
    target_indices."""

    def test_backfill_mode_filters_target_indices(self):
        with mock.patch.object(aws_background, "BACKFILL_MODE", True), \
             mock.patch.object(aws_background, "BACKFILL_BRAND_INDICES", {2}), \
             mock.patch.object(orchestration, "run_brand_preflight_schema_migrations"), \
             mock.patch.object(orchestration, "ThreadPoolExecutor") as executor_cls, \
             mock.patch.object(orchestration, "trigger_cache_pipeline"), \
             mock.patch.object(orchestration, "trigger_utm_pipeline"):
            fake_executor = mock.MagicMock()
            fake_executor.__enter__.return_value = fake_executor
            fake_executor.submit.return_value = mock.Mock()
            executor_cls.return_value = fake_executor

            with mock.patch.object(orchestration, "as_completed", return_value=[]):
                orchestration._execute_pipeline_run([1, 2, 3], run_id=None)

        # Only brand 2 (the backfill filter) should have been submitted.
        submitted_indices = [call.args[1] for call in fake_executor.submit.call_args_list]
        self.assertEqual(submitted_indices, [2])

    def test_backfill_mode_runs_preflight_migrations_only_for_backfill_brands_before_workers(self):
        order = []
        with mock.patch.object(aws_background, "BACKFILL_MODE", True), \
             mock.patch.object(aws_background, "BACKFILL_BRAND_INDICES", {2}), \
             mock.patch.object(
                 orchestration,
                 "run_brand_preflight_schema_migrations",
                 side_effect=lambda idx: order.append(("preflight", idx)),
             ) as preflight, \
             mock.patch.object(orchestration, "ThreadPoolExecutor") as executor_cls, \
             mock.patch.object(orchestration, "trigger_cache_pipeline"), \
             mock.patch.object(orchestration, "trigger_utm_pipeline"):
            fake_executor = mock.MagicMock()
            fake_executor.__enter__.return_value = fake_executor
            fake_executor.submit.side_effect = lambda fn, idx, *a, **k: order.append(("worker", idx)) or mock.Mock()
            executor_cls.return_value = fake_executor

            with mock.patch.object(orchestration, "as_completed", return_value=[]):
                orchestration._execute_pipeline_run([1, 2, 3], run_id=None)

        preflight.assert_called_once_with(2)
        self.assertEqual(order, [("preflight", 2), ("worker", 2)])

    def test_backfill_preflight_failure_stops_the_backfill(self):
        with mock.patch.object(aws_background, "BACKFILL_MODE", True), \
             mock.patch.object(aws_background, "BACKFILL_BRAND_INDICES", {2}), \
             mock.patch.object(
                 orchestration, "run_brand_preflight_schema_migrations", side_effect=RuntimeError("ddl failed")
             ), \
             mock.patch.object(orchestration, "ThreadPoolExecutor") as executor_cls, \
             mock.patch.object(orchestration, "trigger_cache_pipeline"), \
             mock.patch.object(orchestration, "trigger_utm_pipeline"):
            orchestration._execute_pipeline_run([1, 2], run_id=None)  # failure is logged, not raised

        executor_cls.assert_not_called()  # no brand worker started

    def test_no_backfill_mode_runs_every_target_index(self):
        with mock.patch.object(aws_background, "BACKFILL_MODE", False), \
             mock.patch.object(orchestration, "run_brand_preflight_schema_migrations"), \
             mock.patch.object(orchestration, "ThreadPoolExecutor") as executor_cls, \
             mock.patch.object(orchestration, "trigger_cache_pipeline"), \
             mock.patch.object(orchestration, "trigger_utm_pipeline"):
            fake_executor = mock.MagicMock()
            fake_executor.__enter__.return_value = fake_executor
            fake_executor.submit.return_value = mock.Mock()
            executor_cls.return_value = fake_executor

            with mock.patch.object(orchestration, "as_completed", return_value=[]):
                orchestration._execute_pipeline_run([1, 2], run_id=None)

        submitted_indices = [call.args[1] for call in fake_executor.submit.call_args_list]
        self.assertEqual(sorted(submitted_indices), [1, 2])

    def test_empty_target_indices_is_a_no_op(self):
        with mock.patch.object(orchestration, "run_brand_preflight_schema_migrations") as preflight:
            orchestration._execute_pipeline_run([], run_id=None)  # must not raise
        preflight.assert_not_called()


class ManualTriggerTests(unittest.TestCase):
    def setUp(self):
        self.client = orchestration.app.test_client()
        patcher = mock.patch.object(aws_background, "PIPELINE_AUTH_HEADER", "secret-key")
        patcher.start()
        self.addCleanup(patcher.stop)

    def _post(self, body):
        return self.client.post(
            "/trigger", json=body, headers={"x-pipeline-key": "secret-key"}
        )

    def test_no_brand_ids_targets_every_active_brand(self):
        with mock.patch.object(orchestration, "active_brand_indices", [1, 2, 3]), \
             mock.patch.object(orchestration.scheduler, "add_job") as add_job:
            resp = self._post({"runId": "run-1"})

        self.assertEqual(resp.status_code, 200)
        args, kwargs = add_job.call_args
        self.assertEqual(args[0], orchestration._execute_pipeline_run)
        self.assertEqual(kwargs["args"], [[1, 2, 3]])
        self.assertEqual(kwargs["kwargs"], {"run_id": "run-1"})
        self.assertIsNotNone(kwargs["next_run_time"])

    def test_brand_ids_scopes_to_those_brands_only(self):
        with mock.patch.object(orchestration, "active_brand_indices", [1, 2, 3, 4]), \
             mock.patch.object(orchestration.scheduler, "add_job") as add_job:
            resp = self._post({"runId": "run-2", "brandIds": [3, 4]})

        self.assertEqual(resp.status_code, 200)
        kwargs = add_job.call_args.kwargs
        self.assertEqual(kwargs["args"], [[3, 4]])

    def test_single_brand_id_field_also_works(self):
        with mock.patch.object(orchestration, "active_brand_indices", [1, 2]), \
             mock.patch.object(orchestration.scheduler, "add_job") as add_job:
            resp = self._post({"runId": "run-3", "brandId": 2})

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(add_job.call_args.kwargs["args"], [[2]])

    def test_inactive_brand_ids_are_dropped_but_valid_ones_still_run(self):
        with mock.patch.object(orchestration, "active_brand_indices", [1, 2]), \
             mock.patch.object(orchestration.scheduler, "add_job") as add_job:
            resp = self._post({"runId": "run-4", "brandIds": [2, 999]})

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(add_job.call_args.kwargs["args"], [[2]])

    def test_all_invalid_brand_ids_returns_400(self):
        with mock.patch.object(orchestration, "active_brand_indices", [1, 2]), \
             mock.patch.object(orchestration.scheduler, "add_job") as add_job:
            resp = self._post({"runId": "run-5", "brandIds": [999]})

        self.assertEqual(resp.status_code, 400)
        add_job.assert_not_called()

    def test_non_integer_brand_ids_returns_400(self):
        with mock.patch.object(orchestration, "active_brand_indices", [1, 2]), \
             mock.patch.object(orchestration.scheduler, "add_job") as add_job:
            resp = self._post({"runId": "run-6", "brandIds": ["not-a-number"]})

        self.assertEqual(resp.status_code, 400)
        add_job.assert_not_called()

    def test_missing_run_id_returns_400(self):
        with mock.patch.object(orchestration.scheduler, "add_job") as add_job:
            resp = self._post({})

        self.assertEqual(resp.status_code, 400)
        add_job.assert_not_called()

    def test_wrong_auth_key_returns_401(self):
        resp = self.client.post(
            "/trigger", json={"runId": "run-7"}, headers={"x-pipeline-key": "wrong"}
        )
        self.assertEqual(resp.status_code, 401)

    def test_job_id_is_unique_per_call_not_the_shared_pipeline_job(self):
        with mock.patch.object(orchestration, "active_brand_indices", [1]), \
             mock.patch.object(orchestration.scheduler, "add_job") as add_job:
            self._post({"runId": "run-8"})
            self._post({"runId": "run-9"})

        job_ids = [call.kwargs["id"] for call in add_job.call_args_list]
        self.assertEqual(len(set(job_ids)), 2)
        self.assertTrue(all(jid.startswith("manual_trigger_") for jid in job_ids))


class SchedulerRegistrationTests(unittest.TestCase):
    """
    These exercise the registration logic conceptually (grouping + fallback
    job id/minute values) without re-running aws_background.py's __main__
    block (which has real side effects - Flask app.run(), DB pool setup).
    """

    def test_group_and_fallback_job_ids_and_minutes(self):
        cron_map = {1: "0,30", 2: "0,30", 3: "15,45", 4: "15,45"}
        groups = orchestration._group_brands_by_cron(cron_map)

        registered = []
        for cron_value, group_indices in groups.items():
            sanitized = "".join(ch if ch.isalnum() else "_" for ch in cron_value)
            registered.append((f"pipeline_job_group_{sanitized}", cron_value, group_indices))
        registered.append(("pipeline_job", "0,10,20,30,40,50", None))

        job_ids = [r[0] for r in registered]
        self.assertEqual(len(job_ids), len(set(job_ids)))  # all unique
        self.assertIn(("pipeline_job_group_0_30", "0,30", [1, 2]), registered)
        self.assertIn(("pipeline_job_group_15_45", "15,45", [3, 4]), registered)
        self.assertIn(("pipeline_job", "0,10,20,30,40,50", None), registered)


if __name__ == "__main__":
    unittest.main()
