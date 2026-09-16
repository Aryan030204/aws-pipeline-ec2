import os
import sys
import unittest
from unittest.mock import MagicMock, patch

from workers import registry
from workers.run import build_parser, list_workers


class RegistryTests(unittest.TestCase):
    def test_every_worker_module_exists_and_exposes_run_worker(self):
        import importlib.util

        for spec in registry.WORKERS.values():
            self.assertIsNotNone(importlib.util.find_spec(spec.module), spec.module)
            path = os.path.join(registry.PROJECT_ROOT, *spec.module.split(".")) + ".py"
            with open(path, encoding="utf-8") as fh:
                source = fh.read()
            self.assertIn("def run_worker(", source, spec.module)
            self.assertIn(spec.self_run_env, source, spec.module)
            if spec.range_env:
                for var in spec.range_env:
                    self.assertIn(var, source, spec.module)
            self.assertEqual("current_dt" in source.split("def run_worker(", 1)[1][:80], spec.accepts_as_of)

    def test_unknown_worker_is_rejected(self):
        with self.assertRaises(registry.UnknownWorkerError):
            registry.get_spec("nope")

    def test_cli_command_for_pnl_range(self):
        cmd = registry.build_cli_command("PNL", start="2026-09-01", end="2026-09-03")
        self.assertEqual(cmd[1:], ["-m", "workers.run", "pnl", "--start", "2026-09-01", "--end", "2026-09-03"])

    def test_range_requires_both_ends_and_a_range_worker(self):
        with self.assertRaises(ValueError):
            registry.build_cli_command("pnl", start="2026-09-01")
        with self.assertRaises(ValueError):
            registry.build_cli_command("pnl", start="2026-09-05", end="2026-09-01")
        with self.assertRaises(ValueError):
            registry.build_cli_command("checkout-login-status", start="2026-09-01", end="2026-09-02")
        with self.assertRaises(ValueError):
            registry.build_cli_command("intent-events", as_of="2026-09-01T00:00:00")

    def test_run_in_process_sets_env_then_calls_run_worker(self):
        fake_module = MagicMock()
        with patch.dict(os.environ, {}, clear=False), patch(
            "importlib.import_module", return_value=fake_module
        ) as import_module:
            registry.run_in_process("pnl", start="2026-09-01", end="2026-09-02")
            self.assertEqual(os.environ["PNL_WORKER_SELF_RUN"], "false")
            self.assertEqual(os.environ["PNL_WORKER_START_DATE"], "2026-09-01")
            self.assertEqual(os.environ["PNL_WORKER_END_DATE"], "2026-09-02")
        import_module.assert_called_once_with("workers.pnl_worker")
        fake_module.run_worker.assert_called_once_with()

    def test_run_in_process_passes_as_of(self):
        fake_module = MagicMock()
        with patch("importlib.import_module", return_value=fake_module):
            registry.run_in_process("checkout-login-status", as_of="2026-09-10T00:05:00+05:30")
        current_dt = fake_module.run_worker.call_args.kwargs["current_dt"]
        self.assertEqual(current_dt.isoformat(), "2026-09-10T00:05:00+05:30")


class CliTests(unittest.TestCase):
    def test_parser_and_listing(self):
        args = build_parser().parse_args(["pnl", "--start", "2026-09-01", "--end", "2026-09-02"])
        self.assertEqual((args.worker, args.start, args.end), ("pnl", "2026-09-01", "2026-09-02"))
        listing = list_workers()
        for name in registry.WORKERS:
            self.assertIn(name, listing)


class HttpTriggerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pipeline import worker_triggers
        from pipeline.state import app

        cls.module = worker_triggers
        cls.client = app.test_client()

    def setUp(self):
        self.module._runs.clear()
        self.env = patch.dict(os.environ, {"PIPELINE_AUTH_HEADER": "secret"}, clear=False)
        self.env.start()

    def tearDown(self):
        self.env.stop()

    def _fake_popen(self, pid=4242, returncode=None):
        proc = MagicMock()
        proc.pid = pid
        proc.poll.return_value = returncode
        return proc

    def test_list_is_public_and_names_every_worker(self):
        resp = self.client.get("/workers")
        self.assertEqual(resp.status_code, 200)
        names = {w["name"] for w in resp.get_json()["workers"]}
        self.assertEqual(names, set(registry.WORKERS))

    def test_trigger_requires_key(self):
        resp = self.client.post("/workers/pnl/trigger")
        self.assertEqual(resp.status_code, 401)

    def test_trigger_spawns_cli_and_reports_status(self):
        proc = self._fake_popen()
        with patch.object(self.module, "_spawn", return_value=proc) as spawn:
            resp = self.client.post(
                "/workers/pnl/trigger",
                json={"start": "2026-09-01", "end": "2026-09-02"},
                headers={"x-pipeline-key": "secret"},
            )
        self.assertEqual(resp.status_code, 202)
        body = resp.get_json()
        self.assertEqual(body["state"], "running")
        self.assertEqual(body["pid"], 4242)
        self.assertEqual(spawn.call_args[0][0][1:], ["-m", "workers.run", "pnl", "--start", "2026-09-01", "--end", "2026-09-02"])

        status = self.client.get("/workers/pnl/status").get_json()
        self.assertEqual(status["state"], "running")

        proc.poll.return_value = 0
        status = self.client.get("/workers/pnl/status").get_json()
        self.assertEqual(status["state"], "succeeded")

    def test_second_trigger_while_running_is_409(self):
        with patch.object(self.module, "_spawn", return_value=self._fake_popen()):
            self.client.post("/workers/pnl/trigger", headers={"x-pipeline-key": "secret"})
            resp = self.client.post("/workers/pnl/trigger", headers={"x-pipeline-key": "secret"})
        self.assertEqual(resp.status_code, 409)

    def test_bad_input_is_400_and_unknown_is_404(self):
        headers = {"x-pipeline-key": "secret"}
        self.assertEqual(self.client.post("/workers/pnl/trigger", json={"start": "2026-09-01"}, headers=headers).status_code, 400)
        self.assertEqual(self.client.post("/workers/nope/trigger", headers=headers).status_code, 404)
        self.assertEqual(self.client.get("/workers/nope/status").status_code, 404)
        self.assertEqual(self.client.get("/workers/pnl/status").get_json()["state"], "idle")


if __name__ == "__main__":
    unittest.main()
