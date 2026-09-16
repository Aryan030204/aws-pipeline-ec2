"""
HTTP knob for the standalone workers, mounted on the main pipeline's Flask
app (port 5000) next to the existing POST /trigger:

    GET  /workers                      list workers + whether one is running
    POST /workers/<name>/trigger       run <name> now (x-pipeline-key required)
                                       optional JSON: {"start": "YYYY-MM-DD",
                                                       "end":   "YYYY-MM-DD",
                                                       "as_of": "<ISO datetime>"}
    GET  /workers/<name>/status        running / exited (+ exit code)

A trigger spawns `python -m workers.run <name> ...` as a child process of the
pipeline container, inheriting its environment, so the worker discovers
brands and connects to their databases exactly as it does in its own
container - but without touching the pipeline process's own brand state.
Only one run per worker at a time; a second trigger gets 409.
"""

import logging
import os
import subprocess
import threading
from datetime import datetime, timezone
from typing import Dict, Optional

from flask import jsonify, request

from pipeline.state import app
from workers.registry import (
    PROJECT_ROOT,
    WORKERS,
    UnknownWorkerError,
    build_cli_command,
)

logger = logging.getLogger("aws_background")


class _Run:
    def __init__(self, name: str, process: subprocess.Popen, command: list):
        self.name = name
        self.process = process
        self.command = command
        self.started_at = datetime.now(timezone.utc)
        self.finished_at: Optional[datetime] = None

    @property
    def returncode(self) -> Optional[int]:
        code = self.process.poll()
        if code is not None and self.finished_at is None:
            self.finished_at = datetime.now(timezone.utc)
        return code

    def to_dict(self) -> Dict:
        code = self.returncode
        return {
            "worker": self.name,
            "pid": self.process.pid,
            "state": "running" if code is None else ("succeeded" if code == 0 else "failed"),
            "exit_code": code,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "command": self.command[1:],  # drop the interpreter path
        }


_runs: Dict[str, _Run] = {}
_runs_lock = threading.Lock()


def _authorized() -> bool:
    expected = os.environ.get("PIPELINE_AUTH_HEADER", "")
    provided = request.headers.get("x-pipeline-key", "")
    return bool(expected) and provided == expected


def _spawn(command: list) -> subprocess.Popen:
    # stdout/stderr inherit the container's, so `docker logs shopify-pipeline`
    # shows the worker's output interleaved with the pipeline's.
    return subprocess.Popen(command, cwd=PROJECT_ROOT, env=os.environ.copy())


def start_worker_run(name: str, start=None, end=None, as_of=None) -> _Run:
    """Spawns the worker CLI. Raises UnknownWorkerError / ValueError for bad
    input and RuntimeError if that worker is already running."""
    command = build_cli_command(name, start=start, end=end, as_of=as_of)
    key = command[3]  # normalised worker name
    with _runs_lock:
        current = _runs.get(key)
        if current is not None and current.returncode is None:
            raise RuntimeError(f"worker '{key}' is already running (pid {current.process.pid})")
        run = _Run(key, _spawn(command), command)
        _runs[key] = run
    logger.info("🛠️ Manual worker trigger: %s (pid %s) via %s", key, run.process.pid, request.remote_addr)
    return run


@app.route("/workers", methods=["GET"])
def list_workers_route():
    with _runs_lock:
        runs = {name: run.to_dict() for name, run in _runs.items()}
    return jsonify(
        {
            "workers": [
                {
                    "name": name,
                    "description": spec.description,
                    "accepts_range": bool(spec.range_env),
                    "accepts_as_of": spec.accepts_as_of,
                    "last_run": runs.get(name),
                }
                for name, spec in sorted(WORKERS.items())
            ]
        }
    )


@app.route("/workers/<name>/trigger", methods=["POST"])
def trigger_worker_route(name: str):
    if not _authorized():
        logger.warning("🚫 Unauthorized worker trigger attempt from %s", request.remote_addr)
        return jsonify({"error": "Unauthorized"}), 401

    body = request.get_json(silent=True) or {}
    try:
        run = start_worker_run(
            name,
            start=body.get("start"),
            end=body.get("end"),
            as_of=body.get("as_of"),
        )
    except UnknownWorkerError as exc:
        return jsonify({"status": "error", "message": exc.args[0]}), 404
    except ValueError as exc:
        return jsonify({"status": "error", "message": str(exc)}), 400
    except RuntimeError as exc:
        return jsonify({"status": "error", "message": str(exc)}), 409

    return jsonify({"status": "started", **run.to_dict()}), 202


@app.route("/workers/<name>/status", methods=["GET"])
def worker_status_route(name: str):
    key = (name or "").strip().lower()
    if key not in WORKERS:
        return jsonify({"status": "error", "message": f"Unknown worker '{name}'"}), 404
    with _runs_lock:
        run = _runs.get(key)
    if run is None:
        return jsonify({"worker": key, "state": "idle"})
    return jsonify(run.to_dict())
