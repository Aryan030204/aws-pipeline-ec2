"""
Single source of truth for "which workers exist and how to run one on
demand". Used by:

  - `python -m workers.run <name>`            (CLI, runs the worker in-process)
  - POST /workers/<name>/trigger on the main  (HTTP knob, spawns the CLI as a
    pipeline's Flask app                       subprocess so the worker never
                                               re-initialises brand configs
                                               inside the pipeline process)

Adding a worker = adding one WorkerSpec below.
"""

import importlib
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@dataclass(frozen=True)
class WorkerSpec:
    name: str
    module: str
    description: str
    # The env flag the worker's __main__ checks to decide "schedule" vs "run once".
    self_run_env: str
    # (START, END) env vars for workers that take an explicit date range.
    range_env: Optional[Tuple[str, str]] = None
    # Whether run_worker() accepts current_dt=<datetime> to pretend it is
    # running at another time (shifts the rolling window).
    accepts_as_of: bool = True
    # Whether run_worker() accepts brand_key=<brand_id> to restrict the run
    # to one brand instead of every active brand.
    accepts_brand: bool = False


WORKERS: Dict[str, WorkerSpec] = {
    spec.name: spec
    for spec in (
        WorkerSpec(
            name="pnl",
            module="workers.pnl_worker",
            description="Daily brand-level P&L -> overall_pnl in each brand DB",
            self_run_env="PNL_WORKER_SELF_RUN",
            range_env=("PNL_WORKER_START_DATE", "PNL_WORKER_END_DATE"),
            accepts_brand=True,
        ),
        WorkerSpec(
            name="events-summary",
            module="workers.events_summary_worker",
            description="Daily event totals across brands -> master summary table",
            self_run_env="EVENTS_WORKER_SELF_RUN",
        ),
        WorkerSpec(
            name="variant-wise-events",
            module="workers.variant_wise_events_worker",
            description="Any Mongo event type per product variant -> each brand DB",
            self_run_env="VARIANT_WISE_EVENTS_WORKER_SELF_RUN",
        ),
        WorkerSpec(
            name="checkout-login-status",
            module="workers.checkout_login_status_worker",
            description="Daily checkout counts by login status -> each brand DB",
            self_run_env="CHECKOUT_LOGIN_STATUS_WORKER_SELF_RUN",
        ),
        WorkerSpec(
            name="intent-events",
            module="workers.intent_events_worker",
            description="Behavioural intent / click events -> each brand DB",
            self_run_env="INTENT_EVENTS_WORKER_SELF_RUN",
            accepts_as_of=False,
        ),
    )
}


class UnknownWorkerError(KeyError):
    pass


def get_spec(name: str) -> WorkerSpec:
    key = (name or "").strip().lower()
    if key not in WORKERS:
        raise UnknownWorkerError(f"Unknown worker '{name}'. Known: {', '.join(sorted(WORKERS))}")
    return WORKERS[key]


def _iso_date(value: Optional[str], label: str) -> Optional[str]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
    except ValueError as exc:
        raise ValueError(f"{label} must be YYYY-MM-DD, got '{value}'") from exc


def validate_options(
    spec: WorkerSpec,
    start: Optional[str] = None,
    end: Optional[str] = None,
    as_of: Optional[str] = None,
    brand: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    """Normalises/validates the run options for a worker; raises ValueError
    on anything the worker can't honour."""
    start = _iso_date(start, "start")
    end = _iso_date(end, "end")
    if bool(start) != bool(end):
        raise ValueError("start and end must be given together")
    if start and end and start > end:
        raise ValueError("start must be <= end")
    if start and not spec.range_env:
        raise ValueError(f"worker '{spec.name}' does not take a start/end range")

    as_of_norm = None
    if as_of:
        if not spec.accepts_as_of:
            raise ValueError(f"worker '{spec.name}' does not take as_of")
        try:
            as_of_norm = datetime.fromisoformat(as_of).isoformat()
        except ValueError as exc:
            raise ValueError(f"as_of must be an ISO datetime, got '{as_of}'") from exc

    brand_norm = brand.strip().upper() if brand else None
    if brand_norm and not spec.accepts_brand:
        raise ValueError(f"worker '{spec.name}' does not take a brand_id")

    return {"start": start, "end": end, "as_of": as_of_norm, "brand": brand_norm}


def build_cli_command(
    name: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    as_of: Optional[str] = None,
    brand: Optional[str] = None,
    env_file: Optional[str] = None,
) -> List[str]:
    """argv for running the worker once via the CLI (what the HTTP knob spawns)."""
    spec = get_spec(name)
    opts = validate_options(spec, start, end, as_of, brand)
    cmd = [sys.executable, "-m", "workers.run", spec.name]
    if opts["start"]:
        cmd += ["--start", opts["start"], "--end", opts["end"]]
    if opts["as_of"]:
        cmd += ["--as-of", opts["as_of"]]
    if opts["brand"]:
        cmd += ["--brand-id", opts["brand"]]
    if env_file:
        cmd += ["--env-file", env_file]
    return cmd


def run_in_process(
    name: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    as_of: Optional[str] = None,
    brand: Optional[str] = None,
) -> None:
    """Runs the worker once in this process. Env for the range is set before
    the module is imported, because workers read it at import time."""
    spec = get_spec(name)
    opts = validate_options(spec, start, end, as_of, brand)

    os.environ[spec.self_run_env] = "false"
    if opts["start"] and spec.range_env:
        os.environ[spec.range_env[0]] = opts["start"]
        os.environ[spec.range_env[1]] = opts["end"]

    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
    module = importlib.import_module(spec.module)

    kwargs = {}
    if opts["as_of"]:
        kwargs["current_dt"] = datetime.fromisoformat(opts["as_of"])
    if opts["brand"]:
        kwargs["brand_key"] = opts["brand"]
    module.run_worker(**kwargs)
