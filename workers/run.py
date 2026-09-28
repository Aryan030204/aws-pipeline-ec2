"""
Manual trigger CLI for the standalone workers.

    python -m workers.run list
    python -m workers.run pnl
    python -m workers.run pnl --start 2026-09-01 --end 2026-09-15
    python -m workers.run pnl --brand-id BBB --start 2026-09-01 --end 2026-09-15
    python -m workers.run checkout-login-status --as-of 2026-09-10T00:05:00+05:30
    python -m workers.run pnl --env-file env

Runs the chosen worker ONCE in the foreground and exits with 0 on success,
1 on failure. The worker's own scheduler is never started. Loads `.env`
by default (falling back to a file literally named `env`), or whatever
--env-file names.
"""

import argparse
import logging
import os
import sys

from dotenv import load_dotenv

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from workers.registry import WORKERS, UnknownWorkerError, run_in_process  # noqa: E402

logger = logging.getLogger("workers.run")


def _default_env_file() -> str:
    for candidate in (".env", "env"):
        path = os.path.join(PROJECT_ROOT, candidate)
        if os.path.isfile(path):
            return path
    return ""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m workers.run",
        description="Run one standalone worker immediately (once, no scheduler).",
    )
    parser.add_argument("worker", help="worker name, or 'list' to print the known workers")
    parser.add_argument("--start", help="YYYY-MM-DD; first date to rebuild (workers with a range)")
    parser.add_argument("--end", help="YYYY-MM-DD; last date to rebuild (workers with a range)")
    parser.add_argument("--as-of", dest="as_of", help="ISO datetime to run 'as if now' (shifts rolling windows)")
    parser.add_argument(
        "--brand-id",
        dest="brand_id",
        help="restrict the run to one brand's brand_id (workers that support it, e.g. pnl)",
    )
    parser.add_argument(
        "--env-file",
        dest="env_file",
        default=None,
        help="dotenv file to load first (default: .env, else ./env if present)",
    )
    return parser


def list_workers() -> str:
    width = max(len(name) for name in WORKERS)
    lines = []
    for name in sorted(WORKERS):
        spec = WORKERS[name]
        extras = []
        if spec.range_env:
            extras.append("--start/--end")
        if spec.accepts_as_of:
            extras.append("--as-of")
        lines.append(f"  {name.ljust(width)}  {spec.description}  [{', '.join(extras) or 'no options'}]")
    return "Known workers:\n" + "\n".join(lines)


def main(argv=None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    args = build_parser().parse_args(argv)

    if args.worker == "list":
        print(list_workers())
        return 0

    env_file = args.env_file if args.env_file is not None else _default_env_file()
    if env_file:
        if not os.path.isfile(env_file):
            logger.error("env file not found: %s", env_file)
            return 1
        load_dotenv(env_file, override=False)
        logger.info("Loaded env from %s", env_file)

    try:
        logger.info("▶ Running worker '%s' once (manual trigger)", args.worker)
        run_in_process(args.worker, start=args.start, end=args.end, as_of=args.as_of, brand=args.brand_id)
    except UnknownWorkerError as exc:
        logger.error("%s", exc.args[0])
        print(list_workers(), file=sys.stderr)
        return 1
    except ValueError as exc:
        logger.error("Invalid options: %s", exc)
        return 1
    except Exception:
        logger.exception("Worker '%s' failed", args.worker)
        return 1

    logger.info("✅ Worker '%s' finished", args.worker)
    return 0


if __name__ == "__main__":
    sys.exit(main())
