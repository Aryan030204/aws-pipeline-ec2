"""
Optional memory probe (OFF by default). With MEM_PROBE=true every call logs one line of NUMBERS ONLY:

    MEM_PROBE stage=<stage> brand_index=<n> rss_mb=<..> hwm_mb=<..> threads=<..> [py_heap_mb=<..>] [orders=<..> ...]

rss/hwm/threads come from /proc/self/status (Linux; the line is simply skipped elsewhere). Set MEM_PROBE_TRACEMALLOC=true
to also report the Python heap (tracemalloc is started on first use; it slows the process, so use it for a diagnosis
run, not permanently). No order, customer or database content is ever logged: only the stage name, the brand index and
the counters the caller passes.

Use it to compare VmHWM before and after a backfill:  docker logs aws-pipeline | grep MEM_PROBE
"""

import os
from typing import Dict, Optional

from pipeline.state import logger

_STATUS_PATH = "/proc/self/status"
_FIELDS = {"VmRSS": "rss_mb", "VmHWM": "hwm_mb"}


def enabled() -> bool:
    return os.environ.get("MEM_PROBE", "").strip().lower() == "true"


def read_proc_status(path: str = _STATUS_PATH) -> Dict[str, float]:
    """{'rss_mb': .., 'hwm_mb': .., 'threads': ..} from a /proc/<pid>/status file; {} when it cannot be read."""
    out: Dict[str, float] = {}
    try:
        with open(path, encoding="ascii", errors="ignore") as handle:
            for line in handle:
                key, _, rest = line.partition(":")
                if key in _FIELDS:
                    out[_FIELDS[key]] = round(int(rest.split()[0]) / 1024.0, 1)  # kB -> MB
                elif key == "Threads":
                    out["threads"] = int(rest.split()[0])
    except (OSError, ValueError, IndexError):
        return {}
    return out


def _py_heap_mb() -> Optional[float]:
    if os.environ.get("MEM_PROBE_TRACEMALLOC", "").strip().lower() != "true":
        return None
    import tracemalloc

    if not tracemalloc.is_tracing():
        tracemalloc.start()
    return round(tracemalloc.get_traced_memory()[0] / 1048576.0, 1)


def format_probe(stage: str, brand_index, status: Dict[str, float], py_heap_mb, counters: Dict[str, object]) -> str:
    parts = [f"stage={stage}", f"brand_index={brand_index}"]
    parts += [f"{name}={status[name]}" for name in ("rss_mb", "hwm_mb", "threads") if name in status]
    if py_heap_mb is not None:
        parts.append(f"py_heap_mb={py_heap_mb}")
    parts += [f"{key}={value}" for key, value in counters.items() if isinstance(value, (int, float))]
    return "MEM_PROBE " + " ".join(parts)


def log_mem(stage: str, brand_index=None, **counters) -> None:
    """No-op unless MEM_PROBE=true. Never raises."""
    if not enabled():
        return
    try:
        status = read_proc_status()
        if not status:
            return
        logger.info(format_probe(stage, brand_index, status, _py_heap_mb(), counters))
    except Exception:  # a probe must never affect the pipeline
        pass
