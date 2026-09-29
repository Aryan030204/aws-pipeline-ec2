"""
Redis-backed admission control for the standalone workers.

WHY: every worker runs in its own container on a single EC2 box, and several
of them wake at the same moment (events-summary and variant-wise both at
18:30 UTC, with the main pipeline's :30 slot and intent-events' 15-minute
interval landing near it). Concurrent runs spike memory, the host swaps onto
its EBS root volume, and the random I/O that follows drags the whole instance
down - slow disk first, unresponsive box after. This caps how many worker
runs may be in flight at once, ACROSS containers, so that spike can't form.

HOW: one Redis sorted set (`workers:running`) holds a member per in-flight
run, scored with the epoch second its lease expires. Admission is a single
Lua script - atomic, so two workers starting in the same second cannot both
see a free slot: evict members whose lease has expired, count what is left,
and add ourselves only if `running + 1 <= MAX_CONCURRENT_WORKERS`.

WHY LEASES, NOT INCR/DECR: the failure this exists to survive is the OOM
killer. A decrement that never runs would leak the counter upward until no
worker could ever start again - the gate would become the outage. An
expiring lease is self-healing: a killed container's slot frees itself
LEASE_TTL_S later with nothing to clean up by hand. A heartbeat thread
pushes the lease forward while the run is alive, so a slow run keeps its
slot without the TTL having to exceed the slowest worker's runtime.

FAILS OPEN, like pipeline/cache_invalidation.py: if REDIS_URL is unset, the
redis package is missing, or Redis is unreachable, the run proceeds
unguarded rather than not at all - a gate that cannot be reached must not
stop work that is otherwise fine. The one case that does stop a run is
waiting past WORKER_ADMISSION_WAIT_S for a slot: that means something is
genuinely stuck, and failing loudly beats piling onto a host that is already
struggling.

Deliberately dependency-light (stdlib + redis only) so it can be imported by
workers/events_summary_worker.py, which is standalone and never imports
aws_background or anything under pipeline/.
"""

import functools
import logging
import os
import socket
import threading
import time
import uuid
from contextlib import contextmanager
from typing import List, Optional

try:
    import redis
except ImportError:  # pragma: no cover - dependency is in requirements.txt
    redis = None

logger = logging.getLogger(__name__)

REDIS_URL = os.environ.get("REDIS_URL")
SOCKET_TIMEOUT_S = float(os.environ.get("REDIS_SOCKET_TIMEOUT_S", "5"))

# The sorted set every worker container coordinates through.
RUNNING_KEY = os.environ.get("WORKER_ADMISSION_KEY", "workers:running")

# How many worker runs may overlap. 2 leaves headroom for the main pipeline,
# which is the heaviest process on the box (MAX_CONCURRENT_BRANDS threads).
MAX_CONCURRENT_WORKERS = int(os.environ.get("MAX_CONCURRENT_WORKERS", "2"))

# How long a slot stays held without a heartbeat. Short enough that an
# OOM-killed container frees its slot promptly, long enough to survive a
# GC pause or a slow Redis round trip.
LEASE_TTL_S = int(os.environ.get("WORKER_LEASE_TTL_S", "300"))

# Refresh well inside the TTL so one missed beat is not fatal.
HEARTBEAT_INTERVAL_S = max(1, LEASE_TTL_S // 3)

# How long to wait for a slot before giving up. Generous: a daily rollup
# would rather start late than not at all.
WAIT_TIMEOUT_S = int(os.environ.get("WORKER_ADMISSION_WAIT_S", "1800"))
POLL_INTERVAL_S = float(os.environ.get("WORKER_ADMISSION_POLL_S", "10"))


class AdmissionTimeout(RuntimeError):
    """Raised when no slot became free within WORKER_ADMISSION_WAIT_S."""


# Evict expired leases, count, admit if there is room. Returns
# {admitted, running_before} so the caller can log what it was queued behind.
_ACQUIRE_LUA = """
local key    = KEYS[1]
local now    = tonumber(ARGV[1])
local limit  = tonumber(ARGV[2])
local member = ARGV[3]
local ttl    = tonumber(ARGV[4])

redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
local running = redis.call('ZCARD', key)
if running + 1 > limit then
  return {0, running}
end
redis.call('ZADD', key, now + ttl, member)
-- Housekeeping only: the set is rebuilt constantly, this stops an abandoned
-- key lingering forever if every worker stops for good.
redis.call('EXPIRE', key, ttl * 10)
return {1, running}
"""

_client = None
_client_initialized = False
_acquire_script = None


def _test_mode() -> bool:
    # Same expression as aws_background.TEST_MODE, read from the environment
    # directly so this module stays importable by the standalone workers.
    return os.environ.get("TEST_MODE", "false").strip().lower() == "true"


def get_redis_client():
    """Lazily builds the shared client. Returns None (and logs once) when
    Redis isn't configured or the dependency is missing, which disables
    admission control without disabling the workers."""
    global _client, _client_initialized, _acquire_script

    if _client_initialized:
        return _client

    _client_initialized = True

    if redis is None:
        logger.warning("Worker admission control disabled: redis package not installed.")
        return None

    if not REDIS_URL:
        logger.info("Worker admission control disabled: REDIS_URL not set.")
        return None

    try:
        _client = redis.Redis.from_url(
            REDIS_URL,
            socket_timeout=SOCKET_TIMEOUT_S,
            socket_connect_timeout=SOCKET_TIMEOUT_S,
            decode_responses=True,
        )
        _acquire_script = _client.register_script(_ACQUIRE_LUA)
        logger.info(
            "✅ Redis client initialized for worker admission control "
            "(max %s concurrent, %ss lease)",
            MAX_CONCURRENT_WORKERS,
            LEASE_TTL_S,
        )
    except Exception as e:
        logger.error("❌ Failed to initialize Redis client for admission control: %s", e)
        _client = None

    return _client


class Lease:
    """A held slot. `active` is False for the fail-open case, where nothing
    was reserved and release is a no-op."""

    def __init__(self, worker: str, member: Optional[str] = None):
        self.worker = worker
        self.member = member
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @property
    def active(self) -> bool:
        return self.member is not None

    def start_heartbeat(self) -> None:
        if not self.active:
            return
        self._thread = threading.Thread(
            target=self._beat, name=f"admission-heartbeat-{self.worker}", daemon=True
        )
        self._thread.start()

    def _beat(self) -> None:
        client = get_redis_client()
        while not self._stop.wait(HEARTBEAT_INTERVAL_S):
            if client is None:
                return
            try:
                # XX: only refresh a member that is still there. If our lease
                # already expired and was evicted, re-adding it would hold a
                # slot nobody is accounting for.
                client.zadd(RUNNING_KEY, {self.member: time.time() + LEASE_TTL_S}, xx=True)
            except Exception as e:
                logger.warning("Admission heartbeat failed for %s: %s", self.worker, e)

    def release(self) -> None:
        self._stop.set()
        if not self.active:
            return
        client = get_redis_client()
        if client is None:
            return
        try:
            client.zrem(RUNNING_KEY, self.member)
        except Exception as e:
            # Not fatal: the lease expires on its own LEASE_TTL_S from now.
            logger.warning(
                "Could not release admission slot for %s (it will expire in %ss): %s",
                self.worker,
                LEASE_TTL_S,
                e,
            )


def current_running() -> List[str]:
    """Members currently holding a slot, expired ones excluded. Observability
    helper - never raises."""
    client = get_redis_client()
    if client is None:
        return []
    try:
        return client.zrangebyscore(RUNNING_KEY, time.time(), "+inf")
    except Exception as e:
        logger.warning("Could not read running workers: %s", e)
        return []


def _new_member(worker: str) -> str:
    return f"{worker}:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


def acquire(worker: str, wait_timeout: Optional[float] = None) -> Lease:
    """Blocks until a slot is free, then returns the held Lease.

    Returns an inactive Lease (i.e. runs unguarded) when admission control is
    switched off or Redis is unreachable. Raises AdmissionTimeout if no slot
    frees up in time."""
    if _test_mode():
        logger.info("Skipping worker admission control (TEST_MODE)")
        return Lease(worker)

    client = get_redis_client()
    if client is None:
        return Lease(worker)

    timeout = WAIT_TIMEOUT_S if wait_timeout is None else wait_timeout
    member = _new_member(worker)
    deadline = time.time() + timeout
    waited_since: Optional[float] = None

    while True:
        try:
            admitted, running = _acquire_script(
                keys=[RUNNING_KEY],
                args=[time.time(), MAX_CONCURRENT_WORKERS, member, LEASE_TTL_S],
            )
        except Exception as e:
            # Fail open: a gate we cannot reach must not stop the work.
            logger.error(
                "Admission check failed for %s, running unguarded: %s", worker, e
            )
            return Lease(worker)

        if int(admitted) == 1:
            if waited_since is not None:
                logger.info(
                    "▶ %s admitted after waiting %.0fs (%s slot(s) were busy)",
                    worker,
                    time.time() - waited_since,
                    int(running),
                )
            lease = Lease(worker, member)
            lease.start_heartbeat()
            return lease

        if waited_since is None:
            waited_since = time.time()
            logger.info(
                "⏳ %s waiting for a slot: %s/%s in flight (%s)",
                worker,
                int(running),
                MAX_CONCURRENT_WORKERS,
                ", ".join(current_running()) or "unknown",
            )

        if time.time() >= deadline:
            raise AdmissionTimeout(
                f"{worker} waited {timeout:.0f}s for an admission slot and gave up; "
                f"{int(running)}/{MAX_CONCURRENT_WORKERS} still in flight "
                f"({', '.join(current_running()) or 'unknown'})"
            )

        time.sleep(POLL_INTERVAL_S)


@contextmanager
def slot(worker: str, wait_timeout: Optional[float] = None):
    """Holds an admission slot for the duration of the block."""
    lease = acquire(worker, wait_timeout=wait_timeout)
    try:
        yield lease
    finally:
        lease.release()


def admission_controlled(worker: str):
    """Gates a worker's run_worker() on a free admission slot.

    Applied to run_worker itself rather than to each _run_scheduled(), so it
    covers every path a worker can start from: its own scheduler, the
    `python -m workers.run <name>` CLI, and the POST /workers/<name>/trigger
    HTTP knob - all three end up calling run_worker.

    `worker` should match the name in workers/registry.py, so the logs here
    line up with the ones there."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with slot(worker):
                return func(*args, **kwargs)

        return wrapper

    return decorator
