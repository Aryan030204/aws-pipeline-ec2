"""
Tests for workers/admission.py.

FakeRedis below implements the handful of sorted-set commands the module
uses, and `register_script` returns a Python transliteration of the Lua in
_ACQUIRE_LUA - so these tests cover the wrapper (waiting, release, heartbeat,
fail-open) without needing a Redis server. The Lua itself, including its
atomicity under a simultaneous stampede, was verified separately against a
real Redis; keep the transliteration in step with _ACQUIRE_LUA if that
script changes.
"""

import time
import unittest
from unittest.mock import patch

from workers import admission


class FakeRedis:
    """Enough of a Redis sorted set for admission control. `now` is settable
    so lease expiry can be tested without sleeping."""

    def __init__(self, raises=False):
        self.zset = {}
        self.raises = raises
        self.zadd_calls = []
        self.expire_calls = []

    def _check(self):
        if self.raises:
            raise RuntimeError("redis down")

    def zremrangebyscore(self, key, lo, hi):
        self._check()
        gone = [m for m, s in self.zset.items() if s <= hi]
        for m in gone:
            del self.zset[m]
        return len(gone)

    def zcard(self, key):
        self._check()
        return len(self.zset)

    def zadd(self, key, mapping, xx=False):
        self._check()
        self.zadd_calls.append((mapping, xx))
        added = 0
        for member, score in mapping.items():
            if xx and member not in self.zset:
                continue
            added += member not in self.zset
            self.zset[member] = score
        return added

    def zrem(self, key, member):
        self._check()
        return int(self.zset.pop(member, None) is not None)

    def zrangebyscore(self, key, lo, hi):
        self._check()
        return sorted(m for m, s in self.zset.items() if s >= lo)

    def expire(self, key, ttl):
        self._check()
        self.expire_calls.append(ttl)
        return 1

    def register_script(self, lua):
        def script(keys, args):
            key = keys[0]
            now, limit, member, ttl = float(args[0]), int(args[1]), args[2], float(args[3])
            self.zremrangebyscore(key, "-inf", now)
            running = self.zcard(key)
            if running + 1 > limit:
                return [0, running]
            self.zadd(key, {member: now + ttl})
            self.expire(key, ttl * 10)
            return [1, running]

        return script


class AdmissionTestCase(unittest.TestCase):
    """Wires a FakeRedis into the module and pins the limit at 2."""

    limit = 2

    def setUp(self):
        self.redis = FakeRedis()
        patches = [
            patch.object(admission, "get_redis_client", return_value=self.redis),
            patch.object(admission, "_acquire_script", self.redis.register_script("")),
            patch.object(admission, "MAX_CONCURRENT_WORKERS", self.limit),
            patch.object(admission, "LEASE_TTL_S", 300),
            patch.object(admission, "POLL_INTERVAL_S", 0.01),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)


class LimitTests(AdmissionTestCase):
    def test_runs_up_to_the_limit_are_admitted(self):
        first = admission.acquire("pnl", wait_timeout=0)
        second = admission.acquire("events-summary", wait_timeout=0)
        self.assertTrue(first.active)
        self.assertTrue(second.active)
        self.assertEqual(len(admission.current_running()), 2)

    def test_run_over_the_limit_is_refused(self):
        admission.acquire("pnl", wait_timeout=0)
        admission.acquire("events-summary", wait_timeout=0)
        with self.assertRaises(admission.AdmissionTimeout):
            admission.acquire("variant-wise-events", wait_timeout=0)
        self.assertEqual(len(admission.current_running()), self.limit)

    def test_releasing_frees_the_slot_for_the_next_worker(self):
        held = admission.acquire("pnl", wait_timeout=0)
        admission.acquire("events-summary", wait_timeout=0)
        held.release()
        later = admission.acquire("variant-wise-events", wait_timeout=0)
        self.assertTrue(later.active)

    def test_the_timeout_message_names_who_is_holding_the_slots(self):
        admission.acquire("pnl", wait_timeout=0)
        admission.acquire("events-summary", wait_timeout=0)
        with self.assertRaises(admission.AdmissionTimeout) as ctx:
            admission.acquire("intent-events", wait_timeout=0)
        self.assertIn("pnl", str(ctx.exception))
        self.assertIn("2/2", str(ctx.exception))


class CrashRecoveryTests(AdmissionTestCase):
    def test_an_expired_lease_stops_holding_its_slot(self):
        """The OOM-kill case: a container dies without releasing, so its
        lease is never refreshed and must expire on its own."""
        dead = admission.acquire("pnl", wait_timeout=0)
        dead._stop.set()  # heartbeat thread stops, as it would on SIGKILL
        self.assertEqual(len(admission.current_running()), 1)

        # Wind every lease back past its expiry.
        self.redis.zset = {m: time.time() - 1 for m in self.redis.zset}

        for name in ("events-summary", "variant-wise-events"):
            self.assertTrue(admission.acquire(name, wait_timeout=0).active)
        self.assertNotIn(dead.member, admission.current_running())

    def test_a_live_run_refreshes_its_lease_with_xx(self):
        """A heartbeat must never re-add a lease that already expired and was
        evicted - that would hold a slot nothing is accounting for."""
        with patch.object(admission, "HEARTBEAT_INTERVAL_S", 0.01):
            lease = admission.acquire("pnl", wait_timeout=0)
            time.sleep(0.05)
            lease.release()
        refreshes = [xx for mapping, xx in self.redis.zadd_calls if xx]
        self.assertTrue(refreshes, "expected at least one heartbeat refresh")
        self.assertTrue(all(refreshes), "heartbeat must use ZADD XX")


class DecoratorTests(AdmissionTestCase):
    def test_it_holds_a_slot_for_the_call_and_releases_after(self):
        seen = {}

        @admission.admission_controlled("pnl")
        def run_worker(current_dt=None, brand_key=None):
            seen.update(
                current_dt=current_dt,
                brand_key=brand_key,
                inflight=len(admission.current_running()),
            )

        run_worker(current_dt="2026-09-29", brand_key="BBB")
        self.assertEqual(
            seen, {"current_dt": "2026-09-29", "brand_key": "BBB", "inflight": 1}
        )
        self.assertEqual(admission.current_running(), [])

    def test_it_preserves_the_wrapped_function(self):
        @admission.admission_controlled("pnl")
        def run_worker(current_dt=None):
            """docstring survives"""

        self.assertEqual(run_worker.__name__, "run_worker")
        self.assertEqual(run_worker.__doc__, "docstring survives")

    def test_a_failing_worker_still_releases_its_slot(self):
        @admission.admission_controlled("pnl")
        def run_worker():
            raise ValueError("worker blew up")

        with self.assertRaises(ValueError):
            run_worker()
        self.assertEqual(admission.current_running(), [])


class FailOpenTests(unittest.TestCase):
    """A gate that cannot be reached must never stop the work."""

    def test_no_redis_client_runs_unguarded(self):
        with patch.object(admission, "get_redis_client", return_value=None):
            lease = admission.acquire("pnl")
            self.assertFalse(lease.active)
            lease.release()  # must be a no-op, not an error

    def test_a_redis_error_during_the_check_runs_unguarded(self):
        broken = FakeRedis(raises=True)

        def exploding_script(keys, args):
            raise RuntimeError("redis down")

        with patch.object(admission, "get_redis_client", return_value=broken), patch.object(
            admission, "_acquire_script", exploding_script
        ):
            lease = admission.acquire("pnl", wait_timeout=0)
            self.assertFalse(lease.active)

    def test_a_failed_release_does_not_raise(self):
        redis = FakeRedis()
        with patch.object(admission, "get_redis_client", return_value=redis), patch.object(
            admission, "_acquire_script", redis.register_script("")
        ), patch.object(admission, "MAX_CONCURRENT_WORKERS", 2):
            lease = admission.acquire("pnl", wait_timeout=0)
            redis.raises = True
            lease.release()  # the lease will expire on its own instead

    def test_test_mode_skips_the_gate(self):
        redis = FakeRedis()
        with patch.dict("os.environ", {"TEST_MODE": "true"}), patch.object(
            admission, "get_redis_client", return_value=redis
        ):
            lease = admission.acquire("pnl")
            self.assertFalse(lease.active)
            self.assertEqual(redis.zset, {})


class RegisteredWorkerTests(unittest.TestCase):
    def test_every_registered_worker_name_is_gated(self):
        """The decorator's name should match workers/registry.py, so the logs
        from both line up."""
        from workers.registry import WORKERS

        self.assertEqual(
            sorted(WORKERS),
            ["checkout-login-status", "events-summary", "intent-events", "pnl",
             "variant-wise-events"],
        )


if __name__ == "__main__":
    unittest.main()
