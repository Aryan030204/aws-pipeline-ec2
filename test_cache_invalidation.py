import unittest
from datetime import date
from unittest.mock import MagicMock, patch

from pipeline import cache_invalidation


class FakeRedis:
    """Records what was deleted; `delete` returns how many of the requested
    keys existed, like the real client."""

    def __init__(self, existing=(), raises=False):
        self.existing = set(existing)
        self.raises = raises
        self.calls = []

    def delete(self, *keys):
        self.calls.append(keys)
        if self.raises:
            raise RuntimeError("redis down")
        hit = [k for k in keys if k in self.existing]
        self.existing.difference_update(hit)
        return len(hit)


class BuildCacheKeysTests(unittest.TestCase):
    def test_keys_match_the_dashboard_key_shapes(self):
        keys = cache_invalidation.build_cache_keys("PTS", ["2026-09-26", "2026-09-27"])
        self.assertEqual(
            keys,
            [
                "metrics:pts:2026-09-26",
                "hourly_metrics:pts:2026-09-26",
                "metrics:pts:2026-09-27",
                "hourly_metrics:pts:2026-09-27",
                "date_presets:pts",
            ],
        )

    def test_date_objects_and_strings_are_interchangeable(self):
        self.assertEqual(
            cache_invalidation.build_cache_keys("pts", [date(2026, 9, 27)]),
            cache_invalidation.build_cache_keys("PTS", ["2026-09-27"]),
        )

    def test_duplicate_and_blank_dates_are_dropped_and_order_is_stable(self):
        keys = cache_invalidation.build_cache_keys(
            "BBB", ["2026-09-27", "2026-09-26", "2026-09-27", "", None]
        )
        self.assertEqual(
            keys,
            [
                "metrics:bbb:2026-09-26",
                "hourly_metrics:bbb:2026-09-26",
                "metrics:bbb:2026-09-27",
                "hourly_metrics:bbb:2026-09-27",
                "date_presets:bbb",
            ],
        )

    def test_no_keys_without_a_brand_or_without_dates(self):
        self.assertEqual(cache_invalidation.build_cache_keys("", ["2026-09-27"]), [])
        self.assertEqual(cache_invalidation.build_cache_keys("PTS", []), [])


class InvalidateDatesTests(unittest.TestCase):
    def setUp(self):
        self.aws_background = MagicMock()
        self.aws_background.TEST_MODE = False
        patcher = patch.dict("sys.modules", {"aws_background": self.aws_background})
        patcher.start()
        self.addCleanup(patcher.stop)

    def _invalidate(self, client, *args, **kwargs):
        with patch.object(cache_invalidation, "get_redis_client", return_value=client):
            return cache_invalidation.invalidate_dates(*args, **kwargs)

    def test_deletes_every_key_for_the_dates_and_counts_the_hits(self):
        fake = FakeRedis(existing=["metrics:pts:2026-09-27", "date_presets:pts"])
        removed = self._invalidate(fake, "PTS", ["2026-09-27"])
        self.assertEqual(removed, 2)
        self.assertEqual(
            fake.calls,
            [("metrics:pts:2026-09-27", "hourly_metrics:pts:2026-09-27", "date_presets:pts")],
        )

    def test_deletes_are_chunked(self):
        dates = [date(2026, 9, 1) + __import__("datetime").timedelta(days=i) for i in range(200)]
        fake = FakeRedis()
        self._invalidate(fake, "PTS", dates)
        total_keys = 200 * 2 + 1
        self.assertEqual(sum(len(call) for call in fake.calls), total_keys)
        self.assertTrue(all(len(call) <= cache_invalidation.DELETE_BATCH_SIZE for call in fake.calls))

    def test_redis_failure_is_swallowed(self):
        fake = FakeRedis(raises=True)
        self.assertEqual(self._invalidate(fake, "PTS", ["2026-09-27"]), 0)

    def test_no_redis_client_is_a_no_op(self):
        self.assertEqual(self._invalidate(None, "PTS", ["2026-09-27"]), 0)

    def test_test_mode_skips_invalidation(self):
        self.aws_background.TEST_MODE = True
        fake = FakeRedis()
        self.assertEqual(self._invalidate(fake, "PTS", ["2026-09-27"]), 0)
        self.assertEqual(fake.calls, [])

    def test_nothing_to_invalidate_never_touches_redis(self):
        fake = FakeRedis()
        self.assertEqual(self._invalidate(fake, "PTS", []), 0)
        self.assertEqual(fake.calls, [])


if __name__ == "__main__":
    unittest.main()
