import logging
import unittest

from pipeline import state


class LastLogsHandlerTests(unittest.TestCase):
    def test_keeps_only_most_recent(self):
        h = state._LastLogsHandler(maxlen=2)
        for i in range(3):
            h.emit(logging.LogRecord("n", logging.INFO, "f", 1, f"m{i}", None, None))
        self.assertEqual(h.get_recent(), ["m1", "m2"])


class SharedStateTests(unittest.TestCase):
    def test_singletons_exist(self):
        self.assertEqual(str(state.IST), "Asia/Kolkata")
        self.assertEqual(state.app.name, "aws_background")
        self.assertIsNotNone(state.scheduler)
        self.assertTrue(state.MANUAL_TRIGGER_QUEUE.empty())


if __name__ == "__main__":
    unittest.main()
