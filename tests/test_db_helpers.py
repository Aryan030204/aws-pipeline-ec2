import unittest
from unittest.mock import MagicMock

from pipeline import db


class ExecutemanyChunkedTests(unittest.TestCase):
    def test_empty_rows_is_noop(self):
        cur, conn = MagicMock(), MagicMock()
        self.assertEqual(db.executemany_chunked(cur, conn, "SQL", []), 0)
        cur.executemany.assert_not_called()
        conn.commit.assert_not_called()

    def test_chunks_and_commits_between(self):
        cur, conn = MagicMock(), MagicMock()
        rows = [(i,) for i in range(5)]
        self.assertEqual(db.executemany_chunked(cur, conn, "SQL", rows, chunk_size=2), 5)
        self.assertEqual([len(c.args[1]) for c in cur.executemany.call_args_list], [2, 2, 1])
        self.assertEqual(conn.commit.call_count, 3)

    def test_single_commit_when_not_committing_between(self):
        cur, conn = MagicMock(), MagicMock()
        db.executemany_chunked(cur, conn, "SQL", [(1,), (2,), (3,)], chunk_size=1, commit_between_chunks=False)
        self.assertEqual(cur.executemany.call_count, 3)
        self.assertEqual(conn.commit.call_count, 1)


class MiscTests(unittest.TestCase):
    def test_ssl_error_detection(self):
        self.assertTrue(db._is_ssl_connection_error(Exception("SSL handshake failed")))
        self.assertTrue(db._is_ssl_connection_error(Exception("certificate verify failed")))
        self.assertFalse(db._is_ssl_connection_error(Exception("access denied")))

    def test_config_without_ssl_strips_keys_and_does_not_mutate(self):
        cfg = {"host": "h", "ssl_ca": "x", "ssl_verify_cert": True, "ssl_verify_identity": True}
        clean = db._connector_config_without_ssl(cfg)
        self.assertEqual(clean, {"host": "h"})
        self.assertIn("ssl_ca", cfg)

    def test_timed_logs_and_propagates(self):
        with self.assertRaises(ValueError):
            with db.timed("x"):
                raise ValueError()


if __name__ == "__main__":
    unittest.main()
