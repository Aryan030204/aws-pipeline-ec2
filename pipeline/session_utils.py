"""Tiny, dependency-free MySQL session helpers (importable without the pool/env setup in pipeline.db)."""

from contextlib import contextmanager


@contextmanager
def autocommit_session(cursor):
    """Run the block with ``SET SESSION autocommit=1`` and ALWAYS restore autocommit=0 afterwards.

    Connections are pooled and reused (``BrandConnectionPool.release`` only rolls back), so a session flag set
    without being reset leaks into every later user of the connection: their writes each become their own
    redo-fsync commit. With autocommit on, a read-only statement also ends its snapshot immediately instead of
    keeping one InnoDB read view (and the undo history behind it) open until an explicit commit."""
    cursor.execute("SET SESSION autocommit=1")
    try:
        yield cursor
    finally:
        try:
            cursor.execute("SET SESSION autocommit=0")
        except Exception:
            # the connection is broken; the pool discards broken connections, nothing else to do
            pass
