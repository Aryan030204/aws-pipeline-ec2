"""
Tests for pipeline/sql_migrations.py - the runner that applies migrations/*.sql
once per brand database at container start.

The suite has two halves:

  * Unit tests for the statement splitter, migration discovery and the ledger
    skip/drift logic. These need no database.
  * A safety lint over the real files in migrations/ (TestMigrationsAreSafe).
    That half is the regression guard for the property that makes running
    migrations unattended, per brand, on every boot acceptable in the first
    place: every automatic migration must be individually re-runnable. It
    fails the build on a newly added file that is not, which is cheaper than
    finding out from a crash-looping container.
"""

import os
import re
import unittest
from pathlib import Path
from unittest import mock

from pipeline.sql_migrations import (
    LEDGER_TABLE,
    MIGRATIONS_DIR,
    Migration,
    MigrationError,
    discover_migrations,
    split_sql_statements,
)


class TestSplitSqlStatements(unittest.TestCase):
    def test_splits_on_semicolons(self):
        self.assertEqual(
            split_sql_statements("SELECT 1; SELECT 2;"), ["SELECT 1", "SELECT 2"]
        )

    def test_ignores_trailing_empty_statement(self):
        self.assertEqual(split_sql_statements("SELECT 1;\n\n  \n"), ["SELECT 1"])

    def test_line_comment_is_dropped_and_cannot_split(self):
        # A semicolon inside a -- comment must not terminate the statement.
        stmts = split_sql_statements("SELECT 1 -- a; comment\n  + 1;")
        self.assertEqual(len(stmts), 1)
        self.assertNotIn("comment", stmts[0])
        self.assertIn("+ 1", stmts[0])

    def test_hash_comment_is_dropped(self):
        self.assertEqual(split_sql_statements("# nope;\nSELECT 1;"), ["SELECT 1"])

    def test_double_dash_needs_whitespace_to_be_a_comment(self):
        # `--` glued to a token is an operator, not a comment introducer.
        (stmt,) = split_sql_statements("SELECT 1--2;")
        self.assertIn("--2", stmt.replace(" ", ""))

    def test_block_comment_is_dropped_but_executable_comment_is_kept(self):
        self.assertEqual(split_sql_statements("/* x; y */ SELECT 1;"), ["SELECT 1"])
        (stmt,) = split_sql_statements("SELECT /*!40101 1 */;")
        self.assertIn("/*!40101", stmt)

    def test_semicolon_inside_string_literal_does_not_split(self):
        (stmt,) = split_sql_statements("SELECT 'a;b';")
        self.assertEqual(stmt, "SELECT 'a;b'")

    def test_semicolon_inside_backtick_identifier_does_not_split(self):
        (stmt,) = split_sql_statements("SELECT `we;ird`;")
        self.assertEqual(stmt, "SELECT `we;ird`")

    def test_escaped_and_doubled_quotes(self):
        (stmt,) = split_sql_statements(r"SELECT 'it\'s; fine';")
        self.assertEqual(stmt, r"SELECT 'it\'s; fine'")
        (stmt,) = split_sql_statements("SELECT 'it''s; fine';")
        self.assertEqual(stmt, "SELECT 'it''s; fine'")

    def test_delimiter_directive_keeps_procedure_body_whole(self):
        sql = """
DROP PROCEDURE IF EXISTS p;
DELIMITER $$
CREATE PROCEDURE p()
BEGIN
    SELECT 1;
    SELECT 2;
END$$
DELIMITER ;
CALL p();
"""
        stmts = split_sql_statements(sql)
        self.assertEqual(len(stmts), 3, stmts)
        self.assertEqual(stmts[0], "DROP PROCEDURE IF EXISTS p")
        self.assertTrue(stmts[1].startswith("CREATE PROCEDURE p()"))
        # Both inner statements must survive inside the one CREATE PROCEDURE.
        self.assertIn("SELECT 1;", stmts[1])
        self.assertIn("SELECT 2;", stmts[1])
        self.assertEqual(stmts[2], "CALL p()")

    def test_delimiter_never_leaks_into_a_statement(self):
        for stmt in split_sql_statements("DELIMITER $$\nSELECT 1$$\nDELIMITER ;\n"):
            self.assertFalse(stmt.upper().startswith("DELIMITER"), stmt)


class TestDiscoverMigrations(unittest.TestCase):
    def _write(self, tmp, name, body=""):
        (tmp / name).write_text(body or "SELECT 1;\n", encoding="utf-8")

    def test_orders_numerically_not_lexically(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            for name in ("010_ten.sql", "002_two.sql", "001_one.sql"):
                self._write(tmp, name)
            versions = [m.version for m in discover_migrations(tmp)]
            self.assertEqual(versions, ["001", "002", "010"])

    def test_detects_manual_marker(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            self._write(tmp, "001_auto.sql", "SELECT 1;\n")
            self._write(tmp, "002_manual.sql", "-- migrate: manual\nSELECT 1;\n")
            self._write(tmp, "003_spaced.sql", "--   MIGRATE:  MANUAL  \nSELECT 1;\n")
            by_version = {m.version: m for m in discover_migrations(tmp)}
            self.assertFalse(by_version["001"].manual)
            self.assertTrue(by_version["002"].manual)
            self.assertTrue(by_version["003"].manual, "marker should be case/space tolerant")

    def test_marker_must_be_its_own_comment_line(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            # Prose that merely mentions the words is not the marker.
            self._write(tmp, "001_x.sql", "-- you can migrate: manual steps later\nSELECT 1;\n")
            self.assertFalse(discover_migrations(tmp)[0].manual)

    def test_rejects_duplicate_versions(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            self._write(tmp, "001_a.sql")
            self._write(tmp, "001_b.sql")
            with self.assertRaises(MigrationError) as ctx:
                discover_migrations(tmp)
            self.assertIn("Duplicate migration version", str(ctx.exception))

    def test_skips_files_without_a_version_prefix(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            self._write(tmp, "001_ok.sql")
            self._write(tmp, "notes.sql")
            self.assertEqual([m.filename for m in discover_migrations(tmp)], ["001_ok.sql"])

    def test_missing_directory_is_not_fatal(self):
        self.assertEqual(discover_migrations(Path("/nonexistent/migrations")), [])

    def test_checksum_tracks_content(self):
        a = Migration("001", "a.sql", Path("a.sql"), "SELECT 1;", False)
        b = Migration("001", "a.sql", Path("a.sql"), "SELECT 1;", False)
        c = Migration("001", "a.sql", Path("a.sql"), "SELECT 2;", False)
        self.assertEqual(a.checksum, b.checksum)
        self.assertNotEqual(a.checksum, c.checksum)


class TestRunMigrationsForBrand(unittest.TestCase):
    """The ledger decides what runs; these tests pin that decision down."""

    def setUp(self):
        self.cursor = mock.MagicMock()
        self.cursor.with_rows = False
        self.connection = mock.MagicMock()
        # _acquire_lock reads GET_LOCK's single column.
        self.cursor.fetchone.return_value = {"GET_LOCK": 1}
        self.cursor.nextset.return_value = None

        ctx = mock.MagicMock()
        ctx.__enter__ = mock.Mock(return_value=(self.cursor, self.connection))
        ctx.__exit__ = mock.Mock(return_value=False)
        patcher = mock.patch("pipeline.db.get_db_cursor", return_value=ctx)
        self.addCleanup(patcher.stop)
        patcher.start()

    def _run(self, migrations, ledger_rows=(), **kwargs):
        from pipeline import sql_migrations

        with mock.patch.object(sql_migrations, "read_ledger", return_value=dict(ledger_rows)), \
             mock.patch.object(sql_migrations, "ensure_ledger_table"), \
             mock.patch.object(sql_migrations, "record_applied") as recorded:
            result = sql_migrations.run_migrations_for_brand(
                1, migrations=list(migrations), **kwargs
            )
        return result, recorded

    @staticmethod
    def _m(version, manual=False, sql="SELECT 1;"):
        return Migration(version, f"{version}_x.sql", Path(f"{version}_x.sql"), sql, manual)

    def test_applies_pending_and_records_it(self):
        result, recorded = self._run([self._m("001")])
        self.assertEqual(result.applied, ["001"])
        self.assertEqual(result.skipped, [])
        self.assertTrue(result.ok)
        recorded.assert_called_once()

    def test_skips_a_version_already_in_the_ledger(self):
        m = self._m("001")
        ledger = {"001": {"checksum": m.checksum, "applied_at": "2026-01-01"}}
        result, recorded = self._run([m], ledger.items())
        self.assertEqual(result.applied, [])
        self.assertEqual(result.skipped, ["001"])
        recorded.assert_not_called()

    def test_manual_migration_is_never_applied(self):
        result, recorded = self._run([self._m("001", manual=True)])
        self.assertEqual(result.manual, ["001"])
        self.assertEqual(result.applied, [])
        recorded.assert_not_called()
        self.assertTrue(result.ok, "a skipped manual migration is not a failure")

    def test_dry_run_reports_without_recording(self):
        result, recorded = self._run([self._m("001")], dry_run=True)
        self.assertEqual(result.applied, ["001"])
        recorded.assert_not_called()
        # It still takes the lock, but runs none of the file's statements.
        executed = [c.args[0] for c in self.cursor.execute.call_args_list]
        self.assertTrue(all("_LOCK(" in q for q in executed), executed)

    def test_changed_file_warns_but_does_not_rerun(self):
        m = self._m("001", sql="SELECT 2;")
        ledger = {"001": {"checksum": "stale" * 16, "applied_at": "2026-01-01"}}
        result, recorded = self._run([m], ledger.items())
        self.assertEqual(result.drifted, ["001"])
        self.assertEqual(result.skipped, ["001"])
        self.assertEqual(result.applied, [])
        recorded.assert_not_called()
        self.assertTrue(result.ok, "drift is a warning by default, not a failure")

    def test_strict_checksums_turns_drift_into_an_error(self):
        m = self._m("001", sql="SELECT 2;")
        ledger = {"001": {"checksum": "stale" * 16, "applied_at": "2026-01-01"}}
        with self.assertRaises(MigrationError):
            self._run([m], ledger.items(), strict_checksums=True)

    def test_a_failing_migration_stops_later_ones(self):
        self.cursor.execute.side_effect = [None, RuntimeError("boom")]
        result, recorded = self._run([self._m("001"), self._m("002")])
        self.assertIsNotNone(result.failed)
        self.assertEqual(result.failed[0], "001")
        self.assertEqual(result.applied, [])
        self.assertFalse(result.ok)
        recorded.assert_not_called()

    def test_lock_contention_skips_the_brand_without_failing(self):
        self.cursor.fetchone.return_value = {"GET_LOCK": 0}
        result, recorded = self._run([self._m("001")])
        self.assertTrue(result.lock_timeout)
        self.assertFalse(result.ok)
        self.assertEqual(result.applied, [])
        recorded.assert_not_called()


class TestUnreachableBrandsAreReported(unittest.TestCase):
    """
    A brand that fails to initialize must be reported as a failure, not
    silently omitted.

    initialize_brand_configs() drops a brand whose startup connection test
    fails, leaving it out of active_brand_indices. If the runner only looked at
    that list it would migrate the brands it can see, exit 0, and let the
    compose gate start every worker against a database that never got its
    migrations - the exact failure the gate exists to prevent.
    """

    def setUp(self):
        from pipeline import state

        for attr, value in (
            ("active_brand_indices", [1]),
            ("brand_tag_to_index_map", {"one": 1, "two": 2}),
        ):
            patcher = mock.patch.object(state, attr, value)
            self.addCleanup(patcher.stop)
            patcher.start()

    def test_expected_but_missing_brand_is_flagged(self):
        from pipeline.sql_migrations import _unreachable_brands

        self.assertEqual(_unreachable_brands(), [2])

    def test_no_false_positives_when_every_brand_initialized(self):
        from pipeline import state
        from pipeline.sql_migrations import _unreachable_brands

        with mock.patch.object(state, "active_brand_indices", [1, 2]):
            self.assertEqual(_unreachable_brands(), [])

    def test_run_all_brands_marks_the_unreachable_one_failed(self):
        from pipeline import sql_migrations

        migration = Migration("001", "001_x.sql", Path("001_x.sql"), "SELECT 1;", False)
        ok = mock.Mock(return_value=mock.Mock(ok=True, applied=["001"], skipped=[],
                                              manual=[], drifted=[], failed=None,
                                              lock_timeout=False, error=None,
                                              brand_name="BrandOne"))
        with mock.patch.object(sql_migrations, "discover_migrations", return_value=[migration]), \
             mock.patch.object(sql_migrations, "run_migrations_for_brand", ok):
            results = sql_migrations.run_migrations_for_all_brands()

        self.assertEqual(sorted(results), [1, 2])
        self.assertTrue(results[1].ok, "the reachable brand should still succeed")
        self.assertFalse(results[2].ok, "the unreachable brand must be a failure")
        self.assertIn("not migrated", results[2].error)
        ok.assert_called_once()  # only the reachable brand is actually migrated

    def test_explicit_brand_list_does_not_flag_other_brands(self):
        from pipeline import sql_migrations

        migration = Migration("001", "001_x.sql", Path("001_x.sql"), "SELECT 1;", False)
        ok = mock.Mock(return_value=mock.Mock(ok=True, applied=[], skipped=["001"],
                                              manual=[], drifted=[], failed=None,
                                              lock_timeout=False, error=None,
                                              brand_name="BrandOne"))
        with mock.patch.object(sql_migrations, "discover_migrations", return_value=[migration]), \
             mock.patch.object(sql_migrations, "run_migrations_for_brand", ok):
            results = sql_migrations.run_migrations_for_all_brands(brand_indices=[1])

        self.assertEqual(list(results), [1], "asking for brand 1 must not report brand 2")


class TestMigrationsAreSafe(unittest.TestCase):
    """
    The safety contract for migrations/*.sql.

    A migration runs unattended against every brand database on every container
    start, so it must be individually re-runnable: applying it twice has to be
    the same as applying it once, and it must not fail on a brand whose schema
    is behind. A file that cannot meet that bar carries `-- migrate: manual` in
    its header and is skipped by the runner (001_rds_perf_pass.sql is the one
    such file - it has bare DROP INDEX / ADD PRIMARY KEY statements and wants a
    maintenance window).

    If one of these fails on a migration you just added, the fix is to guard the
    statement, not to relax the test: wrap the DDL in an information_schema
    check the way 002/003/005 do, or mark the file manual.
    """

    @classmethod
    def setUpClass(cls):
        cls.migrations = discover_migrations()
        cls.automatic = [m for m in cls.migrations if not m.manual]

    def test_there_are_migrations_to_check(self):
        self.assertGreater(len(self.migrations), 0, f"no .sql files in {MIGRATIONS_DIR}")
        self.assertGreater(len(self.automatic), 0, "every migration is marked manual")

    def test_create_table_always_uses_if_not_exists(self):
        for m in self.automatic:
            for stmt in m.statements:
                flat = " ".join(stmt.split())
                if re.match(r"^CREATE\s+TABLE\b", flat, re.IGNORECASE):
                    self.assertRegex(
                        flat,
                        r"^CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\b",
                        f"{m.filename}: CREATE TABLE without IF NOT EXISTS would fail "
                        f"on re-run: {flat[:120]}",
                    )

    def test_drop_always_uses_if_exists(self):
        for m in self.automatic:
            for stmt in m.statements:
                flat = " ".join(stmt.split())
                if re.match(r"^DROP\s+(TABLE|PROCEDURE|FUNCTION|VIEW|TRIGGER|INDEX)\b",
                            flat, re.IGNORECASE):
                    self.assertRegex(
                        flat,
                        r"^DROP\s+\w+\s+IF\s+EXISTS\b",
                        f"{m.filename}: DROP without IF EXISTS would fail on re-run: "
                        f"{flat[:120]}",
                    )

    def test_no_bare_alter_table_at_the_top_level(self):
        """
        ALTER TABLE has no IF NOT EXISTS form in MySQL, so a top-level one is
        the single most common way a migration becomes non-re-runnable:
        ADD COLUMN fails with errno 1060, ADD PRIMARY KEY with 1068, DROP INDEX
        with 1091, and all of them on a table that does not exist with 1146.
        Every schema change in an automatic migration therefore goes through a
        procedure that checks information_schema first (see 002, 003 and 005).
        """
        for m in self.automatic:
            for stmt in m.statements:
                flat = " ".join(stmt.split())
                self.assertFalse(
                    re.match(r"^ALTER\s+TABLE\b", flat, re.IGNORECASE),
                    f"{m.filename}: top-level ALTER TABLE is not re-runnable - move it "
                    f"into an information_schema-guarded procedure, or mark the file "
                    f"`-- migrate: manual`: {flat[:120]}",
                )

    def test_every_procedure_checks_information_schema(self):
        """A guard procedure that never consults information_schema isn't one."""
        for m in self.automatic:
            for stmt in m.statements:
                flat = " ".join(stmt.split())
                if re.match(r"^CREATE\s+(DEFINER\s*=\s*\S+\s+)?PROCEDURE\b", flat, re.IGNORECASE):
                    self.assertIn(
                        "information_schema",
                        stmt.lower(),
                        f"{m.filename}: procedure performs DDL without an "
                        f"information_schema guard: {flat[:120]}",
                    )

    def test_no_destructive_statements(self):
        forbidden = (
            r"^DROP\s+DATABASE\b",
            r"^DROP\s+SCHEMA\b",
            r"^TRUNCATE\b",
            r"^DELETE\s+FROM\b",
            r"^UPDATE\b",
            r"^RENAME\s+TABLE\b",
        )
        for m in self.automatic:
            for stmt in m.statements:
                flat = " ".join(stmt.split())
                for pattern in forbidden:
                    self.assertFalse(
                        re.match(pattern, flat, re.IGNORECASE),
                        f"{m.filename}: destructive statement in an automatically applied "
                        f"migration - it would run against every brand database on every "
                        f"boot: {flat[:120]}",
                    )

    def test_procedures_are_dropped_before_and_after_use(self):
        """
        A leftover procedure from a half-finished run makes CREATE PROCEDURE
        fail with errno 1304 on the next attempt, so every procedure a file
        creates must be dropped before it is created.
        """
        for m in self.automatic:
            created, dropped = set(), set()
            for stmt in m.statements:
                flat = " ".join(stmt.split())
                made = re.match(r"^CREATE\s+PROCEDURE\s+`?(\w+)`?", flat, re.IGNORECASE)
                if made:
                    self.assertIn(
                        made.group(1),
                        dropped,
                        f"{m.filename}: CREATE PROCEDURE {made.group(1)} is not preceded "
                        f"by DROP PROCEDURE IF EXISTS {made.group(1)}",
                    )
                    created.add(made.group(1))
                gone = re.match(r"^DROP\s+PROCEDURE\s+IF\s+EXISTS\s+`?(\w+)`?", flat, re.IGNORECASE)
                if gone:
                    dropped.add(gone.group(1))
            for name in created:
                self.assertGreaterEqual(
                    sum(
                        1
                        for s in m.statements
                        if re.match(
                            rf"^DROP\s+PROCEDURE\s+IF\s+EXISTS\s+`?{name}`?\s*$",
                            " ".join(s.split()),
                            re.IGNORECASE,
                        )
                    ),
                    2,
                    f"{m.filename}: procedure {name} should be dropped both before "
                    f"creation and after use, so it does not linger in the brand database",
                )

    def test_the_known_unsafe_migration_stays_marked_manual(self):
        by_name = {m.filename: m for m in self.migrations}
        target = "001_rds_perf_pass.sql"
        if target in by_name:
            self.assertTrue(
                by_name[target].manual,
                f"{target} has bare DROP INDEX and ADD PRIMARY KEY statements and is "
                f"superseded by 002_converge_to_canonical.sql; it must keep its "
                f"`-- migrate: manual` marker",
            )

    def test_ledger_table_name_is_not_used_by_a_migration(self):
        """The runner owns `schema_migrations`; a migration must not touch it."""
        for m in self.migrations:
            self.assertNotIn(
                LEDGER_TABLE,
                m.sql.lower(),
                f"{m.filename} references the runner's own ledger table {LEDGER_TABLE}",
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
