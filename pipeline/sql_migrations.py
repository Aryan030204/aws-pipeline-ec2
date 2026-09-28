"""
Runner for the versioned SQL files in migrations/, applied once per brand
database and tracked in a `schema_migrations` ledger table in that same
database.

This is the file-based counterpart to pipeline/schema_migrations.py: that
module holds hand-written, always-re-run `ensure_*` DDL invoked inline by
the pipeline; this one applies migrations/NNN_*.sql exactly once per brand
and remembers that it did. Both are idempotent, but the ledger here means a
heavy migration (index churn on shopify_orders, a table rebuild) costs
nothing on the second and every later container start.

Ordering is by the numeric prefix of the filename, so 003 always lands
before 004, which lands before 005.

Safety model, in order of defence:

  1. The ledger. A version recorded in `schema_migrations` is never run
     again, so a file only has to survive one application, not thousands.
  2. A named MySQL advisory lock per brand database. Six containers booting
     at once (see docker-compose.yml) serialise instead of racing each
     other into "Duplicate key name" / "Duplicate column name" errors.
  3. The files themselves are written to be individually re-runnable
     (CREATE TABLE IF NOT EXISTS, information_schema-guarded ALTERs), which
     is what makes a crash mid-file recoverable: the ledger only records a
     file once every statement in it succeeded, so a partially applied file
     is simply re-applied on the next start.

A file whose header carries the marker

    -- migrate: manual

is listed but never applied automatically. That is the escape hatch for
migrations that need a maintenance window or a human reading pre-flight
output (migrations/001_rds_perf_pass.sql is one).

Run it standalone:

    python -m pipeline.sql_migrations            # every active brand
    python -m pipeline.sql_migrations --dry-run  # show what would run
    python -m pipeline.sql_migrations --status   # per-brand ledger state
    python -m pipeline.sql_migrations --brand 3  # one brand index
"""

import argparse
import hashlib
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

MIGRATIONS_DIR = PROJECT_ROOT / "migrations"

LEDGER_TABLE = "schema_migrations"

# One lock name for the whole project, held for the duration of one brand's
# run. MySQL advisory locks are server-wide, not schema-scoped, so the brand
# database name goes into the lock name - two brands on the same RDS instance
# must still be able to migrate in parallel.
LOCK_NAME_PREFIX = "aws_pipeline_schema_migrations"

MANUAL_MARKER = re.compile(r"^\s*--\s*migrate:\s*manual\s*$", re.IGNORECASE | re.MULTILINE)

logger = logging.getLogger("pipeline.sql_migrations")


class MigrationError(RuntimeError):
    """A migration statement failed; the file was not recorded as applied."""


@dataclass
class Migration:
    version: str
    filename: str
    path: Path
    sql: str
    manual: bool

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.sql.encode("utf-8")).hexdigest()

    @property
    def statements(self) -> List[str]:
        return split_sql_statements(self.sql)


@dataclass
class BrandResult:
    brand_index: int
    brand_name: str
    applied: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)
    manual: List[str] = field(default_factory=list)
    drifted: List[str] = field(default_factory=list)
    failed: Optional[Tuple[str, str]] = None  # (version, error)
    lock_timeout: bool = False
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.failed is None and self.error is None and not self.lock_timeout


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def split_sql_statements(sql: str) -> List[str]:
    """
    Split a .sql file into individually executable statements.

    Handles what the files in migrations/ actually use: `DELIMITER $$` blocks
    around CREATE PROCEDURE (a mysql-client directive the server itself does
    not understand, so it must be interpreted here and stripped), `--` and `#`
    line comments, /* */ block comments, and quoted literals or backtick
    identifiers that may contain a delimiter character.
    """
    statements: List[str] = []
    buf: List[str] = []
    delimiter = ";"
    i = 0
    n = len(sql)
    at_line_start = True

    def flush() -> None:
        stmt = "".join(buf).strip()
        # Strip a trailing delimiter and any stray semicolons/whitespace.
        if stmt:
            statements.append(stmt)
        buf.clear()

    while i < n:
        ch = sql[i]

        # DELIMITER directive: only meaningful at the start of a line.
        if at_line_start and sql[i : i + 9].upper() == "DELIMITER":
            eol = sql.find("\n", i)
            if eol == -1:
                eol = n
            new_delim = sql[i + 9 : eol].strip()
            if new_delim:
                flush()
                delimiter = new_delim
                i = eol + 1
                at_line_start = True
                continue

        # Line comments. Kept out of the statement text entirely so a `--`
        # comment containing a semicolon cannot split a statement.
        if sql[i : i + 2] == "--" and (i + 2 >= n or sql[i + 2] in " \t\r\n"):
            eol = sql.find("\n", i)
            i = n if eol == -1 else eol + 1
            at_line_start = True
            continue
        if ch == "#":
            eol = sql.find("\n", i)
            i = n if eol == -1 else eol + 1
            at_line_start = True
            continue

        # Block comment. MySQL executable comments (/*! ... */) are preserved;
        # plain ones are dropped.
        if sql[i : i + 2] == "/*":
            end = sql.find("*/", i + 2)
            end = n if end == -1 else end + 2
            if sql[i : i + 3] == "/*!":
                buf.append(sql[i:end])
            i = end
            at_line_start = False
            continue

        # Quoted literal / quoted identifier: copy verbatim, honouring
        # backslash escapes and doubled quotes.
        if ch in ("'", '"', "`"):
            quote = ch
            buf.append(ch)
            i += 1
            while i < n:
                c = sql[i]
                if c == "\\" and quote != "`" and i + 1 < n:
                    buf.append(sql[i : i + 2])
                    i += 2
                    continue
                buf.append(c)
                i += 1
                if c == quote:
                    if i < n and sql[i] == quote:  # doubled quote -> literal
                        buf.append(sql[i])
                        i += 1
                        continue
                    break
            at_line_start = False
            continue

        # Statement terminator.
        if sql.startswith(delimiter, i):
            i += len(delimiter)
            flush()
            at_line_start = False
            continue

        buf.append(ch)
        at_line_start = ch == "\n"
        i += 1

    flush()
    return statements


def discover_migrations(migrations_dir: Path = MIGRATIONS_DIR) -> List[Migration]:
    """Every migrations/NNN_*.sql, ordered by its numeric prefix."""
    if not migrations_dir.is_dir():
        logger.warning("No migrations directory at %s", migrations_dir)
        return []

    found: List[Tuple[int, str, Migration]] = []
    for path in sorted(migrations_dir.glob("*.sql")):
        m = re.match(r"^(\d+)", path.name)
        if not m:
            logger.warning(
                "Skipping %s: filename does not start with a version number", path.name
            )
            continue
        sql = path.read_text(encoding="utf-8")
        migration = Migration(
            version=str(int(m.group(1))).zfill(3),
            filename=path.name,
            path=path,
            sql=sql,
            manual=bool(MANUAL_MARKER.search(sql)),
        )
        found.append((int(m.group(1)), path.name, migration))

    found.sort(key=lambda t: (t[0], t[1]))

    seen: Dict[str, str] = {}
    for _, _, migration in found:
        if migration.version in seen:
            raise MigrationError(
                f"Duplicate migration version {migration.version}: "
                f"{seen[migration.version]} and {migration.filename}"
            )
        seen[migration.version] = migration.filename

    return [migration for _, _, migration in found]


# ---------------------------------------------------------------------------
# Ledger
# ---------------------------------------------------------------------------
def ensure_ledger_table(cursor, connection) -> None:
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{LEDGER_TABLE}` (
            `version`         VARCHAR(20)  NOT NULL,
            `filename`        VARCHAR(255) NOT NULL,
            `checksum`        CHAR(64)     NOT NULL,
            `applied_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `duration_ms`     INT UNSIGNED NOT NULL DEFAULT 0,
            `statement_count` INT UNSIGNED NOT NULL DEFAULT 0,
            `applied_by`      VARCHAR(255) NULL,
            PRIMARY KEY (`version`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    if connection is not None:
        connection.commit()


def read_ledger(cursor) -> Dict[str, dict]:
    cursor.execute(
        f"SELECT `version`, `filename`, `checksum`, `applied_at` FROM `{LEDGER_TABLE}`"
    )
    rows = cursor.fetchall() or []
    out: Dict[str, dict] = {}
    for row in rows:
        if isinstance(row, dict):
            out[str(row["version"])] = row
        else:
            out[str(row[0])] = {
                "version": row[0],
                "filename": row[1],
                "checksum": row[2],
                "applied_at": row[3],
            }
    return out


def record_applied(
    cursor,
    connection,
    migration: Migration,
    duration_ms: int,
    statement_count: int,
) -> None:
    cursor.execute(
        f"""
        INSERT INTO `{LEDGER_TABLE}`
            (`version`, `filename`, `checksum`, `duration_ms`, `statement_count`, `applied_by`)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            `filename`        = VALUES(`filename`),
            `checksum`        = VALUES(`checksum`),
            `applied_at`      = CURRENT_TIMESTAMP,
            `duration_ms`     = VALUES(`duration_ms`),
            `statement_count` = VALUES(`statement_count`),
            `applied_by`      = VALUES(`applied_by`)
        """,
        (
            migration.version,
            migration.filename,
            migration.checksum,
            duration_ms,
            statement_count,
            os.environ.get("HOSTNAME") or "unknown",
        ),
    )
    if connection is not None:
        connection.commit()


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------
def _drain(cursor) -> None:
    """
    Consume any result set a statement produced.

    The migration files SELECT progress lines ("=== STEP 1 ... ===") and the
    guarded-DDL procedures SELECT a log row per change. mysql-connector raises
    "Unread result found" on the next execute() if those rows are left sitting
    on the connection, so every statement's output is drained and logged.
    """
    while True:
        try:
            if cursor.with_rows:
                for row in cursor.fetchall() or []:
                    values = list(row.values()) if isinstance(row, dict) else list(row)
                    text = " | ".join("" if v is None else str(v) for v in values)
                    if text.strip():
                        logger.info("      %s", text.strip())
        except Exception:
            pass
        try:
            if not cursor.nextset():
                break
        except Exception:
            break


def _acquire_lock(cursor, db_name: str, timeout_s: int) -> bool:
    cursor.execute("SELECT GET_LOCK(%s, %s)", (f"{LOCK_NAME_PREFIX}:{db_name}", timeout_s))
    row = cursor.fetchone()
    _drain(cursor)
    value = list(row.values())[0] if isinstance(row, dict) else (row[0] if row else None)
    return value == 1


def _release_lock(cursor, db_name: str) -> None:
    try:
        cursor.execute("SELECT RELEASE_LOCK(%s)", (f"{LOCK_NAME_PREFIX}:{db_name}",))
        cursor.fetchall()
        _drain(cursor)
    except Exception:
        pass


def apply_migration(cursor, connection, migration: Migration) -> int:
    """
    Run every statement in one file, then return how long it took in ms.

    DDL in MySQL is not transactional, so there is no wrapping transaction to
    roll back: each statement is committed as it succeeds and the file is only
    recorded in the ledger once all of them have. A file that dies halfway is
    therefore re-applied from the top next time, which is safe because every
    statement in these files is individually guarded.
    """
    statements = migration.statements
    started = time.monotonic()
    for position, statement in enumerate(statements, start=1):
        try:
            cursor.execute(statement)
            _drain(cursor)
            if connection is not None:
                connection.commit()
        except Exception as exc:
            preview = " ".join(statement.split())[:200]
            raise MigrationError(
                f"{migration.filename}: statement {position}/{len(statements)} failed: "
                f"{exc}\n    SQL: {preview}"
            ) from exc
    return int((time.monotonic() - started) * 1000)


def run_migrations_for_brand(
    brand_index: int,
    migrations: Optional[List[Migration]] = None,
    dry_run: bool = False,
    lock_timeout_s: int = 300,
    strict_checksums: bool = False,
) -> BrandResult:
    """Apply every pending migration to one brand's database."""
    from pipeline.db import get_db_cursor

    brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
    db_name = os.environ.get(f"DB_DATABASE_{brand_index}", f"brand_{brand_index}")
    result = BrandResult(brand_index=brand_index, brand_name=brand_name)
    migrations = discover_migrations() if migrations is None else migrations

    try:
        with get_db_cursor(brand_index, dictionary=True) as (cursor, connection):
            if not _acquire_lock(cursor, db_name, lock_timeout_s):
                result.lock_timeout = True
                logger.warning(
                    "🔒 [%s] another process holds the migration lock for %s "
                    "(waited %ss); skipping - it is doing this work",
                    brand_name,
                    db_name,
                    lock_timeout_s,
                )
                return result
            try:
                ensure_ledger_table(cursor, connection)
                ledger = read_ledger(cursor)

                for migration in migrations:
                    entry = ledger.get(migration.version)

                    if entry is not None:
                        if entry.get("checksum") != migration.checksum:
                            result.drifted.append(migration.version)
                            message = (
                                f"[{brand_name}] {migration.filename} was applied on "
                                f"{entry.get('applied_at')} but the file has changed since "
                                f"(recorded checksum {str(entry.get('checksum'))[:12]}..., "
                                f"file now {migration.checksum[:12]}...). It will NOT be "
                                f"re-run; add a new migration instead of editing an applied one."
                            )
                            if strict_checksums:
                                raise MigrationError(message)
                            logger.warning("⚠️  %s", message)
                        result.skipped.append(migration.version)
                        continue

                    if migration.manual:
                        result.manual.append(migration.version)
                        logger.info(
                            "⏭️  [%s] %s is marked `-- migrate: manual`; run it by hand",
                            brand_name,
                            migration.filename,
                        )
                        continue

                    if dry_run:
                        result.applied.append(migration.version)
                        logger.info(
                            "🔎 [%s] would apply %s (%d statements)",
                            brand_name,
                            migration.filename,
                            len(migration.statements),
                        )
                        continue

                    logger.info("⬆️  [%s] applying %s", brand_name, migration.filename)
                    try:
                        duration_ms = apply_migration(cursor, connection, migration)
                    except MigrationError as exc:
                        result.failed = (migration.version, str(exc))
                        logger.error("❌ [%s] %s", brand_name, exc)
                        return result

                    record_applied(
                        cursor,
                        connection,
                        migration,
                        duration_ms,
                        len(migration.statements),
                    )
                    result.applied.append(migration.version)
                    logger.info(
                        "✅ [%s] %s applied in %dms",
                        brand_name,
                        migration.filename,
                        duration_ms,
                    )
            finally:
                _release_lock(cursor, db_name)
    except MigrationError:
        raise
    except Exception as exc:
        result.error = str(exc)
        logger.error("❌ [%s] migration run failed: %s", brand_name, exc)

    return result


def run_migrations_for_all_brands(
    brand_indices: Optional[List[int]] = None,
    dry_run: bool = False,
    lock_timeout_s: int = 300,
    strict_checksums: bool = False,
) -> Dict[int, BrandResult]:
    """
    Apply pending migrations to every active brand database, one brand at a
    time.

    Serial on purpose: these statements rebuild tables and churn indexes, and
    the brands share an RDS instance whose EBS byte balance is the constraint
    migrations/001_rds_perf_pass.sql exists to protect. Brands are independent,
    so one brand failing does not stop the others - every failure is collected
    and reported at the end.
    """
    from pipeline.state import active_brand_indices, brand_tag_to_index_map

    explicit = brand_indices is not None
    targets = list(brand_indices if explicit else active_brand_indices)
    migrations = discover_migrations()

    if not migrations:
        logger.warning("No migrations found in %s; nothing to do", MIGRATIONS_DIR)
        return {}
    if not targets and not explicit:
        # Not "nothing to do": if brands were expected, none of them are
        # reachable, and letting the gate pass would start every worker against
        # unmigrated databases. _unreachable_brands() below decides.
        logger.warning("No active brands; nothing to migrate")
    elif not targets:
        logger.warning("No brands matched --brand; nothing to migrate")
        return {}

    auto = [m for m in migrations if not m.manual]
    manual = [m for m in migrations if m.manual]
    logger.info("=" * 60)
    logger.info(
        "🧱 SQL MIGRATIONS - %d file(s) (%d automatic, %d manual) × %d brand(s)%s",
        len(migrations),
        len(auto),
        len(manual),
        len(targets),
        " [DRY RUN]" if dry_run else "",
    )
    logger.info("=" * 60)

    results: Dict[int, BrandResult] = {}
    for brand_index in targets:
        results[brand_index] = run_migrations_for_brand(
            brand_index,
            migrations=migrations,
            dry_run=dry_run,
            lock_timeout_s=lock_timeout_s,
            strict_checksums=strict_checksums,
        )

    if not explicit:
        for brand_index in _unreachable_brands():
            brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
            logger.error(
                "❌ [%s] brand_index=%s never initialized (bad credentials, unreachable "
                "host, or a failed startup connection test) - its database has NOT been "
                "migrated",
                brand_name,
                brand_index,
            )
            results[brand_index] = BrandResult(
                brand_index=brand_index,
                brand_name=brand_name,
                error="brand did not initialize; database not migrated",
            )

    _log_summary(results)
    return results


def _unreachable_brands() -> List[int]:
    """
    Brands that were expected but are not in active_brand_indices.

    initialize_brand_configs() drops a brand whose engine setup or startup
    connection test fails - it logs the error and moves on, leaving the brand
    out of active_brand_indices entirely. Without this check the runner would
    migrate the brands it can see, report success, and let every worker
    container start against a brand database that never got its migrations.

    brand_tag_to_index_map is the expected set: initialize_brand_configs writes
    to it BEFORE it tests the connection, so it still names the brands that were
    later dropped. Brands rejected earlier - missing env vars, an inactive
    tenant, a config the API would not return - never reach it, and are
    correctly not counted as failures here.
    """
    from pipeline.state import active_brand_indices, brand_tag_to_index_map

    return sorted(set(brand_tag_to_index_map.values()) - set(active_brand_indices))


def _log_summary(results: Dict[int, BrandResult]) -> None:
    logger.info("-" * 60)
    logger.info("🧱 MIGRATION SUMMARY")
    for result in results.values():
        bits = [f"applied={len(result.applied)}", f"already={len(result.skipped)}"]
        if result.manual:
            bits.append(f"manual={len(result.manual)}")
        if result.drifted:
            bits.append(f"drifted={','.join(result.drifted)}")
        if result.lock_timeout:
            bits.append("LOCK-TIMEOUT")
        if result.failed:
            bits.append(f"FAILED at {result.failed[0]}")
        if result.error:
            bits.append(f"ERROR {result.error}")
        icon = "✅" if result.ok else "❌"
        logger.info("  %s %-24s %s", icon, result.brand_name, "  ".join(bits))
    failures = [r for r in results.values() if not r.ok]
    if failures:
        logger.error(
            "❌ %d of %d brand(s) did not migrate cleanly", len(failures), len(results)
        )
    else:
        logger.info("✅ all %d brand(s) up to date", len(results))
    logger.info("-" * 60)


# ---------------------------------------------------------------------------
# Status / CLI
# ---------------------------------------------------------------------------
def print_status(brand_indices: Optional[List[int]] = None) -> int:
    """Print, per brand, which migrations are applied and which are pending."""
    from pipeline.db import get_db_cursor
    from pipeline.state import active_brand_indices

    targets = list(brand_indices if brand_indices is not None else active_brand_indices)
    migrations = discover_migrations()
    width = max((len(m.filename) for m in migrations), default=20)

    for brand_index in targets:
        brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
        db_name = os.environ.get(f"DB_DATABASE_{brand_index}", "?")
        print(f"\n=== {brand_name} (brand_index={brand_index}, db={db_name}) ===")
        try:
            with get_db_cursor(brand_index, dictionary=True) as (cursor, connection):
                ensure_ledger_table(cursor, connection)
                ledger = read_ledger(cursor)
        except Exception as exc:
            print(f"  ! could not read ledger: {exc}")
            continue

        for migration in migrations:
            entry = ledger.get(migration.version)
            if entry is None:
                state = "MANUAL - run by hand" if migration.manual else "PENDING"
            elif entry.get("checksum") != migration.checksum:
                state = f"applied {entry.get('applied_at')}  ⚠️  FILE CHANGED SINCE"
            else:
                state = f"applied {entry.get('applied_at')}"
            print(f"  {migration.filename.ljust(width)}  {state}")

        orphans = set(ledger) - {m.version for m in migrations}
        for version in sorted(orphans):
            print(
                f"  {str(ledger[version].get('filename')).ljust(width)}  "
                f"in ledger but the file is gone"
            )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pipeline.sql_migrations",
        description="Apply migrations/*.sql to every brand database, once each.",
    )
    parser.add_argument(
        "--brand",
        dest="brands",
        action="append",
        type=int,
        help="brand_index to migrate; repeatable (default: every active brand)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="report what would be applied without running any DDL",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="print the per-brand ledger state and exit",
    )
    parser.add_argument(
        "--lock-timeout",
        type=int,
        default=int(os.environ.get("MIGRATIONS_LOCK_TIMEOUT_S", "300")),
        help="seconds to wait for another container's migration lock (default 300)",
    )
    parser.add_argument(
        "--strict-checksums",
        action="store_true",
        default=os.environ.get("MIGRATIONS_STRICT_CHECKSUMS", "false").strip().lower()
        == "true",
        help="fail instead of warning when an applied migration file has changed",
    )
    parser.add_argument(
        "--allow-failure",
        action="store_true",
        default=os.environ.get("MIGRATIONS_ALLOW_FAILURE", "false").strip().lower()
        == "true",
        help="exit 0 even if a brand failed (container starts anyway)",
    )
    parser.add_argument(
        "--env-file",
        dest="env_file",
        default=None,
        help="dotenv file to load first (default: .env, else ./env if present)",
    )
    return parser


def _default_env_file() -> str:
    for candidate in (".env", "env"):
        path = PROJECT_ROOT / candidate
        if path.is_file():
            return str(path)
    return ""


def main(argv=None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    args = build_parser().parse_args(argv)

    env_file = args.env_file if args.env_file is not None else _default_env_file()
    if env_file:
        if not os.path.isfile(env_file):
            logger.error("env file not found: %s", env_file)
            return 1
        from dotenv import load_dotenv

        load_dotenv(env_file, override=False)
        logger.info("Loaded env from %s", env_file)

    # Brand DB credentials come from the brands API (or local env in
    # TEST_MODE); this is what populates brand_db_configs / sqlalchemy_engines
    # / active_brand_indices in pipeline.state.
    import aws_background

    aws_background.initialize_brand_configs()

    if args.status:
        return print_status(args.brands)

    try:
        results = run_migrations_for_all_brands(
            brand_indices=args.brands,
            dry_run=args.dry_run,
            lock_timeout_s=args.lock_timeout,
            strict_checksums=args.strict_checksums,
        )
    except MigrationError as exc:
        logger.error("❌ %s", exc)
        return 0 if args.allow_failure else 1

    if not results:
        return 0
    failed = [r for r in results.values() if not r.ok]
    if failed and not args.allow_failure:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
