"""Tests for pipeline/data_purge.py. No database and no S3: an in-memory fake database that understands the
exact query shapes the module builds, and an in-memory fake S3 store with failure hooks."""
import hashlib
import io
import itertools
import json
import re
import unittest
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from unittest import mock
from zoneinfo import ZoneInfo

from pipeline import data_purge as dp

IST = ZoneInfo("Asia/Kolkata")
RUN_DT = datetime(2026, 10, 1, 3, 23, tzinfo=IST)  # cutoff 2026-04-01, archive month 2026-10


# --------------------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------------------
class FakeTable:
    def __init__(self, name, columns, date_col, rows, types=None, nullable=None, indexes=None,
                 table_type="BASE TABLE", plan=None):
        self.name = name
        self.columns = list(columns)
        self.date_col = date_col
        self.rows = [dict(r) for r in rows]
        self.types = types or {}
        self.nullable = nullable or {}
        self.indexes = indexes or []  # (name, non_unique, [cols])
        self.table_type = table_type
        self.plan = plan  # None = a good range plan on the first index leading with date_col


def orders_table(rows, **kw):
    kw.setdefault("types", {"order_id": "varchar", "created_date": "varchar", "total": "double"})
    kw.setdefault("indexes", [("idx_so_created_date", 1, ["created_date"])])
    return FakeTable("shopify_orders", ["order_id", "created_date", "total"], "created_date", rows, **kw)


def events_table(rows, **kw):
    kw.setdefault("types", {"event_id": "varchar", "created_at_utc": "datetime", "created_at_ist": "datetime",
                            "event_type": "varchar"})
    kw.setdefault("nullable", {"event_id": False, "created_at_utc": True, "created_at_ist": True, "event_type": True})
    kw.setdefault("indexes", [("PRIMARY", 0, ["event_id"]), ("idx_created_at_utc", 1, ["created_at_utc"]),
                              ("idx_created_at_ist", 1, ["created_at_ist"])])
    return FakeTable("raw_mongo_events", ["event_id", "created_at_utc", "created_at_ist", "event_type"],
                     "created_at_ist", rows, **kw)


def ev(event_id, utc, event_type="x"):
    """A raw_mongo_events row: created_at_ist is the IST wall-clock of created_at_utc (UTC + 5:30)."""
    if utc is None:
        return {"event_id": event_id, "created_at_utc": None, "created_at_ist": None, "event_type": event_type}
    return {"event_id": event_id, "created_at_utc": utc, "created_at_ist": utc + timedelta(hours=5, minutes=30),
            "event_type": event_type}


def order_rows(n_old=30, n_new=5):
    rows = []
    for i in range(n_old):
        rows.append({"order_id": f"old{i}", "created_date": f"2026-03-{(i % 28) + 1:02d}", "total": float(i)})
    rows.append({"order_id": "edge-old", "created_date": "2026-03-31", "total": 1.0})
    rows.append({"order_id": "edge-new", "created_date": "2026-04-01", "total": 2.0})
    for i in range(n_new):
        rows.append({"order_id": f"new{i}", "created_date": f"2026-09-{i + 1:02d}", "total": 3.0})
    return rows


def event_rows(n_old=30, n_new=5):
    rows = []
    for i in range(n_old):
        rows.append(ev(f"e-old-{i:04d}", datetime(2026, 3, 1 + i % 28, 10, i % 60), "x"))
    for i in range(n_new):
        rows.append(ev(f"e-new-{i:04d}", datetime(2026, 9, 1 + i, 10, 0), "y"))
    return rows


class FakeConnection:
    def __init__(self, events):
        self.events = events
        self.commits = 0

    def commit(self):
        self.commits += 1
        self.events.append(("commit",))


class FakeDatabase:
    def __init__(self, tables=(), events=None):
        self.tables = {t.name: t for t in tables}
        self.events = events if events is not None else []
        self.executed = []
        self.processlist = []
        self.lock_result = 1
        self.count_hint_fails = False
        self.fail_delete_on_call = None  # nth DELETE raises
        self._deletes = 0
        self.conn = FakeConnection(self.events)

    @property
    def deletes(self):
        return [e for e in self.events if e[0] == "delete"]


class FakeCursor:
    def __init__(self, db):
        self.db = db
        self._result = []
        self.rowcount = 0
        self.description = None

    # -- helpers -------------------------------------------------------------
    @staticmethod
    def _norm(sql):
        return re.sub(r"\s+", " ", sql).strip()

    def _table(self, name):
        return self.db.tables[name]

    def _eval(self, table, cond, params):
        m = re.fullmatch(r"`(\w+)` < %s", cond)
        if m:
            c = m.group(1)
            return [r for r in table.rows if r[c] is not None and r[c] < params[0]]
        m = re.fullmatch(r"`(\w+)` >= %s AND `\1` < %s", cond)
        if m:
            c = m.group(1)
            return [r for r in table.rows if r[c] is not None and params[0] <= r[c] < params[1]]
        raise AssertionError(f"fake DB does not understand condition: {cond}")

    def _default_plan(self, table):
        for name, _nu, cols in table.indexes:
            if cols[0] == table.date_col:
                return [{"type": "range", "key": name, "rows": len(table.rows), "extra": "Using where"}]
        return [{"type": "ALL", "key": None, "rows": len(table.rows), "extra": ""}]

    # -- DB API --------------------------------------------------------------
    def execute(self, sql, params=None):
        db, s, p = self.db, self._norm(sql), tuple(params or ())
        db.executed.append((s, p))
        self.rowcount = 0
        self._result = []

        if "information_schema.tables" in s:
            t = db.tables.get(p[0])
            if t:
                self._result = [{"table_type": t.table_type, "table_rows": len(t.rows), "data_length": 1000, "index_length": 500}]
        elif "information_schema.columns" in s:
            t = db.tables.get(p[0])
            if t:
                self._result = [
                    {"column_name": c, "data_type": t.types.get(c, "varchar"),
                     "is_nullable": "YES" if t.nullable.get(c, True) else "NO"} for c in t.columns
                ]
        elif "information_schema.statistics" in s:
            t = db.tables.get(p[0])
            if t:
                for name, non_unique, cols in sorted(t.indexes):
                    for seq, col in enumerate(cols, 1):
                        self._result.append({"index_name": name, "non_unique": non_unique, "seq_in_index": seq, "column_name": col})
        elif s == "SELECT DATABASE() AS db":
            self._result = [{"db": "brand_db"}]
        elif s == "SELECT DATABASE() AS db, CONNECTION_ID() AS id":
            self._result = [{"db": "brand_db", "id": 1}]
        elif s.startswith("SELECT GET_LOCK"):
            self._result = [{"got": db.lock_result}]
        elif s.startswith("SELECT RELEASE_LOCK"):
            self._result = [{"released": 1}]
        elif s == "SHOW FULL PROCESSLIST":
            self._result = [dict(proc) for proc in db.processlist]
        elif s.startswith("EXPLAIN "):
            m = re.search(r"(?:FROM) `(\w+)`", s)
            t = self._table(m.group(1))
            self._result = list(t.plan) if t.plan is not None else self._default_plan(t)
            if t.plan == "RAISE":
                raise RuntimeError("EXPLAIN not permitted")
        elif "COUNT(*) AS n" in s:
            m = re.fullmatch(r"SELECT (/\*\+ MAX_EXECUTION_TIME\(\d+\) \*/ )?COUNT\(\*\) AS n FROM `(\w+)` WHERE (.+)", s)
            if m.group(1) and db.count_hint_fails:
                raise RuntimeError("Query execution was interrupted, maximum statement execution time exceeded")
            self._result = [{"n": len(self._eval(self._table(m.group(2)), m.group(3), p))}]
        elif s.startswith("SELECT MIN("):
            m = re.fullmatch(r"SELECT MIN\(`(\w+)`\) AS lo, MAX\(`\1`\) AS hi FROM `(\w+)` WHERE `\1` < %s", s)
            vals = [r[m.group(1)] for r in self._table(m.group(2)).rows if r[m.group(1)] is not None and r[m.group(1)] < p[0]]
            self._result = [{"lo": min(vals) if vals else None, "hi": max(vals) if vals else None}]
        elif s.startswith("SELECT 1 AS n FROM"):
            m = re.fullmatch(r"SELECT 1 AS n FROM `(\w+)` WHERE `(\w+)` (>= %s|< %s|IS NULL) LIMIT 1", s)
            t, c, op = self._table(m.group(1)), m.group(2), m.group(3)
            if op == ">= %s":
                hit = any(r[c] is not None and r[c] >= p[0] for r in t.rows)
            elif op == "< %s":
                hit = any(r[c] is not None and r[c] < p[0] for r in t.rows)
            else:
                hit = any(r[c] is None for r in t.rows)
            self._result = [{"n": 1}] if hit else []
        elif s.startswith("DELETE FROM"):
            db._deletes += 1
            if db.fail_delete_on_call == db._deletes:
                raise RuntimeError("deadlock found when trying to get lock")
            m = re.fullmatch(r"DELETE FROM `(\w+)` WHERE `(\w+)` < %s ORDER BY `\2` LIMIT %s", s)
            t, c = self._table(m.group(1)), m.group(2)
            eligible = sorted((r for r in t.rows if r[c] is not None and r[c] < p[0]), key=lambda r: r[c])[: p[1]]
            ids = {id(r) for r in eligible}
            t.rows = [r for r in t.rows if id(r) not in ids]
            self.rowcount = len(eligible)
            db.events.append(("delete", m.group(1), p[1], len(eligible)))
        elif s.startswith("SELECT ") and " ORDER BY " in s:  # keyset export
            m = re.fullmatch(r"SELECT (.+) FROM `(\w+)` WHERE (.+) ORDER BY (.+) LIMIT %s", s)
            cols = re.findall(r"`(\w+)`", m.group(1))
            t = self._table(m.group(2))
            order = re.findall(r"`(\w+)`", m.group(4))
            where = m.group(3)
            if where.startswith("("):
                n = len(order)
                rows = [r for r in t.rows if tuple(r[c] for c in order) > p[:n] and r[order[0]] < p[n]]
            else:
                rows = self._eval(t, where, p)
            rows = sorted(rows, key=lambda r: tuple(r[c] for c in order))[: p[-1]]
            self._result = [{c: r[c] for c in cols} for r in rows]
        elif s.startswith("SELECT "):  # window export
            m = re.fullmatch(r"SELECT (.+) FROM `(\w+)` WHERE (.+)", s)
            cols = re.findall(r"`(\w+)`", m.group(1))
            rows = self._eval(self._table(m.group(2)), m.group(3), p)
            self._result = [{c: r[c] for c in cols} for r in rows]
        else:
            raise AssertionError(f"fake DB does not understand: {s}")

    def fetchall(self):
        out, self._result = self._result, []
        return out

    def fetchmany(self, n):
        out, self._result = self._result[:n], self._result[n:]
        return out


def brand_for(db, name="pts", tz=IST):
    @contextmanager
    def ctx():
        yield FakeCursor(db), db.conn

    return dp.BrandContext(index=1, name=name, tz=tz, cursor_ctx=ctx)


class FakeStore:
    def __init__(self, events=None):
        self.events = events if events is not None else []
        self.objects = {}
        self.tags = {}
        self.jsons = {}
        self.upload_calls = []
        self.fail_upload = False
        self.truncate_on_upload = False
        self.fail_tagging = False
        self.fail_manifest_write = False
        self.fail_keys_containing = None
        self.on_upload = None  # callable(), run after the stream was fully read

    def head(self, key):
        obj = self.objects.get(key)
        return None if obj is None else dp.ObjectInfo(size=len(obj))

    def upload_stream(self, key, fileobj):
        self.upload_calls.append(key)
        if self.fail_keys_containing and self.fail_keys_containing in key:
            raise RuntimeError("S3 is down")
        pieces = []
        while True:
            piece = fileobj.read(64 * 1024)
            if not piece:
                break
            pieces.append(piece)
            if self.fail_upload:
                raise RuntimeError("S3 is down")
        data = b"".join(pieces)
        if self.truncate_on_upload:
            data = data[:-7]
        self.objects[key] = data
        self.events.append(("upload", key))
        if self.on_upload:
            self.on_upload()

    def get_tags(self, key):
        return dict(self.tags.get(key, {}))

    def put_tags(self, key, tags):
        if self.fail_tagging:
            raise RuntimeError("AccessDenied: s3:PutObjectTagging")
        self.tags[key] = dict(tags)

    def get_json(self, key):
        return json.loads(json.dumps(self.jsons[key])) if key in self.jsons else None

    def read_head(self, key, nbytes):
        return self.objects[key][:nbytes]

    def put_json_if_absent(self, key, obj):
        if self.fail_manifest_write and key.endswith(".manifest.json"):
            raise RuntimeError("S3 is down")
        if key in self.jsons:
            return False
        self.jsons[key] = json.loads(json.dumps(obj, default=str))
        self.events.append(("manifest" if key.endswith(".manifest.json") else "marker", key))
        return True


ARCHIVE = {"DATA_PURGE_ENABLED": "true", "DATA_PURGE_S3_BUCKET": "bkt"}


def full_env(tables="raw_mongo_events,shopify_orders", **extra):
    env = dict(ARCHIVE, DATA_PURGE_ARCHIVE_ONLY="false", DATA_PURGE_DELETE_TABLES=tables,
               DATA_PURGE_BATCH_SIZE="1000", DATA_PURGE_SLEEP_BETWEEN_BATCHES_S="0")
    env.update(extra)
    return env


def make_run(env, store=None, run_dt=RUN_DT, proc_env=None):
    cfg = dp.load_config(env)
    return dp.RunContext(
        cfg=cfg, run_dt=run_dt, cutoff=dp.compute_cutoff(run_dt), month=dp.archive_month_label(run_dt),
        store=store, env=proc_env if proc_env is not None else {},
    )


def run_table(db, store, env, table="raw_mongo_events", **kw):
    run = make_run(env, store, **kw)
    spec = dp.SPECS_BY_NAME[table]
    return dp.process_table(run, brand_for(db), spec), run


def csv_rows(data: bytes):
    import csv

    return list(csv.reader(io.StringIO(data.decode("utf-8"))))


# --------------------------------------------------------------------------------------
# Cutoff and boundaries
# --------------------------------------------------------------------------------------
class CutoffTests(unittest.TestCase):
    def test_examples_from_the_spec(self):
        self.assertEqual(dp.compute_cutoff(datetime(2026, 10, 1, tzinfo=IST)), date(2026, 4, 1))
        self.assertEqual(dp.compute_cutoff(datetime(2026, 11, 1, tzinfo=IST)), date(2026, 5, 1))
        self.assertEqual(dp.compute_cutoff(datetime(2026, 1, 1, tzinfo=IST)), date(2025, 7, 1))

    def test_calendar_months_not_180_days(self):
        # 180 days before 2026-10-01 is 2026-04-04; the calendar cutoff is 2026-04-01
        self.assertEqual(dp.compute_cutoff(datetime(2026, 10, 1, tzinfo=IST)), date(2026, 4, 1))
        # months of different length still land on the 1st
        self.assertEqual(dp.compute_cutoff(datetime(2026, 8, 31, tzinfo=IST)), date(2026, 2, 1))
        self.assertEqual(dp.compute_cutoff(datetime(2026, 12, 15, tzinfo=IST)), date(2026, 6, 1))
        self.assertEqual(dp.compute_cutoff(datetime(2026, 6, 15, tzinfo=IST)), date(2025, 12, 1))

    def test_any_day_of_the_month_gives_the_same_cutoff(self):
        cutoffs = {dp.compute_cutoff(datetime(2026, 10, d, 12, 0, tzinfo=IST)) for d in (1, 2, 15, 31)}
        self.assertEqual(cutoffs, {date(2026, 4, 1)})

    def test_month_is_the_ist_month_even_for_a_utc_run_time(self):
        # 2026-09-30 22:00 UTC is already 2026-10-01 03:30 IST
        self.assertEqual(dp.compute_cutoff(datetime(2026, 9, 30, 22, 0, tzinfo=timezone.utc)), date(2026, 4, 1))
        self.assertEqual(dp.archive_month_label(datetime(2026, 9, 30, 22, 0, tzinfo=timezone.utc)), "2026-10")
        # naive datetimes are read as IST
        self.assertEqual(dp.compute_cutoff(datetime(2026, 10, 1, 0, 0)), date(2026, 4, 1))


class BoundaryTests(unittest.TestCase):
    def test_local_date_cutoff_parameter(self):
        self.assertEqual(dp.cutoff_param(dp.DateKind.LOCAL_DATE, date(2026, 4, 1), IST), "2026-04-01")

    def test_local_datetime_cutoff_is_plain_local_midnight_regardless_of_timezone(self):
        # created_at_ist already holds IST wall-clock time, so no conversion is applied
        self.assertEqual(dp.cutoff_param(dp.DateKind.LOCAL_DATETIME, date(2026, 4, 1), IST), datetime(2026, 4, 1, 0, 0))
        self.assertEqual(
            dp.cutoff_param(dp.DateKind.LOCAL_DATETIME, date(2026, 4, 1), ZoneInfo("Asia/Dubai")), datetime(2026, 4, 1, 0, 0)
        )

    def test_utc_cutoff_is_local_midnight_converted_to_utc(self):
        self.assertEqual(dp.cutoff_param(dp.DateKind.UTC_DATETIME, date(2026, 4, 1), IST), datetime(2026, 3, 31, 18, 30))
        self.assertEqual(
            dp.cutoff_param(dp.DateKind.UTC_DATETIME, date(2026, 4, 1), ZoneInfo("Asia/Dubai")), datetime(2026, 3, 31, 20, 0)
        )

    def test_local_date_boundary_rows(self):
        db = FakeDatabase([orders_table([
            {"order_id": "a", "created_date": "2026-03-31", "total": 1.0},
            {"order_id": "b", "created_date": "2026-04-01", "total": 1.0},
        ])])
        cur = FakeCursor(db)
        spec = dp.SPECS_BY_NAME["shopify_orders"]
        self.assertEqual(dp.count_eligible(cur, spec, "2026-04-01"), 1)  # only 03-31

    def test_raw_mongo_events_boundary_is_exact_on_the_ist_column(self):
        db = FakeDatabase([events_table([
            ev("keep", datetime(2026, 3, 31, 18, 30, 0)),  # 2026-04-01 00:00:00 IST: kept
            ev("purge", datetime(2026, 3, 31, 18, 29, 59, 999000)),  # 2026-03-31 23:59:59.999 IST: eligible
            ev("also-keep", datetime(2026, 4, 1, 5, 0)),
        ])])
        spec = dp.SPECS_BY_NAME["raw_mongo_events"]
        value = dp.cutoff_param(spec.date_kind, date(2026, 4, 1), IST)
        self.assertEqual(value, datetime(2026, 4, 1))
        self.assertEqual(dp.count_eligible(FakeCursor(db), spec, value), 1)

    def test_utc_kind_boundary_rows_are_exact(self):
        # the UTC-column conversion is still used by the legacy mongo_events_summary table
        spec = dp.SPECS_BY_NAME["mongo_events_summary"]
        t = FakeTable("mongo_events_summary", ["event_id", "created_at_utc"], "created_at_utc", [
            {"event_id": "keep", "created_at_utc": datetime(2026, 3, 31, 18, 30, 0)},
            {"event_id": "purge", "created_at_utc": datetime(2026, 3, 31, 18, 29, 59, 999000)},
        ])
        value = dp.cutoff_param(spec.date_kind, date(2026, 4, 1), IST)
        self.assertEqual(dp.count_eligible(FakeCursor(FakeDatabase([t])), spec, value), 1)

    def test_row_exactly_on_the_cutoff_is_never_deleted_in_a_full_run(self):
        db = FakeDatabase([orders_table(order_rows())])
        store = FakeStore()
        result, _ = run_table(db, store, full_env(), table="shopify_orders")
        self.assertEqual(result.status, dp.Status.SUCCESS, result.reason)
        remaining = {r["order_id"] for r in db.tables["shopify_orders"].rows}
        self.assertIn("edge-new", remaining)  # created_date == cutoff
        self.assertNotIn("edge-old", remaining)  # 2026-03-31


# --------------------------------------------------------------------------------------
# Configuration state machine
# --------------------------------------------------------------------------------------
class ConfigTests(unittest.TestCase):
    def test_defaults_are_disabled_and_archive_only(self):
        cfg = dp.load_config({})
        self.assertEqual(cfg.mode, dp.Mode.DISABLED)
        self.assertEqual(cfg.intended_mode, dp.Mode.DISABLED)
        self.assertEqual(cfg.delete_tables, ())
        self.assertEqual(len(cfg.tables), 7)

    def test_enabled_without_explicit_archive_only_false_never_deletes(self):
        cfg = dp.load_config(ARCHIVE)
        self.assertEqual(cfg.mode, dp.Mode.ARCHIVE_ONLY)

    def test_purge_test_defaults_off_and_is_strict(self):
        self.assertFalse(dp.load_config({}).purge_test)
        self.assertTrue(dp.load_config({"PURGE_TEST": "true"}).purge_test)
        self.assertTrue(dp.load_config({"PURGE_TEST": " TRUE "}).purge_test)
        self.assertFalse(dp.load_config({"PURGE_TEST": "false"}).purge_test)
        for bad in ("yes", "1", "ture", "on"):
            with self.subTest(value=bad), self.assertRaises(dp.ConfigError):
                dp.load_config({"PURGE_TEST": bad})

    def test_purge_test_caps_an_enabled_run_at_archive_only_whatever_else_is_set(self):
        cfg = dp.load_config(full_env(PURGE_TEST="true"))  # would be FULL without PURGE_TEST
        self.assertEqual((cfg.mode, cfg.intended_mode), (dp.Mode.ARCHIVE_ONLY, dp.Mode.ARCHIVE_ONLY))
        # no allowlist needed: nothing can be deleted
        cfg = dp.load_config(dict(ARCHIVE, DATA_PURGE_ARCHIVE_ONLY="false", PURGE_TEST="true"))
        self.assertEqual(cfg.mode, dp.Mode.ARCHIVE_ONLY)

    def test_purge_test_does_not_enable_a_disabled_worker_or_override_dry_run(self):
        self.assertEqual(dp.load_config({"PURGE_TEST": "true"}).mode, dp.Mode.DISABLED)
        self.assertEqual(dp.load_config({"PURGE_TEST": "true", "DATA_PURGE_DRY_RUN": "true"}).mode, dp.Mode.DRY_RUN)

    def test_purge_test_still_needs_a_bucket_to_export(self):
        with self.assertRaises(dp.ConfigError):
            dp.load_config({"DATA_PURGE_ENABLED": "true", "PURGE_TEST": "true"})

    def test_dry_run_takes_precedence_over_everything(self):
        cfg = dp.load_config({"DATA_PURGE_DRY_RUN": "true", "DATA_PURGE_ENABLED": "true", "DATA_PURGE_ARCHIVE_ONLY": "false",
                              "DATA_PURGE_DELETE_TABLES": "raw_mongo_events"})
        self.assertEqual(cfg.mode, dp.Mode.DRY_RUN)
        self.assertEqual(cfg.intended_mode, dp.Mode.FULL)
        self.assertEqual(dp.load_config({"DATA_PURGE_DRY_RUN": "true"}).mode, dp.Mode.DRY_RUN)  # runs even if disabled

    def test_full_requires_an_explicit_delete_allowlist_and_a_bucket(self):
        with self.assertRaises(dp.ConfigError):
            dp.load_config(dict(ARCHIVE, DATA_PURGE_ARCHIVE_ONLY="false"))
        with self.assertRaises(dp.ConfigError):
            dp.load_config({"DATA_PURGE_ENABLED": "true", "DATA_PURGE_ARCHIVE_ONLY": "false",
                            "DATA_PURGE_DELETE_TABLES": "raw_mongo_events"})
        cfg = dp.load_config(full_env(tables="raw_mongo_events"))
        self.assertEqual((cfg.mode, cfg.delete_tables), (dp.Mode.FULL, ("raw_mongo_events",)))

    def test_archive_runs_need_a_bucket_but_disabled_and_dry_run_do_not(self):
        with self.assertRaises(dp.ConfigError):
            dp.load_config({"DATA_PURGE_ENABLED": "true"})
        dp.load_config({})
        dp.load_config({"DATA_PURGE_DRY_RUN": "true"})

    def test_unparseable_values_fail_closed(self):
        for name in ("DATA_PURGE_ENABLED", "DATA_PURGE_DRY_RUN", "DATA_PURGE_ARCHIVE_ONLY"):
            for bad in ("yes", "1", "ture", "TRUE ", "on"):
                if bad.strip().lower() == "true":  # case/space-insensitive 'true' is fine
                    continue
                with self.subTest(name=name, value=bad), self.assertRaises(dp.ConfigError):
                    dp.load_config(dict(ARCHIVE, **{name: bad}))

    def test_tables_must_be_known_and_delete_must_be_a_subset(self):
        with self.assertRaises(dp.ConfigError):
            dp.load_config({"DATA_PURGE_TABLES": "orders"})
        with self.assertRaises(dp.ConfigError):
            dp.load_config({"DATA_PURGE_DELETE_TABLES": "shopify_orders", "DATA_PURGE_TABLES": "raw_mongo_events"})

    def test_numbers_are_validated_and_configurable(self):
        cfg = dp.load_config({"DATA_PURGE_BATCH_SIZE": "250", "DATA_PURGE_SLEEP_BETWEEN_BATCHES_S": "2.5",
                              "DATA_PURGE_EXPORT_BATCH_SIZE": "10", "DATA_PURGE_MAX_WINDOW_ROWS": "5000"})
        self.assertEqual((cfg.delete_batch_size, cfg.sleep_between_batches_s, cfg.export_batch_size, cfg.max_window_rows),
                         (250, 2.5, 10, 5000))
        for name, bad in (("DATA_PURGE_BATCH_SIZE", "0"), ("DATA_PURGE_BATCH_SIZE", "abc"),
                          ("DATA_PURGE_SLEEP_BETWEEN_BATCHES_S", "-1"), ("DATA_PURGE_MAX_WINDOW_ROWS", "5")):
            with self.subTest(name=name, bad=bad), self.assertRaises(dp.ConfigError):
                dp.load_config({name: bad})

    def test_sse_options(self):
        self.assertEqual(dp.load_config({"DATA_PURGE_S3_SSE": "aws:kms", "DATA_PURGE_S3_KMS_KEY_ID": "k"}).kms_key_id, "k")
        with self.assertRaises(dp.ConfigError):
            dp.load_config({"DATA_PURGE_S3_SSE": "rot13"})
        with self.assertRaises(dp.ConfigError):
            dp.load_config({"DATA_PURGE_S3_KMS_KEY_ID": "k"})

    def test_no_combination_is_destructive_unless_everything_is_explicit(self):
        values = {"DATA_PURGE_ENABLED": (None, "true", "false"), "DATA_PURGE_DRY_RUN": (None, "true", "false"),
                  "DATA_PURGE_ARCHIVE_ONLY": (None, "true", "false"), "DATA_PURGE_DELETE_TABLES": (None, "raw_mongo_events")}
        for combo in itertools.product(*values.values()):
            env = {"DATA_PURGE_S3_BUCKET": "bkt"}
            env.update({k: v for k, v in zip(values, combo) if v is not None})
            with self.subTest(env=env):
                try:
                    cfg = dp.load_config(env)
                except dp.ConfigError:
                    continue  # fail closed
                destructive = cfg.mode is dp.Mode.FULL
                explicit = (env.get("DATA_PURGE_ENABLED") == "true" and env.get("DATA_PURGE_DRY_RUN") != "true"
                            and env.get("DATA_PURGE_ARCHIVE_ONLY") == "false" and bool(cfg.delete_tables))
                self.assertEqual(destructive, explicit)


# --------------------------------------------------------------------------------------
# Table configuration
# --------------------------------------------------------------------------------------
class TableConfigTests(unittest.TestCase):
    def test_each_table_uses_its_configured_date_column(self):
        got = {s.table: (s.date_column, s.date_kind, s.schema_confirmed) for s in dp.PURGE_TABLES}
        self.assertEqual(got, {
            "shopify_orders_update": ("updated_date", dp.DateKind.LOCAL_DATE, True),
            "mongo_events_summary": ("created_at_utc", dp.DateKind.UTC_DATETIME, False),
            "shopify_orders": ("created_date", dp.DateKind.LOCAL_DATE, True),
            "raw_mongo_events": ("created_at_ist", dp.DateKind.LOCAL_DATETIME, True),
            "product_sessions_snapshot": ("date", dp.DateKind.LOCAL_DATE, False),
            "mv_product_sessions_by_path_daily": (None, None, False),
            "mv_product_sessions_by_type_daily": (None, None, False),
        })

    def test_queries_for_each_table_reference_its_own_date_column(self):
        for table, col in (("shopify_orders", "created_date"), ("shopify_orders_update", "updated_date")):
            spec = dp.SPECS_BY_NAME[table]
            db = FakeDatabase([FakeTable(table, ["order_id", col], col,
                                         [{"order_id": "a", col: "2026-01-01"}, {"order_id": "b", col: "2026-09-01"}],
                                         types={"order_id": "varchar", col: "varchar"},
                                         indexes=[("idx_d", 1, [col])])])
            dp.count_eligible(FakeCursor(db), spec, "2026-04-01")
            self.assertTrue(any(f"`{col}` < %s" in sql for sql, _ in db.executed), table)

    def test_mv_tables_are_ambiguous_and_never_touch_the_database(self):
        db = FakeDatabase()
        for table in ("mv_product_sessions_by_path_daily", "mv_product_sessions_by_type_daily"):
            result, _ = run_table(db, FakeStore(), full_env(tables=table, DATA_PURGE_TABLES=table), table=table)
            self.assertEqual(result.status, dp.Status.SKIPPED_AMBIGUOUS)
        self.assertEqual(db.executed, [])

    def test_dry_run_describes_mv_tables_read_only(self):
        view = FakeTable("mv_product_sessions_by_path_daily", ["day", "sessions"], None, [],
                         types={"day": "date", "sessions": "bigint"}, table_type="VIEW")
        db = FakeDatabase([view])
        result, _ = run_table(db, None, {"DATA_PURGE_DRY_RUN": "true"}, table="mv_product_sessions_by_path_daily")
        self.assertEqual(result.status, dp.Status.SKIPPED_AMBIGUOUS)
        self.assertIn("VIEW", result.details["live_schema"])
        self.assertIn("day(date)", result.details["live_schema"])


# --------------------------------------------------------------------------------------
# Live validation
# --------------------------------------------------------------------------------------
class ValidationTests(unittest.TestCase):
    def _validate(self, table, spec_name="shopify_orders"):
        db = FakeDatabase([table] if table else [])
        cfg = dp.load_config({})
        spec = dp.SPECS_BY_NAME[spec_name]
        return dp.validate_table(FakeCursor(db), spec, IST, date(2026, 4, 1), cfg)

    def test_missing_table(self):
        v = self._validate(None)
        self.assertEqual((v.ok, v.status), (False, dp.Status.SKIPPED_MISSING))

    def test_view_is_not_purgeable(self):
        v = self._validate(orders_table(order_rows(), table_type="VIEW"))
        self.assertEqual((v.ok, v.status), (False, dp.Status.SKIPPED_VIEW))

    def test_wrong_date_column_type(self):
        v = self._validate(orders_table(order_rows(), types={"order_id": "varchar", "created_date": "int", "total": "double"}))
        self.assertEqual((v.ok, v.status), (False, dp.Status.SKIPPED_TYPE_MISMATCH))

    def test_missing_date_column(self):
        t = orders_table(order_rows())
        t.columns = ["order_id", "total"]
        self.assertEqual(self._validate(t).status, dp.Status.SKIPPED_TYPE_MISMATCH)

    def test_missing_date_index(self):
        v = self._validate(orders_table(order_rows(), indexes=[("idx_other", 1, ["order_id"])]))
        self.assertEqual((v.ok, v.status), (False, dp.Status.SKIPPED_NO_INDEX))

    def test_index_that_does_not_lead_with_the_date_column_is_rejected(self):
        v = self._validate(orders_table(order_rows(), indexes=[("idx_composite", 1, ["order_id", "created_date"])]))
        self.assertEqual(v.status, dp.Status.SKIPPED_NO_INDEX)

    def test_bad_plans_are_rejected(self):
        for plan in ([{"type": "ALL", "key": None, "rows": 9, "extra": ""}],
                     [{"type": "index", "key": "idx_so_created_date", "rows": 9, "extra": ""}],
                     [{"type": "range", "key": "idx_so_created_date", "rows": 9, "extra": "Using filesort"}],
                     [{"type": "range", "key": "some_other_index", "rows": 9, "extra": ""}],
                     []):
            with self.subTest(plan=plan):
                v = self._validate(orders_table(order_rows(), plan=plan))
                self.assertEqual((v.ok, v.status), (False, dp.Status.SKIPPED_BAD_PLAN))

    def test_explain_failure_fails_closed(self):
        v = self._validate(orders_table(order_rows(), plan="RAISE"))
        self.assertEqual(v.status, dp.Status.SKIPPED_BAD_PLAN)

    def test_orders_without_a_primary_key_use_date_windows_never_keyset(self):
        v = self._validate(orders_table(order_rows()))
        self.assertEqual((v.ok, v.strategy, v.primary_key), (True, "date_window", None))

    def test_raw_mongo_events_is_skipped_until_created_at_ist_is_indexed(self):
        only_utc_index = [("PRIMARY", 0, ["event_id"]), ("idx_created_at_utc", 1, ["created_at_utc"]),
                          ("idx_type_created", 1, ["event_type", "created_at_utc"])]
        v = self._validate(events_table(event_rows(), indexes=only_utc_index), "raw_mongo_events")
        self.assertEqual((v.ok, v.status), (False, dp.Status.SKIPPED_NO_INDEX))
        self.assertIn("created_at_ist", v.reason)

    def test_a_not_null_primary_key_allows_keyset(self):
        v = self._validate(events_table(event_rows()), "raw_mongo_events")
        self.assertEqual((v.ok, v.strategy, v.primary_key), (True, "keyset", ("event_id",)))

    def test_a_nullable_key_does_not_qualify_for_keyset(self):
        t = events_table(event_rows(), nullable={"event_id": True, "created_at_utc": True, "created_at_ist": True, "event_type": True})
        self.assertEqual(self._validate(t, "raw_mongo_events").strategy, "date_window")

    def test_columns_come_from_the_live_schema(self):
        v = self._validate(orders_table(order_rows()))
        self.assertEqual(v.columns, ["order_id", "created_date", "total"])


# --------------------------------------------------------------------------------------
# CSV export
# --------------------------------------------------------------------------------------
class CsvTests(unittest.TestCase):
    def test_header_all_columns_null_token_and_escaping(self):
        counters = dp._Counters()
        rows = iter([(1, None, "a,b"), (2, "", "\\N"), (3, b"\x01\xff", "line\nbreak"), (4, datetime(2026, 1, 2, 3, 4, 5), None)])
        data = b"".join(dp.csv_chunks(["id", "x", "y"], rows, counters))
        parsed = csv_rows(data)
        self.assertEqual(parsed[0], ["id", "x", "y"])
        self.assertEqual(parsed[1], ["1", "\\N", "a,b"])  # NULL is exactly \N
        self.assertEqual(parsed[2], ["2", "", "\\\\N"])  # '' stays '', a literal \N gets an extra backslash
        self.assertEqual(parsed[3], ["3", "01ff", "line\nbreak"])
        self.assertEqual(parsed[4], ["4", "2026-01-02 03:04:05", "\\N"])
        self.assertEqual((counters.rows, counters.bytes), (4, len(data)))
        self.assertEqual(counters.sha256.hexdigest(), hashlib.sha256(data).hexdigest())

    def test_generator_reader_is_lazy(self):
        pulled = []

        def chunks():
            for i in range(1000):
                pulled.append(i)
                yield b"x" * 1000

        reader = dp._GeneratorReader(chunks())
        self.assertEqual(len(reader.read(2500)), 2500)
        self.assertLessEqual(len(pulled), 3)  # only what was needed, never the whole stream
        self.assertEqual(len(reader.read(-1)), 1000 * 1000 - 2500)

    def test_keyset_export_is_streamed_in_batches_not_materialised(self):
        db = FakeDatabase([events_table(event_rows(n_old=2500, n_new=3))])
        cur = FakeCursor(db)
        spec = dp.SPECS_BY_NAME["raw_mongo_events"]
        cfg = dp.load_config({"DATA_PURGE_EXPORT_BATCH_SIZE": "100"})
        v = dp.validate_table(cur, spec, IST, date(2026, 4, 1), cfg)
        value = dp.cutoff_param(spec.date_kind, date(2026, 4, 1), IST)
        before = len(db.executed)

        def pages():
            return [s for s, _ in db.executed[before:] if s.startswith("SELECT `event_id`")]

        with mock.patch.object(dp, "CSV_CHUNK_BYTES", 2000):  # small chunks so laziness is visible on a small fixture
            rows = dp.iter_export_rows(cur, spec, v, IST, date(2026, 4, 1), value, cfg)
            stream = dp._GeneratorReader(dp.csv_chunks(v.columns, rows, dp._Counters()))
            head = stream.read(500)  # the first few rows only
            self.assertEqual(len(pages()), 1)  # one 100-row page was read, not the table
            data = head + stream.read(-1)
        self.assertEqual(len(csv_rows(data)) - 1, 2500)  # header + every eligible row
        self.assertEqual(len(pages()), 26)  # 25 full pages of 100 + the final empty page

    def test_keyset_pages_cover_every_row_exactly_once_in_order(self):
        rows = event_rows(n_old=250, n_new=10)
        db = FakeDatabase([events_table(rows)])
        cur = FakeCursor(db)
        spec = dp.SPECS_BY_NAME["raw_mongo_events"]
        cfg = dp.load_config({"DATA_PURGE_EXPORT_BATCH_SIZE": "37"})
        v = dp.validate_table(cur, spec, IST, date(2026, 4, 1), cfg)
        value = dp.cutoff_param(spec.date_kind, date(2026, 4, 1), IST)
        exported = list(dp.iter_export_rows(cur, spec, v, IST, date(2026, 4, 1), value, cfg))
        self.assertEqual(len(exported), 250)
        self.assertEqual(len({r[0] for r in exported}), 250)

    def test_date_window_export_covers_exactly_the_eligible_set(self):
        db = FakeDatabase([orders_table(order_rows())])
        cur = FakeCursor(db)
        spec = dp.SPECS_BY_NAME["shopify_orders"]
        cfg = dp.load_config({"DATA_PURGE_EXPORT_BATCH_SIZE": "7"})
        v = dp.validate_table(cur, spec, IST, date(2026, 4, 1), cfg)
        exported = list(dp.iter_export_rows(cur, spec, v, IST, date(2026, 4, 1), "2026-04-01", cfg))
        eligible = [r for r in order_rows() if r["created_date"] < "2026-04-01"]
        self.assertEqual(sorted(r[0] for r in exported), sorted(r["order_id"] for r in eligible))

    def test_unparseable_oldest_date_fails_closed(self):
        rows = order_rows() + [{"order_id": "bad", "created_date": "", "total": 0.0}]  # '' < '2026-04-01'
        db = FakeDatabase([orders_table(rows)])
        result, _ = run_table(db, FakeStore(), full_env(), table="shopify_orders")
        self.assertEqual(result.status, dp.Status.FAILED)
        self.assertIn("parseable", result.reason)
        self.assertEqual(db.deletes, [])

    def test_a_window_over_the_row_cap_fails_without_deleting(self):
        db = FakeDatabase([orders_table(order_rows())])
        env = full_env(DATA_PURGE_MAX_WINDOW_ROWS="1000")
        rows = [{"order_id": f"x{i}", "created_date": "2026-03-10", "total": 1.0} for i in range(1500)]
        db.tables["shopify_orders"].rows += rows
        result, _ = run_table(db, FakeStore(), env, table="shopify_orders")
        self.assertEqual(result.status, dp.Status.FAILED)
        self.assertIn("DATA_PURGE_MAX_WINDOW_ROWS", result.reason)
        self.assertEqual(db.deletes, [])


# --------------------------------------------------------------------------------------
# Archive protocol and the delete gate
# --------------------------------------------------------------------------------------
class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.events = []
        self.db = FakeDatabase([events_table(event_rows(n_old=30, n_new=5))], self.events)
        self.store = FakeStore(self.events)

    def keys(self, table="raw_mongo_events"):
        return dp.archive_keys(dp.load_config(ARCHIVE), "2026-10", "pts", table)

    def test_archive_only_archives_verifies_and_never_deletes(self):
        result, _ = run_table(self.db, self.store, ARCHIVE)
        csv_key, manifest_key, _ = self.keys()
        self.assertEqual(result.status, dp.Status.ARCHIVED_ONLY)
        self.assertEqual((result.eligible, result.exported, result.deleted, result.archived), (30, 30, 0, True))
        self.assertEqual(self.db.deletes, [])
        self.assertEqual(len(self.db.tables["raw_mongo_events"].rows), 35)
        rows = csv_rows(self.store.objects[csv_key])
        self.assertEqual(rows[0], ["event_id", "created_at_utc", "created_at_ist", "event_type"])
        self.assertEqual(len(rows) - 1, 30)

    def test_s3_path_is_deterministic_and_per_month_per_brand(self):
        self.assertEqual(self.keys(), (
            "data-purge/2026-10/pts/raw_mongo_events.csv",
            "data-purge/2026-10/pts/raw_mongo_events.manifest.json",
            "data-purge/2026-10/pts/raw_mongo_events.purge-complete.json",
        ))
        result, _ = run_table(self.db, self.store, dict(ARCHIVE, DATA_PURGE_S3_PREFIX="/custom/root/"))
        self.assertTrue(result.s3_key.startswith("custom/root/2026-10/pts/"))
        self.assertTrue(dp.archive_keys(dp.load_config(ARCHIVE), "2026-11", "pts", "x")[0].startswith("data-purge/2026-11/"))

    def test_manifest_records_the_archive_and_is_written_after_the_upload(self):
        run_table(self.db, self.store, ARCHIVE)
        csv_key, manifest_key, _ = self.keys()
        m = self.store.jsons[manifest_key]
        data = self.store.objects[csv_key]
        self.assertEqual((m["table"], m["brand"], m["run_month"], m["cutoff"]), ("raw_mongo_events", "pts", "2026-10", "2026-04-01"))
        self.assertEqual((m["row_count"], m["csv_bytes"], m["sha256"]), (30, len(data), hashlib.sha256(data).hexdigest()))
        self.assertEqual(m["columns"], ["event_id", "created_at_utc", "created_at_ist", "event_type"])
        self.assertEqual(m["date_column"], "created_at_ist")
        self.assertEqual(m["date_kind"], "local_datetime")
        names = [e[0] for e in self.events]
        self.assertLess(names.index("upload"), names.index("manifest"))
        self.assertEqual(self.store.tags[csv_key]["purge-sha256"], m["sha256"])

    def test_zero_eligible_rows_creates_nothing_and_deletes_nothing(self):
        db = FakeDatabase([events_table(event_rows(n_old=0, n_new=5))], self.events)
        result, _ = run_table(db, self.store, full_env())
        self.assertEqual(result.status, dp.Status.NO_DATA)
        self.assertEqual((self.store.upload_calls, self.store.jsons, db.deletes), ([], {}, []))

    def test_upload_failure_means_no_manifest_and_no_delete(self):
        self.store.fail_upload = True
        result, _ = run_table(self.db, self.store, full_env())
        self.assertEqual(result.status, dp.Status.FAILED)
        self.assertEqual((self.db.deletes, self.store.jsons, result.archived), ([], {}, False))
        self.assertEqual(len(self.db.tables["raw_mongo_events"].rows), 35)

    def test_s3_verification_failure_means_no_manifest_and_no_delete(self):
        self.store.truncate_on_upload = True  # object size != bytes written
        result, _ = run_table(self.db, self.store, full_env())
        self.assertEqual(result.status, dp.Status.FAILED)
        self.assertIn("verification", result.reason)
        self.assertEqual((self.db.deletes, self.store.jsons), ([], {}))

    def test_manifest_write_failure_means_no_delete(self):
        self.store.fail_manifest_write = True
        result, _ = run_table(self.db, self.store, full_env())
        self.assertEqual(result.status, dp.Status.FAILED)
        self.assertEqual(self.db.deletes, [])
        self.assertEqual(len(self.db.tables["raw_mongo_events"].rows), 35)

    def test_rows_changing_during_the_export_fail_the_sanity_check(self):
        def sneak_in_a_row():
            self.db.tables["raw_mongo_events"].rows.append(ev("late", datetime(2026, 3, 2, 1, 1)))

        self.store.on_upload = sneak_in_a_row
        result, _ = run_table(self.db, self.store, full_env())
        self.assertEqual(result.status, dp.Status.FAILED)
        self.assertIn("sanity check", result.reason)
        self.assertEqual((self.db.deletes, self.store.jsons), ([], {}))

    def test_tagging_failure_is_not_fatal(self):
        self.store.fail_tagging = True
        result, _ = run_table(self.db, self.store, ARCHIVE)
        self.assertEqual(result.status, dp.Status.ARCHIVED_ONLY)

    def test_unconfirmed_schemas_are_skipped_outside_dry_run(self):
        t = FakeTable("mongo_events_summary", ["event_id", "created_at_utc"], "created_at_utc", [],
                      types={"event_id": "varchar", "created_at_utc": "datetime"},
                      nullable={"event_id": False}, indexes=[("PRIMARY", 0, ["event_id"]), ("idx", 1, ["created_at_utc"])])
        db = FakeDatabase([t])
        result, _ = run_table(db, self.store, ARCHIVE, table="mongo_events_summary")
        self.assertEqual(result.status, dp.Status.SKIPPED_UNCONFIRMED)
        self.assertEqual(self.store.upload_calls, [])


class DeleteTests(unittest.TestCase):
    def setUp(self):
        self.events = []
        self.db = FakeDatabase([events_table(event_rows(n_old=2500, n_new=5))], self.events)
        self.store = FakeStore(self.events)

    def test_full_run_archives_first_then_deletes_in_committed_batches(self):
        with mock.patch.object(dp.time, "sleep") as sleep:
            result, _ = run_table(self.db, self.store, full_env(DATA_PURGE_SLEEP_BETWEEN_BATCHES_S="0.5"))
        self.assertEqual(result.status, dp.Status.SUCCESS, result.reason)
        self.assertEqual((result.eligible, result.exported, result.deleted), (2500, 2500, 2500))
        deletes = self.db.deletes
        self.assertEqual([d[2] for d in deletes], [1000, 1000, 500])  # LIMIT per batch
        self.assertEqual([d[3] for d in deletes], [1000, 1000, 500])
        names = [e[0] for e in self.events]
        first_delete = names.index("delete")
        self.assertLess(names.index("upload"), first_delete)
        self.assertLess(names.index("manifest"), first_delete)  # no delete before the manifest exists
        # a commit follows every delete batch
        self.assertEqual([names[i + 1] for i, n in enumerate(names) if n == "delete"], ["commit"] * 3)
        self.assertEqual(sleep.call_args_list, [mock.call(0.5)] * 3)
        self.assertEqual(len(self.db.tables["raw_mongo_events"].rows), 5)  # only the rows >= cutoff remain
        self.assertTrue(any(k.endswith("purge-complete.json") for k in self.store.jsons))

    def test_archive_only_mode_never_deletes_even_when_the_table_is_allowlisted(self):
        env = dict(ARCHIVE, DATA_PURGE_DELETE_TABLES="raw_mongo_events")  # ARCHIVE_ONLY defaults to true
        result, _ = run_table(self.db, self.store, env)
        self.assertEqual(result.status, dp.Status.ARCHIVED_ONLY)
        self.assertEqual(self.db.deletes, [])

    def test_purge_test_exports_to_s3_but_never_deletes_even_in_a_full_config(self):
        result, run = run_table(self.db, self.store, full_env(PURGE_TEST="true"))
        csv_key, manifest_key, _ = dp.archive_keys(run.cfg, "2026-10", "pts", "raw_mongo_events")
        self.assertEqual(result.status, dp.Status.ARCHIVED_ONLY, result.reason)
        self.assertEqual((result.archived, result.exported, result.deleted), (True, 2500, 0))
        self.assertIn(csv_key, self.store.objects)  # the CSV is in S3...
        self.assertEqual(len(csv_rows(self.store.objects[csv_key])) - 1, 2500)
        self.assertIn(manifest_key, self.store.jsons)  # ...with its manifest
        self.assertEqual(self.db.deletes, [])  # ...and no DELETE was ever issued
        self.assertEqual(len(self.db.tables["raw_mongo_events"].rows), 2505)

    def test_purge_test_blocks_deletion_even_if_the_mode_were_full(self):
        # defence in depth: build a FULL run context and flip purge_test on it directly
        run = make_run(full_env(), self.store)
        object.__setattr__(run.cfg, "purge_test", True)
        result = dp.process_table(run, brand_for(self.db), dp.SPECS_BY_NAME["raw_mongo_events"])
        self.assertEqual(result.status, dp.Status.ARCHIVED_ONLY)
        self.assertIn("PURGE_TEST", result.reason)
        self.assertEqual(self.db.deletes, [])

    def test_purge_test_then_a_real_run_reuses_the_verified_archive_and_deletes(self):
        run_table(self.db, self.store, full_env(PURGE_TEST="true"))
        uploads = len(self.store.upload_calls)
        result, _ = run_table(self.db, self.store, full_env())  # PURGE_TEST turned off later in the same month
        self.assertEqual(result.status, dp.Status.SUCCESS, result.reason)
        self.assertEqual(len(self.store.upload_calls), uploads)  # the test export was reused, not redone
        self.assertEqual(result.deleted, 2500)

    def test_full_mode_still_only_deletes_allowlisted_tables(self):
        result, _ = run_table(self.db, self.store, full_env(tables="shopify_orders"), table="raw_mongo_events",)
        self.assertEqual(result.status, dp.Status.ARCHIVED_ONLY)
        self.assertIn("DATA_PURGE_DELETE_TABLES", result.reason)
        self.assertEqual(self.db.deletes, [])

    def test_a_failing_delete_batch_leaves_a_verified_archive_and_is_retryable(self):
        self.db.fail_delete_on_call = 2
        result, _ = run_table(self.db, self.store, full_env())
        self.assertEqual((result.status, result.error, result.archived), (dp.Status.ARCHIVED_ONLY, True, True))
        self.assertEqual(len(self.db.tables["raw_mongo_events"].rows), 2505 - 1000)  # batch 1 committed
        uploads_before = len(self.store.upload_calls)

        # retry: the manifest is authoritative, nothing is re-exported, the remainder is deleted
        self.db.fail_delete_on_call = None
        result2, _ = run_table(self.db, self.store, full_env())
        self.assertEqual(result2.status, dp.Status.SUCCESS, result2.reason)
        self.assertEqual(len(self.store.upload_calls), uploads_before)  # no new upload
        self.assertEqual(result2.deleted, 1500)
        self.assertEqual(len(self.db.tables["raw_mongo_events"].rows), 5)

    def test_once_purged_a_rerun_is_a_verified_no_op(self):
        run_table(self.db, self.store, full_env())
        uploads = len(self.store.upload_calls)
        deletes = len(self.db.deletes)
        result, _ = run_table(self.db, self.store, full_env())
        self.assertEqual(result.status, dp.Status.SUCCESS)
        self.assertIn("already purged", result.reason)
        self.assertEqual((len(self.store.upload_calls), len(self.db.deletes)), (uploads, deletes))

    def test_more_eligible_rows_than_were_archived_refuses_to_delete(self):
        run_table(self.db, self.store, dict(ARCHIVE))  # archive only
        self.db.tables["raw_mongo_events"].rows.append(ev("appeared-later", datetime(2026, 2, 1, 1, 1)))
        result, _ = run_table(self.db, self.store, full_env())
        self.assertEqual(result.status, dp.Status.FAILED)
        self.assertIn("only 2500 were archived", result.reason)
        self.assertEqual(self.db.deletes, [])

    def test_delete_is_capped_at_the_archived_row_count(self):
        run_table(self.db, self.store, dict(ARCHIVE))
        cfg = dp.load_config(full_env())
        spec = dp.SPECS_BY_NAME["raw_mongo_events"]
        value = dp.cutoff_param(spec.date_kind, date(2026, 4, 1), IST)
        deleted = dp.delete_in_batches(FakeCursor(self.db), self.db.conn, spec, value, 1200, cfg, "pts")
        self.assertEqual(deleted, 1200)
        self.assertEqual([d[2] for d in self.db.deletes], [1000, 200])

    def test_refuses_to_empty_a_table(self):
        db = FakeDatabase([events_table(event_rows(n_old=20, n_new=0))], self.events)
        result, _ = run_table(db, self.store, full_env())
        self.assertEqual(result.status, dp.Status.ARCHIVED_ONLY)
        self.assertIn("empty the table", result.reason)
        self.assertEqual(db.deletes, [])

    def test_a_null_date_row_counts_as_a_row_that_stays(self):
        rows = event_rows(n_old=20, n_new=0) + [ev("nodate", None)]
        db = FakeDatabase([events_table(rows)], self.events)
        result, _ = run_table(db, self.store, full_env())
        self.assertEqual(result.status, dp.Status.SUCCESS, result.reason)
        self.assertEqual([r["event_id"] for r in db.tables["raw_mongo_events"].rows], ["nodate"])


class ExistingArchiveTests(unittest.TestCase):
    def setUp(self):
        self.events = []
        self.db = FakeDatabase([events_table(event_rows(n_old=40, n_new=5))], self.events)
        self.store = FakeStore(self.events)
        run_table(self.db, self.store, ARCHIVE)  # a complete archive + manifest
        self.csv_key, self.manifest_key, _ = dp.archive_keys(dp.load_config(ARCHIVE), "2026-10", "pts", "raw_mongo_events")
        self.uploads = len(self.store.upload_calls)

    def _rerun(self, env=None):
        return run_table(self.db, self.store, env or full_env())[0]

    def _assert_untouched(self, result):
        self.assertEqual(result.status, dp.Status.FAILED)
        self.assertEqual(self.db.deletes, [])
        self.assertEqual(len(self.store.upload_calls), self.uploads)  # never overwritten

    def test_a_valid_manifest_is_reused_without_re_exporting(self):
        result = self._rerun()
        self.assertEqual(result.status, dp.Status.SUCCESS, result.reason)
        self.assertEqual(len(self.store.upload_calls), self.uploads)

    def test_mismatched_manifests_are_rejected_and_never_overwritten(self):
        original = json.dumps(self.store.jsons[self.manifest_key], sort_keys=True)
        for field, value in (("table", "shopify_orders"), ("cutoff", "2026-05-01"), ("brand", "other"),
                             ("run_month", "2026-11"), ("date_column", "created_date"), ("schema_version", 99),
                             ("sha256", "short"), ("row_count", 0), ("csv_bytes", -1),
                             ("columns", ["event_id"]), ("csv_key", "elsewhere.csv")):
            with self.subTest(field=field):
                self.store.jsons[self.manifest_key] = json.loads(original)
                self.store.jsons[self.manifest_key][field] = value
                result = self._rerun()
                self._assert_untouched(result)
                self.assertIn("existing archive invalid", result.reason)
                self.assertEqual(self.store.jsons[self.manifest_key][field], value)  # not rewritten

    def test_schema_change_since_the_archive_blocks_deletion(self):
        self.db.tables["raw_mongo_events"].columns.append("new_col")
        result = self._rerun()
        self._assert_untouched(result)
        self.assertIn("columns changed", result.reason)

    def test_csv_that_does_not_match_the_manifest_blocks_deletion(self):
        self.store.objects[self.csv_key] = self.store.objects[self.csv_key][:-3]
        self._assert_untouched(self._rerun())

    def test_missing_csv_with_a_manifest_blocks_deletion(self):
        del self.store.objects[self.csv_key]
        self._assert_untouched(self._rerun())

    def test_sha_tag_that_disagrees_with_the_manifest_blocks_deletion(self):
        self.store.tags[self.csv_key]["purge-sha256"] = "0" * 64
        self._assert_untouched(self._rerun())

    def test_a_manifest_that_appears_concurrently_is_validated_not_overwritten(self):
        db = FakeDatabase([events_table(event_rows(n_old=10, n_new=2))], [])
        store = FakeStore()
        csv_key, manifest_key, _ = dp.archive_keys(dp.load_config(ARCHIVE), "2026-10", "pts", "raw_mongo_events")

        def other_process_writes_a_bad_manifest():
            store.jsons[manifest_key] = {"table": "x"}

        store.on_upload = other_process_writes_a_bad_manifest
        result, _ = run_table(db, store, full_env())
        self.assertEqual(result.status, dp.Status.FAILED)
        self.assertEqual(db.deletes, [])
        self.assertEqual(store.jsons[manifest_key], {"table": "x"})


class OrphanCsvTests(unittest.TestCase):
    """A CSV exists but there is no manifest (crash between upload and manifest)."""

    def setUp(self):
        self.events = []
        self.db = FakeDatabase([events_table(event_rows(n_old=40, n_new=5))], self.events)
        self.store = FakeStore(self.events)
        run_table(self.db, self.store, ARCHIVE)
        self.csv_key, self.manifest_key, _ = dp.archive_keys(dp.load_config(ARCHIVE), "2026-10", "pts", "raw_mongo_events")
        del self.store.jsons[self.manifest_key]  # crash before the manifest
        self.uploads = len(self.store.upload_calls)

    def test_positively_confirmed_orphan_is_reused_and_gets_its_manifest(self):
        result, _ = run_table(self.db, self.store, ARCHIVE)
        self.assertEqual(result.status, dp.Status.ARCHIVED_ONLY)
        self.assertEqual(len(self.store.upload_calls), self.uploads)  # reused, not re-uploaded
        self.assertEqual(self.store.jsons[self.manifest_key]["row_count"], 40)

    def test_orphan_without_completion_tags_is_replaced(self):
        self.store.tags.pop(self.csv_key)
        result, _ = run_table(self.db, self.store, ARCHIVE)
        self.assertEqual(result.status, dp.Status.ARCHIVED_ONLY)
        self.assertEqual(len(self.store.upload_calls), self.uploads + 1)
        self.assertIn(self.manifest_key, self.store.jsons)

    def test_orphan_with_a_different_row_count_cutoff_size_or_header_is_replaced(self):
        for mutate in (
            lambda: self.store.tags[self.csv_key].update({"purge-rows": "39"}),
            lambda: self.store.tags[self.csv_key].update({"purge-cutoff": "2026-03-01"}),
            lambda: self.store.tags[self.csv_key].update({"purge-bytes": "5"}),
            lambda: self.store.tags[self.csv_key].update({"purge-status": "partial"}),
            lambda: self.store.objects.__setitem__(self.csv_key, b"other,header\n" + self.store.objects[self.csv_key].split(b"\n", 1)[1]),
        ):
            with self.subTest():
                self.setUp()
                mutate()
                before = len(self.store.upload_calls)
                result, _ = run_table(self.db, self.store, ARCHIVE)
                self.assertEqual(result.status, dp.Status.ARCHIVED_ONLY, result.reason)
                self.assertEqual(len(self.store.upload_calls), before + 1)

    def test_a_replaced_orphan_never_leads_to_deletion_unless_it_is_re_verified(self):
        self.store.tags.pop(self.csv_key)
        self.store.truncate_on_upload = True  # the replacement upload is corrupt
        result, _ = run_table(self.db, self.store, full_env())
        self.assertEqual(result.status, dp.Status.FAILED)
        self.assertEqual(self.db.deletes, [])


# --------------------------------------------------------------------------------------
# Preflight guards, locks, dry run, isolation, summary
# --------------------------------------------------------------------------------------
class GuardTests(unittest.TestCase):
    def test_env_flags_block_only_the_tables_they_affect(self):
        cur = FakeCursor(FakeDatabase())
        self.assertIn("BACKFILL_MODE", dp.check_conflicts(cur, "shopify_orders", {"BACKFILL_MODE": "true"}))
        self.assertIn("BACKFILL_MODE", dp.check_conflicts(cur, "shopify_orders_update", {"BACKFILL_MODE": "true"}))
        self.assertIsNone(dp.check_conflicts(cur, "raw_mongo_events", {"BACKFILL_MODE": "true"}))
        self.assertIn("MONGO_EVENTS_LEGACY_COPY", dp.check_conflicts(cur, "raw_mongo_events", {"MONGO_EVENTS_LEGACY_COPY": "true"}))
        self.assertIsNone(dp.check_conflicts(cur, "shopify_orders", {"BACKFILL_MODE": "false"}))

    def test_processlist_detects_a_running_write_on_the_table_but_not_selects_or_other_dbs(self):
        db = FakeDatabase()
        cur = FakeCursor(db)
        db.processlist = [
            {"id": 7, "db": "brand_db", "info": "SELECT * FROM shopify_orders"},
            {"id": 8, "db": "other_db", "info": "DELETE FROM shopify_orders WHERE created_date < 'x'"},
            {"id": 1, "db": "brand_db", "info": "DELETE FROM shopify_orders WHERE 1"},  # our own session
            {"id": 9, "db": "brand_db", "info": None},
            {"id": 10, "db": "brand_db", "info": "INSERT INTO shopify_orders_update VALUES (1)"},
        ]
        self.assertIsNone(dp.check_conflicts(cur, "shopify_orders", {}))
        db.processlist.append({"id": 11, "db": "brand_db", "info": "delete from `shopify_orders` where created_date = 'secret'"})
        reason = dp.check_conflicts(cur, "shopify_orders", {})
        self.assertIn("DELETE", reason)
        self.assertNotIn("secret", reason)  # statements are never echoed

    def test_a_conflict_skips_the_table_without_touching_s3_or_the_data(self):
        db = FakeDatabase([orders_table(order_rows())])
        store = FakeStore()
        result, _ = run_table(db, store, full_env(), table="shopify_orders", proc_env={"BACKFILL_MODE": "true"})
        self.assertEqual(result.status, dp.Status.SKIPPED_CONFLICT)
        self.assertEqual((store.upload_calls, db.deletes), ([], []))

    def test_a_held_lock_skips_the_table(self):
        db = FakeDatabase([events_table(event_rows())])
        db.lock_result = 0
        result, _ = run_table(db, FakeStore(), full_env())
        self.assertEqual(result.status, dp.Status.SKIPPED_LOCKED)
        self.assertEqual(db.deletes, [])

    def test_the_lock_is_released_even_when_the_table_fails(self):
        db = FakeDatabase([events_table(event_rows())])
        store = FakeStore()
        store.fail_upload = True
        run_table(db, store, full_env())
        self.assertTrue(any(s.startswith("SELECT RELEASE_LOCK") for s, _ in db.executed))


class DryRunTests(unittest.TestCase):
    ENV = {"DATA_PURGE_DRY_RUN": "true"}

    def test_dry_run_reports_and_changes_nothing_and_needs_no_store(self):
        db = FakeDatabase([orders_table(order_rows())])
        before = [dict(r) for r in db.tables["shopify_orders"].rows]
        result, _ = run_table(db, None, self.ENV, table="shopify_orders")
        self.assertEqual(result.status, dp.Status.DRY_RUN)
        self.assertEqual((result.eligible, result.eligible_kind), (31, "exact"))
        self.assertEqual(db.tables["shopify_orders"].rows, before)
        self.assertEqual(db.deletes, [])
        d = result.details
        self.assertEqual((d["date_column"], d["date_type"], d["index"], d["strategy"]), ("created_date", "varchar", "idx_so_created_date", "date_window"))
        self.assertIsNone(d["unique_key"])
        self.assertTrue(d["archiveable"])
        self.assertFalse(d["deletion_supported"])  # disabled by default
        self.assertEqual(d["oldest_eligible"], "2026-03-01")
        self.assertEqual(d["size_bytes"], 1500)

    def test_dry_run_reports_purge_test_as_export_only(self):
        db = FakeDatabase([events_table(event_rows())])
        env = dict(self.ENV, DATA_PURGE_ENABLED="true", DATA_PURGE_ARCHIVE_ONLY="false",
                   DATA_PURGE_DELETE_TABLES="raw_mongo_events", PURGE_TEST="true")
        result, run = run_table(db, None, env)
        self.assertFalse(result.details["deletion_supported"])
        self.assertIn("PURGE_TEST", result.details["deletion_reason"])
        self.assertIn("PURGE_TEST=true", dp.format_summary([result], run, 0.1))

    def test_dry_run_reports_whether_deletion_would_be_supported(self):
        db = FakeDatabase([events_table(event_rows())])
        env = dict(self.ENV, DATA_PURGE_ENABLED="true", DATA_PURGE_ARCHIVE_ONLY="false", DATA_PURGE_DELETE_TABLES="raw_mongo_events")
        result, _ = run_table(db, None, env)
        self.assertTrue(result.details["deletion_supported"])
        result2, _ = run_table(db, None, dict(self.ENV, DATA_PURGE_ENABLED="true"))
        self.assertFalse(result2.details["deletion_supported"])
        self.assertIn("ARCHIVE_ONLY", result2.details["deletion_reason"])

    def test_expensive_exact_count_falls_back_to_the_explain_estimate(self):
        db = FakeDatabase([orders_table(order_rows())])
        db.count_hint_fails = True
        result, _ = run_table(db, None, self.ENV, table="shopify_orders")
        self.assertEqual(result.eligible_kind, "estimated")
        self.assertEqual(result.eligible, len(db.tables["shopify_orders"].rows))

    def test_dry_run_reports_unconfirmed_tables_and_skip_reasons(self):
        t = FakeTable("mongo_events_summary", ["event_id", "created_at_utc"], "created_at_utc",
                      [{"event_id": "a", "created_at_utc": datetime(2026, 1, 1)}],
                      types={"event_id": "varchar", "created_at_utc": "datetime"}, nullable={"event_id": False},
                      indexes=[("PRIMARY", 0, ["event_id"]), ("idx", 1, ["created_at_utc"])])
        db = FakeDatabase([t])
        result, _ = run_table(db, None, self.ENV, table="mongo_events_summary")
        self.assertEqual(result.status, dp.Status.DRY_RUN)
        self.assertFalse(result.details["archiveable"])
        self.assertIn("not confirmed", result.details["archiveable_reason"])
        missing, _ = run_table(FakeDatabase(), None, self.ENV, table="shopify_orders")
        self.assertEqual(missing.status, dp.Status.SKIPPED_MISSING)
        no_index, _ = run_table(FakeDatabase([orders_table(order_rows(), indexes=[])]), None, self.ENV, table="shopify_orders")
        self.assertEqual(no_index.status, dp.Status.SKIPPED_NO_INDEX)


class RunIsolationAndSummaryTests(unittest.TestCase):
    def test_a_failure_in_one_table_or_brand_does_not_stop_the_others(self):
        events = []
        dbs = {
            "pts": FakeDatabase([events_table(event_rows(5, 3)), orders_table(order_rows())], events),
            "bbb": FakeDatabase([events_table(event_rows(5, 3)), orders_table(order_rows())], events),
        }
        store = FakeStore(events)
        store.fail_keys_containing = "pts/shopify_orders"
        run = make_run(full_env(), store)
        brands = [brand_for(dbs["pts"], "pts"), brand_for(dbs["bbb"], "bbb")]
        results = dp.run_purge(brands, run)
        by = {(r.brand, r.table): r for r in results}
        self.assertEqual(by[("pts", "shopify_orders")].status, dp.Status.FAILED)
        self.assertEqual(by[("pts", "raw_mongo_events")].status, dp.Status.SUCCESS)
        self.assertEqual(by[("bbb", "shopify_orders")].status, dp.Status.SUCCESS)
        self.assertEqual(by[("bbb", "raw_mongo_events")].status, dp.Status.SUCCESS)
        self.assertEqual(len(results), 14)
        self.assertTrue(dp.any_failure(results))
        # the failed table kept every row
        self.assertEqual(len(dbs["pts"].tables["shopify_orders"].rows), len(order_rows()))

    def test_tables_outside_the_archive_allowlist_are_skipped(self):
        db = FakeDatabase([events_table(event_rows()), orders_table(order_rows())])
        env = dict(ARCHIVE, DATA_PURGE_TABLES="raw_mongo_events")
        results = dp.run_purge([brand_for(db)], make_run(env, FakeStore()))
        by = {r.table: r for r in results}
        self.assertEqual(by["shopify_orders"].status, dp.Status.SKIPPED)
        self.assertEqual(by["raw_mongo_events"].status, dp.Status.ARCHIVED_ONLY)

    def test_summary_lists_each_table_and_totals(self):
        events = []
        db = FakeDatabase([events_table(event_rows(30, 5))], events)
        run = make_run(full_env(), FakeStore(events))
        results = dp.run_purge([brand_for(db)], run)
        text = dp.format_summary(results, run, 12.3)
        for needle in ("DATA PURGE SUMMARY", "Cutoff: 2026-04-01", "s3://bkt/data-purge/2026-10/", "pts / raw_mongo_events",
                       "eligible: 30", "exported: 30", "archived: YES", "deleted: 30", "status: SUCCESS",
                       "Total rows archived: 30", "Total rows deleted: 30", "Duration: 12.3s", "SKIPPED_AMBIGUOUS"):
            self.assertIn(needle, text)

    def test_row_values_never_appear_in_logs(self):
        events = []
        rows = event_rows(5, 2)
        rows[0]["event_type"] = "SECRET-PAYLOAD-XYZ"
        db = FakeDatabase([events_table(rows)], events)
        with self.assertLogs("data_purge", level="DEBUG") as logs:
            run_table(db, FakeStore(events), full_env())
        self.assertNotIn("SECRET-PAYLOAD-XYZ", "\n".join(logs.output))


class S3StoreTests(unittest.TestCase):
    """The real boto3 wrapper, against a stub client (boto3 itself is never imported)."""

    class ClientError(Exception):
        def __init__(self, code):
            self.response = {"Error": {"Code": code}}

    def test_head_translates_not_found_to_none(self):
        client = mock.Mock()
        client.head_object.side_effect = self.ClientError("404")
        self.assertIsNone(dp.S3ArchiveStore("b", client=client).head("k"))
        client.head_object.side_effect = self.ClientError("AccessDenied")
        with self.assertRaises(self.ClientError):
            dp.S3ArchiveStore("b", client=client).head("k")
        client.head_object.side_effect = None
        client.head_object.return_value = {"ContentLength": 42}
        self.assertEqual(dp.S3ArchiveStore("b", client=client).head("k").size, 42)

    def test_manifest_put_is_conditional_and_reports_an_existing_object(self):
        client = mock.Mock()
        store = dp.S3ArchiveStore("b", client=client, sse="aws:kms", kms_key_id="kid")
        self.assertTrue(store.put_json_if_absent("k", {"a": 1}))
        kwargs = client.put_object.call_args.kwargs
        self.assertEqual(kwargs["IfNoneMatch"], "*")
        self.assertEqual((kwargs["ServerSideEncryption"], kwargs["SSEKMSKeyId"]), ("aws:kms", "kid"))
        client.put_object.side_effect = self.ClientError("PreconditionFailed")
        self.assertFalse(store.put_json_if_absent("k", {"a": 1}))
        client.put_object.side_effect = self.ClientError("AccessDenied")
        with self.assertRaises(self.ClientError):
            store.put_json_if_absent("k", {"a": 1})

    def test_get_json_and_tags(self):
        client = mock.Mock()
        client.get_object.return_value = {"Body": io.BytesIO(b'{"x": 1}')}
        store = dp.S3ArchiveStore("b", client=client)
        self.assertEqual(store.get_json("k"), {"x": 1})
        client.get_object.side_effect = self.ClientError("NoSuchKey")
        self.assertIsNone(store.get_json("k"))
        client.get_object_tagging.return_value = {"TagSet": [{"Key": "a", "Value": "1"}]}
        self.assertEqual(store.get_tags("k"), {"a": "1"})

    def test_boto3_is_only_imported_when_a_real_client_is_built(self):
        self.assertNotIn("boto3", vars(dp))  # no module-level import: importing data_purge needs no AWS SDK
        self.assertTrue(hasattr(dp, "S3ArchiveStore"))


if __name__ == "__main__":
    unittest.main()
