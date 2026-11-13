import json
import re
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest import mock
from zoneinfo import ZoneInfo

# Safe import order (see test_mongo_events.py's comment): aws_background
# first, then the pipeline module - otherwise, run in isolation, this starts
# a circular partial-init (aws_background.py re-exports from
# pipeline.mongo_events, which itself does `import aws_background`).
import aws_background  # noqa: F401
from pipeline import mongo_events_raw as raw

IST = ZoneInfo("Asia/Kolkata")


class FakeConnection:
    def __init__(self):
        self.commit_count = 0

    def commit(self):
        self.commit_count += 1


def _fixed_show_columns_rows():
    rows = [(name, "text") for name in raw._FIXED_COLUMNS]
    rows.append(("ingested_at", "datetime(6)"))
    return rows


class FakeCursor:
    """Answers SHOW COLUMNS from a mutable column list (updated by ALTERs, like
    a real table), simulates MySQL's row-size limit via max_columns, and
    records everything else."""

    def __init__(self, show_columns_rows=None, max_columns=None, legacy_columns=None, legacy_minmax=None):
        self.columns = list(show_columns_rows if show_columns_rows is not None else _fixed_show_columns_rows())
        self.max_columns = max_columns
        self.legacy_columns = legacy_columns or []
        self.legacy_minmax = legacy_minmax
        self.executed = []
        self.executemany_calls = []
        self.insert_ignore_calls = []
        self.rowcount = 0
        self._result = []

    def execute(self, sql, params=None):
        self.executed.append(sql)
        self._result = []
        if sql.startswith("SHOW COLUMNS"):
            self._result = list(self.columns)
        elif sql.startswith("ALTER TABLE"):
            names = re.findall(r"ADD COLUMN `([^`]+)` MEDIUMTEXT", sql)
            if self.max_columns is not None and len(self.columns) + len(names) > self.max_columns:
                raise RuntimeError("1118 (42000): Row size too large. The maximum row size is 8126")
            for name in names:
                self.columns.append((name, "mediumtext"))
        elif "information_schema.columns" in sql:
            self._result = [(c,) for c in self.legacy_columns]
        elif sql.startswith("SELECT MIN(created_at_utc)"):
            self._result = [self.legacy_minmax] if self.legacy_minmax else [(None, None)]
        elif sql.startswith("INSERT IGNORE"):
            self.insert_ignore_calls.append((sql, params))
            self.rowcount = 10

    def executemany(self, sql, rows):
        self.executemany_calls.append((sql, list(rows)))

    def fetchall(self):
        return list(self._result)

    def fetchone(self):
        return self._result[0] if self._result else None

    @property
    def alters(self):
        return [s for s in self.executed if s.startswith("ALTER TABLE")]


SAMPLE_DOC = {
    "_id": "6ab661822f0aa0dcf5825493",
    "event_id": "54f60071-6f0a-4177-aff7-f0d4edd72c45",
    "idempotency_key": "c535c3e7e68773407f1aa66868513a57e499241b709d22a9d3a8bd36e1b6b8be",
    "event_type": "TAB_Click",
    "session_id": "ce226a1e-8f86-407e-8af6-8164f13719ee",
    "variantId": None,
    "shop_name": "shyle",
    "cart_token": "hWNHEoB5e8aKHXEk6vndVETk?key=baf9ecb2b0758bb11422ed3db431f883",
    "user_agent": "Mozilla/5.0",
    "url": None,
    "data": {"collection": "bangles", "click": "https://www.shyle.in/collections/bracelets"},
    "createdAt": datetime(2026, 9, 25, 11, 56, 50, 100000, tzinfo=timezone.utc),
    "updatedAt": datetime(2026, 9, 25, 11, 56, 50, 100000, tzinfo=timezone.utc),
    "__v": 0,
}

PURCHASE_DOC = dict(
    SAMPLE_DOC,
    event_id="69c9e404-43e7-43d1-beb9-793e20511c30",
    event_type="purchase_TECHIT",
    data={
        "custom_data": {
            "order_name": "#627813",
            "order_type": "cod",
            "isPpcod": False,
            "customer_id": "",
            "merchant_id": "",
            "cod_charges": "",
        },
        "referrer": "https://www.google.com/",
        "page": "/collections/gifting/",
        "session": "f99bd1f3-d873-46c8-ae76-772a0dbf022c",
    },
)


class FlattenAndSanitizeTests(unittest.TestCase):
    def test_flatten_nested_dicts_recursively_and_keeps_arrays(self):
        flat = raw._flatten({"a": 1, "data": {"click": "x", "deep": {"k": 2}}, "tags": [1, 2]})
        self.assertEqual(flat, {"a": 1, "data_click": "x", "data_deep_k": 2, "tags": [1, 2]})

    def test_flatten_stops_at_max_depth_keeping_remaining_object(self):
        doc = {"l1": {"l2": {"l3": {"l4": {"l5": {"l6": {"l7": 1}}}}}}}
        flat = raw._flatten(doc)
        self.assertEqual(len(flat), 1)
        self.assertIsInstance(next(iter(flat.values())), dict)

    def test_sanitize_keeps_mongo_key_as_is(self):
        self.assertEqual(raw._sanitize_column_name("variantId"), "variantId")
        self.assertEqual(raw._sanitize_column_name("__v"), "__v")

    def test_sanitize_replaces_unsafe_characters(self):
        self.assertEqual(raw._sanitize_column_name("a-b.c d"), "a_b_c_d")

    def test_sanitize_truncates_long_names_uniquely(self):
        a = raw._sanitize_column_name("x" * 100 + "a")
        b = raw._sanitize_column_name("x" * 100 + "b")
        self.assertLessEqual(len(a), 64)
        self.assertLessEqual(len(b), 64)
        self.assertNotEqual(a, b)


class BuildRecordTests(unittest.TestCase):
    def test_sample_document_becomes_expected_record(self):
        record = raw._build_record(dict(SAMPLE_DOC))

        self.assertEqual(record["event_id"], SAMPLE_DOC["event_id"])
        self.assertEqual(record["event_type"], "TAB_Click")
        self.assertEqual(record["data_collection"], "bangles")
        self.assertEqual(record["data_click"], "https://www.shyle.in/collections/bracelets")
        self.assertEqual(record["__v"], 0)
        self.assertIsNone(record["variantId"])

    def test_excluded_keys_are_not_stored(self):
        record = raw._build_record(dict(SAMPLE_DOC))
        for key in ("_id", "idempotency_key", "createdAt", "updatedAt", "data"):
            self.assertNotIn(key, record)

    def test_timestamps_stored_in_both_utc_and_ist(self):
        record = raw._build_record(dict(SAMPLE_DOC))

        self.assertEqual(record["created_at_utc"], datetime(2026, 9, 25, 11, 56, 50, 100000))
        self.assertEqual(record["created_at_ist"], datetime(2026, 9, 25, 17, 26, 50, 100000))
        self.assertEqual(record["updated_at_utc"], datetime(2026, 9, 25, 11, 56, 50, 100000))
        self.assertEqual(record["updated_at_ist"], datetime(2026, 9, 25, 17, 26, 50, 100000))
        self.assertIsNone(record["created_at_utc"].tzinfo)

    def test_naive_datetimes_are_treated_as_utc(self):
        doc = dict(SAMPLE_DOC, createdAt=datetime(2026, 9, 25, 0, 0), updatedAt=datetime(2026, 9, 25, 0, 0))
        record = raw._build_record(doc)
        self.assertEqual(record["created_at_ist"], datetime(2026, 9, 25, 5, 30))

    def test_missing_updated_at_gives_null_updated_columns(self):
        doc = {k: v for k, v in SAMPLE_DOC.items() if k != "updatedAt"}
        record = raw._build_record(doc)
        self.assertIsNone(record["updated_at_utc"])
        self.assertIsNone(record["updated_at_ist"])

    def test_document_without_event_id_or_created_at_is_skipped(self):
        self.assertIsNone(raw._build_record({k: v for k, v in SAMPLE_DOC.items() if k != "event_id"}))
        self.assertIsNone(raw._build_record({k: v for k, v in SAMPLE_DOC.items() if k != "createdAt"}))


class ToTextTests(unittest.TestCase):
    def test_every_value_type_becomes_text_and_nothing_is_rejected(self):
        self.assertIsNone(raw._to_text(None))
        self.assertEqual(raw._to_text(""), "")
        self.assertEqual(raw._to_text("abc"), "abc")
        self.assertEqual(raw._to_text(True), "true")
        self.assertEqual(raw._to_text(False), "false")
        self.assertEqual(raw._to_text(7), "7")
        self.assertEqual(raw._to_text(299.5), "299.5")
        self.assertEqual(raw._to_text([1, "a"]), '[1, "a"]')
        self.assertEqual(json.loads(raw._to_text({"k": datetime(2026, 1, 1)})), {"k": "2026-01-01T00:00:00"})
        self.assertEqual(raw._to_text(datetime(2026, 1, 1)), "2026-01-01T00:00:00")


class ColumnCatalogTests(unittest.TestCase):
    def test_lookup_is_case_insensitive_and_returns_existing_name(self):
        catalog = raw._ColumnCatalog(FakeCursor(), FakeConnection())
        self.assertEqual(catalog.get("VARIANTID"), "variantId")
        self.assertIsNone(catalog.get("nope"))

    def test_dict_cursor_rows_are_supported(self):
        cursor = FakeCursor([{"Field": "event_id", "Type": "varchar(255)"}])
        self.assertEqual(raw._ColumnCatalog(cursor, FakeConnection()).get("event_id"), "event_id")

    def test_add_creates_mediumtext_columns_in_one_statement(self):
        cursor = FakeCursor()
        catalog = raw._ColumnCatalog(cursor, FakeConnection())
        added = catalog.add(["a", "b"])
        self.assertEqual(added, ["a", "b"])
        self.assertEqual(len(cursor.alters), 1)
        self.assertIn("ADD COLUMN `a` MEDIUMTEXT NULL", cursor.alters[0])

    def test_row_size_error_stops_adding_and_marks_the_catalog_full(self):
        base = _fixed_show_columns_rows()
        cursor = FakeCursor(base, max_columns=len(base) + 2)  # room for only 2 more
        catalog = raw._ColumnCatalog(cursor, FakeConnection())
        added = catalog.add(["a", "b", "c", "d"])

        self.assertEqual(added, ["a", "b"])
        self.assertTrue(catalog.full)
        self.assertEqual(catalog.failed, {"c", "d"})
        self.assertIsNotNone(catalog.get("a"))
        self.assertIsNone(catalog.get("c"))


class FlushBatchTests(unittest.TestCase):
    def _flush(self, records, cursor=None, catalog=None):
        cursor = cursor or FakeCursor()
        connection = FakeConnection()
        catalog = catalog or raw._ColumnCatalog(cursor, connection)
        raw._flush_batch(cursor, connection, catalog, records, set(), "[test]")
        return cursor

    @staticmethod
    def _rows(cursor):
        sql, rows = cursor.executemany_calls[0]
        columns = [c.strip(" `") for c in sql.split("(", 1)[1].split(") VALUES")[0].split(",")]
        return [dict(zip(columns, r)) for r in rows]

    def test_new_key_gets_a_column_and_value_is_ingested_in_the_same_run(self):
        cursor = self._flush([raw._build_record(dict(SAMPLE_DOC))])

        altered = " ".join(cursor.alters)
        self.assertIn("`data_collection` MEDIUMTEXT", altered)
        self.assertIn("`data_click` MEDIUMTEXT", altered)
        self.assertIn("`__v` MEDIUMTEXT", altered)
        row = self._rows(cursor)[0]
        self.assertEqual(row["data_collection"], "bangles")
        self.assertEqual(row["__v"], "0")

    def test_existing_columns_are_not_altered_again(self):
        cursor = FakeCursor()
        catalog = raw._ColumnCatalog(cursor, FakeConnection())
        self._flush([raw._build_record(dict(SAMPLE_DOC))], cursor, catalog)
        alters_after_first = len(cursor.alters)
        self._flush([raw._build_record(dict(SAMPLE_DOC, event_id="other"))], cursor, catalog)
        self.assertEqual(len(cursor.alters), alters_after_first)

    def test_null_only_key_creates_no_column(self):
        cursor = self._flush([raw._build_record(dict(SAMPLE_DOC, brand_new_key=None))])
        self.assertNotIn("brand_new_key", " ".join(cursor.alters))

    def test_key_differing_only_by_case_reuses_existing_column(self):
        doc = dict(SAMPLE_DOC, variantid="123", variantId=None)
        cursor = self._flush([raw._build_record(doc)])
        self.assertNotIn("`variantid`", " ".join(cursor.alters))
        self.assertEqual(self._rows(cursor)[0]["variantId"], "123")

    def test_upsert_updates_everything_except_event_id_and_ingested_at(self):
        cursor = self._flush([raw._build_record(dict(SAMPLE_DOC))])
        sql, _ = cursor.executemany_calls[0]

        self.assertIn("INSERT INTO `raw_mongo_events`", sql)
        update_clause = sql.split("ON DUPLICATE KEY UPDATE")[1]
        self.assertNotIn("`event_id` = VALUES", update_clause)
        self.assertNotIn("ingested_at", sql)
        self.assertIn("`event_type` = VALUES(`event_type`)", update_clause)

    def test_purchase_event_custom_data_is_fully_stored_with_no_nulls(self):
        cursor = self._flush([raw._build_record(dict(PURCHASE_DOC))])
        row = self._rows(cursor)[0]

        self.assertEqual(row["data_custom_data_order_name"], "#627813")
        self.assertEqual(row["data_custom_data_order_type"], "cod")
        self.assertEqual(row["data_custom_data_isPpcod"], "false")
        # empty strings are values: stored as '', not NULL
        self.assertEqual(row["data_custom_data_customer_id"], "")
        self.assertEqual(row["data_custom_data_merchant_id"], "")
        self.assertEqual(row["data_custom_data_cod_charges"], "")
        self.assertEqual(row["data_page"], "/collections/gifting/")
        self.assertEqual(row["data_referrer"], "https://www.google.com/")
        self.assertNotIn("extra_data", row)

    def test_value_of_a_different_type_than_earlier_events_is_kept_as_text(self):
        # the old typed design nulled '' / '#627813' arriving for a numeric column
        cursor = FakeCursor(_fixed_show_columns_rows() + [("score", "mediumtext")])
        records = [
            raw._build_record(dict(SAMPLE_DOC, score=7)),
            raw._build_record(dict(SAMPLE_DOC, event_id="e2", score="not a number")),
            raw._build_record(dict(SAMPLE_DOC, event_id="e3", score="")),
            raw._build_record(dict(SAMPLE_DOC, event_id="e4", score=299.5)),
        ]
        self._flush(records, cursor)
        self.assertEqual([r["score"] for r in self._rows(cursor)], ["7", "not a number", "", "299.5"])

    def test_column_cap_keeps_values_in_extra_data(self):
        with mock.patch.object(raw, "MAX_COLUMNS", 1):
            cursor = self._flush([raw._build_record(dict(SAMPLE_DOC))])
        self.assertEqual(cursor.alters, [])
        extra = json.loads(self._rows(cursor)[0]["extra_data"])
        self.assertEqual(
            extra,
            {"data_collection": "bangles", "data_click": "https://www.shyle.in/collections/bracelets", "__v": 0},
        )

    def test_row_size_failure_keeps_values_in_extra_data_and_is_not_retried(self):
        base = _fixed_show_columns_rows()
        cursor = FakeCursor(base, max_columns=len(base))  # no room at all (MySQL 1118)
        connection = FakeConnection()
        catalog = raw._ColumnCatalog(cursor, connection)

        for event_id in ("e1", "e2"):
            raw._flush_batch(
                cursor, connection, catalog,
                [raw._build_record(dict(PURCHASE_DOC, event_id=event_id))], set(), "[test]",
            )

        attempts_after_first_batch = len(cursor.alters)
        self.assertTrue(catalog.full)
        rows = []
        for sql, batch_rows in cursor.executemany_calls:
            columns = [c.strip(" `") for c in sql.split("(", 1)[1].split(") VALUES")[0].split(",")]
            rows.extend(dict(zip(columns, r)) for r in batch_rows)
        for row in rows:
            extra = json.loads(row["extra_data"])
            self.assertEqual(extra["data_custom_data_order_name"], "#627813")
            self.assertEqual(extra["data_custom_data_isPpcod"], False)
            self.assertEqual(extra["data_custom_data_customer_id"], "")
        # the second batch did not even try another ALTER
        self.assertEqual(len(cursor.alters), attempts_after_first_batch)

    def test_keys_colliding_case_insensitively_in_one_document_lose_nothing(self):
        doc = dict(SAMPLE_DOC, data={"utm_source": "google", "Utm_Source": "Google-Ads"})
        cursor = self._flush([raw._build_record(doc)])
        row = self._rows(cursor)[0]
        self.assertEqual(row["data_utm_source"], "google")
        self.assertEqual(json.loads(row["extra_data"]), {"data_Utm_Source": "Google-Ads"})

    def test_a_mongo_key_named_extra_data_does_not_clobber_our_column(self):
        doc = dict(SAMPLE_DOC, extra_data="from mongo")
        cursor = self._flush([raw._build_record(doc)])
        self.assertEqual(json.loads(self._rows(cursor)[0]["extra_data"]), {"extra_data": "from mongo"})

    def test_value_too_long_for_an_indexed_column_keeps_a_prefix_and_the_full_text(self):
        long_value = "x" * 400
        cursor = self._flush([raw._build_record(dict(SAMPLE_DOC, cart_token=long_value))])
        row = self._rows(cursor)[0]
        self.assertEqual(row["cart_token"], "x" * 255)
        self.assertEqual(json.loads(row["extra_data"]), {"cart_token": long_value})

    def test_extra_data_column_is_left_out_when_nothing_overflowed(self):
        cursor = self._flush([raw._build_record(dict(SAMPLE_DOC))])
        sql, _ = cursor.executemany_calls[0]
        self.assertNotIn("extra_data", sql)

    def test_empty_batch_writes_nothing(self):
        cursor = self._flush([])
        self.assertEqual(cursor.executemany_calls, [])


class LegacyCopyTests(unittest.TestCase):
    LEGACY_COLUMNS = raw._LEGACY_COPY_COLUMNS + ["data_click", "ingested_at"]
    MINMAX = (datetime(2026, 11, 10, 3, 20), datetime(2026, 11, 11, 14, 25))

    def _run(self, cursor, store, chunks_per_run=3, enabled=True, max_seconds=1e9):
        def get(cur, key, default=None):
            return store.get(key, default)

        def update(cur, conn, key, ts):
            store[key] = ts

        with mock.patch.object(raw, "get_pipeline_metadata_timestamp", side_effect=get), \
             mock.patch.object(raw, "update_pipeline_metadata_timestamp", side_effect=update), \
             mock.patch.object(raw, "LEGACY_COPY_ENABLED", enabled), \
             mock.patch.object(raw, "LEGACY_COPY_MAX_SECONDS", max_seconds), \
             mock.patch.object(raw, "LEGACY_COPY_CHUNKS_PER_RUN", chunks_per_run), \
             mock.patch.object(raw, "LEGACY_COPY_CHUNK_HOURS", 6):
            return raw._copy_legacy_history(cursor, FakeConnection(), "[test]")

    def _cursor(self):
        return FakeCursor(legacy_columns=self.LEGACY_COLUMNS, legacy_minmax=self.MINMAX)

    def test_copies_newest_first_in_chunks_and_resumes_until_done(self):
        store = {}
        cursor = self._cursor()

        copied = self._run(cursor, store, chunks_per_run=3)
        self.assertEqual(copied, 30)
        self.assertEqual(len(cursor.insert_ignore_calls), 3)
        # newest chunk first; bounds are whole hours, upper bound exclusive
        self.assertEqual(
            cursor.insert_ignore_calls[0][1],
            (datetime(2026, 11, 11, 9), datetime(2026, 11, 11, 15)),
        )
        self.assertEqual(
            store[raw.LEGACY_COPY_PROGRESS_KEY].astimezone(timezone.utc).replace(tzinfo=None),
            datetime(2026, 11, 10, 21),
        )
        self.assertNotIn(raw.LEGACY_COPY_DONE_KEY, store)

        cursor2 = self._cursor()
        self._run(cursor2, store, chunks_per_run=10)
        self.assertEqual(
            [params for _, params in cursor2.insert_ignore_calls],
            [
                (datetime(2026, 11, 10, 15), datetime(2026, 11, 10, 21)),
                (datetime(2026, 11, 10, 9), datetime(2026, 11, 10, 15)),
                (datetime(2026, 11, 10, 3), datetime(2026, 11, 10, 9)),
            ],
        )
        self.assertIn(raw.LEGACY_COPY_DONE_KEY, store)

    def test_insert_ignore_never_overwrites_live_rows_and_copies_only_known_columns(self):
        cursor = self._cursor()
        self._run(cursor, {})
        sql = cursor.insert_ignore_calls[0][0]
        self.assertIn("INSERT IGNORE INTO `raw_mongo_events`", sql)
        self.assertIn("FROM `mongo_events_summary`", sql)
        self.assertIn("`event_id`", sql)
        self.assertNotIn("data_click", sql)
        self.assertNotIn("ingested_at", sql)

    def test_no_legacy_table_marks_it_done_and_copies_nothing(self):
        store = {}
        cursor = FakeCursor(legacy_columns=[])
        self.assertEqual(self._run(cursor, store), 0)
        self.assertEqual(cursor.insert_ignore_calls, [])
        self.assertIn(raw.LEGACY_COPY_DONE_KEY, store)

    def test_empty_legacy_table_is_marked_done(self):
        store = {}
        cursor = FakeCursor(legacy_columns=self.LEGACY_COLUMNS, legacy_minmax=(None, None))
        self.assertEqual(self._run(cursor, store), 0)
        self.assertIn(raw.LEGACY_COPY_DONE_KEY, store)

    def test_once_done_nothing_is_queried(self):
        cursor = self._cursor()
        self._run(cursor, {raw.LEGACY_COPY_DONE_KEY: datetime.now(IST)})
        self.assertEqual(cursor.executed, [])

    def test_time_budget_stops_the_copy_after_the_first_chunk_and_it_resumes_later(self):
        store = {}
        cursor = self._cursor()
        clock = mock.Mock(monotonic=mock.Mock(side_effect=[0, 100]))  # start, then budget exceeded
        with mock.patch.object(raw, "time", clock):
            self._run(cursor, store, chunks_per_run=10, max_seconds=45)

        self.assertEqual(len(cursor.insert_ignore_calls), 1)  # progress is still guaranteed
        self.assertIn(raw.LEGACY_COPY_PROGRESS_KEY, store)
        self.assertNotIn(raw.LEGACY_COPY_DONE_KEY, store)

    def test_is_off_by_default(self):
        import os

        if "MONGO_EVENTS_LEGACY_COPY" not in os.environ:
            self.assertFalse(raw.LEGACY_COPY_ENABLED)

    def test_can_be_switched_off(self):
        cursor = self._cursor()
        self.assertEqual(self._run(cursor, {}, enabled=False), 0)
        self.assertEqual(cursor.executed, [])


def _sync(docs, cursor=None, explicit_window=True, delete_enabled=True, stored_watermark=None,
          find_side_effect=None, brand_name="SHYLENEW"):
    """Runs sync_mongo_events_raw_for_brand against fakes, with the legacy copy
    stubbed out (it has its own tests above)."""
    cursor = cursor or FakeCursor()
    collection = mock.Mock()
    collection.delete_many.return_value = mock.Mock(deleted_count=len(docs) if docs else 0)
    if find_side_effect:
        collection.find.side_effect = find_side_effect
    else:
        collection.find.return_value = list(docs)
    client = mock.Mock()
    kwargs = {}
    if explicit_window:
        kwargs = {"start_date": date(2026, 9, 24), "end_date": date(2026, 9, 25)}
    with mock.patch.object(raw.aws_background, "get_brand_timezone", return_value=IST), \
         mock.patch.object(raw, "_resolve_mongo_sessions_collection", return_value=(client, collection)), \
         mock.patch.object(raw, "_copy_legacy_history", return_value=0) as copy_history, \
         mock.patch.object(raw, "MONGO_EVENTS_DELETE_INGESTED_DOCS", delete_enabled), \
         mock.patch.object(raw, "get_pipeline_metadata_timestamp", return_value=stored_watermark) as get_wm, \
         mock.patch.object(raw, "update_pipeline_metadata_timestamp") as update_wm:
        affected = raw.sync_mongo_events_raw_for_brand(
            brand_index=8,
            brand_name=brand_name,
            brand_key=brand_name,
            cursor=cursor,
            connection=FakeConnection(),
            **kwargs,
        )
    return cursor, collection, client, affected, get_wm, update_wm, copy_history


class SyncForBrandTests(unittest.TestCase):
    def test_queries_shop_and_window_without_sort_and_excluding_id(self):
        _, collection, client, *_ = _sync([dict(SAMPLE_DOC)])

        args, kwargs = collection.find.call_args
        query, projection = args
        self.assertEqual(query["shop_name"], "shyle")
        self.assertIn("$gte", query["createdAt"])
        self.assertIn("$lt", query["createdAt"])
        self.assertEqual(projection, {"_id": 0})
        self.assertEqual(kwargs["batch_size"], raw.BATCH_SIZE)
        client.close.assert_called_once()

    def test_window_bounds_use_the_brand_timezone(self):
        _, collection, *_ = _sync([])
        query = collection.find.call_args.args[0]
        # 2026-09-24 00:00 IST == 2026-09-23 18:30 UTC
        self.assertEqual(query["createdAt"]["$gte"], datetime(2026, 9, 23, 18, 30, tzinfo=timezone.utc))
        self.assertEqual(query["createdAt"]["$lt"], datetime(2026, 9, 25, 18, 30, tzinfo=timezone.utc))

    def test_table_ddl_has_only_the_indexes_the_rollups_need(self):
        cursor, *_ = _sync([dict(SAMPLE_DOC)])
        ddl = next(s for s in cursor.executed if "CREATE TABLE IF NOT EXISTS `raw_mongo_events`" in s)
        self.assertIn("`idx_type_created` (`event_type`, `created_at_utc`)", ddl)
        self.assertIn("`idx_created_at_utc` (`created_at_utc`)", ddl)
        for dropped in ("idx_event_type", "idx_session_id", "idx_cart_token"):
            self.assertNotIn(dropped, ddl)

    def test_creates_table_runs_legacy_copy_and_upserts_rows(self):
        cursor, *_, copy_history = _sync([dict(SAMPLE_DOC)])
        self.assertTrue(any("CREATE TABLE IF NOT EXISTS `raw_mongo_events`" in s for s in cursor.executed))
        self.assertEqual(len(cursor.executemany_calls), 1)
        copy_history.assert_called_once()

    def test_legacy_copy_runs_after_live_ingestion_so_it_never_delays_fresh_events(self):
        cursor = FakeCursor()
        seen_at_copy = {}
        collection = mock.Mock()
        collection.find.return_value = [dict(SAMPLE_DOC)]
        collection.delete_many.return_value = mock.Mock(deleted_count=1)

        def fake_copy(c, conn, prefix):
            seen_at_copy["upserts"] = len(c.executemany_calls)
            seen_at_copy["deleted"] = collection.delete_many.call_count
            return 0

        with mock.patch.object(raw.aws_background, "get_brand_timezone", return_value=IST),              mock.patch.object(raw, "_resolve_mongo_sessions_collection", return_value=(mock.Mock(), collection)),              mock.patch.object(raw, "_copy_legacy_history", side_effect=fake_copy):
            raw.sync_mongo_events_raw_for_brand(
                brand_index=8, brand_name="SHYLENEW", brand_key="SHYLENEW",
                start_date=date(2026, 9, 24), end_date=date(2026, 9, 25),
                cursor=cursor, connection=FakeConnection(),
            )
        self.assertEqual(seen_at_copy, {"upserts": 1, "deleted": 1})

    def test_legacy_copy_failure_does_not_stop_live_ingestion(self):
        cursor = FakeCursor()
        collection = mock.Mock()
        collection.find.return_value = [dict(SAMPLE_DOC)]
        collection.delete_many.return_value = mock.Mock(deleted_count=1)
        with mock.patch.object(raw.aws_background, "get_brand_timezone", return_value=IST), \
             mock.patch.object(raw, "_resolve_mongo_sessions_collection", return_value=(mock.Mock(), collection)), \
             mock.patch.object(raw, "_copy_legacy_history", side_effect=RuntimeError("copy boom")):
            raw.sync_mongo_events_raw_for_brand(
                brand_index=8, brand_name="SHYLENEW", brand_key="SHYLENEW",
                start_date=date(2026, 9, 24), end_date=date(2026, 9, 25),
                cursor=cursor, connection=FakeConnection(),
            )
        self.assertEqual(len(cursor.executemany_calls), 1)
        collection.delete_many.assert_called_once()

    def test_documents_without_event_id_are_skipped(self):
        bad = {k: v for k, v in SAMPLE_DOC.items() if k != "event_id"}
        cursor, *_ = _sync([bad])
        self.assertEqual(cursor.executemany_calls, [])

    def test_non_ci_events_brand_is_skipped_entirely(self):
        cursor, collection, *_ = _sync([dict(SAMPLE_DOC)], brand_name="SomeOtherBrand")
        collection.find.assert_not_called()
        self.assertEqual(cursor.executed, [])

    def test_mongo_failure_is_logged_not_raised(self):
        _sync([], find_side_effect=RuntimeError("mongo down"))  # must not raise

    def test_batches_are_flushed_at_batch_size(self):
        docs = [dict(SAMPLE_DOC, event_id=f"e{i}") for i in range(5)]
        with mock.patch.object(raw, "BATCH_SIZE", 2):
            cursor, *_ = _sync(docs)
        self.assertEqual([len(rows) for _, rows in cursor.executemany_calls], [2, 2, 1])


class DeleteAfterIngestTests(unittest.TestCase):
    def test_ingested_documents_are_deleted_by_event_id(self):
        _, collection, *_ = _sync([dict(SAMPLE_DOC)])
        collection.delete_many.assert_called_once_with({"event_id": {"$in": [SAMPLE_DOC["event_id"]]}})

    def test_kill_switch_disables_deletion(self):
        _, collection, *_ = _sync([dict(SAMPLE_DOC)], delete_enabled=False)
        collection.delete_many.assert_not_called()

    def test_delete_failure_is_caught_and_does_not_raise(self):
        cursor = FakeCursor()
        collection = mock.Mock()
        collection.find.return_value = [dict(SAMPLE_DOC)]
        collection.delete_many.side_effect = RuntimeError("mongo boom")
        with mock.patch.object(raw.aws_background, "get_brand_timezone", return_value=IST), \
             mock.patch.object(raw, "_resolve_mongo_sessions_collection", return_value=(mock.Mock(), collection)), \
             mock.patch.object(raw, "_copy_legacy_history", return_value=0):
            raw.sync_mongo_events_raw_for_brand(
                brand_index=8, brand_name="SHYLENEW", brand_key="SHYLENEW",
                start_date=date(2026, 9, 24), end_date=date(2026, 9, 25),
                cursor=cursor, connection=FakeConnection(),
            )  # must not raise
        # the upsert itself must still have gone through despite the delete failure
        self.assertEqual(len(cursor.executemany_calls), 1)

    def test_documents_are_kept_in_mongo_when_the_upsert_fails(self):
        cursor = FakeCursor()
        cursor.executemany = mock.Mock(side_effect=RuntimeError("db down"))
        _, collection, *_ = _sync([dict(SAMPLE_DOC)], cursor=cursor)
        collection.delete_many.assert_not_called()


class WatermarkIncrementalTests(unittest.TestCase):
    def _run_incremental(self, docs, stored_watermark=None):
        _, collection, _, _, get_wm, update_wm, _ = _sync(
            docs, explicit_window=False, stored_watermark=stored_watermark
        )
        return collection, get_wm, update_wm

    def test_no_window_given_queries_by_watermark_only(self):
        watermark = datetime(2026, 9, 24, 12, 0, tzinfo=IST)
        collection, get_wm, update_wm = self._run_incremental([], stored_watermark=watermark)

        get_wm.assert_called_once()
        self.assertEqual(get_wm.call_args.args[1], raw.RAW_EVENTS_METADATA_KEY)
        query = collection.find.call_args.args[0]
        self.assertEqual(set(query["createdAt"].keys()), {"$gt"})
        # lower bound is watermark minus the overlap, not the raw watermark
        expected_lower = (watermark - raw.RAW_EVENTS_OVERLAP).astimezone(timezone.utc)
        self.assertEqual(query["createdAt"]["$gt"], expected_lower)

    def test_watermark_key_is_shared_with_the_legacy_ingest_so_cutover_has_no_gap(self):
        self.assertEqual(raw.RAW_EVENTS_METADATA_KEY, "mongo_events_raw_last_created_at")

    def test_first_run_with_no_stored_watermark_looks_back_further(self):
        collection, get_wm, update_wm = self._run_incremental([], stored_watermark=None)
        query = collection.find.call_args.args[0]
        barely_before_now = datetime.now(timezone.utc) - timedelta(minutes=10)
        self.assertLess(query["createdAt"]["$gt"], barely_before_now)

    def test_watermark_advances_to_max_created_at_ingested(self):
        doc1 = dict(SAMPLE_DOC, event_id="e1", createdAt=datetime(2026, 9, 24, 10, 0, tzinfo=timezone.utc))
        doc2 = dict(SAMPLE_DOC, event_id="e2", createdAt=datetime(2026, 9, 24, 12, 0, tzinfo=timezone.utc))
        collection, get_wm, update_wm = self._run_incremental(
            [doc1, doc2], stored_watermark=datetime(2026, 9, 24, 9, 0, tzinfo=IST)
        )

        update_wm.assert_called()
        new_watermark = update_wm.call_args.args[3]
        self.assertEqual(new_watermark.astimezone(timezone.utc), datetime(2026, 9, 24, 12, 0, tzinfo=timezone.utc))

    def test_no_new_documents_does_not_advance_watermark(self):
        _, _, update_wm = self._run_incremental([], stored_watermark=datetime(2026, 9, 24, 9, 0, tzinfo=IST))
        update_wm.assert_not_called()

    def test_returns_affected_ist_dates_for_rollup_event_types(self):
        doc = dict(
            SAMPLE_DOC,
            event_id="e1",
            event_type="checkout_initiated",
            createdAt=datetime(2026, 9, 24, 20, 0, tzinfo=timezone.utc),  # 2026-09-25 01:30 IST
        )
        *_, affected, _, _, _ = _sync([doc], explicit_window=False)
        self.assertEqual(affected, {"2026-09-25"})

    def test_events_the_rollups_do_not_use_trigger_no_rollup_dates(self):
        docs = [
            dict(SAMPLE_DOC, event_id="e1", event_type="TAB_Click"),
            dict(SAMPLE_DOC, event_id="e2", event_type="purchase_TECHIT"),
            dict(SAMPLE_DOC, event_id="e3", event_type="login"),
        ]
        cursor, collection, _, affected, *_ = _sync(docs, explicit_window=False)
        self.assertEqual(affected, set())
        # ...but they are still all ingested and deleted from Mongo
        self.assertEqual(sum(len(rows) for _, rows in cursor.executemany_calls), 3)
        collection.delete_many.assert_called_once()

    def test_only_rollup_event_types_decide_the_dates_in_a_mixed_batch(self):
        docs = [
            dict(SAMPLE_DOC, event_id="e1", event_type="TAB_Click",
                 createdAt=datetime(2026, 9, 24, 3, 0, tzinfo=timezone.utc)),
            dict(SAMPLE_DOC, event_id="e2", event_type="buy_now",
                 createdAt=datetime(2026, 9, 25, 3, 0, tzinfo=timezone.utc)),
        ]
        *_, affected, _, _, _ = _sync(docs, explicit_window=False)
        self.assertEqual(affected, {"2026-09-25"})


if __name__ == "__main__":
    unittest.main()
