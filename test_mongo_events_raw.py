import unittest
from datetime import date, datetime, timezone
from unittest import mock
from zoneinfo import ZoneInfo

from pipeline import mongo_events_raw as raw

IST = ZoneInfo("Asia/Kolkata")


class FakeConnection:
    def __init__(self):
        self.commit_count = 0

    def commit(self):
        self.commit_count += 1


def _fixed_show_columns_rows():
    rows = [
        (name, definition.replace(" NOT NULL", "").replace(" NULL", "").lower())
        for name, definition in raw._FIXED_COLUMNS.items()
    ]
    rows.append(("ingested_at", "datetime(6)"))
    return rows


class FakeCursor:
    """Answers SHOW COLUMNS from a mutable column list (updated by ALTERs, like
    a real table) and records everything else."""

    def __init__(self, show_columns_rows=None, fail_alter_containing=None):
        self.columns = list(show_columns_rows if show_columns_rows is not None else _fixed_show_columns_rows())
        self.executed = []
        self.executemany_calls = []
        self.fail_alter_containing = fail_alter_containing
        self._result = []

    def execute(self, sql, params=None):
        self.executed.append(sql)
        self._result = []
        if sql.startswith("SHOW COLUMNS"):
            self._result = list(self.columns)
        elif sql.startswith("ALTER TABLE"):
            if self.fail_alter_containing and self.fail_alter_containing in sql:
                raise RuntimeError("Row size too large")
            import re

            for name, sql_type in re.findall(r"ADD COLUMN `([^`]+)` (\w+(?:\(\d+\))?)", sql):
                self.columns.append((name, sql_type.lower()))

    def executemany(self, sql, rows):
        self.executemany_calls.append((sql, list(rows)))

    def fetchall(self):
        return list(self._result)

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


class TypeInferenceAndCoercionTests(unittest.TestCase):
    def test_infer_sql_types(self):
        self.assertEqual(raw._infer_sql_type(True), "TINYINT(1)")  # bool before int
        self.assertEqual(raw._infer_sql_type(5), "BIGINT")
        self.assertEqual(raw._infer_sql_type(1.5), "DOUBLE")
        self.assertEqual(raw._infer_sql_type(datetime(2026, 1, 1)), "DATETIME(3)")
        self.assertEqual(raw._infer_sql_type([1, 2]), "JSON")
        self.assertEqual(raw._infer_sql_type("x"), "TEXT")
        self.assertEqual(raw._infer_sql_type(2**70), "TEXT")

    def test_coerce_mismatch_returns_sentinel(self):
        self.assertIs(raw._coerce_value("abc", "int"), raw._MISMATCH)
        self.assertIs(raw._coerce_value("abc", "float"), raw._MISMATCH)
        self.assertIs(raw._coerce_value(5, "bool"), raw._MISMATCH)
        self.assertIs(raw._coerce_value("2026", "datetime"), raw._MISMATCH)
        self.assertIs(raw._coerce_value(2**70, "int"), raw._MISMATCH)

    def test_coerce_lists_and_scalars_into_text(self):
        self.assertEqual(raw._coerce_value(["1", "2"], "str"), '["1", "2"]')
        self.assertEqual(raw._coerce_value(123, "str"), "123")
        self.assertEqual(raw._coerce_value(True, "str"), "true")

    def test_coerce_anything_into_json_is_valid_json(self):
        self.assertEqual(raw._coerce_value("plain string", "json"), '"plain string"')
        self.assertEqual(raw._coerce_value([1], "json"), "[1]")

    def test_coerce_none_is_none(self):
        self.assertIsNone(raw._coerce_value(None, "int"))

    def test_category_from_sql_type(self):
        self.assertEqual(raw._category_from_sql_type("tinyint(1)"), "bool")
        self.assertEqual(raw._category_from_sql_type("bigint"), "int")
        self.assertEqual(raw._category_from_sql_type("double"), "float")
        self.assertEqual(raw._category_from_sql_type("datetime(3)"), "datetime")
        self.assertEqual(raw._category_from_sql_type("json"), "json")
        self.assertEqual(raw._category_from_sql_type("varchar(100)"), "str")
        self.assertEqual(raw._category_from_sql_type("text"), "str")


class ColumnCatalogTests(unittest.TestCase):
    def test_lookup_is_case_insensitive_and_returns_existing_name(self):
        catalog = raw._ColumnCatalog(FakeCursor(), FakeConnection())
        self.assertEqual(catalog.get("variantid")[0], "variantId")

    def test_dict_cursor_rows_are_supported(self):
        cursor = FakeCursor([{"Field": "event_id", "Type": "varchar(64)"}])
        catalog = raw._ColumnCatalog(cursor, FakeConnection())
        self.assertEqual(catalog.get("EVENT_ID"), ("event_id", "str"))

    def test_add_falls_back_to_one_at_a_time_when_combined_alter_fails(self):
        cursor = FakeCursor(fail_alter_containing="bad_col")
        catalog = raw._ColumnCatalog(cursor, FakeConnection())
        # The combined ALTER contains bad_col and fails; each is then retried alone.
        added = catalog.add([("good_col", "TEXT"), ("bad_col", "TEXT")])
        self.assertEqual(added, ["good_col"])
        self.assertIsNotNone(catalog.get("good_col"))
        self.assertIsNone(catalog.get("bad_col"))


class FlushBatchTests(unittest.TestCase):
    def _flush(self, records, cursor=None, catalog=None):
        cursor = cursor or FakeCursor()
        connection = FakeConnection()
        catalog = catalog or raw._ColumnCatalog(cursor, connection)
        raw._flush_batch(cursor, connection, catalog, records, set(), "[test]")
        return cursor

    def test_new_key_gets_a_column_and_value_is_ingested_in_the_same_run(self):
        record = raw._build_record(dict(SAMPLE_DOC))
        cursor = self._flush([record])

        altered = " ".join(cursor.alters)
        self.assertIn("`data_collection` TEXT", altered)
        self.assertIn("`data_click` TEXT", altered)
        self.assertIn("`__v` BIGINT", altered)

        sql, rows = cursor.executemany_calls[0]
        self.assertIn("`data_collection`", sql)
        columns = [c.strip(" `") for c in sql.split("(", 1)[1].split(") VALUES")[0].split(",")]
        row = dict(zip(columns, rows[0]))
        self.assertEqual(row["data_collection"], "bangles")
        self.assertEqual(row["__v"], 0)

    def test_existing_columns_are_not_altered_again(self):
        record = raw._build_record(dict(SAMPLE_DOC))
        cursor = FakeCursor()
        catalog = raw._ColumnCatalog(cursor, FakeConnection())
        self._flush([record], cursor, catalog)
        alters_after_first = len(cursor.alters)
        self._flush([raw._build_record(dict(SAMPLE_DOC, event_id="other"))], cursor, catalog)

        self.assertEqual(len(cursor.alters), alters_after_first)

    def test_null_only_key_creates_no_column(self):
        doc = dict(SAMPLE_DOC, brand_new_key=None)
        cursor = self._flush([raw._build_record(doc)])
        self.assertNotIn("brand_new_key", " ".join(cursor.alters))

    def test_key_differing_only_by_case_reuses_existing_column(self):
        doc = dict(SAMPLE_DOC, variantid="123", variantId=None)
        cursor = self._flush([raw._build_record(doc)])
        self.assertNotIn("`variantid`", " ".join(cursor.alters))

    def test_upsert_updates_everything_except_event_id_and_ingested_at(self):
        cursor = self._flush([raw._build_record(dict(SAMPLE_DOC))])
        sql, _ = cursor.executemany_calls[0]

        self.assertIn("INSERT INTO `mongo_events_summary`", sql)
        update_clause = sql.split("ON DUPLICATE KEY UPDATE")[1]
        self.assertNotIn("`event_id` = VALUES", update_clause)
        self.assertNotIn("ingested_at", sql)
        self.assertIn("`event_type` = VALUES(`event_type`)", update_clause)

    def test_mismatched_value_is_stored_as_null_without_failing_the_batch(self):
        cursor = FakeCursor(_fixed_show_columns_rows() + [("score", "bigint")])
        good = raw._build_record(dict(SAMPLE_DOC, score=7))
        bad = raw._build_record(dict(SAMPLE_DOC, event_id="e2", score="not a number"))
        self._flush([good, bad], cursor)

        sql, rows = cursor.executemany_calls[0]
        columns = [c.strip(" `") for c in sql.split("(", 1)[1].split(") VALUES")[0].split(",")]
        scores = [dict(zip(columns, r))["score"] for r in rows]
        self.assertEqual(scores, [7, None])

    def test_column_cap_stops_new_columns(self):
        record = raw._build_record(dict(SAMPLE_DOC))
        with mock.patch.object(raw, "MAX_COLUMNS", 1):
            cursor = self._flush([record])
        self.assertEqual(cursor.alters, [])

    def test_empty_batch_writes_nothing(self):
        cursor = self._flush([])
        self.assertEqual(cursor.executemany_calls, [])


class SyncForBrandTests(unittest.TestCase):
    def _run(self, docs, brand_name="SHYLENEW", brand_key="SHYLENEW", cursor=None, find_side_effect=None):
        cursor = cursor or FakeCursor()
        collection = mock.Mock()
        if find_side_effect:
            collection.find.side_effect = find_side_effect
        else:
            collection.find.return_value = list(docs)
        client = mock.Mock()
        with mock.patch.object(raw.aws_background, "get_brand_timezone", return_value=IST), \
             mock.patch.object(raw, "_resolve_mongo_sessions_collection", return_value=(client, collection)):
            raw.sync_mongo_events_raw_for_brand(
                brand_index=8,
                brand_name=brand_name,
                brand_key=brand_key,
                start_date=date(2026, 9, 24),
                end_date=date(2026, 9, 25),
                cursor=cursor,
                connection=FakeConnection(),
            )
        return cursor, collection, client

    def test_queries_shop_and_window_without_sort_and_excluding_id(self):
        cursor, collection, client = self._run([dict(SAMPLE_DOC)])

        args, kwargs = collection.find.call_args
        query, projection = args
        self.assertEqual(query["shop_name"], "shyle")
        self.assertIn("$gte", query["createdAt"])
        self.assertIn("$lt", query["createdAt"])
        self.assertEqual(projection, {"_id": 0})
        self.assertEqual(kwargs["batch_size"], raw.BATCH_SIZE)
        client.close.assert_called_once()

    def test_window_bounds_use_the_brand_timezone(self):
        _, collection, _ = self._run([])
        query = collection.find.call_args.args[0]
        # 2026-09-24 00:00 IST == 2026-09-23 18:30 UTC
        self.assertEqual(query["createdAt"]["$gte"], datetime(2026, 9, 23, 18, 30, tzinfo=timezone.utc))
        self.assertEqual(query["createdAt"]["$lt"], datetime(2026, 9, 25, 18, 30, tzinfo=timezone.utc))

    def test_creates_table_and_upserts_rows(self):
        cursor, _, _ = self._run([dict(SAMPLE_DOC)])
        self.assertTrue(any("CREATE TABLE IF NOT EXISTS `mongo_events_summary`" in s for s in cursor.executed))
        self.assertEqual(len(cursor.executemany_calls), 1)

    def test_documents_without_event_id_are_skipped(self):
        bad = {k: v for k, v in SAMPLE_DOC.items() if k != "event_id"}
        cursor, _, _ = self._run([bad])
        self.assertEqual(cursor.executemany_calls, [])

    def test_non_ci_events_brand_is_skipped_entirely(self):
        cursor, collection, _ = self._run([dict(SAMPLE_DOC)], brand_name="SomeOtherBrand", brand_key="other")
        collection.find.assert_not_called()
        self.assertEqual(cursor.executed, [])

    def test_mongo_failure_is_logged_not_raised(self):
        self._run([], find_side_effect=RuntimeError("mongo down"))  # must not raise

    def test_batches_are_flushed_at_batch_size(self):
        docs = [dict(SAMPLE_DOC, event_id=f"e{i}") for i in range(5)]
        with mock.patch.object(raw, "BATCH_SIZE", 2):
            cursor, _, _ = self._run(docs)
        self.assertEqual([len(rows) for _, rows in cursor.executemany_calls], [2, 2, 1])


class RebuildWindowTests(unittest.TestCase):
    def test_window_is_day_before_yesterday_to_yesterday_in_brand_tz(self):
        now = datetime(2026, 9, 25, 20, 0, tzinfo=timezone.utc)  # already 26th 01:30 IST
        self.assertEqual(
            raw.compute_raw_rebuild_window(IST, now), (date(2026, 9, 24), date(2026, 9, 25))
        )


if __name__ == "__main__":
    unittest.main()
