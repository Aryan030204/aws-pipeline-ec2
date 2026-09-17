import json
import sys
import types
import unittest
from datetime import datetime, timezone
from unittest import mock

try:
    import pymongo  # noqa: F401
except ModuleNotFoundError:
    fake_pymongo = types.ModuleType("pymongo")
    fake_pymongo.MongoClient = object
    sys.modules["pymongo"] = fake_pymongo

# Same import-order requirement as the other pipeline.* test files: import
# aws_background first so its facade re-export of pipeline.intent_events
# resolves normally instead of hitting a circular partial-init.
import aws_background  # noqa: F401
from pipeline import intent_events as ie


class _DummyConnection:
    def __init__(self):
        self.commit_count = 0

    def commit(self):
        self.commit_count += 1


class _DummyCursor:
    def __init__(self, fetchone_results=None):
        self.executed = []
        self.executemany_calls = []
        self._fetchone_results = list(fetchone_results or [])

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def executemany(self, sql, rows):
        self.executemany_calls.append((sql, list(rows)))

    def fetchone(self):
        if self._fetchone_results:
            return self._fetchone_results.pop(0)
        return None


def _sample_doc(**overrides):
    doc = {
        "event_id": "sh-9e2e4ee1-FF7F-4393-B700-DC477CFFAC90",
        "actor_id": "actor_1789548948942_9vb1nu0pbtu17wri6c132t",
        "brand_id": "test_shop",
        "client_id": "574823dd-70b0-45fe-923c-be1c67793deb",
        "event_name": "product_viewed",
        "occurred_at": datetime(2026, 9, 14, 4, 30, 29, 607000, tzinfo=timezone.utc),
        "raw": {
            "product_id": "10442243637429",
            "variant_id": "52538216087733",
            "product_title": "Bathing Experience Set",
            "variant_title": "Default Title",
            "price": 450,
            "currency": "USD",
        },
        "referrer": "https://reels-app-testing.myshopify.com/",
        "session_id": "5579a413-e398-47d9-98d2-16f77e53596b",
        "url": "https://reels-app-testing.myshopify.com/products/build-your-own-body-w...",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "visitor_id": None,
    }
    doc.update(overrides)
    return doc


class ExtractBehavioralEventRowTests(unittest.TestCase):
    def test_extracts_all_fields_from_sample_document(self):
        row = ie._extract_behavioral_event_row(_sample_doc())
        self.assertIsNotNone(row)
        (
            event_id, event_name, actor_id, client_id, visitor_id, session_id, url, referrer,
            user_agent, product_id, variant_id, product_title, variant_title,
            quantity, price, currency, checkout_total, scroll_percent, data, occurred_at,
        ) = row

        self.assertEqual(event_id, "sh-9e2e4ee1-FF7F-4393-B700-DC477CFFAC90")
        self.assertEqual(event_name, "product_viewed")
        self.assertEqual(actor_id, "actor_1789548948942_9vb1nu0pbtu17wri6c132t")
        self.assertEqual(client_id, "574823dd-70b0-45fe-923c-be1c67793deb")
        self.assertIsNone(visitor_id)
        self.assertEqual(product_id, "10442243637429")
        self.assertEqual(variant_id, "52538216087733")
        self.assertEqual(price, 450.0)
        self.assertEqual(currency, "USD")
        self.assertIsNone(quantity)
        self.assertIsNone(checkout_total)
        self.assertIsNone(scroll_percent)
        self.assertEqual(json.loads(data), _sample_doc()["raw"])
        self.assertEqual(occurred_at, _sample_doc()["occurred_at"])

    def test_missing_raw_fields_default_to_none(self):
        doc = _sample_doc(
            event_name="scroll",
            raw={"percent": 75.5},
        )
        row = ie._extract_behavioral_event_row(doc)
        self.assertIsNotNone(row)
        self.assertIsNone(row[9])  # product_id
        self.assertIsNone(row[10])  # variant_id
        self.assertEqual(row[17], 75.5)  # scroll_percent

    def test_scroll_percent_reads_from_raw_percent_key(self):
        # Real observed shape: scroll events store raw={"percent": N}, NOT
        # raw={"scroll_percent": N} - the MySQL column name and the Mongo
        # raw key genuinely differ for this one field.
        doc = _sample_doc(event_name="scroll", raw={"percent": 50})
        row = ie._extract_behavioral_event_row(doc)
        self.assertEqual(row[17], 50.0)  # scroll_percent

    def test_scroll_percent_stays_none_when_raw_has_no_percent_key(self):
        doc = _sample_doc(event_name="scroll", raw={"scroll_percent": 999})
        row = ie._extract_behavioral_event_row(doc)
        self.assertIsNone(row[17])  # scroll_percent - wrong key name, ignored

    def test_missing_event_id_is_skipped(self):
        doc = _sample_doc(event_id="")
        self.assertIsNone(ie._extract_behavioral_event_row(doc))

    def test_missing_occurred_at_is_skipped(self):
        doc = _sample_doc(occurred_at=None)
        self.assertIsNone(ie._extract_behavioral_event_row(doc))

    def test_missing_raw_object_entirely_still_extracts_flat_fields(self):
        doc = _sample_doc(raw=None)
        row = ie._extract_behavioral_event_row(doc)
        self.assertIsNotNone(row)
        self.assertIsNone(row[9])  # product_id


def _sample_click_doc(**overrides):
    # Matches a real dead_click document supplied by the user.
    doc = {
        "event_id": "shu-a44ac102-774D-41FD-85A8-F0018CCA492C",
        "actor_id": "actor_1789548948942_9vb1nu0pbtu17wri6c132t",
        "brand_id": "bbb_shop",
        "click": {
            "x": 306,
            "y": 579,
            "tag_name": "SPAN",
            "element_id": None,
            "element_name": None,
            "element_type": None,
            "element_value": None,
            "href": None,
        },
        "click_bucket": "useful_click",
        "client_id": "6d351267-53c7-4a2d-b96e-cad5ce8c5587",
        "event_name": "click",
        "occurred_at": datetime(2026, 9, 15, 8, 59, 8, 237000, tzinfo=timezone.utc),
        "referrer": "https://blabliblulife.com/collections/all",
        "session_id": None,
        "signals": {
            "url_changed": False,
            "cart_changed": False,
            "ui_changed": True,
            "meaningful_scroll": False,
        },
        "url": "https://blabliblulife.com/collections/body-wash",
        "user_agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36",
        "visitor_id": "10198230171943",
    }
    doc.update(overrides)
    return doc


class ExtractClickEventRowTests(unittest.TestCase):
    def test_extracts_all_fields_from_useful_click_document(self):
        row = ie._extract_click_event_row(_sample_click_doc())
        self.assertIsNotNone(row)
        (
            event_id, event_name, actor_id, client_id, visitor_id, session_id, url, referrer,
            user_agent, click_x, click_y, click_tag, click_element_id, click_element_name,
            click_element_type, click_element_value, click_href, url_changed, cart_changed,
            ui_changed, meaningful_scroll, click_bucket, data, occurred_at,
        ) = row

        self.assertEqual(event_id, "shu-a44ac102-774D-41FD-85A8-F0018CCA492C")
        self.assertEqual(event_name, "click")
        self.assertEqual(actor_id, "actor_1789548948942_9vb1nu0pbtu17wri6c132t")
        self.assertEqual(client_id, "6d351267-53c7-4a2d-b96e-cad5ce8c5587")
        self.assertEqual(visitor_id, "10198230171943")
        self.assertIsNone(session_id)
        self.assertEqual(click_x, 306)
        self.assertEqual(click_y, 579)
        self.assertEqual(click_tag, "SPAN")
        self.assertIsNone(click_element_id)
        self.assertIsNone(click_element_type)
        self.assertEqual(url_changed, False)
        self.assertEqual(ui_changed, True)
        self.assertEqual(click_bucket, "useful_click")
        self.assertEqual(
            json.loads(data),
            {
                "click": _sample_click_doc()["click"],
                "signals": _sample_click_doc()["signals"],
            },
        )
        self.assertEqual(occurred_at, _sample_click_doc()["occurred_at"])

    def test_extracts_dead_click_document(self):
        doc = _sample_click_doc(
            event_id="shu-a44ae837-4A21-4FF5-AB6B-57D24EA1ADF5",
            click_bucket="dead_click",
            click={
                "x": 153,
                "y": 595,
                "tag_name": "BUTTON",
                "element_id": None,
                "element_name": None,
                "element_type": "submit",
                "element_value": None,
                "href": None,
            },
            signals={
                "url_changed": False,
                "cart_changed": False,
                "ui_changed": False,
                "meaningful_scroll": False,
            },
        )
        row = ie._extract_click_event_row(doc)
        self.assertIsNotNone(row)
        self.assertEqual(row[10], 595)  # click_y
        self.assertEqual(row[14], "submit")  # click_element_type
        self.assertEqual(row[21], "dead_click")  # click_bucket

    def test_missing_click_and_signals_objects_default_to_none(self):
        doc = _sample_click_doc(click=None, signals=None)
        row = ie._extract_click_event_row(doc)
        self.assertIsNotNone(row)
        self.assertIsNone(row[9])  # click_x
        self.assertIsNone(row[17])  # url_changed

    def test_invalid_click_bucket_becomes_none(self):
        doc = _sample_click_doc(click_bucket="not_a_real_bucket")
        row = ie._extract_click_event_row(doc)
        self.assertIsNone(row[21])

    def test_missing_click_bucket_becomes_none(self):
        doc = _sample_click_doc()
        del doc["click_bucket"]
        row = ie._extract_click_event_row(doc)
        self.assertIsNone(row[21])

    def test_missing_event_id_is_skipped(self):
        doc = _sample_click_doc(event_id="")
        self.assertIsNone(ie._extract_click_event_row(doc))


class EnsureColumnExistsTests(unittest.TestCase):
    def test_adds_column_when_missing(self):
        cursor = _DummyCursor(fetchone_results=[None])
        connection = _DummyConnection()

        ie._ensure_column_exists(cursor, connection, "behavioral_events", "actor_id", "VARCHAR(100) NULL")

        alter_statements = [sql for sql, _ in cursor.executed if sql.startswith("ALTER TABLE")]
        self.assertEqual(len(alter_statements), 1)
        self.assertIn("ADD COLUMN actor_id VARCHAR(100) NULL", alter_statements[0])
        self.assertEqual(connection.commit_count, 1)

    def test_no_op_when_column_already_exists(self):
        cursor = _DummyCursor(fetchone_results=[{"Field": "actor_id"}])
        connection = _DummyConnection()

        ie._ensure_column_exists(cursor, connection, "behavioral_events", "actor_id", "VARCHAR(100) NULL")

        alter_statements = [sql for sql, _ in cursor.executed if sql.startswith("ALTER TABLE")]
        self.assertEqual(alter_statements, [])
        self.assertEqual(connection.commit_count, 0)


def _sample_session_history_doc(**overrides):
    # Matches the real 6-step session document supplied by the user:
    # 2x page_viewed, 3x click (all dead_click), 1x scroll_depth. visitor_id
    # changes mid-session (starts equal to client_id, later resolves to a
    # real persistent value) - deliberately preserved from the real example
    # to exercise the "last non-null wins" rule.
    doc = {
        "brand_id": "bbb_shop",
        "actor_id": "actor_1789628294501_tluczosx4encyuydxtzcb",
        "session_id": "dd0bbbc3-03c2-471d-9493-ca71f4c71804",
        "session_start": datetime(2026, 9, 17, 6, 58, 14, 199000, tzinfo=timezone.utc),
        "session_end": datetime(2026, 9, 17, 6, 58, 47, 270000, tzinfo=timezone.utc),
        "session_time_spent": 33071,
        "events_seq": {
            "1": {
                "event_id": "sh-ae28a850-8466-4F13-302E-C090D7EA9BE1",
                "event_name": "page_viewed",
                "client_id": "ae28a842-C5FD-4EAD-9F78-D6EF4DA67710",
                "visitor_id": "ae28a842-C5FD-4EAD-9F78-D6EF4DA67710",
            },
            "2": {
                "event_id": "shu-ae28bc31-872D-4EC5-4191-67945EE6B014",
                "event_name": "click",
                "client_id": "ae28a842-c5fd-4ead-9f78-d6ef4da67710",
                "visitor_id": "ae28a842-c5fd-4ead-9f78-d6ef4da67710",
                "click_bucket": "dead_click",
            },
            "3": {
                "event_id": "shu-ae28f708-F41A-46AE-A6CD-6C1964A0753B",
                "event_name": "click",
                "client_id": "ae28a842-c5fd-4ead-9f78-d6ef4da67710",
                "visitor_id": "ae28a842-c5fd-4ead-9f78-d6ef4da67710",
                "click_bucket": "dead_click",
            },
            "4": {
                "event_id": "shu-ae29006c-0CED-4E2A-18AB-56143EB66A3B",
                "event_name": "click",
                "client_id": "ae28a842-c5fd-4ead-9f78-d6ef4da67710",
                "visitor_id": "ae28a842-c5fd-4ead-9f78-d6ef4da67710",
                "click_bucket": "dead_click",
            },
            "5": {
                "event_id": "sh-ae292473-9B00-4C12-A70F-40F21346B13A",
                "event_name": "page_viewed",
                "client_id": "ae28a842-c5fd-4ead-9f78-d6ef4da67710",
                "visitor_id": "9947030683943",
            },
            "6": {
                "event_id": "baa768f8-e535-42cd-bc25-51f4ba7df120",
                "event_name": "scroll_depth",
                "client_id": "ae28a842-c5fd-4ead-9f78-d6ef4da67710",
                "visitor_id": "9947030683943",
            },
        },
        "updatedAt": datetime(2026, 9, 17, 6, 58, 48, 698000, tzinfo=timezone.utc),
    }
    doc.update(overrides)
    return doc


class ExtractSessionHistoryRowTests(unittest.TestCase):
    def test_extracts_all_fields_from_real_session_document(self):
        row = ie._extract_session_history_row(_sample_session_history_doc())
        self.assertIsNotNone(row)
        (
            session_id, actor_id, client_id, visitor_id, session_start, session_end,
            session_time_spent_ms, event_count, page_view_count, product_view_count,
            click_count, useful_click_count, dead_click_count, add_to_cart_count,
            checkout_started_count, scroll_count, event_sequence, updated_at,
        ) = row

        self.assertEqual(session_id, "dd0bbbc3-03c2-471d-9493-ca71f4c71804")
        self.assertEqual(actor_id, "actor_1789628294501_tluczosx4encyuydxtzcb")
        self.assertEqual(session_time_spent_ms, 33071)
        self.assertEqual(event_count, 6)
        self.assertEqual(page_view_count, 2)
        self.assertEqual(product_view_count, 0)
        self.assertEqual(click_count, 3)
        self.assertEqual(dead_click_count, 3)
        self.assertEqual(useful_click_count, 0)
        self.assertEqual(add_to_cart_count, 0)
        self.assertEqual(checkout_started_count, 0)
        self.assertEqual(scroll_count, 1)
        # visitor_id changes mid-session (steps 1-4: same as client_id;
        # steps 5-6: resolves to "9947030683943") - last non-null wins.
        self.assertEqual(visitor_id, "9947030683943")
        self.assertEqual(client_id, "ae28a842-c5fd-4ead-9f78-d6ef4da67710")
        self.assertEqual(
            json.loads(event_sequence),
            {
                "1": {"page_viewed": "sh-ae28a850-8466-4F13-302E-C090D7EA9BE1"},
                "2": {"click": "shu-ae28bc31-872D-4EC5-4191-67945EE6B014"},
                "3": {"click": "shu-ae28f708-F41A-46AE-A6CD-6C1964A0753B"},
                "4": {"click": "shu-ae29006c-0CED-4E2A-18AB-56143EB66A3B"},
                "5": {"page_viewed": "sh-ae292473-9B00-4C12-A70F-40F21346B13A"},
                "6": {"scroll_depth": "baa768f8-e535-42cd-bc25-51f4ba7df120"},
            },
        )
        self.assertEqual(updated_at, _sample_session_history_doc()["updatedAt"])

    def test_missing_session_id_is_skipped(self):
        doc = _sample_session_history_doc(session_id="")
        self.assertIsNone(ie._extract_session_history_row(doc))

    def test_missing_actor_id_is_skipped(self):
        doc = _sample_session_history_doc(actor_id="")
        self.assertIsNone(ie._extract_session_history_row(doc))

    def test_missing_updated_at_is_skipped(self):
        doc = _sample_session_history_doc(updatedAt=None)
        self.assertIsNone(ie._extract_session_history_row(doc))

    def test_missing_session_start_is_skipped(self):
        doc = _sample_session_history_doc(session_start=None)
        self.assertIsNone(ie._extract_session_history_row(doc))

    def test_missing_session_end_defaults_to_none(self):
        # session_end is nullable - a still-open session hasn't closed yet.
        doc = _sample_session_history_doc(session_end=None)
        row = ie._extract_session_history_row(doc)
        self.assertIsNotNone(row)
        self.assertIsNone(row[5])  # session_end

    def test_missing_events_seq_yields_zero_counts(self):
        doc = _sample_session_history_doc(events_seq=None)
        row = ie._extract_session_history_row(doc)
        self.assertIsNotNone(row)
        self.assertEqual(row[7], 0)  # event_count
        self.assertEqual(json.loads(row[16]), {})  # event_sequence

    def test_unrecognized_event_name_only_contributes_to_event_count(self):
        doc = _sample_session_history_doc(
            events_seq={
                "1": {
                    "event_id": "evt-1",
                    "event_name": "some_future_event_type",
                    "client_id": "c1",
                    "visitor_id": "v1",
                },
            },
        )
        row = ie._extract_session_history_row(doc)
        self.assertEqual(row[7], 1)  # event_count
        self.assertEqual(row[8], 0)  # page_view_count
        self.assertEqual(row[10], 0)  # click_count

    def test_add_to_cart_and_checkout_started_counters(self):
        doc = _sample_session_history_doc(
            events_seq={
                "1": {"event_id": "e1", "event_name": "product_added_to_cart"},
                "2": {"event_id": "e2", "event_name": "checkout_started"},
            },
        )
        row = ie._extract_session_history_row(doc)
        self.assertEqual(row[7], 2)  # event_count
        self.assertEqual(row[13], 1)  # add_to_cart_count
        self.assertEqual(row[14], 1)  # checkout_started_count


class UpsertIntentSessionsTests(unittest.TestCase):
    def test_builds_full_upsert_and_drops_trailing_watermark(self):
        cursor = _DummyCursor()
        connection = _DummyConnection()
        row = ie._extract_session_history_row(_sample_session_history_doc())

        inserted = ie._upsert_intent_sessions(cursor, connection, [row])

        self.assertEqual(inserted, 1)
        self.assertEqual(len(cursor.executemany_calls), 1)
        sql, rows = cursor.executemany_calls[0]
        self.assertIn("INSERT INTO intent_sessions", sql)
        self.assertIn("ON DUPLICATE KEY UPDATE", sql)
        self.assertIn("event_count = VALUES(event_count)", sql)
        self.assertNotIn("session_id = VALUES(session_id)", sql)  # key column not in UPDATE clause
        self.assertNotIn("created_at", sql)
        self.assertNotIn("updated_at", sql)
        # The trailing updatedAt (watermark-tracking only) must be dropped
        # before the row reaches the actual INSERT params.
        self.assertEqual(len(rows[0]), len(row) - 1)

    def test_empty_rows_is_a_no_op(self):
        cursor = _DummyCursor()
        connection = _DummyConnection()
        inserted = ie._upsert_intent_sessions(cursor, connection, [])
        self.assertEqual(inserted, 0)
        self.assertEqual(cursor.executemany_calls, [])


class ParseIntentDbMapTests(unittest.TestCase):
    def test_valid_json_object(self):
        with mock.patch.dict("os.environ", {"INTENT_DB_MAP": '{"test_shop": "tmc"}'}):
            self.assertEqual(ie._parse_intent_db_map(), {"test_shop": "tmc"})

    def test_missing_env_var_defaults_to_empty(self):
        with mock.patch.dict("os.environ", {}, clear=False):
            import os

            os.environ.pop("INTENT_DB_MAP", None)
            self.assertEqual(ie._parse_intent_db_map(), {})

    def test_invalid_json_falls_back_to_empty(self):
        with mock.patch.dict("os.environ", {"INTENT_DB_MAP": "not json"}):
            self.assertEqual(ie._parse_intent_db_map(), {})

    def test_non_object_json_falls_back_to_empty(self):
        with mock.patch.dict("os.environ", {"INTENT_DB_MAP": "[1, 2, 3]"}):
            self.assertEqual(ie._parse_intent_db_map(), {})


class UpsertBehavioralEventsTests(unittest.TestCase):
    def test_builds_full_upsert_excluding_ingested_at(self):
        cursor = _DummyCursor()
        connection = _DummyConnection()
        row = ie._extract_behavioral_event_row(_sample_doc())

        inserted = ie._upsert_behavioral_events(cursor, connection, [row])

        self.assertEqual(inserted, 1)
        self.assertEqual(len(cursor.executemany_calls), 1)
        sql, rows = cursor.executemany_calls[0]
        self.assertIn("INSERT INTO behavioral_events", sql)
        self.assertIn("ON DUPLICATE KEY UPDATE", sql)
        self.assertIn("event_name = VALUES(event_name)", sql)
        self.assertIn("actor_id = VALUES(actor_id)", sql)
        self.assertNotIn("ingested_at", sql)
        self.assertNotIn("event_id = VALUES(event_id)", sql)  # key column not in UPDATE clause
        self.assertEqual(rows, [row])

    def test_empty_rows_is_a_no_op(self):
        cursor = _DummyCursor()
        connection = _DummyConnection()
        inserted = ie._upsert_behavioral_events(cursor, connection, [])
        self.assertEqual(inserted, 0)
        self.assertEqual(cursor.executemany_calls, [])


class UpsertClickEventsTests(unittest.TestCase):
    def test_builds_full_upsert_excluding_ingested_at(self):
        cursor = _DummyCursor()
        connection = _DummyConnection()
        row = ie._extract_click_event_row(_sample_click_doc())

        inserted = ie._upsert_click_events(cursor, connection, [row])

        self.assertEqual(inserted, 1)
        self.assertEqual(len(cursor.executemany_calls), 1)
        sql, rows = cursor.executemany_calls[0]
        self.assertIn("INSERT INTO click_events", sql)
        self.assertIn("ON DUPLICATE KEY UPDATE", sql)
        self.assertIn("click_bucket = VALUES(click_bucket)", sql)
        self.assertIn("actor_id = VALUES(actor_id)", sql)
        self.assertNotIn("ingested_at", sql)
        self.assertNotIn("event_id = VALUES(event_id)", sql)  # key column not in UPDATE clause
        self.assertEqual(rows, [row])

    def test_empty_rows_is_a_no_op(self):
        cursor = _DummyCursor()
        connection = _DummyConnection()
        inserted = ie._upsert_click_events(cursor, connection, [])
        self.assertEqual(inserted, 0)
        self.assertEqual(cursor.executemany_calls, [])


if __name__ == "__main__":
    unittest.main()
