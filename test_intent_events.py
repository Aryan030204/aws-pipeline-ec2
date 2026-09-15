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
    def __init__(self):
        self.executed = []
        self.executemany_calls = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def executemany(self, sql, rows):
        self.executemany_calls.append((sql, list(rows)))


def _sample_doc(**overrides):
    doc = {
        "event_id": "sh-9e2e4ee1-FF7F-4393-B700-DC477CFFAC90",
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
            event_id, event_name, client_id, visitor_id, session_id, url, referrer,
            user_agent, product_id, variant_id, product_title, variant_title,
            quantity, price, currency, checkout_total, scroll_percent, data, occurred_at,
        ) = row

        self.assertEqual(event_id, "sh-9e2e4ee1-FF7F-4393-B700-DC477CFFAC90")
        self.assertEqual(event_name, "product_viewed")
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
            raw={"scroll_percent": 75.5},
        )
        row = ie._extract_behavioral_event_row(doc)
        self.assertIsNotNone(row)
        self.assertIsNone(row[8])  # product_id
        self.assertIsNone(row[9])  # variant_id
        self.assertEqual(row[16], 75.5)  # scroll_percent

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
        self.assertIsNone(row[8])  # product_id


def _sample_click_doc(**overrides):
    # Matches a real dead_click document supplied by the user.
    doc = {
        "event_id": "shu-a44ac102-774D-41FD-85A8-F0018CCA492C",
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
            event_id, event_name, client_id, visitor_id, session_id, url, referrer,
            user_agent, click_x, click_y, click_tag, click_element_id, click_element_name,
            click_element_type, click_element_value, click_href, url_changed, cart_changed,
            ui_changed, meaningful_scroll, click_bucket, data, occurred_at,
        ) = row

        self.assertEqual(event_id, "shu-a44ac102-774D-41FD-85A8-F0018CCA492C")
        self.assertEqual(event_name, "click")
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
        self.assertEqual(row[9], 595)  # click_y
        self.assertEqual(row[13], "submit")  # click_element_type
        self.assertEqual(row[20], "dead_click")  # click_bucket

    def test_missing_click_and_signals_objects_default_to_none(self):
        doc = _sample_click_doc(click=None, signals=None)
        row = ie._extract_click_event_row(doc)
        self.assertIsNotNone(row)
        self.assertIsNone(row[8])  # click_x
        self.assertIsNone(row[16])  # url_changed

    def test_invalid_click_bucket_becomes_none(self):
        doc = _sample_click_doc(click_bucket="not_a_real_bucket")
        row = ie._extract_click_event_row(doc)
        self.assertIsNone(row[20])

    def test_missing_click_bucket_becomes_none(self):
        doc = _sample_click_doc()
        del doc["click_bucket"]
        row = ie._extract_click_event_row(doc)
        self.assertIsNone(row[20])

    def test_missing_event_id_is_skipped(self):
        doc = _sample_click_doc(event_id="")
        self.assertIsNone(ie._extract_click_event_row(doc))


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
