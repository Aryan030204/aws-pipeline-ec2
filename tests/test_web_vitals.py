import unittest
from datetime import date, datetime

from pipeline import web_vitals as wv


class ParserTests(unittest.TestCase):
    def test_optional_int_and_decimal(self):
        self.assertEqual(wv._parse_optional_int("12"), 12)
        self.assertIsNone(wv._parse_optional_int(""))
        self.assertIsNone(wv._parse_optional_int("abc"))
        self.assertEqual(wv._parse_optional_decimal("1.5"), 1.5)
        self.assertIsNone(wv._parse_optional_decimal(None))
        self.assertIsNone(wv._parse_optional_decimal("x"))

    def test_optional_date_and_time(self):
        self.assertEqual(wv._parse_optional_date("2026-01-02"), date(2026, 1, 2))
        self.assertEqual(wv._parse_optional_date("2026-01-02T03:04:05"), date(2026, 1, 2))
        self.assertIsNone(wv._parse_optional_date("nope"))
        self.assertEqual(wv._parse_optional_time("10:11:12").hour, 10)
        self.assertIsNone(wv._parse_optional_time("bad"))

    def test_iso_datetime_assumes_ist_when_naive(self):
        out = wv._parse_iso_datetime_string("2026-01-02T03:04:05", "created_at", "b", "1")
        self.assertEqual(out.tzinfo, wv.IST)
        self.assertIsNone(wv._parse_iso_datetime_string("garbage", "created_at", "b", "1"))
        self.assertIsNone(wv._parse_iso_datetime_string(None, "created_at", "b", "1", required=True))

    def test_normalize_mysql_date(self):
        self.assertEqual(wv._normalize_mysql_date_value(datetime(2026, 1, 2, 5)), date(2026, 1, 2))
        self.assertEqual(wv._normalize_mysql_date_value("2026-01-02"), date(2026, 1, 2))
        self.assertIsNone(wv._normalize_mysql_date_value("zzz"))
        self.assertIsNone(wv._normalize_mysql_date_value(None))

    def test_url_hash_is_stable_sha256(self):
        self.assertEqual(wv._web_vitals_url_hash("a"), wv._web_vitals_url_hash("a"))
        self.assertEqual(len(wv._web_vitals_url_hash("a")), 64)
        self.assertNotEqual(wv._web_vitals_url_hash("a"), wv._web_vitals_url_hash("b"))


class CoerceRowTests(unittest.TestCase):
    def _doc(self, **over):
        doc = {
            "_id": "abc", "created_at": "2026-01-02T03:04:05", "brand_key": "k",
            "url": "https://x.com/p", "lcp": "2.5", "performance": "90", "date": "2026-01-02",
        }
        doc.update(over)
        return doc

    def test_valid_row(self):
        row = wv._coerce_web_vitals_row(self._doc(), "Brand")
        self.assertEqual(row["mongo_object_id"], "abc")
        self.assertEqual(row["brand_name"], "Brand")
        self.assertEqual(row["lcp"], 2.5)
        self.assertEqual(row["performance"], 90)
        self.assertIsNone(row["created_at"].tzinfo)
        self.assertIsNone(row["updated_at"])

    def test_missing_required_fields_skipped(self):
        for missing in ("_id", "created_at", "brand_key", "url"):
            with self.subTest(missing=missing):
                doc = self._doc()
                doc.pop(missing)
                self.assertIsNone(wv._coerce_web_vitals_row(doc, "Brand"))


class AggregateTests(unittest.TestCase):
    def test_groups_by_date_and_url_and_averages(self):
        rows = [
            {"date": "2026-01-02", "url": "u1", "performance": 80, "lcp": 2, "created_at": "2026-01-02T01:00:00", "page_name": "old"},
            {"date": "2026-01-02", "url": "u1", "performance": 100, "lcp": None, "created_at": "2026-01-02T02:00:00", "page_name": "new"},
            {"date": "2026-01-03", "url": "u1", "performance": 50},
            {"date": "2026-01-02", "url": "", "performance": 50},
        ]
        out = wv._aggregate_daily_web_vitals_rows(rows, "Brand")
        self.assertEqual(len(out), 2)
        first = next(r for r in out if r["date"] == date(2026, 1, 2))
        self.assertEqual(first["sample_count"], 2)
        self.assertEqual(first["avg_performance"], 90)
        self.assertEqual(first["avg_lcp"], 2)
        self.assertIsNone(first["avg_cls"])
        self.assertEqual(first["page_name"], "new")
        self.assertEqual(first["brand_name"], "Brand")


if __name__ == "__main__":
    unittest.main()
