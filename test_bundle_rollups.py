import json
import unittest
from datetime import date
from unittest import mock

import bundle_rollups


class _DummyConnection:
    def __init__(self):
        self.commit_count = 0

    def commit(self):
        self.commit_count += 1


class _DummyCursor:
    def __init__(self):
        self.executed = []
        self.executemany_calls = []
        self._fetch_rows = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def executemany(self, sql, rows):
        self.executemany_calls.append((sql, list(rows)))

    def fetchall(self):
        return list(self._fetch_rows)


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, payloads):
        self._payloads = list(payloads)
        self.posts = []
        self.closed = False

    def post(self, url, headers=None, data=None, timeout=None):
        self.posts.append(
            {
                "url": url,
                "headers": headers,
                "data": data,
                "timeout": timeout,
            }
        )
        if not self._payloads:
            raise AssertionError("No more payloads configured for fake session")
        return _FakeResponse(self._payloads.pop(0))

    def close(self):
        self.closed = True


class BundleRollupTests(unittest.TestCase):
    def test_build_last_full_month_window(self):
        start_date, end_date = bundle_rollups._build_last_full_month_window(
            today=date(2026, 8, 17)
        )
        self.assertEqual(start_date, "2026-05-01")
        self.assertEqual(end_date, "2026-08-01")

    def test_parse_dynamic_bundle_products_dedupes_and_skips_summary(self):
        rows = [
            {
                "product_id": "101",
                "product_title": "Bundle A",
                "product_type": "BUNDLE",
            },
            {
                "product_id": "101",
                "product_title": "Bundle A duplicate",
                "product_type": "BUNDLE",
            },
            {
                "product_id": "999",
                "product_title": "Summary",
                "product_type": "",
            },
            {
                "product_id": "202",
                "product_title": "Cyob A",
                "product_type": "CYOB",
            },
        ]

        parsed = bundle_rollups._parse_dynamic_bundle_products(rows)

        self.assertEqual(
            parsed,
            [
                {"product_id": 101, "product_title": "Bundle A"},
                {"product_id": 202, "product_title": "Cyob A"},
            ],
        )

    def test_fetch_dynamic_bundle_products_paginates_and_dedupes(self):
        first_page_rows = [
            {"product_id": str(i), "product_title": f"Bundle {i}", "product_type": "BUNDLE"}
            for i in range(1, 1001)
        ]
        second_page_rows = [
            {"product_id": "1000", "product_title": "Bundle 1000 duplicate", "product_type": "BUNDLE"},
            {"product_id": "1001", "product_title": "Cyob 1001", "product_type": "CYOB"},
        ]
        payloads = [
            {
                "data": {
                    "shopifyqlQuery": {
                        "tableData": {
                            "columns": [
                                {"name": "product_id"},
                                {"name": "product_title"},
                                {"name": "product_type"},
                            ],
                            "rows": first_page_rows,
                        },
                        "parseErrors": [],
                    }
                }
            },
            {
                "data": {
                    "shopifyqlQuery": {
                        "tableData": {
                            "columns": [
                                {"name": "product_id"},
                                {"name": "product_title"},
                                {"name": "product_type"},
                            ],
                            "rows": second_page_rows,
                        },
                        "parseErrors": [],
                    }
                }
            },
        ]
        fake_session = _FakeSession(payloads)

        with mock.patch.object(bundle_rollups, "_make_shopifyql_session", return_value=fake_session):
            products = bundle_rollups.fetch_dynamic_bundle_products(
                shop_name="the-man-company",
                access_token="token",
                api_version="2025-10",
                today=date(2026, 8, 17),
                page_size=1000,
            )

        self.assertTrue(fake_session.closed)
        self.assertEqual(len(fake_session.posts), 2)
        first_query = json.loads(fake_session.posts[0]["data"])["query"]
        second_query = json.loads(fake_session.posts[1]["data"])["query"]
        self.assertIn("SINCE 2026-05-01 UNTIL 2026-08-01", first_query)
        self.assertIn("LIMIT 1000 OFFSET 0", first_query)
        self.assertIn("LIMIT 1000 OFFSET 1000", second_query)
        self.assertEqual(len(products), 1001)
        self.assertEqual(products[0], {"product_id": 1, "product_title": "Bundle 1"})
        self.assertEqual(products[-1], {"product_id": 1001, "product_title": "Cyob 1001"})

    def test_sync_bundle_master_uses_dynamic_products(self):
        cursor = _DummyCursor()
        connection = _DummyConnection()
        products = [
            {"product_id": 201, "product_title": "Bundle X"},
            {"product_id": 202, "product_title": "Cyob Y"},
        ]

        bundle_rollups.sync_bundle_master(cursor, connection, bundle_products=products)

        self.assertEqual(len(cursor.executemany_calls), 1)
        _sql, rows = cursor.executemany_calls[0]
        self.assertEqual(
            rows,
            [
                (201, "Bundle X", 1, 1),
                (202, "Cyob Y", 1, 2),
            ],
        )
        self.assertIn(("UPDATE bundle_master SET is_active = 0", None), cursor.executed)

    def test_build_rollup_rows_uses_dynamic_names_and_existing_allocation_logic(self):
        products = [{"product_id": 101, "product_title": "Dynamic Bundle"}]
        source_rows = [
            {
                "order_id": "5001",
                "created_date": "2026-08-01",
                "product_id": "101",
                "line_item": None,
                "line_item_price": "100.00",
                "line_item_quantity": "2",
                "line_item_properties_json": json.dumps(
                    [
                        {"name": "sub_item_1", "value": "Child A x 2 :: image1"},
                        {"name": "sub_item_2", "value": "Child B x 1 :: image2"},
                    ]
                ),
                "_ITEM1_name": None,
                "_ITEM1_value": None,
            }
        ]

        with mock.patch.object(
            bundle_rollups,
            "fetch_bundle_parent_rows_for_dates",
            return_value=source_rows,
        ):
            daily_rows, product_rows = bundle_rollups._build_rollup_rows(
                cursor=_DummyCursor(),
                target_dates={"2026-08-01"},
                shop_name="the-man-company",
                access_token="token",
                api_version="2025-10",
                bundle_products=products,
            )

        self.assertEqual(
            daily_rows,
            [("2026-08-01", 101, "Dynamic Bundle", 1, 2, 200.0)],
        )
        self.assertEqual(
            product_rows,
            [
                ("2026-08-01", 101, "Dynamic Bundle", "Child A", None, 4, 133.33),
                ("2026-08-01", 101, "Dynamic Bundle", "Child B", None, 2, 66.67),
            ],
        )

    def test_is_bundle_rollup_brand_matches_tmc_and_pts(self):
        self.assertTrue(
            bundle_rollups.is_bundle_rollup_brand("The Man Company", "tmc", "the-man-company")
        )
        self.assertTrue(
            bundle_rollups.is_bundle_rollup_brand("PTS", "pts", "skincarepersonaltouch")
        )
        self.assertTrue(
            bundle_rollups.is_bundle_rollup_brand(None, None, "skincarepersonaltouch")
        )
        self.assertFalse(
            bundle_rollups.is_bundle_rollup_brand("Some Other Brand", "other", "some-other-shop")
        )

    def test_extract_bundle_children_uses_bundle_product_properties(self):
        row = {
            "order_id": "6001",
            "product_id": "201",
            "line_item": "Buy any 3 at 999",
            "line_item_properties_json": json.dumps(
                [
                    {"name": "_bundle_product1", "value": "Face Wash/SKU-FW-100"},
                    {"name": "_bundle_product2", "value": "Face Wash/SKU-FW-100"},
                    {"name": "_bundle_product3", "value": "Sunscreen/SKU-SS-200"},
                ]
            ),
        }

        items, source = bundle_rollups.extract_bundle_children(row)

        self.assertEqual(source, "bundle_product_property")
        self.assertEqual(
            items,
            [
                {
                    "title": "Face Wash",
                    "sku": "SKU-FW-100",
                    "quantity": 1,
                    "source": "bundle_product_property",
                },
                {
                    "title": "Face Wash",
                    "sku": "SKU-FW-100",
                    "quantity": 1,
                    "source": "bundle_product_property",
                },
                {
                    "title": "Sunscreen",
                    "sku": "SKU-SS-200",
                    "quantity": 1,
                    "source": "bundle_product_property",
                },
            ],
        )


if __name__ == "__main__":
    unittest.main()
