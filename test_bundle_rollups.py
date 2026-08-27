import json
import unittest
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
    def test_build_bundle_discovery_query_filter_is_active_only(self):
        query = bundle_rollups._build_bundle_discovery_query_filter()
        self.assertIn("status:active", query)
        self.assertIn("product_type:'CYOB'", query)
        self.assertIn("product_type:'BUNDLE'", query)

    def test_extract_numeric_id_from_gid(self):
        self.assertEqual(
            bundle_rollups._extract_numeric_id_from_gid("gid://shopify/Product/1234567890"),
            1234567890,
        )
        self.assertIsNone(bundle_rollups._extract_numeric_id_from_gid(None))
        self.assertIsNone(bundle_rollups._extract_numeric_id_from_gid(""))

    def test_parse_bundle_product_nodes_dedupes_and_skips_untitled(self):
        nodes = [
            {"id": "gid://shopify/Product/101", "title": "Bundle A"},
            {"id": "gid://shopify/Product/101", "title": "Bundle A duplicate"},
            {"id": "gid://shopify/Product/999", "title": ""},
            {"id": "gid://shopify/Product/202", "title": "Cyob A"},
        ]

        parsed = bundle_rollups._parse_bundle_product_nodes(nodes)

        self.assertEqual(
            parsed,
            [
                {"product_id": 101, "product_title": "Bundle A"},
                {"product_id": 202, "product_title": "Cyob A"},
            ],
        )

    def test_fetch_dynamic_bundle_products_paginates_and_dedupes(self):
        first_page_edges = [
            {"cursor": f"cursor-{i}", "node": {"id": f"gid://shopify/Product/{i}", "title": f"Bundle {i}"}}
            for i in range(1, 251)
        ]
        second_page_edges = [
            {
                "cursor": "cursor-250-dup",
                "node": {"id": "gid://shopify/Product/250", "title": "Bundle 250 duplicate"},
            },
            {
                "cursor": "cursor-251",
                "node": {"id": "gid://shopify/Product/251", "title": "Cyob 251"},
            },
        ]
        payloads = [
            {
                "data": {
                    "products": {
                        "edges": first_page_edges,
                        "pageInfo": {"hasNextPage": True},
                    }
                }
            },
            {
                "data": {
                    "products": {
                        "edges": second_page_edges,
                        "pageInfo": {"hasNextPage": False},
                    }
                }
            },
        ]
        fake_session = _FakeSession(payloads)

        with mock.patch.object(bundle_rollups, "_make_admin_api_session", return_value=fake_session):
            products = bundle_rollups.fetch_dynamic_bundle_products(
                shop_name="the-man-company",
                access_token="token",
                api_version="2025-10",
                page_size=250,
            )

        self.assertTrue(fake_session.closed)
        self.assertEqual(len(fake_session.posts), 2)
        first_payload = json.loads(fake_session.posts[0]["data"])
        second_payload = json.loads(fake_session.posts[1]["data"])
        self.assertEqual(first_payload["variables"]["after"], None)
        self.assertIn("status:active", first_payload["variables"]["searchQuery"])
        self.assertEqual(second_payload["variables"]["after"], "cursor-250")
        self.assertEqual(len(products), 251)
        self.assertEqual(products[0], {"product_id": 1, "product_title": "Bundle 1"})
        self.assertEqual(products[-1], {"product_id": 251, "product_title": "Cyob 251"})

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
