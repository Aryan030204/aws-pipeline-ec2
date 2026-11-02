import unittest
from unittest import mock
from zoneinfo import ZoneInfo

import aws_background  # noqa: F401  (import before pipeline modules; see other tests)
from pipeline import landing_path as lp
from pipeline import schema_migrations as sm
from pipeline import shopify_orders as so


class NormalizeLandingPathTests(unittest.TestCase):
    def test_spec_examples(self):
        cases = {
            "https://store.com/collections/Shoes?utm_source=x": "/collections/shoes",
            "https://store.myshopify.com/collections/shoes/": "/collections/shoes",
            "https://store.com/collections/shoes?variant=123": "/collections/shoes",
            "https://store.com/collections/shoes": "/collections/shoes",
            "https://store.com/collections/shoes/": "/collections/shoes",
            "https://store.com/collections/shoes/products/product-a": "/collections/shoes/products/product-a",
        }
        for raw, expected in cases.items():
            self.assertEqual(lp.normalize_landing_path(raw), expected, raw)

    def test_query_hash_case_slashes_host_and_port(self):
        self.assertEqual(lp.normalize_landing_path("/collections/shoes#top"), "/collections/shoes")
        self.assertEqual(lp.normalize_landing_path("HTTPS://STORE.COM/Collections/SHOES/"), "/collections/shoes")
        self.assertEqual(lp.normalize_landing_path("https://store.com//collections//shoes//"), "/collections/shoes")
        self.assertEqual(lp.normalize_landing_path("https://store.com:8080/collections/hair"), "/collections/hair")
        self.assertEqual(lp.normalize_landing_path("//store.com/collections/Hair?x=1"), "/collections/hair")
        self.assertEqual(lp.normalize_landing_path("  /Collections/Hair  "), "/collections/hair")

    def test_percent_encoded_paths_match_the_decoded_session_form(self):
        # orders carry the encoded form, Shopify session data the decoded one
        self.assertEqual(
            lp.normalize_landing_path("https://s.myshopify.com/collections/%F0%9F%94%A5-upto-50-sale/"),
            "/collections/🔥-upto-50-sale",
        )
        self.assertEqual(lp.normalize_landing_path("/collections/Caf%C3%A9?x=1"), "/collections/café")
        self.assertEqual(lp.normalize_landing_path("/collections/%D8%AF%D9%87%D9%86"), "/collections/دهن")
        self.assertTrue(
            lp.is_collection_landing_path(
                lp.normalize_landing_path("https://s.myshopify.com/collections/%F0%9F%94%A5-upto-50-sale/")
            )
        )

    def test_locale_prefixed_collection_is_not_an_exact_collection_in_v1(self):
        self.assertEqual(lp.normalize_landing_path("/en/collections/national-day-sale"), "/en/collections/national-day-sale")
        self.assertFalse(lp.is_collection_landing_path(lp.normalize_landing_path("/en/collections/national-day-sale")))

    def test_homepage_forms_become_root(self):
        for raw in ("/", "https://store.com", "https://store.com/", "https://store.com/?utm_source=x"):
            self.assertEqual(lp.normalize_landing_path(raw), "/", raw)

    def test_unparseable_and_empty_are_none(self):
        for raw in (None, "", "   ", "?utm_source=x", "not a url", "store.com/collections/x"):
            self.assertIsNone(lp.normalize_landing_path(raw), repr(raw))


class CollectionLandingTests(unittest.TestCase):
    def test_exact_collection_only(self):
        for raw in (
            "https://store.com/collections/shoes",
            "https://store.com/collections/shoes/",
            "https://store.com/collections/Shoes?utm=x",
            "/collections/shoes",
        ):
            self.assertTrue(lp.is_collection_landing_path(lp.normalize_landing_path(raw)), raw)

    def test_everything_else_is_not_a_collection(self):
        for raw in (
            "https://store.com/collections/shoes/products/x",   # product landing within a collection
            "/collections/shoes/red",                            # extra segment
            "/collections/",
            "/collections",
            "/products/x",
            "/",
            "/cart",
            "/checkouts/abc123",
            "https://store.com/pages/collections/shoes",         # contains /collections/ but is not one
            None,
        ):
            self.assertFalse(lp.is_collection_landing_path(lp.normalize_landing_path(raw)), repr(raw))


class FieldFallbackTests(unittest.TestCase):
    def test_landing_site_wins_when_present(self):
        self.assertEqual(
            lp.pick_landing_path("/collections/a?x=1", "https://s.com/collections/b"), "/collections/a"
        )

    def test_falls_back_to_full_url_when_landing_site_empty(self):
        for empty in (None, "", "   ", "?utm_source=x"):
            self.assertEqual(
                lp.pick_landing_path(empty, "https://s.com/collections/B/"), "/collections/b", repr(empty)
            )

    def test_both_empty_is_none(self):
        self.assertIsNone(lp.pick_landing_path(None, ""))
        self.assertIsNone(lp.pick_landing_path("", None))


def _order(order_id, landing_site=None, note_attributes=None, items=2):
    return {
        "id": order_id,
        "name": f"#{order_id}",
        "created_at": "2026-11-02T10:00:00+05:30",
        "updated_at": "2026-11-02T10:05:00+05:30",
        "landing_site": landing_site,
        "note_attributes": note_attributes or [],
        "line_items": [
            {"product_id": 100 + i, "variant_id": 200 + i, "title": f"item {i}", "price": "10.0", "quantity": 1}
            for i in range(items)
        ],
    }


class OrderTransformTests(unittest.TestCase):
    TZ = ZoneInfo("Asia/Kolkata")

    def _rows(self, orders):
        with mock.patch.object(so.aws_background, "extract_bundle_item_columns_from_props", return_value={}):
            return so.transform_orders_to_df_optimized(orders, {}, self.TZ)

    def test_landing_site_is_persisted_raw_and_path_is_normalized_on_every_line_item_row(self):
        df = self._rows([_order(1, landing_site="/Collections/Shoes/?utm_source=x", items=3)])

        self.assertEqual(len(df), 3)  # 3 line items -> 3 rows (one order)
        self.assertEqual(df["order_id"].nunique(), 1)
        self.assertTrue((df["landing_site"] == "/Collections/Shoes/?utm_source=x").all())
        self.assertTrue((df["landing_page_path"] == "/collections/shoes").all())

    def test_full_url_note_attribute_is_used_when_landing_site_is_missing(self):
        df = self._rows([
            _order(2, landing_site=None, note_attributes=[{"name": "full_url", "value": "https://s.myshopify.com/collections/hair?x=1"}])
        ])
        self.assertTrue((df["landing_page_path"] == "/collections/hair").all())
        self.assertTrue(df["landing_site"].isna().all())

    def test_landing_site_beats_full_url_per_order(self):
        df = self._rows([
            _order(3, landing_site="/products/a", note_attributes=[{"name": "full_url", "value": "https://s.com/collections/hair"}])
        ])
        self.assertTrue((df["landing_page_path"] == "/products/a").all())

    def test_both_missing_gives_null(self):
        df = self._rows([_order(4)])
        self.assertTrue(df["landing_page_path"].isna().all())

    def test_note_attributes_cannot_overwrite_the_derived_columns(self):
        df = self._rows([
            _order(
                5,
                landing_site="/collections/real",
                note_attributes=[
                    {"name": "landing_page_path", "value": "/collections/spoofed"},
                    {"name": "landing_site", "value": "/collections/spoofed"},
                ],
            )
        ])
        self.assertTrue((df["landing_page_path"] == "/collections/real").all())
        self.assertTrue((df["landing_site"] == "/collections/real").all())

    def test_fallback_is_per_order_not_per_brand(self):
        df = self._rows([
            _order(6, landing_site="/collections/native"),  # native checkout order
            _order(7, note_attributes=[{"name": "full_url", "value": "https://s.com/collections/gokwik"}]),  # GoKwik order
            _order(8),
        ])
        by_order = df.groupby("order_id")["landing_page_path"].first().to_dict()
        self.assertEqual(by_order, {"6": "/collections/native", "7": "/collections/gokwik", "8": None})

    def test_order_without_line_items_still_gets_the_landing_fields(self):
        order = _order(9, landing_site="/collections/solo", items=0)
        df = self._rows([order])
        self.assertEqual(len(df), 1)
        self.assertEqual(df.loc[0, "landing_page_path"], "/collections/solo")


class SchemaTests(unittest.TestCase):
    def test_columns_added_to_both_order_tables(self):
        with mock.patch.object(sm, "_ensure_columns") as ensure:
            sm.ensure_landing_page_columns(7, "shopify_orders")
        ensure.assert_called_once_with(7, "shopify_orders", {"landing_site": "TEXT", "landing_page_path": "VARCHAR(500)"})

    def test_preflight_adds_them_to_orders_and_orders_update(self):
        with mock.patch.multiple(
            sm,
            ensure_user_agent_column=mock.DEFAULT, ensure_landing_page_columns=mock.DEFAULT,
            ensure_customer_tag_column=mock.DEFAULT, ensure_bundle_property_columns=mock.DEFAULT,
            ensure_device_summary_columns=mock.DEFAULT, ensure_ci_events_column=mock.DEFAULT,
            ensure_buy_now_events_column=mock.DEFAULT, ensure_overall_summary_event_columns=mock.DEFAULT,
            ensure_hour_wise_ppcod_column=mock.DEFAULT, ensure_hour_wise_payment_sales_columns=mock.DEFAULT,
            ensure_overall_summary_payment_sales_columns=mock.DEFAULT, ensure_utm_names_column=mock.DEFAULT,
            ensure_payment_unmapped_columns=mock.DEFAULT, ensure_payment_mode_unmapped_enum=mock.DEFAULT,
        ) as mocks:
            sm.run_brand_preflight_schema_migrations(7)
        called = [c.args for c in mocks["ensure_landing_page_columns"].call_args_list]
        self.assertEqual(called, [(7, "shopify_orders"), (7, "shopify_orders_update")])


if __name__ == "__main__":
    unittest.main()
