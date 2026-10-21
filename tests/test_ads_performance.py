import os
import unittest
from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

from workers import ads_performance as ap
from workers import pnl_worker as pw

D1 = date(2026, 10, 19)
D2 = date(2026, 10, 20)


def _response(payload, status=200):
    response = MagicMock()
    response.status_code = status
    response.json.return_value = payload
    return response


def _meta_insight(day, ad_id="a1", campaign_id="c1", **overrides):
    item = {
        "date_start": day.isoformat(),
        "campaign_id": campaign_id,
        "campaign_name": "TOF_Sales_CBO",
        "adset_id": "s1",
        "adset_name": "LLA",
        "ad_id": ad_id,
        "ad_name": "Ad 1",
        "spend": "100.50",
        "impressions": "1000",
        "clicks": "40",
        "inline_link_clicks": "30",
        "account_currency": "INR",
        "actions": [
            {"action_type": "landing_page_view", "value": "21"},
            {"action_type": "omni_purchase", "value": "3"},
            {"action_type": "offsite_conversion.fb_pixel_purchase", "value": "3"},
        ],
        "action_values": [
            {"action_type": "omni_purchase", "value": "450"},
            {"action_type": "offsite_conversion.fb_pixel_purchase", "value": "450"},
        ],
    }
    item.update(overrides)
    return item


class HelperTests(unittest.TestCase):
    def test_landing_path_is_lowercased_without_trailing_slash(self):
        self.assertEqual(ap.landing_path("https://x.com/Products/Deal-5/?utm=1"), "/products/deal-5")
        self.assertEqual(ap.landing_path("https://x.com"), "/")
        self.assertIsNone(ap.landing_path(None))

    def test_product_handle_handles_collection_scoped_urls(self):
        self.assertEqual(ap.product_handle("/collections/men/products/alpha-dusk"), "alpha-dusk")
        self.assertIsNone(ap.product_handle("/pages/diwali-sale"))

    def test_shopping_item_id_carries_the_shopify_product_id(self):
        self.assertEqual(ap.shopping_product_id("shopify_IN_9802714022183_47123"), 9802714022183)
        self.assertEqual(ap.shopping_product_id("SKU-123"), ap.UNATTRIBUTED_PRODUCT_ID)

    def test_creative_url_prefers_link_data_then_video_cta_then_feed(self):
        self.assertEqual(
            ap.creative_url({"object_story_spec": {"link_data": {"link": "https://a.com/products/x"}}}),
            "https://a.com/products/x",
        )
        self.assertEqual(
            ap.creative_url(
                {"object_story_spec": {"video_data": {"call_to_action": {"value": {"link": "https://a.com/v"}}}}}
            ),
            "https://a.com/v",
        )
        self.assertEqual(
            ap.creative_url({"asset_feed_spec": {"link_urls": [{"website_url": "https://a.com/f"}]}}),
            "https://a.com/f",
        )
        self.assertIsNone(ap.creative_url({"template_url_spec": {"web": {"url": "{{product.url}}"}}}))

    def test_overlapping_purchase_actions_are_not_double_counted(self):
        actions = _meta_insight(D1)["actions"]
        self.assertEqual(ap._first_action(actions, ap.META_PURCHASE_ACTIONS), Decimal("3"))


class MetaFetchTests(unittest.TestCase):
    def test_ad_insights_are_normalized_with_lc_lpv_and_purchases(self):
        with patch("requests.get", return_value=_response({"data": [_meta_insight(D1)]})) as get:
            rows = ap.fetch_meta_ad_insights("tok", "123", D1, D1)
        params = get.call_args.kwargs["params"]
        self.assertEqual(params["level"], "ad")
        self.assertIn("inline_link_clicks", params["fields"])
        self.assertTrue(get.call_args.args[0].endswith("/act_123/insights"))
        row = rows[0]
        self.assertEqual((row["spend"], row["link_clicks"], row["landing_page_views"]), (Decimal("100.50"), 30, 21))
        self.assertEqual((row["platform_purchases"], row["platform_purchase_value"]), (Decimal("3"), Decimal("450")))

    def test_metadata_failure_is_not_fatal(self):
        with patch("requests.get", return_value=_response({"error": {"message": "nope"}}, 400)):
            self.assertEqual(ap.fetch_meta_ad_metadata("tok", ["a1"]), {})

    def test_metadata_reads_campaign_status_and_destination(self):
        payload = {
            "a1": {
                "effective_status": "ACTIVE",
                "campaign": {"effective_status": "PAUSED", "objective": "OUTCOME_SALES"},
                "creative": {"object_story_spec": {"link_data": {"link": "https://b.com/products/deal-5"}}},
            }
        }
        with patch("requests.get", return_value=_response(payload)) as get:
            meta = ap.fetch_meta_ad_metadata("tok", ["a1", "a1"])
        self.assertEqual(get.call_args.kwargs["params"]["ids"], "a1")
        self.assertEqual(meta["a1"]["campaign_status"], "PAUSED")
        self.assertEqual(meta["a1"]["landing_url"], "https://b.com/products/deal-5")


class ShapeMetaTests(unittest.TestCase):
    def _fetched(self):
        with patch("requests.get", return_value=_response({"data": [
            _meta_insight(D1, ad_id="a1"),
            _meta_insight(D1, ad_id="a2", spend="50", landing_page_view=None),
            _meta_insight(D2, ad_id="a1"),
        ]})):
            ads = ap.fetch_meta_ad_insights("tok", "123", D1, D2)
        ads[0]["landing_url"] = "https://b.com/products/deal-5"
        ads[1]["landing_url"] = "https://b.com/pages/sale"
        ads[2]["landing_url"] = "https://b.com/products/deal-5"
        return {"ads": ads, "campaign_meta": {"c1": {"campaign_status": "ACTIVE", "campaign_type": "OUTCOME_SALES"}}}

    def test_campaign_rows_sum_their_ads_per_day(self):
        shaped = ap.shape_channel("meta", self._fetched(), {"deal-5": 77}, [])
        by_day = {row["date"]: row for row in shaped["campaigns"]}
        self.assertEqual(by_day[D1]["spend"], Decimal("150.50"))
        self.assertEqual(by_day[D1]["link_clicks"], 60)
        self.assertEqual(by_day[D1]["campaign_status"], "ACTIVE")

    def test_product_rows_follow_the_landing_product_and_sum_to_spend(self):
        shaped = ap.shape_channel("meta", self._fetched(), {"deal-5": 77}, [])
        d1 = {row["product_id"]: row for row in shaped["products"] if row["date"] == D1}
        self.assertEqual(d1[77]["spend"], Decimal("100.50"))
        self.assertEqual(d1[77]["source"], "creative")
        self.assertEqual(d1[0]["spend"], Decimal("50"))
        self.assertEqual(d1[0]["source"], "unattributed")

    def test_utm_orders_match_ad_id_first_then_campaign_name(self):
        utm = [
            {"date": D1, "utm_medium": "s1", "utm_campaign": "x", "utm_content": "a2", "utm_term": "", "total_orders": 2, "total_sales": "900"},
            {"date": D1, "utm_medium": "paid", "utm_campaign": "tof_sales_cbo", "utm_content": "", "utm_term": "", "total_orders": 1, "total_sales": "300"},
            {"date": D1, "utm_medium": "paid", "utm_campaign": "unknown", "utm_content": "", "utm_term": "", "total_orders": 9, "total_sales": "9999"},
        ]
        shaped = ap.shape_channel("meta", self._fetched(), {}, utm)
        camp = next(r for r in shaped["campaigns"] if r["date"] == D1)
        self.assertEqual((camp["utm_orders"], camp["utm_sales"]), (3, Decimal("1200")))
        ad2 = next(r for r in shaped["ads"] if r["ad_id"] == "a2")
        self.assertEqual(ad2["utm_orders"], 2)

    def test_orders_on_a_non_delivery_day_get_a_zero_spend_campaign_row(self):
        utm = [{"date": D1 - timedelta(days=1), "utm_medium": "", "utm_campaign": "c1", "utm_content": "", "utm_term": "", "total_orders": 1, "total_sales": "100"}]
        shaped = ap.shape_channel("meta", self._fetched(), {}, utm)
        extra = next(r for r in shaped["campaigns"] if r["date"] == D1 - timedelta(days=1))
        self.assertEqual((extra["spend"], extra["utm_sales"], extra["campaign_name"]), (Decimal("0"), Decimal("100"), "TOF_Sales_CBO"))

    def test_ambiguous_campaign_names_are_not_matched(self):
        index = ap.build_utm_index([], [
            {"campaign_id": "c1", "campaign_name": "Same"},
            {"campaign_id": "c2", "campaign_name": "Same"},
        ])
        self.assertIsNone(ap.match_utm_row({"utm_campaign": "same"}, index))
        self.assertEqual(ap.match_utm_row({"utm_campaign": "c2"}, index), ("c2", None))


class GoogleTests(unittest.TestCase):
    def test_customer_performance_reads_campaigns_ad_groups_and_shopping(self):
        campaign = {"results": [{
            "segments": {"date": "2026-10-19"},
            "campaign": {"id": "238", "name": "PMax", "status": "ENABLED", "advertisingChannelType": "PERFORMANCE_MAX"},
            "customer": {"currencyCode": "INR"},
            "metrics": {"costMicros": "300000000", "impressions": "900", "clicks": "30", "conversions": 4.5, "conversionsValue": 2400.0},
        }]}
        ad_group = {"results": [{
            "segments": {"date": "2026-10-19"},
            "campaign": {"id": "238"},
            "adGroup": {"id": "8084", "name": "AG"},
            "metrics": {"costMicros": "300000000", "impressions": "900", "clicks": "30"},
        }]}
        shopping = {"results": [{
            "segments": {"date": "2026-10-19", "productItemId": "shopify_in_77_1"},
            "metrics": {"costMicros": "120000000", "impressions": "400", "clicks": "10", "conversionsValue": 800},
        }]}
        with patch.object(pw, "GOOGLE_ADS_DEVELOPER_TOKEN", "dev"), patch(
            "requests.post", side_effect=[_response(campaign), _response(ad_group), _response(shopping)]
        ) as post:
            fetched = ap.fetch_google_customer_performance("access", "123-456", None, D1, D1)
        self.assertIn("FROM campaign", post.call_args_list[0].kwargs["json"]["query"])
        self.assertIn("FROM shopping_performance_view", post.call_args_list[2].kwargs["json"]["query"])
        self.assertEqual(fetched["campaigns"][0]["spend"], Decimal("300"))
        self.assertEqual(fetched["campaigns"][0]["platform_purchase_value"], Decimal("2400.0"))
        self.assertEqual(fetched["ads"][0]["ad_id"], "8084")

        shaped = ap.shape_channel("google", fetched, {}, [
            {"date": D1, "utm_medium": "cpc", "utm_campaign": "238", "utm_content": "8084", "utm_term": "", "total_orders": 2, "total_sales": "1000"},
        ])
        products = {row["product_id"]: row for row in shaped["products"]}
        self.assertEqual(products[77]["spend"], Decimal("120"))
        self.assertEqual(products[0]["spend"], Decimal("180"))
        self.assertEqual(sum(r["spend"] for r in shaped["products"]), Decimal("300"))
        self.assertEqual(shaped["campaigns"][0]["utm_sales"], Decimal("1000"))
        self.assertEqual(shaped["ads"][0]["utm_orders"], 2)

    def test_shopping_view_error_is_treated_as_no_shopping(self):
        empty = {"results": []}
        with patch.object(pw, "GOOGLE_ADS_DEVELOPER_TOKEN", "dev"), patch(
            "requests.post", side_effect=[_response(empty), _response(empty), _response({"error": {"message": "x"}}, 400)]
        ):
            fetched = ap.fetch_google_customer_performance("access", "1", None, D1, D1)
        self.assertEqual(fetched["shopping"], [])


class WriteTests(unittest.TestCase):
    def test_replace_window_deletes_the_channels_dates_then_inserts(self):
        cursor, connection = MagicMock(), MagicMock()
        row = {"date": D1, "channel": "meta", "product_id": 7, "source": "creative", "spend": Decimal("1.23456"),
               "impressions": 1, "clicks": 0, "platform_purchase_value": Decimal("0")}
        with patch("pipeline.db.executemany_chunked", return_value=1) as chunked:
            ap.replace_window(cursor, connection, ap.ADS_PRODUCT_TABLE, ap.PRODUCT_COLUMNS, "meta", D1, D2, [row])
        delete_sql, delete_args = cursor.execute.call_args[0]
        self.assertIn("DELETE FROM `ads_product_daily` WHERE `channel` = %s", delete_sql)
        self.assertEqual(delete_args, ("meta", D1, D2))
        sql, values = chunked.call_args[0][2], chunked.call_args[0][3]
        self.assertNotIn("`product_id` = VALUES", sql)
        self.assertEqual(values[0][4], Decimal("1.2346"))

    def test_missing_tables_skip_the_sync_without_fetching(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = None
        creds = {"meta": {"token_type": "system_user"}, "google": None}
        with patch.object(ap, "fetch_meta_performance") as fetch:
            outcomes = ap.sync_ads_performance(cursor, MagicMock(), "BBB", "BBB", creds, [D1])
        fetch.assert_not_called()
        self.assertEqual(outcomes, {"meta": "no-table", "google": "no-credentials"})

    def test_meta_user_token_is_not_synced(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = ("x",)
        with patch.object(ap, "fetch_meta_performance") as fetch:
            outcomes = ap.sync_ads_performance(cursor, MagicMock(), "BBB", "BBB", {"meta": {"token_type": "user"}}, [D1])
        fetch.assert_not_called()
        self.assertEqual(outcomes["meta"], "not-system-user")

    def test_api_failure_never_raises(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = ("x",)
        with patch.object(ap, "fetch_google_performance", side_effect=pw.AdSpendSyncError("boom")):
            outcomes = ap.sync_ads_performance(cursor, MagicMock(), "BBB", "BBB", {"google": {"customer_ids": ["1"]}}, [D1])
        self.assertEqual(outcomes["google"], "failed")

    def test_migration_creates_every_table_and_column(self):
        path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "migrations", "007_ads_performance.sql")
        with open(path) as fh:
            sql = fh.read()
        for table, columns in (
            (ap.ADS_CAMPAIGN_TABLE, ap.CAMPAIGN_COLUMNS),
            (ap.ADS_AD_TABLE, ap.AD_COLUMNS),
            (ap.ADS_PRODUCT_TABLE, ap.PRODUCT_COLUMNS),
        ):
            self.assertIn(f"CREATE TABLE IF NOT EXISTS `{table}`", sql)
            for column in columns:
                self.assertIn(f"`{column}`", sql)

    def test_worker_module_runs_no_ddl(self):
        import inspect
        import re

        src = inspect.getsource(ap)
        for verb in ("CREATE TABLE", "ALTER TABLE", "DROP TABLE"):
            self.assertIsNone(re.search(rf"\b{verb}\b", src))


class AlertRuleTests(unittest.TestCase):
    ROW = {"channel": "meta", "campaign_id": "c1", "spend": 1000, "impressions": 10000, "clicks": 200,
           "link_clicks": 150, "landing_page_views": 60, "platform_purchases": 4, "platform_purchase_value": 1500,
           "utm_sales": 2500}

    def test_metric_definitions(self):
        self.assertAlmostEqual(ap.metric_value("roas", self.ROW), 1.5)
        self.assertAlmostEqual(ap.metric_value("shopify_roas", self.ROW), 2.5)
        self.assertAlmostEqual(ap.metric_value("lpv_rate", self.ROW), 40.0)
        self.assertAlmostEqual(ap.metric_value("ctr", self.ROW), 2.0)
        self.assertAlmostEqual(ap.metric_value("cpc", self.ROW), 5.0)
        self.assertAlmostEqual(ap.metric_value("cpa", self.ROW), 250.0)
        self.assertIsNone(ap.metric_value("lpv_rate", {**self.ROW, "link_clicks": None}))

    def test_breach_respects_operator_channel_and_volume_gates(self):
        rule = {"metric": "roas", "operator": "below", "threshold": 2, "channel": "meta", "min_spend": 500}
        self.assertAlmostEqual(ap.evaluate_rule(rule, self.ROW), 1.5)
        self.assertIsNone(ap.evaluate_rule({**rule, "threshold": 1}, self.ROW))
        self.assertIsNone(ap.evaluate_rule({**rule, "channel": "google"}, self.ROW))
        self.assertIsNone(ap.evaluate_rule({**rule, "min_spend": 5000}, self.ROW))
        self.assertIsNone(ap.evaluate_rule({**rule, "is_active": False}, self.ROW))
        self.assertAlmostEqual(ap.evaluate_rule({"metric": "cpc", "operator": "above", "threshold": 4}, self.ROW), 5.0)
        self.assertIsNone(ap.evaluate_rule({"metric": "lpv_rate", "threshold": 50, "min_clicks": 500}, self.ROW))

    def _events_db(self, rules_doc, events):
        db = MagicMock()
        db.__getitem__.side_effect = lambda name: {
            ap.ALERT_RULES_COLLECTION: MagicMock(find_one=MagicMock(return_value=rules_doc)),
            ap.ALERT_EVENTS_COLLECTION: events,
        }[name]
        return db

    def test_new_breach_is_stored_once_unsent(self):
        rules_doc = {"rules": [{"rule_id": "r1", "metric": "roas", "operator": "below", "threshold": 2, "lookback_days": 3}]}
        events = MagicMock()
        events.update_one.return_value = MagicMock(upserted_id="new")
        db = self._events_db(rules_doc, events)
        cursor = MagicMock()
        cursor.fetchall.return_value = [{**self.ROW, "campaign_name": "TOF"}]
        with patch.object(pw, "PNL_CONFIG_MONGO_URI", "mongodb://x"), patch.object(
            ap, "_mongo_db", return_value=(MagicMock(), db)
        ), patch.object(ap, "deliver_pending_alerts") as deliver:
            created = ap.evaluate_brand_alerts(cursor, "BBB", "BBB", D2)
        self.assertEqual(created, 1)
        key_filter, update = events.update_one.call_args[0][:2]
        self.assertEqual(key_filter, {"event_key": "BBB:r1:meta:c1:2026-10-20"})
        self.assertIsNone(update["$setOnInsert"]["pushed_at"])
        self.assertEqual(cursor.execute.call_args[0][1], (D2 - timedelta(days=2), D2))
        deliver.assert_called_once_with(db, "BBB", "BBB")

        # Re-running the same night records nothing new but still delivers.
        events.update_one.return_value = MagicMock(upserted_id=None)
        with patch.object(pw, "PNL_CONFIG_MONGO_URI", "mongodb://x"), patch.object(
            ap, "_mongo_db", return_value=(MagicMock(), db)
        ), patch.object(ap, "deliver_pending_alerts") as deliver:
            self.assertEqual(ap.evaluate_brand_alerts(cursor, "BBB", "BBB", D2), 0)
        deliver.assert_called_once()

    def test_pending_events_are_stamped_only_when_accepted(self):
        events = MagicMock()
        events.find.return_value.sort.return_value = [{"_id": 1, "event_key": "a"}, {"_id": 2, "event_key": "b"}]
        db = self._events_db({}, events)
        with patch.object(ap, "ALERTS_PUSH_URL", "http://alerts/push/receive"), patch.object(
            ap, "ALERTS_PUSH_TOKEN", "t"
        ), patch.object(ap, "push_alert", side_effect=[True, False]):
            sent = ap.deliver_pending_alerts(db, "TMC", "TMC")
        self.assertEqual(sent, 1)
        query = events.find.call_args[0][0]
        self.assertEqual((query["brand_id"], query["pushed_at"]), ("TMC", None))
        self.assertEqual(events.update_one.call_count, 1)
        self.assertEqual(events.update_one.call_args[0][0], {"_id": 1})

    def test_pending_delivery_is_a_noop_until_configured(self):
        events = MagicMock()
        with patch.object(ap, "ALERTS_PUSH_URL", ""):
            self.assertEqual(ap.deliver_pending_alerts(self._events_db({}, events), "TMC", "TMC"), 0)
        events.find.assert_not_called()

    def test_rejected_push_is_not_success(self):
        event = {"event_key": "k", "channel": "google", "metric": "roas", "operator": "below", "threshold": 2.0,
                 "value": 1.5, "campaign_id": "238", "lookback_days": 3, "window_start": "a", "window_end": "b"}
        with patch.object(ap, "ALERTS_PUSH_URL", "http://x"), patch.object(ap, "ALERTS_PUSH_TOKEN", "t"), patch(
            "requests.post", return_value=MagicMock(status_code=401)
        ):
            self.assertFalse(ap.push_alert(event, "TMC"))
        with patch.object(ap, "ALERTS_PUSH_URL", "http://x"), patch.object(ap, "ALERTS_PUSH_TOKEN", "t"), patch(
            "requests.post", return_value=MagicMock(status_code=200)
        ):
            self.assertTrue(ap.push_alert(event, "TMC"))

    def test_push_is_a_noop_until_configured(self):
        with patch.object(ap, "ALERTS_PUSH_URL", ""), patch("requests.post") as post:
            ap.push_alert({"threshold": 2, "value": 1}, "BBB")
        post.assert_not_called()

    def test_push_payload_matches_alerts_service_shape(self):
        event = {"event_key": "k", "channel": "google", "metric": "roas", "operator": "below", "threshold": 2.0,
                 "value": 1.5, "campaign_id": "238", "campaign_name": "PMax", "lookback_days": 3,
                 "window_start": "2026-10-18", "window_end": "2026-10-20"}
        with patch.object(ap, "ALERTS_PUSH_URL", "http://alerts/push/receive"), patch.object(
            ap, "ALERTS_PUSH_TOKEN", "t"
        ), patch("requests.post", return_value=MagicMock(status_code=200)) as post:
            self.assertTrue(ap.push_alert(event, "BBB"))
        body = post.call_args.kwargs["json"]["event"]
        self.assertEqual(post.call_args.kwargs["headers"], {"x-push-token": "t"})
        self.assertEqual((body["brand"], body["threshold_type"], body["delta_percent"]), ("BBB", "less_than", -25.0))
        self.assertEqual(body["metric"], "Google ROAS - PMax")


if __name__ == "__main__":
    unittest.main()
