import base64
import os
import unittest
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from workers import pnl_worker


IST = ZoneInfo("Asia/Kolkata")


def _doc(gst_pct=18, **costs):
    """A total_config document with the given cost lines; e.g.
    _doc(cogs=("percentage", 30), rent=("flat", 3000), meta=None)."""
    return {
        "brand_id": "BBB",
        "gst_pct": gst_pct,
        "costs": {
            key: (None if spec is None else {"value_type": spec[0], "value": spec[1]})
            for key, spec in costs.items()
        },
    }


def _config(**kwargs):
    return pnl_worker.build_brand_config("BBB", _doc(**kwargs))


DEFAULTS = pnl_worker.build_brand_config("BBB", None)


class TargetDatesTests(unittest.TestCase):
    def test_default_window_is_yesterday_and_day_before(self):
        now = datetime(2026, 9, 15, 0, 10, tzinfo=IST)
        self.assertEqual(
            pnl_worker.compute_target_dates(IST, current_dt=now, lookback_days=2, start_date="", end_date=""),
            [date(2026, 9, 13), date(2026, 9, 14)],
        )

    def test_explicit_range_wins(self):
        self.assertEqual(
            pnl_worker.compute_target_dates(IST, lookback_days=2, start_date="2026-09-01", end_date="2026-09-03"),
            [date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 3)],
        )


class BrandConfigTests(unittest.TestCase):
    def test_missing_document_means_all_defaults(self):
        self.assertEqual(DEFAULTS.source, pnl_worker.CONFIG_SOURCE_DEFAULT)
        self.assertEqual(DEFAULTS.gst_pct, Decimal("18"))
        self.assertEqual(DEFAULTS.defaulted, set(pnl_worker.COST_COLUMNS))
        self.assertEqual(set(DEFAULTS.costs), set(pnl_worker.COST_COLUMNS))
        self.assertEqual(DEFAULTS.costs["cogs"], {"value_type": "percentage", "value": Decimal("32")})

    def test_document_lines_win_and_null_lines_fall_back_per_line(self):
        config = _config(gst_pct=12, cogs=("percentage", 30), rent=("flat", 90000), meta=None)
        self.assertEqual(config.source, pnl_worker.CONFIG_SOURCE_MONGO)
        self.assertEqual(config.gst_pct, Decimal("12"))
        self.assertEqual(config.costs["cogs"], {"value_type": "percentage", "value": Decimal("30")})
        self.assertEqual(config.costs["rent"], {"value_type": "flat", "value": Decimal("90000")})
        self.assertEqual(config.costs["meta"], pnl_worker.default_cost_line("meta"))
        self.assertIn("meta", config.defaulted)
        self.assertIn("salaries", config.defaulted)  # absent key, not just null
        self.assertNotIn("cogs", config.defaulted)

    def test_unusable_lines_and_gst_fall_back(self):
        config = _config(gst_pct=250, cogs=("weird", 30), rent=("flat", "abc"), shipping=("flat", -5))
        self.assertEqual(config.gst_pct, Decimal("18"))
        for column in ("cogs", "rent", "shipping"):
            self.assertIn(column, config.defaulted)

    def test_gst_rate_is_inclusive_share(self):
        self.assertEqual(_config(gst_pct=18).gst_rate, Decimal(18) / Decimal(118))
        self.assertEqual(_config(gst_pct=0).gst_rate, Decimal(0))

    def test_percentage_is_share_of_net_sales(self):
        line = {"value_type": "percentage", "value": Decimal("32")}
        self.assertEqual(pnl_worker.cost_line_amount(line, date(2026, 9, 15), Decimal("1000.00")), Decimal("320.00"))

    def test_flat_is_a_monthly_amount_prorated_per_day(self):
        line = {"value_type": "flat", "value": Decimal("3000")}
        self.assertEqual(pnl_worker.cost_line_amount(line, date(2026, 9, 15), Decimal("0")), Decimal("100.00"))
        self.assertEqual(pnl_worker.cost_line_amount(line, date(2026, 2, 1), Decimal("0")), Decimal("107.14"))


class ConfigFetchTests(unittest.TestCase):
    def test_reads_one_document_by_brand_id_from_total_config(self):
        fake_client = MagicMock()
        collection = fake_client.__getitem__.return_value.__getitem__.return_value
        collection.find_one.return_value = _doc(cogs=("percentage", 30))
        with patch.object(pnl_worker, "PNL_CONFIG_MONGO_URI", "mongodb://x"), patch(
            "pymongo.MongoClient", return_value=fake_client
        ):
            doc = pnl_worker.fetch_total_config_doc("BBB")
        self.assertEqual(doc["costs"]["cogs"]["value"], 30)
        self.assertEqual(collection.find_one.call_args[0][0], {"brand_id": "BBB"})
        self.assertEqual(pnl_worker.PNL_CONFIG_COLLECTION, "total_config")
        fake_client.close.assert_called_once()

    def test_missing_mongo_uri_falls_back_to_defaults(self):
        with patch.object(pnl_worker, "PNL_CONFIG_MONGO_URI", ""):
            self.assertIsNone(pnl_worker.fetch_total_config_doc("BBB"))

    def test_resolve_returns_defaults_when_brand_has_no_document(self):
        with patch.object(pnl_worker, "fetch_total_config_doc", return_value=None):
            config = pnl_worker.resolve_brand_config("BBB")
        self.assertEqual(config.source, pnl_worker.CONFIG_SOURCE_DEFAULT)


class PnlRowTests(unittest.TestCase):
    def test_cascade_matches_dashboard_model(self):
        day = date(2026, 9, 15)
        sales = {
            "gross_sales": "1180.00",
            "discounts": "100.00",
            "total_orders": 7,
            "cod_sales": "300.00",
            "prepaid_sales": "800.00",
            "partially_prepaid_sales": "80.00",
            "rto_orders": 1,
        }
        returns = {"cancellations": Decimal("80.00"), "rto_returns": Decimal("50.00")}
        config = _config(
            cogs=("percentage", 30),
            rent=("flat", 3000),
            meta=("flat", 999),
            **{c: ("flat", 0) for c in pnl_worker.COST_COLUMNS if c not in ("cogs", "rent", "meta")},
        )

        row = pnl_worker.compute_pnl_row(day, sales, returns, Decimal("120.00"), config, brand_key="BBB")
        self.assertEqual(row["google_spend_synced"], 0)

        # (1180 - 100 - 80) = 1000 taxable -> GST 152.54 -> net 847.46
        self.assertEqual(row["gst"], Decimal("152.54"))
        self.assertEqual(row["gst_pct"], Decimal("18.00"))
        self.assertEqual(row["net_sales"], Decimal("847.46"))
        self.assertEqual(row["cogs"], Decimal("254.24"))
        self.assertEqual(row["gross_margin"], Decimal("593.22"))
        self.assertEqual(row["cm1"], Decimal("593.22"))
        # synced Meta spend beats the configured 999
        self.assertEqual(row["meta"], Decimal("120.00"))
        self.assertEqual(row["meta_spend_synced"], 1)
        self.assertEqual(row["cm2"], Decimal("473.22"))
        self.assertEqual(row["cm3"], Decimal("473.22"))
        self.assertEqual(row["rent"], Decimal("100.00"))
        self.assertEqual(row["ebitda"], Decimal("373.22"))
        self.assertEqual(row["rto_returns"], Decimal("50.00"))
        self.assertEqual(row["total_orders"], 7)
        self.assertEqual(row["rto_orders"], 1)
        self.assertEqual(row["brand_key"], "BBB")
        self.assertEqual(row["cost_config_source"], "mongo")

    def test_gst_pct_from_config_drives_the_gst_line(self):
        sales = {"gross_sales": "1120.00", "discounts": "0"}
        config = _config(gst_pct=12, **{c: ("flat", 0) for c in pnl_worker.COST_COLUMNS})
        row = pnl_worker.compute_pnl_row(date(2026, 9, 15), sales, None, None, config)
        # 1120 * 12/112 = 120
        self.assertEqual(row["gst"], Decimal("120.00"))
        self.assertEqual(row["net_sales"], Decimal("1000.00"))
        self.assertEqual(row["gst_pct"], Decimal("12.00"))

    def test_day_without_orders_still_carries_fixed_costs(self):
        config = _config(rent=("flat", 3000), **{c: ("flat", 0) for c in pnl_worker.COST_COLUMNS if c != "rent"})
        row = pnl_worker.compute_pnl_row(date(2026, 9, 15), None, None, None, config)
        self.assertEqual(row["net_sales"], Decimal("0.00"))
        self.assertEqual(row["rent"], Decimal("100.00"))
        self.assertEqual(row["ebitda"], Decimal("-100.00"))
        self.assertEqual(row["meta"], Decimal("0.00"))
        self.assertEqual(row["meta_spend_synced"], 0)

    def test_synced_google_spend_beats_the_configured_google_cost(self):
        sales = {"gross_sales": "1180.00", "discounts": "0", "total_orders": 1}
        config = _config(google=("percentage", 50), **{c: ("flat", 0) for c in pnl_worker.COST_COLUMNS if c != "google"})
        unsynced = pnl_worker.compute_pnl_row(date(2026, 9, 15), sales, None, None, config)
        self.assertEqual(unsynced["google"], Decimal("500.00"))
        self.assertEqual(unsynced["google_spend_synced"], 0)

        row = pnl_worker.compute_pnl_row(date(2026, 9, 15), sales, None, None, config, google_spend=Decimal("42.50"))
        self.assertEqual(row["google"], Decimal("42.50"))
        self.assertEqual(row["google_spend_synced"], 1)
        self.assertEqual(row["cm2"], Decimal("957.50"))
        self.assertEqual(row["ebitda"], Decimal("957.50"))

    def test_row_covers_every_table_column(self):
        row = pnl_worker.compute_pnl_row(date(2026, 9, 15), None, None, None, DEFAULTS, brand_key="BBB")
        self.assertEqual(set(row), set(pnl_worker.ALL_COLUMNS))
        self.assertEqual(row["cost_config_source"], pnl_worker.CONFIG_SOURCE_DEFAULT)

    def test_all_defaults_cost_every_line_as_percent_of_net_sales(self):
        sales = {"gross_sales": "1180.00", "discounts": "0", "total_orders": 1}
        row = pnl_worker.compute_pnl_row(date(2026, 9, 15), sales, None, None, DEFAULTS)
        # 1180 taxable -> GST 180 -> net 1000, so each default % is the amount
        self.assertEqual(row["net_sales"], Decimal("1000.00"))
        self.assertEqual(row["cogs"], Decimal("320.00"))
        self.assertEqual(row["packaging"], Decimal("15.00"))
        self.assertEqual(row["meta"], Decimal("80.00"))
        self.assertEqual(row["cost_config_source"], "default")
        total_pct = sum(Decimal(v) for v in pnl_worker.DEFAULT_COST_PERCENTAGES.values())
        self.assertEqual(row["ebitda"], Decimal("1000.00") - Decimal("10") * total_pct)


class ReturnsRowsTests(unittest.TestCase):
    def test_cancels_refunds_and_rto_are_separate_buckets(self):
        cursor = MagicMock()
        cursor.fetchall.side_effect = [
            [{"d": date(2026, 9, 15), "cancel_amount": "300.00", "rto_amount": "120.00"}],
            [{"d": date(2026, 9, 15), "refund_amount": "45.50"}, {"d": date(2026, 9, 16), "refund_amount": "10.00"}],
        ]
        rows = pnl_worker.fetch_returns_rows(cursor, date(2026, 9, 15), date(2026, 9, 16))

        self.assertEqual(rows[date(2026, 9, 15)], {"cancellations": Decimal("345.50"), "rto_returns": Decimal("120.00")})
        self.assertEqual(rows[date(2026, 9, 16)], {"cancellations": Decimal("10.00"), "rto_returns": Decimal("0")})
        refund_sql = cursor.execute.call_args_list[1][0][0]
        self.assertIn("NOT EXISTS", refund_sql)


class TableOwnershipTests(unittest.TestCase):
    def test_worker_runs_no_ddl(self):
        """Table creation lives in migrations/003_overall_pnl.sql, not here."""
        import inspect
        import re

        self.assertEqual(pnl_worker.PNL_TABLE, "overall_pnl")
        self.assertFalse(hasattr(pnl_worker, "ensure_overall_pnl_table"))
        src = inspect.getsource(pnl_worker)
        for verb in ("CREATE TABLE", "ALTER TABLE", "DROP TABLE"):
            self.assertIsNone(re.search(rf"\b{verb}\b", src, re.IGNORECASE), f"worker contains {verb}")

    def test_migration_creates_overall_pnl_if_not_exists(self):
        migration_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "migrations", "003_overall_pnl.sql")
        with open(migration_path) as fh:
            sql = fh.read()
        self.assertIn("CREATE TABLE IF NOT EXISTS `overall_pnl`", sql)
        self.assertIn("PRIMARY KEY (`brand_key`, `date`)", sql)
        for column in pnl_worker.ALL_COLUMNS:
            self.assertIn(f"`{column}`", sql, f"migration is missing column {column}")

    def test_rollup_migration_creates_both_spend_tables(self):
        migration_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "migrations", "004_pnl_ad_spend_rollups.sql"
        )
        with open(migration_path) as fh:
            sql = fh.read()
        for channel, table in pnl_worker.SPEND_TABLES.items():
            self.assertIn(f"CREATE TABLE IF NOT EXISTS `{table}`", sql)
            self.assertIn(f"`{pnl_worker.SPEND_ACCOUNT_COLUMNS[channel]}`", sql)
        self.assertIn("UNIQUE KEY `uniq_pnl_meta_ad_spend_date` (`date`)", sql)
        self.assertIn("UNIQUE KEY `uniq_pnl_google_ad_spend_date` (`date`)", sql)

    def test_spend_rows_come_from_the_named_rollup_table(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = ("pnl_google_ad_spend_rollup",)
        cursor.fetchall.return_value = [{"date": date(2026, 9, 15), "spend": "12.3456"}]
        rows = pnl_worker.fetch_spend_rows(cursor, pnl_worker.GOOGLE_SPEND_TABLE, date(2026, 9, 15), date(2026, 9, 15))
        self.assertEqual(rows, {date(2026, 9, 15): Decimal("12.35")})
        self.assertIn("FROM `pnl_google_ad_spend_rollup`", cursor.execute.call_args_list[1][0][0])

    def test_spend_rows_are_empty_when_the_table_is_missing(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = None
        self.assertEqual(pnl_worker.fetch_spend_rows(cursor, pnl_worker.META_SPEND_TABLE, date(2026, 9, 15), date(2026, 9, 15)), {})
        self.assertEqual(cursor.execute.call_count, 1)

    def test_upsert_never_overwrites_key_columns(self):
        cursor, connection = MagicMock(), MagicMock()
        row = pnl_worker.compute_pnl_row(date(2026, 9, 15), None, None, None, DEFAULTS, brand_key="BBB")
        with patch("pipeline.db.executemany_chunked", return_value=1) as chunked:
            pnl_worker.upsert_pnl_rows(cursor, connection, [row])
        sql, values = chunked.call_args[0][2], chunked.call_args[0][3]
        self.assertIn("INSERT INTO `overall_pnl`", sql)
        self.assertNotIn("`brand_key` = VALUES", sql)
        self.assertNotIn("`date` = VALUES", sql)
        self.assertEqual(values[0][:2], ("BBB", date(2026, 9, 15)))

    def test_source_reads_never_write(self):
        import inspect
        import re

        for fn in (pnl_worker.fetch_sales_rows, pnl_worker.fetch_returns_rows, pnl_worker.fetch_spend_rows):
            src = inspect.getsource(fn)
            for verb in ("INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP", "TRUNCATE"):
                self.assertIsNone(re.search(rf"\b{verb}\b", src, re.IGNORECASE), f"{fn.__name__} contains {verb}")


# Encrypted with the dashboard's analytics/shared/utils/crypto.js
# (encryptText) using PASSWORD_AES_KEY=unit-test-key.
NODE_ENCRYPTED_TOKEN = "gOkHByIz/ydTiX6tAYo1IQ==:/rAWHxTfYurcrcFsCGmdwEtXL4B+HakyIGgYe8EyQMI="


def _node_style_encrypt(plain: str, key: str) -> str:
    iv = b"\x01" * 16
    padder = padding.PKCS7(128).padder()
    padded = padder.update(plain.encode("utf-8")) + padder.finalize()
    encryptor = Cipher(algorithms.AES(pnl_worker._aes_key(key)), modes.CBC(iv)).encryptor()
    body = encryptor.update(padded) + encryptor.finalize()
    return f"{base64.b64encode(iv).decode()}:{base64.b64encode(body).decode()}"


class DecryptTests(unittest.TestCase):
    def test_decrypts_what_the_dashboard_encrypted(self):
        self.assertEqual(pnl_worker.decrypt_text(NODE_ENCRYPTED_TOKEN, key="unit-test-key"), "EAAB-example-token-123")

    def test_key_is_cut_or_zero_padded_to_32_bytes_like_crypto_js(self):
        long_key = "k" * 40
        self.assertEqual(pnl_worker.decrypt_text(_node_style_encrypt("tok", long_key), key=long_key[:32]), "tok")
        self.assertEqual(pnl_worker.decrypt_text(_node_style_encrypt("tok", "short"), key="short"), "tok")

    def test_missing_key_or_bad_format_is_a_sync_error(self):
        with patch.object(pnl_worker, "PASSWORD_AES_KEY", ""):
            with self.assertRaises(pnl_worker.AdSpendSyncError):
                pnl_worker.decrypt_text(NODE_ENCRYPTED_TOKEN)
        with self.assertRaises(pnl_worker.AdSpendSyncError):
            pnl_worker.decrypt_text("not-encrypted", key="unit-test-key")


class CredentialFetchTests(unittest.TestCase):
    def _client(self, docs_by_collection):
        fake_client = MagicMock()
        db = fake_client.__getitem__.return_value
        collections = {}

        def collection(name):
            coll = collections.setdefault(name, MagicMock())
            coll.find_one.return_value = docs_by_collection.get(name)
            return coll

        db.__getitem__.side_effect = collection
        return fake_client, collections

    def test_reads_one_document_per_channel_by_brand_id(self):
        meta_doc = {"brand_id": "BBB", "ad_account_id": "act_1", "access_token_encrypted": "x:y"}
        fake_client, collections = self._client({"meta_ads_credentials": meta_doc})
        with patch.object(pnl_worker, "PNL_CONFIG_MONGO_URI", "mongodb://x"), patch(
            "pymongo.MongoClient", return_value=fake_client
        ):
            creds = pnl_worker.fetch_ad_credential_docs("BBB")
        self.assertEqual(creds, {"meta": meta_doc, "google": None})
        for name in ("meta_ads_credentials", "google_ads_credentials"):
            self.assertEqual(collections[name].find_one.call_args[0][0], {"brand_id": "BBB"})
        fake_client.close.assert_called_once()

    def test_no_mongo_means_no_credentials(self):
        with patch.object(pnl_worker, "PNL_CONFIG_MONGO_URI", ""):
            self.assertEqual(pnl_worker.fetch_ad_credential_docs("BBB"), {"meta": None, "google": None})

    def test_record_error_writes_last_error_on_the_channel_document(self):
        fake_client, collections = self._client({})
        with patch.object(pnl_worker, "PNL_CONFIG_MONGO_URI", "mongodb://x"), patch(
            "pymongo.MongoClient", return_value=fake_client
        ):
            pnl_worker.record_credential_error("BBB", "google", "boom")
            pnl_worker.record_credential_error("BBB", "google", None)
        calls = collections["google_ads_credentials"].update_one.call_args_list
        self.assertEqual(calls[0][0], ({"brand_id": "BBB"}, {"$set": {"last_error": "boom"}}))
        self.assertEqual(calls[1][0], ({"brand_id": "BBB"}, {"$set": {"last_error": None}}))


def _response(payload, status=200):
    response = MagicMock()
    response.status_code = status
    response.json.return_value = payload
    return response


class MetaSpendApiTests(unittest.TestCase):
    def test_pulls_daily_account_spend_for_exactly_the_range(self):
        page1 = {
            "data": [
                {"date_start": "2026-09-14", "date_stop": "2026-09-14", "spend": "100.50", "account_currency": "INR"},
            ],
            "paging": {"next": "https://graph.facebook.com/next-page"},
        }
        page2 = {"data": [{"date_start": "2026-09-15", "date_stop": "2026-09-15", "spend": "20", "account_currency": "INR"}]}
        with patch("requests.get", side_effect=[_response(page1), _response(page2)]) as get:
            rows = pnl_worker.fetch_meta_spend_from_api("tok", "123", date(2026, 9, 14), date(2026, 9, 15))

        self.assertEqual(rows[date(2026, 9, 14)], {"spend": Decimal("100.50"), "currency": "INR", "account_id": "act_123"})
        self.assertEqual(rows[date(2026, 9, 15)]["spend"], Decimal("20.00"))
        first_url, first_kwargs = get.call_args_list[0][0][0], get.call_args_list[0][1]
        self.assertIn("/act_123/insights", first_url)
        self.assertEqual(first_kwargs["params"]["time_increment"], 1)
        self.assertEqual(first_kwargs["params"]["time_range"], '{"since": "2026-09-14", "until": "2026-09-15"}')
        self.assertEqual(first_kwargs["params"]["access_token"], "tok")
        # paging.next is followed verbatim, without re-sending params
        self.assertEqual(get.call_args_list[1][0][0], "https://graph.facebook.com/next-page")
        self.assertIsNone(get.call_args_list[1][1]["params"])

    def test_meta_error_becomes_a_sync_error_with_metas_message(self):
        payload = {"error": {"message": "Invalid OAuth access token.", "code": 190}}
        with patch("requests.get", return_value=_response(payload, status=400)):
            with self.assertRaises(pnl_worker.AdSpendSyncError) as ctx:
                pnl_worker.fetch_meta_spend_from_api("tok", "act_1", date(2026, 9, 14), date(2026, 9, 14))
        self.assertIn("Invalid OAuth access token.", str(ctx.exception))


class GoogleSpendApiTests(unittest.TestCase):
    def _env(self):
        return patch.multiple(
            pnl_worker,
            GOOGLE_ADS_CLIENT_ID="cid",
            GOOGLE_ADS_CLIENT_SECRET="sec",
            GOOGLE_ADS_DEVELOPER_TOKEN="dev",
        )

    def test_mints_an_access_token_then_sums_cost_micros_per_day(self):
        token_payload = {"access_token": "at", "expires_in": 3599}
        search1 = {
            "results": [
                {"segments": {"date": "2026-09-14"}, "metrics": {"costMicros": "1500000"}, "customer": {"currencyCode": "INR"}},
                {"segments": {"date": "2026-09-14"}, "metrics": {"costMicros": "500000"}, "customer": {"currencyCode": "INR"}},
            ],
            "nextPageToken": "p2",
        }
        search2 = {"results": [{"segments": {"date": "2026-09-15"}, "metrics": {"costMicros": "10"}, "customer": {"currencyCode": "INR"}}]}
        with self._env(), patch(
            "requests.post", side_effect=[_response(token_payload), _response(search1), _response(search2)]
        ) as post:
            rows = pnl_worker.fetch_google_spend_from_api("rt", "123-456-7890", "987-654-3210", date(2026, 9, 14), date(2026, 9, 15))

        self.assertEqual(rows[date(2026, 9, 14)], {"spend": Decimal("2.00"), "currency": "INR", "account_id": "1234567890"})
        self.assertEqual(rows[date(2026, 9, 15)]["spend"], Decimal("0.00"))
        token_call = post.call_args_list[0]
        self.assertEqual(token_call[0][0], pnl_worker.GOOGLE_OAUTH_TOKEN_URL)
        self.assertEqual(token_call[1]["data"]["refresh_token"], "rt")
        search_call = post.call_args_list[1]
        self.assertIn("/customers/1234567890/googleAds:search", search_call[0][0])
        self.assertEqual(search_call[1]["headers"]["developer-token"], "dev")
        self.assertEqual(search_call[1]["headers"]["login-customer-id"], "9876543210")
        self.assertIn("BETWEEN '2026-09-14' AND '2026-09-15'", search_call[1]["json"]["query"])
        self.assertEqual(post.call_args_list[2][1]["json"]["pageToken"], "p2")

    def test_missing_worker_side_google_config_is_a_sync_error(self):
        with patch.multiple(pnl_worker, GOOGLE_ADS_DEVELOPER_TOKEN=""):
            with self.assertRaises(pnl_worker.AdSpendSyncError):
                pnl_worker.fetch_google_spend_from_api("rt", "1234567890", None, date(2026, 9, 14), date(2026, 9, 14))
        with patch.multiple(pnl_worker, GOOGLE_ADS_DEVELOPER_TOKEN="dev", GOOGLE_ADS_CLIENT_ID="", GOOGLE_ADS_CLIENT_SECRET=""):
            with self.assertRaises(pnl_worker.AdSpendSyncError):
                pnl_worker.fetch_google_spend_from_api("rt", "1234567890", None, date(2026, 9, 14), date(2026, 9, 14))

    def test_google_error_uses_the_nested_message(self):
        payload = [{"error": {"message": "Request contains an invalid argument.", "details": [{"errors": [{"message": "The developer token is not approved."}]}]}}]
        with self._env(), patch("requests.post", side_effect=[_response({"access_token": "at"}), _response(payload, status=403)]):
            with self.assertRaises(pnl_worker.AdSpendSyncError) as ctx:
                pnl_worker.fetch_google_spend_from_api("rt", "1234567890", None, date(2026, 9, 14), date(2026, 9, 14))
        self.assertIn("developer token is not approved", str(ctx.exception))


class AdSpendSyncTests(unittest.TestCase):
    DATES = [date(2026, 9, 14), date(2026, 9, 15)]

    def test_channels_without_credentials_are_not_fetched(self):
        cursor, connection = MagicMock(), MagicMock()
        with patch.object(pnl_worker, "fetch_channel_spend") as fetch:
            outcomes = pnl_worker.sync_ad_spend(cursor, connection, "BBB", "Brand", {"meta": None, "google": None}, self.DATES)
        self.assertEqual(outcomes, {"meta": "no-credentials", "google": "no-credentials"})
        fetch.assert_not_called()
        cursor.execute.assert_not_called()

    def test_synced_channel_writes_one_row_per_target_date_and_zero_fills(self):
        cursor, connection = MagicMock(), MagicMock()
        cursor.fetchone.return_value = ("pnl_meta_ad_spend_rollup",)
        api_rows = {date(2026, 9, 15): {"spend": Decimal("80.00"), "currency": "INR", "account_id": "act_1"}}
        creds = {"meta": {"ad_account_id": "1", "access_token_encrypted": "x:y", "token_type": "system_user"}, "google": None}
        with patch.object(pnl_worker, "fetch_channel_spend", return_value=api_rows) as fetch, patch(
            "pipeline.db.executemany_chunked", return_value=2
        ) as chunked, patch.object(pnl_worker, "record_credential_error") as record:
            outcomes = pnl_worker.sync_ad_spend(cursor, connection, "BBB", "Brand", creds, self.DATES)

        self.assertEqual(outcomes["meta"], "synced")
        self.assertEqual(fetch.call_args[0][0], "meta")
        self.assertEqual(fetch.call_args[0][2:], (date(2026, 9, 14), date(2026, 9, 15)))
        sql, values = chunked.call_args[0][2], chunked.call_args[0][3]
        self.assertIn("INSERT INTO `pnl_meta_ad_spend_rollup`", sql)
        self.assertIn("ON DUPLICATE KEY UPDATE", sql)
        self.assertEqual(
            values,
            [
                (date(2026, 9, 14), "act_1", Decimal("0.00"), "INR"),
                (date(2026, 9, 15), "act_1", Decimal("80.00"), "INR"),
            ],
        )
        record.assert_called_once_with("BBB", "meta", None)

    def test_meta_oauth_user_token_is_skipped_until_reconnected_with_system_user(self):
        cursor, connection = MagicMock(), MagicMock()
        creds = {"meta": {"ad_account_id": "1", "access_token_encrypted": "x:y", "token_type": "user"}, "google": None}
        with patch.object(pnl_worker, "fetch_channel_spend") as fetch, patch.object(
            pnl_worker, "record_credential_error"
        ) as record:
            outcomes = pnl_worker.sync_ad_spend(cursor, connection, "BBB", "Brand", creds, self.DATES)
        self.assertEqual(outcomes["meta"], "not-system-user")
        fetch.assert_not_called()
        cursor.execute.assert_not_called()
        record.assert_called_once()
        self.assertEqual(record.call_args[0][0], "BBB")
        self.assertEqual(record.call_args[0][1], "meta")
        self.assertIn("System User token", record.call_args[0][2])

    def test_meta_credential_missing_token_type_is_treated_as_user(self):
        cursor, connection = MagicMock(), MagicMock()
        creds = {"meta": {"ad_account_id": "1", "access_token_encrypted": "x:y"}, "google": None}
        with patch.object(pnl_worker, "fetch_channel_spend") as fetch:
            outcomes = pnl_worker.sync_ad_spend(cursor, connection, "BBB", "Brand", creds, self.DATES)
        self.assertEqual(outcomes["meta"], "not-system-user")
        fetch.assert_not_called()

    def test_missing_rollup_table_skips_the_channel(self):
        cursor, connection = MagicMock(), MagicMock()
        cursor.fetchone.return_value = None
        creds = {"meta": None, "google": {"customer_id": "1234567890", "token_encrypted": "x:y"}}
        with patch.object(pnl_worker, "fetch_channel_spend") as fetch:
            outcomes = pnl_worker.sync_ad_spend(cursor, connection, "BBB", "Brand", creds, self.DATES)
        self.assertEqual(outcomes["google"], "no-table")
        fetch.assert_not_called()

    def test_api_failure_is_recorded_and_does_not_raise(self):
        cursor, connection = MagicMock(), MagicMock()
        cursor.fetchone.return_value = ("pnl_google_ad_spend_rollup",)
        creds = {"meta": None, "google": {"customer_id": "1234567890", "token_encrypted": "x:y"}}
        with patch.object(
            pnl_worker, "fetch_channel_spend", side_effect=pnl_worker.AdSpendSyncError("Google Ads: token revoked")
        ), patch.object(pnl_worker, "record_credential_error") as record, patch("pipeline.db.executemany_chunked") as chunked:
            outcomes = pnl_worker.sync_ad_spend(cursor, connection, "BBB", "Brand", creds, self.DATES)
        self.assertEqual(outcomes["google"], "failed")
        record.assert_called_once_with("BBB", "google", "Google Ads: token revoked")
        chunked.assert_not_called()

    def test_channel_spend_decrypts_the_right_field_per_channel(self):
        with patch.object(pnl_worker, "decrypt_text", side_effect=lambda enc: f"dec({enc})") as dec, patch.object(
            pnl_worker, "fetch_meta_spend_from_api", return_value={}
        ) as meta, patch.object(pnl_worker, "fetch_google_spend_from_api", return_value={}) as google:
            pnl_worker.fetch_channel_spend("meta", {"ad_account_id": "act_1", "access_token_encrypted": "m"}, self.DATES[0], self.DATES[1])
            pnl_worker.fetch_channel_spend(
                "google", {"customer_id": "1234567890", "login_customer_id": "9", "token_encrypted": "g"}, self.DATES[0], self.DATES[1]
            )
        self.assertEqual(meta.call_args[0][:2], ("dec(m)", "act_1"))
        self.assertEqual(google.call_args[0][:3], ("dec(g)", "1234567890", "9"))
        self.assertEqual(dec.call_count, 2)


if __name__ == "__main__":
    unittest.main()
