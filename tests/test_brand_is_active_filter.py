import importlib.util
import os
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

try:
    import pymongo  # noqa: F401
except ModuleNotFoundError:
    fake_pymongo = types.ModuleType("pymongo")
    fake_pymongo.MongoClient = object
    sys.modules["pymongo"] = fake_pymongo

import aws_background
import sqlalchemy


REPO_ROOT = Path(__file__).resolve().parent.parent
AWS_DATA_UPDATE_PATH = REPO_ROOT / "backfill scripts" / "aws_data_update.py"


def load_aws_data_update_module():
    module_name = "aws_data_update_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, AWS_DATA_UPDATE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


aws_data_update = load_aws_data_update_module()


BASE_CONFIG = {
    "brand_name": "Active Brand",
    "brand_tag": "active-brand",
    "db_host": "db.example.com",
    "db_user": "admin",
    "db_password": "encrypted-password",
    "port": 3306,
    "db_database": "ACTIVE_DB",
    "shop_name": "active-shop",
    "access_token": "encrypted-token",
    "speed_key": "encrypted-speed-key",
    "app_id_mapping": "{}",
    "store_timezone": "(GMT+05:30) Asia/Kolkata",
}


class DummyEngine:
    def connect(self):
        class _Connection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, *_args, **_kwargs):
                return None

        return _Connection()


class BrandIsActiveFilterTests(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self._tracked_env_keys = {
            "DB_TLS_CA_MODE",
            "DB_SSL_VERIFY_CERT",
            "DB_SSL_VERIFY_IDENTITY",
        }
        self._tracked_env_keys.update(
            {
                f"{prefix}_{idx}"
                for idx in (1, 2, 3)
                for prefix in (
                    "BRAND_NAME",
                    "SHOP_NAME",
                    "ACCESS_TOKEN",
                    "API_VERSION",
                    "BRAND_TAG",
                    "DB_DATABASE",
                    "SPEED_KEY",
                    "APP_ID_MAPPING",
                )
            }
        )
        self._original_env = {key: os.environ.get(key) for key in self._tracked_env_keys}
        self._reset_module_state(aws_background)
        self._reset_module_state(aws_data_update)

    def tearDown(self):
        for key, value in self._original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self._reset_module_state(aws_background)
        self._reset_module_state(aws_data_update)

    def _reset_module_state(self, module):
        module.active_brand_indices.clear()
        module.brand_tag_to_index_map.clear()
        if hasattr(module, "brand_db_configs"):
            module.brand_db_configs.clear()
        if hasattr(module, "brand_timezones"):
            module.brand_timezones.clear()
        if hasattr(module, "brand_id_from_config"):
            module.brand_id_from_config.clear()
        if hasattr(module, "brand_rto_tags_by_index"):
            module.brand_rto_tags_by_index.clear()
        if hasattr(module, "sqlalchemy_engines"):
            module.sqlalchemy_engines.clear()
        if hasattr(module, "db_connection_pools"):
            module.db_connection_pools.clear()

    @staticmethod
    def _fake_fetch_brand_config(brand_id):
        if str(brand_id) == "1":
            return dict(BASE_CONFIG, is_active=True)
        if str(brand_id) == "2":
            return dict(BASE_CONFIG, brand_name="Inactive Brand", brand_tag="inactive-brand", is_active=False)
        if str(brand_id) == "3":
            return dict(BASE_CONFIG, brand_name="Missing Flag Brand", brand_tag="missing-flag")
        raise AssertionError(f"Unexpected brand id {brand_id}")

    @staticmethod
    def _saw_skip_log(mock_info, brand_id):
        for call in mock_info.call_args_list:
            if not call.args:
                continue
            if "tenant config is not active" not in str(call.args[0]):
                continue
            if len(call.args) >= 3 and str(call.args[2]) == str(brand_id):
                return True
        return False

    def test_aws_background_skips_inactive_brands_before_infra_setup(self):
        decrypt_calls = []

        def fake_decrypt(value):
            decrypt_calls.append(value)
            return f"decrypted:{value}"

        with mock.patch.object(aws_background, "TEST_MODE", False), \
            mock.patch.object(aws_background, "DB_TEST_ON_STARTUP", False), \
            mock.patch.object(aws_background, "fetch_active_brands", return_value={"1": "ACTIVE", "2": "INACTIVE", "3": "MISSING"}), \
            mock.patch.object(aws_background, "fetch_brand_config", side_effect=self._fake_fetch_brand_config), \
            mock.patch.object(aws_background, "decrypt_value", side_effect=fake_decrypt), \
            mock.patch.object(aws_background, "_validate_rds_proxy_host"), \
            mock.patch.object(aws_background, "_create_sqlalchemy_engine_for_brand", return_value=DummyEngine()), \
            mock.patch.object(aws_background.logger, "info") as mock_info:
            aws_background.initialize_brand_configs()

        self.assertEqual(aws_background.active_brand_indices, [1])
        self.assertEqual(len(decrypt_calls), 3)
        self.assertIn(1, aws_background.brand_db_configs)
        self.assertIn(1, aws_background.sqlalchemy_engines)
        self.assertNotIn(2, aws_background.brand_db_configs)
        self.assertNotIn(2, aws_background.sqlalchemy_engines)
        self.assertNotIn(3, aws_background.brand_db_configs)
        self.assertNotIn(3, aws_background.sqlalchemy_engines)
        self.assertNotIn("inactive-brand", aws_background.brand_tag_to_index_map)
        self.assertNotIn("missing-flag", aws_background.brand_tag_to_index_map)
        self.assertEqual(os.environ.get("BRAND_NAME_1"), "Active Brand")
        self.assertIsNone(os.environ.get("BRAND_NAME_2"))
        self.assertIsNone(os.environ.get("BRAND_NAME_3"))

        self.assertTrue(self._saw_skip_log(mock_info, 2))
        self.assertTrue(self._saw_skip_log(mock_info, 3))

    def test_aws_data_update_skips_inactive_brands_before_pool_creation(self):
        decrypt_calls = []

        def fake_decrypt(value):
            decrypt_calls.append(value)
            return f"decrypted:{value}"

        class DummyPool:
            pass

        with mock.patch.object(aws_data_update, "TEST_MODE", False), \
            mock.patch.object(aws_data_update, "fetch_active_brands", return_value={"1": "ACTIVE", "2": "INACTIVE", "3": "MISSING"}), \
            mock.patch.object(aws_data_update, "fetch_brand_config", side_effect=self._fake_fetch_brand_config), \
            mock.patch.object(aws_data_update, "decrypt_value", side_effect=fake_decrypt), \
            mock.patch.object(aws_data_update.pooling, "MySQLConnectionPool", return_value=DummyPool()), \
            mock.patch.object(sqlalchemy, "create_engine", return_value=DummyEngine()), \
            mock.patch.object(aws_data_update.logger, "info") as mock_info:
            aws_data_update.initialize_brand_configs()

        self.assertEqual(aws_data_update.active_brand_indices, [1])
        self.assertEqual(len(decrypt_calls), 3)
        self.assertIn(1, aws_data_update.db_connection_pools)
        self.assertIn(1, aws_data_update.sqlalchemy_engines)
        self.assertNotIn(2, aws_data_update.db_connection_pools)
        self.assertNotIn(2, aws_data_update.sqlalchemy_engines)
        self.assertNotIn(3, aws_data_update.db_connection_pools)
        self.assertNotIn(3, aws_data_update.sqlalchemy_engines)
        self.assertNotIn("inactive-brand", aws_data_update.brand_tag_to_index_map)
        self.assertNotIn("missing-flag", aws_data_update.brand_tag_to_index_map)
        self.assertEqual(os.environ.get("BRAND_NAME_1"), "Active Brand")
        self.assertIsNone(os.environ.get("BRAND_NAME_2"))
        self.assertIsNone(os.environ.get("BRAND_NAME_3"))

        self.assertTrue(self._saw_skip_log(mock_info, 2))
        self.assertTrue(self._saw_skip_log(mock_info, 3))


if __name__ == "__main__":
    unittest.main()
