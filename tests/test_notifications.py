import asyncio
import unittest

import aiohttp
import mysql.connector
import requests

import aws_background  # noqa: F401  (import order: see test_brand_cron_scheduling.py)
from pipeline.notifications import _derive_error_code


class DeriveErrorCodeTests(unittest.TestCase):
    def test_mapping(self):
        cases = [
            (None, "PIPELINE_FAILED"),
            (mysql.connector.Error("x"), "DB_ERROR"),
            (requests.exceptions.Timeout(), "TIMEOUT"),
            (asyncio.TimeoutError(), "TIMEOUT"),
            (requests.exceptions.ConnectionError(), "HTTP_ERROR"),
            (aiohttp.ClientError(), "HTTP_ERROR"),
            (KeyError("X"), "MISSING_ENV"),
            (ValueError(), "VALIDATION_ERROR"),
            (TypeError(), "VALIDATION_ERROR"),
            (FileNotFoundError(), "IO_ERROR"),
            (RuntimeError(), "RUNTIMEERROR"),
        ]
        for exc, code in cases:
            with self.subTest(exc=repr(exc)):
                self.assertEqual(_derive_error_code(exc), code)


if __name__ == "__main__":
    unittest.main()
