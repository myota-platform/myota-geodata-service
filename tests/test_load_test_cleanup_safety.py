from __future__ import annotations

import os
import unittest
from unittest.mock import patch

import geodata
from geodata import GeoHandler


class LoadTestCleanupSafetyTests(unittest.TestCase):
    def test_cleanup_requires_explicit_enablement(self):
        with patch.dict(os.environ, {
            "MYOTA_ENV": "development",
        }, clear=False):
            os.environ.pop("MYOTA_LOAD_TEST_CLEANUP_ENABLED", None)
            with self.assertRaisesRegex(PermissionError, "disabled unless explicitly enabled"):
                GeoHandler.cleanup_load_test_run(None, {"testRunId": "lt-test"})

    def test_cleanup_is_disabled_when_environment_is_not_declared(self):
        with patch.dict(os.environ, {"MYOTA_LOAD_TEST_CLEANUP_ENABLED": "1"}, clear=False):
            os.environ.pop("MYOTA_ENV", None)
            with self.assertRaisesRegex(PermissionError, "disabled unless explicitly enabled"):
                GeoHandler.cleanup_load_test_run(None, {"testRunId": "lt-test"})

    def test_production_cleanup_requires_its_separate_explicit_switch(self):
        with patch.dict(os.environ, {
            "MYOTA_ENV": "production",
            "MYOTA_LOAD_TEST_CLEANUP_ENABLED": "1",
        }, clear=False):
            os.environ.pop("MYOTA_LOAD_TEST_ALLOW_PRODUCTION_CLEANUP", None)
            with self.assertRaisesRegex(PermissionError, "production load-test cleanup requires explicit"):
                GeoHandler.cleanup_load_test_run(None, {"testRunId": "lt-production-test"})

    def test_enabled_production_cleanup_still_requires_bearer_authentication(self):
        with patch.dict(os.environ, {
            "MYOTA_ENV": "production",
            "MYOTA_LOAD_TEST_CLEANUP_ENABLED": "1",
            "MYOTA_LOAD_TEST_ALLOW_PRODUCTION_CLEANUP": "YES",
        }, clear=False):
            with self.assertRaisesRegex(PermissionError, "Bearer authentication is required"):
                GeoHandler.cleanup_load_test_run(None, {"testRunId": "lt-production-test"})

    def test_cleanup_requires_global_admin_even_with_wildcard_scope(self):
        with patch.dict(os.environ, {
            "MYOTA_ENV": "production",
            "MYOTA_LOAD_TEST_CLEANUP_ENABLED": "1",
            "MYOTA_LOAD_TEST_ALLOW_PRODUCTION_CLEANUP": "YES",
        }, clear=False), patch.object(geodata, "verify_token", return_value={
            "roles": ["GLOBAL_OPERATOR"], "scp": ["*"],
        }):
            params = {
                "Authorization": "Bearer valid-test-token",
                "testRunId": "lt-production-test",
                "_body": {"confirmation": "DELETE LOAD TEST DATA lt-production-test"},
            }
            with self.assertRaisesRegex(PermissionError, "global administrator access is required"):
                GeoHandler.cleanup_load_test_run(None, params)


if __name__ == "__main__":
    unittest.main()
