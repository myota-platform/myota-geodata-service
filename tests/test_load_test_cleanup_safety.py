from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from geodata import GeoHandler


class LoadTestCleanupSafetyTests(unittest.TestCase):
    def test_cleanup_requires_an_explicit_nonproduction_environment(self):
        with patch.dict(os.environ, {
            "MYOTA_ENV": "production",
            "MYOTA_LOAD_TEST_CLEANUP_ENABLED": "1",
        }, clear=False):
            with self.assertRaisesRegex(PermissionError, "disabled outside explicitly enabled non-production"):
                GeoHandler.cleanup_load_test_run(None, {"testRunId": "lt-test"})

    def test_cleanup_is_disabled_when_environment_is_not_declared(self):
        with patch.dict(os.environ, {"MYOTA_LOAD_TEST_CLEANUP_ENABLED": "1"}, clear=False):
            os.environ.pop("MYOTA_ENV", None)
            with self.assertRaisesRegex(PermissionError, "disabled outside explicitly enabled non-production"):
                GeoHandler.cleanup_load_test_run(None, {"testRunId": "lt-test"})


if __name__ == "__main__":
    unittest.main()
