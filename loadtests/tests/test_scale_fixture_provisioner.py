import os
import unittest
from unittest.mock import patch

from provision_scale_fixtures import (
    CATEGORY_CODE,
    FIXTURE_COUNT,
    fixture_feature,
    require_production_acknowledgement,
)


class PermanentScaleFixtureTests(unittest.TestCase):
    def test_generated_features_are_tagged_and_sevilla_points(self):
        first = fixture_feature(0)
        neighbor = fixture_feature(1)

        self.assertEqual(first["properties"]["entityTypes"], [CATEGORY_CODE])
        self.assertEqual(first["properties"]["countryCode"], "ES")
        self.assertEqual(first["properties"]["city"], "Sevilla")
        self.assertEqual(first["geometry"]["type"], "Point")
        self.assertAlmostEqual(
            neighbor["geometry"]["coordinates"][0]
            - first["geometry"]["coordinates"][0],
            0.0012,
        )
        self.assertEqual(FIXTURE_COUNT, 10_000)

    def test_production_writes_require_exact_host_and_permanent_ack(self):
        with patch.dict(
            os.environ,
            {
                "MYOTA_API_BASE_URL": "https://api.myota.top",
                "MYOTA_SCALE_FIXTURES_ALLOW_PRODUCTION": "YES",
                "MYOTA_SCALE_FIXTURES_PERMANENT": "YES",
            },
        ):
            require_production_acknowledgement()

        with patch.dict(
            os.environ,
            {
                "MYOTA_API_BASE_URL": "https://example.invalid",
                "MYOTA_SCALE_FIXTURES_ALLOW_PRODUCTION": "YES",
                "MYOTA_SCALE_FIXTURES_PERMANENT": "YES",
            },
        ):
            with self.assertRaisesRegex(RuntimeError, "restricted"):
                require_production_acknowledgement()

        with patch.dict(
            os.environ,
            {
                "MYOTA_API_BASE_URL": "https://api.myota.top",
                "MYOTA_SCALE_FIXTURES_ALLOW_PRODUCTION": "YES",
                "MYOTA_SCALE_FIXTURES_PERMANENT": "NO",
            },
        ):
            with self.assertRaisesRegex(
                RuntimeError, "must not be automatically deleted"
            ):
                require_production_acknowledgement()


if __name__ == "__main__":
    unittest.main()
