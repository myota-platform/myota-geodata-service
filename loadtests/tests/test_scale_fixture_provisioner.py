import os
import unittest
from unittest.mock import patch

from provision_scale_fixtures import (
    BATCH_SIZE,
    CATEGORY_CODE,
    FIXTURE_COUNT,
    expected_prefix_counts,
    fixture_feature,
    legacy_complete_count,
    promotion_counts,
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
        self.assertEqual(BATCH_SIZE, 2_500)
        self.assertEqual(FIXTURE_COUNT // BATCH_SIZE, 4)

    def test_each_batch_promotes_exact_five_percent_with_balanced_statuses(
        self,
    ):
        batches = [promotion_counts(index) for index in range(4)]

        self.assertEqual([125] * 4, [sum(batch) for batch in batches])
        self.assertEqual([63, 62, 63, 62], [batch[0] for batch in batches])
        self.assertEqual([62, 63, 62, 63], [batch[1] for batch in batches])
        self.assertEqual(sum(batch[0] for batch in batches), 250)
        self.assertEqual(sum(batch[1] for batch in batches), 250)
        self.assertEqual(expected_prefix_counts(), [0, 125, 250, 375, 500])
        self.assertEqual(legacy_complete_count(), 2_875)

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
