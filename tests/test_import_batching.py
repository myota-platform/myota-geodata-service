import json
import unittest

from geodata import _bounded_import_batches


class ImportBatchingTests(unittest.TestCase):
    def test_batches_obey_count_and_serialized_byte_budgets(self):
        features = [
            {"type": "Feature", "properties": {"name": f"park-{index}"}}
            for index in range(7)
        ]
        feature_size = len(
            json.dumps(features[0], separators=(",", ":")).encode("utf-8")
        )

        batches = list(
            _bounded_import_batches(
                iter(features), max_features=3, max_bytes=feature_size * 2
            )
        )

        self.assertEqual([len(batch) for batch in batches], [2, 2, 2, 1])
        self.assertEqual(
            [feature for batch in batches for feature in batch], features
        )
        for batch in batches:
            batch_size = sum(
                len(json.dumps(feature, separators=(",", ":")).encode("utf-8"))
                for feature in batch
            )
            self.assertLessEqual(len(batch), 3)
            self.assertLessEqual(batch_size, feature_size * 2)

    def test_single_feature_larger_than_batch_budget_is_isolated(self):
        large = {"type": "Feature", "properties": {"name": "x" * 100}}
        small = {"type": "Feature", "properties": {"name": "a"}}

        batches = list(
            _bounded_import_batches(
                iter([large, small, small]), max_features=10, max_bytes=90
            )
        )

        self.assertEqual(batches, [[large], [small, small]])


if __name__ == "__main__":
    unittest.main()
