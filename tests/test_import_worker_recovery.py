from __future__ import annotations

import unittest
from contextlib import contextmanager
from unittest.mock import Mock, patch

from geodata_import_worker import (
    _stale_cancellation_ids,
)
from geodata import GeoHandler


class StaleCancellationRecoveryTests(unittest.TestCase):
    def test_only_expired_cancellation_leases_are_selected(self) -> None:
        cursor = Mock()
        cursor.execute.return_value.fetchall.return_value = [
            ("run-1",),
            ("run-2",),
        ]

        @contextmanager
        def transaction():
            yield cursor

        store = Mock()
        store.transaction = transaction
        store.refresh_import_runs = Mock()

        with patch.object(GeoHandler, "store", store):
            self.assertEqual(_stale_cancellation_ids(), ["run-1", "run-2"])

        store.refresh_import_runs.assert_called_once_with()
        query = cursor.execute.call_args.args[0]
        self.assertIn("status='CANCELLING'", query)
        self.assertIn("lease_until <= now()", query)
        self.assertIn("lease_until IS NULL", query)


class BoundedPreprocessingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.previous_items = GeoHandler.store.items
        self.previous_data = GeoHandler.store.data
        self.previous_events = GeoHandler.store.events
        GeoHandler.store.items = {}
        GeoHandler.store.data = {}
        GeoHandler.store.events = []

    def tearDown(self) -> None:
        GeoHandler.store.items = self.previous_items
        GeoHandler.store.data = self.previous_data
        GeoHandler.store.events = self.previous_events

    def test_preprocessing_commits_candidates_in_bounded_windows(self) -> None:
        run_id = "bounded-run"
        GeoHandler.store.data["importRuns"] = {
            run_id: {
                "id": run_id,
                "status": "PROCESSING",
                "adapter": "MANUAL",
                "source": {"name": "bounded test"},
                "entityType": "MUNICIPAL_PARK",
                "entityTypes": ["MUNICIPAL_PARK"],
            }
        }
        features = [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [-5.99 + index / 100000, 37.39],
                },
                "properties": {"name": f"Feature {index}"},
            }
            for index in range(205)
        ]
        persist_calls = []
        with (
            patch.object(GeoHandler, "_claim_import_run", return_value=True),
            patch.object(
                GeoHandler.store,
                "persist",
                side_effect=lambda **_: persist_calls.append(1),
            ),
            patch.object(
                GeoHandler.store,
                "evict_import_candidates",
                wraps=GeoHandler.store.evict_import_candidates,
            ) as evict,
            patch.dict("os.environ", {"MYOTA_IMPORT_BATCH_SIZE": "40"}),
        ):
            self.assertTrue(
                GeoHandler._process_import_run(
                    run_id,
                    {
                        "adapter": "MANUAL",
                        "source": {"name": "bounded test"},
                        "entityType": "MUNICIPAL_PARK",
                        "entityTypes": ["MUNICIPAL_PARK"],
                    },
                    lambda: iter(features),
                    already_claimed=True,
                )
            )

        self.assertEqual(len(GeoHandler.store.data["importCandidates"]), 205)
        self.assertEqual(
            GeoHandler.store.data["importRuns"][run_id]["featureCount"], 205
        )
        self.assertGreaterEqual(len(persist_calls), 6)
        self.assertEqual(evict.call_count, 6)


if __name__ == "__main__":
    unittest.main()
