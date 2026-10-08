from __future__ import annotations

import unittest
from contextlib import contextmanager
from unittest.mock import Mock, patch

from geodata_import_worker import (
    _pending_entity_deletion_ids,
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


class PendingDeletionRecoveryTests(unittest.TestCase):
    def test_selects_queued_and_expired_processing_jobs_in_a_bounded_batch(
        self,
    ) -> None:
        cursor = Mock()
        cursor.execute.return_value.fetchall.return_value = [
            ("job-1",),
            ("job-2",),
        ]

        @contextmanager
        def transaction():
            yield cursor

        store = Mock()
        store.transaction = transaction

        with patch.object(GeoHandler, "store", store):
            self.assertEqual(
                _pending_entity_deletion_ids(), ["job-1", "job-2"]
            )

        query, parameters = cursor.execute.call_args.args
        self.assertIn("kind=%s", query)
        self.assertIn("payload->>'status'='QUEUED'", query)
        self.assertIn("payload->>'status'='PROCESSING'", query)
        self.assertIn("leaseUntil", query)
        self.assertIn("LIMIT %s", query)
        self.assertEqual(parameters[0], "entityDeletionJobs")
        self.assertEqual(parameters[1], 50)


if __name__ == "__main__":
    unittest.main()
