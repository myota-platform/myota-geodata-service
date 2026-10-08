from __future__ import annotations

import unittest
from contextlib import contextmanager
from unittest.mock import Mock, patch

from geodata_import_worker import _stale_cancellation_ids
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


if __name__ == "__main__":
    unittest.main()
