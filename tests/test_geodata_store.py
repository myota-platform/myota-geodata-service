import unittest
from contextlib import contextmanager
from unittest.mock import patch

from geodata_store import GeodataStore
from relational_state import RowMap, RowRepository


class GeodataStoreHydrationTests(unittest.TestCase):
    def test_cancellation_discards_only_loaded_rows_from_its_import(self):
        class Connection:
            def execute(self, sql, params):
                self.sql, self.params = sql, params

        repository = RowRepository(None, None)
        connection = Connection()
        repository.scope.connection = connection
        own = ("importCandidates", "own-record")
        other = ("importCandidates", "other-record")
        repository.scope.loaded = {
            own: {"importRunId": "cancelled-run"},
            other: {"importRunId": "unrelated-run"},
        }
        repository.scope.original = dict(repository.scope.loaded)
        repository.scope.deleted.add(own)
        repository.discard_import_candidates("cancelled-run")
        self.assertEqual(connection.params, ("cancelled-run",))
        self.assertIn("WHERE import_run_id=%s", connection.sql)
        self.assertNotIn(own, repository.scope.loaded)
        self.assertNotIn(own, repository.scope.original)
        self.assertNotIn(own, repository.scope.deleted)
        self.assertIn(other, repository.scope.loaded)
        self.assertIn(other, repository.scope.original)

    def test_hydrate_installs_repositories_without_loading_snapshot(self):
        store = GeodataStore()
        store.dsn = "unused-test-dsn"
        with patch.object(store, "base_transaction") as connection:
            store.hydrate()
        connection.assert_not_called()
        self.assertIsInstance(store.items, RowMap)
        self.assertIsInstance(store.data["importRuns"], RowMap)
        self.assertEqual(store.idempotency, {})

    def test_relational_entity_reads_ignore_legacy_snapshot(self):
        class Result:
            def fetchone(self):
                return (
                    {
                        "id": "entity-1",
                        "status": "APPROVED",
                        "name": "Actual park",
                        "version": 4,
                    },
                )

        class Connection:
            def execute(self, sql, params):
                self.sql = sql
                assert "service_state" not in sql
                return Result()

        connection = Connection()

        @contextmanager
        def transaction():
            yield connection

        store = GeodataStore()
        store.dsn = "unused-test-dsn"
        with patch.object(store, "base_transaction", transaction):
            store.hydrate()
            self.assertEqual(store.items["entity-1"]["status"], "APPROVED")
            self.assertEqual(store.items["entity-1"]["version"], 4)

    def test_snapshot_only_persistence_does_not_flush_or_clear_dirty_imports(
        self,
    ):
        store = GeodataStore()
        store._dirty_import_candidate_ids.add("active-candidate")
        store._dirty_import_queue_ids.add("active-queue")
        with patch.object(store, "_sync_relational") as relational_sync:
            store.persist_snapshot_only()
        relational_sync.assert_not_called()
        self.assertEqual(
            store._dirty_import_candidate_ids, {"active-candidate"}
        )
        self.assertEqual(store._dirty_import_queue_ids, {"active-queue"})


if __name__ == "__main__":
    unittest.main()
