import unittest
from contextlib import contextmanager
from unittest.mock import patch

from geodata_store import GeodataStore
from relational_state import RowMap, RowRepository


class GeodataStoreHydrationTests(unittest.TestCase):
    def test_run_candidate_lookup_is_scoped_and_primes_repository_baseline(
        self,
    ):
        class Result:
            @staticmethod
            def fetchall():
                return [
                    (
                        {
                            "id": "candidate-1",
                            "ordinal": 12,
                            "importRunId": "run-1",
                            "entity": {"name": "Park"},
                        },
                    )
                ]

        class Connection:
            def execute(self, sql, params):
                self.sql, self.params = sql, params
                return Result()

        repository = RowRepository(None, None)
        connection = Connection()

        @contextmanager
        def transaction():
            yield connection

        repository.connection = transaction
        candidates = repository.import_candidates_for_run("run-1")

        self.assertIn("WHERE import_run_id=%s", connection.sql)
        self.assertEqual(connection.params, ("run-1",))
        self.assertEqual(candidates[12]["id"], "candidate-1")
        identity = ("importCandidates", "candidate-1")
        self.assertIn(identity, repository.scope.original)
        self.assertEqual(repository.scope.original[identity]["ordinal"], 12)

    def test_candidate_staging_does_not_query_for_new_row_existence(self):
        repository = RowRepository(None, None)
        candidate = {"id": "candidate-new", "ordinal": 2}
        repository.stage_import_candidate(candidate, is_new=True)

        identity = ("importCandidates", "candidate-new")
        self.assertIsNone(repository.scope.original[identity])
        self.assertIs(repository.scope.loaded[identity], candidate)

    def test_entity_source_lookup_uses_targeted_indexable_predicate(self):
        class Result:
            @staticmethod
            def fetchone():
                return ({"id": "entity-1", "sourceRef": "osm:1"},)

        class Connection:
            def execute(self, sql, params):
                self.sql, self.params = sql, params
                return Result()

        repository = RowRepository(None, None)
        connection = Connection()

        @contextmanager
        def transaction():
            yield connection

        repository.connection = transaction
        result = repository.entity_by_source_ref("osm:1", "mpota")

        self.assertIn("public_properties->>'sourceRef'=%s", connection.sql)
        self.assertIn("programme_slug=%s", connection.sql)
        self.assertEqual(connection.params, ("osm:1", "mpota"))
        self.assertEqual(result["id"], "entity-1")

    def test_nearby_lookup_uses_postgis_distance_and_radius_filter(self):
        class Result:
            @staticmethod
            def fetchall():
                return [("entity-2", 17.5)]

        class Connection:
            def execute(self, sql, params):
                self.sql, self.params = sql, params
                return Result()

        repository = RowRepository(
            type("Store", (), {"_observe_postgis_query": lambda *_: None})(),
            None,
        )
        connection = Connection()

        @contextmanager
        def transaction():
            yield connection

        repository.connection = transaction
        geometry = {"type": "Point", "coordinates": [-5.99, 37.39]}
        matches = repository.nearby_entity_ids(geometry, "entity-1", 50)

        self.assertIn("ST_DWithin(geom::geography", connection.sql)
        self.assertEqual(connection.params[1], "entity-1")
        self.assertEqual(connection.params[3], 50)
        self.assertEqual(matches, [("entity-2", 17.5)])

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

    def test_relational_entity_read_restores_centroid_from_postgis(self):
        class Result:
            def fetchone(self):
                return (
                    {
                        "id": "entity-1",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [-5.99, 37.39],
                        },
                        "centroid": {"lon": -5.99, "lat": 37.39},
                    },
                )

        class Connection:
            def execute(self, sql, params):
                self.sql = sql
                self.params = params
                return Result()

        repository = RowRepository(None, None)
        connection = Connection()

        @contextmanager
        def transaction():
            yield connection

        repository.connection = transaction
        entity = repository.read(connection, "entities", "entity-1")

        self.assertEqual(entity["centroid"], {"lon": -5.99, "lat": 37.39})
        self.assertIn("'centroid'", connection.sql)
        self.assertIn("ST_X(COALESCE(centroid", connection.sql)
        self.assertEqual(connection.params, ("entity-1",))

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
