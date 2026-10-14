import unittest
from pathlib import Path
from unittest.mock import Mock
from contextlib import contextmanager
from unittest.mock import patch

from geodata_store import GeodataStore
from relational_queries import page_query
from relational_state import RowMap, RowRepository


class GeodataStoreHydrationTests(unittest.TestCase):
    def test_candidate_page_restores_durable_duplicate_review_metadata(self):
        class Result:
            def __init__(self, one=None, many=None):
                self.one = one
                self.many = many or []

            def fetchone(self):
                return self.one

            def fetchall(self):
                return self.many

        class Connection:
            def execute(self, sql, params):
                if sql.startswith("SELECT count(*)"):
                    return Result(one=(1,))
                return Result(
                    many=[
                        (
                            {
                                "id": "candidate-1",
                                "entity": {
                                    "name": "Staged park",
                                    "_importReview": {
                                        "dedupeWarning": "POSSIBLE_DUPLICATE",
                                        "possibleDuplicates": [
                                            {
                                                "name": "Nearby park",
                                                "distanceMeters": 12.5,
                                            }
                                        ],
                                    },
                                },
                            },
                        )
                    ]
                )

        store = GeodataStore()
        connection = Connection()

        @contextmanager
        def transaction():
            yield connection

        with patch.object(store, "transaction", transaction):
            result = page_query(
                store,
                "importCandidates",
                {"page": ["1"], "pageSize": ["10"]},
            )

        candidate = result["items"][0]
        self.assertEqual(candidate["dedupeWarning"], "POSSIBLE_DUPLICATE")
        self.assertEqual(
            candidate["possibleDuplicates"][0]["name"], "Nearby park"
        )
        self.assertNotIn("_importReview", candidate["entity"])

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
        result = repository.entity_by_source_ref("osm:1", "mpota", "OSM")

        self.assertIn("s.source_record_id=%s", connection.sql)
        self.assertIn("s.adapter_code=%s", connection.sql)
        self.assertIn("%s::text IS NOT NULL", connection.sql)
        self.assertIn("%s::text IS NULL", connection.sql)
        self.assertIn("e.programme_slug=%s", connection.sql)
        self.assertEqual(
            connection.params,
            ("OSM", "osm:1", "OSM", "osm:1", "OSM", "OSM", "mpota"),
        )
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
                        "website": "https://park.example",
                        "description": "Curated park description",
                        "version": 4,
                    },
                )

        class Connection:
            def execute(self, sql, params):
                self.sql = sql
                assert "service_state" not in sql
                assert "'website', website" in sql
                assert "'description', description" in sql
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
            self.assertEqual(
                store.items["entity-1"]["website"], "https://park.example"
            )
            self.assertEqual(
                store.items["entity-1"]["description"],
                "Curated park description",
            )

    def test_entity_upsert_persists_dedicated_metadata_and_migration_is_additive(
        self,
    ):
        store = GeodataStore()
        connection = Mock()
        entity = {
            "id": "11111111-1111-4111-8111-111111111111",
            "name": "Test park",
            "website": "https://park.example",
            "description": "Curated park description",
            "geometry": {"type": "Point", "coordinates": [-5.99, 37.39]},
            "status": "CANDIDATE",
        }
        with patch.object(
            store,
            "_category_id",
            return_value="22222222-2222-4222-8222-222222222222",
        ):
            store._upsert_entity(connection, entity)
        insert_sql, params = connection.execute.call_args_list[0].args
        self.assertIn("website, description", insert_sql)
        self.assertIn("website=EXCLUDED.website", insert_sql)
        self.assertIn("description=EXCLUDED.description", insert_sql)
        self.assertEqual(
            params[5:7], ("https://park.example", "Curated park description")
        )
        self.assertNotIn("website", store._public_properties(entity))
        self.assertNotIn("description", store._public_properties(entity))
        baseline = (
            Path(__file__).parents[1]
            / "migrations"
            / "001_geodata_baseline.sql"
        ).read_text()
        self.assertRegex(baseline, r"website text[\s,]")
        self.assertRegex(baseline, r"description text[\s,]")
        self.assertNotIn("INSERT INTO public.geodata_entity", baseline)

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

    def test_candidate_flush_persists_duplicate_review_metadata(self):
        import json

        store = GeodataStore()
        store.dsn = "test-dsn"
        store.data = {
            "importRuns": {},
            "importCandidates": {
                "33333333-3333-4333-8333-333333333333": {
                    "id": "33333333-3333-4333-8333-333333333333",
                    "importRunId": "44444444-4444-4444-8444-444444444444",
                    "ordinal": 0,
                    "validationStatus": "PENDING",
                    "dedupeWarning": "POSSIBLE_DUPLICATE",
                    "possibleDuplicates": [
                        {
                            "entityId": "55555555-5555-4555-8555-555555555555",
                            "name": "Nearby park",
                            "distanceMeters": 12.5,
                        }
                    ],
                    "entity": {
                        "id": "66666666-6666-4666-8666-666666666666",
                        "name": "Staged park",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [-5.99, 37.39],
                        },
                    },
                }
            },
        }
        store._dirty_import_candidate_ids.add(
            "33333333-3333-4333-8333-333333333333"
        )
        connection = Mock()

        @contextmanager
        def transaction():
            yield connection

        with patch.object(store, "transaction", transaction):
            store._sync_relational(include_import_state=True)

        sql, params = connection.execute.call_args.args
        self.assertIn("INSERT INTO geodata_import_candidate", sql)
        persisted = json.loads(params[12])
        self.assertEqual(
            persisted["_importReview"]["dedupeWarning"], "POSSIBLE_DUPLICATE"
        )
        self.assertEqual(
            persisted["_importReview"]["possibleDuplicates"][0][
                "distanceMeters"
            ],
            12.5,
        )


if __name__ == "__main__":
    unittest.main()
