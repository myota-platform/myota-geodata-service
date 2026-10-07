"""Run with GEO_TEST_DATABASE_URL pointing at an isolated *_tests database."""

import os
import threading
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from geodata_store import GeodataStore
from relational_queries import catalogue_page
from relational_state import StateConflict
from common import sign_token, now


@unittest.skipUnless(
    os.environ.get("GEO_TEST_DATABASE_URL"),
    "isolated PostGIS test database is not configured",
)
class RelationalConcurrencyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg.conninfo import conninfo_to_dict

        cls.dsn = os.environ["GEO_TEST_DATABASE_URL"]
        if not conninfo_to_dict(cls.dsn).get("dbname", "").endswith("_tests"):
            raise RuntimeError(
                "integration tests require an isolated *_tests DB"
            )
        cls.psycopg = psycopg

    def setUp(self):
        self.stores = [GeodataStore(), GeodataStore()]
        for store in self.stores:
            store.dsn = self.dsn
            store.hydrate()
        self.entity_id = str(uuid.uuid4())
        self.run_id = str(uuid.uuid4())
        self.extra_entity_ids = []
        with self.stores[0].operation(write=True):
            self.stores[0].items[self.entity_id] = {
                "id": self.entity_id,
                "name": "Concurrency park",
                "status": "CANDIDATE",
                "programmeSlug": None,
                "entityType": "TEST_PARK",
                "entityTypes": ["TEST_PARK"],
                "geometry": {"type": "Point", "coordinates": [-5.99, 37.38]},
                "country": "Spain",
                "continent": "Europe",
                "provenance": {"adapter": "MANUAL"},
            }

    def tearDown(self):
        with self.psycopg.connect(self.dsn) as connection:
            connection.execute("SET LOCAL myota.geodata_writer = 'row-v1'")
            connection.execute(
                "DELETE FROM geodata_audit_event WHERE aggregate_id=%s",
                (self.entity_id,),
            )
            connection.execute(
                "DELETE FROM outbox_event WHERE aggregate_id=%s",
                (self.entity_id,),
            )
            connection.execute(
                "DELETE FROM source_reference WHERE entity_id=%s",
                (self.entity_id,),
            )
            connection.execute(
                "DELETE FROM geodata_entity WHERE id=%s", (self.entity_id,)
            )
            connection.execute(
                "DELETE FROM import_run WHERE id=%s", (self.run_id,)
            )
            for entity_id in self.extra_entity_ids:
                connection.execute(
                    "DELETE FROM source_reference WHERE entity_id=%s",
                    (entity_id,),
                )
                connection.execute(
                    "DELETE FROM geodata_entity WHERE id=%s", (entity_id,)
                )
                connection.execute(
                    "DELETE FROM geodata_audit_event WHERE aggregate_id=%s",
                    (entity_id,),
                )
                connection.execute(
                    "DELETE FROM outbox_event WHERE aggregate_id=%s",
                    (entity_id,),
                )
        for store in self.stores:
            store.close()

    def test_two_instances_serialize_edits_without_losing_fields(self):
        start = threading.Barrier(2)

        def edit(store, field_name, value):
            start.wait(timeout=5)
            with store.operation(write=True):
                entity = store.items[self.entity_id]
                entity[field_name] = value
                time.sleep(0.05)

        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(edit, self.stores[0], "name", "New name")
            second = pool.submit(edit, self.stores[1], "country", "España")
            first.result(timeout=15)
            second.result(timeout=15)
        for store in self.stores:
            with store.operation():
                entity = store.items[self.entity_id]
                self.assertEqual(entity["name"], "New name")
                self.assertEqual(entity["country"], "España")
                self.assertEqual(entity["version"], 3)

    def test_stale_worker_write_returns_conflict_instead_of_overwriting(self):
        stale, fresh = self.stores
        with stale.operation(write=True, atomic=False):
            old = stale.items[self.entity_id]
            with fresh.operation(write=True):
                fresh.items[self.entity_id]["status"] = "APPROVED"
            old["name"] = "Stale edit"
            with self.assertRaises(StateConflict):
                stale.persist()
            # Discard the rejected delta, as a job would on rollback/retry.
            old["name"] = "Concurrency park"
        with fresh.operation():
            self.assertEqual(fresh.items[self.entity_id]["status"], "APPROVED")
            self.assertEqual(
                fresh.items[self.entity_id]["name"], "Concurrency park"
            )

    def test_worker_cannot_resurrect_an_entity_deleted_by_another_instance(
        self,
    ):
        stale, fresh = self.stores
        with stale.operation(write=True, atomic=False):
            old = stale.items[self.entity_id]
            with fresh.operation(write=True):
                del fresh.items[self.entity_id]
            old["name"] = "Resurrected"
            with self.assertRaises(StateConflict):
                stale.persist()
            old["name"] = "Concurrency park"
        with fresh.operation():
            self.assertNotIn(self.entity_id, fresh.items)

    def test_entity_audit_and_outbox_rollback_with_entity_write(self):
        store = self.stores[0]
        with self.assertRaisesRegex(RuntimeError, "rollback"):
            with store.operation(write=True):
                store.items[self.entity_id]["name"] = "Not committed"
                store.event(
                    "geodata.entity.changed.v1", "entity", self.entity_id, {}
                )
                store.persist()
                raise RuntimeError("rollback")
        with self.stores[1].operation():
            self.assertEqual(
                self.stores[1].items[self.entity_id]["name"],
                "Concurrency park",
            )
        with self.psycopg.connect(self.dsn) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT count(*) FROM geodata_audit_event WHERE aggregate_id=%s",
                    (self.entity_id,),
                ).fetchone()[0],
                0,
            )

    def test_concurrent_idempotency_key_executes_one_mutation(self):
        key = f"phase1-{self.entity_id}"
        start = threading.Barrier(2)
        calls = []

        def execute(store):
            start.wait(timeout=5)
            with store.operation(write=True):

                def callback():
                    calls.append(1)
                    store.items[self.entity_id]["name"] = "Exactly once"
                    store.event(
                        "geodata.entity.changed.v1",
                        "entity",
                        self.entity_id,
                        {},
                    )
                    time.sleep(0.05)
                    return {"id": self.entity_id}

                return store._repository.request_once(
                    key, "same-request", callback
                )

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(execute, self.stores))
        self.assertEqual(results[0], results[1])
        self.assertEqual(len(calls), 1)
        with self.stores[0].operation(write=True):
            with self.assertRaises(StateConflict):
                self.stores[0]._repository.request_once(
                    key, "changed-request", lambda: {}
                )
        with self.psycopg.connect(self.dsn) as connection:
            connection.execute(
                "DELETE FROM idempotency_record WHERE key=%s", (key,)
            )

    def test_import_delta_does_not_overwrite_independent_worker_lease(self):
        stale, fresh = self.stores
        with stale.operation(write=True):
            stale.data["importRuns"][self.run_id] = {
                "id": self.run_id,
                "adapter": "MANUAL",
                "status": "QUEUED",
                "source": {},
                "stats": {},
                "attemptCount": 0,
            }
        with stale.operation(write=True, atomic=False):
            run = stale.data["importRuns"][self.run_id]
            with fresh.transaction() as connection:
                connection.execute(
                    "UPDATE import_run SET attempt_count=1,heartbeat_at=now() WHERE id=%s",
                    (self.run_id,),
                )
            run["featureCount"] = 25
            stale.persist()
        with fresh.operation():
            run = fresh.data["importRuns"][self.run_id]
            self.assertEqual(run["attemptCount"], 1)
            self.assertIsNotNone(run["heartbeatAt"])
            self.assertEqual(run["featureCount"], 25)

    def test_pending_import_cancellation_has_one_timestamp_writer(self):
        from geodata import GeoHandler

        store = self.stores[0]
        with (
            patch.object(GeoHandler, "store", store),
            patch.object(GeoHandler, "_authorize_import"),
            patch.object(GeoHandler, "_import_owner", return_value="admin"),
            patch.object(GeoHandler, "_delete_import_source"),
        ):
            for status in ("UPLOAD_PENDING", "QUEUED"):
                with self.subTest(status=status):
                    with store.operation(write=True):
                        store.data["importRuns"][self.run_id] = {
                            "id": self.run_id,
                            "adapter": "MANUAL",
                            "status": status,
                            "source": {},
                        }
                    with store.operation(write=True):
                        result = GeoHandler.cancel_import(
                            None, {"runId": self.run_id}
                        )
                    self.assertEqual(result["status"], "CANCELLED")
                    self.assertEqual(result["_status"], 200)
                    self.assertIsNotNone(result["completedAt"])
                    with self.stores[1].operation():
                        persisted = self.stores[1].data["importRuns"][
                            self.run_id
                        ]
                        timestamp = persisted["cancellationRequestedAt"]
                        self.assertEqual(persisted["status"], "CANCELLED")
                        self.assertEqual(
                            persisted["cancellationRequestedBy"], "admin"
                        )
                    with store.operation(write=True):
                        retry = GeoHandler.cancel_import(
                            None, {"runId": self.run_id}
                        )
                    self.assertEqual(
                        retry["cancellationRequestedAt"], timestamp
                    )
                    with store.operation(write=True):
                        del store.data["importRuns"][self.run_id]

    def test_active_cancellation_finalizes_over_stale_worker_projection(self):
        from geodata import GeoHandler

        stale, fresh = self.stores
        with fresh.operation(write=True):
            fresh.data["importRuns"][self.run_id] = {
                "id": self.run_id,
                "adapter": "MANUAL",
                "status": "PROCESSING",
                "source": {},
            }
        with stale.operation(write=True, atomic=False):
            pending = stale.data["importRuns"][self.run_id]
            pending["featureCount"] = 999
            with (
                patch.object(GeoHandler, "store", fresh),
                patch.object(GeoHandler, "_authorize_import"),
                patch.object(
                    GeoHandler, "_import_owner", return_value="admin"
                ),
                fresh.operation(write=True),
            ):
                result = GeoHandler.cancel_import(None, {"runId": self.run_id})
            self.assertEqual(result["status"], "CANCELLING")
            self.assertEqual(result["_status"], 202)
            with patch.object(GeoHandler, "store", stale):
                GeoHandler._finish_import_cancellation(self.run_id)
                stale.persist()
        with fresh.operation():
            persisted = fresh.data["importRuns"][self.run_id]
            self.assertEqual(persisted["status"], "CANCELLED")
            self.assertEqual(persisted["cancellationRequestedBy"], "admin")
            self.assertIsNotNone(persisted["completedAt"])
            self.assertEqual(
                persisted["cancellationRequestedAt"],
                result["cancellationRequestedAt"],
            )

    def test_paged_catalogue_filters_use_relational_rows(self):
        with self.stores[1].operation():
            result = catalogue_page(
                self.stores[1],
                {
                    "entityType": ["TEST_PARK"],
                    "country": ["Spain"],
                    "status": ["CANDIDATE"],
                    "pageSize": ["1"],
                },
                (-6.1, 37.2, -5.8, 37.5),
            )
            self.assertEqual(result["total"], 1)
            self.assertEqual(result["items"][0]["id"], self.entity_id)
            empty = catalogue_page(
                self.stores[1], {"country": ["France"]}, None
            )
            self.assertEqual(empty["total"], 0)

    def test_hydration_does_not_read_legacy_snapshot_or_restore_it(self):
        with self.psycopg.connect(self.dsn) as connection:
            connection.execute(
                'INSERT INTO service_state(service,state) VALUES (\'geodata\',\'{"items":{"fake":{"name":"legacy"}}}\') '
                "ON CONFLICT (service) DO UPDATE SET state=EXCLUDED.state"
            )
        store = GeodataStore()
        store.dsn = self.dsn
        store.hydrate()
        with store.operation(write=True):
            self.assertNotIn("fake", list(store.items))
            store.items[self.entity_id]["name"] = "Actual row"
        with self.psycopg.connect(self.dsn) as connection:
            snapshot = connection.execute(
                "SELECT state FROM service_state WHERE service='geodata'"
            ).fetchone()[0]
            self.assertEqual(snapshot["items"]["fake"]["name"], "legacy")
            connection.execute(
                "DELETE FROM service_state WHERE service='geodata'"
            )
        store.close()

    def test_old_snapshot_writer_is_fenced_by_the_database(self):
        with self.psycopg.connect(self.dsn) as connection:
            with self.assertRaises(self.psycopg.Error) as error:
                connection.execute(
                    "UPDATE geodata_entity SET name='Old pod' WHERE id=%s",
                    (self.entity_id,),
                )
            self.assertEqual(error.exception.sqlstate, "55000")

    def test_migration_replay_does_not_restore_deleted_archived_resources(
        self,
    ):
        resource_id = str(uuid.uuid4())
        with self.psycopg.connect(self.dsn) as connection:
            connection.execute(
                "INSERT INTO service_state(service,state) VALUES ('geodata',%s::jsonb) "
                "ON CONFLICT (service) DO UPDATE SET state=EXCLUDED.state",
                (
                    '{"data":{"schedules":{"'
                    + resource_id
                    + '":{"id":"'
                    + resource_id
                    + '"}}}}',
                ),
            )
            connection.execute(
                (
                    Path(__file__).parent.parent
                    / "migrations/016_relational_authority.sql"
                ).read_text()
            )
            self.assertEqual(
                connection.execute(
                    "SELECT count(*) FROM geodata_control_record WHERE id=%s",
                    (resource_id,),
                ).fetchone()[0],
                0,
            )
            connection.execute(
                "DELETE FROM service_state WHERE service='geodata'"
            )

    def test_promotion_confirms_atomically_and_worker_result_is_durable(self):
        from geodata import GeoHandler

        candidate_id = str(uuid.uuid4())
        planned_id = str(uuid.uuid4())
        self.extra_entity_ids.append(planned_id)
        store = self.stores[0]
        with store.operation(write=True):
            store.data["importRuns"][self.run_id] = {
                "id": self.run_id,
                "adapter": "MANUAL",
                "status": "PREPROCESSED",
                "queuedAt": now(),
                "source": {},
                "stats": {},
            }
            store.data["importCandidates"][candidate_id] = {
                "id": candidate_id,
                "importRunId": self.run_id,
                "ordinal": 0,
                "validationStatus": "PENDING",
                "entity": {
                    "id": planned_id,
                    "name": "Import park",
                    "programmeSlug": None,
                    "entityType": "TEST_PARK",
                    "entityTypes": ["TEST_PARK"],
                    "geometry": {"type": "Point", "coordinates": [-5.7, 37.4]},
                    "provenance": {"adapter": "MANUAL"},
                },
            }
        token = sign_token(
            {
                "sub": "test-admin",
                "scp": ["*"],
                "roles": [{"role": "GLOBAL_ADMIN"}],
                "exp": int(time.time()) + 300,
            }
        )
        params = {
            "runId": self.run_id,
            "_http": "1",
            "Authorization": f"Bearer {token}",
            "Idempotency-Key": f"promotion-{candidate_id}",
            "_body": {
                "candidateIds": [candidate_id],
                "targetStatus": "CANDIDATE",
                "processorId": "test-admin",
            },
        }
        with patch.object(GeoHandler, "store", store):
            job = GeoHandler.process_import_candidates(None, params)
            repeated = GeoHandler.process_import_candidates(None, params)
            self.assertEqual(job["id"], repeated["id"])
            self.assertTrue(GeoHandler._process_import_queue(job["id"]))
            self.assertTrue(GeoHandler._process_import_queue(job["id"]))
        with self.stores[1].operation():
            self.assertEqual(
                self.stores[1].items[planned_id]["status"], "CANDIDATE"
            )
            self.assertEqual(
                self.stores[1].data["importCandidates"][candidate_id][
                    "validationStatus"
                ],
                "PROCESSED",
            )
            self.assertEqual(
                self.stores[1].data["importProcessingQueues"][job["id"]][
                    "status"
                ],
                "COMPLETED",
            )
            self.assertEqual(
                self.stores[1].data["importRuns"][self.run_id]["stats"][
                    "created"
                ],
                1,
            )
        with self.psycopg.connect(self.dsn) as connection:
            connection.execute(
                "DELETE FROM idempotency_record WHERE key LIKE %s",
                (f"%promotion-{candidate_id}",),
            )
