"""Process-death recovery against a disposable PostGIS test database."""

from __future__ import annotations

import os
import json
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch


DATABASE_URL = os.environ.get("GEO_TEST_DATABASE_URL")


@unittest.skipUnless(
    DATABASE_URL,
    "an isolated PostGIS *_tests database is required",
)
class ImportWorkerKillRecoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from geodata import GeoHandler

        import psycopg
        from psycopg.conninfo import conninfo_to_dict

        cls.previous_dsn = GeoHandler.store.dsn
        cls.psycopg = psycopg
        cls.dsn = str(DATABASE_URL)
        if not conninfo_to_dict(cls.dsn).get("dbname", "").endswith("_tests"):
            raise RuntimeError(
                "worker failure-injection requires an isolated *_tests DB"
            )

    @classmethod
    def tearDownClass(cls) -> None:
        from geodata import GeoHandler

        GeoHandler.store.close()
        GeoHandler.store.dsn = cls.previous_dsn

    def test_killed_worker_replays_committed_candidate_checkpoint(
        self,
    ) -> None:
        from geodata import GeoHandler

        GeoHandler.store.dsn = self.dsn
        run_id = str(uuid.uuid4())
        body = {
            "adapter": "MANUAL",
            "source": {"name": "isolated worker-kill fixture"},
            "format": "GEOJSON",
            "entityType": "TEST_PARK",
            "entityTypes": ["TEST_PARK"],
        }
        GeoHandler.store.hydrate()
        with self.psycopg.connect(self.dsn) as connection:
            connection.execute("SET LOCAL myota.geodata_writer = 'row-v1'")
            connection.execute(
                "INSERT INTO import_run(id, adapter_code, source_metadata, status) "
                "VALUES (%s, 'MANUAL', %s::jsonb, 'QUEUED')",
                (run_id, json.dumps({"source": body["source"]})),
            )

        with tempfile.TemporaryDirectory(prefix="myota-worker-kill-") as tmp:
            marker = Path(tmp) / "checkpoint-reached"
            child = r"""
import os, sys, time
from pathlib import Path
from geodata import GeoHandler
run_id, marker = sys.argv[1], Path(sys.argv[2])
GeoHandler.store.hydrate()
GeoHandler.store.refresh_import_runs()
def loader():
    for index in range(250):
        if index == 100 and os.environ.get("TEST_PAUSE_AFTER_CHECKPOINT") == "1":
            marker.touch()
            time.sleep(120)
        yield {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-5.99, 37.39]},
            "properties": {"name": f"worker fixture {index}", "sourceRef": f"worker-fixture-{index}"},
        }
body = {
    "adapter": "MANUAL",
    "source": {"name": "isolated worker-kill fixture"},
    "entityType": "TEST_PARK",
    "entityTypes": ["TEST_PARK"],
}
if not GeoHandler._process_import_run(run_id, body, loader):
    raise SystemExit("worker failed to claim fixture import")
"""
            environment = os.environ.copy()
            environment.update(
                {
                    "GEO_DATABASE_URL": self.dsn,
                    "MYOTA_REQUIRE_DURABILITY": "1",
                    "MYOTA_IMPORT_BATCH_SIZE": "100",
                    "MYOTA_IMPORT_HEARTBEAT_SECONDS": "60",
                    "MYOTA_IMPORT_LEASE_SECONDS": "60",
                    "TEST_PAUSE_AFTER_CHECKPOINT": "1",
                }
            )
            interrupted = subprocess.Popen(
                [sys.executable, "-c", child, run_id, str(marker)],
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            try:
                deadline = time.monotonic() + 45
                while not marker.exists() and time.monotonic() < deadline:
                    if interrupted.poll() is not None:
                        output = interrupted.communicate()[0].decode(
                            "utf-8", "replace"
                        )
                        self.fail(
                            f"worker exited before checkpoint:\n{output}"
                        )
                    time.sleep(0.1)
                self.assertTrue(
                    marker.exists(), "worker did not reach checkpoint"
                )
                with self.psycopg.connect(self.dsn) as connection:
                    persisted = connection.execute(
                        "SELECT count(*) FROM geodata_import_candidate "
                        "WHERE import_run_id=%s",
                        (run_id,),
                    ).fetchone()[0]
                self.assertEqual(persisted, 100)
                interrupted.kill()
                interrupted.wait(timeout=15)
                interrupted.communicate()

                # Simulate the lease-expiry condition the recovery scanner sees
                # after a forced process loss. This touches only the *_tests DB.
                with self.psycopg.connect(self.dsn) as connection:
                    connection.execute(
                        "SET LOCAL myota.geodata_writer = 'row-v1'"
                    )
                    connection.execute(
                        "UPDATE import_run SET status='PROCESSING', "
                        "lease_until=now() - interval '1 second' WHERE id=%s",
                        (run_id,),
                    )

                environment.pop("TEST_PAUSE_AFTER_CHECKPOINT", None)
                replay = subprocess.run(
                    [sys.executable, "-c", child, run_id, str(marker)],
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=120,
                    check=False,
                )
                self.assertEqual(
                    replay.returncode, 0, replay.stdout + replay.stderr
                )
                with self.psycopg.connect(self.dsn) as connection:
                    count, distinct_ordinals = connection.execute(
                        "SELECT count(*), count(DISTINCT ordinal) "
                        "FROM geodata_import_candidate WHERE import_run_id=%s",
                        (run_id,),
                    ).fetchone()
                    status = connection.execute(
                        "SELECT status FROM import_run WHERE id=%s", (run_id,)
                    ).fetchone()[0]
                self.assertEqual((count, distinct_ordinals), (250, 250))
                self.assertEqual(status, "PREPROCESSED")
            finally:
                if interrupted.poll() is None:
                    interrupted.kill()
                    interrupted.wait(timeout=15)
                if interrupted.stdout and not interrupted.stdout.closed:
                    interrupted.stdout.close()
                with self.psycopg.connect(self.dsn) as connection:
                    connection.execute(
                        "SET LOCAL myota.geodata_writer = 'row-v1'"
                    )
                    connection.execute(
                        "DELETE FROM geodata_control_record "
                        "WHERE kind='sourceManifests' AND id=%s",
                        (run_id,),
                    )
                    connection.execute(
                        "DELETE FROM outbox_event WHERE aggregate_id=%s",
                        (run_id,),
                    )
                    connection.execute(
                        "DELETE FROM import_run WHERE id=%s", (run_id,)
                    )
                GeoHandler.store.close()

    def test_killed_worker_restarts_import_interrupted_before_first_checkpoint(
        self,
    ) -> None:
        from geodata import GeoHandler

        GeoHandler.store.dsn = self.dsn
        GeoHandler.store.hydrate()
        run_id = str(uuid.uuid4())
        with self.psycopg.connect(self.dsn) as connection:
            connection.execute("SET LOCAL myota.geodata_writer = 'row-v1'")
            connection.execute(
                "INSERT INTO import_run(id, adapter_code, source_metadata, status) "
                "VALUES (%s, 'MANUAL', %s::jsonb, 'QUEUED')",
                (run_id, json.dumps({"source": {"name": "parse restart"}})),
            )

        with tempfile.TemporaryDirectory(prefix="myota-parse-kill-") as tmp:
            marker = Path(tmp) / "parse-started"
            child = r"""
import os, sys, time
from pathlib import Path
from geodata import GeoHandler
run_id, marker = sys.argv[1:]
GeoHandler.store.hydrate()
GeoHandler.store.refresh_import_runs()
def loader():
    if os.environ.get("TEST_PAUSE_BEFORE_FIRST_FEATURE") == "1":
        Path(marker).touch()
        time.sleep(120)
    for index in range(5):
        yield {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-5.99, 37.39]},
            "properties": {"name": f"parse restart {index}"},
        }
body = {
    "adapter": "MANUAL",
    "source": {"name": "parse restart"},
    "entityType": "TEST_PARK",
    "entityTypes": ["TEST_PARK"],
}
if not GeoHandler._process_import_run(run_id, body, loader):
    raise SystemExit("preprocessing worker failed to claim import")
"""
            environment = os.environ.copy()
            environment.update(
                {
                    "GEO_DATABASE_URL": self.dsn,
                    "MYOTA_REQUIRE_DURABILITY": "1",
                    "TEST_PAUSE_BEFORE_FIRST_FEATURE": "1",
                }
            )
            interrupted = subprocess.Popen(
                [sys.executable, "-c", child, run_id, str(marker)],
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            try:
                deadline = time.monotonic() + 30
                while not marker.exists() and time.monotonic() < deadline:
                    if interrupted.poll() is not None:
                        output = interrupted.communicate()[0].decode(
                            "utf-8", "replace"
                        )
                        self.fail(
                            f"worker exited before parsing fixture:\n{output}"
                        )
                    time.sleep(0.1)
                self.assertTrue(marker.exists(), "parser did not start")
                with self.psycopg.connect(self.dsn) as connection:
                    count = connection.execute(
                        "SELECT count(*) FROM geodata_import_candidate "
                        "WHERE import_run_id=%s",
                        (run_id,),
                    ).fetchone()[0]
                self.assertEqual(count, 0)
                interrupted.kill()
                interrupted.wait(timeout=15)
                interrupted.communicate()

                with self.psycopg.connect(self.dsn) as connection:
                    connection.execute(
                        "SET LOCAL myota.geodata_writer = 'row-v1'"
                    )
                    connection.execute(
                        "UPDATE import_run SET status='PROCESSING', "
                        "lease_until=now()-interval '1 second' WHERE id=%s",
                        (run_id,),
                    )
                environment.pop("TEST_PAUSE_BEFORE_FIRST_FEATURE", None)
                replay = subprocess.run(
                    [sys.executable, "-c", child, run_id, str(marker)],
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=60,
                    check=False,
                )
                self.assertEqual(
                    replay.returncode, 0, replay.stdout + replay.stderr
                )
                with self.psycopg.connect(self.dsn) as connection:
                    count, status = connection.execute(
                        "SELECT count(*), run.status "
                        "FROM geodata_import_candidate AS candidate "
                        "JOIN import_run AS run ON run.id=candidate.import_run_id "
                        "WHERE candidate.import_run_id=%s GROUP BY run.status",
                        (run_id,),
                    ).fetchone()
                self.assertEqual((count, status), (5, "PREPROCESSED"))
            finally:
                if interrupted.poll() is None:
                    interrupted.kill()
                    interrupted.wait(timeout=15)
                if interrupted.stdout and not interrupted.stdout.closed:
                    interrupted.stdout.close()
                with self.psycopg.connect(self.dsn) as connection:
                    connection.execute(
                        "SET LOCAL myota.geodata_writer = 'row-v1'"
                    )
                    connection.execute(
                        "DELETE FROM geodata_control_record "
                        "WHERE kind='sourceManifests' AND id=%s",
                        (run_id,),
                    )
                    connection.execute(
                        "DELETE FROM outbox_event WHERE aggregate_id=%s",
                        (run_id,),
                    )
                    connection.execute(
                        "DELETE FROM import_run WHERE id=%s", (run_id,)
                    )
                GeoHandler.store.close()

    def test_complete_snapshot_streaming_reopens_source_with_bounded_rss(
        self,
    ) -> None:
        from geodata import GeoHandler

        GeoHandler.store.dsn = self.dsn
        GeoHandler.store.hydrate()
        run_id = str(uuid.uuid4())
        with self.psycopg.connect(self.dsn) as connection:
            connection.execute("SET LOCAL myota.geodata_writer = 'row-v1'")
            connection.execute(
                "INSERT INTO import_run(id, adapter_code, source_metadata, status) "
                "VALUES (%s, 'MANUAL', %s::jsonb, 'QUEUED')",
                (run_id, json.dumps({"source": {"name": "snapshot RSS"}})),
            )

        child = r"""
import os, resource, sys
from geodata import GeoHandler
run_id = sys.argv[1]
GeoHandler.store.hydrate()
GeoHandler.store.refresh_import_runs()
passes = 0
def loader():
    global passes
    passes += 1
    for index in range(1000):
        yield {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [-5.99 + index * 0.0001, 37.39],
            },
            "properties": {"name": f"snapshot fixture {index}"},
        }
body = {
    "adapter": "MANUAL",
    "source": {"name": "complete snapshot fixture"},
    "entityType": "TEST_PARK",
    "entityTypes": ["TEST_PARK"],
    "completeSnapshot": True,
}
if not GeoHandler._process_import_run(run_id, body, loader):
    raise SystemExit("snapshot worker failed")
peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
peak_bytes = peak if sys.platform == "darwin" else peak * 1024
print(f"loader_passes={passes} peak_rss_bytes={peak_bytes}")
if passes != 2 or peak_bytes > 96 * 1024 * 1024:
    raise SystemExit(1)
GeoHandler.store.close()
"""
        environment = os.environ.copy()
        environment.update(
            {
                "GEO_DATABASE_URL": self.dsn,
                "MYOTA_REQUIRE_DURABILITY": "1",
                "MYOTA_IMPORT_BATCH_SIZE": "100",
            }
        )
        try:
            result = subprocess.run(
                [sys.executable, "-c", child, run_id],
                env=environment,
                capture_output=True,
                text=True,
                timeout=120,
                check=False,
            )
            self.assertEqual(
                result.returncode, 0, result.stdout + result.stderr
            )
            self.assertIn("loader_passes=2", result.stdout)
            print(result.stdout.strip())
            with self.psycopg.connect(self.dsn) as connection:
                count, status = connection.execute(
                    "SELECT count(*), run.status "
                    "FROM geodata_import_candidate AS candidate "
                    "JOIN import_run AS run ON run.id=candidate.import_run_id "
                    "WHERE candidate.import_run_id=%s GROUP BY run.status",
                    (run_id,),
                ).fetchone()
            self.assertEqual((count, status), (1000, "PREPROCESSED"))
        finally:
            with self.psycopg.connect(self.dsn) as connection:
                connection.execute("SET LOCAL myota.geodata_writer = 'row-v1'")
                connection.execute(
                    "DELETE FROM geodata_control_record "
                    "WHERE kind='sourceManifests' AND id=%s",
                    (run_id,),
                )
                connection.execute(
                    "DELETE FROM outbox_event WHERE aggregate_id=%s",
                    (run_id,),
                )
                connection.execute(
                    "DELETE FROM import_run WHERE id=%s", (run_id,)
                )
            GeoHandler.store.close()

    def test_killed_location_enrichment_retries_before_commit(self) -> None:
        from geodata import GeoHandler
        from geodata_pipeline import digest

        GeoHandler.store.dsn = self.dsn
        GeoHandler.store.hydrate()
        entity_id = str(uuid.uuid4())
        request_id = str(uuid.uuid4())
        geometry = {"type": "Point", "coordinates": [-5.99, 37.39]}
        geometry_hash = digest(geometry)
        with GeoHandler.store.operation(write=True):
            GeoHandler.store.items[entity_id] = {
                "id": entity_id,
                "name": "isolated enrichment restart fixture",
                "status": "CANDIDATE",
                "entityType": "TEST_PARK",
                "entityTypes": ["TEST_PARK"],
                "geometry": geometry,
                "centroid": {"lon": -5.99, "lat": 37.39},
                "locationEnrichmentRequestId": request_id,
                "locationEnrichmentGeometryHash": geometry_hash,
                "locationEnrichmentStatus": "QUEUED",
                "manualLocationFields": [],
                "provenance": {},
            }

        with tempfile.TemporaryDirectory(
            prefix="myota-enrichment-kill-"
        ) as tmp:
            marker = Path(tmp) / "provider-started"
            child = r"""
import sys, time
from pathlib import Path
import geodata
from geodata import GeoHandler
entity_id, request_id, geometry_hash, marker = sys.argv[1:]
GeoHandler.store.hydrate()
def provider(_entity):
    Path(marker).touch()
    time.sleep(120)
geodata.lookup_entity_location = provider
GeoHandler._process_location_enrichment(
    entity_id, request_id, geometry_hash, False, "ENTITY_CREATED"
)
"""
            environment = os.environ.copy()
            environment.update(
                {
                    "GEO_DATABASE_URL": self.dsn,
                    "MYOTA_REQUIRE_DURABILITY": "1",
                }
            )
            interrupted = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    child,
                    entity_id,
                    request_id,
                    geometry_hash,
                    str(marker),
                ],
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            try:
                deadline = time.monotonic() + 30
                while not marker.exists() and time.monotonic() < deadline:
                    if interrupted.poll() is not None:
                        output = interrupted.communicate()[0].decode(
                            "utf-8", "replace"
                        )
                        self.fail(
                            f"enrichment worker exited before provider call:\n{output}"
                        )
                    time.sleep(0.1)
                self.assertTrue(marker.exists(), "provider was not entered")
                interrupted.kill()
                interrupted.wait(timeout=15)
                interrupted.communicate()

                replay = r"""
import sys
import geodata
from geodata import GeoHandler
entity_id, request_id, geometry_hash = sys.argv[1:]
GeoHandler.store.hydrate()
geodata.lookup_entity_location = lambda _entity: {
    "country": "Spain", "countryCode": "ES",
    "geocodeStatus": "ENRICHED", "geocodeProvider": "TEST",
}
if not GeoHandler._process_location_enrichment(
    entity_id, request_id, geometry_hash, False, "ENTITY_CREATED"
):
    raise SystemExit("replayed enrichment was not applied")
GeoHandler.store.close()
"""
                result = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        replay,
                        entity_id,
                        request_id,
                        geometry_hash,
                    ],
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=60,
                    check=False,
                )
                self.assertEqual(
                    result.returncode, 0, result.stdout + result.stderr
                )
                with self.psycopg.connect(self.dsn) as connection:
                    status, country = connection.execute(
                        "SELECT public_properties->>'locationEnrichmentStatus', "
                        "public_properties->>'country' "
                        "FROM geodata_entity WHERE id=%s",
                        (entity_id,),
                    ).fetchone()
                self.assertEqual((status, country), ("COMPLETED", "Spain"))
            finally:
                if interrupted.poll() is None:
                    interrupted.kill()
                    interrupted.wait(timeout=15)
                if interrupted.stdout and not interrupted.stdout.closed:
                    interrupted.stdout.close()
                with self.psycopg.connect(self.dsn) as connection:
                    connection.execute(
                        "SET LOCAL myota.geodata_writer = 'row-v1'"
                    )
                    connection.execute(
                        "DELETE FROM geodata_audit_event WHERE aggregate_id=%s",
                        (entity_id,),
                    )
                    connection.execute(
                        "DELETE FROM outbox_event WHERE aggregate_id=%s",
                        (entity_id,),
                    )
                    connection.execute(
                        "DELETE FROM source_reference WHERE entity_id=%s",
                        (entity_id,),
                    )
                    connection.execute(
                        "DELETE FROM geodata_entity WHERE id=%s", (entity_id,)
                    )
                GeoHandler.store.close()

    def test_killed_promotion_replays_without_duplicate_entity(self) -> None:
        from geodata import GeoHandler

        GeoHandler.store.dsn = self.dsn
        GeoHandler.store.hydrate()
        run_id = str(uuid.uuid4())
        candidate_id = str(uuid.uuid4())
        entity_id = str(uuid.uuid4())
        with self.psycopg.connect(self.dsn) as connection:
            connection.execute("SET LOCAL myota.geodata_writer = 'row-v1'")
            connection.execute(
                "INSERT INTO import_run(id, adapter_code, source_metadata, status) "
                "VALUES (%s, 'MANUAL', %s::jsonb, 'PREPROCESSED')",
                (
                    run_id,
                    json.dumps({"source": {"name": "promotion restart"}}),
                ),
            )
            candidate_entity = {
                "id": entity_id,
                "name": "isolated promotion restart fixture",
                "status": "CANDIDATE",
                "programmeSlug": None,
                "entityType": "TEST_PARK",
                "entityTypes": ["TEST_PARK"],
                "geometry": {
                    "type": "Point",
                    "coordinates": [-5.99, 37.39],
                },
                "provenance": {"adapter": "MANUAL"},
            }
            connection.execute(
                "INSERT INTO geodata_import_candidate "
                "(id, import_run_id, ordinal, planned_entity_id, "
                "entity_type_codes, name, geom, entity_payload, "
                "validation_status) VALUES (%s, %s, 0, %s, %s::jsonb, %s, "
                "ST_SetSRID(ST_GeomFromGeoJSON(%s),4326), %s::jsonb, 'PENDING')",
                (
                    candidate_id,
                    run_id,
                    entity_id,
                    json.dumps(["TEST_PARK"]),
                    candidate_entity["name"],
                    json.dumps(candidate_entity["geometry"]),
                    json.dumps(candidate_entity),
                ),
            )
        GeoHandler.store.refresh_import_runs()
        GeoHandler.store.refresh_import_candidates_for_run(run_id)
        with (
            patch.object(GeoHandler, "_authorize_import"),
            patch.object(GeoHandler, "_authorize_review"),
        ):
            queue = GeoHandler.process_import_candidates(
                None,
                {
                    "runId": run_id,
                    "_http": "1",
                    "_body": {
                        "candidateIds": [candidate_id],
                        "targetStatus": "CANDIDATE",
                        "processorId": "isolated-test",
                    },
                },
            )
        queue_id = queue["id"]

        with tempfile.TemporaryDirectory(
            prefix="myota-promotion-kill-"
        ) as tmp:
            marker = Path(tmp) / "entity-materialized"
            child = r"""
import sys, time
from pathlib import Path
from geodata import GeoHandler
queue_id, marker = sys.argv[1:]
GeoHandler.store.hydrate()
original = GeoHandler._materialize_import_candidate
def materialize(*args, **kwargs):
    result = original(*args, **kwargs)
    Path(marker).touch()
    time.sleep(120)
    return result
GeoHandler._materialize_import_candidate = staticmethod(materialize)
if not GeoHandler._process_import_queue(queue_id):
    raise SystemExit("promotion worker did not claim the queue")
"""
            environment = os.environ.copy()
            environment.update(
                {
                    "GEO_DATABASE_URL": self.dsn,
                    "MYOTA_REQUIRE_DURABILITY": "1",
                }
            )
            interrupted = subprocess.Popen(
                [sys.executable, "-c", child, queue_id, str(marker)],
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            try:
                deadline = time.monotonic() + 30
                while not marker.exists() and time.monotonic() < deadline:
                    if interrupted.poll() is not None:
                        output = interrupted.communicate()[0].decode(
                            "utf-8", "replace"
                        )
                        self.fail(
                            f"promotion worker exited before materializing:\n{output}"
                        )
                    time.sleep(0.1)
                self.assertTrue(marker.exists(), "entity was not materialized")
                interrupted.kill()
                interrupted.wait(timeout=15)
                interrupted.communicate()

                with self.psycopg.connect(self.dsn) as connection:
                    connection.execute(
                        "SET LOCAL myota.geodata_writer = 'row-v1'"
                    )
                    connection.execute(
                        "UPDATE geodata_import_processing_queue SET status='PROCESSING', "
                        "lease_until=now()-interval '1 second' WHERE id=%s",
                        (queue_id,),
                    )
                replay = r"""
import sys
from geodata import GeoHandler
queue_id = sys.argv[1]
GeoHandler.store.hydrate()
if not GeoHandler._process_import_queue(queue_id):
    raise SystemExit("promotion replay did not complete")
GeoHandler.store.close()
"""
                result = subprocess.run(
                    [sys.executable, "-c", replay, queue_id],
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=60,
                    check=False,
                )
                self.assertEqual(
                    result.returncode, 0, result.stdout + result.stderr
                )
                with self.psycopg.connect(self.dsn) as connection:
                    status, candidate_status, entity_count = (
                        connection.execute(
                            "SELECT queue.status, candidate.validation_status, "
                            "(SELECT count(*) FROM geodata_entity WHERE id=%s) "
                            "FROM geodata_import_processing_queue AS queue "
                            "JOIN geodata_import_candidate AS candidate "
                            "ON candidate.id=%s WHERE queue.id=%s",
                            (entity_id, candidate_id, queue_id),
                        ).fetchone()
                    )
                self.assertEqual(
                    (status, candidate_status, entity_count),
                    ("COMPLETED", "PROCESSED", 1),
                )
            finally:
                if interrupted.poll() is None:
                    interrupted.kill()
                    interrupted.wait(timeout=15)
                if interrupted.stdout and not interrupted.stdout.closed:
                    interrupted.stdout.close()
                with self.psycopg.connect(self.dsn) as connection:
                    connection.execute(
                        "SET LOCAL myota.geodata_writer = 'row-v1'"
                    )
                    connection.execute(
                        "DELETE FROM geodata_audit_event WHERE aggregate_id=%s",
                        (entity_id,),
                    )
                    connection.execute(
                        "DELETE FROM outbox_event WHERE aggregate_id IN (%s, %s)",
                        (entity_id, queue_id),
                    )
                    connection.execute(
                        "DELETE FROM source_reference WHERE entity_id=%s",
                        (entity_id,),
                    )
                    connection.execute(
                        "DELETE FROM geodata_entity WHERE id=%s", (entity_id,)
                    )
                    connection.execute(
                        "DELETE FROM import_run WHERE id=%s", (run_id,)
                    )
                GeoHandler.store.close()

    def test_concurrent_worker_claims_have_one_owner(self) -> None:
        from geodata import GeoHandler

        GeoHandler.store.dsn = self.dsn
        GeoHandler.store.hydrate()
        run_id = str(uuid.uuid4())
        with self.psycopg.connect(self.dsn) as connection:
            connection.execute("SET LOCAL myota.geodata_writer = 'row-v1'")
            connection.execute(
                "INSERT INTO import_run(id, adapter_code, source_metadata, status) "
                "VALUES (%s, 'MANUAL', %s::jsonb, 'QUEUED')",
                (
                    run_id,
                    json.dumps(
                        {
                            "source": {
                                "name": "isolated concurrent-claim fixture"
                            },
                            "entityType": "TEST_PARK",
                            "entityTypes": ["TEST_PARK"],
                            "format": "GEOJSON",
                        }
                    ),
                ),
            )

        child = r"""
import sys, time
from pathlib import Path
from geodata import GeoHandler
run_id, ready, start, result = sys.argv[1:]
GeoHandler.store.hydrate()
GeoHandler.store.refresh_import_runs()
Path(ready).touch()
while not Path(start).exists():
    time.sleep(0.01)
claimed = GeoHandler._claim_import_run(run_id)
Path(result).write_text("claimed" if claimed else "not-claimed")
GeoHandler.store.close()
"""
        processes: list[subprocess.Popen[bytes]] = []
        try:
            with tempfile.TemporaryDirectory(
                prefix="myota-claim-race-"
            ) as tmp:
                root = Path(tmp)
                start = root / "start"
                results = []
                environment = os.environ.copy()
                environment.update(
                    {
                        "GEO_DATABASE_URL": self.dsn,
                        "MYOTA_REQUIRE_DURABILITY": "1",
                    }
                )
                for index in range(2):
                    ready = root / f"ready-{index}"
                    result = root / f"result-{index}"
                    results.append(result)
                    processes.append(
                        subprocess.Popen(
                            [
                                sys.executable,
                                "-c",
                                child,
                                run_id,
                                str(ready),
                                str(start),
                                str(result),
                            ],
                            env=environment,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                        )
                    )
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    if all((root / f"ready-{i}").exists() for i in range(2)):
                        break
                    time.sleep(0.05)
                self.assertTrue(
                    all((root / f"ready-{i}").exists() for i in range(2)),
                    "both workers did not reach the claim barrier",
                )
                start.touch()
                for process in processes:
                    output, _ = process.communicate(timeout=30)
                    self.assertEqual(process.returncode, 0, output.decode())
                self.assertEqual(
                    sorted(path.read_text() for path in results),
                    ["claimed", "not-claimed"],
                )
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=10)
                process.communicate()
            with self.psycopg.connect(self.dsn) as connection:
                connection.execute("SET LOCAL myota.geodata_writer='row-v1'")
                connection.execute(
                    "DELETE FROM outbox_event WHERE aggregate_id=%s", (run_id,)
                )
                connection.execute(
                    "DELETE FROM import_run WHERE id=%s", (run_id,)
                )
            GeoHandler.store.close()


if __name__ == "__main__":
    unittest.main()
