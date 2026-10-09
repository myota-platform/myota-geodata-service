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
