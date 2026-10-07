import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock

from import_retention import purge_expired_imports, source_object
from geodata_store import _cached_import_expired
from storage import ObjectStore


class ImportRetentionTests(unittest.TestCase):
    def test_cached_retention_preserves_active_processing_heartbeat(self):
        now = datetime(2026, 10, 5, tzinfo=timezone.utc)
        old = (now - timedelta(days=31)).isoformat().replace("+00:00", "Z")
        recent = (
            (now - timedelta(minutes=10)).isoformat().replace("+00:00", "Z")
        )
        self.assertTrue(
            _cached_import_expired(
                {"status": "FAILED", "completedAt": old}, now
            )
        )
        self.assertTrue(
            _cached_import_expired(
                {"status": "PROCESSED", "processedAt": old}, now
            )
        )
        self.assertFalse(
            _cached_import_expired(
                {
                    "status": "PROCESSING",
                    "startedAt": old,
                    "heartbeatAt": recent,
                },
                now,
            )
        )
        self.assertTrue(
            _cached_import_expired(
                {"status": "PREPROCESSED", "completedAt": old}, now
            )
        )

    def test_only_configured_import_bucket_objects_are_selected(self):
        self.assertEqual(
            source_object(
                {
                    "source": {
                        "bucket": "imports",
                        "objectKey": "geodata-imports/run.zip",
                    }
                },
                "imports",
            ),
            ("imports", "geodata-imports/run.zip"),
        )
        self.assertIsNone(source_object({"source": {}}, "imports"))
        with self.assertRaisesRegex(
            ValueError, "outside the configured import bucket"
        ):
            source_object(
                {
                    "source": {
                        "bucket": "myota-awards",
                        "objectKey": "award.pdf",
                    }
                },
                "imports",
            )
        with self.assertRaisesRegex(ValueError, "unsafe"):
            source_object(
                {"source": {"bucket": "imports", "objectKey": "../award.pdf"}},
                "imports",
            )

    def test_filesystem_object_delete_is_idempotent_and_bucket_scoped(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ObjectStore()
            store.local_root = Path(directory)
            source = (
                Path(directory) / "imports" / "geodata-imports" / "run.zip"
            )
            source.parent.mkdir(parents=True)
            source.write_bytes(b"uploaded source")

            store.delete("imports", "geodata-imports/run.zip")
            store.delete("imports", "geodata-imports/run.zip")
            self.assertFalse(source.exists())
            with self.assertRaises(ValueError):
                store.delete("imports", "../../outside")

    def test_worker_selects_only_old_processed_runs_and_cleans_their_object(
        self,
    ):
        class Cursor:
            def __init__(self, rows=(), rowcount=0):
                self.rows = rows
                self.rowcount = rowcount

            def fetchall(self):
                return self.rows

            def fetchone(self):
                return self.rows[0] if self.rows else None

        class Connection:
            def __init__(self, selection=False):
                self.selection = selection
                self.statements = []

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def execute(self, query, params=()):
                self.statements.append((query, params))
                if query.startswith("SELECT id::text, source_metadata"):
                    self.assert_eligibility_query(query, params)
                    assert "id = ANY(%s::uuid[])" in query
                    return Cursor(
                        [
                            (
                                "run-1",
                                {
                                    "source": {
                                        "bucket": "myota-geodata-imports",
                                        "objectKey": "geodata-imports/run-1.geojson",
                                    }
                                },
                            )
                        ]
                    )
                if query.startswith("SELECT id FROM import_run"):
                    assert "status = 'PROCESSED'" in query
                    assert "status IN ('UPLOAD_PENDING'" in query
                    assert "FOR UPDATE" in query
                    return Cursor([("run-1",)])
                if query.startswith("SELECT event_id FROM outbox_event"):
                    return Cursor()
                if query.startswith("DELETE FROM import_run"):
                    assert "status = 'PROCESSED'" in query
                    return Cursor(rowcount=1)
                return Cursor()

            @staticmethod
            def assert_eligibility_query(query, params):
                assert "status = 'PROCESSED'" in query
                assert "status IN ('UPLOAD_PENDING'" in query
                assert "GREATEST(started_at" in query
                assert params == (30, 30, ["bad-run"], 100)

        connections = []

        def connect(_dsn):
            connection = Connection()
            connections.append(connection)
            return connection

        objects = Mock()
        result = purge_expired_imports(
            dsn="postgresql://test",
            object_store=objects,
            connection_factory=connect,
            retention_days=30,
            batch_size=100,
            excluded_run_ids={"bad-run"},
        )

        self.assertEqual(
            result,
            {"eligible": 1, "purged": 1, "failed": 0, "failedRunIds": []},
        )
        objects.delete.assert_called_once_with(
            "myota-geodata-imports", "geodata-imports/run-1.geojson"
        )
        self.assertEqual(len(connections), 2)

    def test_object_delete_failure_keeps_import_history_for_retry(self):
        class Cursor:
            def fetchall(self):
                return [
                    (
                        "run-1",
                        {
                            "source": {
                                "bucket": "myota-geodata-imports",
                                "objectKey": "geodata-imports/run-1.geojson",
                            }
                        },
                    )
                ]

        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def execute(self, query, _params=()):
                assert "status = 'PROCESSED'" in query
                assert "status IN ('UPLOAD_PENDING'" in query
                assert "GREATEST(started_at" in query
                return Cursor()

        connection_count = []

        def connect(_dsn):
            connection_count.append(True)
            return Connection()

        objects = Mock()
        objects.delete.side_effect = RuntimeError(
            "object store temporarily unavailable"
        )
        with self.assertLogs("myota.geodata.import_retention", level="ERROR"):
            result = purge_expired_imports(
                dsn="postgresql://test",
                object_store=objects,
                connection_factory=connect,
                retention_days=30,
                batch_size=100,
            )

        self.assertEqual(
            result,
            {
                "eligible": 1,
                "purged": 0,
                "failed": 1,
                "failedRunIds": ["run-1"],
            },
        )
        self.assertEqual(len(connection_count), 1)

    def test_unavailable_database_configuration_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, "GEO_DATABASE_URL"):
            purge_expired_imports(dsn="", object_store=Mock())


if __name__ == "__main__":
    unittest.main()
