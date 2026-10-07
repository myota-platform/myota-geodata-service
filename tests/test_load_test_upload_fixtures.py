"""Upload cleanup remains exact-tag scoped and refuses active transfers."""

import json
import os
import unittest
import uuid
from unittest.mock import MagicMock

from load_test_upload_fixtures import (
    UploadFixture,
    purge_upload_fixtures,
    tagged_upload_fixtures,
)


class UploadFixtureTests(unittest.TestCase):
    @staticmethod
    def store(rows):
        store = MagicMock()
        store.durable = True
        connection = store.transaction.return_value.__enter__.return_value
        connection.execute.return_value.fetchall.return_value = rows
        return store, connection

    def test_terminal_sessions_are_locked_and_exact_tag_scoped(self):
        rows = [
            (str(index), "imports", "object", status)
            for index, status in enumerate(
                ["COMPLETED", "ABORTED", "FAILED", "EXPIRED"]
            )
        ]
        store, connection = self.store(rows)
        fixtures = tagged_upload_fixtures(store, "lt-one")
        self.assertEqual(len(fixtures), 4)
        sql, parameters = connection.execute.call_args.args
        self.assertIn("FOR UPDATE", sql)
        self.assertIn("metadata #>> '{source,loadTestRunId}'=%s", sql)
        self.assertEqual(parameters, ("lt-one",))

    def test_active_uploads_are_not_deleted(self):
        for status in ["UPLOADING", "COMPLETING"]:
            with self.subTest(status=status):
                store, _ = self.store([("id", "imports", "object", status)])
                with self.assertRaisesRegex(
                    ValueError, "finish or be aborted"
                ):
                    tagged_upload_fixtures(store, "lt-one")

    def test_delete_rechecks_tag_identity_and_terminal_state(self):
        store, connection = self.store([])
        connection.execute.return_value.rowcount = 1
        fixtures = [UploadFixture("id", "imports", "object", "COMPLETED")]
        self.assertEqual(purge_upload_fixtures(store, "lt-one", fixtures), 1)
        sql, parameters = connection.execute.call_args.args
        self.assertIn("id=ANY(%s::uuid[])", sql)
        self.assertIn("metadata #>> '{source,loadTestRunId}'=%s", sql)
        self.assertIn("status IN", sql)
        self.assertEqual(parameters, (["id"], "lt-one"))

    def test_empty_fixture_set_does_not_delete_anything(self):
        store, connection = self.store([])
        self.assertEqual(purge_upload_fixtures(store, "lt-one", []), 0)
        connection.execute.assert_not_called()

    def test_in_memory_mode_has_no_upload_session_table(self):
        store, connection = self.store([])
        store.durable = False
        self.assertEqual(tagged_upload_fixtures(store, "lt-one"), [])
        connection.execute.assert_not_called()


@unittest.skipUnless(
    os.environ.get("GEO_TEST_DATABASE_URL"),
    "isolated PostGIS test database is not configured",
)
class UploadFixtureDatabaseTests(unittest.TestCase):
    def test_cleanup_cascades_parts_and_preserves_other_tags(self):
        import psycopg
        from psycopg.conninfo import conninfo_to_dict

        from geodata_store import GeodataStore

        dsn = os.environ["GEO_TEST_DATABASE_URL"]
        if not conninfo_to_dict(dsn).get("dbname", "").endswith("_tests"):
            raise RuntimeError(
                "integration tests require an isolated *_tests DB"
            )
        ids = [str(uuid.uuid4()) for _ in range(3)]
        tag = f"lt-{uuid.uuid4()}"
        store = GeodataStore()
        store.dsn = dsn
        try:
            with psycopg.connect(dsn) as connection:
                for upload_id, status, run_tag in zip(
                    ids,
                    ["COMPLETED", "ABORTED", "UPLOADING"],
                    [tag, f"{tag}-other", f"{tag}-active"],
                    strict=True,
                ):
                    connection.execute(
                        "INSERT INTO geodata_upload_session "
                        "(id, owner_subject, idempotency_key, filename, metadata, "
                        "bucket, object_key, multipart_upload_id, expected_size, "
                        "status, expires_at) VALUES "
                        "(%s, 'test-admin', %s, 'test.geojson', %s::jsonb, "
                        "'test-imports', %s, 'test-multipart', 1, %s, "
                        "now() + interval '1 day')",
                        (
                            upload_id,
                            upload_id,
                            json.dumps({"source": {"loadTestRunId": run_tag}}),
                            upload_id,
                            status,
                        ),
                    )
                    connection.execute(
                        "INSERT INTO geodata_upload_part "
                        "(upload_session_id, part_number, size_bytes, sha256, etag) "
                        "VALUES (%s, 1, 1, %s, 'test-etag')",
                        (upload_id, "a" * 64),
                    )
            with store.operation(write=True):
                fixtures = tagged_upload_fixtures(store, tag)
                self.assertEqual(
                    [item.upload_id for item in fixtures], [ids[0]]
                )
                self.assertEqual(
                    purge_upload_fixtures(store, tag, fixtures), 1
                )
            with store.operation(write=True):
                with self.assertRaisesRegex(
                    ValueError, "finish or be aborted"
                ):
                    tagged_upload_fixtures(store, f"{tag}-active")
            with psycopg.connect(dsn) as connection:
                remaining = connection.execute(
                    "SELECT id::text FROM geodata_upload_session "
                    "WHERE id=ANY(%s::uuid[])",
                    (ids,),
                ).fetchall()
                parts = connection.execute(
                    "SELECT upload_session_id::text FROM geodata_upload_part "
                    "WHERE upload_session_id=ANY(%s::uuid[])",
                    (ids,),
                ).fetchall()
                self.assertEqual({row[0] for row in remaining}, set(ids[1:]))
                self.assertEqual({row[0] for row in parts}, set(ids[1:]))
        finally:
            with psycopg.connect(dsn) as connection:
                connection.execute(
                    "DELETE FROM geodata_upload_session WHERE id=ANY(%s::uuid[])",
                    (ids,),
                )
            store.close()


if __name__ == "__main__":
    unittest.main()
