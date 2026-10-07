"""HTTP concurrency proof against two independently running API processes."""

import json
import os
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from common import sign_token
from geodata_store import GeodataStore


@unittest.skipUnless(
    os.environ.get("GEO_TEST_API_A_URL")
    and os.environ.get("GEO_TEST_API_B_URL"),
    "two isolated API processes are not configured",
)
class TwoApiInstancesTests(unittest.TestCase):
    def test_stale_version_conflicts_and_both_replicas_read_the_winner(self):
        from psycopg.conninfo import conninfo_to_dict

        dsn = os.environ["GEO_TEST_DATABASE_URL"]
        self.assertTrue(conninfo_to_dict(dsn)["dbname"].endswith("_tests"))
        store = GeodataStore()
        store.dsn = dsn
        store.hydrate()
        entity_id = str(uuid.uuid4())
        urls = [
            os.environ["GEO_TEST_API_A_URL"],
            os.environ["GEO_TEST_API_B_URL"],
        ]
        token = sign_token(
            {
                "sub": "isolated-test-admin",
                "scp": ["*"],
                "roles": [{"role": "GLOBAL_OPERATOR"}],
                "exp": int(time.time()) + 300,
            }
        )
        path = f"/v1/geodata/entities/{entity_id}"

        def request(url, body=None, version=None):
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
            if version is not None:
                headers["If-Match"] = f'"{version}"'
                headers["Idempotency-Key"] = str(uuid.uuid4())
            req = Request(
                url + path,
                method="PATCH" if body else "GET",
                data=json.dumps(body).encode() if body else None,
                headers=headers,
            )
            try:
                response = urlopen(req, timeout=10)
            except HTTPError as error:
                response = error
            with response:
                return (
                    response.status,
                    json.loads(response.read()),
                    response.headers.get("ETag"),
                )

        try:
            with store.operation(write=True):
                store.items[entity_id] = {
                    "id": entity_id,
                    "name": "Two API fixture",
                    "programmeSlug": None,
                    "status": "CANDIDATE",
                    "entityType": "TEST_PARK",
                    "entityTypes": ["TEST_PARK"],
                    "geometry": {
                        "type": "Point",
                        "coordinates": [-5.99, 37.38],
                    },
                    "provenance": {"adapter": "MANUAL"},
                }
            for url in urls:
                self.assertEqual(request(url)[1]["version"], 1)
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(
                    pool.map(
                        lambda item: request(
                            item[0],
                            {
                                "name": item[1],
                                "editorId": "isolated-test-admin",
                            },
                            1,
                        ),
                        zip(urls, ["Replica A edit", "Replica B edit"]),
                    )
                )
            self.assertEqual(
                sorted(result[0] for result in results), [200, 409]
            )
            reads = [request(url) for url in urls]
            self.assertEqual(reads[0][1], reads[1][1])
            self.assertEqual(reads[0][1]["version"], 2)
            self.assertEqual(reads[0][2], '"2"')
        finally:
            with store.operation(write=True):
                del store.items[entity_id]
            with store.transaction() as connection:
                connection.execute(
                    "DELETE FROM outbox_event WHERE aggregate_id=%s",
                    (entity_id,),
                )
                connection.execute(
                    "DELETE FROM idempotency_record WHERE key LIKE %s",
                    (f"%{entity_id}%",),
                )
            store.close()
