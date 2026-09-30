import json
import unittest
from contextlib import contextmanager
from unittest.mock import patch

from geodata_store import GeodataStore


class _Result:
    def __init__(self, one=None, many=None):
        self.one = one
        self.many = many if many is not None else []

    def fetchone(self):
        return self.one

    def fetchall(self):
        return self.many


class _Connection:
    def __init__(self, state, entity_rows):
        self.state = state
        self.entity_rows = entity_rows

    def execute(self, statement, _params=None):
        if "SELECT state FROM service_state" in statement:
            return _Result(one=(self.state,))
        if "SELECT key, response FROM idempotency_record" in statement:
            return _Result(many=[])
        if "FROM source_reference" in statement:
            return _Result(many=[])
        if "FROM geodata_entity_category" in statement:
            return _Result(many=[("entity-1", "MUNICIPAL_PARK", True)])
        if "FROM geodata_entity" in statement:
            return _Result(many=self.entity_rows)
        if "FROM import_run" in statement:
            return _Result(many=[])
        raise AssertionError(f"unexpected query: {statement}")


class GeodataStoreHydrationTests(unittest.TestCase):
    def test_relational_status_overrides_stale_compatibility_snapshot(self):
        snapshot = {
            "items": {"entity-1": {
                "id": "entity-1", "name": "Parque de los Príncipes",
                "status": "CANDIDATE", "entityType": "MUNICIPAL_PARK",
            }},
            "events": [], "data": {},
        }
        entity_row = (
            "entity-1", None, "MUNICIPAL_PARK", "Parque de los Príncipes", "APPROVED",
            {"type": "Point", "coordinates": [-6.006222, 37.3739359]},
            json.dumps({"reviewHistory": [{"action": "APPROVED"}]}), "CURRENT", None, [],
        )
        connection = _Connection(snapshot, [entity_row])
        store = GeodataStore()
        store.dsn = "test-dsn"

        @contextmanager
        def transaction():
            yield connection

        with patch.object(store, "transaction", transaction):
            store.hydrate()

        self.assertEqual(store.items["entity-1"]["status"], "APPROVED")
        self.assertEqual(store.items["entity-1"]["name"], "Parque de los Príncipes")
        self.assertEqual(store.items["entity-1"]["geometry"]["type"], "Point")
        self.assertEqual(store.items["entity-1"]["entityTypes"], ["MUNICIPAL_PARK"])


if __name__ == "__main__":
    unittest.main()
