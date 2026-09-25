import time
import unittest
from unittest.mock import patch

from geodata import GeoHandler


class ImportQueueTests(unittest.TestCase):
    def setUp(self):
        self.previous_items = GeoHandler.store.items
        self.previous_data = GeoHandler.store.data
        self.previous_events = GeoHandler.store.events
        self.previous_idempotency = GeoHandler.store.idempotency
        GeoHandler.store.items = {}
        GeoHandler.store.data = {}
        GeoHandler.store.events = []
        GeoHandler.store.idempotency = {}

    def tearDown(self):
        GeoHandler.store.items = self.previous_items
        GeoHandler.store.data = self.previous_data
        GeoHandler.store.events = self.previous_events
        GeoHandler.store.idempotency = self.previous_idempotency

    def test_http_text_import_returns_before_processing_and_exposes_summary(self):
        body = {
            "adapter": "MANUAL",
            "format": "GEOJSON",
            "entityType": "TRAIL",
            "source": {"name": "large pasted dataset", "license": "CC0"},
            "content": '{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"name":"Queued trail"},"geometry":{"type":"LineString","coordinates":[[-5.99,37.39],[-5.98,37.40]]}}]}',
        }
        with patch("geodata.enrich_entity_location", side_effect=lambda entity, force=False: entity), patch.object(GeoHandler, "_authorize_import"):
            result = GeoHandler.enqueue_import(None, {"_body": body, "_http": "1", "Idempotency-Key": "queue-test-1"})
            self.assertEqual(result["_status"], 202)
            self.assertEqual(result["status"], "QUEUED")
            run_id = result["id"]
            deadline = time.time() + 3
            while time.time() < deadline:
                status = GeoHandler.store.data["importRuns"][run_id]["status"]
                if status in {"COMPLETED", "COMPLETED_WITH_ERRORS", "FAILED"}:
                    break
                time.sleep(0.01)

        run = GeoHandler.store.data["importRuns"][run_id]
        self.assertEqual(run["status"], "COMPLETED")
        self.assertEqual(run["stats"]["created"], 1)
        self.assertEqual(len(GeoHandler.store.items), 1)

    def test_empty_pasted_content_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "content must not be empty"):
            GeoHandler.enqueue_import(None, {"_body": {
                "adapter": "MANUAL", "format": "GEOJSON", "entityType": "TRAIL",
                "source": {"name": "empty"}, "content": "   ",
            }})


if __name__ == "__main__":
    unittest.main()
