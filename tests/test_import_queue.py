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
                if status in {"PREPROCESSED", "PREPROCESSED_WITH_ERRORS", "FAILED"}:
                    break
                time.sleep(0.01)

        run = GeoHandler.store.data["importRuns"][run_id]
        self.assertEqual(run["status"], "PREPROCESSED")
        self.assertEqual(run["stats"]["preprocessed"], 1)
        self.assertEqual(len(GeoHandler.store.items), 0)
        candidates = GeoHandler.list_import_candidates(None, {"runId": run_id, "_path": f"?pageSize=10"})
        self.assertEqual(candidates["total"], 1)
        candidate_id = candidates["items"][0]["id"]
        with patch.object(GeoHandler, "_authorize_import"):
            validation = GeoHandler.validate_import_candidates(None, {"runId": run_id, "_body": {
                "candidateIds": [candidate_id], "reviewerId": "admin-1", "note": "Checked in import queue",
            }})
            self.assertEqual(validation["validationStatus"], "CONFIRMED")
            queued = GeoHandler.process_import_candidates(None, {"runId": run_id, "_body": {
                "candidateIds": [candidate_id], "targetStatus": "CANDIDATE", "processorId": "admin-1",
            }})
        queue_id = queued["id"]
        deadline = time.time() + 3
        while time.time() < deadline:
            if GeoHandler.store.data["importProcessingQueues"][queue_id]["status"] in {"COMPLETED", "FAILED"}:
                break
            time.sleep(0.01)
        self.assertEqual(GeoHandler.store.data["importProcessingQueues"][queue_id]["status"], "COMPLETED")
        self.assertEqual(run["stats"]["created"], 1)
        self.assertEqual(len(GeoHandler.store.items), 1)

    def test_empty_pasted_content_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "content must not be empty"):
            GeoHandler.enqueue_import(None, {"_body": {
                "adapter": "MANUAL", "format": "GEOJSON", "entityType": "TRAIL",
                "source": {"name": "empty"}, "content": "   ",
            }})

    def test_confirmed_records_can_be_promoted_directly_to_approved(self):
        body = {
            "adapter": "MANUAL", "format": "GEOJSON", "entityType": "MUNICIPAL_PARK",
            "source": {"name": "validated import", "license": "CC0"},
            "features": [{"type": "Feature", "properties": {"name": "Validated park"},
                          "geometry": {"type": "Point", "coordinates": [-5.99, 37.39]}}],
        }
        result = GeoHandler.enqueue_import(None, {"_body": body})
        candidate_id = result["preprocessed"][0]
        GeoHandler.validate_import_candidates(None, {"runId": result["importRunId"], "_body": {
            "candidateIds": [candidate_id], "reviewerId": "global-admin",
        }})
        queue = GeoHandler.process_import_candidates(None, {"runId": result["importRunId"], "_body": {
            "candidateIds": [candidate_id], "targetStatus": "APPROVED", "processorId": "global-admin",
        }})
        entity = GeoHandler.get_entity(None, {"entityId": queue["result"]["created"][0]})
        self.assertEqual(entity["status"], "APPROVED")

    def test_preprocessing_marks_existing_entity_within_fifty_metres(self):
        existing_id = "existing-seville-park"
        GeoHandler.store.items[existing_id] = {
            "id": existing_id, "name": "Existing Seville park", "status": "APPROVED",
            "entityType": "MUNICIPAL_PARK", "entityTypes": ["MUNICIPAL_PARK"],
            "geometry": {"type": "Point", "coordinates": [-5.9900, 37.3900]},
            "centroid": {"lon": -5.9900, "lat": 37.3900},
        }
        body = {
            "adapter": "MANUAL", "format": "GEOJSON", "entityType": "MUNICIPAL_PARK",
            "source": {"name": "OSM GeoJSON", "license": "ODbL 1.0"},
            "features": [{"type": "Feature", "properties": {"name": "Nearby imported park"},
                          "geometry": {"type": "Point", "coordinates": [-5.9902, 37.3901]}}],
        }
        with patch("geodata.enrich_entity_location", side_effect=lambda entity, force=False: entity):
            result = GeoHandler.enqueue_import(None, {"_body": body})
        candidates = GeoHandler.list_import_candidates(None, {"runId": result["importRunId"], "_path": "?pageSize=10"})
        candidate = candidates["items"][0]
        self.assertEqual(candidate["dedupeWarning"], "POSSIBLE_DUPLICATE")
        self.assertEqual(candidate["possibleDuplicates"][0]["entityId"], existing_id)
        self.assertLess(candidate["possibleDuplicates"][0]["distanceMeters"], 50)


if __name__ == "__main__":
    unittest.main()
