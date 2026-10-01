import os
import tempfile
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
        listed_runs = GeoHandler.list_imports(None, {"_path": "?pageSize=10"})
        listed_run = next(item for item in listed_runs["items"] if item["id"] == run_id)
        self.assertEqual(listed_run["candidateCounts"]["pending"], 1)
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

    def test_preprocessing_keeps_valid_entities_when_one_feature_errors(self):
        body = {
            "adapter": "MANUAL", "format": "GEOJSON", "entityType": "TRAIL",
            "source": {"name": "mixed-quality import", "license": "CC0"},
            "features": [
                {"type": "Feature", "properties": {"name": "Valid trail"},
                 "geometry": {"type": "LineString", "coordinates": [[-5.99, 37.39], [-5.98, 37.40]]}},
                {"type": "Feature", "properties": {"name": "Broken trail"},
                 "geometry": {"type": "LineString", "coordinates": [[-5.97, 37.39], [-5.96, 37.40]]}},
            ],
        }

        def enrich(entity, force=False):
            if entity["name"] == "Broken trail":
                raise RuntimeError("reverse geocoder failed for this feature")
            return entity

        with patch("geodata.enrich_entity_location", side_effect=enrich):
            result = GeoHandler.enqueue_import(None, {"_body": body})

        run = GeoHandler.store.data["importRuns"][result["importRunId"]]
        self.assertEqual(run["status"], "PREPROCESSED_WITH_ERRORS")
        self.assertEqual(run["stats"]["preprocessed"], 1)
        self.assertEqual(run["stats"]["errors"], 1)
        candidates = GeoHandler.list_import_candidates(None, {"runId": result["importRunId"], "_path": "?pageSize=10"})
        self.assertEqual(candidates["total"], 1)
        self.assertEqual(candidates["items"][0]["name"], "Valid trail")
        self.assertIn("reverse geocoder failed", run["errors"][0]["message"])

    def test_binary_upload_keeps_pending_run_durable(self):
        body = {
            "adapter": "MANUAL", "format": "OSM_PBF", "entityType": "TRAIL",
            "source": {"name": "uploaded OSM extract"}, "filename": "seville.osm.pbf",
            "contentBase64": "Ynl0ZXM=",
        }
        with patch.object(GeoHandler, "_authorize_import"), \
             patch("storage.ObjectStore.scan_content", return_value={"status": "CLEAN", "sha256": "hash", "size": 5}), \
             patch("storage.ObjectStore.put", return_value={"sha256": "hash", "size": 5}), \
             patch("geodata.parse_uploaded", side_effect=ValueError("binary parser pending")), \
             patch.object(GeoHandler.store, "persist") as persist:
            result = GeoHandler.upload_import(None, {"_body": body, "_http": "1"})
        self.assertEqual(result["status"], "QUEUED")
        self.assertEqual(GeoHandler.store.data["importRuns"][result["id"]]["status"], "QUEUED")
        persist.assert_called_once_with(include_import_state=True)

    def test_multipart_file_bytes_keep_pending_run_durable(self):
        body = {
            "adapter": "MANUAL", "format": "OSM_PBF", "entityType": "TRAIL",
            "source": {"name": "multipart OSM extract"}, "filename": "seville.osm.pbf",
            "_uploadBytes": b"bytes", "_uploadFilename": "seville.osm.pbf",
        }
        with patch.object(GeoHandler, "_authorize_import"), \
             patch("storage.ObjectStore.scan_content", return_value={"status": "CLEAN", "sha256": "hash", "size": 5}), \
             patch("storage.ObjectStore.put", return_value={"sha256": "hash", "size": 5}), \
             patch("geodata.parse_uploaded", side_effect=ValueError("binary parser pending")), \
             patch.object(GeoHandler.store, "persist") as persist:
            result = GeoHandler.upload_import(None, {"_body": body, "_http": "1"})
        self.assertEqual(result["status"], "QUEUED")
        self.assertEqual(GeoHandler.store.data["importRuns"][result["id"]]["status"], "QUEUED")
        persist.assert_called_once_with(include_import_state=True)

    def test_spooled_file_returns_before_storage_handoff(self):
        body = {
            "adapter": "MANUAL", "format": "GEOJSON", "entityType": "TRAIL",
            "source": {"name": "large multipart dataset"}, "filename": "seville.geojson",
        }
        with tempfile.NamedTemporaryFile(suffix=".part", delete=False) as upload:
            upload.write(b'{"type":"FeatureCollection","features":[]}')
            upload_path = upload.name
        try:
            scan = {"status": "CLEAN", "sha256": "hash", "size": os.path.getsize(upload_path)}
            with patch.object(GeoHandler, "_authorize_import"), \
                 patch("storage.ObjectStore.scan_path", return_value=scan), \
                 patch.object(GeoHandler.upload_executor, "submit") as submit, \
                 patch.object(GeoHandler.store, "persist") as persist:
                result = GeoHandler.upload_import(None, {"_body": {**body, "_uploadPath": upload_path}, "_http": "1"})
            self.assertEqual(result["status"], "UPLOAD_PENDING")
            self.assertEqual(result["_status"], 202)
            submit.assert_called_once()
            persist.assert_called_once_with(include_import_state=True)
            self.assertTrue(os.path.exists(upload_path))
        finally:
            os.unlink(upload_path)

    def test_recovery_uses_normalized_format_for_pasted_kml(self):
        run_id = "run-recovery-format"
        GeoHandler.store.data["importRuns"] = {run_id: {
            "id": run_id, "status": "QUEUED", "adapter": "MANUAL", "format": "KML",
            "filename": "pasted.kml", "entityType": "TRAIL", "entityTypes": ["TRAIL"],
            "source": {"bucket": "myota-geodata-imports", "objectKey": "sources/pasted",
                       "recoveryFormat": "GEOJSON"},
        }}
        with patch("storage.ObjectStore.get", return_value=b'{"type":"FeatureCollection","features":[]}'), \
             patch.object(GeoHandler, "_process_import_run") as process:
            GeoHandler._recover_import_run(run_id)
        process.assert_called_once()
        self.assertEqual(process.call_args.args[1]["format"], "GEOJSON")
        self.assertEqual(process.call_args.args[1]["filename"], "pasted.kml")

    def test_binary_recovery_keeps_pending_run_queued(self):
        run_id = "run-binary-recovery"
        GeoHandler.store.data["importRuns"] = {run_id: {
            # Older rows may predate the persisted binaryObjectPending flag;
            # the format itself must still keep them queued.
            "id": run_id, "status": "QUEUED", "format": "OSM_PBF",
            "source": {"bucket": "myota-geodata-imports", "objectKey": "sources/pbf"},
        }}
        with patch.object(GeoHandler, "_process_import_run") as process, \
             patch.object(GeoHandler.store, "persist") as persist:
            GeoHandler._recover_import_run(run_id)
        process.assert_not_called()
        self.assertEqual(GeoHandler.store.data["importRuns"][run_id]["status"], "QUEUED")
        self.assertIn("queued for an available parser", GeoHandler.store.data["importRuns"][run_id]["lastError"])
        persist.assert_called_once_with(include_import_state=True)

    def test_startup_recovery_requeues_processing_runs_before_dispatch(self):
        run_id = "run-startup-recovery"
        GeoHandler.store.data["importRuns"] = {run_id: {
            "id": run_id, "status": "PROCESSING", "format": "GEOJSON",
            "source": {"bucket": "myota-geodata-imports", "objectKey": "sources/geojson"},
        }}

        class Cursor:
            def fetchall(self):
                return [(run_id, "PROCESSING", None)]

            def execute(self, *_args):
                return self

        class Transaction:
            def __enter__(self):
                return Cursor()

            def __exit__(self, *_args):
                return False

        with patch.object(GeoHandler.store, "dsn", "test-dsn"), \
             patch.object(GeoHandler.store, "transaction", return_value=Transaction()), \
             patch.object(GeoHandler.store, "persist") as persist, \
             patch.object(GeoHandler.import_executor, "submit") as submit:
            GeoHandler.recover_import_runs()
        self.assertEqual(GeoHandler.store.data["importRuns"][run_id]["status"], "QUEUED")
        self.assertIsNone(GeoHandler.store.data["importRuns"][run_id]["leaseUntil"])
        submit.assert_called_once_with(GeoHandler._recover_import_run, run_id)
        persist.assert_called_once_with(include_import_state=True)

    def test_import_history_returns_newest_runs_first(self):
        GeoHandler.store.data["importRuns"] = {
            f"run-{index}": {
                "id": f"run-{index}", "status": "PREPROCESSED", "adapter": "MANUAL",
                "format": "GEOJSON", "queuedAt": f"2026-09-28T00:{index:02d}:00Z",
            }
            for index in range(37)
        }
        result = GeoHandler.list_imports(None, {"_path": "?pageSize=20"})
        self.assertEqual(result["items"][0]["id"], "run-36")
        self.assertEqual(result["items"][-1]["id"], "run-17")
        self.assertEqual(result["nextPage"], 2)

    def test_import_reads_refresh_durable_run_projection(self):
        with patch.object(GeoHandler.store, "refresh_import_runs") as refresh:
            GeoHandler.list_imports(None, {"_path": "?pageSize=10"})
        refresh.assert_called_once_with()

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

    def test_pending_records_can_be_promoted_without_a_confirmation_step(self):
        result = GeoHandler.enqueue_import(None, {"_body": {
            "adapter": "MANUAL", "format": "GEOJSON", "entityType": "TRAIL",
            "source": {"name": "direct promotion import"},
            "features": [{"type": "Feature", "properties": {"name": "Directly promoted trail"},
                          "geometry": {"type": "LineString", "coordinates": [[-5.99, 37.39], [-5.98, 37.40]]}}],
        }})
        run_id = result["importRunId"]
        candidate_id = result["preprocessed"][0]
        self.assertEqual(GeoHandler.list_import_candidates(None, {"runId": run_id, "_path": "?pageSize=10"})["total"], 1)

        queue = GeoHandler.process_import_candidates(None, {"runId": run_id, "_body": {
            "candidateIds": [candidate_id], "targetStatus": "CANDIDATE", "processorId": "admin-1",
        }})

        self.assertEqual(queue["status"], "COMPLETED")
        self.assertEqual(queue["result"]["errors"], [])
        self.assertEqual(GeoHandler.list_import_candidates(None, {"runId": run_id, "_path": "?pageSize=10"})["total"], 0)
        entity = GeoHandler.get_entity(None, {"entityId": queue["result"]["created"][0]})
        self.assertEqual(entity["status"], "CANDIDATE")

    def test_rejected_records_are_removed_from_the_import(self):
        result = GeoHandler.enqueue_import(None, {"_body": {
            "adapter": "MANUAL", "format": "GEOJSON", "entityType": "TRAIL",
            "source": {"name": "rejected import"},
            "features": [{"type": "Feature", "properties": {"name": "Rejected trail"},
                          "geometry": {"type": "LineString", "coordinates": [[-5.99, 37.39], [-5.98, 37.40]]}}],
        }})
        run_id = result["importRunId"]
        candidate_id = result["preprocessed"][0]
        rejected = GeoHandler.validate_import_candidates(None, {"runId": run_id, "_body": {
            "candidateIds": [candidate_id], "validationStatus": "REJECTED", "reviewerId": "admin-1",
        }})
        self.assertEqual(rejected["validationStatus"], "REJECTED")
        self.assertEqual(GeoHandler.list_import_candidates(None, {"runId": run_id, "_path": "?pageSize=10"})["total"], 0)
        self.assertNotIn(candidate_id, GeoHandler.store.data["importCandidates"])

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

    def test_mark_import_processed_discards_staged_records_and_keeps_summary(self):
        result = GeoHandler.enqueue_import(None, {"_body": {
            "adapter": "MANUAL", "format": "GEOJSON", "entityType": "TRAIL",
            "source": {"name": "finalized import", "license": "CC0"},
            "features": [{"type": "Feature", "properties": {"name": "Staged trail"},
                          "geometry": {"type": "LineString", "coordinates": [[-5.99, 37.39], [-5.98, 37.40]]}}],
        }})
        run_id = result["importRunId"]
        self.assertEqual(GeoHandler.list_import_candidates(None, {"runId": run_id, "_path": "?pageSize=10"})["total"], 1)
        finalized = GeoHandler.mark_import_processed(None, {"runId": run_id, "_body": {"processedBy": "admin-1"}})
        self.assertEqual(finalized["status"], "PROCESSED")
        self.assertEqual(finalized["stagedRecordsDiscarded"], 1)
        self.assertEqual(GeoHandler.list_import_candidates(None, {"runId": run_id, "_path": "?pageSize=10"})["total"], 0)


if __name__ == "__main__":
    unittest.main()
