import os
import copy
import tempfile
import threading
from pathlib import Path
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from geodata import GeoHandler, ImportCancelled


class _MissingQueueConnection:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, *_args):
        return self

    @staticmethod
    def fetchone():
        return None


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

    def test_import_source_identity_uses_stable_ids_or_run_scoped_fallbacks(
        self,
    ):
        feature = {
            "type": "Feature",
            "id": "relation/7917223",
            "properties": {
                "@id": "relation/7917223",
                "name": "Purísima Concepción park",
            },
            "geometry": {
                "type": "Point",
                "coordinates": [-5.99, 37.39],
            },
        }
        body = {
            "adapter": "MANUAL",
            "format": "GEOJSON",
            "entityType": "MUNICIPAL_PARK",
            "source": {"name": "manual import"},
            "features": [feature],
        }

        stable = GeoHandler._import_features(body, "run-stable-id")
        stable_candidate = GeoHandler.store.data["importCandidates"][
            stable["preprocessed"][0]
        ]
        self.assertEqual(
            stable_candidate["entity"]["sourceRef"], "relation/7917223"
        )

        existing_entity = {
            "id": "existing-source-reference",
            "name": "Existing imported park",
            "programmeSlug": None,
            "status": "CANDIDATE",
            "sourceRef": "relation/7917223",
            "geometry": feature["geometry"],
            "provenance": {"adapter": "MANUAL"},
        }
        GeoHandler.store.items[existing_entity["id"]] = existing_entity
        reimport = GeoHandler._import_features(body, "run-stable-reimport")
        reimport_candidate = GeoHandler.store.data["importCandidates"][
            reimport["preprocessed"][0]
        ]
        self.assertEqual(
            reimport_candidate["entity"]["sourceRef"], "relation/7917223"
        )
        self.assertEqual(
            reimport_candidate["existingEntityId"], existing_entity["id"]
        )

        generated_feature = {
            **feature,
            "id": None,
            "properties": {"name": "Manual park without a source ID"},
        }
        generated_body = {**body, "features": [generated_feature]}
        first = GeoHandler._import_features(generated_body, "run-first")
        second = GeoHandler._import_features(generated_body, "run-second")
        first_ref = GeoHandler.store.data["importCandidates"][
            first["preprocessed"][0]
        ]["entity"]["sourceRef"]
        second_ref = GeoHandler.store.data["importCandidates"][
            second["preprocessed"][0]
        ]["entity"]["sourceRef"]
        self.assertEqual(first_ref, "manual import:import-run-first:record-0")
        self.assertEqual(
            second_ref, "manual import:import-run-second:record-0"
        )
        self.assertNotEqual(first_ref, second_ref)

    def test_legacy_manual_record_refs_are_replaced_with_stable_or_scoped_ids(
        self,
    ):
        body = {
            "adapter": "MANUAL",
            "format": "GEOJSON",
            "entityType": "MUNICIPAL_PARK",
            "source": {"name": "legacy manual import"},
            "features": [
                {
                    "type": "Feature",
                    "id": "relation/18321907",
                    "properties": {
                        "name": "Imported park",
                        "sourceRef": "MANUAL:record-4",
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [-5.99, 37.39],
                    },
                }
            ],
        }

        result = GeoHandler._import_features(body, "run-repair-id")
        candidate = GeoHandler.store.data["importCandidates"][
            result["preprocessed"][0]
        ]
        self.assertEqual(candidate["entity"]["sourceRef"], "relation/18321907")

        no_id_body = {
            **body,
            "features": [
                {
                    **body["features"][0],
                    "id": None,
                    "properties": {
                        "name": "Imported park without source ID",
                        "sourceRef": "MANUAL:record-4",
                    },
                }
            ],
        }
        first = GeoHandler._import_features(no_id_body, "run-no-id-first")
        second = GeoHandler._import_features(no_id_body, "run-no-id-second")
        first_candidate = GeoHandler.store.data["importCandidates"][
            first["preprocessed"][0]
        ]
        second_candidate = GeoHandler.store.data["importCandidates"][
            second["preprocessed"][0]
        ]
        self.assertEqual(
            first_candidate["entity"]["sourceRef"],
            "legacy manual import:import-run-no-id-first:record-0",
        )
        self.assertNotEqual(
            first_candidate["entity"]["sourceRef"],
            second_candidate["entity"]["sourceRef"],
        )

    def test_legacy_candidate_promotion_does_not_mutate_conflicting_entity(
        self,
    ):
        result = GeoHandler.enqueue_import(
            None,
            {
                "_body": {
                    "adapter": "MANUAL",
                    "format": "GEOJSON",
                    "entityType": "MUNICIPAL_PARK",
                    "source": {"name": "legacy staged import"},
                    "features": [
                        {
                            "type": "Feature",
                            "id": "relation/18321907",
                            "properties": {"name": "Imported park"},
                            "geometry": {
                                "type": "Point",
                                "coordinates": [-5.99, 37.39],
                            },
                        }
                    ],
                }
            },
        )
        candidate_id = result["preprocessed"][0]
        candidate = GeoHandler.store.data["importCandidates"][candidate_id]
        candidate["entity"]["sourceRef"] = "MANUAL:record-0"
        candidate["entity"]["candidateSource"]["sourceRef"] = "MANUAL:record-0"
        candidate["candidateSource"]["sourceRef"] = "MANUAL:record-0"
        conflicting_entity = {
            "id": "existing-manual-record-zero",
            "name": "Unrelated existing park",
            "status": "APPROVED",
            "sourceRef": "MANUAL:record-0",
            "geometry": {
                "type": "Point",
                "coordinates": [-5.8, 37.5],
            },
            "provenance": {"adapter": "MANUAL"},
        }
        GeoHandler.store.items[conflicting_entity["id"]] = copy.deepcopy(
            conflicting_entity
        )

        with patch.object(GeoHandler, "_authorize_import"):
            GeoHandler.validate_import_candidates(
                None,
                {
                    "runId": result["importRunId"],
                    "_body": {
                        "candidateIds": [candidate_id],
                        "reviewerId": "admin-1",
                    },
                },
            )
            queue = GeoHandler.process_import_candidates(
                None,
                {
                    "runId": result["importRunId"],
                    "_body": {
                        "candidateIds": [candidate_id],
                        "targetStatus": "APPROVED",
                        "processorId": "admin-1",
                    },
                },
            )

        self.assertEqual(queue["status"], "COMPLETED")
        self.assertEqual(queue["result"]["errors"], [])
        created_id = queue["result"]["created"][0]
        self.assertNotEqual(created_id, conflicting_entity["id"])
        promoted = GeoHandler.store.items[created_id]
        self.assertEqual(promoted["status"], "APPROVED")
        self.assertEqual(promoted["sourceRef"], "relation/18321907")
        self.assertEqual(
            GeoHandler.store.items[conflicting_entity["id"]],
            conflicting_entity,
        )

    def test_import_does_not_compare_staged_candidates_from_same_run(self):
        body = {
            "adapter": "MANUAL",
            "format": "GEOJSON",
            "entityType": "MUNICIPAL_PARK",
            "source": {"name": "nearby records"},
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "First park"},
                    "geometry": {
                        "type": "Point",
                        "coordinates": [-5.99, 37.39],
                    },
                },
                {
                    "type": "Feature",
                    "properties": {"name": "Second park"},
                    "geometry": {
                        "type": "Point",
                        "coordinates": [-5.9901, 37.3901],
                    },
                },
            ],
        }
        result = GeoHandler._import_features(body, "run-nearby-candidates")
        candidates = GeoHandler.store.data["importCandidates"]
        for candidate_id in result["preprocessed"]:
            candidate = candidates[candidate_id]
            self.assertIsNone(candidate["dedupeWarning"])
            self.assertEqual(candidate["possibleDuplicates"], [])

    def test_imported_location_metadata_is_preserved_without_remote_lookup(
        self,
    ):
        entity = {"centroid": {"lat": 37.39, "lon": -5.99}}
        GeoHandler._apply_import_location_metadata(
            entity,
            {
                "continent": "Europe",
                "continentCode": "EU",
                "country": "Spain",
                "countryCode": "ES",
                "region": "Andalucía",
                "regionCode": "ES-AN",
                "city": "Sevilla",
            },
        )
        with patch("reverse_geocoder.GEOCODER.lookup") as lookup:
            from reverse_geocoder import enrich_entity_location

            enrich_entity_location(entity)

        lookup.assert_not_called()
        self.assertEqual(entity["geocodeStatus"], "SOURCE_DATA")
        self.assertEqual(entity["location"]["countryCode"], "ES")
        self.assertEqual(entity["city"], "Sevilla")

    def test_http_text_import_returns_before_processing_and_exposes_summary(
        self,
    ):
        body = {
            "adapter": "MANUAL",
            "format": "GEOJSON",
            "entityType": "TRAIL",
            "source": {"name": "large pasted dataset", "license": "CC0"},
            "content": '{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"name":"Queued trail"},"geometry":{"type":"LineString","coordinates":[[-5.99,37.39],[-5.98,37.40]]}}]}',
        }
        with patch.object(GeoHandler, "_authorize_import"):
            result = GeoHandler.enqueue_import(
                None,
                {
                    "_body": body,
                    "_http": "1",
                    "Idempotency-Key": "queue-test-1",
                },
            )
            self.assertEqual(result["_status"], 202)
            self.assertEqual(result["status"], "QUEUED")
            run_id = result["id"]
            deadline = time.time() + 3
            while time.time() < deadline:
                status = GeoHandler.store.data["importRuns"][run_id]["status"]
                if status in {
                    "PREPROCESSED",
                    "PREPROCESSED_WITH_ERRORS",
                    "FAILED",
                }:
                    break
                time.sleep(0.01)

        run = GeoHandler.store.data["importRuns"][run_id]
        self.assertEqual(run["status"], "PREPROCESSED")
        self.assertEqual(run["stats"]["preprocessed"], 1)
        self.assertEqual(len(GeoHandler.store.items), 0)
        candidates = GeoHandler.list_import_candidates(
            None, {"runId": run_id, "_path": "?pageSize=10"}
        )
        self.assertEqual(candidates["total"], 1)
        candidate_id = candidates["items"][0]["id"]
        listed_runs = GeoHandler.list_imports(None, {"_path": "?pageSize=10"})
        listed_run = next(
            item for item in listed_runs["items"] if item["id"] == run_id
        )
        self.assertEqual(listed_run["candidateCounts"]["pending"], 1)
        with patch.object(GeoHandler, "_authorize_import"):
            validation = GeoHandler.validate_import_candidates(
                None,
                {
                    "runId": run_id,
                    "_body": {
                        "candidateIds": [candidate_id],
                        "reviewerId": "admin-1",
                        "note": "Checked in import queue",
                    },
                },
            )
            self.assertEqual(validation["validationStatus"], "CONFIRMED")
            queued = GeoHandler.process_import_candidates(
                None,
                {
                    "runId": run_id,
                    "_body": {
                        "candidateIds": [candidate_id],
                        "targetStatus": "CANDIDATE",
                        "processorId": "admin-1",
                    },
                },
            )
        queue_id = queued["id"]
        deadline = time.time() + 3
        while time.time() < deadline:
            if GeoHandler.store.data["importProcessingQueues"][queue_id][
                "status"
            ] in {"COMPLETED", "FAILED"}:
                break
            time.sleep(0.01)
        self.assertEqual(
            GeoHandler.store.data["importProcessingQueues"][queue_id][
                "status"
            ],
            "COMPLETED",
        )
        self.assertEqual(run["stats"]["created"], 1)
        self.assertEqual(len(GeoHandler.store.items), 1)

    def test_cancel_queued_import_is_immediate_and_idempotent(self):
        run_id = "run-cancel-queued"
        GeoHandler.store.data["importRuns"] = {
            run_id: {"id": run_id, "status": "QUEUED", "source": {}}
        }
        params = {"runId": run_id, "_http": "1"}
        with (
            patch.object(GeoHandler, "_authorize_import"),
            patch.object(GeoHandler, "_import_owner", return_value="admin-1"),
        ):
            result = GeoHandler.cancel_import(None, params)
            repeated = GeoHandler.cancel_import(None, params)

        self.assertEqual(result["status"], "CANCELLED")
        self.assertEqual(repeated["status"], "CANCELLED")
        self.assertIsNotNone(result["cancellationRequestedAt"])
        self.assertEqual(
            GeoHandler.store.events[-1]["eventType"],
            "geodata.import.cancelled.v1",
        )

    def test_active_preprocessing_cancellation_discards_staged_candidates(
        self,
    ):
        run_id = "run-cancel-active"
        GeoHandler.store.data["importRuns"] = {
            run_id: {"id": run_id, "status": "PROCESSING", "source": {}}
        }
        GeoHandler.store.data["importCandidates"] = {
            "candidate-1": {"id": "candidate-1", "importRunId": run_id}
        }
        with (
            patch.object(GeoHandler, "_claim_import_run", return_value=True),
            patch.object(GeoHandler, "_prepare_import_body", return_value={}),
            patch.object(
                GeoHandler,
                "_import_features",
                side_effect=ImportCancelled(
                    "cancelled during feature processing"
                ),
            ),
            patch.object(GeoHandler.store, "persist"),
        ):
            result = GeoHandler._process_import_run(
                run_id,
                {"adapter": "MANUAL", "source": {}},
                lambda: [{"type": "Feature", "geometry": None}],
                already_claimed=True,
            )

        self.assertTrue(result)
        self.assertEqual(
            GeoHandler.store.data["importRuns"][run_id]["status"],
            "CANCELLED",
            GeoHandler.store.data["importRuns"][run_id].get("lastError"),
        )
        self.assertNotIn(
            "candidate-1", GeoHandler.store.data["importCandidates"]
        )

    def test_import_completion_joins_heartbeat_before_refresh_and_finalize(
        self,
    ):
        run_id = "run-heartbeat-finalize"
        run = {"id": run_id, "status": "PROCESSING"}
        GeoHandler.store.data["importRuns"] = {run_id: run}
        heartbeat_finished = threading.Event()
        refresh_calls = []

        def heartbeat(_run_id, stop, _cancel_event):
            stop.wait()
            run["heartbeatAt"] = "last-heartbeat"
            heartbeat_finished.set()

        def refresh(_run_id):
            self.assertTrue(heartbeat_finished.is_set())
            refresh_calls.append(_run_id)
            return run

        result = {
            "preprocessed": ["candidate-1"],
            "created": [],
            "updated": [],
            "skipped": [],
            "errors": [],
            "disappeared": [],
            "conflationCandidates": [],
            "manifest": None,
        }
        body = {
            "adapter": "MANUAL",
            "source": {"name": "heartbeat test"},
            "entityType": "TRAIL",
        }

        with (
            patch.object(GeoHandler, "_claim_import_run", return_value=True),
            patch.object(
                GeoHandler, "_heartbeat_import_run", side_effect=heartbeat
            ),
            patch.object(GeoHandler, "_import_features", return_value=result),
            patch.object(
                GeoHandler.store, "refresh_import_run", side_effect=refresh
            ),
            patch.object(GeoHandler.store, "persist"),
        ):
            self.assertTrue(
                GeoHandler._process_import_run(
                    run_id, body, lambda: [], already_claimed=True
                )
            )

        self.assertEqual(refresh_calls, [run_id])
        self.assertEqual(run["status"], "PREPROCESSED")

    def test_empty_pasted_content_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "content must not be empty"):
            GeoHandler.enqueue_import(
                None,
                {
                    "_body": {
                        "adapter": "MANUAL",
                        "format": "GEOJSON",
                        "entityType": "TRAIL",
                        "source": {"name": "empty"},
                        "content": "   ",
                    }
                },
            )

    def test_missing_promotion_queue_acknowledges_stale_delivery(self):
        with (
            patch.object(GeoHandler.store, "dsn", "postgresql://test"),
            patch.object(
                GeoHandler.store,
                "transaction",
                return_value=_MissingQueueConnection(),
            ),
        ):
            self.assertTrue(GeoHandler._process_import_queue("deleted-queue"))

    def test_preprocessing_does_not_wait_for_reverse_geocoding(self):
        body = {
            "adapter": "MANUAL",
            "format": "GEOJSON",
            "entityType": "TRAIL",
            "source": {"name": "mixed-quality import", "license": "CC0"},
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "Valid trail"},
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [[-5.99, 37.39], [-5.98, 37.40]],
                    },
                },
                {
                    "type": "Feature",
                    "properties": {"name": "Broken trail"},
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [[-5.97, 37.39], [-5.96, 37.40]],
                    },
                },
            ],
        }

        with patch("geodata.lookup_entity_location") as lookup:
            result = GeoHandler.enqueue_import(None, {"_body": body})

        run = GeoHandler.store.data["importRuns"][result["importRunId"]]
        self.assertEqual(run["status"], "PREPROCESSED")
        self.assertEqual(run["stats"]["preprocessed"], 2)
        self.assertEqual(run["stats"]["errors"], 0)
        lookup.assert_not_called()
        candidates = GeoHandler.list_import_candidates(
            None, {"runId": result["importRunId"], "_path": "?pageSize=10"}
        )
        self.assertEqual(candidates["total"], 2)
        self.assertEqual(candidates["items"][0]["name"], "Valid trail")
        self.assertEqual(candidates["items"][1]["name"], "Broken trail")

    def test_concurrent_imports_safely_update_shared_candidate_and_manifest_state(
        self,
    ):
        def make_body(label):
            return {
                "adapter": "MANUAL",
                "format": "GEOJSON",
                "entityType": "TRAIL",
                "source": {
                    "name": f"parallel import {label}",
                    "license": "CC0",
                },
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"name": f"{label} trail {index}"},
                        "geometry": {
                            "type": "LineString",
                            "coordinates": [
                                [-5.99 + index * 0.0001, 37.39],
                                [-5.989 + index * 0.0001, 37.391],
                            ],
                        },
                    }
                    for index in range(20)
                ],
            }

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = []
            for label in ("alpha", "bravo"):
                body = make_body(label)
                futures.append(
                    pool.submit(
                        GeoHandler._start_import,
                        body,
                        body["features"],
                        {},
                    )
                )
            results = [future.result(timeout=10) for future in futures]

        self.assertEqual(
            [len(result["preprocessed"]) for result in results], [20, 20]
        )
        self.assertEqual(len(GeoHandler.store.data["importCandidates"]), 40)
        self.assertEqual(len(GeoHandler.store.data["sourceManifests"]), 2)
        self.assertTrue(
            all(result["status"] == "PREPROCESSED" for result in results)
        )

    def test_binary_upload_keeps_pending_run_durable(self):
        body = {
            "adapter": "MANUAL",
            "format": "OSM_PBF",
            "entityType": "TRAIL",
            "source": {"name": "uploaded OSM extract"},
            "filename": "seville.osm.pbf",
            "contentBase64": "Ynl0ZXM=",
        }
        with (
            patch.object(GeoHandler, "_authorize_import"),
            patch(
                "storage.ObjectStore.scan_content",
                return_value={"status": "CLEAN", "sha256": "hash", "size": 5},
            ),
            patch(
                "storage.ObjectStore.put",
                return_value={"sha256": "hash", "size": 5},
            ),
            patch.object(GeoHandler.import_executor, "submit") as submit,
            patch.object(GeoHandler.store, "persist") as persist,
        ):
            result = GeoHandler.upload_import(
                None, {"_body": body, "_http": "1"}
            )
        self.assertEqual(result["status"], "QUEUED")
        self.assertEqual(
            GeoHandler.store.data["importRuns"][result["id"]]["status"],
            "QUEUED",
        )
        persist.assert_called_once_with(include_import_state=True)
        submit.assert_called_once()

    def test_multipart_file_bytes_keep_pending_run_durable(self):
        body = {
            "adapter": "MANUAL",
            "format": "OSM_PBF",
            "entityType": "TRAIL",
            "source": {"name": "multipart OSM extract"},
            "filename": "seville.osm.pbf",
            "_uploadBytes": b"bytes",
            "_uploadFilename": "seville.osm.pbf",
        }
        with (
            patch.object(GeoHandler, "_authorize_import"),
            patch(
                "storage.ObjectStore.scan_content",
                return_value={"status": "CLEAN", "sha256": "hash", "size": 5},
            ),
            patch(
                "storage.ObjectStore.put",
                return_value={"sha256": "hash", "size": 5},
            ),
            patch(
                "geodata.parse_uploaded",
                side_effect=ValueError("binary parser pending"),
            ),
            patch.object(GeoHandler.store, "persist") as persist,
        ):
            result = GeoHandler.upload_import(
                None, {"_body": body, "_http": "1"}
            )
        self.assertEqual(result["status"], "QUEUED")
        self.assertEqual(
            GeoHandler.store.data["importRuns"][result["id"]]["status"],
            "QUEUED",
        )
        persist.assert_called_once_with(include_import_state=True)

    def test_spooled_file_returns_before_storage_handoff(self):
        body = {
            "adapter": "MANUAL",
            "format": "GEOJSON",
            "entityType": "TRAIL",
            "source": {"name": "large multipart dataset"},
            "filename": "seville.geojson",
        }
        with tempfile.NamedTemporaryFile(
            suffix=".part", delete=False
        ) as upload:
            upload.write(b'{"type":"FeatureCollection","features":[]}')
            upload_path = upload.name
        try:
            scan = {
                "status": "CLEAN",
                "sha256": "hash",
                "size": os.path.getsize(upload_path),
            }
            with (
                patch.object(GeoHandler, "_authorize_import"),
                patch("storage.ObjectStore.scan_path", return_value=scan),
                patch.object(GeoHandler.upload_executor, "submit") as submit,
                patch.object(GeoHandler.store, "persist") as persist,
            ):
                result = GeoHandler.upload_import(
                    None,
                    {
                        "_body": {**body, "_uploadPath": upload_path},
                        "_http": "1",
                    },
                )
            self.assertEqual(result["status"], "UPLOAD_PENDING")
            self.assertEqual(result["_status"], 202)
            submit.assert_called_once()
            persist.assert_called_once_with(include_import_state=True)
            self.assertTrue(os.path.exists(upload_path))
        finally:
            os.unlink(upload_path)

    def test_recovery_uses_normalized_format_for_pasted_kml(self):
        run_id = "run-recovery-format"
        GeoHandler.store.data["importRuns"] = {
            run_id: {
                "id": run_id,
                "status": "QUEUED",
                "adapter": "MANUAL",
                "format": "KML",
                "filename": "pasted.kml",
                "entityType": "TRAIL",
                "entityTypes": ["TRAIL"],
                "source": {
                    "bucket": "myota-geodata-imports",
                    "objectKey": "sources/pasted",
                    "recoveryFormat": "GEOJSON",
                },
            }
        }
        with tempfile.TemporaryDirectory() as directory:
            source_path = Path(directory) / "source.geojson"
            source_path.write_text(
                '{"type":"FeatureCollection","features":[]}',
                encoding="utf-8",
            )
            with (
                patch(
                    "storage.ObjectStore.download_to_path",
                    return_value=source_path,
                ),
                patch.object(GeoHandler, "_process_import_run") as process,
            ):
                GeoHandler._recover_import_run(run_id)
        process.assert_called_once()
        self.assertEqual(process.call_args.args[1]["format"], "GEOJSON")
        self.assertEqual(process.call_args.args[1]["filename"], "pasted.kml")

    def test_osm_pbf_recovery_uses_streaming_parser(self):
        run_id = "run-binary-recovery"
        GeoHandler.store.data["importRuns"] = {
            run_id: {
                "id": run_id,
                "status": "QUEUED",
                "format": "OSM_PBF",
                "source": {
                    "bucket": "myota-geodata-imports",
                    "objectKey": "sources/pbf",
                },
            }
        }
        with tempfile.TemporaryDirectory() as directory:
            source_path = Path(directory) / "source.osm.pbf"
            source_path.write_bytes(b"pbf fixture")
            with (
                patch(
                    "storage.ObjectStore.download_to_path",
                    return_value=source_path,
                ),
                patch(
                    "geodata.iter_uploaded_file", return_value=iter([])
                ) as parse,
                patch.object(GeoHandler, "_process_import_run") as process,
                patch.object(GeoHandler.store, "persist"),
            ):
                GeoHandler._recover_import_run(run_id)
                process.assert_called_once()
                args, kwargs = process.call_args
                self.assertEqual(args[1]["format"], "OSM_PBF")
                self.assertTrue(kwargs["already_claimed"])
                self.assertEqual(list(args[2]()), [])
                parse.assert_called_once_with(
                    "OSM_PBF", source_path, "import.geojson"
                )

    def test_startup_recovery_requeues_processing_runs_through_outbox(self):
        run_id = "run-startup-recovery"
        GeoHandler.store.data["importRuns"] = {
            run_id: {
                "id": run_id,
                "status": "PROCESSING",
                "format": "GEOJSON",
                "source": {
                    "bucket": "myota-geodata-imports",
                    "objectKey": "sources/geojson",
                },
            }
        }

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

        with (
            patch.object(GeoHandler.store, "dsn", "test-dsn"),
            patch.object(
                GeoHandler.store, "transaction", return_value=Transaction()
            ),
            patch.object(GeoHandler.store, "persist") as persist,
            patch.object(GeoHandler.import_executor, "submit") as submit,
            patch.object(GeoHandler.store, "event") as event,
        ):
            GeoHandler.recover_import_runs()
        self.assertEqual(
            GeoHandler.store.data["importRuns"][run_id]["status"], "QUEUED"
        )
        self.assertIsNone(
            GeoHandler.store.data["importRuns"][run_id]["leaseUntil"]
        )
        submit.assert_not_called()
        event.assert_called_once_with(
            "geodata.import.recovered.v1",
            "import_run",
            run_id,
            {
                "importRunId": run_id,
                "natsSubject": "myota.geodata.import.preprocess.v1",
            },
        )
        persist.assert_called_once_with(include_import_state=True)

    def test_import_history_returns_newest_runs_first(self):
        GeoHandler.store.data["importRuns"] = {
            f"run-{index}": {
                "id": f"run-{index}",
                "status": "PREPROCESSED",
                "adapter": "MANUAL",
                "format": "GEOJSON",
                "queuedAt": f"2026-09-28T00:{index:02d}:00Z",
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
            "adapter": "MANUAL",
            "format": "GEOJSON",
            "entityType": "MUNICIPAL_PARK",
            "source": {"name": "validated import", "license": "CC0"},
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "Validated park"},
                    "geometry": {
                        "type": "Point",
                        "coordinates": [-5.99, 37.39],
                    },
                }
            ],
        }
        result = GeoHandler.enqueue_import(None, {"_body": body})
        candidate_id = result["preprocessed"][0]
        GeoHandler.validate_import_candidates(
            None,
            {
                "runId": result["importRunId"],
                "_body": {
                    "candidateIds": [candidate_id],
                    "reviewerId": "global-admin",
                },
            },
        )
        queue = GeoHandler.process_import_candidates(
            None,
            {
                "runId": result["importRunId"],
                "_body": {
                    "candidateIds": [candidate_id],
                    "targetStatus": "APPROVED",
                    "processorId": "global-admin",
                },
            },
        )
        entity = GeoHandler.get_entity(
            None, {"entityId": queue["result"]["created"][0]}
        )
        self.assertEqual(entity["status"], "APPROVED")

    def test_pending_records_can_be_promoted_without_a_confirmation_step(self):
        result = GeoHandler.enqueue_import(
            None,
            {
                "_body": {
                    "adapter": "MANUAL",
                    "format": "GEOJSON",
                    "entityType": "TRAIL",
                    "source": {"name": "direct promotion import"},
                    "features": [
                        {
                            "type": "Feature",
                            "properties": {"name": "Directly promoted trail"},
                            "geometry": {
                                "type": "LineString",
                                "coordinates": [
                                    [-5.99, 37.39],
                                    [-5.98, 37.40],
                                ],
                            },
                        }
                    ],
                }
            },
        )
        run_id = result["importRunId"]
        candidate_id = result["preprocessed"][0]
        self.assertEqual(
            GeoHandler.list_import_candidates(
                None, {"runId": run_id, "_path": "?pageSize=10"}
            )["total"],
            1,
        )

        queue = GeoHandler.process_import_candidates(
            None,
            {
                "runId": run_id,
                "_body": {
                    "candidateIds": [candidate_id],
                    "targetStatus": "CANDIDATE",
                    "processorId": "admin-1",
                },
            },
        )

        self.assertEqual(queue["status"], "COMPLETED")
        self.assertEqual(queue["result"]["errors"], [])
        self.assertEqual(
            GeoHandler.list_import_candidates(
                None, {"runId": run_id, "_path": "?pageSize=10"}
            )["total"],
            0,
        )
        entity = GeoHandler.get_entity(
            None, {"entityId": queue["result"]["created"][0]}
        )
        self.assertEqual(entity["status"], "CANDIDATE")

    def test_processing_queue_status_is_scoped_to_its_import_run(self):
        result = GeoHandler.enqueue_import(
            None,
            {
                "_body": {
                    "adapter": "MANUAL",
                    "format": "GEOJSON",
                    "entityType": "TRAIL",
                    "source": {"name": "queue status import"},
                    "features": [
                        {
                            "type": "Feature",
                            "properties": {"name": "Queue status trail"},
                            "geometry": {
                                "type": "LineString",
                                "coordinates": [
                                    [-5.99, 37.39],
                                    [-5.98, 37.40],
                                ],
                            },
                        }
                    ],
                }
            },
        )
        candidate_id = result["preprocessed"][0]
        queue = GeoHandler.process_import_candidates(
            None,
            {
                "runId": result["importRunId"],
                "_body": {
                    "candidateIds": [candidate_id],
                    "targetStatus": "CANDIDATE",
                    "processorId": "admin-1",
                },
            },
        )

        status = GeoHandler.get_import_processing_queue(
            None,
            {
                "runId": result["importRunId"],
                "queueId": queue["id"],
            },
        )

        self.assertEqual(status["id"], queue["id"])
        self.assertEqual(status["status"], "COMPLETED")
        self.assertEqual(status["result"]["errors"], [])
        with self.assertRaisesRegex(KeyError, "queue not found"):
            GeoHandler.get_import_processing_queue(
                None,
                {"runId": "another-import", "queueId": queue["id"]},
            )

    def test_processing_queue_status_reports_queued_and_processing_states(
        self,
    ):
        run_id = "run-visible-queue-state"
        queue_id = "queue-visible-state"
        GeoHandler.store.data["importRuns"] = {
            run_id: {"id": run_id, "status": "PREPROCESSED"}
        }
        queue = {
            "id": queue_id,
            "importRunId": run_id,
            "candidateIds": ["candidate-1"],
            "targetStatus": "CANDIDATE",
            "requestedBy": "reviewer-1",
            "note": "private note",
            "status": "QUEUED",
            "requestedAt": "2026-10-14T08:00:00Z",
            "startedAt": None,
            "completedAt": None,
            "result": {},
        }
        GeoHandler.store.data["importProcessingQueues"] = {queue_id: queue}

        with patch.object(GeoHandler, "_authorize_import"):
            queued = GeoHandler.get_import_processing_queue(
                None, {"runId": run_id, "queueId": queue_id}
            )
            queue.update(
                {"status": "PROCESSING", "startedAt": "2026-10-14T08:01:00Z"}
            )
            processing = GeoHandler.get_import_processing_queue(
                None, {"runId": run_id, "queueId": queue_id}
            )

        self.assertEqual(queued["status"], "QUEUED")
        self.assertEqual(processing["status"], "PROCESSING")
        self.assertEqual(processing["startedAt"], "2026-10-14T08:01:00Z")
        self.assertNotIn("requestedBy", processing)
        self.assertNotIn("note", processing)

    def test_mixed_promotion_outcome_preserves_success_and_reports_failure(
        self,
    ):
        result = GeoHandler.enqueue_import(
            None,
            {
                "_body": {
                    "adapter": "MANUAL",
                    "format": "GEOJSON",
                    "entityType": "TRAIL",
                    "source": {"name": "mixed promotion import"},
                    "features": [
                        {
                            "type": "Feature",
                            "properties": {"name": "Failing trail"},
                            "geometry": {
                                "type": "LineString",
                                "coordinates": [[-5.99, 37.39], [-5.98, 37.4]],
                            },
                        },
                        {
                            "type": "Feature",
                            "properties": {"name": "Successful trail"},
                            "geometry": {
                                "type": "LineString",
                                "coordinates": [[-5.97, 37.39], [-5.96, 37.4]],
                            },
                        },
                    ],
                }
            },
        )
        run_id = result["importRunId"]
        failed_id, successful_id = result["preprocessed"]
        materialize = GeoHandler._materialize_import_candidate

        def fail_one(candidate, target_status, actor, note):
            if candidate["id"] == failed_id:
                raise ValueError("temporary promotion failure")
            return materialize(candidate, target_status, actor, note)

        with (
            patch.object(
                GeoHandler,
                "_materialize_import_candidate",
                side_effect=fail_one,
            ),
            patch.object(GeoHandler.store, "rollback_pending"),
        ):
            queue = GeoHandler.process_import_candidates(
                None,
                {
                    "runId": run_id,
                    "_body": {
                        "candidateIds": [failed_id, successful_id],
                        "targetStatus": "CANDIDATE",
                        "processorId": "admin-1",
                    },
                },
            )
            outcome = GeoHandler.get_import_processing_queue(
                None, {"runId": run_id, "queueId": queue["id"]}
            )

        self.assertEqual(outcome["status"], "FAILED")
        self.assertEqual(
            outcome["result"]["created"],
            [
                GeoHandler.store.data["importCandidates"][successful_id][
                    "entity"
                ]["id"]
            ],
        )
        self.assertEqual(
            outcome["result"]["errors"],
            [
                {
                    "candidateId": failed_id,
                    "message": "temporary promotion failure",
                }
            ],
        )

    def test_integrity_conflict_fails_queue_instead_of_leaving_it_processing(
        self,
    ):
        result = GeoHandler.enqueue_import(
            None,
            {
                "_body": {
                    "adapter": "MANUAL",
                    "format": "GEOJSON",
                    "entityType": "TRAIL",
                    "source": {"name": "conflicting source import"},
                    "features": [
                        {
                            "type": "Feature",
                            "properties": {"name": "Conflicting trail"},
                            "geometry": {
                                "type": "LineString",
                                "coordinates": [[-5.99, 37.39], [-5.98, 37.4]],
                            },
                        }
                    ],
                }
            },
        )
        candidate_id = result["preprocessed"][0]

        class UniqueConstraintError(Exception):
            sqlstate = "23505"

        with patch.object(
            GeoHandler,
            "_materialize_import_candidate",
            side_effect=UniqueConstraintError("duplicate source reference"),
        ):
            queue = GeoHandler.process_import_candidates(
                None,
                {
                    "runId": result["importRunId"],
                    "_body": {
                        "candidateIds": [candidate_id],
                        "targetStatus": "APPROVED",
                        "processorId": "admin-1",
                    },
                },
            )

        outcome = GeoHandler.get_import_processing_queue(
            None,
            {"runId": result["importRunId"], "queueId": queue["id"]},
        )
        candidate = GeoHandler.store.data["importCandidates"][candidate_id]
        self.assertEqual(outcome["status"], "FAILED")
        self.assertEqual(
            outcome["result"]["errors"],
            [
                {
                    "candidateId": candidate_id,
                    "message": (
                        "The source record conflicts with an existing entity. "
                        "Check the import source ID and retry."
                    ),
                }
            ],
        )
        self.assertEqual(candidate["validationStatus"], "CONFIRMED")

    def test_candidate_cannot_be_queued_twice_while_first_queue_is_active(
        self,
    ):
        result = GeoHandler.enqueue_import(
            None,
            {
                "_body": {
                    "adapter": "MANUAL",
                    "format": "GEOJSON",
                    "entityType": "TRAIL",
                    "source": {"name": "duplicate promotion import"},
                    "features": [
                        {
                            "type": "Feature",
                            "properties": {"name": "Single trail"},
                            "geometry": {
                                "type": "LineString",
                                "coordinates": [[-5.99, 37.39], [-5.98, 37.4]],
                            },
                        }
                    ],
                }
            },
        )
        run_id = result["importRunId"]
        candidate_id = result["preprocessed"][0]
        params = {
            "runId": run_id,
            "_http": "1",
            "_body": {
                "candidateIds": [candidate_id],
                "targetStatus": "CANDIDATE",
                "processorId": "admin-1",
            },
        }
        with (
            patch.object(GeoHandler, "_authorize_import"),
            patch.object(GeoHandler.import_executor, "submit"),
        ):
            first = GeoHandler.process_import_candidates(
                None, {**params, "Idempotency-Key": "first-review-attempt"}
            )
            with self.assertRaisesRegex(
                ValueError, "already promoted or queued"
            ):
                GeoHandler.process_import_candidates(
                    None,
                    {**params, "Idempotency-Key": "competing-review-attempt"},
                )

        self.assertEqual(first["status"], "QUEUED")
        self.assertEqual(
            len(GeoHandler.store.data["importProcessingQueues"]), 1
        )

    def test_failed_single_candidate_promotion_can_be_retried(self):
        result = GeoHandler.enqueue_import(
            None,
            {
                "_body": {
                    "adapter": "MANUAL",
                    "format": "GEOJSON",
                    "entityType": "TRAIL",
                    "source": {"name": "promotion retry import"},
                    "features": [
                        {
                            "type": "Feature",
                            "properties": {"name": "Retry trail"},
                            "geometry": {
                                "type": "LineString",
                                "coordinates": [
                                    [-5.99, 37.39],
                                    [-5.98, 37.40],
                                ],
                            },
                        }
                    ],
                }
            },
        )
        run_id = result["importRunId"]
        candidate_id = result["preprocessed"][0]
        materialize = GeoHandler._materialize_import_candidate
        with (
            patch.object(
                GeoHandler,
                "_materialize_import_candidate",
                side_effect=[
                    ValueError("temporary promotion error"),
                    materialize,
                ],
            ),
            patch.object(GeoHandler.store, "rollback_pending"),
        ):
            failed = GeoHandler.process_import_candidates(
                None,
                {
                    "runId": run_id,
                    "_body": {
                        "candidateIds": [candidate_id],
                        "targetStatus": "CANDIDATE",
                        "processorId": "admin-1",
                    },
                },
            )
            failed_status = GeoHandler.get_import_processing_queue(
                None, {"runId": run_id, "queueId": failed["id"]}
            )
            retry = GeoHandler.process_import_candidates(
                None,
                {
                    "runId": run_id,
                    "_body": {
                        "candidateIds": [candidate_id],
                        "targetStatus": "CANDIDATE",
                        "processorId": "admin-1",
                    },
                },
            )

        self.assertEqual(failed_status["status"], "FAILED")
        self.assertEqual(
            failed_status["result"]["errors"][0]["candidateId"],
            candidate_id,
        )
        self.assertEqual(retry["status"], "COMPLETED")
        self.assertEqual(retry["result"]["errors"], [])
        self.assertEqual(len(retry["result"]["created"]), 1)
        candidate = GeoHandler.store.data["importCandidates"][candidate_id]
        self.assertEqual(candidate["validationStatus"], "PROCESSED")
        self.assertEqual(
            candidate["processedEntityId"], retry["result"]["created"][0]
        )

    def test_rejected_records_are_removed_from_the_import(self):
        result = GeoHandler.enqueue_import(
            None,
            {
                "_body": {
                    "adapter": "MANUAL",
                    "format": "GEOJSON",
                    "entityType": "TRAIL",
                    "source": {"name": "rejected import"},
                    "features": [
                        {
                            "type": "Feature",
                            "properties": {"name": "Rejected trail"},
                            "geometry": {
                                "type": "LineString",
                                "coordinates": [
                                    [-5.99, 37.39],
                                    [-5.98, 37.40],
                                ],
                            },
                        }
                    ],
                }
            },
        )
        run_id = result["importRunId"]
        candidate_id = result["preprocessed"][0]
        rejected = GeoHandler.validate_import_candidates(
            None,
            {
                "runId": run_id,
                "_body": {
                    "candidateIds": [candidate_id],
                    "validationStatus": "REJECTED",
                    "reviewerId": "admin-1",
                },
            },
        )
        self.assertEqual(rejected["validationStatus"], "REJECTED")
        self.assertEqual(
            GeoHandler.list_import_candidates(
                None, {"runId": run_id, "_path": "?pageSize=10"}
            )["total"],
            0,
        )
        self.assertNotIn(
            candidate_id, GeoHandler.store.data["importCandidates"]
        )

    def test_preprocessing_marks_existing_entity_within_fifty_metres(self):
        existing_id = "existing-seville-park"
        GeoHandler.store.items[existing_id] = {
            "id": existing_id,
            "name": "Existing Seville park",
            "status": "APPROVED",
            "entityType": "MUNICIPAL_PARK",
            "entityTypes": ["MUNICIPAL_PARK"],
            "geometry": {"type": "Point", "coordinates": [-5.9900, 37.3900]},
            "centroid": {"lon": -5.9900, "lat": 37.3900},
        }
        body = {
            "adapter": "MANUAL",
            "format": "GEOJSON",
            "entityType": "MUNICIPAL_PARK",
            "source": {"name": "OSM GeoJSON", "license": "ODbL 1.0"},
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "Nearby imported park"},
                    "geometry": {
                        "type": "Point",
                        "coordinates": [-5.9902, 37.3901],
                    },
                }
            ],
        }
        result = GeoHandler.enqueue_import(None, {"_body": body})
        candidates = GeoHandler.list_import_candidates(
            None, {"runId": result["importRunId"], "_path": "?pageSize=10"}
        )
        candidate = candidates["items"][0]
        self.assertEqual(candidate["dedupeWarning"], "POSSIBLE_DUPLICATE")
        self.assertEqual(
            candidate["possibleDuplicates"][0]["entityId"], existing_id
        )
        self.assertLess(
            candidate["possibleDuplicates"][0]["distanceMeters"], 50
        )

    def test_import_duplicates_only_match_catalogue_candidates_and_approved(
        self,
    ):
        geometry = {"type": "Point", "coordinates": [-5.99, 37.39]}
        for entity_id, status in (
            ("catalogue-candidate", "CANDIDATE"),
            ("catalogue-approved", "APPROVED"),
            ("catalogue-rejected", "REJECTED"),
            ("catalogue-retired", "RETIRED"),
        ):
            GeoHandler.store.items[entity_id] = {
                "id": entity_id,
                "name": entity_id,
                "status": status,
                "entityType": "MUNICIPAL_PARK",
                "entityTypes": ["MUNICIPAL_PARK"],
                "geometry": geometry,
            }

        other_import_candidate_id = "staged-from-another-import"
        GeoHandler.store.stage_import_candidate(
            {
                "id": other_import_candidate_id,
                "importRunId": "unrelated-run",
                "ordinal": 0,
                "entity": {
                    "id": "staged-other-entity",
                    "name": "Staged from another import",
                    "status": "CANDIDATE",
                    "geometry": geometry,
                },
            },
            is_new=True,
        )

        body = {
            "adapter": "MANUAL",
            "format": "GEOJSON",
            "entityType": "MUNICIPAL_PARK",
            "source": {"name": "same import"},
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": f"Imported park {index}"},
                    "geometry": geometry,
                }
                for index in range(2)
            ],
        }
        result = GeoHandler._import_features(body, "current-run")
        candidates = [
            GeoHandler.store.data["importCandidates"][candidate_id]
            for candidate_id in result["preprocessed"]
        ]

        self.assertEqual(len(candidates), 2)
        for candidate in candidates:
            matches = candidate["possibleDuplicates"]
            self.assertEqual(
                {match["entityId"] for match in matches},
                {"catalogue-candidate", "catalogue-approved"},
            )
            self.assertEqual(
                {match["status"] for match in matches},
                {"CANDIDATE", "APPROVED"},
            )
            self.assertNotIn(
                other_import_candidate_id,
                {match.get("candidateId") for match in matches},
            )

    def test_mark_import_processed_discards_staged_records_and_keeps_summary(
        self,
    ):
        result = GeoHandler.enqueue_import(
            None,
            {
                "_body": {
                    "adapter": "MANUAL",
                    "format": "GEOJSON",
                    "entityType": "TRAIL",
                    "source": {"name": "finalized import", "license": "CC0"},
                    "features": [
                        {
                            "type": "Feature",
                            "properties": {"name": "Staged trail"},
                            "geometry": {
                                "type": "LineString",
                                "coordinates": [
                                    [-5.99, 37.39],
                                    [-5.98, 37.40],
                                ],
                            },
                        }
                    ],
                }
            },
        )
        run_id = result["importRunId"]
        self.assertEqual(
            GeoHandler.list_import_candidates(
                None, {"runId": run_id, "_path": "?pageSize=10"}
            )["total"],
            1,
        )
        finalized = GeoHandler.mark_import_processed(
            None, {"runId": run_id, "_body": {"processedBy": "admin-1"}}
        )
        self.assertEqual(finalized["status"], "PROCESSED")
        self.assertEqual(finalized["stagedRecordsDiscarded"], 1)
        self.assertEqual(
            GeoHandler.list_import_candidates(
                None, {"runId": run_id, "_path": "?pageSize=10"}
            )["total"],
            0,
        )


if __name__ == "__main__":
    unittest.main()
