"""Durable relational persistence for the geodata catalogue.

The compatibility JSON snapshot is retained for older service metadata and
tests. Entity writes are additionally stored in the PostGIS-owned tables so a
manual proposal is durable and visible to GIS tooling.
"""
from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from common import Store, json_default, now


def _uuid(value: Any) -> uuid.UUID | None:
    try:
        return uuid.UUID(str(value)) if value else None
    except (ValueError, TypeError, AttributeError):
        return None


def _entity_categories(entity: dict[str, Any]) -> list[str]:
    raw = entity.get("entityTypes") or entity.get("entityTypeCodes") or entity.get("entityType")
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list):
        return []
    result: list[str] = []
    for item in raw:
        code = str(item.get("code") if isinstance(item, dict) else item).strip().upper()
        if code and code not in result:
            result.append(code)
    return result


def _cached_import_expired(run: dict[str, Any], now: datetime | None = None) -> bool:
    """Mirror database retention rules so stale API memory cannot resurrect runs."""
    status = str(run.get("status") or "").upper()
    days = int(os.environ.get("GEODATA_IMPORT_RETENTION_DAYS", "30"))
    if status == "PROCESSED":
        values = [run.get("processedAt")]
    elif status in {"UPLOAD_PENDING", "QUEUED", "PROCESSING", "PREPROCESSED",
                    "PREPROCESSED_WITH_ERRORS", "COMPLETED", "COMPLETED_WITH_ERRORS", "FAILED"}:
        values = [run.get("startedAt") or run.get("queuedAt"), run.get("heartbeatAt"), run.get("completedAt")]
    else:
        return False
    timestamps = []
    for value in values:
        if not value:
            continue
        if isinstance(value, datetime):
            parsed = value
        else:
            try:
                parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            except ValueError:
                continue
        timestamps.append(parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed)
    if not timestamps:
        return False
    reference = min(timestamps) if status == "PROCESSED" else max(timestamps)
    return reference < (now or datetime.now(timezone.utc)) - timedelta(days=days)


class GeodataStore(Store):
    def __init__(self, service: str = "geodata", dsn_env: str | None = None) -> None:
        super().__init__(service, dsn_env)
        self._dirty_import_candidate_ids: set[str] = set()
        self._deleted_import_candidate_ids: set[str] = set()
        self._dirty_import_queue_ids: set[str] = set()
        self._deleted_import_queue_ids: set[str] = set()

    def mark_import_candidate_dirty(self, candidate_id: str) -> None:
        self._dirty_import_candidate_ids.add(str(candidate_id))
        self._deleted_import_candidate_ids.discard(str(candidate_id))

    def mark_import_candidate_deleted(self, candidate_id: str) -> None:
        self._deleted_import_candidate_ids.add(str(candidate_id))
        self._dirty_import_candidate_ids.discard(str(candidate_id))

    def mark_import_queue_dirty(self, queue_id: str) -> None:
        self._dirty_import_queue_ids.add(str(queue_id))
        self._deleted_import_queue_ids.discard(str(queue_id))

    def mark_import_queue_deleted(self, queue_id: str) -> None:
        self._deleted_import_queue_ids.add(str(queue_id))
        self._dirty_import_queue_ids.discard(str(queue_id))

    def _category_id(self, connection: Any, code: str, geometry_type: str) -> uuid.UUID:
        row = connection.execute(
            "SELECT id FROM entity_type WHERE programme_id IS NULL AND code = %s LIMIT 1", (code,)
        ).fetchone()
        if row:
            return row[0]
        category_id = uuid.uuid4()
        connection.execute(
            "INSERT INTO entity_type(id, programme_id, code, label, geometry_kind, config) "
            "VALUES (%s, NULL, %s, %s, %s, '{}'::jsonb)",
            (category_id, code, code.replace("_", " ").title(), geometry_type.upper()),
        )
        return category_id

    @staticmethod
    def _public_properties(entity: dict[str, Any]) -> dict[str, Any]:
        properties = dict(entity)
        properties.pop("geometry", None)
        properties.pop("centroid", None)
        return properties

    def _upsert_entity(self, connection: Any, entity: dict[str, Any]) -> None:
        entity_id = _uuid(entity.get("id"))
        if not entity_id:
            raise ValueError("geodata entity ids must be UUIDs")
        geometry = entity.get("geometry") or {}
        geometry_json = json.dumps(geometry, separators=(",", ":"), default=json_default)
        categories = _entity_categories(entity) or ["UNKNOWN"]
        entity_type = categories[0]
        category_id = self._category_id(connection, entity_type, str(geometry.get("type") or "GEOMETRY"))
        provenance = entity.get("provenance") or {}
        source = provenance.get("source") or {}
        connection.execute(
            "INSERT INTO geodata_entity(id, programme_id, programme_slug, entity_type_id, entity_type_code, name, lifecycle_status, geom, centroid, public_properties, source_state, source_key, source_hash, jurisdiction, attachments) "
            "VALUES (%s, NULL, %s, %s, %s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), ST_Centroid(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))::geography, %s::jsonb, %s, %s, %s, %s, %s::jsonb) "
            "ON CONFLICT (id) DO UPDATE SET programme_slug=EXCLUDED.programme_slug, entity_type_id=EXCLUDED.entity_type_id, entity_type_code=EXCLUDED.entity_type_code, name=EXCLUDED.name, lifecycle_status=EXCLUDED.lifecycle_status, geom=EXCLUDED.geom, centroid=EXCLUDED.centroid, public_properties=EXCLUDED.public_properties, source_state=EXCLUDED.source_state, source_key=EXCLUDED.source_key, source_hash=EXCLUDED.source_hash, jurisdiction=EXCLUDED.jurisdiction, attachments=EXCLUDED.attachments, updated_at=now()",
            (entity_id, entity.get("programmeSlug"), category_id, entity_type, entity.get("name") or "Unnamed entity",
             entity.get("status") or "CANDIDATE", geometry_json, geometry_json, json.dumps(self._public_properties(entity), default=json_default),
             entity.get("sourceState") or "CURRENT", provenance.get("sourceKey"), provenance.get("sourceHash"),
             entity.get("jurisdiction"), json.dumps(entity.get("attachments") or [], default=json_default)),
        )
        connection.execute("DELETE FROM geodata_entity_category WHERE entity_id = %s", (entity_id,))
        for index, category in enumerate(categories):
            category_id = self._category_id(connection, category, str(geometry.get("type") or "GEOMETRY"))
            connection.execute(
                "INSERT INTO geodata_entity_category(entity_id, category_id, category_code, is_primary) VALUES (%s, %s, %s, %s)",
                (entity_id, category_id, category, index == 0),
            )
        connection.execute("DELETE FROM source_reference WHERE entity_id = %s", (entity_id,))
        connection.execute(
            "INSERT INTO source_reference(id, entity_id, adapter_code, source_uri, source_record_id, license, attribution, retrieved_at, source_payload) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)",
            (uuid.uuid4(), entity_id, provenance.get("adapter") or "MANUAL", source.get("url") or source.get("uri"),
             entity.get("sourceRef") or str(entity_id), source.get("license") or provenance.get("license"),
             source.get("attribution") or provenance.get("attribution"), source.get("retrievedAt"),
             json.dumps(provenance.get("sourceFeature") or source, default=json_default)),
        )

    def _sync_relational(self, include_import_state: bool = False) -> None:
        if not self.durable:
            return
        with self.transaction() as connection:
            for entity in self.items.values():
                self._upsert_entity(connection, entity)
            if not include_import_state:
                return
            import_runs = self.data.get("importRuns", {})
            for run_id, run in list(import_runs.items()):
                if _cached_import_expired(run):
                    import_runs.pop(run_id, None)
            for run in import_runs.values():
                run_id = _uuid(run.get("id"))
                if not run_id:
                    continue
                connection.execute(
                    "INSERT INTO import_run(id, adapter_code, source_metadata, started_at, completed_at, stats, status, "
                    "attempt_count, heartbeat_at, lease_until, last_error, processed_at, processed_by) "
                    "VALUES (%s, %s, %s::jsonb, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (id) DO UPDATE SET source_metadata=EXCLUDED.source_metadata, started_at=EXCLUDED.started_at, "
                    "completed_at=EXCLUDED.completed_at, stats=EXCLUDED.stats, status=EXCLUDED.status, "
                    "attempt_count=EXCLUDED.attempt_count, heartbeat_at=EXCLUDED.heartbeat_at, lease_until=EXCLUDED.lease_until, "
                    "last_error=EXCLUDED.last_error, processed_at=EXCLUDED.processed_at, processed_by=EXCLUDED.processed_by",
                    (run_id, run.get("adapter") or "MANUAL", json.dumps({"source": run.get("source") or {}, "programmeSlug": run.get("programmeSlug"),
                                                                           "format": run.get("format"), "filename": run.get("filename"),
                                                                           "entityType": run.get("entityType"), "entityTypes": run.get("entityTypes") or [],
                                                                           "queuedAt": run.get("queuedAt"), "featureCount": run.get("featureCount"),
                                                                           "errors": run.get("errors") or [], "manifest": run.get("manifest"),
                                                                           "conflationCandidateCount": run.get("conflationCandidateCount", 0),
                                                                           "binaryObjectPending": bool(run.get("binaryObjectPending")),
                                                                           "uploadSpoolPath": run.get("uploadSpoolPath")}, default=json_default),
                     run.get("startedAt") or run.get("queuedAt") or now(), run.get("completedAt"), json.dumps(run.get("stats") or {}, default=json_default),
                     run.get("status") or "QUEUED", int(run.get("attemptCount") or 0), run.get("heartbeatAt"),
                     run.get("leaseUntil"), run.get("lastError"), run.get("processedAt"), run.get("processedBy")),
                )
            for candidate_id in self._deleted_import_candidate_ids:
                connection.execute("DELETE FROM geodata_import_candidate WHERE id = %s", (_uuid(candidate_id),))
            for candidate_id in self._dirty_import_candidate_ids:
                candidate = self.data.get("importCandidates", {}).get(candidate_id)
                if not candidate:
                    continue
                entity = candidate.get("entity") or {}
                geometry = entity.get("geometry") or {}
                candidate_id = _uuid(candidate.get("id"))
                run_id = _uuid(candidate.get("importRunId"))
                if not candidate_id or not run_id or not geometry:
                    continue
                connection.execute(
                    "INSERT INTO geodata_import_candidate "
                    "(id, import_run_id, ordinal, planned_entity_id, programme_slug, entity_type_codes, name, geom, candidate_source, source_ref, source_hash, provenance, entity_payload, validation_status, validation_note, validated_by, validated_at, target_status, processed_entity_id, processed_at, updated_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), %s::jsonb, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, now()) "
                    "ON CONFLICT (id) DO UPDATE SET validation_status=EXCLUDED.validation_status, validation_note=EXCLUDED.validation_note, validated_by=EXCLUDED.validated_by, validated_at=EXCLUDED.validated_at, target_status=EXCLUDED.target_status, processed_entity_id=EXCLUDED.processed_entity_id, processed_at=EXCLUDED.processed_at, entity_payload=EXCLUDED.entity_payload, updated_at=now()",
                    (candidate_id, run_id, int(candidate.get("ordinal", 0)), _uuid(entity.get("id")), entity.get("programmeSlug"),
                     json.dumps(entity.get("entityTypes") or [], default=json_default), entity.get("name") or "Unnamed candidate", json.dumps(geometry, default=json_default),
                     json.dumps(candidate.get("candidateSource") or {}, default=json_default), entity.get("sourceRef"), (entity.get("provenance") or {}).get("sourceHash"),
                     json.dumps(entity.get("provenance") or {}, default=json_default), json.dumps(entity, default=json_default), candidate.get("validationStatus", "PENDING"),
                     candidate.get("validationNote"), candidate.get("validatedBy"), candidate.get("validatedAt"), candidate.get("targetStatus"),
                     _uuid(candidate.get("processedEntityId")), candidate.get("processedAt")),
                )
            for queue_id in self._deleted_import_queue_ids:
                connection.execute("DELETE FROM geodata_import_processing_queue WHERE id = %s", (_uuid(queue_id),))
            for queue_id in self._dirty_import_queue_ids:
                queue = self.data.get("importProcessingQueues", {}).get(queue_id)
                if not queue:
                    continue
                queue_id = _uuid(queue.get("id"))
                run_id = _uuid(queue.get("importRunId"))
                if not queue_id or not run_id:
                    continue
                connection.execute(
                    "INSERT INTO geodata_import_processing_queue(id, import_run_id, candidate_ids, target_status, requested_by, status, result, error, requested_at, started_at, completed_at) "
                    "VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s::jsonb, %s, %s, %s, %s) "
                    "ON CONFLICT (id) DO UPDATE SET status=EXCLUDED.status, result=EXCLUDED.result, error=EXCLUDED.error, started_at=EXCLUDED.started_at, completed_at=EXCLUDED.completed_at",
                    (queue_id, run_id, json.dumps(queue.get("candidateIds") or [], default=json_default), queue.get("targetStatus"), queue.get("requestedBy"),
                     queue.get("status", "QUEUED"), json.dumps(queue.get("result") or {}, default=json_default), queue.get("error"), queue.get("requestedAt"),
                     queue.get("startedAt"), queue.get("completedAt")),
                )

    def delete_relational(self, entity_id: str) -> None:
        if not self.durable:
            return
        entity_uuid = _uuid(entity_id)
        if not entity_uuid:
            return
        with self.transaction() as connection:
            connection.execute("DELETE FROM geodata_entity_category WHERE entity_id = %s", (entity_uuid,))
            connection.execute("DELETE FROM source_reference WHERE entity_id = %s", (entity_uuid,))
            connection.execute("DELETE FROM entity_review WHERE entity_id = %s", (entity_uuid,))
            connection.execute("DELETE FROM conflation_candidate WHERE left_entity_id = %s OR right_entity_id = %s", (entity_uuid, entity_uuid))
            connection.execute("DELETE FROM geodata_entity WHERE id = %s", (entity_uuid,))

    def hydrate(self) -> None:
        super().hydrate()
        if not self.durable:
            return
        self._dirty_import_candidate_ids.clear()
        self._deleted_import_candidate_ids.clear()
        self._dirty_import_queue_ids.clear()
        self._deleted_import_queue_ids.clear()
        with self.transaction() as connection:
            rows = connection.execute(
                "SELECT id::text, programme_slug, entity_type_code, name, lifecycle_status, ST_AsGeoJSON(geom)::jsonb, public_properties, source_state, jurisdiction, attachments FROM geodata_entity"
            ).fetchall()
            source_rows = connection.execute(
                "SELECT entity_id::text, adapter_code, source_uri, source_record_id, license, attribution, retrieved_at, source_payload FROM source_reference"
            ).fetchall()
            category_rows = connection.execute(
                "SELECT entity_id::text, category_code, is_primary FROM geodata_entity_category ORDER BY entity_id, is_primary DESC, category_code"
            ).fetchall()
            import_rows = connection.execute(
                "SELECT id::text, adapter_code, source_metadata, started_at, completed_at, stats, status, "
                "attempt_count, heartbeat_at, lease_until, last_error, processed_at, processed_by FROM import_run"
            ).fetchall()
            candidate_rows = connection.execute(
                "SELECT id::text, import_run_id::text, ordinal, planned_entity_id::text, programme_slug, "
                "entity_type_codes, candidate_source, source_ref, source_hash, provenance, entity_payload, "
                "validation_status, validation_note, validated_by, validated_at, target_status, "
                "processed_entity_id::text, processed_at FROM geodata_import_candidate"
            ).fetchall()
            queue_rows = connection.execute(
                "SELECT id::text, import_run_id::text, candidate_ids, target_status, requested_by, status, "
                "result, error, requested_at, started_at, completed_at FROM geodata_import_processing_queue"
            ).fetchall()
        import_runs = self.data.setdefault("importRuns", {})
        for row in import_rows:
            metadata = row[2] if isinstance(row[2], dict) else json.loads(row[2] or "{}")
            run = import_runs.setdefault(row[0], {"id": row[0]})
            # Relational state is authoritative for lifecycle and timestamps;
            # preserve compatibility-only fields such as manifest and errors.
            run.update({
                "id": row[0], "adapter": row[1], "source": metadata.get("source") or run.get("source") or {},
                "programmeSlug": metadata.get("programmeSlug", run.get("programmeSlug")),
                "format": metadata.get("format", run.get("format") or "GEOJSON"),
                "filename": metadata.get("filename", run.get("filename")),
                "entityType": metadata.get("entityType", run.get("entityType")),
                "entityTypes": metadata.get("entityTypes") or run.get("entityTypes") or [],
                "queuedAt": metadata.get("queuedAt", run.get("queuedAt")),
                "featureCount": metadata.get("featureCount", run.get("featureCount")),
                "binaryObjectPending": bool(metadata.get("binaryObjectPending", run.get("binaryObjectPending", False))),
                "uploadSpoolPath": metadata.get("uploadSpoolPath", run.get("uploadSpoolPath")),
                "status": row[6], "startedAt": row[3].isoformat().replace("+00:00", "Z") if row[3] else run.get("startedAt"),
                "completedAt": row[4].isoformat().replace("+00:00", "Z") if row[4] else run.get("completedAt"),
                "stats": row[5] if isinstance(row[5], dict) else json.loads(row[5] or "{}"),
                "attemptCount": row[7] or 0,
                "heartbeatAt": row[8].isoformat().replace("+00:00", "Z") if row[8] else None,
                "leaseUntil": row[9].isoformat().replace("+00:00", "Z") if row[9] else None,
                "lastError": row[10],
                "processedAt": row[11].isoformat().replace("+00:00", "Z") if row[11] else None,
                "processedBy": row[12],
            })
        snapshot_candidates = self.data.get("importCandidates") or {}
        import_candidates: dict[str, dict[str, Any]] = {}
        for row in candidate_rows:
            entity = row[10] if isinstance(row[10], dict) else json.loads(row[10] or "{}")
            if not entity.get("geometry"):
                # The relational geometry is intentionally not selected here:
                # entity_payload is the canonical normalized candidate payload.
                continue
            import_candidates[row[0]] = {
                "id": row[0], "importRunId": row[1], "ordinal": row[2],
                "existingEntityId": row[3], "candidateSource": row[7] or {},
                "validationStatus": row[11], "validationNote": row[12],
                "validatedBy": row[13], "validatedAt": row[14].isoformat().replace("+00:00", "Z") if row[14] else None,
                "targetStatus": row[15], "processedEntityId": row[16],
                "processedAt": row[17].isoformat().replace("+00:00", "Z") if row[17] else None,
                "dedupeWarning": entity.get("dedupeWarning"),
                "possibleDuplicates": entity.get("possibleDuplicates") or [], "entity": entity,
            }
        if not candidate_rows and snapshot_candidates:
            # Older deployments kept staged records only in service_state. Keep
            # them available for a one-time migration into the relational
            # tables instead of silently losing an in-flight import on restart.
            import_candidates = snapshot_candidates
            self._dirty_import_candidate_ids.update(import_candidates)
        self.data["importCandidates"] = import_candidates
        snapshot_queues = self.data.get("importProcessingQueues") or {}
        import_queues: dict[str, dict[str, Any]] = {}
        for row in queue_rows:
            import_queues[row[0]] = {
                "id": row[0], "importRunId": row[1],
                "candidateIds": row[2] if isinstance(row[2], list) else json.loads(row[2] or "[]"),
                "targetStatus": row[3], "requestedBy": row[4], "status": row[5],
                "result": row[6] if isinstance(row[6], dict) else json.loads(row[6] or "{}"),
                "error": row[7],
                "requestedAt": row[8].isoformat().replace("+00:00", "Z") if row[8] else None,
                "startedAt": row[9].isoformat().replace("+00:00", "Z") if row[9] else None,
                "completedAt": row[10].isoformat().replace("+00:00", "Z") if row[10] else None,
            }
        if not queue_rows and snapshot_queues:
            import_queues = snapshot_queues
            self._dirty_import_queue_ids.update(import_queues)
        self.data["importProcessingQueues"] = import_queues
        self._deleted_import_candidate_ids.clear()
        self._deleted_import_queue_ids.clear()
        sources = {row[0]: row for row in source_rows}
        categories = {}
        for entity_id, category_code, _ in category_rows:
            categories.setdefault(entity_id, []).append(category_code)
        for row in rows:
            properties = row[6] if isinstance(row[6], dict) else json.loads(row[6] or "{}")
            # The relational entity row is authoritative for lifecycle fields.
            # service_state is retained as a compatibility snapshot, but it may
            # lag behind a status mutation when a process is restarted between
            # the relational write and the snapshot write. Always overlay the
            # durable columns, including for entities already present there.
            entity = {**self.items.get(row[0], {}), **properties, "id": row[0],
                      "programmeSlug": row[1], "entityType": row[2], "name": row[3],
                      "status": row[4], "geometry": row[5], "sourceState": row[7],
                      "jurisdiction": row[8], "attachments": row[9] or []}
            entity["entityTypes"] = categories.get(row[0]) or [row[2]]
            entity["entityTypeCodes"] = list(entity["entityTypes"])
            source = sources.get(row[0])
            if source:
                payload = source[7] if isinstance(source[7], dict) else json.loads(source[7] or "{}")
                entity["sourceRef"] = source[3]
                entity.setdefault("provenance", {}).update({
                    "adapter": source[1], "source": {"url": source[2], "license": source[4], "attribution": source[5], "retrievedAt": source[6].isoformat().replace("+00:00", "Z") if source[6] else None},
                    "sourceFeature": payload,
                })
            self.items[row[0]] = entity

    def refresh_import_runs(self) -> None:
        """Refresh import lifecycle state from PostgreSQL for read endpoints."""
        if not self.durable:
            return
        with self.transaction() as connection:
            rows = connection.execute(
                "SELECT id::text, adapter_code, source_metadata, started_at, completed_at, stats, status, "
                "attempt_count, heartbeat_at, lease_until, last_error, processed_at, processed_by FROM import_run"
            ).fetchall()
        with self.lock:
            import_runs = self.data.setdefault("importRuns", {})
            durable_ids = {row[0] for row in rows}
            for run_id in set(import_runs) - durable_ids:
                import_runs.pop(run_id, None)
            for row in rows:
                metadata = row[2] if isinstance(row[2], dict) else json.loads(row[2] or "{}")
                run = import_runs.setdefault(row[0], {"id": row[0]})
                run.update({
                    "id": row[0], "adapter": row[1], "source": metadata.get("source") or run.get("source") or {},
                    "programmeSlug": metadata.get("programmeSlug", run.get("programmeSlug")),
                    "format": metadata.get("format", run.get("format") or "GEOJSON"),
                    "filename": metadata.get("filename", run.get("filename")),
                    "entityType": metadata.get("entityType", run.get("entityType")),
                    "entityTypes": metadata.get("entityTypes") or run.get("entityTypes") or [],
                    "queuedAt": metadata.get("queuedAt", run.get("queuedAt")),
                    "featureCount": metadata.get("featureCount", run.get("featureCount")),
                    "binaryObjectPending": bool(metadata.get("binaryObjectPending", run.get("binaryObjectPending", False))),
                    "uploadSpoolPath": metadata.get("uploadSpoolPath", run.get("uploadSpoolPath")),
                    "status": row[6], "startedAt": row[3].isoformat().replace("+00:00", "Z") if row[3] else run.get("startedAt"),
                    "completedAt": row[4].isoformat().replace("+00:00", "Z") if row[4] else run.get("completedAt"),
                    "stats": row[5] if isinstance(row[5], dict) else json.loads(row[5] or "{}"),
                    "attemptCount": row[7] or 0,
                    "heartbeatAt": row[8].isoformat().replace("+00:00", "Z") if row[8] else None,
                    "leaseUntil": row[9].isoformat().replace("+00:00", "Z") if row[9] else None,
                    "lastError": row[10],
                    "processedAt": row[11].isoformat().replace("+00:00", "Z") if row[11] else None,
                    "processedBy": row[12],
                })

    def persist(self, include_import_state: bool = False) -> None:
        self._sync_relational(include_import_state)
        # Candidate payloads are durable in PostGIS. Keep the compatibility
        # snapshot small so a large import does not rewrite every staged
        # feature on every review action.
        compact_data = {key: value for key, value in self.data.items()
                        if key not in {"importCandidates", "importProcessingQueues"}}
        super().persist({"items": self.items, "events": self.events, "data": compact_data})
        self._dirty_import_candidate_ids.clear()
        self._deleted_import_candidate_ids.clear()
        self._dirty_import_queue_ids.clear()
        self._deleted_import_queue_ids.clear()
