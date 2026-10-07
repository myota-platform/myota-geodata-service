"""Row encoders and database-authoritative geodata repositories."""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

from common import Store, json_default, now
from metrics import METRICS


def _uuid(value: Any) -> uuid.UUID | None:
    try:
        return uuid.UUID(str(value)) if value else None
    except (ValueError, TypeError, AttributeError):
        return None


def _entity_categories(entity: dict[str, Any]) -> list[str]:
    raw = (
        entity.get("entityTypes")
        or entity.get("entityTypeCodes")
        or entity.get("entityType")
    )
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list):
        return []
    result: list[str] = []
    for item in raw:
        code = (
            str(item.get("code") if isinstance(item, dict) else item)
            .strip()
            .upper()
        )
        if code and code not in result:
            result.append(code)
    return result


def _cached_import_expired(
    run: dict[str, Any], now: datetime | None = None
) -> bool:
    """Mirror database retention rules so stale API memory cannot resurrect runs."""
    status = str(run.get("status") or "").upper()
    days = int(os.environ.get("GEODATA_IMPORT_RETENTION_DAYS", "30"))
    if status == "PROCESSED":
        values = [run.get("processedAt")]
    elif status in {
        "UPLOAD_PENDING",
        "QUEUED",
        "PROCESSING",
        "PREPROCESSED",
        "PREPROCESSED_WITH_ERRORS",
        "COMPLETED",
        "COMPLETED_WITH_ERRORS",
        "FAILED",
    }:
        values = [
            run.get("startedAt") or run.get("queuedAt"),
            run.get("heartbeatAt"),
            run.get("completedAt"),
        ]
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
                parsed = datetime.fromisoformat(
                    str(value).replace("Z", "+00:00")
                )
            except ValueError:
                continue
        timestamps.append(
            parsed.replace(tzinfo=timezone.utc)
            if parsed.tzinfo is None
            else parsed
        )
    if not timestamps:
        return False
    reference = min(timestamps) if status == "PROCESSED" else max(timestamps)
    return reference < (now or datetime.now(timezone.utc)) - timedelta(
        days=days
    )


class CompatibilityGeodataStore(Store):
    def __init__(
        self, service: str = "geodata", dsn_env: str | None = None
    ) -> None:
        super().__init__(service, dsn_env)
        self._dirty_import_candidate_ids: set[str] = set()
        self._deleted_import_candidate_ids: set[str] = set()
        self._dirty_import_queue_ids: set[str] = set()
        self._deleted_import_queue_ids: set[str] = set()

    def mark_import_candidate_dirty(self, candidate_id: str) -> None:
        with self.lock:
            self._dirty_import_candidate_ids.add(str(candidate_id))
            self._deleted_import_candidate_ids.discard(str(candidate_id))

    def mark_import_candidate_deleted(self, candidate_id: str) -> None:
        with self.lock:
            self._deleted_import_candidate_ids.add(str(candidate_id))
            self._dirty_import_candidate_ids.discard(str(candidate_id))

    def mark_import_queue_dirty(self, queue_id: str) -> None:
        with self.lock:
            self._dirty_import_queue_ids.add(str(queue_id))
            self._deleted_import_queue_ids.discard(str(queue_id))

    def mark_import_queue_deleted(self, queue_id: str) -> None:
        with self.lock:
            self._deleted_import_queue_ids.add(str(queue_id))
            self._dirty_import_queue_ids.discard(str(queue_id))

    def _category_id(
        self, connection: Any, code: str, geometry_type: str
    ) -> uuid.UUID:
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (f"geodata:category:{code}",),
        )
        row = connection.execute(
            "SELECT id FROM entity_type WHERE programme_id IS NULL AND code = %s LIMIT 1",
            (code,),
        ).fetchone()
        if row:
            return row[0]
        category_id = uuid.uuid4()
        connection.execute(
            "INSERT INTO entity_type(id, programme_id, code, label, geometry_kind, config) "
            "VALUES (%s, NULL, %s, %s, %s, '{}'::jsonb)",
            (
                category_id,
                code,
                code.replace("_", " ").title(),
                geometry_type.upper(),
            ),
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
        geometry_json = json.dumps(
            geometry, separators=(",", ":"), default=json_default
        )
        categories = _entity_categories(entity) or ["UNKNOWN"]
        entity_type = categories[0]
        category_id = self._category_id(
            connection, entity_type, str(geometry.get("type") or "GEOMETRY")
        )
        provenance = entity.get("provenance") or {}
        source = provenance.get("source") or {}
        connection.execute(
            "INSERT INTO geodata_entity(id, programme_id, programme_slug, entity_type_id, entity_type_code, name, lifecycle_status, geom, centroid, public_properties, source_state, source_key, source_hash, jurisdiction, attachments) "
            "VALUES (%s, NULL, %s, %s, %s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), ST_Centroid(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))::geography, %s::jsonb, %s, %s, %s, %s, %s::jsonb) "
            "ON CONFLICT (id) DO UPDATE SET programme_slug=EXCLUDED.programme_slug, entity_type_id=EXCLUDED.entity_type_id, entity_type_code=EXCLUDED.entity_type_code, name=EXCLUDED.name, lifecycle_status=EXCLUDED.lifecycle_status, geom=EXCLUDED.geom, centroid=EXCLUDED.centroid, public_properties=EXCLUDED.public_properties, source_state=EXCLUDED.source_state, source_key=EXCLUDED.source_key, source_hash=EXCLUDED.source_hash, jurisdiction=EXCLUDED.jurisdiction, attachments=EXCLUDED.attachments, updated_at=now()",
            (
                entity_id,
                entity.get("programmeSlug"),
                category_id,
                entity_type,
                entity.get("name") or "Unnamed entity",
                entity.get("status") or "CANDIDATE",
                geometry_json,
                geometry_json,
                json.dumps(
                    self._public_properties(entity), default=json_default
                ),
                entity.get("sourceState") or "CURRENT",
                provenance.get("sourceKey"),
                provenance.get("sourceHash"),
                entity.get("jurisdiction"),
                json.dumps(
                    entity.get("attachments") or [], default=json_default
                ),
            ),
        )
        connection.execute(
            "DELETE FROM geodata_entity_category WHERE entity_id = %s",
            (entity_id,),
        )
        for index, category in enumerate(categories):
            category_id = self._category_id(
                connection, category, str(geometry.get("type") or "GEOMETRY")
            )
            connection.execute(
                "INSERT INTO geodata_entity_category(entity_id, category_id, category_code, is_primary) VALUES (%s, %s, %s, %s)",
                (entity_id, category_id, category, index == 0),
            )
        connection.execute(
            "DELETE FROM source_reference WHERE entity_id = %s", (entity_id,)
        )
        connection.execute(
            "INSERT INTO source_reference(id, entity_id, adapter_code, source_uri, source_record_id, license, attribution, retrieved_at, source_payload) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)",
            (
                uuid.uuid4(),
                entity_id,
                provenance.get("adapter") or "MANUAL",
                source.get("url") or source.get("uri"),
                entity.get("sourceRef") or str(entity_id),
                source.get("license") or provenance.get("license"),
                source.get("attribution") or provenance.get("attribution"),
                source.get("retrievedAt"),
                json.dumps(
                    provenance.get("sourceFeature") or source,
                    default=json_default,
                ),
            ),
        )

    def _sync_relational(self, include_import_state: bool = False) -> None:
        if not self.durable:
            return
        with self.transaction() as connection:
            for entity in self.items.values():
                started = time.perf_counter()
                try:
                    self._upsert_entity(connection, entity)
                finally:
                    self._observe_postgis_query("entity_upsert", started)
            if not include_import_state:
                return
            import_runs = self.data.get("importRuns", {})
            for run in import_runs.values():
                run_id = _uuid(run.get("id"))
                if not run_id:
                    continue
                connection.execute(
                    "INSERT INTO import_run(id, adapter_code, source_metadata, started_at, completed_at, stats, status, "
                    "attempt_count, heartbeat_at, lease_until, last_error, processed_at, processed_by, "
                    "cancellation_requested_at, cancellation_requested_by) "
                    "VALUES (%s, %s, %s::jsonb, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (id) DO UPDATE SET source_metadata=EXCLUDED.source_metadata, started_at=EXCLUDED.started_at, "
                    "completed_at=EXCLUDED.completed_at, stats=EXCLUDED.stats, status=EXCLUDED.status, "
                    "attempt_count=EXCLUDED.attempt_count, heartbeat_at=EXCLUDED.heartbeat_at, lease_until=EXCLUDED.lease_until, "
                    "last_error=EXCLUDED.last_error, processed_at=EXCLUDED.processed_at, processed_by=EXCLUDED.processed_by, "
                    "cancellation_requested_at=EXCLUDED.cancellation_requested_at, "
                    "cancellation_requested_by=EXCLUDED.cancellation_requested_by "
                    "WHERE import_run.status NOT IN ('CANCELLING','CANCELLED') "
                    "OR EXCLUDED.status='CANCELLED'",
                    (
                        run_id,
                        run.get("adapter") or "MANUAL",
                        json.dumps(
                            {
                                "source": run.get("source") or {},
                                "programmeSlug": run.get("programmeSlug"),
                                "format": run.get("format"),
                                "filename": run.get("filename"),
                                "entityType": run.get("entityType"),
                                "entityTypes": run.get("entityTypes") or [],
                                "queuedAt": run.get("queuedAt"),
                                "featureCount": run.get("featureCount"),
                                "errors": run.get("errors") or [],
                                "manifest": run.get("manifest"),
                                "conflationCandidateCount": run.get(
                                    "conflationCandidateCount", 0
                                ),
                                "binaryObjectPending": bool(
                                    run.get("binaryObjectPending")
                                ),
                                "uploadSpoolPath": run.get("uploadSpoolPath"),
                            },
                            default=json_default,
                        ),
                        run.get("startedAt") or run.get("queuedAt") or now(),
                        run.get("completedAt"),
                        json.dumps(
                            run.get("stats") or {}, default=json_default
                        ),
                        run.get("status") or "QUEUED",
                        int(run.get("attemptCount") or 0),
                        run.get("heartbeatAt"),
                        run.get("leaseUntil"),
                        run.get("lastError"),
                        run.get("processedAt"),
                        run.get("processedBy"),
                        run.get("cancellationRequestedAt"),
                        run.get("cancellationRequestedBy"),
                    ),
                )
            for candidate_id in self._deleted_import_candidate_ids:
                connection.execute(
                    "DELETE FROM geodata_import_candidate WHERE id = %s",
                    (_uuid(candidate_id),),
                )
            for candidate_id in self._dirty_import_candidate_ids:
                candidate = self.data.get("importCandidates", {}).get(
                    candidate_id
                )
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
                    "ON CONFLICT (import_run_id, ordinal) DO UPDATE SET planned_entity_id=EXCLUDED.planned_entity_id, programme_slug=EXCLUDED.programme_slug, entity_type_codes=EXCLUDED.entity_type_codes, name=EXCLUDED.name, geom=EXCLUDED.geom, candidate_source=EXCLUDED.candidate_source, source_ref=EXCLUDED.source_ref, source_hash=EXCLUDED.source_hash, provenance=EXCLUDED.provenance, entity_payload=EXCLUDED.entity_payload, validation_status=EXCLUDED.validation_status, validation_note=EXCLUDED.validation_note, validated_by=EXCLUDED.validated_by, validated_at=EXCLUDED.validated_at, target_status=EXCLUDED.target_status, processed_entity_id=EXCLUDED.processed_entity_id, processed_at=EXCLUDED.processed_at, updated_at=now()",
                    (
                        candidate_id,
                        run_id,
                        int(candidate.get("ordinal", 0)),
                        _uuid(entity.get("id")),
                        entity.get("programmeSlug"),
                        json.dumps(
                            entity.get("entityTypes") or [],
                            default=json_default,
                        ),
                        entity.get("name") or "Unnamed candidate",
                        json.dumps(geometry, default=json_default),
                        json.dumps(
                            candidate.get("candidateSource") or {},
                            default=json_default,
                        ),
                        entity.get("sourceRef"),
                        (entity.get("provenance") or {}).get("sourceHash"),
                        json.dumps(
                            entity.get("provenance") or {},
                            default=json_default,
                        ),
                        json.dumps(entity, default=json_default),
                        candidate.get("validationStatus", "PENDING"),
                        candidate.get("validationNote"),
                        candidate.get("validatedBy"),
                        candidate.get("validatedAt"),
                        candidate.get("targetStatus"),
                        _uuid(candidate.get("processedEntityId")),
                        candidate.get("processedAt"),
                    ),
                )
            for queue_id in self._deleted_import_queue_ids:
                connection.execute(
                    "DELETE FROM geodata_import_processing_queue WHERE id = %s",
                    (_uuid(queue_id),),
                )
            for queue_id in self._dirty_import_queue_ids:
                queue = self.data.get("importProcessingQueues", {}).get(
                    queue_id
                )
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
                    (
                        queue_id,
                        run_id,
                        json.dumps(
                            queue.get("candidateIds") or [],
                            default=json_default,
                        ),
                        queue.get("targetStatus"),
                        queue.get("requestedBy"),
                        queue.get("status", "QUEUED"),
                        json.dumps(
                            queue.get("result") or {}, default=json_default
                        ),
                        queue.get("error"),
                        queue.get("requestedAt"),
                        queue.get("startedAt"),
                        queue.get("completedAt"),
                    ),
                )

    def delete_relational(self, entity_id: str) -> None:
        if not self.durable:
            return
        entity_uuid = _uuid(entity_id)
        if not entity_uuid:
            return
        with self.transaction() as connection:
            connection.execute(
                "DELETE FROM geodata_entity_category WHERE entity_id = %s",
                (entity_uuid,),
            )
            connection.execute(
                "DELETE FROM source_reference WHERE entity_id = %s",
                (entity_uuid,),
            )
            connection.execute(
                "DELETE FROM entity_review WHERE entity_id = %s",
                (entity_uuid,),
            )
            connection.execute(
                "DELETE FROM conflation_candidate WHERE left_entity_id = %s OR right_entity_id = %s",
                (entity_uuid, entity_uuid),
            )
            connection.execute(
                "DELETE FROM geodata_entity WHERE id = %s", (entity_uuid,)
            )

    @staticmethod
    def _observe_postgis_query(query: str, started: float) -> None:
        duration = time.perf_counter() - started
        METRICS.observe(
            "myota_geodata_postgis_query_duration_seconds",
            duration,
            {"query": query},
        )
        try:
            threshold_ms = max(
                0.0,
                float(os.environ.get("MYOTA_SLOW_QUERY_THRESHOLD_MS", "250")),
            )
        except ValueError:
            threshold_ms = 250.0
        if duration * 1000 >= threshold_ms:
            METRICS.inc("myota_geodata_slow_queries_total", {"query": query})

    def query_bbox(
        self,
        bounds: tuple[float, float, float, float],
        limit: int,
        programme: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        """Run the map query through the PostGIS GiST index and time the SQL."""
        if not self.durable:
            raise RuntimeError(
                "PostGIS bounding-box query requires durable storage"
            )
        started = time.perf_counter()
        try:
            with self.transaction() as connection:
                rows = connection.execute(
                    "SELECT id::text, programme_slug, entity_type_code, name, lifecycle_status, "
                    "ST_AsGeoJSON(geom)::jsonb, public_properties "
                    "FROM geodata_entity "
                    "WHERE geom && ST_MakeEnvelope(%s, %s, %s, %s, 4326) "
                    "AND ST_Intersects(geom, ST_MakeEnvelope(%s, %s, %s, %s, 4326)) "
                    "AND (%s IS NULL OR programme_slug = %s) "
                    "AND (%s IS NULL OR lifecycle_status = %s) "
                    "ORDER BY name, id LIMIT %s",
                    (
                        *bounds,
                        *bounds,
                        programme,
                        programme,
                        status,
                        status,
                        limit,
                    ),
                ).fetchall()
            return [
                {
                    "id": row[0],
                    "programmeSlug": row[1],
                    "entityType": row[2],
                    "name": row[3],
                    "status": row[4],
                    "geometry": row[5],
                    "publicProperties": row[6]
                    if isinstance(row[6], dict)
                    else json.loads(row[6] or "{}"),
                }
                for row in rows
            ]
        finally:
            self._observe_postgis_query("bbox", started)


class GeodataStore(CompatibilityGeodataStore):
    """Database-authoritative geodata with scoped compatibility projections."""

    def __init__(self, service="geodata", dsn_env=None):
        super().__init__(service, dsn_env)
        self._repository = None

    def wait_for_authority_schema(self):
        """Do not serve or consume work before the row-authority migration."""
        if not self.durable:
            return
        import psycopg

        while True:
            try:
                with self.base_transaction() as connection:
                    if connection.execute(
                        "SELECT 1 FROM geodata_schema_feature "
                        "WHERE name='row_authority_v1'"
                    ).fetchone():
                        return
            except psycopg.Error:
                pass
            logging.getLogger(__name__).warning(
                "Waiting for geodata migration 016 before accepting work"
            )
            time.sleep(3)

    def hydrate(self):
        if not self.durable:
            self._hydrated = True
            return
        if self._repository is not None:
            return
        from relational_state import DatabaseEvents, RowRepository

        self._repository = RowRepository(
            self, CompatibilityGeodataStore._sync_relational
        )
        self.items = self._repository.mapping("entities")
        self.data = {
            kind: self._repository.mapping(kind)
            for kind in (
                "importRuns",
                "importCandidates",
                "importProcessingQueues",
                "schedules",
                "conflationCandidates",
                "sourceManifests",
                "entityDeletionJobs",
            )
        }
        self.events = DatabaseEvents(self._repository)
        self.idempotency = {}
        self._hydrated = True

    @contextmanager
    def base_transaction(self):
        with Store.transaction(self) as connection:
            if connection is not None:
                connection.execute("SET LOCAL myota.geodata_writer = 'row-v1'")
            yield connection

    def transaction(self):
        if self._repository is not None:
            return self._repository.connection()
        return self.base_transaction()

    def operation(self, write=False, atomic=True):
        from contextlib import nullcontext

        if not self.durable:
            return nullcontext()
        self.hydrate()
        return self._repository.operation(write, atomic)

    def persist(self, include_import_state=False):
        if self.durable:
            self.hydrate()
            self._repository.flush()

    def persist_snapshot_only(self):
        if self.durable:
            self.hydrate()
            self._repository.flush(events_only=True)

    def rollback_pending(self):
        if self._repository is not None:
            self._repository.rollback_pending()

    def delete_relational(self, entity_id):
        if self.durable:
            self.hydrate()
            self._repository.scope.deleted.add(("entities", str(entity_id)))

    def delete_encoded_entity(self, entity_id):
        CompatibilityGeodataStore.delete_relational(self, entity_id)

    def once(self, key, callback):
        if not self.durable:
            return super().once(key, callback)
        self.hydrate()
        return self._repository.once(key, callback)

    def refresh_import_runs(self):
        if self.durable:
            self.hydrate()
            self._repository.invalidate("importRuns")

    def refresh_import_run(self, run_id):
        """Discard a cancelled job's local run changes and read its authority."""
        if self.durable:
            self.hydrate()
            return self._repository.reload("importRuns", run_id)
        return self.data.get("importRuns", {}).get(run_id)

    def discard_import_candidates(self, run_id):
        """Remove a cancelled run's staging rows and pending projections."""
        self.hydrate()
        self._repository.discard_import_candidates(run_id)

    def refresh_import_candidates_for_run(self, run_id):
        if self.durable:
            self.hydrate()
            self._repository.invalidate("importCandidates")

    def refresh_import_queue(self, queue_id):
        if self.durable:
            self.hydrate()
            self._repository.invalidate("importProcessingQueues", queue_id)
            self._repository.invalidate("importCandidates")

    def mark_import_candidate_dirty(self, candidate_id):
        if not self.durable:
            super().mark_import_candidate_dirty(candidate_id)

    def mark_import_candidate_deleted(self, candidate_id):
        if not self.durable:
            super().mark_import_candidate_deleted(candidate_id)

    def mark_import_queue_dirty(self, queue_id):
        if not self.durable:
            super().mark_import_queue_dirty(queue_id)

    def mark_import_queue_deleted(self, queue_id):
        if not self.durable:
            super().mark_import_queue_deleted(queue_id)
