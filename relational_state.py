"""Request/job-scoped row repositories; no durable process-wide snapshots.

Dictionary-shaped domain projections are retained for existing domain code.
Their authority is the relational row, and only changed rows are written.
"""

from __future__ import annotations

import copy
import json
import threading
from collections.abc import Iterator, MutableMapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

from common import json_default


class StateConflict(ValueError):
    """A newer row conflicts with this request's intended changes."""

    status_code = 409


TABLES = {
    "entities": "geodata_entity",
    "importRuns": "import_run",
    "importCandidates": "geodata_import_candidate",
    "importProcessingQueues": "geodata_import_processing_queue",
}
RUN_FIELDS = {
    "adapter": "adapter_code",
    "status": "status",
    "startedAt": "started_at",
    "completedAt": "completed_at",
    "stats": "stats",
    "attemptCount": "attempt_count",
    "heartbeatAt": "heartbeat_at",
    "leaseUntil": "lease_until",
    "lastError": "last_error",
    "processedAt": "processed_at",
    "processedBy": "processed_by",
    "cancellationRequestedAt": "cancellation_requested_at",
    "cancellationRequestedBy": "cancellation_requested_by",
}
CANDIDATE_FIELDS = {
    "importRunId": "import_run_id",
    "ordinal": "ordinal",
    "existingEntityId": "existing_entity_id",
    "entity": "entity_payload",
    "candidateSource": "candidate_source",
    "validationStatus": "validation_status",
    "validationNote": "validation_note",
    "validatedBy": "validated_by",
    "validatedAt": "validated_at",
    "targetStatus": "target_status",
    "processedEntityId": "processed_entity_id",
    "processedAt": "processed_at",
    "processingQueueId": "processing_queue_id",
}
QUEUE_FIELDS = {
    "importRunId": "import_run_id",
    "candidateIds": "candidate_ids",
    "targetStatus": "target_status",
    "requestedBy": "requested_by",
    "status": "status",
    "result": "result",
    "error": "error",
    "requestedAt": "requested_at",
    "startedAt": "started_at",
    "completedAt": "completed_at",
    "attemptCount": "attempt_count",
    "heartbeatAt": "heartbeat_at",
    "leaseUntil": "lease_until",
}


def _projection(fields: dict[str, str]) -> str:
    values = ["'id', id::text"]
    for public, column in fields.items():
        values.append(f"'{public}', {column}")
    return "jsonb_build_object(" + ", ".join(values) + ")"


PROJECTIONS = {
    "entities": "public_properties || "
    + _projection(
        {
            "programmeSlug": "programme_slug",
            "entityType": "entity_type_code",
            "name": "name",
            "status": "lifecycle_status",
            "geometry": "ST_AsGeoJSON(geom)::jsonb",
            "sourceState": "source_state",
            "jurisdiction": "jurisdiction",
            "attachments": "attachments",
            "version": "revision",
        }
    ),
    "importRuns": "source_metadata || " + _projection(RUN_FIELDS),
    "importCandidates": _projection(CANDIDATE_FIELDS),
    "importProcessingQueues": "job_metadata || " + _projection(QUEUE_FIELDS),
}


def canonical(value: Any) -> Any:
    return json.loads(json.dumps(value, default=json_default))


@dataclass
class Scope:
    connection: Any = None
    write: bool = False
    loaded: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    original: dict[tuple[str, str], dict[str, Any] | None] = field(
        default_factory=dict
    )
    deleted: set[tuple[str, str]] = field(default_factory=set)
    events: list[dict[str, Any]] = field(default_factory=list)
    idempotency_managed: bool = False


class RowMap(MutableMapping):
    def __init__(self, repository: RowRepository, kind: str) -> None:
        self.repository, self.kind = repository, kind

    def __getitem__(self, key: str) -> dict[str, Any]:
        return self.repository.load(self.kind, str(key))

    def __setitem__(self, key: str, value: dict[str, Any]) -> None:
        identity = (self.kind, str(key))
        scope = self.repository.scope
        if identity not in scope.original:
            try:
                self[key]
            except KeyError:
                scope.original[identity] = None
        scope.deleted.discard(identity)
        scope.loaded[identity] = value

    def __delitem__(self, key: str) -> None:
        self[key]
        self.repository.scope.deleted.add((self.kind, str(key)))

    def __iter__(self) -> Iterator[str]:
        scope = self.repository.scope
        keys = self.repository.keys(self.kind)
        keys.update(key for kind, key in scope.loaded if kind == self.kind)
        return iter(
            sorted(
                key for key in keys if (self.kind, key) not in scope.deleted
            )
        )

    def __len__(self) -> int:
        return sum(1 for _ in self)


class RowRepository:
    def __init__(self, store: Any, writer: Any) -> None:
        self.store, self.writer = store, writer
        self.local = threading.local()
        self.maps = {kind: RowMap(self, kind) for kind in TABLES}

    @property
    def scope(self) -> Scope:
        if not hasattr(self.local, "scope"):
            # Direct maintenance calls use a thread-local buffer, not shared
            # pod state. HTTP/jobs enter explicit scopes and discard it.
            self.local.scope = Scope()
        return self.local.scope

    def mapping(self, kind: str) -> RowMap:
        if kind not in self.maps:
            self.maps[kind] = RowMap(self, kind)
        return self.maps[kind]

    @contextmanager
    def connection(self) -> Iterator[Any]:
        if self.scope.connection is not None:
            yield self.scope.connection
        else:
            with self.store.base_transaction() as connection:
                scope = self.scope
                scope.connection = connection
                try:
                    yield connection
                finally:
                    scope.connection = None

    @contextmanager
    def operation(self, write: bool = False, atomic: bool = True):
        prior = getattr(self.local, "scope", None)
        if getattr(self.local, "depth", 0):
            yield
            return
        self.local.depth = 1
        scope = Scope(write=write)
        self.local.scope = scope
        try:
            if atomic:
                with self.store.base_transaction() as connection:
                    scope.connection = connection
                    connection.execute("SET LOCAL lock_timeout = '5s'")
                    yield
                    if write:
                        self.flush()
            else:
                # Parsing/object I/O must not hold transactions or row locks;
                # explicit persist() checkpoints commit row changes + events.
                yield
                if write:
                    self.flush()
        finally:
            self.local.depth = 0
            self.local.scope = prior or Scope()

    def load(self, kind: str, key: str) -> dict[str, Any]:
        identity = (kind, key)
        scope = self.scope
        if identity in scope.deleted:
            raise KeyError(key)
        if identity in scope.loaded:
            return scope.loaded[identity]
        with self.connection() as connection:
            value = self.read(
                connection,
                kind,
                key,
                lock=scope.write and scope.connection is not None,
            )
        if value is None:
            raise KeyError(key)
        scope.loaded[identity] = value
        scope.original[identity] = copy.deepcopy(value)
        return value

    def read(
        self, connection: Any, kind: str, key: str, lock: bool = False
    ) -> dict[str, Any] | None:
        suffix = " FOR UPDATE" if lock else ""
        if kind in TABLES:
            row = connection.execute(
                f"SELECT {PROJECTIONS[kind]} FROM {TABLES[kind]} "
                f"WHERE id=%s{suffix}",
                (key,),
            ).fetchone()
        else:
            row = connection.execute(
                "SELECT payload FROM geodata_control_record "
                f"WHERE kind=%s AND id=%s{suffix}",
                (kind, key),
            ).fetchone()
        if not row:
            return None
        value = canonical(row[0])
        if kind == "importCandidates":
            entity = value.get("entity") or {}
            value["dedupeWarning"] = entity.get("dedupeWarning")
            value["possibleDuplicates"] = entity.get("possibleDuplicates", [])
        return value

    def keys(self, kind: str) -> set[str]:
        with self.connection() as connection:
            if kind in TABLES:
                rows = connection.execute(
                    f"SELECT id::text FROM {TABLES[kind]} ORDER BY id"
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT id FROM geodata_control_record WHERE kind=%s "
                    "ORDER BY id",
                    (kind,),
                ).fetchall()
        return {row[0] for row in rows}

    def invalidate(self, kind: str, key: str | None = None) -> None:
        scope = self.scope
        for identity, value in list(scope.loaded.items()):
            if identity[0] == kind and (key is None or identity[1] == key):
                if canonical(value) == scope.original.get(identity):
                    scope.loaded.pop(identity)
                    scope.original.pop(identity, None)

    def reload(self, kind: str, key: str) -> dict[str, Any]:
        """Replace a cancelled job's pending projection with its locked row."""
        with self.connection() as connection:
            latest = self.read(connection, kind, key, lock=True)
        if latest is None:
            raise KeyError(key)
        identity = (kind, key)
        value = self.scope.loaded.setdefault(identity, {})
        value.clear()
        value.update(latest)
        self.scope.original[identity] = copy.deepcopy(latest)
        return value

    def discard_import_candidates(self, run_id: str) -> None:
        """Delete one run's staging rows without loading other imports."""
        with self.connection() as connection:
            connection.execute(
                "DELETE FROM geodata_import_candidate WHERE import_run_id=%s",
                (run_id,),
            )
        for identity, value in list(self.scope.loaded.items()):
            if (
                identity[0] == "importCandidates"
                and value.get("importRunId") == run_id
            ):
                self.scope.loaded.pop(identity)
                self.scope.original.pop(identity, None)
                self.scope.deleted.discard(identity)

    def _write(
        self, connection: Any, kind: str, key: str, value: dict[str, Any]
    ) -> None:
        if kind not in TABLES:
            connection.execute(
                "INSERT INTO geodata_control_record(kind,id,payload) "
                "VALUES (%s,%s,%s::jsonb) ON CONFLICT (kind,id) DO UPDATE "
                "SET payload=EXCLUDED.payload, updated_at=now()",
                (kind, key, json.dumps(value, default=json_default)),
            )
            return

        # Reuse the schema-owned row encoders with exactly ONE changed row.
        # Never pass the live catalogue or import collection to this encoder.
        @contextmanager
        def transaction():
            yield connection

        adapter = SimpleNamespace(
            durable=True,
            transaction=transaction,
            items={key: value} if kind == "entities" else {},
            data={kind: {key: value}},
            _dirty_import_candidate_ids={key}
            if kind == "importCandidates"
            else set(),
            _dirty_import_queue_ids={key}
            if kind == "importProcessingQueues"
            else set(),
            _deleted_import_candidate_ids=set(),
            _deleted_import_queue_ids=set(),
            _upsert_entity=self.store._upsert_entity,
            _observe_postgis_query=self.store._observe_postgis_query,
        )
        self.writer(adapter, include_import_state=True)
        if kind == "importCandidates":
            connection.execute(
                "UPDATE geodata_import_candidate SET existing_entity_id=%s, "
                "processing_queue_id=%s WHERE id=%s",
                (
                    value.get("existingEntityId"),
                    value.get("processingQueueId"),
                    key,
                ),
            )
        if kind == "importRuns":
            metadata = {
                name: item
                for name, item in value.items()
                if name not in RUN_FIELDS and name != "id"
            }
            connection.execute(
                "UPDATE import_run SET source_metadata=%s::jsonb WHERE id=%s",
                (json.dumps(metadata, default=json_default), key),
            )
        if kind == "importProcessingQueues":
            metadata = {
                name: item
                for name, item in value.items()
                if name not in QUEUE_FIELDS and name != "id"
            }
            connection.execute(
                "UPDATE geodata_import_processing_queue "
                "SET job_metadata=%s::jsonb WHERE id=%s",
                (json.dumps(metadata, default=json_default), key),
            )

    def flush(self, events_only: bool = False) -> None:
        scope = self.scope
        changed = (
            []
            if events_only
            else [
                (kind, key, value)
                for (kind, key), value in scope.loaded.items()
                if (kind, key) not in scope.deleted
                and canonical(value) != scope.original.get((kind, key))
            ]
        )
        priority = {
            "importRuns": 0,
            "entities": 1,
            "importCandidates": 2,
            "importProcessingQueues": 3,
        }
        changed.sort(key=lambda item: (priority.get(item[0], 4), item[1]))
        if (
            not changed
            and not scope.events
            and (events_only or not scope.deleted)
        ):
            return
        committed: list[tuple[str, str, dict[str, Any]]] = []
        with self.connection() as connection:
            for kind, key, value in changed:
                baseline = scope.original.get((kind, key))
                # Lock absent rows too. Two replicas creating the same key
                # must recheck after the winner commits.
                connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                    (f"geodata:{kind}:{key}",),
                )
                latest = self.read(connection, kind, key, lock=True)
                normalized = canonical(value)
                if baseline is None:
                    if latest is not None and latest != normalized:
                        raise StateConflict(f"{kind}/{key} already exists")
                elif latest is None:
                    raise StateConflict(f"{kind}/{key} was deleted; reload")
                elif kind == "entities":
                    if latest.get("version") != baseline.get("version"):
                        raise StateConflict(f"entity {key} changed; reload")
                else:
                    # Worker heartbeat/leases may change independent fields.
                    # Merge only the intended delta; never persist a stale row.
                    for field_name in set(baseline) | set(normalized):
                        before = baseline.get(field_name)
                        after = normalized.get(field_name)
                        if before == after:
                            continue
                        if latest.get(field_name) not in (before, after):
                            raise StateConflict(
                                f"{kind}/{key} changed at {field_name}; reload"
                            )
                        if field_name in normalized:
                            latest[field_name] = after
                        else:
                            latest.pop(field_name, None)
                    normalized = latest
                self._write(connection, kind, key, normalized)
                result = self.read(connection, kind, key)
                committed.append((kind, key, result or normalized))
            if not events_only:
                for kind, key in sorted(scope.deleted):
                    if kind == "entities":
                        self.store.delete_encoded_entity(key)
                        connection.execute(
                            "DELETE FROM geodata_audit_event "
                            "WHERE aggregate_type='entity' AND aggregate_id=%s",
                            (key,),
                        )
                    elif kind in TABLES:
                        connection.execute(
                            f"DELETE FROM {TABLES[kind]} WHERE id=%s", (key,)
                        )
                    else:
                        connection.execute(
                            "DELETE FROM geodata_control_record "
                            "WHERE kind=%s AND id=%s",
                            (kind, key),
                        )
            for event in scope.events:
                if ("entities", event["aggregate"]["id"]) not in scope.deleted:
                    connection.execute(
                        "INSERT INTO geodata_audit_event(event_id,aggregate_type,"
                        "aggregate_id,event,occurred_at) VALUES (%s,%s,%s,%s::jsonb,"
                        "%s) ON CONFLICT DO NOTHING",
                        (
                            event["eventId"],
                            event["aggregate"]["type"],
                            event["aggregate"]["id"],
                            json.dumps(event, default=json_default),
                            event["occurredAt"],
                        ),
                    )
                connection.execute(
                    "INSERT INTO outbox_event(event_id,event_type,producer,"
                    "aggregate_type,aggregate_id,payload,occurred_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s) ON CONFLICT DO NOTHING",
                    (
                        event["eventId"],
                        event["eventType"],
                        event["producer"],
                        event["aggregate"]["type"],
                        event["aggregate"]["id"],
                        json.dumps(event["payload"], default=json_default),
                        event["occurredAt"],
                    ),
                )
        for kind, key, result in committed:
            # Preserve references held by domain code across worker checkpoints.
            value = scope.loaded[(kind, key)]
            value.clear()
            value.update(result)
            scope.original[(kind, key)] = copy.deepcopy(result)
        if not events_only:
            for identity in scope.deleted:
                scope.loaded.pop(identity, None)
                scope.original.pop(identity, None)
            scope.deleted.clear()
        scope.events.clear()

    def rollback_pending(self) -> None:
        scope = self.scope
        for identity, value in list(scope.loaded.items()):
            baseline = scope.original.get(identity)
            if baseline is None:
                scope.loaded.pop(identity)
                scope.original.pop(identity, None)
            else:
                value.clear()
                value.update(copy.deepcopy(baseline))
        scope.deleted.clear()
        scope.events.clear()

    def once(self, key: str | None, callback: Any) -> Any:
        if self.scope.idempotency_managed:
            return callback()
        if not key:
            return callback()
        with self.connection() as connection:
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"geodata:idempotency:{key}",),
            )
            row = connection.execute(
                "SELECT response FROM idempotency_record "
                "WHERE service=%s AND key=%s",
                (self.store.service, key),
            ).fetchone()
            if row:
                return canonical(row[0])
            result = callback()
            self.flush()
            connection.execute(
                "INSERT INTO idempotency_record(service,key,response) "
                "VALUES (%s,%s,%s::jsonb)",
                (
                    self.store.service,
                    key,
                    json.dumps(result, default=json_default),
                ),
            )
            return result

    def request_once(self, key: str, fingerprint: str, callback: Any) -> Any:
        with self.connection() as connection:
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"geodata:request:{key}",),
            )
            row = connection.execute(
                "SELECT response FROM idempotency_record "
                "WHERE service=%s AND key=%s",
                (self.store.service, key),
            ).fetchone()
            if row:
                response = canonical(row[0])
                if response.get("requestHash") != fingerprint:
                    raise StateConflict(
                        "Idempotency-Key was already used for another request"
                    )
                return response["result"]
            self.scope.idempotency_managed = True
            try:
                result = callback()
                self.flush()
            finally:
                self.scope.idempotency_managed = False
            connection.execute(
                "INSERT INTO idempotency_record(service,key,response) "
                "VALUES (%s,%s,%s::jsonb)",
                (
                    self.store.service,
                    key,
                    json.dumps(
                        {"requestHash": fingerprint, "result": result},
                        default=json_default,
                    ),
                ),
            )
            return result


class DatabaseEvents:
    def __init__(self, repository: RowRepository) -> None:
        self.repository = repository

    def append(self, event: dict[str, Any]) -> None:
        self.repository.scope.events.append(copy.deepcopy(event))

    def __iter__(self) -> Iterator[dict[str, Any]]:
        with self.repository.connection() as connection:
            rows = connection.execute(
                "SELECT event FROM geodata_audit_event ORDER BY occurred_at"
            ).fetchall()
        events = {str(row[0]["eventId"]): canonical(row[0]) for row in rows}
        events.update(
            {event["eventId"]: event for event in self.repository.scope.events}
        )
        return iter(events.values())

    def __setitem__(self, key: slice, value: list[dict[str, Any]]) -> None:
        if key != slice(None):
            raise TypeError("only complete audit filtering is supported")
        retained = {event["eventId"] for event in value}
        removed = [
            event["eventId"]
            for event in self
            if event["eventId"] not in retained
        ]
        if removed:
            with self.repository.connection() as connection:
                connection.execute(
                    "DELETE FROM geodata_audit_event WHERE event_id=ANY(%s::uuid[])",
                    (removed,),
                )
        self.repository.scope.events[:] = [
            event
            for event in self.repository.scope.events
            if event["eventId"] in retained
        ]
