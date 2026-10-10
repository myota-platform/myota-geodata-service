Warning: truncated output (original token count: 54325)
Total output lines: 5516

from __future__ import annotations

from http.server import ThreadingHTTPServer
from concurrent.futures import ThreadPoolExecutor
import copy
import hashlib
from itertools import islice
import json
import math
import os
import re
import threading
import tempfile
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs, urlparse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from common import (
    JsonHandler,
    new_id,
    now,
    page_result,
    require,
    verify_token,
    sign_token,
)
from geodata_pipeline import (
    MAX_IMPORT_FEATURES,
    conflation_score,
    digest,
    geometry_bbox,
    geometry_centroid,
    normalize_geometry,
    source_manifest,
    validate_attachments,
)
from geodata_store import GeodataStore
from geodata_operations import install_operations
from import_adapters import normalize
from import_formats import (
    SUPPORTED_FORMATS,
    TEXT_FORMATS,
    parse_text,
    parse_uploaded,
    iter_uploaded_file,
)
from location_catalog import build_location_tree, derive_location_codes
from maidenhead import apply_maidenhead_fields
from reverse_geocoder import (
    LOCATION_FIELDS,
    apply_location_result,
    lookup_entity_location,
)

GEODATA_IMPORT_BUCKET = os.environ.get(
    "MYOTA_GEODATA_IMPORT_BUCKET", "myota-geodata-imports"
)
MAX_IMPORT_BATCH_BYTES = int(
    os.environ.get("MYOTA_IMPORT_BATCH_BYTES", str(32 * 1024 * 1024))
)
MAX_IMPORT_BATCH_FEATURES = max(
    1, int(os.environ.get("MYOTA_IMPORT_BATCH_SIZE", "100"))
)


def _bounded_import_batches(
    features: Any,
    *,
    max_features: int = MAX_IMPORT_BATCH_FEATURES,
    max_bytes: int = MAX_IMPORT_BATCH_BYTES,
) -> Any:
    """Yield feature batches bounded by both count and serialized bytes.

    A single feature may exceed the batch byte budget, but decoders enforce a
    separate per-feature ceiling. Such a feature is emitted alone so the
    worker never combines several individually-large geometries in memory.
    """
    batch = []
    batch_bytes = 0
    for feature in features:
        feature_bytes = len(
            json.dumps(
                feature, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
        )
        if batch and (
            len(batch) >= max_features
            or batch_bytes + feature_bytes > max_bytes
        ):
            yield batch
            batch = []
            batch_bytes = 0
        batch.append(feature)
        batch_bytes += feature_bytes
        if len(batch) >= max_features or batch_bytes >= max_bytes:
            yield batch
            batch = []
            batch_bytes = 0
    if batch:
        yield batch


def _iter_import_content(
    format_code: str, content: bytes, filename: str
) -> Any:
    """Stream pasted bytes through the same bounded decoder as file uploads."""
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix="myota-import-", delete=False
        ) as temporary:
            temporary.write(content)
            temporary_path = Path(temporary.name)
        yield from iter_uploaded_file(format_code, temporary_path, filename)
    finally:
        if temporary_path:
            temporary_path.unlink(missing_ok=True)


LOCATION_REQUIRED_GROUPS = (
    ("continent",),
    ("continentCode",),
    ("country",),
    ("countryCode",),
    ("region", "subdivision"),
    ("regionCode", "subdivisionCode"),
    ("city", "municipality"),
)


class ImportCancelled(Exception):
    """Raised inside preprocessing when an administrator cancels a run."""


def _import_status(run_id: str) -> str | None:
    if GeoHandler.store.durable:
        with GeoHandler.store.transaction() as connection:
            row = connection.execute(
                "SELECT status FROM import_run WHERE id=%s", (run_id,)
            ).fetchone()
        return str(row[0]).upper() if row else None
    run = GeoHandler.store.data.setdefault("importRuns", {}).get(run_id)
    return str(run.get("status") or "").upper() if run else None


def entity_type_codes(value: Any, fallback: Any = None) -> list[str]:
    """Return stable shared category codes, keeping the first as primary."""
    raw = value if value is not None else fallback
    if isinstance(raw, dict):
        raw = raw.get("code") or raw.get("entityType")
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list):
        return []
    result = []
    for item in raw:
        code = (
            str(item.get("code") if isinstance(item, dict) else item)
            .strip()
            .upper()
        )
        if (
            code
            and re.fullmatch(r"[A-Z][A-Z0-9_]{1,63}", code)
            and code not in result
        ):
            result.append(code)
    return result


def entity_categories(entity: dict[str, Any]) -> list[str]:
    return entity_type_codes(
        entity.get("entityTypes") or entity.get("entityTypeCodes"),
        entity.get("entityType"),
    )


class GeoHandler(JsonHandler):
    service = "geodata-service"
    store = GeodataStore("geodata", "GEO_DATABASE_URL")
    upload_executor = ThreadPoolExecutor(
        max_workers=max(1, int(os.environ.get("MYOTA_UPLOAD_WORKERS", "2"))),
        thread_name_prefix="geodata-upload",
    )
    import_executor = ThreadPoolExecutor(
        max_workers=max(1, int(os.environ.get("MYOTA_IMPORT_WORKERS", "2"))),
        thread_name_prefix="geodata-import",
    )
    location_executor = ThreadPoolExecutor(
        max_workers=max(1, int(os.environ.get("MYOTA_LOCATION_WORKERS", "2"))),
        thread_name_prefix="geodata-location",
    )
    deletion_executor = ThreadPoolExecutor(
        max_workers=max(1, int(os.environ.get("MYOTA_DELETION_WORKERS", "2"))),
        thread_name_prefix="geodata-deletion",
    )

    @classmethod
    def metrics_extra(cls) -> dict[str, float]:
        """Return counts from the durable catalogue and import lifecycle."""
        if cls.store.durable:
            from relational_metrics import durable_metrics

            try:
                return durable_metrics(cls.store)
            except Exception:
                return {"myota_geodata_metrics_database_unavailable": 1}
        try:
            cls.store.refresh_import_runs()
        except Exception:
            # The scrape must stay available while PostGIS is restarting.
            return {"myota_geodata_metrics_database_unavailable": 1}
        with cls.store.lock:
            entities = list(cls.store.items.values())
            runs = list((cls.store.data.get("importRuns") or {}).values())
            candidates = list(
                (cls.store.data.get("importCandidates") or {}).values()
            )
        result: dict[str, float] = {
            "myota_geodata_entities_total": float(len(entities)),
            "myota_geodata_entity_categories_total": float(
                len(
                    {
                        code
                        for entity in entities
                        for code in entity_categories(entity)
                    }
                )
            ),
            "myota_geodata_import_runs_total": float(len(runs)),
            "myota_geodata_import_candidates_total": float(len(candidates)),
        }
        for status in ("CANDIDATE", "APPROVED", "RETIRED", "REJECTED"):
            result[
                f'myota_geodata_entities_by_status_total{{status="{status}"}}'
            ] = float(
                sum(
                    str(entity.get("status", "")).upper() == status
                    for entity in entities
                )
            )
        geometry_types = (
            "Point",
            "LineString",
            "MultiLineString",
            "Polygon",
            "MultiPolygon",
        )
        for geometry_type in geometry_types:
            result[
                f'myota_geodata_entities_by_geometry_total{{geometry_type="{geometry_type}"}}'
            ] = float(
                sum(
                    (entity.get("geometry") or {}).get("type") == geometry_type
                    for entity in entities
                )
            )
        for status in (
            "QUEUED",
            "PROCESSING",
            "PREPROCESSED",
            "PREPROCESSED_WITH_ERRORS",
            "PROCESSED",
            "FAILED",
        ):
            result[
                f'myota_geodata_import_runs_by_status_total{{status="{status}"}}'
            ] = float(
                sum(
                    str(run.get("status", "")).upper() == status
                    for run in runs
                )
            )
        for validation in ("PENDING", "CONFIRMED", "REJECTED", "PROCESSED"):
            result[
                f'myota_geodata_import_candidates_by_validation_total{{status="{validation}"}}'
            ] = float(
                sum(
                    str(candidate.get("validationStatus", "PENDING")).upper()
                    == validation
                    for candidate in candidates
                )
            )
        queued = [
            run
            for run in runs
            if str(run.get("status", "")).upper()
            in {"QUEUED", "UPLOAD_PENDING"}
        ]
        processing = [
            run
            for run in runs
            if str(run.get("status", "")).upper() == "PROCESSING"
        ]
        result["myota_geodata_import_queue_depth"] = float(len(queued))
        result["myota_geodata_import_processing_runs"] = float(len(processing))
        result["myota_geodata_import_attempt_count_sum"] = float(
            sum(int(run.get("attemptCount") or 0) for run in runs)
        )
        result["myota_geodata_import_features_preprocessed_sum"] = float(
            sum(
                int((run.get("stats") or {}).get("preprocessed", 0) or 0)
                for run in runs
            )
        )
        result["myota_geodata_import_features_promoted_sum"] = float(
            sum(
                int((run.get("stats") or {}).get("created", 0) or 0)
                + int((run.get("stats") or {}).get("updated", 0) or 0)
                for run in runs
            )
        )

        def seconds_since(value: Any) -> float | None:
            if not value:
                return None
            try:
                timestamp = (
                    value
                    if isinstance(value, datetime)
                    else datetime.fromisoformat(
                        str(value).replace("Z", "+00:00")
                    )
                )
                timestamp = (
                    timestamp.replace(tzinfo=timezone.utc)
                    if timestamp.tzinfo is None
                    else timestamp
                )
                return max(
                    0.0,
                    (datetime.now(timezone.utc) - timestamp).total_seconds(),
                )
            except (TypeError, ValueError):
                return None

        queue_ages = [
            age
            for run in queued
            if (
                age := seconds_since(
                    run.get("queuedAt") or run.get("startedAt")
                )
            )
            is not None
        ]
        heartbeat_ages = [
            age
            for run in processing
            if (
                age := seconds_since(
                    run.get("heartbeatAt") or run.get("startedAt")
                )
            )
            is not None
        ]
        result["myota_geodata_import_oldest_queued_age_seconds"] = max(
            queue_ages, default=0.0
        )
        result[
            "myota_geodata_import_oldest_processing_heartbeat_age_seconds"
        ] = max(heartbeat_ages, default=0.0)

        try:
            pool_stats = cls.store._ensure_pool().get_stats()
            result.update(
                {
                    "myota_geodata_postgres_pool_connections": float(
                        pool_stats.get("pool_size", 0)
                    ),
                    "myota_geodata_postgres_pool_available_connections": float(
                        pool_stats.get("pool_available", 0)
                    ),
                    "myota_geodata_postgres_pool_waiting_requests": float(
                        pool_stats.get("requests_waiting", 0)
                    ),
                    "myota_geodata_postgres_pool_wait_milliseconds_total": float(
                        pool_stats.get("requests_wait_ms", 0)
                    ),
                }
            )
            with cls.store.transaction() as connection:
                row = connection.execute(
                    "SELECT count(*) FILTER (WHERE state = 'active' AND pid <> pg_backend_pid()), "
                    "count(*) FILTER (WHERE wait_event_type = 'Lock'), "
                    "(SELECT setting::bigint FROM pg_settings WHERE name = 'max_connections') "
                    "FROM pg_stat_activity WHERE datname = current_database()"
                ).fetchone()
                result["myota_geodata_postgres_active_connections"] = float(
                    row[0] or 0
                )
                result["myota_geodata_postgres_lock_waiting_connections"] = (
                    float(row[1] or 0)
                )
                result["myota_geodata_postgres_max_connections"] = float(
                    row[2] or 0
                )
                outbox = connection.execute(
                    "SELECT count(*), COALESCE(extract(epoch FROM now() - min(occurred_at)), 0) "
                    "FROM outbox_event WHERE published_at IS NULL"
                ).fetchone()
                result["myota_geodata_outbox_pending_events"] = float(
                    outbox[0] or 0
                )
                result["myota_geodata_outbox_oldest_pending_age_seconds"] = (
                    float(outbox[1] or 0)
                )
        except Exception:
            result["myota_geodata_postgres_pool_metrics_available"] = 0.0
        return result

    @staticmethod
    def _authorize_review(p: dict[str, str], entity: dict[str, Any]) -> None:
        if not p.get("_http"):
            return
        authorization = p.get("Authorization", "")
        if not authorization.startswith("Bearer "):
            raise PermissionError("Bearer authentication is required")
        claims = verify_token(authorization[7:])
        scopes = set(claims.get("scp", []))
        if "*" in scopes:
            return
        if "geodata.review" not in scopes:
            raise PermissionError("geodata.review scope is required")
        roles = claims.get("roles", [])
        scoped = [
            r
            for r in roles
            if "geodata.review" in r.get("scopes", [])
            or r.get("role") == "GLOBAL_OPERATOR"
        ]
        for role in scoped:
            if role.get("programmeSlug") and role[
                "programmeSlug"
            ] != entity.get("programmeSlug"):
                continue
            if role.get("entityType") and str(
                role["entityType"]
            ).upper() not in entity_categories(entity):
                continue
            if role.get("jurisdiction") and role["jurisdiction"] != entity.get(
                "jurisdiction"
            ):
                continue
            return
        if not any(r.get("role") == "GLOBAL_OPERATOR" for r in roles):
            raise PermissionError("approver scope does not cover this entity")

    @staticmethod
    def _authorize_import(p: dict[str, str]) -> None:
        if not p.get("_http"):
            return
        authorization = p.get("Authorization", "")
        if not authorization.startswith("Bearer "):
            raise PermissionError("Bearer authentication is required")
        claims = verify_token(authorization[7:])
        scopes = set(claims.get("scp", []))
        if "*" in scopes or "geodata.import" in scopes:
            return
        raise PermissionError("geodata.import scope is required")

    @staticmethod
    def _import_owner(p: dict[str, str]) -> str:
        authorization = p.get("Authorization", "")
        claims = verify_token(authorization[7:])
        owner = claims.get("sub") or claims.get("email")
        if not owner:
            raise PermissionError("upload token has no stable subject")
        return str(owner)

    @staticmethod
    def _upload_session(upload_id: str, owner: str) -> dict[str, Any]:
        with GeoHandler.store.transaction() as connection:
            row = connection.execute(
                "SELECT id::text, owner_subject, filename, metadata, bucket, object_key, "
                "multipart_upload_id, expected_size, expected_sha256, status, "
                "import_run_id::text, expires_at FROM geodata_upload_session "
                "WHERE id=%s AND owner_subject=%s",
                (upload_id, owner),
            ).fetchone()
        if not row:
            raise KeyError("upload session not found")
        session = {
            "id": row[0],
            "ownerSubject": row[1],
            "filename": row[2],
            "metadata": row[3]
            if isinstance(row[3], dict)
            else json.loads(row[3]),
            "bucket": row[4],
            "objectKey": row[5],
            "multipartUploadId": row[6],
            "expectedSize": int(row[7]),
            "expectedSha256": row[8],
            "status": row[9],
            "importRunId": row[10],
            "expiresAt": row[11],
        }
        if session["status"] not in {"UPLOADING", "COMPLETING"}:
            raise ValueError(f"upload session is {session['status'].lower()}")
        if (
            session["expiresAt"].timestamp()
            <= datetime.now(timezone.utc).timestamp()
        ):
            raise ValueError("upload session has expired")
        return session

    @staticmethod
    def get_import_upload(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        GeoHandler._authorize_import(p)
        owner = GeoHandler._import_owner(p)
        with GeoHandler.store.transaction() as connection:
            row = connection.execute(
                "SELECT status, filename, expected_size, import_run_id::text, expires_at "
                "FROM geodata_upload_session WHERE id=%s AND owner_subject=%s",
                (p["uploadId"], owner),
            ).fetchone()
        if not row:
            raise KeyError("upload session not found")
        if row[0] == "COMPLETED":
            return {
                "uploadId": p["uploadId"],
                "status": "COMPLETED",
                "filename": row[1],
                "expectedSize": int(row[2]),
                "importRunId": row[3],
                "parts": [],
            }
        session = GeoHandler._upload_session(p["uploadId"], owner)
        with GeoHandler.store.transaction() as connection:
            rows = connection.execute(
                "SELECT part_number, size_bytes, sha256 FROM geodata_upload_part "
                "WHERE upload_session_id=%s ORDER BY part_number",
                (p["uploadId"],),
            ).fetchall()
        return {
            "uploadId": p["uploadId"],
            "status": session["status"],
            "filename": session["filename"],
            "expectedSize": session["expectedSize"],
            "parts": [
                {
                    "partNumber": row[0],
                    "sizeBytes": int(row[1]),
                    "sha256": row[2],
                }
                for row in rows
            ],
        }

    @staticmethod
    def create_import_upload(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        """Create a resumable S3 multipart session without a pod-local spool."""
        GeoHandler._authorize_import(p)
        if not GeoHandler.store.durable:
            raise ValueError(
                "resumable uploads require durable PostgreSQL storage"
            )
        body = p["_body"]
        require(
            body,
            "adapter",
            "source",
            "filename",
            "expectedSize",
            "entityTypes",
        )
        filename = Path(str(body["filename"])).name
        owner = GeoHandler._import_owner(p)
        idempotency_key = p.get("Idempotency-Key") or new_id()
        if not filename:
            raise ValueError("filename must not be empty")
        expected_size = int(body["expectedSize"])
        max_bytes = int(os.environ.get("MYOTA_UPLOAD_MAX_BYTES", str(1024**3)))
        if expected_size < 1 or expected_size > max_bytes:
            raise ValueError(
                f"upload size must be between 1 and {max_bytes} bytes"
            )
        with GeoHandler.store.transaction() as connection:
            previous = connection.execute(
                "SELECT id::text, filename, expected_size, status "
                "FROM geodata_upload_session WHERE owner_subject=%s AND idempotency_key=%s",
                (owner, idempotency_key),
            ).fetchone()
        if previous:
            if previous[1] != filename or int(previous[2]) != expected_size:
                raise ValueError(
                    "Idempotency-Key was already used for different upload metadata"
                )
            return {
                "uploadId": previous[0],
                "status": previous[3],
                "partSizeBytes": max(
                    5 * 1024 * 1024,
                    int(
                        os.environ.get(
                            "MYOTA_UPLOAD_PART_MAX_BYTES",
                            str(16 * 1024 * 1024),
                        )
                    ),
                ),
                "expiresInSeconds": 86400,
                "_status": 201,
            }
        expected_digest = str(body.get("sha256") or "").lower() or None
        if expected_digest and not re.fullmatch(
            r"[a-f0-9]{64}", expected_digest
        ):
            raise ValueError(
                "sha256 must be a 64-character hexadecimal digest"
            )
        categories = entity_type_codes(
            body.get("entityTypes"), body.get("entityType")
        )
        if not categories:
            raise ValueError("entityTypes must contain at least one category")
        format_code = str(
            body.get("format") or filename.rsplit(".", 1)[-1]
        ).upper()
        if format_code == "JSON":
            format_code = "GEOJSON"
        if format_code == "SHP":
            format_code = "SHAPEFILE"
        metadata = {
            key: value
            for key, value in body.items()
            if key not in {"expectedSize", "sha256"}
        }
        metadata.update(
            {
                "filename": filename,
                "format": format_code,
                "entityType": categories[0],
                "entityTypes": categories,
            }
        )
        upload_id = new_id()
        bucket = GEODATA_IMPORT_BUCKET
        object_key = f"geodata-imports/{upload_id}-{filename}"
        from storage import ObjectStore

        multipart_id = ObjectStore().create_multipart(
            bucket, object_key, "application/octet-stream"
        )
        try:
            with GeoHandler.store.transaction() as connection:
                connection.execute(
                    "INSERT INTO geodata_upload_session "
                    "(id, owner_subject, idempotency_key, filename, metadata, bucket, object_key, multipart_upload_id, "
                    "expected_size, expected_sha256, status, expires_at) "
                    "VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, 'UPLOADING', now() + interval '24 hours')",
                    (
                        upload_id,
                        owner,
                        idempotency_key,
                        filename,
                        json.dumps(metadata),
                        bucket,
                        object_key,
                        multipart_id,
                        expected_size,
                        expected_digest,
                    ),
                )
        except Exception:
            ObjectStore().abort_multipart(bucket, object_key, multipart_id)
            with GeoHandler.store.transaction() as connection:
                raced = connection.execute(
                    "SELECT id::text, filename, expected_size, status "
                    "FROM geodata_upload_session WHERE owner_subject=%s AND idempotency_key=%s",
                    (owner, idempotency_key),
                ).fetchone()
            if raced:
                if raced[1] != filename or int(raced[2]) != expected_size:
                    raise ValueError(
                        "Idempotency-Key was already used for different upload metadata"
                    )
                return {
                    "uploadId": raced[0],
                    "status": raced[3],
                    "partSizeBytes": max(
                        5 * 1024 * 1024,
                        int(
                            os.environ.get(
                                "MYOTA_UPLOAD_PART_MAX_BYTES",
                                str(16 * 1024 * 1024),
                            )
                        ),
                    ),
                    "expiresInSeconds": 86400,
                    "_status": 201,
                }
            raise
        part_bytes = int(
            os.environ.get(
                "MYOTA_UPLOAD_PART_MAX_BYTES", str(16 * 1024 * 1024)
            )
        )
        part_bytes = max(5 * 1024 * 1024, part_bytes)
        return {
            "uploadId": upload_id,
            "status": "UPLOADING",
            "partSizeBytes": part_bytes,
            "expiresInSeconds": 86400,
            "_status": 201,
        }

    @staticmethod
    def upload_import_part(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        GeoHandler._authorize_import(p)
        upload_id = p["uploadId"]
        part_number = int(p["partNumber"])
        if not 1 <= part_number <= 10000:
            raise ValueError("partNumber must be between 1 and 10000")
        owner = GeoHandler._import_owner(p)
        session = GeoHandler._upload_session(upload_id, owner)
        path = p["_body"].get("_uploadPath")
        if not path:
            raise ValueError("binary request body is required")
        from storage import ObjectStore

        try:
            digest = hashlib.sha256()
            size = 0
            with Path(path).open("rb") as source:
                while chunk := source.read(1024 * 1024):
                    digest.update(chunk)
                    size += len(chunk)
            if size > int(
                os.environ.get(
                    "MYOTA_UPLOAD_PART_MAX_BYTES", str(16 * 1024 * 1024)
                )
            ):
                raise ValueError(
                    "upload part exceeds the configured part limit"
                )
            checksum = digest.hexdigest()
            declared = p.get("X-Part-SHA256")
            if declared and declared.lower() != checksum:
                raise ValueError("upload part checksum does not match")
            with GeoHandler.store.transaction() as connection:
                connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, %s))",
                    (upload_id, part_number),
                )
                existing_total = connection.execute(
                    "SELECT COALESCE(sum(size_bytes), 0) FROM geodata_upload_part "
                    "WHERE upload_session_id=%s AND part_number<>%s",
                    (upload_id, part_number),
                ).fetchone()[0]
                if int(existing_total) + size > session["expectedSize"]:
                    raise ValueError(
                        "uploaded parts exceed the declared upload size"
                    )
                etag = ObjectStore().upload_part(
                    session["bucket"],
                    session["objectKey"],
                    session["multipartUploadId"],
                    part_number,
                    path,
                )
                connection.execute(
                    "INSERT INTO geodata_upload_part(upload_session_id, part_number, size_bytes, sha256, etag) "
                    "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (upload_session_id, part_number) "
                    "DO UPDATE SET size_bytes=EXCLUDED.size_bytes, sha256=EXCLUDED.sha256, "
                    "etag=EXCLUDED.etag, uploaded_at=now()",
                    (upload_id, part_number, size, checksum, etag),
                )
            return {
                "uploadId": upload_id,
                "partNumber": part_number,
                "sizeBytes": size,
                "sha256": checksum,
                "etag": etag,
            }
        finally:
            Path(path).unlink(missing_ok=True)

    @staticmethod
    def complete_import_upload(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        GeoHandler._authorize_import(p)
        upload_id = p["uploadId"]
        owner = GeoHandler._import_owner(p)
        with GeoHandler.store.transaction() as connection:
            completed = connection.execute(
                "SELECT status, import_run_id::text FROM geodata_upload_session "
                "WHERE id=%s AND owner_subject=%s",
                (upload_id, owner),
            ).fetchone()
        if completed and completed[0] == "COMPLETED":
            GeoHandler.store.refresh_import_runs()
            record = GeoHandler.store.data.get("importRuns", {}).get(
                completed[1], {"id": completed[1]}
            )
            return {
                "uploadId": upload_id,
                "status": "COMPLETED",
                "importRun": record,
                "_status": 202,
            }
        session = GeoHandler._upload_session(upload_id, owner)
        with GeoHandler.store.transaction() as connection:
            connection.execute(
                "UPDATE geodata_upload_session SET status='COMPLETING', updated_at=now() "
                "WHERE id=%s AND status='UPLOADING'",
                (upload_id,),
            )
        with GeoHandler.store.transaction() as connection:
            rows = connection.execute(
                "SELECT part_number, size_bytes, sha256, etag FROM geodata_upload_part "
                "WHERE upload_session_id=%s ORDER BY part_number",
                (upload_id,),
            ).fetchall()
        parts = [
            {
                "partNumber": row[0],
                "size": int(row[1]),
                "sha256": row[2],
                "etag": row[3],
            }
            for row in rows
        ]
        if not parts or [item["partNumber"] for item in parts] != list(
            range(1, len(parts) + 1)
        ):
            raise ValueError("upload parts must be contiguous and start at 1")
        if sum(item["size"] for item in parts) != session["expectedSize"]:
            raise ValueError("uploaded byte total does not match expectedSize")
        if any(item["size"] < 5 * 1024 * 1024 for item in parts[:-1]):
            raise ValueError(
                "all multipart upload parts except the last must be at least 5 MiB"
            )
        from storage import ObjectStore

        storage = ObjectStore()
        try:
            storage.complete_multipart(
                session["bucket"],
                session["objectKey"],
                session["multipartUploadId"],
                parts,
            )
        except Exception:
            # A retry after object completion is safe: the deterministic key is
            # checked below before any run or event is created.
            client = storage._s3()
            if not client:
                raise
            head = client.head_object(
                Bucket=session["bucket"], Key=session["objectKey"]
            )
            if int(head.get("ContentLength", -1)) != session["expectedSize"]:
                raise
        try:
            scan = storage.scan_object(
                session["bucket"], session["objectKey"], session["filename"]
            )
        except ValueError:
            storage.delete(session["bucket"], session["objectKey"])
            with GeoHandler.store.transaction() as connection:
                connection.execute(
                    "UPDATE geodata_upload_session SET status='FAILED', updated_at=now() WHERE id=%s",
                    (upload_id,),
                )
            raise
        digest, size = str(scan["sha256"]), int(scan["size"])
        if size != session["expectedSize"]:
            raise ValueError(
                "completed object size does not match expectedSize"
            )
        if session["expectedSha256"] and digest != session["expectedSha256"]:
            raise ValueError("completed object checksum does not match sha256")
        body = session["metadata"]
        source = {
            **(body.get("source") or {}),
            "bucket": session["bucket"],
            "objectKey": session["objectKey"],
            "sha256": digest,
            "size": size,
            "scan": scan,
        }
        run_id, record = GeoHandler._create_import_run(
            {**body, "source": source},
            session["filename"],
            run_id=upload_id,
            dispatch=False,
        )
        with GeoHandler.store.lock:
            GeoHandler.store.event(
                "geodata.import.queued.v1",
                "import_run",
                run_id,
                {
                    "importRunId": run_id,
                    "uploadId": upload_id,
                    "filename": session["filename"],
                    "natsSubject": "myota.geodata.import.preprocess.v1",
                },
            )
            GeoHandler.store.persist(include_import_state=True)
        with GeoHandler.store.transaction() as connection:
            connection.execute(
                "UPDATE geodata_upload_session SET status='COMPLETED', import_run_id=%s, "
                "completed_at=now(), updated_at=now() WHERE id=%s AND status IN ('UPLOADING','COMPLETING')",
                (run_id, upload_id),
            )
        return {
            "uploadId": upload_id,
            "status": "COMPLETED",
            "importRun": record,
            "_status": 202,
        }

    @staticmethod
    def abort_import_upload(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        GeoHandler._authorize_import(p)
        owner = GeoHandler._import_owner(p)
        session = GeoHandler._upload_session(p["uploadId"], owner)
        from storage import ObjectStore

        ObjectStore().abort_multipart(
            session["bucket"],
            session["objectKey"],
            session["multipartUploadId"],
        )
        with GeoHandler.store.transaction() as connection:
            connection.execute(
                "UPDATE geodata_upload_session SET status='ABORTED', updated_at=now() WHERE id=%s",
                (p["uploadId"],),
            )
        return {"uploadId": p["uploadId"], "status": "ABORTED"}

    @staticmethod
    def _authorize_gis_admin(
        p: dict[str, str], entity: dict[str, Any], permission: str
    ) -> None:
        if not p.get("_http"):
            return
        authorization = p.get("Authorization", "")
        if not authorization.startswith("Bearer "):
            raise PermissionError("Bearer authentication is required")
        claims = verify_token(authorization[7:])
        scopes = set(claims.get("scp", []))
        if "*" in scopes:
            return
        roles = claims.get("roles", [])
        for role in roles:
            if role.get("role") not in ("GIS_ADMIN", "GLOBAL_OPERATOR"):
                continue
            if role.get("programmeSlug") and role[
                "programmeSlug"
            ] != entity.get("programmeSlug"):
                continue
            if role.get("jurisdiction") and role["jurisdiction"] != entity.get(
                "jurisdiction"
            ):
                continue
            return
        if permission in scopes:
            return
        raise PermissionError("global or GIS administrator access is required")

    @staticmethod
    def _query_bounds(
        query: dict[str, list[str]],
    ) -> tuple[float, float, float, float] | None:
        keys = ("minLon", "minLat", "maxLon", "maxLat")
        if query.get("bbox") and not any(key in query for key in keys):
            values = [value.strip() for value in query["bbox"][0].split(",")]
            if len(values) != 4:
                raise ValueError(
                    "bbox must contain minLon,minLat,maxLon,maxLat"
                )
            try:
                bounds = tuple(float(value) for value in values)
            except ValueError as exc:
                raise ValueError(
                    "bbox must contain numeric coordinates"
                ) from exc
            if bounds[0] >= bounds[2] or bounds[1] >= bounds[3]:
                raise ValueError(
                    "map bounds must have minimum values below maximum values"
                )
            return bounds
        if not any(key in query for key in keys):
            return None
        try:
            bounds = tuple(float(query.get(key, [""])[0]) for key in keys)
        except ValueError as exc:
            raise ValueError(
                "minLon, minLat, maxLon and maxLat must be numbers"
            ) from exc
        if bounds[0] >= bounds[2] or bounds[1] >= bounds[3]:
            raise ValueError(
                "map bounds must have minimum values below maximum values"
            )
        return bounds

    @staticmethod
    def list_entities(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        query = parse_qs(urlparse(p.get("_path", "")).query)
        programme = query.get("programme", [None])[0]
        statuses = {
            value.strip().upper()
            for raw in query.get("status", [])
            for value in raw.split(",")
            if value.strip()
        }
        bounds = GeoHandler._query_bounds(query)
        if GeoHandler.store.durable:
            from relational_queries import catalogue_page

            return catalogue_page(GeoHandler.store, query, bounds)
        items = list(GeoHandler.store.items.values())
        if programme:
            items = [i for i in items if i["programmeSlug"] == programme]
        if statuses and "ALL" not in statuses:
            items = [
                i
                for i in items
                if str(i.get("status", "")).upper() in statuses
            ]
        exact_filters = {
            "entityType": "entityType",
            "continent": "continent",
            "country": "country",
            "region": "region",
            "province": "province",
        }
        for query_field, entity_field in exact_filters.items():
            value = query.get(query_field, [None])[0]
            if value:
                if entity_field == "entityType":
                    items = [
                        i
                        for i in items
                        if value.casefold()
                        in {code.casefold() for code in entity_categories(i)}
                    ]
                else:
                    items = [
                        i
                        for i in items
                        if str(
                            i.get(entity_field)
                            or (i.get("location") or {}).get(entity_field)
                            or ""
                        ).casefold()
                        == value.casefold()
                    ]
        city = query.get("city", [None])[0]
        if city:
            target = city.casefold()
            items = [
                i
                for i in items
                if any(
                    str(
                        i.get(field)
                        or (i.get("location") or {}).get(field)
                        or ""
                    ).casefold()
                    == target
                    for field in ("city", "municipality")
                )
            ]
        if bounds:
            items = [
                i
                for i in items
                if i.get("geometry")
                and not (
                    (
                        lambda box: (
                            box[2] < bounds[0]
                            or box[0] > bounds[2]
                            or box[3] < bounds[1]
                            or box[1] > bounds[3]
                        )
                    )(geometry_bbox(i["geometry"]))
                )
            ]
        items.sort(
            key=lambda item: (
                str(item.get("name") or "").casefold(),
                str(item.get("id") or ""),
            )
        )
        return page_result(items, query)

    @staticmethod
    def adapters(_: JsonHandler, __: dict[str, str]) -> dict[str, Any]:
        return {
            "adapters": [
                {
                    "code": "PARKSERVE_US",
                    "formats": ["PARKSERVE_US", "GEOJSON", "KML", "GPX"],
                    "requires": ["license", "retrievedAt", "sourceRef"],
                },
                {
                    "code": "OSM",
                    "formats": ["OSM_PBF", "GEOJSON", "KML", "GPX"],
                    "requiredTags": [
                        "leisure=park",
                        "leisure=nature_reserve",
                        "boundary=protected_area",
                        "landuse=recreation_ground",
                        "highway=path",
                        "highway=footway",
                        "highway=track",
                        "highway=bridleway",
                        "route=hiking",
                    ],
                    "attribution": "© OpenStreetMap contributors",
                },
                {
                    "code": "GOVERNMENT_GIS",
                    "formats": [
                        "WFS",
                        "GEOJSON",
                        "KML",
                        "GPX",
                        "SHAPEFILE",
                        "SHP",
                        "ARCGIS_FEATURESERVER",
                    ],
                    "requires": ["license", "attribution", "sourceFormat"],
                },
                {
                    "code": "MANUAL",
                    "formats": ["GEOJSON", "KML", "GPX", "SHAPEFILE", "SHP"],
                    "requires": ["feature", "entityTypes"],
                },
            ]
        }

    @staticmethod
    def location_options(_: JsonHandler, __: dict[str, str]) -> dict[str, Any]:
        if GeoHandler.store.durable:
            from relational_queries import location_rows

            return build_location_tree(location_rows(GeoHandler.store))
        return build_location_tree(GeoHandler.store.items.values())

    @staticmethod
    def get_entity(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        return GeoHandler.store.items[p["entityId"]]

    @staticmethod
    def audit_entity(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        entity = GeoHandler.store.items[p["entityId"]]
        if GeoHandler.store.durable:
            from relational_queries import audit_rows

            events = audit_rows(GeoHandler.store, entity["id"])
        else:
            events = [
                event
                for event in GeoHandler.store.events
                if event.get("aggregate", {}).get("id") == entity["id"]
            ]
        return {
            "entityId": entity["id"],
            "status": entity["status"],
            "reviewHistory": entity.get("reviewHistory", []),
            "geometryHistory": entity.get("geometryHistory", []),
            "events": events,
        }

    @staticmethod
    def _source_key(source: dict[str, Any]) -> str:
        return str(
            source.get("sourceKey")
            or source.get("url")
            or source.get("name")
            or "unknown-source"
        )

    @staticmethod
    def _create_conflation_candidates(
        entity: dict[str, Any],
    ) -> list[dict[str, Any]]:
        candidates = GeoHandler.store.data.setdefault(
            "conflationCandidates", {}
        )
        created = []
        for existing in GeoHandler.store.items.values():
            if existing["id"] == entity["id"] or existing.get(
                "programmeSlug"
            ) != entity.get("programmeSlug"):
                continue
            if entity.get("sourceRef") and entity.get(
                "sourceRef"
            ) == existing.get("sourceRef"):
                continue
            comparison = (
                conflation_score(entity, existing)
                if entity.get("geometry") and existing.get("geometry")
                else {"score": 0.0, "signals": {}}
            )
            if comparison["score"] < 0.55:
                continue
            pair = sorted([entity["id"], existing["id"]])
            candidate_id = digest(pair)[:32]
            if candidate_id in candidates:
                continue
            candidate = {
                "id": candidate_id,
                "programmeSlug": entity["programmeSlug"],
                "leftEntityId": pair[0],
                "rightEntityId": pair[1],
                "score": comparison["score"],
                "signals": comparison["signals"],
                "resolution": "OPEN",
                "resolutionHistory": [],
                "createdAt": now(),
                "updatedAt": now(),
            }
            candidates[candidate_id] = candidate
            created.append(candidate)
        return created

    @staticmethod
    def _apply_disappearance(
        programme: str,
        source_key: str,
        adapter: str,
        seen_refs: set[str],
        policy: str,
    ) -> list[str]:
        if policy not in {"UNCHANGED", "STALE", "RETIRED", "REVIEW_REQUIRED"}:
            raise ValueError(
                "disappearancePolicy must be UNCHANGED, STALE, RETIRED, or REVIEW_REQUIRED"
            )
        changed = []
        for entity in GeoHandler.store.items.values():
            provenance = entity.get("provenance") or {}
            if (
                entity.get("programmeSlug") != programme
                or provenance.get("adapter") != adapter
                or provenance.get("sourceKey") != source_key
            ):
                continue
            if not entity.get("sourceRef") or entity["sourceRef"] in seen_refs:
                continue
            if policy == "UNCHANGED":
                continue
            occurred_at = now()
            entity["sourceState"] = (
                "STALE"
                if policy == "STALE"
                else "REVIEW_REQUIRED"
                if policy == "REVIEW_REQUIRED"
                else "RETIRED"
            )
            if policy == "RETIRED" and entity["status"] != "RETIRED":
                previous = entity["status"]
                entity["status"] = "RETIRED"
            else:
                previous = entity["status"]
            entity.setdefault("reviewHistory", []).append(
                {
                    "action": "SOURCE_DISAPPEARED",
                    "policy": policy,
                    "previousStatus": previous,
                    "occurredAt": occurred_at,
                }
            )
            entity["updatedAt"] = occurred_at
            changed.append(entity["id"])
            GeoHandler.store.event(
                "geodata.entity.source-disappeared.v1",
                "entity",
                entity["id"],
                {
                    "entityId": entity["id"],
                    "policy": policy,
                    "previousStatus": previous,
                },
            )
        return changed

    @staticmethod
    def _preserve_manual_location(
        existing: dict[str, Any] | None, entity: dict[str, Any]
    ) -> None:
        if not existing:
            entity["manualLocationFields"] = []
            return
        existing_location = existing.get("location") or {}
        for field in LOCATION_FIELDS:
            if field in existing:
                entity[field] = existing[field]
            elif field in existing_location:
                entity[field] = existing_location[field]
        manual_fields = set(existing.get("manualLocationFields") or []) & set(
            LOCATION_FIELDS
        )
        entity["manualLocationFields"] = sorted(manual_fields)
        for field in (
            "geocodeProvider",
            "geocodeStatus",
            "geocodeLookupSource",
            "geocodeError",
            "geocodedAt",
        ):
            if field in existing:
                entity[field] = existing[field]
        previous_geocoding = (existing.get("provenance") or {}).get(
            "reverseGeocoding"
        )
        if previous_geocoding:
            entity.setdefault("provenance", {})["reverseGeocoding"] = (
                previous_geocoding
            )

    @staticmethod
    def _missing_location_fields(entity: dict[str, Any]) -> list[str]:
        """Return required location fields that neither data nor manual values satisfy."""
        manual_fields = set(entity.get("manualLocationFields") or [])
        missing = []
        for group in LOCATION_REQUIRED_GROUPS:
            if any(entity.get(field) not in (None, "") for field in group):
                continue
            if any(field in manual_fields for field in group):
                continue
            missing.append(group[0])
        return missing

    @staticmethod
    def _queue_location_enrichment(
        entity: dict[str, Any],
        *,
        reason: str,
        requested_by: str | None = None,
        only_missing: bool = False,
    ) -> dict[str, Any]:
        """Queue an enrichment request tied to the entity's current geometry."""
        geometry = entity.get("geometry")
        if not isinstance(geometry, dict):
            entity["locationEnrichmentStatus"] = "FAILED"
            entity["locationEnrichmentError"] = "Entity has no geometry."
            return entity

        geometry_hash = digest(geometry)
        if (
            entity.get("locationEnrichmentStatus") == "QUEUED"
            and entity.get("locationEnrichmentGeometryHash") == geometry_hash
        ):
            return entity

        request_id = new_id()
        requested_at = now()
        entity.update(
            {
                "locationEnrichmentRequestId": request_id,
                "locationEnrichmentGeometryHash": geometry_hash,
                "locationEnrichmentStatus": "QUEUED",
                "locationEnrichmentOnlyMissing": only_missing,
                "locationEnrichmentReason": reason,
                "locationEnrichmentRequestedAt": requested_at,
                "locationEnrichmentRequestedBy": requested_by,
            }
        )
        entity.pop("locationEnrichmentError", None)
        payload = {
            "entityId": entity["id"],
            "requestId": request_id,
            "geometryHash": geometry_hash,
            "onlyMissing": only_missing,
            "reason": reason,
            "requestedBy": requested_by,
            "natsSubject": "myota.geodata.entity.location-enrichment.v1",
        }
        GeoHandler.store.event(
            "geodata.entity.location-enrichment-requested.v1",
            "entity",
            entity["id"],
            payload,
        )
        if not GeoHandler.store.durable:
            GeoHandler.location_executor.submit(
                GeoHandler._process_location_enrichment,
                entity["id"],
                request_id,
                geometry_hash,
                only_missing,
                reason,
            )
        return entity

    @staticmethod
    def _process_location_enrichment(
        entity_id: str,
        request_id: str,
        geometry_hash: str,
        only_missing: bool,
        reason: str,
    ) -> bool:
        """Geocode outside a transaction, then commit only if the request is current."""
        store = GeoHandler.store
        try:
            with store.operation(write=False):
                entity = store.items[entity_id]
                if (
                    entity.get("locationEnrichmentRequestId") != request_id
                    or digest(entity.get("geometry")) != geometry_hash
                ):
                    return False
                if entity.get("locationEnrichmentStatus") in {
                    "COMPLETED",
                    "FAILED",
                }:
                    # The database commit may have succeeded immediately
                    # before the worker lost its acknowledgement. Do not
                    # repeat a paid provider lookup when JetStream redelivers.
                    return True
                snapshot = copy.deepcopy(entity)
        except KeyError:
            return False

        result = lookup_entity_location(snapshot)
        result_status = str(result.get("geocodeStatus") or "FAILED")
        enrichment_status = (
            "COMPLETED" if result_status == "ENRICHED" else "FAILED"
        )
        with store.operation(write=True, atomic=True):
            try:
                entity = store.items[entity_id]
            except KeyError:
                return False
            if (
                entity.get("locationEnrichmentRequestId") != request_id
                or digest(entity.get("geometry")) != geometry_hash
            ):
                return False
            apply_location_result(entity, result, only_missing=only_missing)
            completed_at = now()
            entity.update(
                {
                    "locationEnrichmentStatus": enrichment_status,
                    "locationEnrichmentCompletedAt": completed_at,
                    "locationEnrichmentGeometryHash": geometry_hash,
                    "locationEnrichmentReason": reason,
                    "updatedAt": completed_at,
                }
            )
            if result.get("geocodeError"):
                entity["locationEnrichmentError"] = result["geocodeError"]
            else:
                entity.pop("locationEnrichmentError", None)
            GeoHandler.store.event(
                "geodata.entity.location-enriched.v1",
                "entity",
                entity_id,
                {
                    "entityId": entity_id,
                    "requestId": request_id,
                    "geometryHash": geometry_hash,
                    "status": enrichment_status,
                    "reason": reason,
                },
            )
        return True

    @staticmethod
    def _apply_import_location_metadata(
        entity: dict[str, Any], properties: dict[str, Any]
    ) -> None:
        """Preserve location fields supplied by the imported source feature."""
        nested = properties.get("location")
        location = nested if isinstance(nested, dict) else {}
        found = False
        for field in LOCATION_FIELDS:
            value = properties.get(field)
            if value in (None, ""):
                value = location.get(field)
            if value not in (None, ""):
                entity[field] = value
                found = True
        if found and entity.get("countryCode"):
            entity["geocodeProvider"] = "IMPORT_SOURCE"
            entity["geocodeStatus"] = "SOURCE_DATA"
            entity["geocodeLookupSource"] = (
                properties.get("geocodeLookupSource") or "import-properties"
            )
        if found:
            entity["location"] = {
                field: entity.get(field) for field in LOCATION_FIELDS
            }

    @staticmethod
    def _possible_duplicates(entity: dict[str, Any]) -> list[dict[str, Any]]:
        """Find existing entities whose geometry is identical or under 50 m away."""
        geometry = entity.get("geometry")
        if not geometry:
            return []
        matches = []
        candidate_digest = digest(geometry)
        nearby = GeoHandler.store.nearby_entity_ids(
            geometry, entity.get("id"), 50
        )
        for existing_id, distance in nearby:
            if distance >= 50:
                continue
            try:
                existing = GeoHandler.store.items[existing_id]
            except KeyError:
                continue
            existing_geometry = existing["geometry"]
            matches.append(
                {
                    "entityId": existing.get("id"),
                    "name": existing.get("name") or "Unnamed entity",
                    "status": existing.get("status"),
                    "entityTypes": entity_categories(existing),
                    "geometry": existing_geometry,
                    "centroid": existing.get("centroid")
                    or geometry_centroid(existing_geometry),
                    "distanceMeters": round(distance, 2),
                    "matchType": "IDENTICAL_GEOMETRY"
                    if digest(existing_geometry) == candidate_digest
                    else "WITHIN_50_METERS",
                }
            )
        return sorted(
            matches, key=lambda match: (match["distanceMeters"], match["name"])
        )

    @staticmethod
    def _import_features(
        body: dict[str, Any],
        run_id: str,
        cancel_event: threading.Event | None = None,
        ordinal_offset: int = 0,
        final_batch: bool = True,
    ) -> dict[str, Any]:
        if len(body["features"]) > MAX_IMPORT_FEATURES:
            raise ValueError(
                f"an import may contain at most {MAX_IMPORT_FEATURES} features"
            )
        adapter = body["adapter"]
        programme_slug = body.get("programmeSlug") or None
        source = dict(body["source"])
        if adapter == "OSM":
            source.setdefault("attribution", "© OpenStreetMap contributors")
        source_key = GeoHandler._source_key(source)
        records = []
        preprocessed, skipped, errors = [], [], []
        previous_candidates = GeoHandler.store.import_candidates_for_ordinals(
            run_id,
            ordinal_offset,
            ordinal_offset + len(body["features"]),
        )
        for local_index, raw_feature in enumerate(body["features"]):
            index = ordinal_offset + local_index
            if cancel_event and cancel_event.is_set():
                raise ImportCancelled("preprocessing was cancelled")
            try:
                feature = normalize(adapter, raw_feature)
                props = feature.get("properties", {})
                source_ref = str(
                    props.get("sourceRef")
                    or props.get("id")
                    or f"{source_key}:record-{index}"
                )
                if props.get("skipReason") == "FILTERED_TAG":
                    skipped.append(
                        {
                            "sourceRef": source_ref,
                            "reason": props["skipReason"],
                        }
                    )
                    continue
                if not feature.get("geometry"):
                    skipped.append(
                        {"sourceRef": source_ref, "reason": "MISSING_GEOMETRY"}
                    )
                    continue
                source_crs = (
                    raw_feature.get("crs")
                    or raw_feature.get("spatialReference")
                    or props.get("crs")
                )
                # Adapters normalize and reproject source geometry at the intake
                # boundary. Validate the normalized WGS84 value here without
                # applying the source tr…24325 tokens truncated…on.get("MYOTA_IMPORT_HEARTBEAT_SECONDS", "30"))
        )
        lease_seconds = max(
            60, int(os.environ.get("MYOTA_IMPORT_LEASE_SECONDS", "900"))
        )
        while not stop.wait(interval):
            try:
                with GeoHandler.store.transaction() as connection:
                    connection.execute(
                        "UPDATE geodata_import_processing_queue SET heartbeat_at=now(), "
                        "lease_until=now()+make_interval(secs => %s) "
                        "WHERE id=%s AND status='PROCESSING'",
                        (lease_seconds, queue_id),
                    )
            except Exception:
                continue

    @staticmethod
    def process_import_candidates(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        GeoHandler._authorize_import(p)
        body = p["_body"]
        require(body, "candidateIds", "targetStatus", "processorId")
        GeoHandler.store.refresh_import_candidates_for_run(p["runId"])
        candidate_ids = body["candidateIds"]
        target_status = str(body["targetStatus"]).upper()
        if not isinstance(candidate_ids, list) or not candidate_ids:
            raise ValueError("candidateIds must be a non-empty list")
        if target_status not in {"CANDIDATE", "APPROVED"}:
            raise ValueError("targetStatus must be CANDIDATE or APPROVED")
        candidates = GeoHandler.store.data.setdefault("importCandidates", {})
        queue_id = new_id()
        for candidate_id in sorted(set(candidate_ids)):
            candidate = candidates.get(str(candidate_id))
            if not candidate or candidate.get("importRunId") != p["runId"]:
                raise ValueError(
                    f"candidate {candidate_id} does not belong to import run"
                )
            if target_status == "APPROVED":
                GeoHandler._authorize_review(p, candidate.get("entity") or {})
            if candidate.get(
                "validationStatus"
            ) == "PROCESSED" or candidate.get("processingQueueId"):
                raise ValueError("record is already promoted or queued")
            candidate.update(
                {
                    "validationStatus": "CONFIRMED",
                    "validatedBy": body["processorId"],
                    "validatedAt": now(),
                    "targetStatus": target_status,
                    "processingQueueId": queue_id,
                }
            )
            GeoHandler.store.mark_import_candidate_dirty(str(candidate_id))
        queue = {
            "id": queue_id,
            "importRunId": p["runId"],
            "candidateIds": [str(value) for value in candidate_ids],
            "targetStatus": target_status,
            "requestedBy": body["processorId"],
            "note": body.get("note"),
            "status": "QUEUED",
            "requestedAt": now(),
            "startedAt": None,
            "completedAt": None,
            "result": {},
        }
        GeoHandler.store.data.setdefault("importProcessingQueues", {})[
            queue_id
        ] = queue
        GeoHandler.store.mark_import_queue_dirty(queue_id)
        GeoHandler.store.event(
            "geodata.import.processing.queued.v1",
            "import_processing_queue",
            queue_id,
            {
                "queueId": queue_id,
                "importRunId": p["runId"],
                "candidateIds": queue["candidateIds"],
                "targetStatus": target_status,
                "requestedBy": body["processorId"],
                "natsSubject": "myota.geodata.import.process.v1",
            },
        )
        if p.get("_http"):
            GeoHandler.store.persist(include_import_state=True)
            if not GeoHandler.store.durable:
                GeoHandler.import_executor.submit(
                    GeoHandler._process_import_queue, queue_id
                )
        else:
            if GeoHandler.store.durable:
                GeoHandler.store.persist(include_import_state=True)
            GeoHandler._process_import_queue(queue_id)
        return {**queue, "queued": True, "_status": 202}

    @staticmethod
    def create_schedule(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        body = p["_body"]
        require(body, "adapter", "source", "intervalSeconds")
        categories = entity_type_codes(
            body.get("entityTypes") or body.get("entityTypeCodes"),
            body.get("entityType"),
        )
        if not categories:
            raise ValueError(
                "entityTypes must contain at least one shared entity category code"
            )
        interval = int(body["intervalSeconds"])
        if interval < 300:
            raise ValueError("refresh interval must be at least 300 seconds")
        schedule = {
            "id": new_id(),
            "programmeSlug": body.get("programmeSlug"),
            "entityType": categories[0],
            "entityTypes": categories,
            "adapter": body["adapter"],
            "source": body["source"],
            "intervalSeconds": interval,
            "disappearancePolicy": body.get(
                "disappearancePolicy", "REVIEW_REQUIRED"
            ),
            "enabled": bool(body.get("enabled", True)),
            "lastRunAt": None,
            "nextRunAt": now(),
            "createdAt": now(),
        }
        GeoHandler.store.data.setdefault("schedules", {})[schedule["id"]] = (
            schedule
        )
        GeoHandler.store.event(
            "geodata.refresh-schedule.created.v1",
            "refresh_schedule",
            schedule["id"],
            schedule,
        )
        return {**schedule, "_status": 201}

    @staticmethod
    def list_schedules(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        query = parse_qs(urlparse(p.get("_path", "")).query)
        return page_result(
            list(GeoHandler.store.data.setdefault("schedules", {}).values()),
            query,
        )

    @staticmethod
    def refresh_import(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        schedule = GeoHandler.store.data.setdefault("schedules", {})[
            p["scheduleId"]
        ]
        if not schedule.get("enabled"):
            raise ValueError("refresh schedule is disabled")
        body = p["_body"]
        if "features" not in body or not isinstance(body["features"], list):
            raise ValueError("features must be a list")
        body = {
            **body,
            "programmeSlug": schedule.get("programmeSlug"),
            "entityType": schedule["entityType"],
            "entityTypes": schedule.get("entityTypes")
            or [schedule["entityType"]],
            "adapter": schedule["adapter"],
            "source": schedule["source"],
            "disappearancePolicy": schedule["disappearancePolicy"],
            "completeSnapshot": True,
        }
        result = GeoHandler.import_manual(
            None, {"_body": body, "Idempotency-Key": p.get("Idempotency-Key")}
        )
        schedule["lastRunAt"], schedule["nextRunAt"] = now(), now()
        return result

    @staticmethod
    def bbox(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        query = parse_qs(urlparse(p.get("_path", "")).query)
        try:
            bounds = tuple(
                float(query.get(key, [""])[0])
                for key in ("minLon", "minLat", "maxLon", "maxLat")
            )
        except ValueError as exc:
            raise ValueError(
                "minLon, minLat, maxLon and maxLat are required numbers"
            ) from exc
        if bounds[0] >= bounds[2] or bounds[1] >= bounds[3]:
            raise ValueError("bbox must have min values below max values")
        programme = query.get("programme", [None])[0]
        status = query.get("status", [None])[0]
        max_features = min(1000, max(1, int(query.get("limit", ["500"])[0])))
        if GeoHandler.store.durable:
            entities = GeoHandler.store.query_bbox(
                bounds, max_features + 1, programme, status
            )
            features = []
            for entity in entities:
                properties = entity.pop("publicProperties")
                features.append(
                    {
                        "type": "Feature",
                        "id": entity["id"],
                        "geometry": entity["geometry"],
                        "properties": {
                            "name": entity["name"],
                            "programmeSlug": entity["programmeSlug"],
                            "status": entity["status"],
                            "entityType": entity.get("entityType"),
                            "entityTypes": properties.get("entityTypes")
                            or entity.get("entityType"),
                            "sourceRef": properties.get("sourceRef"),
                            "continentCode": properties.get("continentCode"),
                            "countryCode": properties.get("countryCode"),
                            "regionCode": properties.get("regionCode"),
                            "city": properties.get("city"),
                        },
                    }
                )
            truncated = len(features) > max_features
            features = features[:max_features]
        else:
            features = []
            for entity in GeoHandler.store.items.values():
                if programme and entity.get("programmeSlug") != programme:
                    continue
                if status and entity.get("status") != status:
                    continue
                if not entity.get("geometry"):
                    continue
                entity_box = geometry_bbox(entity["geometry"])
                if (
                    entity_box[2] < bounds[0]
                    or entity_box[0] > bounds[2]
                    or entity_box[3] < bounds[1]
                    or entity_box[1] > bounds[3]
                ):
                    continue
                features.append(
                    {
                        "type": "Feature",
                        "id": entity["id"],
                        "geometry": entity["geometry"],
                        "properties": {
                            "name": entity["name"],
                            "programmeSlug": entity["programmeSlug"],
                            "status": entity["status"],
                            "entityType": entity.get("entityType"),
                            "entityTypes": entity_categories(entity),
                            "sourceRef": entity.get("sourceRef"),
                            "continentCode": entity.get("continentCode"),
                            "countryCode": entity.get("countryCode"),
                            "regionCode": entity.get("regionCode"),
                            "city": entity.get("city"),
                        },
                    }
                )
            truncated = len(features) > max_features
            features = features[:max_features]
        return {
            "type": "FeatureCollection",
            "bbox": list(bounds),
            "features": features,
            "count": len(features),
            "truncated": truncated,
            "cacheTtlSeconds": 60,
            "cacheKey": digest(
                {
                    "bbox": bounds,
                    "programme": programme,
                    "status": status,
                    "minute": int(
                        datetime.now(timezone.utc).timestamp() // 60
                    ),
                }
            ),
            "performanceBudgetMs": 250,
        }

    @staticmethod
    def tile(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        try:
            zoom, tile_x, tile_y = int(p["z"]), int(p["x"]), int(p["y"])
        except ValueError as exc:
            raise ValueError("tile coordinates must be integers") from exc
        if (
            not 0 <= zoom <= 22
            or not 0 <= tile_x < 2**zoom
            or not 0 <= tile_y < 2**zoom
        ):
            raise ValueError("invalid Web Mercator tile coordinates")
        count = 2**zoom
        min_lon = tile_x / count * 360 - 180
        max_lon = (tile_x + 1) / count * 360 - 180

        def latitude(tile_row: int) -> float:
            radians = math.atan(
                math.sinh(math.pi * (1 - 2 * tile_row / count))
            )
            return math.degrees(radians)

        max_lat, min_lat = latitude(tile_y), latitude(tile_y + 1)
        query = f"/v1/geodata/bbox?minLon={min_lon}&minLat={min_lat}&maxLon={max_lon}&maxLat={max_lat}&limit=500"
        return GeoHandler.bbox(None, {"_path": query}) | {
            "tile": {"z": zoom, "x": tile_x, "y": tile_y},
            "format": "geojson-vector",
        }

    @staticmethod
    def list_conflation(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        query = parse_qs(urlparse(p.get("_path", "")).query)
        items = list(
            GeoHandler.store.data.setdefault(
                "conflationCandidates", {}
            ).values()
        )
        resolution = query.get("resolution", [None])[0]
        if resolution:
            items = [
                item for item in items if item.get("resolution") == resolution
            ]
        return page_result(items, query)

    @staticmethod
    def resolve_conflation(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        body = p["_body"]
        require(body, "decision", "reviewerId")
        if body["decision"] not in {
            "MERGED",
            "KEPT_SEPARATE",
            "IGNORED",
            "OPEN",
        }:
            raise ValueError(
                "decision must be MERGED, KEPT_SEPARATE, IGNORED, or OPEN"
            )
        item = GeoHandler.store.data.setdefault("conflationCandidates", {})[
            p["candidateId"]
        ]
        previous = item["resolution"]
        item["resolution"] = body["decision"]
        item["survivorEntityId"] = body.get("survivorEntityId")
        item.setdefault("resolutionHistory", []).append(
            {
                "decision": body["decision"],
                "previousDecision": previous,
                "reviewerId": body["reviewerId"],
                "note": body.get("note"),
                "occurredAt": now(),
            }
        )
        item["updatedAt"] = now()
        GeoHandler.store.event(
            "geodata.conflation.resolved.v1",
            "conflation_candidate",
            item["id"],
            item,
        )
        return item

    @staticmethod
    def draw_proposal(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        body = dict(p["_body"])
        if p.get("_http"):
            authorization = p.get("Authorization", "")
            if not authorization.startswith("Bearer "):
                raise PermissionError("Bearer authentication is required")
            body["proposerId"] = verify_token(authorization[7:], "access")[
                "sub"
            ]
        require(body, "feature")
        feature = dict(body["feature"])
        feature["attachments"] = body.get(
            "attachments", feature.get("attachments")
        )
        properties = feature.get("properties") or {}
        # The preferred proposal contract carries shared categories at the
        # request level. Preserve compatibility with older clients that put
        # them in feature properties before normalizing the candidate.
        request_categories = (
            body.get("entityTypes")
            or body.get("entityTypeCodes")
            or body.get("entityType")
        )
        if request_categories and not (
            properties.get("entityTypes")
            or properties.get("entityTypeCodes")
            or properties.get("entityType")
        ):
            properties["entityTypes"] = request_categories
        categories = entity_type_codes(
            properties.get("entityTypes") or properties.get("entityTypeCodes"),
            properties.get("entityType"),
        )
        if not categories:
            raise ValueError(
                "entityTypes must contain at least one shared entity category code"
            )
        properties["candidateSource"] = {
            "type": "COMMUNITY_PROPOSAL",
            "proposalId": new_id(),
            "proposerId": body.get("proposerId") or p.get("accountId"),
        }
        feature["properties"] = properties
        result = GeoHandler.import_manual(
            None,
            {
                "_body": {
                    "programmeSlug": body.get("programmeSlug"),
                    "adapter": "MANUAL",
                    "entityTypes": categories,
                    "entityType": categories[0],
                    "source": {
                        **(body.get("source") or {}),
                        "name": (body.get("source") or {}).get(
                            "name", "Manual proposal"
                        ),
                        "license": (body.get("source") or {}).get(
                            "license", "programme-supplied"
                        ),
                    },
                    "features": [feature],
                }
            },
        )
        # Community proposals are already an interactive, user-reviewed action;
        # unlike bulk file/paste imports they enter the normal CANDIDATE queue
        # immediately after the same normalization step.
        run_id = result["importRunId"]
        candidate_ids = result.get("preprocessed", [])
        GeoHandler.validate_import_candidates(
            None,
            {
                "runId": run_id,
                "_body": {
                    "candidateIds": candidate_ids,
                    "reviewerId": body.get("proposerId")
                    or p.get("accountId")
                    or "proposal",
                },
            },
        )
        queue = GeoHandler.process_import_candidates(
            None,
            {
                "runId": run_id,
                "_body": {
                    "candidateIds": candidate_ids,
                    "targetStatus": "CANDIDATE",
                    "processorId": body.get("proposerId")
                    or p.get("accountId")
                    or "proposal",
                },
            },
        )
        processed = queue.get("result", {}).get("created", []) + queue.get(
            "result", {}
        ).get("updated", [])
        return {
            **result,
            "created": processed,
            "status": "COMPLETED",
            "processingQueueId": queue["id"],
        }

    @staticmethod
    def review(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        entity = GeoHandler.store.items[p["entityId"]]
        GeoHandler._authorize_review(p, entity)
        body = p["_body"]
        require(body, "decision", "reviewerId")
        if entity["status"] != "CANDIDATE":
            raise ValueError("only candidate entities can be reviewed")
        if body["decision"] not in ("APPROVED", "REJECTED"):
            raise ValueError("decision must be APPROVED or REJECTED")
        reviewed_at = now()
        entity["status"] = body["decision"]
        entity["review"] = {
            **(entity.get("review") or {}),
            "reviewerId": body["reviewerId"],
            "note": body.get("note"),
            "reviewedAt": reviewed_at,
        }
        entity.setdefault("reviewHistory", []).append(
            {
                "action": body["decision"],
                "reviewerId": body["reviewerId"],
                "note": body.get("note"),
                "occurredAt": reviewed_at,
                "previousStatus": "CANDIDATE",
            }
        )
        entity["updatedAt"] = now()
        GeoHandler.store.event(
            "geodata.entity.reviewed.v1", "entity", entity["id"], entity
        )
        return entity

    @staticmethod
    def set_status(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        entity = GeoHandler.store.items[p["entityId"]]
        GeoHandler._authorize_review(p, entity)
        body = p["_body"]
        require(body, "status", "reviewerId")
        allowed = {"APPROVED", "CANDIDATE", "RETIRED", "REJECTED"}
        if body["status"] not in allowed:
            raise ValueError(
                "status must be APPROVED, CANDIDATE, RETIRED, or REJECTED"
            )
        previous_status = entity["status"]
        target_status = body["status"]
        if previous_status == "APPROVED" and target_status != "RETIRED":
            raise ValueError(
                "approved entities can only be retired to protect historical QSOs"
            )
        if previous_status == "RETIRED" and target_status != "RETIRED":
            raise ValueError("retired entities cannot be reactivated")
        if previous_status == "CANDIDATE" and target_status not in {
            "CANDIDATE",
            "APPROVED",
            "REJECTED",
        }:
            raise ValueError(
                "candidate entities can only remain candidates, be approved, or be rejected"
            )
        if previous_status == "REJECTED" and target_status != "REJECTED":
            raise ValueError(
                "rejected entities cannot be moved back into the review lifecycle"
            )
        if previous_status == target_status:
            return entity
        changed_at = now()
        entity["status"] = target_status
        entity["review"] = {
            **(entity.get("review") or {}),
            "reviewerId": body["reviewerId"],
            "note": body.get("note"),
            "changedAt": changed_at,
        }
        entity.setdefault("reviewHistory", []).append(
            {
                "action": "STATUS_CHANGED",
                "status": target_status,
                "previousStatus": previous_status,
                "reviewerId": body["reviewerId"],
                "note": body.get("note"),
                "occurredAt": changed_at,
            }
        )
        entity["updatedAt"] = changed_at
        GeoHandler.store.event(
            "geodata.entity.status-changed.v1",
            "entity",
            entity["id"],
            {
                "entityId": entity["id"],
                "status": target_status,
                "previousStatus": previous_status,
                "reviewerId": body["reviewerId"],
                "note": body.get("note"),
            },
        )
        return entity

    @staticmethod
    def update_geometry(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        entity = GeoHandler.store.items[p["entityId"]]
        GeoHandler._authorize_review(p, entity)
        body = p["_body"]
        require(body, "geometry", "editorId")
        geometry = body["geometry"]
        if not isinstance(geometry, dict) or "coordinates" not in geometry:
            raise ValueError(
                "geometry must be a GeoJSON Point, LineString, MultiLineString, Polygon, or MultiPolygon"
            )
        previous = entity.get("geometry")
        previous_geometry_hash = digest(previous)
        entity.setdefault("geometryHistory", []).append(
            {
                "editorId": body["editorId"],
                "note": body.get("note"),
                "geometry": previous,
                "editedAt": now(),
            }
        )
        entity["geometry"] = normalize_geometry(geometry)
        entity["centroid"] = geometry_centroid(entity["geometry"])
        apply_maidenhead_fields(entity)
        geometry_hash = digest(entity["geometry"])
        if geometry_hash != previous_geometry_hash:
            GeoHandler._queue_location_enrichment(
                entity,
                reason="GEOMETRY_CHANGED",
                requested_by=body["editorId"],
            )
        entity["updatedAt"] = now()
        GeoHandler.store.event(
            "geodata.entity.geometry-updated.v1",
            "entity",
            entity["id"],
            {
                "entityId": entity["id"],
                "editorId": body["editorId"],
                "note": body.get("note"),
                "geometry": geometry,
            },
        )
        return entity

    @staticmethod
    def update_location(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        entity = GeoHandler.store.items[p["entityId"]]
        GeoHandler._authorize_gis_admin(p, entity, "geodata.location.manage")
        body = p["_body"]
        require(body, "editorId")
        if "location" not in body:
            raise ValueError("location must be an object")
        location = body["location"]
        if not isinstance(location, dict):
            raise ValueError("location must be an object")
        allowed = set(LOCATION_FIELDS)
        unknown = set(location) - allowed
        if unknown:
            raise ValueError(
                f"unsupported location fields: {', '.join(sorted(unknown))}"
            )
        requested_manual = body.get("manualFields", list(location))
        if (
            not isinstance(requested_manual, list)
            or not set(requested_manual) <= allowed
        ):
            raise ValueError(
                "manualFields must be a list of supported location fields"
            )
        manual_fields = set(requested_manual)
        code_fields = {
            "continentCode",
            "countryCode",
            "regionCode",
            "subdivisionCode",
            "provinceCode",
        }
        if manual_fields.intersection(code_fields):
            raise ValueError(
                "continent, country, subdivision, and province codes are provider-derived and cannot be manually edited"
            )
        previous_manual = set(entity.get("manualLocationFields") or [])
        released_fields = previous_manual - manual_fields

        def normalize_value(field: str, value: Any) -> Any:
            if value is None:
                return None
            if not isinstance(value, (str, int, float, bool)):
                raise ValueError(
                    f"location field {field} must be a scalar or null"
                )
            value = str(value).strip()
            return value.upper() if field.endswith("Code") else value or None

        previous = {field: entity.get(field) for field in LOCATION_FIELDS}
        for field in manual_fields:
            if field in location:
                entity[field] = normalize_value(field, location[field])
        if GeoHandler.store.durable:
            from relational_queries import location_rows

            catalogue_locations = location_rows(GeoHandler.store)
        else:
            catalogue_locations = GeoHandler.store.items.values()
        entity.update(
            derive_location_codes(location, manual_fields, catalogue_locations)
        )
        for field in released_fields:
            entity[field] = None
        entity["manualLocationFields"] = sorted(manual_fields)
        if released_fields:
            GeoHandler._queue_location_enrichment(
                entity,
                reason="MANUAL_FIELDS_RELEASED",
                requested_by=body["editorId"],
                only_missing=True,
            )
        changed_at = now()
        entity.setdefault("reviewHistory", []).append(
            {
                "action": "LOCATION_UPDATED",
                "editorId": body["editorId"],
                "note": body.get("note"),
                "manualFields": sorted(manual_fields),
                "previous": previous,
                "location": {
                    field: entity.get(field) for field in LOCATION_FIELDS
                },
                "occurredAt": changed_at,
            }
        )
        entity.setdefault("provenance", {})["manualLocation"] = {
            "fields": sorted(manual_fields),
            "editorId": body["editorId"],
            "note": body.get("note"),
            "updatedAt": changed_at,
        }
        entity["updatedAt"] = changed_at
        GeoHandler.store.event(
            "geodata.entity.location-updated.v1",
            "entity",
            entity["id"],
            {
                "entityId": entity["id"],
                "editorId": body["editorId"],
                "manualFields": sorted(manual_fields),
                "note": body.get("note"),
            },
        )
        return entity

    @staticmethod
    def request_location_enrichment(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        entity = GeoHandler.store.items[p["entityId"]]
        GeoHandler._authorize_gis_admin(p, entity, "geodata.location.manage")
        body = p["_body"]
        require(body, "editorId")
        missing = GeoHandler._missing_location_fields(entity)
        if not missing:
            from relational_state import StateConflict

            raise StateConflict("location metadata is already complete")
        if not isinstance(entity.get("geometry"), dict):
            raise ValueError("location enrichment requires entity geometry")
        GeoHandler._queue_location_enrichment(
            entity,
            reason="ADMIN_REQUESTED_MISSING_METADATA",
            requested_by=body["editorId"],
            only_missing=True,
        )
        return {
            "entityId": entity["id"],
            "requestId": entity.get("locationEnrichmentRequestId"),
            "status": entity.get("locationEnrichmentStatus"),
            "missingFields": missing,
            "queued": True,
            "_status": 202,
        }

    @staticmethod
    def change_entity_type(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        entity = GeoHandler.store.items[p["entityId"]]
        GeoHandler._authorize_review(p, entity)
        body = p["_body"]
        require(body, "editorId")
        if entity.get("status") == "RETIRED":
            raise ValueError("retired entities cannot change category")
        categories = entity_type_codes(
            body.get("entityTypes") or body.get("entityTypeCodes"),
            body.get("entityType"),
        )
        if not categories:
            raise ValueError(
                "entityTypes must contain at least one stable category code"
            )
        previous = entity_categories(entity)
        if categories == previous:
            return entity
        changed_at = now()
        entity["entityType"] = categories[0]
        entity["entityTypes"] = categories
        entity["entityTypeCodes"] = categories
        entity.setdefault("reviewHistory", []).append(
            {
                "action": "ENTITY_TYPE_CHANGED",
                "editorId": body["editorId"],
                "previousEntityTypes": previous,
                "entityTypes": categories,
                "previousEntityType": previous[0] if previous else None,
                "entityType": categories[0],
                "note": body.get("note"),
                "occurredAt": changed_at,
            }
        )
        entity["updatedAt"] = changed_at
        GeoHandler.store.event(
            "geodata.entity.entity-type-changed.v1",
            "entity",
            entity["id"],
            {
                "entityId": entity["id"],
                "editorId": body["editorId"],
                "previousEntityTypes": previous,
                "entityTypes": categories,
                "note": body.get("note"),
            },
        )
        return entity

    @staticmethod
    def change_entity_name(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        entity = GeoHandler.store.items[p["entityId"]]
        GeoHandler._authorize_review(p, entity)
        body = p["_body"]
        require(body, "name", "editorId")
        name = str(body["name"]).strip()
        if not name:
            raise ValueError("name must not be empty")
        if len(name) > 240:
            raise ValueError("name must be 240 characters or fewer")
        previous = entity.get("name") or ""
        if name == previous:
            return entity
        changed_at = now()
        entity["name"] = name
        entity.setdefault("reviewHistory", []).append(
            {
                "action": "ENTITY_NAME_CHANGED",
                "editorId": body["editorId"],
                "previousName": previous,
                "name": name,
                "note": body.get("note"),
                "occurredAt": changed_at,
            }
        )
        entity["updatedAt"] = changed_at
        GeoHandler.store.event(
            "geodata.entity.name-changed.v1",
            "entity",
            entity["id"],
            {
                "entityId": entity["id"],
                "editorId": body["editorId"],
                "previousName": previous,
                "name": name,
                "note": body.get("note"),
            },
        )
        return entity

    @staticmethod
    def change_geometry_type(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        entity = GeoHandler.store.items[p["entityId"]]
        GeoHandler._authorize_gis_admin(p, entity, "geodata.geometry.manage")
        body = p["_body"]
        require(body, "geometryType", "editorId")
        target = str(body["geometryType"]).upper()
        target = {"WAY": "LINESTRING"}.get(target, target)
        if target not in {
            "POINT",
            "LINESTRING",
            "MULTILINESTRING",
            "POLYGON",
            "MULTIPOLYGON",
        }:
            raise ValueError(
                "geometryType must be POINT, LINESTRING, MULTILINESTRING, POLYGON, or MULTIPOLYGON"
            )
        current = str(entity.get("geometry", {}).get("type", "")).upper()
        current = {"WAY": "LINESTRING"}.get(current, current)
        if current == target:
            return entity
        geometry = entity.get("geometry") or {}
        if target == "POINT":
            centre = geometry_centroid(geometry)
            converted = {
                "type": "Point",
                "coordinates": [centre["lon"], centre["lat"]],
            }
        elif target in {"LINESTRING", "MULTILINESTRING"}:
            if current == "POINT":
                coordinates = geometry.get("coordinates") or []
                if len(coordinates) < 2:
                    raise ValueError("the existing point geometry is invalid")
                lon, lat = float(coordinates[0]), float(coordinates[1])
                delta = 0.0005
                lines = [[[lon - delta, lat], [lon + delta, lat]]]
            elif current == "LINESTRING":
                lines = [geometry.get("coordinates") or []]
            elif current == "MULTILINESTRING":
                lines = geometry.get("coordinates") or []
            else:
                rings = geometry.get("coordinates", [])
                if current == "MULTIPOLYGON":
                    rings = rings[0] if rings else []
                ring = (
                    rings[0]
                    if current in {"POLYGON", "MULTIPOLYGON"} and rings
                    else rings
                )
                if len(ring) > 1 and ring[0] == ring[-1]:
                    ring = ring[:-1]
                lines = [ring]
            converted = {
                "type": target.title()
                if target == "LINESTRING"
                else "MultiLineString",
                "coordinates": lines[0] if target == "LINESTRING" else lines,
            }
        elif target in {"POLYGON", "MULTIPOLYGON"} and current == "POINT":
            coordinates = geometry.get("coordinates") or []
            if len(coordinates) < 2:
                raise ValueError("the existing point geometry is invalid")
            lon, lat = float(coordinates[0]), float(coordinates[1])
            delta = 0.0005
            polygon = [
                [
                    [lon - delta, lat - delta],
                    [lon + delta, lat - delta],
                    [lon + delta, lat + delta],
                    [lon - delta, lat + delta],
                    [lon - delta, lat - delta],
                ]
            ]
            converted = {
                "type": "Polygon" if target == "POLYGON" else "MultiPolygon",
                "coordinates": polygon if target == "POLYGON" else [polygon],
            }
        elif target in {"POLYGON", "MULTIPOLYGON"} and current in {
            "LINESTRING",
            "MULTILINESTRING",
        }:
            lines = geometry.get("coordinates") or []
            points = (
                lines
                if current == "LINESTRING"
                else [point for line in lines for point in line]
            )
            if len(points) < 2:
                raise ValueError("the existing line geometry is invalid")
            longitudes = [float(point[0]) for point in points]
            latitudes = [float(point[1]) for point in points]
            delta = max(
                (max(longitudes) - min(longitudes)) * 0.05,
                (max(latitudes) - min(latitudes)) * 0.05,
                0.0001,
            )
            min_lon, max_lon = min(longitudes) - delta, max(longitudes) + delta
            min_lat, max_lat = min(latitudes) - delta, max(latitudes) + delta
            polygon = [
                [
                    [min_lon, min_lat],
                    [max_lon, min_lat],
                    [max_lon, max_lat],
                    [min_lon, max_lat],
                    [min_lon, min_lat],
                ]
            ]
            converted = {
                "type": "Polygon" if target == "POLYGON" else "MultiPolygon",
                "coordinates": polygon if target == "POLYGON" else [polygon],
            }
        else:
            polygons = geometry.get("coordinates", [])
            if current == "POLYGON":
                polygons = [polygons]
            if not polygons:
                raise ValueError("the existing polygon geometry is invalid")
            converted = {
                "type": "Polygon" if target == "POLYGON" else "MultiPolygon",
                "coordinates": polygons[0]
                if target == "POLYGON"
                else polygons,
            }
        converted = normalize_geometry(converted)
        changed_at = now()
        entity.setdefault("geometryHistory", []).append(
            {
                "action": "GEOMETRY_TYPE_CHANGED",
                "editorId": body["editorId"],
                "note": body.get("note"),
                "previousGeometry": geometry,
                "geometry": converted,
                "editedAt": changed_at,
            }
        )
        entity.setdefault("reviewHistory", []).append(
            {
                "action": "GEOMETRY_TYPE_CHANGED",
                "editorId": body["editorId"],
                "note": body.get("note"),
                "previousType": current,
                "geometryType": target,
                "occurredAt": changed_at,
            }
        )
        entity["geometry"] = converted
        entity["centroid"] = geometry_centroid(converted)
        apply_maidenhead_fields(entity)
        GeoHandler._queue_location_enrichment(
            entity,
            reason="GEOMETRY_TYPE_CHANGED",
            requested_by=body["editorId"],
        )
        entity["updatedAt"] = changed_at
        GeoHandler.store.event(
            "geodata.entity.geometry-type-changed.v1",
            "entity",
            entity["id"],
            {
                "entityId": entity["id"],
                "editorId": body["editorId"],
                "previousType": current,
                "geometryType": target,
                "note": body.get("note"),
            },
        )
        return entity

    @staticmethod
    def delete_entity(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        entity = GeoHandler.store.items[p["entityId"]]
        GeoHandler._authorize_gis_admin(p, entity, "geodata.delete")
        authorization = p.get("Authorization", "")
        claims = (
            verify_token(authorization[7:])
            if authorization.startswith("Bearer ")
            else {}
        )
        global_admin = any(
            role.get("role") in {"GLOBAL_ADMIN", "GLOBAL_OPERATOR"}
            for role in claims.get("roles", [])
        )
        if not global_admin and entity.get("status") != "REJECTED":
            raise ValueError(
                "only rejected entities can be permanently deleted"
            )
        entity_id = entity["id"]
        for candidate_id, candidate in list(
            GeoHandler.store.data.setdefault(
                "conflationCandidates", {}
            ).items()
        ):
            if entity_id in (
                candidate.get("leftEntityId"),
                candidate.get("rightEntityId"),
            ):
                GeoHandler.store.data["conflationCandidates"].pop(
                    candidate_id, None
                )
        GeoHandler.store.delete_relational(entity_id)
        GeoHandler.store.items.pop(entity_id, None)
        if not GeoHandler.store.durable:
            GeoHandler.store.events[:] = [
                event
                for event in GeoHandler.store.events
                if event.get("aggregate", {}).get("id") != entity_id
            ]
        GeoHandler.store.event(
            "geodata.entity.deleted.v1",
            "entity",
            entity_id,
            {
                "entityId": entity_id,
                "deletedBy": p.get("_body", {}).get("deletedBy"),
                "previousStatus": entity.get("status"),
            },
        )
        return {
            "entityId": entity_id,
            "deleted": True,
            "previousStatus": entity.get("status"),
            "_status": 204,
        }

    @staticmethod
    def patch_entity_metadata(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        body = dict(p.get("_body") or {})
        editor_id = body.get("editorId")
        require(body, "editorId")
        supported = {"editorId", "name", "location", "manualFields", "note"}
        unknown = set(body) - supported
        if unknown:
            raise ValueError(
                f"unsupported entity metadata fields: {', '.join(sorted(unknown))}"
            )
        entity = GeoHandler.store.items[p["entityId"]]
        result = entity
        if "name" in body:
            result = GeoHandler.change_entity_name(
                None,
                {
                    **p,
                    "_body": {
                        "name": body["name"],
                        "editorId": editor_id,
                        "note": body.get("note"),
                    },
                },
            )
        if "location" in body:
            location_body = {
                "location": body["location"],
                "editorId": editor_id,
                "note": body.get("note"),
            }
            if "manualFields" in body:
                location_body["manualFields"] = body["manualFields"]
            result = GeoHandler.update_location(
                None, {**p, "_body": location_body}
            )
        if "name" not in body and "location" not in body:
            raise ValueError(
                "at least one editable metadata field is required: name or location"
            )
        return result

    @staticmethod
    def put_entity_categories(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        body = dict(p.get("_body") or {})
        categories = (
            body.get("entityTypes")
            or body.get("entityTypeCodes")
            or body.get("entityType")
        )
        return GeoHandler.change_entity_type(
            None,
            {
                **p,
                "_body": {
                    "entityTypes": categories,
                    "editorId": body.get("editorId"),
                    "note": body.get("note"),
                },
            },
        )

    @staticmethod
    def review_resource(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        body = dict(p.get("_body") or {})
        status = body.get("status") or body.get("decision")
        if status:
            return GeoHandler.set_status(
                None,
                {
                    **p,
                    "_body": {
                        "status": status,
                        "reviewerId": body.get("reviewerId"),
                        "note": body.get("note"),
                    },
                },
            )
        raise ValueError("review status or decision is required")

    @staticmethod
    def _activity_request(
        p: dict[str, str],
        path: str,
        method: str = "GET",
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not p.get("_http"):
            return {
                "qsoCount": 0,
                "activationCount": 0,
                "awardProgressCount": 0,
            }
        url = (
            os.environ.get(
                "MYOTA_ACTIVITY_URL", "http://activity:8004"
            ).rstrip("/")
            + path
        )
        headers = {
            "Accept": "application/json",
            "User-Agent": "MyOTA-geodata-service/1.0",
        }
        if p.get("Authorization"):
            headers["Authorization"] = p["Authorization"]
        if p.get("Idempotency-Key"):
            headers["Idempotency-Key"] = p["Idempotency-Key"]
        data = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        request = Request(url, data=data, method=method, headers=headers)
        try:
            with urlopen(request, timeout=15) as response:  # nosec B310 - deployment-controlled internal URL
                return json.loads(response.read().decode("utf-8"))
        except (
            HTTPError,
            URLError,
            TimeoutError,
            json.JSONDecodeError,
        ) as exc:
            raise RuntimeError(
                f"activity service request failed: {exc}"
            ) from exc

    @staticmethod
    def create_deletion_job(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        body = p.get("_body") or {}
        require(body, "entityId", "requestedBy")
        entity = GeoHandler.store.items[body["entityId"]]
        GeoHandler._authorize_gis_admin(p, entity, "geodata.delete")
        authorization = p.get("Authorization", "")
        claims = (
            verify_token(authorization[7:])
            if authorization.startswith("Bearer ")
            else {}
        )
        global_admin = any(
            role.get("role") in {"GLOBAL_ADMIN", "GLOBAL_OPERATOR"}
            for role in claims.get("roles", [])
        )
        if not global_admin and entity.get("status") != "REJECTED":
            raise ValueError(
                "only global administrators can delete non-rejected entities"
            )
        impact = GeoHandler._activity_request(
            p, f"/v1/activations/entity-deletion-impacts/{entity['id']}"
        )
        job = {
            "id": new_id(),
            "entityId": entity["id"],
            "status": "AWAITING_CONFIRMATION",
            "requestedBy": body["requestedBy"],
            "impact": impact,
            "confirmationRequired": True,
            "createdAt": now(),
            "updatedAt": now(),
        }
        GeoHandler.store.data.setdefault("entityDeletionJobs", {})[
            job["id"]
        ] = job
        GeoHandler.store.event(
            "geodata.entity-deletion-job.created.v1",
            "entity_deletion_job",
            job["id"],
            job,
        )
        GeoHandler.store.persist()
        return {**job, "_status": 201}

    @staticmethod
    def get_deletion_job(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        job = GeoHandler.store.data.setdefault("entityDeletionJobs", {})[
            p["jobId"]
        ]
        GeoHandler._authorize_gis_admin(p, job, "geodata.delete")
        return {
            key: value
            for key, value in job.items()
            if key != "executionClaims"
        }

    @staticmethod
    def _execute_deletion_job(
        job_id: str, p: dict[str, str] | None = None
    ) -> bool:
        if GeoHandler.store.durable:
            GeoHandler.store.refresh_entity_deletion_job(job_id)
        jobs = GeoHandler.store.data.setdefault("entityDeletionJobs", {})
        job = jobs.get(job_id)
        if not job:
            raise KeyError(f"entity deletion job {job_id} does not exist")
        if job.get("status") in {"COMPLETED", "FAILED"}:
            return True
        if GeoHandler.store.durable:
            with GeoHandler.store.transaction() as connection:
                claimed = connection.execute(
                    "UPDATE geodata_control_record SET payload=payload || "
                    "jsonb_build_object('status','PROCESSING','leaseUntil',"
                    "now()+interval '120 seconds'),updated_at=now() "
                    "WHERE kind='entityDeletionJobs' AND id=%s AND "
                    "(payload->>'status'='QUEUED' OR (payload->>'status'='PROCESSING' "
                    "AND (payload->>'leaseUntil')::timestamptz<=now())) RETURNING id",
                    (job_id,),
                ).fetchone()
            if not claimed:
                return False
            GeoHandler.store._repository.invalidate(
                "entityDeletionJobs", job_id
            )
            job = jobs[job_id]
            claims = job.get("executionClaims")
            if not claims:
                raise PermissionError(
                    "deletion job has no authorized execution context"
                )
            claims = {
                **claims,
                "exp": int(datetime.now(timezone.utc).timestamp()) + 300,
            }
            p = {
                "_http": "1",
                "Authorization": f"Bearer {sign_token(claims)}",
                "_body": {},
                "Idempotency-Key": f"geodata-delete:{job_id}",
            }
        p = p or {}
        job.update({"status": "PROCESSING", "updatedAt": now()})
        GeoHandler.store.persist()
        try:
            cascade = GeoHandler._activity_request(
                p,
                "/v1/activations/entity-deletion-cascades",
                "POST",
                {
                    "entityId": job["entityId"],
                    "deletedBy": p.get("_body", {}).get("deletedBy")
                    or job["requestedBy"],
                },
            )
            deleted = {"entityId": job["entityId"], "deleted": True}
            if job["entityId"] in GeoHandler.store.items:
                deleted = GeoHandler.delete_entity(
                    None,
                    {
                        **p,
                        "entityId": job["entityId"],
                        "_body": {
                            "deletedBy": p.get("_body", {}).get("deletedBy")
                            or job["requestedBy"]
                        },
                    },
                )
            job.update(
                {
                    "status": "COMPLETED",
                    "cascade": cascade,
                    "deleted": deleted,
                    "completedAt": now(),
                    "updatedAt": now(),
                }
            )
            GeoHandler.store.event(
                "geodata.entity-deletion-job.completed.v1",
                "entity_deletion_job",
                job_id,
                {
                    key: value
                    for key, value in job.items()
                    if key != "executionClaims"
                },
            )
        except Exception as exc:  # pragma: no cover - exercised by service integration failures
            GeoHandler.store.rollback_pending()
            job.update(
                {"status": "FAILED", "error": str(exc), "updatedAt": now()}
            )
            GeoHandler.store.event(
                "geodata.entity-deletion-job.failed.v1",
                "entity_deletion_job",
                job_id,
                {
                    key: value
                    for key, value in job.items()
                    if key != "executionClaims"
                },
            )
        GeoHandler.store.persist()
        return True

    @staticmethod
    def confirm_deletion_job(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        body = p.get("_body") or {}
        require(body, "confirmation", "deletedBy")
        if str(body["confirmation"]).upper() != "DELETE":
            raise ValueError("confirmation must be DELETE")
        job = GeoHandler.store.data.setdefault("entityDeletionJobs", {})[
            p["jobId"]
        ]
        GeoHandler._authorize_gis_admin(p, job, "geodata.delete")
        if job.get("status") == "COMPLETED":
            return {
                key: value
                for key, value in job.items()
                if key != "executionClaims"
            }
        if job.get("status") != "AWAITING_CONFIRMATION":
            raise ValueError("deletion job is not awaiting confirmation")
        job.update(
            {
                "status": "QUEUED",
                "confirmedBy": body["deletedBy"],
                "confirmedAt": now(),
                "updatedAt": now(),
            }
        )
        request_context = {**p, "_body": body}
        if GeoHandler.store.durable:
            authorization = p.get("Authorization", "")
            claims = verify_token(authorization[7:])
            job["executionClaims"] = {
                name: claims.get(name) for name in ("sub", "roles", "scp")
            }
            GeoHandler.store.event(
                "geodata.entity-deletion-job.queued.v1",
                "entity_deletion_job",
                job["id"],
                {
                    "jobId": job["id"],
                    "natsSubject": "myota.geodata.entity.delete.v1",
                },
            )
            return {
                **{
                    key: value
                    for key, value in job.items()
                    if key != "executionClaims"
                },
                "queued": True,
                "_status": 202,
            }
        if p.get("_http"):
            GeoHandler.deletion_executor.submit(
                GeoHandler._execute_deletion_job, job["id"], request_context
            )
            return {**job, "queued": True, "_status": 202}
        GeoHandler._execute_deletion_job(job["id"], request_context)
        return job

    delete_rejected_entity = delete_entity


GeoHandler.routes = {
    ("GET", "/v1/geodata/adapters"): GeoHandler.adapters,
    ("GET", "/v1/geodata/location-options"): GeoHandler.location_options,
    ("GET", "/v1/geodata/entities"): GeoHandler.list_entities,
    ("GET", "/v1/geodata/entities/{entityId}"): GeoHandler.get_entity,
    ("GET", "/v1/geodata/entities/{entityId}/audit"): GeoHandler.audit_entity,
    ("GET", "/v1/geodata/bbox"): GeoHandler.bbox,
    ("GET", "/v1/geodata/tiles/{z}/{x}/{y}"): GeoHandler.tile,
    ("GET", "/v1/geodata/imports"): GeoHandler.list_imports,
    (
        "GET",
        "/v1/geodata/import-uploads/{uploadId}",
    ): GeoHandler.get_import_upload,
    ("GET", "/v1/geodata/imports/{runId}"): GeoHandler.get_import,
    (
        "PUT",
        "/v1/geodata/imports/{runId}/cancellation",
    ): GeoHandler.cancel_import,
    (
        "GET",
        "/v1/geodata/imports/{runId}/candidates",
    ): GeoHandler.list_import_candidates,
    ("GET", "/v1/geodata/refresh-schedules"): GeoHandler.list_schedules,
    ("GET", "/v1/geodata/conflation"): GeoHandler.list_conflation,
    ("POST", "/v1/geodata/imports/manual"): GeoHandler.import_manual,
    ("POST", "/v1/geodata/imports"): GeoHandler.enqueue_import,
    ("POST", "/v1/geodata/imports/upload"): GeoHandler.upload_import,
    ("POST", "/v1/geodata/import-uploads"): GeoHandler.create_import_upload,
    (
        "POST",
        "/v1/geodata/import-uploads/{uploadId}/parts/{partNumber}",
    ): GeoHandler.upload_import_part,
    (
        "POST",
        "/v1/geodata/import-uploads/{uploadId}/complete",
    ): GeoHandler.complete_import_upload,
    (
        "DELETE",
        "/v1/geodata/import-uploads/{uploadId}",
    ): GeoHandler.abort_import_upload,
    (
        "POST",
        "/v1/geodata/imports/{runId}/candidates/validate",
    ): GeoHandler.validate_import_candidates,
    (
        "POST",
        "/v1/geodata/imports/{runId}/process",
    ): GeoHandler.process_import_candidates,
    (
        "POST",
        "/v1/geodata/imports/{runId}/processed",
    ): GeoHandler.mark_import_processed,
    (
        "DELETE",
        "/v1/geodata/load-test-runs/{testRunId}",
    ): GeoHandler.cleanup_load_test_run,
    ("POST", "/v1/geodata/refresh-schedules"): GeoHandler.create_schedule,
    (
        "POST",
        "/v1/geodata/refresh-schedules/{scheduleId}/run",
    ): GeoHandler.refresh_import,
    ("POST", "/v1/geodata/proposals/draw"): GeoHandler.draw_proposal,
    ("POST", "/v1/geodata/proposals"): GeoHandler.draw_proposal,
    (
        "POST",
        "/v1/geodata/conflation/{candidateId}/resolve",
    ): GeoHandler.resolve_conflation,
    ("POST", "/v1/geodata/entities/{entityId}/review"): GeoHandler.review,
    (
        "POST",
        "/v1/geodata/entities/{entityId}/reviews",
    ): GeoHandler.review_resource,
    ("POST", "/v1/geodata/entities/{entityId}/status"): GeoHandler.set_status,
    (
        "POST",
        "/v1/geodata/entities/{entityId}/geometry",
    ): GeoHandler.update_geometry,
    (
        "PUT",
        "/v1/geodata/entities/{entityId}/geometry",
    ): GeoHandler.update_geometry,
    (
        "PATCH",
        "/v1/geodata/entities/{entityId}",
    ): GeoHandler.patch_entity_metadata,
    (
        "PUT",
        "/v1/geodata/entities/{entityId}/categories",
    ): GeoHandler.put_entity_categories,
    (
        "POST",
        "/v1/geodata/entities/{entityId}/location",
    ): GeoHandler.update_location,
    (
        "POST",
        "/v1/geodata/entities/{entityId}/location-enrichment-requests",
    ): GeoHandler.request_location_enrichment,
    (
        "POST",
        "/v1/geodata/entities/{entityId}/entity-type",
    ): GeoHandler.change_entity_type,
    (
        "POST",
        "/v1/geodata/entities/{entityId}/name",
    ): GeoHandler.change_entity_name,
    (
        "POST",
        "/v1/geodata/entities/{entityId}/geometry-type",
    ): GeoHandler.change_geometry_type,
    (
        "POST",
        "/v1/geodata/entities/{entityId}/delete",
    ): GeoHandler.delete_entity,
    (
        "POST",
        "/v1/geodata/entity-deletion-jobs",
    ): GeoHandler.create_deletion_job,
    (
        "GET",
        "/v1/geodata/entity-deletion-jobs/{jobId}",
    ): GeoHandler.get_deletion_job,
    (
        "POST",
        "/v1/geodata/entity-deletion-jobs/{jobId}/confirm",
    ): GeoHandler.confirm_deletion_job,
}

GeoHandler.deprecated_routes = {
    ("POST", "/v1/geodata/imports/manual"),
    ("POST", "/v1/geodata/imports/upload"),
    ("POST", "/v1/geodata/proposals/draw"),
    ("GET", "/v1/geodata/bbox"),
    ("POST", "/v1/geodata/entities/{entityId}/review"),
    ("POST", "/v1/geodata/entities/{entityId}/status"),
    ("POST", "/v1/geodata/entities/{entityId}/geometry"),
    ("POST", "/v1/geodata/entities/{entityId}/location"),
    ("POST", "/v1/geodata/entities/{entityId}/entity-type"),
    ("POST", "/v1/geodata/entities/{entityId}/name"),
    ("POST", "/v1/geodata/entities/{entityId}/delete"),
}


install_operations(GeoHandler)


if __name__ == "__main__":
    GeoHandler.store.hydrate()
    ThreadingHTTPServer(("0.0.0.0", 8003), GeoHandler).serve_forever()
