from __future__ import annotations

from http.server import ThreadingHTTPServer
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
import re
import threading
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import parse_qs, urlparse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from common import JsonHandler, new_id, now, page_result, require, verify_token
from geodata_pipeline import (MAX_IMPORT_FEATURES, conflation_score, digest, geometry_bbox, geometry_centroid,
                              geometry_distance_meters,
                              normalize_geometry, source_manifest, validate_attachments)
from geodata_store import GeodataStore
from import_adapters import normalize
from import_formats import SUPPORTED_FORMATS, TEXT_FORMATS, parse_text, parse_uploaded
from location_catalog import build_location_tree, derive_location_codes
from reverse_geocoder import LOCATION_FIELDS, enrich_entity_location

GEODATA_IMPORT_BUCKET = os.environ.get("MYOTA_GEODATA_IMPORT_BUCKET", "myota-geodata-imports")


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
        code = str(item.get("code") if isinstance(item, dict) else item).strip().upper()
        if code and re.fullmatch(r"[A-Z][A-Z0-9_]{1,63}", code) and code not in result:
            result.append(code)
    return result


def entity_categories(entity: dict[str, Any]) -> list[str]:
    return entity_type_codes(entity.get("entityTypes") or entity.get("entityTypeCodes"), entity.get("entityType"))


class GeoHandler(JsonHandler):
    service = "geodata-service"
    store = GeodataStore("geodata", "GEO_DATABASE_URL")
    upload_executor = ThreadPoolExecutor(max_workers=max(1, int(os.environ.get("MYOTA_UPLOAD_WORKERS", "2"))),
                                          thread_name_prefix="geodata-upload")
    import_executor = ThreadPoolExecutor(max_workers=max(1, int(os.environ.get("MYOTA_IMPORT_WORKERS", "2"))),
                                          thread_name_prefix="geodata-import")
    deletion_executor = ThreadPoolExecutor(max_workers=max(1, int(os.environ.get("MYOTA_DELETION_WORKERS", "2"))),
                                            thread_name_prefix="geodata-deletion")

    @classmethod
    def metrics_extra(cls) -> dict[str, float]:
        """Return counts from the durable catalogue and import lifecycle."""
        try:
            cls.store.refresh_import_runs()
        except Exception:
            # The scrape must stay available while PostGIS is restarting.
            return {"myota_geodata_metrics_database_unavailable": 1}
        with cls.store.lock:
            entities = list(cls.store.items.values())
            runs = list((cls.store.data.get("importRuns") or {}).values())
            candidates = list((cls.store.data.get("importCandidates") or {}).values())
        result: dict[str, float] = {
            "myota_geodata_entities_total": float(len(entities)),
            "myota_geodata_entity_categories_total": float(len({code for entity in entities for code in entity_categories(entity)})),
            "myota_geodata_import_runs_total": float(len(runs)),
            "myota_geodata_import_candidates_total": float(len(candidates)),
        }
        for status in ("CANDIDATE", "APPROVED", "RETIRED", "REJECTED"):
            result[f'myota_geodata_entities_by_status_total{{status="{status}"}}'] = float(sum(str(entity.get("status", "")).upper() == status for entity in entities))
        geometry_types = ("Point", "LineString", "MultiLineString", "Polygon", "MultiPolygon")
        for geometry_type in geometry_types:
            result[f'myota_geodata_entities_by_geometry_total{{geometry_type="{geometry_type}"}}'] = float(sum((entity.get("geometry") or {}).get("type") == geometry_type for entity in entities))
        for status in ("QUEUED", "PROCESSING", "PREPROCESSED", "PREPROCESSED_WITH_ERRORS", "PROCESSED", "FAILED"):
            result[f'myota_geodata_import_runs_by_status_total{{status="{status}"}}'] = float(sum(str(run.get("status", "")).upper() == status for run in runs))
        for validation in ("PENDING", "CONFIRMED", "REJECTED", "PROCESSED"):
            result[f'myota_geodata_import_candidates_by_validation_total{{status="{validation}"}}'] = float(sum(str(candidate.get("validationStatus", "PENDING")).upper() == validation for candidate in candidates))
        queued = [run for run in runs if str(run.get("status", "")).upper() in {"QUEUED", "UPLOAD_PENDING"}]
        processing = [run for run in runs if str(run.get("status", "")).upper() == "PROCESSING"]
        result["myota_geodata_import_queue_depth"] = float(len(queued))
        result["myota_geodata_import_processing_runs"] = float(len(processing))
        result["myota_geodata_import_attempt_count_sum"] = float(sum(int(run.get("attemptCount") or 0) for run in runs))
        result["myota_geodata_import_features_preprocessed_sum"] = float(sum(
            int((run.get("stats") or {}).get("preprocessed", 0) or 0) for run in runs))
        result["myota_geodata_import_features_promoted_sum"] = float(sum(
            int((run.get("stats") or {}).get("created", 0) or 0) + int((run.get("stats") or {}).get("updated", 0) or 0)
            for run in runs))

        def seconds_since(value: Any) -> float | None:
            if not value:
                return None
            try:
                timestamp = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                timestamp = timestamp.replace(tzinfo=timezone.utc) if timestamp.tzinfo is None else timestamp
                return max(0.0, (datetime.now(timezone.utc) - timestamp).total_seconds())
            except (TypeError, ValueError):
                return None

        queue_ages = [age for run in queued if (age := seconds_since(run.get("queuedAt") or run.get("startedAt"))) is not None]
        heartbeat_ages = [age for run in processing if (age := seconds_since(run.get("heartbeatAt") or run.get("startedAt"))) is not None]
        result["myota_geodata_import_oldest_queued_age_seconds"] = max(queue_ages, default=0.0)
        result["myota_geodata_import_oldest_processing_heartbeat_age_seconds"] = max(heartbeat_ages, default=0.0)

        try:
            pool_stats = cls.store._ensure_pool().get_stats()
            result.update({
                "myota_geodata_postgres_pool_connections": float(pool_stats.get("pool_size", 0)),
                "myota_geodata_postgres_pool_available_connections": float(pool_stats.get("pool_available", 0)),
                "myota_geodata_postgres_pool_waiting_requests": float(pool_stats.get("requests_waiting", 0)),
                "myota_geodata_postgres_pool_wait_milliseconds_total": float(pool_stats.get("requests_wait_ms", 0)),
            })
            with cls.store.transaction() as connection:
                row = connection.execute(
                    "SELECT count(*) FILTER (WHERE state = 'active' AND pid <> pg_backend_pid()), "
                    "count(*) FILTER (WHERE wait_event_type = 'Lock'), "
                    "(SELECT setting::bigint FROM pg_settings WHERE name = 'max_connections') "
                    "FROM pg_stat_activity WHERE datname = current_database()"
                ).fetchone()
                result["myota_geodata_postgres_active_connections"] = float(row[0] or 0)
                result["myota_geodata_postgres_lock_waiting_connections"] = float(row[1] or 0)
                result["myota_geodata_postgres_max_connections"] = float(row[2] or 0)
                outbox = connection.execute(
                    "SELECT count(*), COALESCE(extract(epoch FROM now() - min(occurred_at)), 0) "
                    "FROM outbox_event WHERE published_at IS NULL"
                ).fetchone()
                result["myota_geodata_outbox_pending_events"] = float(outbox[0] or 0)
                result["myota_geodata_outbox_oldest_pending_age_seconds"] = float(outbox[1] or 0)
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
        scoped = [r for r in roles if "geodata.review" in r.get("scopes", []) or r.get("role") == "GLOBAL_OPERATOR"]
        for role in scoped:
            if role.get("programmeSlug") and role["programmeSlug"] != entity.get("programmeSlug"):
                continue
            if role.get("entityType") and str(role["entityType"]).upper() not in entity_categories(entity):
                continue
            if role.get("jurisdiction") and role["jurisdiction"] != entity.get("jurisdiction"):
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
    def _authorize_gis_admin(p: dict[str, str], entity: dict[str, Any], permission: str) -> None:
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
            if role.get("programmeSlug") and role["programmeSlug"] != entity.get("programmeSlug"):
                continue
            if role.get("jurisdiction") and role["jurisdiction"] != entity.get("jurisdiction"):
                continue
            return
        if permission in scopes:
            return
        raise PermissionError("global or GIS administrator access is required")

    @staticmethod
    def _query_bounds(query: dict[str, list[str]]) -> tuple[float, float, float, float] | None:
        keys = ("minLon", "minLat", "maxLon", "maxLat")
        if query.get("bbox") and not any(key in query for key in keys):
            values = [value.strip() for value in query["bbox"][0].split(",")]
            if len(values) != 4:
                raise ValueError("bbox must contain minLon,minLat,maxLon,maxLat")
            try:
                bounds = tuple(float(value) for value in values)
            except ValueError as exc:
                raise ValueError("bbox must contain numeric coordinates") from exc
            if bounds[0] >= bounds[2] or bounds[1] >= bounds[3]:
                raise ValueError("map bounds must have minimum values below maximum values")
            return bounds
        if not any(key in query for key in keys):
            return None
        try:
            bounds = tuple(float(query.get(key, [""])[0]) for key in keys)
        except ValueError as exc:
            raise ValueError("minLon, minLat, maxLon and maxLat must be numbers") from exc
        if bounds[0] >= bounds[2] or bounds[1] >= bounds[3]:
            raise ValueError("map bounds must have minimum values below maximum values")
        return bounds

    @staticmethod
    def list_entities(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        query = parse_qs(urlparse(p.get("_path", "")).query)
        programme = query.get("programme", [None])[0]
        statuses = {value.strip().upper() for raw in query.get("status", []) for value in raw.split(",") if value.strip()}
        bounds = GeoHandler._query_bounds(query)
        items = list(GeoHandler.store.items.values())
        if programme:
            items = [i for i in items if i["programmeSlug"] == programme]
        if statuses and "ALL" not in statuses:
            items = [i for i in items if str(i.get("status", "")).upper() in statuses]
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
                    items = [i for i in items if value.casefold() in {code.casefold() for code in entity_categories(i)}]
                else:
                    items = [i for i in items if str(i.get(entity_field) or (i.get("location") or {}).get(entity_field) or "").casefold() == value.casefold()]
        city = query.get("city", [None])[0]
        if city:
            target = city.casefold()
            items = [i for i in items if any(str(i.get(field) or (i.get("location") or {}).get(field) or "").casefold() == target for field in ("city", "municipality"))]
        if bounds:
            items = [i for i in items if i.get("geometry") and not (
                (lambda box: box[2] < bounds[0] or box[0] > bounds[2] or box[3] < bounds[1] or box[1] > bounds[3])(geometry_bbox(i["geometry"]))
            )]
        items.sort(key=lambda item: (str(item.get("name") or "").casefold(), str(item.get("id") or "")))
        return page_result(items, query)

    @staticmethod
    def adapters(_: JsonHandler, __: dict[str, str]) -> dict[str, Any]:
        return {"adapters": [
            {"code": "PARKSERVE_US", "formats": ["PARKSERVE_US", "GEOJSON", "KML", "GPX"], "requires": ["license", "retrievedAt", "sourceRef"]},
            {"code": "OSM", "formats": ["OSM_PBF", "GEOJSON", "KML", "GPX"], "requiredTags": ["leisure=park", "leisure=nature_reserve", "boundary=protected_area", "landuse=recreation_ground", "highway=path", "highway=footway", "highway=track", "highway=bridleway", "route=hiking"], "attribution": "© OpenStreetMap contributors"},
            {"code": "GOVERNMENT_GIS", "formats": ["WFS", "GEOJSON", "KML", "GPX", "SHAPEFILE", "SHP", "ARCGIS_FEATURESERVER"], "requires": ["license", "attribution", "sourceFormat"]},
            {"code": "MANUAL", "formats": ["GEOJSON", "KML", "GPX", "SHAPEFILE", "SHP"], "requires": ["feature", "entityTypes"]}
        ]}

    @staticmethod
    def location_options(_: JsonHandler, __: dict[str, str]) -> dict[str, Any]:
        return build_location_tree(GeoHandler.store.items.values())

    @staticmethod
    def get_entity(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        return GeoHandler.store.items[p["entityId"]]

    @staticmethod
    def audit_entity(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        entity = GeoHandler.store.items[p["entityId"]]
        return {"entityId": entity["id"], "status": entity["status"],
                "reviewHistory": entity.get("reviewHistory", []),
                "geometryHistory": entity.get("geometryHistory", []),
                "events": [event for event in GeoHandler.store.events
                           if event.get("aggregate", {}).get("id") == entity["id"]]}

    @staticmethod
    def _source_key(source: dict[str, Any]) -> str:
        return str(source.get("sourceKey") or source.get("url") or source.get("name") or "unknown-source")

    @staticmethod
    def _create_conflation_candidates(entity: dict[str, Any]) -> list[dict[str, Any]]:
        candidates = GeoHandler.store.data.setdefault("conflationCandidates", {})
        created = []
        for existing in GeoHandler.store.items.values():
            if existing["id"] == entity["id"] or existing.get("programmeSlug") != entity.get("programmeSlug"):
                continue
            if entity.get("sourceRef") and entity.get("sourceRef") == existing.get("sourceRef"):
                continue
            comparison = conflation_score(entity, existing) if entity.get("geometry") and existing.get("geometry") else {"score": 0.0, "signals": {}}
            if comparison["score"] < 0.55:
                continue
            pair = sorted([entity["id"], existing["id"]])
            candidate_id = digest(pair)[:32]
            if candidate_id in candidates:
                continue
            candidate = {"id": candidate_id, "programmeSlug": entity["programmeSlug"], "leftEntityId": pair[0], "rightEntityId": pair[1],
                         "score": comparison["score"], "signals": comparison["signals"], "resolution": "OPEN",
                         "resolutionHistory": [], "createdAt": now(), "updatedAt": now()}
            candidates[candidate_id] = candidate
            created.append(candidate)
        return created

    @staticmethod
    def _apply_disappearance(programme: str, source_key: str, adapter: str, seen_refs: set[str], policy: str) -> list[str]:
        if policy not in {"UNCHANGED", "STALE", "RETIRED", "REVIEW_REQUIRED"}:
            raise ValueError("disappearancePolicy must be UNCHANGED, STALE, RETIRED, or REVIEW_REQUIRED")
        changed = []
        for entity in GeoHandler.store.items.values():
            provenance = entity.get("provenance") or {}
            if entity.get("programmeSlug") != programme or provenance.get("adapter") != adapter or provenance.get("sourceKey") != source_key:
                continue
            if not entity.get("sourceRef") or entity["sourceRef"] in seen_refs:
                continue
            if policy == "UNCHANGED":
                continue
            occurred_at = now()
            entity["sourceState"] = "STALE" if policy == "STALE" else "REVIEW_REQUIRED" if policy == "REVIEW_REQUIRED" else "RETIRED"
            if policy == "RETIRED" and entity["status"] != "RETIRED":
                previous = entity["status"]
                entity["status"] = "RETIRED"
            else:
                previous = entity["status"]
            entity.setdefault("reviewHistory", []).append({"action": "SOURCE_DISAPPEARED", "policy": policy,
                                                             "previousStatus": previous, "occurredAt": occurred_at})
            entity["updatedAt"] = occurred_at
            changed.append(entity["id"])
            GeoHandler.store.event("geodata.entity.source-disappeared.v1", "entity", entity["id"],
                                   {"entityId": entity["id"], "policy": policy, "previousStatus": previous})
        return changed

    @staticmethod
    def _preserve_manual_location(existing: dict[str, Any] | None, entity: dict[str, Any]) -> None:
        if not existing:
            entity["manualLocationFields"] = []
            return
        existing_location = existing.get("location") or {}
        for field in LOCATION_FIELDS:
            if field in existing:
                entity[field] = existing[field]
            elif field in existing_location:
                entity[field] = existing_location[field]
        manual_fields = set(existing.get("manualLocationFields") or []) & set(LOCATION_FIELDS)
        entity["manualLocationFields"] = sorted(manual_fields)
        for field in ("geocodeProvider", "geocodeStatus", "geocodeLookupSource", "geocodeError", "geocodedAt"):
            if field in existing:
                entity[field] = existing[field]
        previous_geocoding = (existing.get("provenance") or {}).get("reverseGeocoding")
        if previous_geocoding:
            entity.setdefault("provenance", {})["reverseGeocoding"] = previous_geocoding

    @staticmethod
    def _possible_duplicates(entity: dict[str, Any]) -> list[dict[str, Any]]:
        """Find existing entities whose geometry is identical or under 50 m away."""
        geometry = entity.get("geometry")
        if not geometry:
            return []
        matches = []
        candidate_digest = digest(geometry)
        with GeoHandler.store.lock:
            existing_entities = list(GeoHandler.store.items.values())
        for existing in existing_entities:
            if existing.get("id") == entity.get("id") or not existing.get("geometry"):
                continue
            distance = geometry_distance_meters(geometry, existing["geometry"])
            if distance >= 50:
                continue
            existing_geometry = existing["geometry"]
            matches.append({
                "entityId": existing.get("id"),
                "name": existing.get("name") or "Unnamed entity",
                "status": existing.get("status"),
                "entityTypes": entity_categories(existing),
                "geometry": existing_geometry,
                "centroid": existing.get("centroid") or geometry_centroid(existing_geometry),
                "distanceMeters": round(distance, 2),
                "matchType": "IDENTICAL_GEOMETRY" if digest(existing_geometry) == candidate_digest else "WITHIN_50_METERS",
            })
        return sorted(matches, key=lambda match: (match["distanceMeters"], match["name"]))

    @staticmethod
    def _import_features(body: dict[str, Any], run_id: str) -> dict[str, Any]:
        if len(body["features"]) > MAX_IMPORT_FEATURES:
            raise ValueError(f"an import may contain at most {MAX_IMPORT_FEATURES} features")
        adapter = body["adapter"]
        programme_slug = body.get("programmeSlug") or None
        source = dict(body["source"])
        if adapter == "OSM":
            source.setdefault("attribution", "© OpenStreetMap contributors")
        source_key = GeoHandler._source_key(source)
        records = []
        preprocessed, skipped, errors = [], [], []
        with GeoHandler.store.lock:
            import_candidates = GeoHandler.store.data.setdefault("importCandidates", {})
        for index, raw_feature in enumerate(body["features"]):
            try:
                feature = normalize(adapter, raw_feature)
                props = feature.get("properties", {})
                source_ref = str(props.get("sourceRef") or props.get("id") or f"{source_key}:record-{index}")
                if props.get("skipReason") == "FILTERED_TAG":
                    skipped.append({"sourceRef": source_ref, "reason": props["skipReason"]})
                    continue
                if not feature.get("geometry"):
                    skipped.append({"sourceRef": source_ref, "reason": "MISSING_GEOMETRY"})
                    continue
                source_crs = raw_feature.get("crs") or raw_feature.get("spatialReference") or props.get("crs")
                # Adapters normalize and reproject source geometry at the intake
                # boundary. Validate the normalized WGS84 value here without
                # applying the source transformation a second time.
                geometry = normalize_geometry(feature["geometry"])
                attachments = validate_attachments(raw_feature.get("attachments") or props.get("attachments"))
                with GeoHandler.store.lock:
                    existing = next((item for item in GeoHandler.store.items.values()
                                     if source_ref and item.get("sourceRef") == source_ref and item.get("programmeSlug") == programme_slug), None)
                occurred_at = now()
                default_entity_type = "TRAIL" if str(props.get("featureType") or "").casefold() == "way" or geometry.get("type") == "LineString" else "MUNICIPAL_PARK"
                categories = entity_type_codes(props.get("entityTypes") or props.get("entityType"), default_entity_type) or [default_entity_type]
                candidate_source = props.get("candidateSource") or {
                    "type": "ADAPTER_IMPORT", "adapter": adapter, "importRunId": run_id,
                    "sourceKey": source_key, "sourceRef": source_ref,
                }
                entity = {"id": existing["id"] if existing else new_id(), "programmeSlug": programme_slug,
                          "entityType": categories[0], "entityTypes": categories, "entityTypeCodes": categories,
                          "name": props.get("name", "Unnamed candidate"),
                          "status": existing["status"] if existing else "CANDIDATE", "sourceState": "CURRENT", "geometry": geometry,
                          "centroid": geometry_centroid(geometry), "jurisdiction": props.get("jurisdiction"), "sourceRef": source_ref,
                          "attachments": attachments, "candidateSource": candidate_source,
                          "provenance": {"adapter": adapter, "source": source, "sourceKey": source_key,
                                         "sourceFeature": raw_feature, "sourceCrs": source_crs, "importRunId": run_id, "sourceHash": digest(raw_feature),
                                         "license": source.get("license"), "attribution": source.get("attribution"),
                                         "retrievedAt": source.get("retrievedAt", occurred_at)},
                          "review": existing.get("review") if existing else None,
                          "reviewHistory": existing.get("reviewHistory", []) if existing else [],
                          "geometryHistory": existing.get("geometryHistory", []) if existing else [],
                          "createdAt": existing.get("createdAt", occurred_at) if existing else occurred_at, "updatedAt": occurred_at}
                GeoHandler._preserve_manual_location(existing, entity)
                enrich_entity_location(entity)
                possible_duplicates = GeoHandler._possible_duplicates(entity)
                candidate_id = new_id()
                candidate = {"id": candidate_id, "importRunId": run_id, "ordinal": index,
                             "existingEntityId": existing["id"] if existing else None,
                             "candidateSource": candidate_source, "validationStatus": "PENDING",
                             "dedupeWarning": "POSSIBLE_DUPLICATE" if possible_duplicates else None,
                             "possibleDuplicates": possible_duplicates,
                             "targetStatus": None, "processedEntityId": None, "processedAt": None,
                             "entity": entity}
                with GeoHandler.store.lock:
                    import_candidates[candidate_id] = candidate
                    GeoHandler.store.mark_import_candidate_dirty(candidate_id)
                preprocessed.append(candidate_id)
                records.append({"sourceRef": source_ref, "sourceHash": entity["provenance"]["sourceHash"]})
            except Exception as error:
                # A malformed or otherwise unprocessable feature must not
                # invalidate the rest of the dataset. Keep the record-level
                # error in the run summary and continue staging independent
                # features for administrator validation. Database/source
                # failures outside this loop still fail the whole run.
                errors.append({"index": index, "message": str(error)})
        seen_refs = {record["sourceRef"] for record in records}
        disappeared = GeoHandler._apply_disappearance(programme_slug, source_key, adapter, seen_refs,
                                                       body.get("disappearancePolicy", "REVIEW_REQUIRED")) if body.get("completeSnapshot") else []
        manifest = source_manifest(adapter, source, records, run_id)
        manifest["sourceKey"] = source_key
        # Imports run concurrently in the executor. Keep the manifest lookup
        # and insert atomic so another worker cannot resize the dictionary
        # while this worker iterates its values (RuntimeError: dictionary
        # changed size during iteration).
        with GeoHandler.store.lock:
            source_manifests = GeoHandler.store.data.setdefault("sourceManifests", {})
            manifest["sourceChanged"] = not any(
                item.get("sourceHash") == manifest["sourceHash"]
                for item in list(source_manifests.values())
                if item.get("sourceKey") == source_key
            )
            source_manifests[run_id] = manifest
        result = {"importRunId": run_id, "adapter": adapter, "preprocessed": preprocessed, "created": [], "updated": [], "skipped": skipped,
                  "errors": errors, "disappeared": disappeared, "conflationCandidates": [],
                  "manifest": manifest, "_status": 202}
        return result

    @staticmethod
    def _candidate_view(candidate: dict[str, Any]) -> dict[str, Any]:
        entity = candidate.get("entity") or {}
        return {"id": candidate["id"], "importRunId": candidate["importRunId"], "ordinal": candidate.get("ordinal"),
                "existingEntityId": candidate.get("existingEntityId"), "name": entity.get("name"),
                "entityTypes": entity.get("entityTypes") or [], "geometry": entity.get("geometry"),
                "centroid": entity.get("centroid"), "sourceRef": entity.get("sourceRef"),
                "candidateSource": candidate.get("candidateSource") or {}, "validationStatus": candidate.get("validationStatus", "PENDING"),
                "dedupeWarning": candidate.get("dedupeWarning"),
                "possibleDuplicates": candidate.get("possibleDuplicates") or [],
                "validationNote": candidate.get("validationNote"), "validatedBy": candidate.get("validatedBy"),
                "validatedAt": candidate.get("validatedAt"), "targetStatus": candidate.get("targetStatus"),
                "processedEntityId": candidate.get("processedEntityId"), "processedAt": candidate.get("processedAt"),
                "location": {field: entity.get(field) for field in LOCATION_FIELDS}}

    @staticmethod
    def _materialize_import_candidate(candidate: dict[str, Any], target_status: str, actor: str, note: str | None) -> dict[str, Any]:
        entity = {**(candidate.get("entity") or {})}
        entity_id = entity["id"]
        existing = GeoHandler.store.items.get(entity_id)
        if existing and existing.get("status") == "APPROVED" and target_status != "RETIRED":
            raise ValueError("an approved entity cannot be changed by import processing")
        entity["status"] = target_status
        entity["updatedAt"] = now()
        if target_status == "APPROVED":
            previous = existing.get("status") if existing else "CANDIDATE"
            entity["review"] = {"reviewerId": actor, "reviewedAt": entity["updatedAt"], "note": note or "Approved from validated import"}
            entity.setdefault("reviewHistory", []).append({"action": "APPROVED", "reviewerId": actor,
                                                               "note": note or "Approved from validated import",
                                                               "occurredAt": entity["updatedAt"], "previousStatus": previous})
        GeoHandler.store.items[entity_id] = entity
        GeoHandler._create_conflation_candidates(entity)
        if existing:
            GeoHandler.store.event("geodata.entity.import-updated.v1", "entity", entity_id,
                                   {"entityId": entity_id, "status": target_status, "importRunId": candidate["importRunId"], "actor": actor})
        else:
            event_type = "geodata.entity.candidate.created.v1" if target_status == "CANDIDATE" else "geodata.entity.import-approved.v1"
            GeoHandler.store.event(event_type, "entity", entity_id,
                                   {"entityId": entity_id, "status": target_status, "importRunId": candidate["importRunId"], "actor": actor})
        candidate["validationStatus"] = "PROCESSED"
        candidate["targetStatus"] = target_status
        candidate["processedEntityId"] = entity_id
        candidate["processedAt"] = entity["updatedAt"]
        candidate["entity"] = entity
        return entity

    @staticmethod
    def _prepare_import_body(body: dict[str, Any], features: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        body = {**body}
        require(body, "adapter", "source")
        categories = entity_type_codes(body.get("entityTypes") or body.get("entityTypeCodes"), body.get("entityType"))
        if not categories:
            raise ValueError("entityTypes must contain at least one shared entity category code")
        body["entityTypes"] = categories
        body["entityTypeCodes"] = categories
        body["entityType"] = categories[0]
        if body["adapter"] not in ("PARKSERVE_US", "OSM", "GOVERNMENT_GIS", "MANUAL"):
            raise ValueError("unsupported adapter")
        if body.get("format", "GEOJSON").upper() not in SUPPORTED_FORMATS:
            raise ValueError("unsupported import format")
        for feature in features or []:
            feature["properties"] = {**(feature.get("properties") or {}), "entityType": body["entityType"], "entityTypes": categories, "entityTypeCodes": categories}
        return body

    @staticmethod
    def _create_import_run(body: dict[str, Any], filename: str | None, run_id: str | None = None) -> tuple[str, dict[str, Any]]:
        run_id = run_id or new_id()
        categories = entity_type_codes(body.get("entityTypes") or body.get("entityTypeCodes"), body.get("entityType"))
        record = {"id": run_id, "programmeSlug": body.get("programmeSlug"),
            "adapter": body["adapter"], "format": body.get("format", "GEOJSON").upper(), "entityType": body["entityType"], "entityTypes": categories,
            "source": body["source"], "filename": filename, "status": "QUEUED", "queuedAt": now(), "featureCount": len(body.get("features") or []) or None}
        with GeoHandler.store.lock:
            GeoHandler.store.data.setdefault("importRuns", {})[run_id] = record
            GeoHandler.store.event("geodata.import.queued.v1", "import_run", run_id, {"importRunId": run_id,
                "programmeSlug": body.get("programmeSlug"), "adapter": body["adapter"], "format": body.get("format", "GEOJSON").upper(),
                "entityType": body["entityType"], "entityTypes": categories, "filename": filename})
        return run_id, record

    @staticmethod
    def _complete_import_run(run_id: str, result: dict[str, Any]) -> dict[str, Any]:
        with GeoHandler.store.lock:
            run = GeoHandler.store.data.setdefault("importRuns", {})[run_id]
            run.update({"status": "COMPLETED" if not result.get("errors") else "COMPLETED_WITH_ERRORS", "completedAt": now(),
                        "stats": {key: len(result.get(key, [])) for key in ("preprocessed", "created", "updated", "skipped", "errors", "disappeared")},
                        "errors": result.get("errors", []), "manifest": result.get("manifest"),
                        "conflationCandidateCount": len(result.get("conflationCandidates", [])),
                        "heartbeatAt": None, "leaseUntil": None, "lastError": None})
            run["status"] = "PREPROCESSED" if not result.get("errors") else "PREPROCESSED_WITH_ERRORS"
            GeoHandler.store.event("geodata.import.preprocessed.v1", "import_run", run_id, result)
        return run

    @staticmethod
    def _fail_import_run(run_id: str, error: Exception) -> dict[str, Any]:
        with GeoHandler.store.lock:
            run = GeoHandler.store.data.setdefault("importRuns", {})[run_id]
            run.update({"status": "FAILED", "completedAt": now(), "errors": [{"message": str(error)}],
                        "stats": {"preprocessed": 0, "created": 0, "updated": 0, "skipped": 0, "errors": 1, "disappeared": 0},
                        "heartbeatAt": None, "leaseUntil": None, "lastError": str(error)})
            GeoHandler.store.event("geodata.import.failed.v1", "import_run", run_id, {"importRunId": run_id, "error": str(error)})
        return run

    @staticmethod
    def _finish_uploaded_import(run_id: str, body: dict[str, Any], upload_path: str,
                                object_key: str, bucket: str, format_code: str,
                                filename: str, scan: dict[str, Any]) -> None:
        """Finish durable storage after the upload HTTP request has returned."""
        try:
            from storage import ObjectStore
            stored = ObjectStore().put_file(bucket, object_key, upload_path, "application/octet-stream",
                                            sha256=str(scan["sha256"]), size=int(scan["size"]))
            source = {**(body.get("source") or {}), "objectKey": object_key, "bucket": bucket,
                      "sha256": stored["sha256"], "scan": scan}
            with GeoHandler.store.lock:
                run = GeoHandler.store.data.setdefault("importRuns", {})[run_id]
                run.update({"source": source, "status": "QUEUED", "uploadSpoolPath": None,
                            "uploadCompletedAt": now(), "lastError": None})
                GeoHandler.store.persist(include_import_state=True)

            if format_code in TEXT_FORMATS or format_code in {"WFS", "ARCGIS_FEATURESERVER", "SHAPEFILE", "SHP"}:
                prepared = GeoHandler._prepare_import_body({**body, "format": format_code, "source": source})
                loader = lambda: parse_uploaded(
                    format_code, ObjectStore().get(bucket, object_key) or b"", filename)
                GeoHandler.import_executor.submit(GeoHandler._process_import_run, run_id, prepared, loader)
                return

            if format_code in {"OSM_PBF", "PARKSERVE_US"}:
                with GeoHandler.store.lock:
                    run = GeoHandler.store.data["importRuns"][run_id]
                    run.update({"binaryObjectPending": True, "status": "QUEUED"})
                    GeoHandler.store.persist(include_import_state=True)
                return

            raise ValueError(f"unsupported upload format {format_code}")
        except Exception as error:
            with GeoHandler.store.lock:
                GeoHandler._fail_import_run(run_id, error)
                GeoHandler.store.persist(include_import_state=True)
        finally:
            Path(upload_path).unlink(missing_ok=True)

    @staticmethod
    def _resume_pending_upload(run_id: str) -> None:
        """Resume a spooled upload after a geodata service restart."""
        run = GeoHandler.store.data.setdefault("importRuns", {}).get(run_id)
        if not run:
            return
        upload_path = run.get("uploadSpoolPath")
        if not upload_path or not Path(upload_path).is_file():
            with GeoHandler.store.lock:
                GeoHandler._fail_import_run(run_id, RuntimeError("upload spool file is missing after restart"))
                GeoHandler.store.persist(include_import_state=True)
            return
        source = run.get("source") or {}
        filename = run.get("filename") or "upload"
        scan = source.get("scan") or {}
        if not scan.get("sha256") or not scan.get("size"):
            from storage import ObjectStore
            scan = ObjectStore.scan_path(upload_path, filename)
        bucket = source.get("bucket") or GEODATA_IMPORT_BUCKET
        object_key = source.get("objectKey") or f"geodata-imports/{new_id()}-{filename.replace('/', '_')}"
        body = {"adapter": run.get("adapter") or "MANUAL", "format": run.get("format") or "GEOJSON",
                "source": source, "filename": filename, "programmeSlug": run.get("programmeSlug"),
                "entityType": run.get("entityType"), "entityTypes": run.get("entityTypes") or []}
        GeoHandler._finish_uploaded_import(run_id, body, upload_path, object_key, bucket,
                                            str(body["format"]).upper(), filename, scan)

    @staticmethod
    def _store_import_source(run_id: str, body: dict[str, Any], content: bytes, filename: str | None) -> dict[str, Any]:
        """Store a recovery source for pasted/manual imports in object storage."""
        from storage import ObjectStore

        safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", filename or "import.geojson")
        bucket = GEODATA_IMPORT_BUCKET
        object_key = f"geodata-import-sources/{run_id}-{safe_name}"
        stored = ObjectStore().put(bucket, object_key, content, "application/octet-stream")
        return {**(body.get("source") or {}), "bucket": bucket, "objectKey": object_key,
                "sha256": stored["sha256"], "size": stored["size"], "recoverySource": True,
                # The queued source is a normalized FeatureCollection even
                # when the original pasted document was KML or GPX. Keep the
                # parser format explicit so a restart can replay it safely.
                "recoveryFormat": "GEOJSON"}

    @staticmethod
    def _claim_import_run(run_id: str) -> bool:
        with GeoHandler.store.lock:
            run = GeoHandler.store.data.setdefault("importRuns", {}).get(run_id)
            if not run:
                return False
            attempt = int(run.get("attemptCount") or 0) + 1
        if GeoHandler.store.durable:
            lease_seconds = max(60, int(os.environ.get("MYOTA_IMPORT_LEASE_SECONDS", "900")))
            with GeoHandler.store.transaction() as connection:
                claimed = connection.execute(
                    "UPDATE import_run SET status='PROCESSING', attempt_count=attempt_count + 1, "
                    "heartbeat_at=now(), lease_until=now() + make_interval(secs => %s), last_error=NULL "
                    "WHERE id=%s AND status IN ('QUEUED', 'PROCESSING') "
                    "AND (status='QUEUED' OR lease_until IS NULL OR lease_until <= now()) RETURNING id",
                    (lease_seconds, run_id),
                ).fetchone()
            if not claimed:
                return False
            with GeoHandler.store.lock:
                run = GeoHandler.store.data.setdefault("importRuns", {}).get(run_id)
                if not run:
                    return False
                run.update({"status": "PROCESSING", "startedAt": run.get("startedAt") or now(), "attemptCount": attempt,
                            "heartbeatAt": now(), "lastError": None})
                run["leaseUntil"] = (datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)).isoformat().replace("+00:00", "Z")
        else:
            with GeoHandler.store.lock:
                run.update({"status": "PROCESSING", "startedAt": run.get("startedAt") or now(), "attemptCount": attempt,
                            "heartbeatAt": now(), "lastError": None})
        return True

    @staticmethod
    def _heartbeat_import_run(run_id: str, stop: threading.Event) -> None:
        interval = max(15, int(os.environ.get("MYOTA_IMPORT_HEARTBEAT_SECONDS", "30")))
        lease_seconds = max(60, int(os.environ.get("MYOTA_IMPORT_LEASE_SECONDS", "900")))
        while not stop.wait(interval):
            try:
                if GeoHandler.store.durable:
                    with GeoHandler.store.transaction() as connection:
                        connection.execute(
                            "UPDATE import_run SET heartbeat_at=now(), lease_until=now() + make_interval(secs => %s) "
                            "WHERE id=%s AND status='PROCESSING'", (lease_seconds, run_id))
                with GeoHandler.store.lock:
                    run = GeoHandler.store.data.setdefault("importRuns", {}).get(run_id)
                    if run:
                        run["heartbeatAt"] = now()
            except Exception:
                # A heartbeat failure must not hide the original import error.
                continue

    @staticmethod
    def _process_import_run(run_id: str, body: dict[str, Any], loader: Any) -> None:
        if not GeoHandler._claim_import_run(run_id):
            return
        stop = threading.Event()
        heartbeat = threading.Thread(target=GeoHandler._heartbeat_import_run, args=(run_id, stop),
                                     name=f"geodata-import-heartbeat-{run_id[:8]}", daemon=True)
        heartbeat.start()
        try:
            with GeoHandler.store.lock:
                GeoHandler.store.persist(include_import_state=True)
            features = loader()
            prepared = GeoHandler._prepare_import_body(body, features)
            with GeoHandler.store.lock:
                GeoHandler.store.data["importRuns"][run_id]["featureCount"] = len(features)
            result = GeoHandler._import_features({**prepared, "features": features}, run_id)
            with GeoHandler.store.lock:
                GeoHandler._complete_import_run(run_id, result)
                GeoHandler.store.persist(include_import_state=True)
        except Exception as error:  # imports must report failure in the run, not fail the HTTP request
            with GeoHandler.store.lock:
                GeoHandler._fail_import_run(run_id, error)
                GeoHandler.store.persist(include_import_state=True)
        finally:
            stop.set()
            heartbeat.join(timeout=2)

    @staticmethod
    def _recover_import_run(run_id: str) -> None:
        """Resume a queued/stale run from its immutable object-storage source."""
        run = GeoHandler.store.data.setdefault("importRuns", {}).get(run_id)
        if not run:
            return
        if run.get("binaryObjectPending") or str(run.get("format") or "").upper() in {"OSM_PBF", "PARKSERVE_US"}:
            # OSM PBF and ParkServe binary uploads are durable, but their
            # parser is supplied by a separate adapter. Keep them queued for
            # that worker instead of converting a restart into a false error.
            with GeoHandler.store.lock:
                run["lastError"] = "binary import is queued for an available parser adapter"
                GeoHandler.store.persist(include_import_state=True)
            return
        source = run.get("source") or {}
        bucket, object_key = source.get("bucket"), source.get("objectKey")
        if not bucket or not object_key:
            with GeoHandler.store.lock:
                GeoHandler._fail_import_run(run_id, RuntimeError("import source is not recoverable; no object-storage source was recorded"))
                GeoHandler.store.persist(include_import_state=True)
            return
        from storage import ObjectStore
        content = ObjectStore().get(bucket, object_key)
        if content is None:
            with GeoHandler.store.lock:
                GeoHandler._fail_import_run(run_id, RuntimeError("import source is not recoverable; object is missing from storage"))
                GeoHandler.store.persist(include_import_state=True)
            return
        format_code = str(source.get("recoveryFormat") or run.get("format") or "GEOJSON").upper()
        filename = run.get("filename") or "import.geojson"
        body = {"adapter": run.get("adapter") or "MANUAL", "format": format_code,
                "source": source, "filename": filename, "programmeSlug": run.get("programmeSlug"),
                "entityType": run.get("entityType"), "entityTypes": run.get("entityTypes") or []}
        try:
            loader = lambda: parse_uploaded(format_code, content, filename)
            GeoHandler._process_import_run(run_id, body, loader)
        except Exception as error:
            with GeoHandler.store.lock:
                GeoHandler._fail_import_run(run_id, error)
                GeoHandler.store.persist(include_import_state=True)

    @staticmethod
    def recover_import_runs() -> None:
        """Requeue durable jobs abandoned by the previous service instance.

        This is called once after service hydration during startup. A service
        restart means no in-process importer from the previous instance can
        still be running, so PROCESSING rows are requeued immediately instead
        of waiting for the normal lease timeout. The claim/update in
        _claim_import_run still prevents duplicate execution after the
        recovery queue is submitted.
        """
        if not GeoHandler.store.durable:
            return
        with GeoHandler.store.transaction() as connection:
            rows = connection.execute(
                "SELECT id::text, status, lease_until FROM import_run "
                "WHERE status IN ('UPLOAD_PENDING', 'QUEUED', 'PROCESSING')"
            ).fetchall()
        recovered_ids = []
        with GeoHandler.store.lock:
            for run_id, status, _lease_until in rows:
                run = GeoHandler.store.data.setdefault("importRuns", {}).setdefault(run_id, {"id": run_id})
                if status == "PROCESSING":
                    run.update({"status": "QUEUED", "heartbeatAt": None, "leaseUntil": None,
                                "lastError": "Previous geodata service instance stopped; run was recovered"})
                recovered_ids.append((run_id, status))
            if recovered_ids:
                # Persist the requeue before submitting any background work.
                # This makes a second restart during startup recover the same
                # durable state rather than leaving PROCESSING rows behind.
                GeoHandler.store.persist(include_import_state=True)
        for run_id, status in recovered_ids:
            executor = GeoHandler.upload_executor if status == "UPLOAD_PENDING" else GeoHandler.import_executor
            callback = GeoHandler._resume_pending_upload if status == "UPLOAD_PENDING" else GeoHandler._recover_import_run
            executor.submit(callback, run_id)

    @staticmethod
    def _queue_import(body: dict[str, Any], p: dict[str, str], filename: str | None,
                      loader: Any, source_content: bytes | None = None) -> dict[str, Any]:
        run_id, record = GeoHandler._create_import_run(body, filename)
        if source_content is not None and not (body.get("source") or {}).get("objectKey") and GeoHandler.store.durable:
            source = GeoHandler._store_import_source(run_id, body, source_content, filename)
            body = {**body, "source": source}
            record["source"] = source

        # Persist the QUEUED record before the worker can finish, preventing a
        # fast worker from being overwritten by the request handler's final save.
        if p.get("_http"):
            GeoHandler.store.persist(include_import_state=True)
        GeoHandler.import_executor.submit(GeoHandler._process_import_run, run_id, body, loader)
        return {**record, "status": "QUEUED", "queued": True, "_status": 202}

    @staticmethod
    def _start_import(body: dict[str, Any], features: list[dict[str, Any]], p: dict[str, str], filename: str | None = None) -> dict[str, Any]:
        body = GeoHandler._prepare_import_body({**body, "features": features}, features)
        if p.get("_http"):
            source_content = json.dumps({"type": "FeatureCollection", "features": features}, separators=(",", ":")).encode("utf-8")
            return GeoHandler._queue_import(body, p, filename, lambda: features, source_content)
        run_id, _ = GeoHandler._create_import_run(body, filename)
        result = GeoHandler._import_features(body, run_id)
        run = GeoHandler._complete_import_run(run_id, result)
        if body.get("completeSnapshot") or body.get("autoProcess"):
            candidate_ids = result.get("preprocessed", [])
            if candidate_ids:
                GeoHandler.validate_import_candidates(None, {"runId": run_id, "_body": {"candidateIds": candidate_ids, "reviewerId": body.get("processorId") or "scheduled-import"}})
                queue = GeoHandler.process_import_candidates(None, {"runId": run_id, "_body": {"candidateIds": candidate_ids, "targetStatus": "CANDIDATE", "processorId": body.get("processorId") or "scheduled-import"}})
                result = {**result, "created": queue.get("result", {}).get("created", []), "updated": queue.get("result", {}).get("updated", []),
                          "processingQueueId": queue["id"]}
            run = GeoHandler.store.data["importRuns"][run_id]
        return {**result, "status": run["status"], "queued": True}

    @staticmethod
    def enqueue_import(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        GeoHandler._authorize_import(p)
        body = {**p["_body"]}
        if any(key in body for key in ("_uploadBytes", "_uploadPath", "contentBase64")) or (body.get("source") or {}).get("objectKey"):
            return GeoHandler.upload_import(_, p)
        require(body, "adapter", "source")
        if not entity_type_codes(body.get("entityTypes") or body.get("entityTypeCodes"), body.get("entityType")):
            raise ValueError("entityTypes must contain at least one shared entity category code")
        format_code = str(body.get("format", "GEOJSON")).upper()
        if "features" in body:
            if not isinstance(body["features"], list): raise ValueError("features must be a list")
            return GeoHandler.store.once(p.get("Idempotency-Key"), lambda: GeoHandler._start_import(body, body["features"], p, body.get("filename")))
        if "content" not in body or not isinstance(body["content"], str):
            raise ValueError("content must contain copied text or features must be a list")
        content = body["content"]
        if not content.strip():
            raise ValueError("content must not be empty")
        if not p.get("_http"):
            return GeoHandler.store.once(p.get("Idempotency-Key"), lambda: GeoHandler._start_import(body, parse_text(format_code, content), p, body.get("filename")))
        prepared = GeoHandler._prepare_import_body({**body, "format": format_code})
        return GeoHandler.store.once(p.get("Idempotency-Key"), lambda: GeoHandler._queue_import(
            prepared, p, body.get("filename"), lambda: parse_text(format_code, content), content.encode("utf-8")))

    @staticmethod
    def upload_import(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        GeoHandler._authorize_import(p)
        body = {**p["_body"]}
        upload_bytes = body.pop("_uploadBytes", None)
        upload_path = body.pop("_uploadPath", None)
        upload_filename = body.pop("_uploadFilename", None)
        if upload_bytes is not None or upload_path is not None:
            body["filename"] = upload_filename or body.get("filename") or "upload"
        require(body, "adapter", "source", "filename")
        categories = entity_type_codes(body.get("entityTypes") or body.get("entityTypeCodes"), body.get("entityType"))
        if not categories:
            raise ValueError("entityTypes must contain at least one shared entity category code")
        body["entityTypes"], body["entityType"] = categories, categories[0]
        if upload_bytes is not None:
            content = upload_bytes
        elif upload_path is not None:
            content = None
        elif (body.get("source") or {}).get("objectKey"):
            from storage import ObjectStore
            object_source = body["source"]
            content = ObjectStore().get(object_source.get("bucket", GEODATA_IMPORT_BUCKET), object_source["objectKey"])
            if content is None:
                raise ValueError("source object was not found")
        else:
            require(body, "contentBase64")
            import base64
            try: content = base64.b64decode(body["contentBase64"], validate=True)
            except Exception as exc: raise ValueError("contentBase64 must be valid base64") from exc
        format_code = str(body.get("format") or body["filename"].rsplit(".", 1)[-1]).upper()
        if format_code == "JSON": format_code = "GEOJSON"
        if format_code == "SHP": format_code = "SHAPEFILE"
        defer_upload_cleanup = False
        try:
            from storage import ObjectStore
            scan = ObjectStore.scan_path(upload_path, body["filename"]) if upload_path else ObjectStore.scan_content(content, body["filename"])
            bucket = GEODATA_IMPORT_BUCKET
            if upload_path:
                # Return after the upload has been spooled and scanned. The
                # durable object-storage handoff continues in the background
                # so a large browser request is not held open by SeaweedFS.
                run_id, record = GeoHandler._create_import_run({**body, "format": format_code}, body["filename"])
                object_key = f"geodata-imports/{run_id}-{body['filename'].replace('/', '_')}"
                source = {**body["source"], "bucket": bucket, "objectKey": object_key,
                          "sha256": scan["sha256"], "scan": scan}
                record.update({"status": "UPLOAD_PENDING", "source": source, "uploadSpoolPath": upload_path})
                with GeoHandler.store.lock:
                    GeoHandler.store.data.setdefault("importRuns", {})[run_id].update(record)
                    GeoHandler.store.persist(include_import_state=True)
                GeoHandler.upload_executor.submit(GeoHandler._finish_uploaded_import, run_id,
                                                  {**body, "format": format_code, "source": source},
                                                  upload_path, object_key, bucket, format_code,
                                                  body["filename"], scan)
                defer_upload_cleanup = True
                public_record = {key: value for key, value in record.items() if key != "uploadSpoolPath"}
                return {**public_record, "status": "UPLOAD_PENDING", "queued": True, "_status": 202}

            if not (body.get("source") or {}).get("objectKey"):
                object_key = f"geodata-imports/{new_id()}-{body['filename'].replace('/', '_')}"
                stored = ObjectStore().put(bucket, object_key, content, "application/octet-stream")
                body = {**body, "source": {**body["source"], "objectKey": object_key, "bucket": bucket, "sha256": stored["sha256"], "scan": scan}}
            else:
                body = {**body, "source": {**body["source"], "bucket": body["source"].get("bucket", bucket), "sha256": scan["sha256"], "scan": scan}}
        except ImportError:
            digest = scan["sha256"] if upload_path else __import__("hashlib").sha256(content).hexdigest()
            body = {**body, "source": {**body["source"], "sha256": digest}}
        finally:
            if upload_path and not defer_upload_cleanup:
                Path(upload_path).unlink(missing_ok=True)
        if format_code in TEXT_FORMATS or format_code in {"WFS", "ARCGIS_FEATURESERVER", "SHAPEFILE", "SHP"}:
            prepared = GeoHandler._prepare_import_body({**body, "format": format_code})
            if upload_path:
                loader = lambda: parse_uploaded(format_code, ObjectStore().get(body["source"]["bucket"], body["source"]["objectKey"]) or b"", body["filename"])
            else:
                loader = lambda: parse_uploaded(format_code, content, body["filename"])
            return GeoHandler.store.once(p.get("Idempotency-Key"), lambda: GeoHandler._queue_import(
                prepared, p, body["filename"], loader))
        try:
            parse_uploaded(format_code, content or b"", body["filename"])
        except ValueError:
            if format_code not in {"OSM_PBF", "PARKSERVE_US"}:
                raise
            run_id = new_id()
            record = {"id": run_id, "programmeSlug": body.get("programmeSlug"), "adapter": body["adapter"], "format": format_code,
                      "entityType": body["entityType"], "entityTypes": categories, "source": body["source"], "filename": body["filename"], "status": "QUEUED",
                      "queuedAt": now(), "binaryObjectPending": True}
            GeoHandler.store.data.setdefault("importRuns", {})[run_id] = record
            GeoHandler.store.event("geodata.import.queued.v1", "import_run", run_id, {"importRunId": run_id, **record})
            # Binary uploads cannot be parsed by the synchronous adapter yet,
            # but they must still appear in the durable preprocessing queue.
            # Persist before returning so a service restart cannot make the
            # uploaded file disappear from Import History/Pre-processing.
            GeoHandler.store.persist(include_import_state=True)
            return {**record, "queued": True, "_status": 202}
        raise ValueError(f"unsupported upload format {format_code}")

    @staticmethod
    def import_manual(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        return GeoHandler.enqueue_import(_, p)

    @staticmethod
    def _import_run_view(run: dict[str, Any]) -> dict[str, Any]:
        """Return a run with candidate-queue counts for the imports page."""
        run_id = run.get("id") or run.get("importRunId")
        candidates = [candidate for candidate in GeoHandler.store.data.setdefault("importCandidates", {}).values()
                      if candidate.get("importRunId") == run_id]
        counts = {
            "total": len(candidates),
            "pending": sum(candidate.get("validationStatus") == "PENDING" for candidate in candidates),
            "confirmed": sum(candidate.get("validationStatus") == "CONFIRMED" for candidate in candidates),
            "processed": sum(candidate.get("validationStatus") == "PROCESSED" for candidate in candidates),
            "rejected": sum(candidate.get("validationStatus") == "REJECTED" for candidate in candidates),
        }
        public_run = {key: value for key, value in run.items() if key != "uploadSpoolPath"}
        return {**public_run, "candidateCounts": counts}

    @staticmethod
    def list_imports(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        GeoHandler.store.refresh_import_runs()
        query = parse_qs(urlparse(p.get("_path", "")).query)
        runs = [GeoHandler._import_run_view(run) for run in GeoHandler.store.data.setdefault("importRuns", {}).values()]
        runs.sort(key=lambda run: run.get("queuedAt") or run.get("startedAt") or "", reverse=True)
        return page_result(runs, query)

    @staticmethod
    def get_import(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        GeoHandler.store.refresh_import_runs()
        run = GeoHandler.store.data.setdefault("importRuns", {})[p["runId"]]
        candidates = [candidate for candidate in GeoHandler.store.data.setdefault("importCandidates", {}).values()
                      if candidate.get("importRunId") == p["runId"]]
        return GeoHandler._import_run_view(run)

    @staticmethod
    def mark_import_processed(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        """Finalize a run after review and discard its staged records."""
        GeoHandler._authorize_import(p)
        run_id = p["runId"]
        body = p.get("_body") or {}
        run = GeoHandler.store.data.setdefault("importRuns", {}).get(run_id)
        if not run:
            raise KeyError(f"import run {run_id} not found")
        status = str(run.get("status") or "").upper()
        if status in {"QUEUED", "PROCESSING"}:
            raise ValueError("an import cannot be finalized while it is still processing")
        if status == "PROCESSED":
            return {**GeoHandler._import_run_view(run), "finalized": True}

        actor = body.get("processedBy") or body.get("actor") or "administrator"
        candidates = GeoHandler.store.data.setdefault("importCandidates", {})
        queues = GeoHandler.store.data.setdefault("importProcessingQueues", {})
        candidate_ids = [candidate_id for candidate_id, candidate in candidates.items()
                         if candidate.get("importRunId") == run_id]
        queue_ids = [queue_id for queue_id, queue in queues.items()
                     if queue.get("importRunId") == run_id]
        processed_at = now()
        if GeoHandler.store.durable:
            with GeoHandler.store.transaction() as connection:
                connection.execute("DELETE FROM geodata_import_processing_queue WHERE import_run_id = %s", (run_id,))
                connection.execute("DELETE FROM geodata_import_candidate WHERE import_run_id = %s", (run_id,))
                connection.execute(
                    "UPDATE import_run SET status='PROCESSED', processed_at=now(), processed_by=%s, "
                    "last_error=NULL, heartbeat_at=NULL, lease_until=NULL WHERE id=%s", (actor, run_id))
        for candidate_id in candidate_ids:
            candidates.pop(candidate_id, None)
            GeoHandler.store.mark_import_candidate_deleted(candidate_id)
        for queue_id in queue_ids:
            queues.pop(queue_id, None)
            GeoHandler.store.mark_import_queue_deleted(queue_id)
        run.update({"status": "PROCESSED", "processedAt": processed_at, "processedBy": actor,
                    "lastError": None, "heartbeatAt": None, "leaseUntil": None,
                    "stagedRecordsDiscarded": len(candidate_ids), "processingQueuesDiscarded": len(queue_ids)})
        GeoHandler.store.event("geodata.import.processed.v1", "import_run", run_id,
                               {"importRunId": run_id, "processedBy": actor,
                                "stagedRecordsDiscarded": len(candidate_ids),
                                "processingQueuesDiscarded": len(queue_ids)})
        GeoHandler.store.persist(include_import_state=True)
        return {**GeoHandler._import_run_view(run), "finalized": True}

    @staticmethod
    def cleanup_load_test_run(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        """Permanently remove one tagged test run after environment-specific opt-in."""
        environment = os.environ.get("MYOTA_ENV", "").lower()
        cleanup_enabled = os.environ.get("MYOTA_LOAD_TEST_CLEANUP_ENABLED") == "1"
        production_cleanup_enabled = os.environ.get("MYOTA_LOAD_TEST_ALLOW_PRODUCTION_CLEANUP") == "YES"
        if not cleanup_enabled or environment not in {"development", "test", "staging", "production"}:
            raise PermissionError("load-test cleanup is disabled unless explicitly enabled in a declared environment")
        if environment == "production" and not production_cleanup_enabled:
            raise PermissionError("production load-test cleanup requires explicit production cleanup acknowledgement")
        authorization = p.get("Authorization", "")
        if not authorization.startswith("Bearer "):
            raise PermissionError("Bearer authentication is required")
        claims = verify_token(authorization[7:])
        roles = claims.get("roles", [])
        if not isinstance(roles, list):
            roles = []
        global_roles = {
            role.get("role") if isinstance(role, dict) else role
            for role in roles
        }
        # GLOBAL_OPERATOR is the identity service's canonical global-admin role.
        # Keep GLOBAL_ADMIN accepted for compatibility with older/external tokens.
        if not global_roles.intersection({"GLOBAL_ADMIN", "GLOBAL_OPERATOR"}):
            raise PermissionError("global administrator access is required")

        test_run_id = p["testRunId"]
        if not re.fullmatch(r"lt-[A-Za-z0-9._-]{1,77}", test_run_id):
            raise ValueError("testRunId must be a generated lt- fixture identifier")
        confirmation = (p.get("_body") or {}).get("confirmation")
        if confirmation != f"DELETE LOAD TEST DATA {test_run_id}":
            raise ValueError("confirmation must exactly match DELETE LOAD TEST DATA <testRunId>")
        GeoHandler.store.refresh_import_runs()
        runs = GeoHandler.store.data.setdefault("importRuns", {})
        matching_runs = {run_id: run for run_id, run in runs.items()
                         if (run.get("source") or {}).get("loadTestRunId") == test_run_id}
        if any(str(run.get("status", "")).upper() in {"UPLOAD_PENDING", "QUEUED", "PROCESSING"}
               for run in matching_runs.values()):
            raise ValueError("load-test runs must finish or fail before cleanup can remove them")

        run_ids = set(matching_runs)
        queues = GeoHandler.store.data.setdefault("importProcessingQueues", {})
        matching_queues = {queue_id: queue for queue_id, queue in queues.items()
                           if queue.get("importRunId") in run_ids}
        if any(str(queue.get("status", "")).upper() in {"QUEUED", "PROCESSING"}
               for queue in matching_queues.values()):
            raise ValueError("load-test promotion queues must finish before cleanup can remove them")
        entities = {entity_id: entity for entity_id, entity in GeoHandler.store.items.items()
                    if (entity.get("provenance") or {}).get("importRunId") in run_ids
                    and ((entity.get("provenance") or {}).get("source") or {}).get("loadTestRunId") == test_run_id}
        # Do not leave cross-service activity or awards orphaned if a test
        # fixture was accidentally used outside the intended geodata profile.
        with ThreadPoolExecutor(max_workers=8, thread_name_prefix="loadtest-cleanup-impact") as impact_pool:
            impacts = impact_pool.map(
                lambda entity: GeoHandler._activity_request(
                    p, f"/v1/activations/entity-deletion-impacts/{entity['id']}"),
                entities.values())
            for impact in impacts:
                if any(int(impact.get(field) or 0) for field in ("qsoCount", "activationCount", "awardProgressCount")):
                    raise ValueError("a tagged load-test entity has activity; cleanup stopped to protect QSOs and awards")

        from storage import ObjectStore
        object_store = ObjectStore()
        removed_objects = 0
        for run in matching_runs.values():
            source = run.get("source") or {}
            object_key = source.get("objectKey")
            bucket = source.get("bucket") or GEODATA_IMPORT_BUCKET
            if object_key:
                if bucket != GEODATA_IMPORT_BUCKET:
                    raise ValueError("load-test cleanup refuses to delete an object outside the geodata import bucket")
                object_store.delete(bucket, object_key)
                removed_objects += 1
            spool = run.get("uploadSpoolPath")
            if spool:
                spool_root = Path(os.environ.get("MYOTA_UPLOAD_SPOOL_DIR", "/tmp/myota-geodata-uploads")).resolve()
                spool_path = Path(spool).resolve()
                if not spool_path.is_relative_to(spool_root):
                    raise ValueError("load-test spool path is outside the configured upload spool directory")
                spool_path.unlink(missing_ok=True)

        for entity_id in entities:
            GeoHandler.store.delete_relational(entity_id)
            GeoHandler.store.items.pop(entity_id, None)
        candidates = GeoHandler.store.data.setdefault("importCandidates", {})
        for candidate_id, candidate in list(candidates.items()):
            if candidate.get("importRunId") in run_ids:
                candidates.pop(candidate_id, None)
                GeoHandler.store.mark_import_candidate_deleted(candidate_id)
        for queue_id, queue in matching_queues.items():
            if queue.get("importRunId") in run_ids:
                queues.pop(queue_id, None)
                GeoHandler.store.mark_import_queue_deleted(queue_id)
        for run_id in run_ids:
            runs.pop(run_id, None)
            GeoHandler.store.data.setdefault("sourceManifests", {}).pop(run_id, None)

        if GeoHandler.store.durable and run_ids:
            with GeoHandler.store.transaction() as connection:
                connection.execute("DELETE FROM import_run WHERE id = ANY(%s::uuid[])", (list(run_ids),))
        removed_entity_ids = set(entities)
        fixture_aggregates = run_ids | removed_entity_ids | set(matching_queues)
        GeoHandler.store.events[:] = [event for event in GeoHandler.store.events
                                      if event.get("aggregate", {}).get("id") not in fixture_aggregates]
        if GeoHandler.store.durable and fixture_aggregates:
            fixture_aggregate_ids = list(fixture_aggregates)
            with GeoHandler.store.transaction() as connection:
                connection.execute("DELETE FROM outbox_event WHERE aggregate_id = ANY(%s::text[])",
                                   (fixture_aggregate_ids,))
        GeoHandler.store.event("geodata.loadtest.cleaned.v1", "load_test_run", test_run_id,
                               {"testRunId": test_run_id, "importsDeleted": len(run_ids),
                                "entitiesDeleted": len(entities), "objectsDeleted": removed_objects})
        GeoHandler.store.persist(include_import_state=True)
        return {"testRunId": test_run_id, "importsDeleted": len(run_ids),
                "entitiesDeleted": len(entities), "objectsDeleted": removed_objects, "cleaned": True}

    @staticmethod
    def list_import_candidates(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        query = parse_qs(urlparse(p.get("_path", "")).query)
        run_id = p["runId"]
        candidates = [GeoHandler._candidate_view(candidate) for candidate in GeoHandler.store.data.setdefault("importCandidates", {}).values()
                      if candidate.get("importRunId") == run_id and candidate.get("validationStatus", "PENDING") == "PENDING"]
        candidates.sort(key=lambda candidate: (candidate.get("ordinal") or 0, candidate["id"]))
        return page_result(candidates, query)

    @staticmethod
    def validate_import_candidates(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        GeoHandler._authorize_import(p)
        body = p["_body"]
        require(body, "candidateIds", "reviewerId")
        candidate_ids = body["candidateIds"]
        if not isinstance(candidate_ids, list) or not candidate_ids:
            raise ValueError("candidateIds must be a non-empty list")
        run_id = p["runId"]
        candidates = GeoHandler.store.data.setdefault("importCandidates", {})
        requested_status = str(body.get("validationStatus") or "VALID").upper()
        if requested_status not in {"VALID", "CONFIRMED", "REJECTED", "INVALID"}:
            raise ValueError("validationStatus must be VALID or REJECTED")
        selected = []
        for candidate_id in candidate_ids:
            candidate = candidates.get(str(candidate_id))
            if not candidate or candidate.get("importRunId") != run_id:
                raise ValueError(f"candidate {candidate_id} does not belong to import run")
            selected.append(str(candidate_id))
        if requested_status in {"REJECTED", "INVALID"}:
            if GeoHandler.store.durable:
                with GeoHandler.store.transaction() as connection:
                    for candidate_id in selected:
                        connection.execute("DELETE FROM geodata_import_candidate WHERE id = %s", (candidate_id,))
            for candidate_id in selected:
                candidates.pop(candidate_id, None)
                GeoHandler.store.mark_import_candidate_deleted(candidate_id)
            GeoHandler.store.event("geodata.import.candidates.rejected.v1", "import_run", run_id,
                                   {"importRunId": run_id, "candidateIds": selected, "reviewerId": body["reviewerId"]})
            response_status = "REJECTED"
        else:
            for candidate_id in selected:
                candidate = candidates[candidate_id]
                if candidate.get("validationStatus") == "PROCESSED":
                    continue
                candidate["validationStatus"] = "CONFIRMED"
                candidate["validationNote"] = body.get("note")
                candidate["validatedBy"] = body["reviewerId"]
                candidate["validatedAt"] = now()
                GeoHandler.store.mark_import_candidate_dirty(candidate_id)
            GeoHandler.store.event("geodata.import.candidates.validated.v1", "import_run", run_id,
                                   {"importRunId": run_id, "candidateIds": selected, "reviewerId": body["reviewerId"]})
            response_status = "CONFIRMED"
        if p.get("_http"):
            GeoHandler.store.persist(include_import_state=True)
        return {"importRunId": run_id, "candidateIds": selected, "validationStatus": response_status, "_status": 200}

    @staticmethod
    def _process_import_queue(queue_id: str) -> None:
        with GeoHandler.store.lock:
            queue = GeoHandler.store.data.setdefault("importProcessingQueues", {}).get(queue_id)
            if not queue:
                return
            if queue.get("status") == "COMPLETED":
                return
            queue.update({"status": "PROCESSING", "startedAt": now()})
            GeoHandler.store.mark_import_queue_dirty(queue_id)
            GeoHandler.store.persist(include_import_state=True)
        created, updated, errors = [], [], []
        candidates = GeoHandler.store.data.setdefault("importCandidates", {})
        for candidate_id in queue["candidateIds"]:
            try:
                with GeoHandler.store.lock:
                    candidate = candidates.get(candidate_id)
                    if not candidate:
                        raise ValueError("pre-processed candidate no longer exists")
                    entity_id = candidate.get("entity", {}).get("id")
                    was_existing = entity_id in GeoHandler.store.items
                    GeoHandler._materialize_import_candidate(candidate, queue["targetStatus"], queue["requestedBy"], queue.get("note"))
                    if GeoHandler.store.durable:
                        with GeoHandler.store.transaction() as connection:
                            connection.execute("DELETE FROM geodata_import_candidate WHERE id = %s", (candidate_id,))
                    candidates.pop(candidate_id, None)
                    GeoHandler.store.mark_import_candidate_deleted(candidate_id)
                    (updated if was_existing else created).append(entity_id)
                    GeoHandler.store.persist(include_import_state=True)
            except (TypeError, ValueError) as error:
                errors.append({"candidateId": candidate_id, "message": str(error)})
        with GeoHandler.store.lock:
            queue = GeoHandler.store.data["importProcessingQueues"][queue_id]
            queue.update({"status": "COMPLETED" if not errors else "FAILED", "completedAt": now(),
                          "result": {"created": created, "updated": updated, "errors": errors}})
            GeoHandler.store.mark_import_queue_dirty(queue_id)
            run = GeoHandler.store.data.setdefault("importRuns", {}).get(queue["importRunId"])
            if run:
                run.setdefault("stats", {}).update({"processed": len(created) + len(updated),
                                                     "created": run.get("stats", {}).get("created", 0) + len(created),
                                                     "updated": run.get("stats", {}).get("updated", 0) + len(updated),
                                                     "processingErrors": len(errors)})
                remaining = [candidate for candidate in candidates.values()
                             if candidate.get("importRunId") == queue["importRunId"] and candidate.get("validationStatus") != "PROCESSED"]
                if not remaining and not errors:
                    run["status"] = "COMPLETED"
                    run["completedAt"] = now()
            GeoHandler.store.event("geodata.import.processing.completed.v1", "import_processing_queue", queue_id,
                                   {"queueId": queue_id, "importRunId": queue["importRunId"], "status": queue["status"],
                                    "targetStatus": queue["targetStatus"], **queue["result"]})
            GeoHandler.store.persist(include_import_state=True)

    @staticmethod
    def process_import_candidates(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        GeoHandler._authorize_import(p)
        body = p["_body"]
        require(body, "candidateIds", "targetStatus", "processorId")
        candidate_ids = body["candidateIds"]
        target_status = str(body["targetStatus"]).upper()
        if not isinstance(candidate_ids, list) or not candidate_ids:
            raise ValueError("candidateIds must be a non-empty list")
        if target_status not in {"CANDIDATE", "APPROVED"}:
            raise ValueError("targetStatus must be CANDIDATE or APPROVED")
        candidates = GeoHandler.store.data.setdefault("importCandidates", {})
        for candidate_id in candidate_ids:
            candidate = candidates.get(str(candidate_id))
            if not candidate or candidate.get("importRunId") != p["runId"]:
                raise ValueError(f"candidate {candidate_id} does not belong to import run")
            if target_status == "APPROVED":
                GeoHandler._authorize_review(p, candidate.get("entity") or {})
        queue_id = new_id()
        queue = {"id": queue_id, "importRunId": p["runId"], "candidateIds": [str(value) for value in candidate_ids],
                 "targetStatus": target_status, "requestedBy": body["processorId"], "note": body.get("note"),
                 "status": "QUEUED", "requestedAt": now(), "startedAt": None, "completedAt": None, "result": {}}
        GeoHandler.store.data.setdefault("importProcessingQueues", {})[queue_id] = queue
        GeoHandler.store.mark_import_queue_dirty(queue_id)
        GeoHandler.store.event("geodata.import.processing.queued.v1", "import_processing_queue", queue_id,
                               {"queueId": queue_id, "importRunId": p["runId"], "candidateIds": queue["candidateIds"],
                                "targetStatus": target_status, "requestedBy": body["processorId"],
                                "natsSubject": "myota.geodata.import.process.v1"})
        if p.get("_http"):
            GeoHandler.store.persist(include_import_state=True)
            GeoHandler.import_executor.submit(GeoHandler._process_import_queue, queue_id)
        else:
            GeoHandler._process_import_queue(queue_id)
        return {**queue, "queued": True, "_status": 202}

    @staticmethod
    def create_schedule(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        body = p["_body"]
        require(body, "adapter", "source", "intervalSeconds")
        categories = entity_type_codes(body.get("entityTypes") or body.get("entityTypeCodes"), body.get("entityType"))
        if not categories:
            raise ValueError("entityTypes must contain at least one shared entity category code")
        interval = int(body["intervalSeconds"])
        if interval < 300:
            raise ValueError("refresh interval must be at least 300 seconds")
        schedule = {"id": new_id(), "programmeSlug": body.get("programmeSlug"), "entityType": categories[0], "entityTypes": categories, "adapter": body["adapter"], "source": body["source"],
                    "intervalSeconds": interval, "disappearancePolicy": body.get("disappearancePolicy", "REVIEW_REQUIRED"),
                    "enabled": bool(body.get("enabled", True)), "lastRunAt": None, "nextRunAt": now(), "createdAt": now()}
        GeoHandler.store.data.setdefault("schedules", {})[schedule["id"]] = schedule
        GeoHandler.store.event("geodata.refresh-schedule.created.v1", "refresh_schedule", schedule["id"], schedule)
        return {**schedule, "_status": 201}

    @staticmethod
    def list_schedules(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        query = parse_qs(urlparse(p.get("_path", "")).query)
        return page_result(list(GeoHandler.store.data.setdefault("schedules", {}).values()), query)

    @staticmethod
    def refresh_import(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        schedule = GeoHandler.store.data.setdefault("schedules", {})[p["scheduleId"]]
        if not schedule.get("enabled"):
            raise ValueError("refresh schedule is disabled")
        body = p["_body"]
        if "features" not in body or not isinstance(body["features"], list):
            raise ValueError("features must be a list")
        body = {**body, "programmeSlug": schedule.get("programmeSlug"), "entityType": schedule["entityType"], "entityTypes": schedule.get("entityTypes") or [schedule["entityType"]], "adapter": schedule["adapter"], "source": schedule["source"],
                "disappearancePolicy": schedule["disappearancePolicy"], "completeSnapshot": True}
        result = GeoHandler.import_manual(None, {"_body": body, "Idempotency-Key": p.get("Idempotency-Key")})
        schedule["lastRunAt"], schedule["nextRunAt"] = now(), now()
        return result

    @staticmethod
    def bbox(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        query = parse_qs(urlparse(p.get("_path", "")).query)
        try:
            bounds = tuple(float(query.get(key, [""])[0]) for key in ("minLon", "minLat", "maxLon", "maxLat"))
        except ValueError as exc:
            raise ValueError("minLon, minLat, maxLon and maxLat are required numbers") from exc
        if bounds[0] >= bounds[2] or bounds[1] >= bounds[3]:
            raise ValueError("bbox must have min values below max values")
        programme = query.get("programme", [None])[0]
        status = query.get("status", [None])[0]
        max_features = min(1000, max(1, int(query.get("limit", ["500"])[0])))
        if GeoHandler.store.durable:
            entities = GeoHandler.store.query_bbox(bounds, max_features + 1, programme, status)
            features = []
            for entity in entities:
                properties = entity.pop("publicProperties")
                features.append({"type": "Feature", "id": entity["id"], "geometry": entity["geometry"],
                                 "properties": {"name": entity["name"], "programmeSlug": entity["programmeSlug"],
                                                "status": entity["status"], "entityType": entity.get("entityType"),
                                                "entityTypes": properties.get("entityTypes") or entity.get("entityType"),
                                                "sourceRef": properties.get("sourceRef"),
                                                "continentCode": properties.get("continentCode"),
                                                "countryCode": properties.get("countryCode"),
                                                "regionCode": properties.get("regionCode"), "city": properties.get("city")}})
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
                if entity_box[2] < bounds[0] or entity_box[0] > bounds[2] or entity_box[3] < bounds[1] or entity_box[1] > bounds[3]:
                    continue
                features.append({"type": "Feature", "id": entity["id"], "geometry": entity["geometry"],
                                 "properties": {"name": entity["name"], "programmeSlug": entity["programmeSlug"], "status": entity["status"],
                                                "entityType": entity.get("entityType"), "entityTypes": entity_categories(entity), "sourceRef": entity.get("sourceRef"),
                                                "continentCode": entity.get("continentCode"), "countryCode": entity.get("countryCode"),
                                                "regionCode": entity.get("regionCode"), "city": entity.get("city")}})
            truncated = len(features) > max_features
            features = features[:max_features]
        return {"type": "FeatureCollection", "bbox": list(bounds), "features": features,
                "count": len(features), "truncated": truncated, "cacheTtlSeconds": 60,
                "cacheKey": digest({"bbox": bounds, "programme": programme, "status": status,
                                     "minute": int(datetime.now(timezone.utc).timestamp() // 60)}),
                "performanceBudgetMs": 250}

    @staticmethod
    def tile(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        try:
            zoom, tile_x, tile_y = int(p["z"]), int(p["x"]), int(p["y"])
        except ValueError as exc:
            raise ValueError("tile coordinates must be integers") from exc
        if not 0 <= zoom <= 22 or not 0 <= tile_x < 2 ** zoom or not 0 <= tile_y < 2 ** zoom:
            raise ValueError("invalid Web Mercator tile coordinates")
        count = 2 ** zoom
        min_lon = tile_x / count * 360 - 180
        max_lon = (tile_x + 1) / count * 360 - 180
        def latitude(tile_row: int) -> float:
            radians = math.atan(math.sinh(math.pi * (1 - 2 * tile_row / count)))
            return math.degrees(radians)
        max_lat, min_lat = latitude(tile_y), latitude(tile_y + 1)
        query = f"/v1/geodata/bbox?minLon={min_lon}&minLat={min_lat}&maxLon={max_lon}&maxLat={max_lat}&limit=500"
        return GeoHandler.bbox(None, {"_path": query}) | {"tile": {"z": zoom, "x": tile_x, "y": tile_y}, "format": "geojson-vector"}

    @staticmethod
    def list_conflation(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        query = parse_qs(urlparse(p.get("_path", "")).query)
        items = list(GeoHandler.store.data.setdefault("conflationCandidates", {}).values())
        resolution = query.get("resolution", [None])[0]
        if resolution:
            items = [item for item in items if item.get("resolution") == resolution]
        return page_result(items, query)

    @staticmethod
    def resolve_conflation(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        body = p["_body"]
        require(body, "decision", "reviewerId")
        if body["decision"] not in {"MERGED", "KEPT_SEPARATE", "IGNORED", "OPEN"}:
            raise ValueError("decision must be MERGED, KEPT_SEPARATE, IGNORED, or OPEN")
        item = GeoHandler.store.data.setdefault("conflationCandidates", {})[p["candidateId"]]
        previous = item["resolution"]
        item["resolution"] = body["decision"]
        item["survivorEntityId"] = body.get("survivorEntityId")
        item.setdefault("resolutionHistory", []).append({"decision": body["decision"], "previousDecision": previous,
                                                           "reviewerId": body["reviewerId"], "note": body.get("note"), "occurredAt": now()})
        item["updatedAt"] = now()
        GeoHandler.store.event("geodata.conflation.resolved.v1", "conflation_candidate", item["id"], item)
        return item

    @staticmethod
    def draw_proposal(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        body = p["_body"]
        require(body, "feature")
        feature = dict(body["feature"])
        feature["attachments"] = body.get("attachments", feature.get("attachments"))
        properties = feature.get("properties") or {}
        # The preferred proposal contract carries shared categories at the
        # request level. Preserve compatibility with older clients that put
        # them in feature properties before normalizing the candidate.
        request_categories = body.get("entityTypes") or body.get("entityTypeCodes") or body.get("entityType")
        if request_categories and not (properties.get("entityTypes") or properties.get("entityTypeCodes") or properties.get("entityType")):
            properties["entityTypes"] = request_categories
        categories = entity_type_codes(properties.get("entityTypes") or properties.get("entityTypeCodes"), properties.get("entityType"))
        if not categories:
            raise ValueError("entityTypes must contain at least one shared entity category code")
        properties["candidateSource"] = {
            "type": "COMMUNITY_PROPOSAL",
            "proposalId": new_id(),
            "proposerId": body.get("proposerId") or p.get("accountId"),
        }
        feature["properties"] = properties
        result = GeoHandler.import_manual(None, {"_body": {"programmeSlug": body.get("programmeSlug"), "adapter": "MANUAL",
            "entityTypes": categories, "entityType": categories[0],
            "source": {**(body.get("source") or {}), "name": (body.get("source") or {}).get("name", "Manual proposal"),
                        "license": (body.get("source") or {}).get("license", "programme-supplied")}, "features": [feature]}})
        # Community proposals are already an interactive, user-reviewed action;
        # unlike bulk file/paste imports they enter the normal CANDIDATE queue
        # immediately after the same normalization step.
        run_id = result["importRunId"]
        candidate_ids = result.get("preprocessed", [])
        GeoHandler.validate_import_candidates(None, {"runId": run_id, "_body": {"candidateIds": candidate_ids, "reviewerId": body.get("proposerId") or p.get("accountId") or "proposal"}})
        queue = GeoHandler.process_import_candidates(None, {"runId": run_id, "_body": {"candidateIds": candidate_ids, "targetStatus": "CANDIDATE", "processorId": body.get("proposerId") or p.get("accountId") or "proposal"}})
        processed = queue.get("result", {}).get("created", []) + queue.get("result", {}).get("updated", [])
        return {**result, "created": processed, "status": "COMPLETED", "processingQueueId": queue["id"]}

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
        entity["review"] = {**(entity.get("review") or {}), "reviewerId": body["reviewerId"], "note": body.get("note"), "reviewedAt": reviewed_at}
        entity.setdefault("reviewHistory", []).append({"action": body["decision"], "reviewerId": body["reviewerId"],
                                                        "note": body.get("note"), "occurredAt": reviewed_at,
                                                        "previousStatus": "CANDIDATE"})
        entity["updatedAt"] = now()
        GeoHandler.store.event("geodata.entity.reviewed.v1", "entity", entity["id"], entity)
        return entity

    @staticmethod
    def set_status(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        entity = GeoHandler.store.items[p["entityId"]]
        GeoHandler._authorize_review(p, entity)
        body = p["_body"]
        require(body, "status", "reviewerId")
        allowed = {"APPROVED", "CANDIDATE", "RETIRED", "REJECTED"}
        if body["status"] not in allowed:
            raise ValueError("status must be APPROVED, CANDIDATE, RETIRED, or REJECTED")
        previous_status = entity["status"]
        target_status = body["status"]
        if previous_status == "APPROVED" and target_status != "RETIRED":
            raise ValueError("approved entities can only be retired to protect historical QSOs")
        if previous_status == "RETIRED" and target_status != "RETIRED":
            raise ValueError("retired entities cannot be reactivated")
        if previous_status == "CANDIDATE" and target_status not in {"CANDIDATE", "APPROVED", "REJECTED"}:
            raise ValueError("candidate entities can only remain candidates, be approved, or be rejected")
        if previous_status == "REJECTED" and target_status != "REJECTED":
            raise ValueError("rejected entities cannot be moved back into the review lifecycle")
        if previous_status == target_status:
            return entity
        changed_at = now()
        entity["status"] = target_status
        entity["review"] = {**(entity.get("review") or {}), "reviewerId": body["reviewerId"],
                             "note": body.get("note"), "changedAt": changed_at}
        entity.setdefault("reviewHistory", []).append({"action": "STATUS_CHANGED", "status": target_status,
                                                        "previousStatus": previous_status, "reviewerId": body["reviewerId"],
                                                        "note": body.get("note"), "occurredAt": changed_at})
        entity["updatedAt"] = changed_at
        GeoHandler.store.event("geodata.entity.status-changed.v1", "entity", entity["id"],
                               {"entityId": entity["id"], "status": target_status,
                                "previousStatus": previous_status, "reviewerId": body["reviewerId"],
                                "note": body.get("note")})
        return entity

    @staticmethod
    def update_geometry(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        entity = GeoHandler.store.items[p["entityId"]]
        GeoHandler._authorize_review(p, entity)
        body = p["_body"]
        require(body, "geometry", "editorId")
        geometry = body["geometry"]
        if not isinstance(geometry, dict) or "coordinates" not in geometry:
            raise ValueError("geometry must be a GeoJSON Point, LineString, MultiLineString, Polygon, or MultiPolygon")
        previous = entity.get("geometry")
        entity.setdefault("geometryHistory", []).append({"editorId": body["editorId"], "note": body.get("note"),
                                                          "geometry": previous, "editedAt": now()})
        entity["geometry"] = normalize_geometry(geometry)
        entity["centroid"] = geometry_centroid(entity["geometry"])
        enrich_entity_location(entity)
        entity["updatedAt"] = now()
        GeoHandler.store.event("geodata.entity.geometry-updated.v1", "entity", entity["id"],
                               {"entityId": entity["id"], "editorId": body["editorId"], "note": body.get("note"), "geometry": geometry})
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
            raise ValueError(f"unsupported location fields: {', '.join(sorted(unknown))}")
        requested_manual = body.get("manualFields", list(location))
        if not isinstance(requested_manual, list) or not set(requested_manual) <= allowed:
            raise ValueError("manualFields must be a list of supported location fields")
        manual_fields = set(requested_manual)
        code_fields = {"continentCode", "countryCode", "regionCode", "subdivisionCode", "provinceCode"}
        if manual_fields.intersection(code_fields):
            raise ValueError("continent, country, subdivision, and province codes are provider-derived and cannot be manually edited")
        previous_manual = set(entity.get("manualLocationFields") or [])
        released_fields = previous_manual - manual_fields

        def normalize_value(field: str, value: Any) -> Any:
            if value is None:
                return None
            if not isinstance(value, (str, int, float, bool)):
                raise ValueError(f"location field {field} must be a scalar or null")
            value = str(value).strip()
            return value.upper() if field.endswith("Code") else value or None

        previous = {field: entity.get(field) for field in LOCATION_FIELDS}
        for field in manual_fields:
            if field in location:
                entity[field] = normalize_value(field, location[field])
        entity.update(derive_location_codes(location, manual_fields, GeoHandler.store.items.values()))
        for field in released_fields:
            entity[field] = None
        entity["manualLocationFields"] = sorted(manual_fields)
        enrich_entity_location(entity, force=bool(released_fields))
        changed_at = now()
        entity.setdefault("reviewHistory", []).append({
            "action": "LOCATION_UPDATED", "editorId": body["editorId"], "note": body.get("note"),
            "manualFields": sorted(manual_fields), "previous": previous,
            "location": {field: entity.get(field) for field in LOCATION_FIELDS}, "occurredAt": changed_at,
        })
        entity.setdefault("provenance", {})["manualLocation"] = {
            "fields": sorted(manual_fields), "editorId": body["editorId"], "note": body.get("note"),
            "updatedAt": changed_at,
        }
        entity["updatedAt"] = changed_at
        GeoHandler.store.event("geodata.entity.location-updated.v1", "entity", entity["id"], {
            "entityId": entity["id"], "editorId": body["editorId"], "manualFields": sorted(manual_fields),
            "note": body.get("note"),
        })
        return entity

    @staticmethod
    def change_entity_type(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        entity = GeoHandler.store.items[p["entityId"]]
        GeoHandler._authorize_review(p, entity)
        body = p["_body"]
        require(body, "editorId")
        if entity.get("status") == "RETIRED":
            raise ValueError("retired entities cannot change category")
        categories = entity_type_codes(body.get("entityTypes") or body.get("entityTypeCodes"), body.get("entityType"))
        if not categories:
            raise ValueError("entityTypes must contain at least one stable category code")
        previous = entity_categories(entity)
        if categories == previous:
            return entity
        changed_at = now()
        entity["entityType"] = categories[0]
        entity["entityTypes"] = categories
        entity["entityTypeCodes"] = categories
        entity.setdefault("reviewHistory", []).append({"action": "ENTITY_TYPE_CHANGED", "editorId": body["editorId"],
                                                         "previousEntityTypes": previous, "entityTypes": categories,
                                                         "previousEntityType": previous[0] if previous else None, "entityType": categories[0],
                                                         "note": body.get("note"), "occurredAt": changed_at})
        entity["updatedAt"] = changed_at
        GeoHandler.store.event("geodata.entity.entity-type-changed.v1", "entity", entity["id"],
                               {"entityId": entity["id"], "editorId": body["editorId"],
                                "previousEntityTypes": previous, "entityTypes": categories, "note": body.get("note")})
        return entity

    @staticmethod
    def change_entity_name(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
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
        entity.setdefault("reviewHistory", []).append({
            "action": "ENTITY_NAME_CHANGED", "editorId": body["editorId"],
            "previousName": previous, "name": name, "note": body.get("note"),
            "occurredAt": changed_at,
        })
        entity["updatedAt"] = changed_at
        GeoHandler.store.event("geodata.entity.name-changed.v1", "entity", entity["id"], {
            "entityId": entity["id"], "editorId": body["editorId"],
            "previousName": previous, "name": name, "note": body.get("note"),
        })
        return entity

    @staticmethod
    def change_geometry_type(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        entity = GeoHandler.store.items[p["entityId"]]
        GeoHandler._authorize_gis_admin(p, entity, "geodata.geometry.manage")
        body = p["_body"]
        require(body, "geometryType", "editorId")
        target = str(body["geometryType"]).upper()
        target = {"WAY": "LINESTRING"}.get(target, target)
        if target not in {"POINT", "LINESTRING", "MULTILINESTRING", "POLYGON", "MULTIPOLYGON"}:
            raise ValueError("geometryType must be POINT, LINESTRING, MULTILINESTRING, POLYGON, or MULTIPOLYGON")
        current = str(entity.get("geometry", {}).get("type", "")).upper()
        current = {"WAY": "LINESTRING"}.get(current, current)
        if current == target:
            return entity
        geometry = entity.get("geometry") or {}
        if target == "POINT":
            centre = geometry_centroid(geometry)
            converted = {"type": "Point", "coordinates": [centre["lon"], centre["lat"]]}
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
                ring = rings[0] if current in {"POLYGON", "MULTIPOLYGON"} and rings else rings
                if len(ring) > 1 and ring[0] == ring[-1]:
                    ring = ring[:-1]
                lines = [ring]
            converted = {"type": target.title() if target == "LINESTRING" else "MultiLineString",
                         "coordinates": lines[0] if target == "LINESTRING" else lines}
        elif target in {"POLYGON", "MULTIPOLYGON"} and current == "POINT":
            coordinates = geometry.get("coordinates") or []
            if len(coordinates) < 2:
                raise ValueError("the existing point geometry is invalid")
            lon, lat = float(coordinates[0]), float(coordinates[1])
            delta = 0.0005
            polygon = [[[lon - delta, lat - delta], [lon + delta, lat - delta],
                        [lon + delta, lat + delta], [lon - delta, lat + delta],
                        [lon - delta, lat - delta]]]
            converted = {"type": "Polygon" if target == "POLYGON" else "MultiPolygon",
                         "coordinates": polygon if target == "POLYGON" else [polygon]}
        elif target in {"POLYGON", "MULTIPOLYGON"} and current in {"LINESTRING", "MULTILINESTRING"}:
            lines = geometry.get("coordinates") or []
            points = lines if current == "LINESTRING" else [point for line in lines for point in line]
            if len(points) < 2:
                raise ValueError("the existing line geometry is invalid")
            longitudes = [float(point[0]) for point in points]
            latitudes = [float(point[1]) for point in points]
            delta = max((max(longitudes) - min(longitudes)) * 0.05, (max(latitudes) - min(latitudes)) * 0.05, 0.0001)
            min_lon, max_lon = min(longitudes) - delta, max(longitudes) + delta
            min_lat, max_lat = min(latitudes) - delta, max(latitudes) + delta
            polygon = [[[min_lon, min_lat], [max_lon, min_lat], [max_lon, max_lat],
                        [min_lon, max_lat], [min_lon, min_lat]]]
            converted = {"type": "Polygon" if target == "POLYGON" else "MultiPolygon",
                         "coordinates": polygon if target == "POLYGON" else [polygon]}
        else:
            polygons = geometry.get("coordinates", [])
            if current == "POLYGON":
                polygons = [polygons]
            if not polygons:
                raise ValueError("the existing polygon geometry is invalid")
            converted = {"type": "Polygon" if target == "POLYGON" else "MultiPolygon",
                         "coordinates": polygons[0] if target == "POLYGON" else polygons}
        converted = normalize_geometry(converted)
        changed_at = now()
        entity.setdefault("geometryHistory", []).append({"action": "GEOMETRY_TYPE_CHANGED", "editorId": body["editorId"],
                                                          "note": body.get("note"), "previousGeometry": geometry,
                                                          "geometry": converted, "editedAt": changed_at})
        entity.setdefault("reviewHistory", []).append({"action": "GEOMETRY_TYPE_CHANGED", "editorId": body["editorId"],
                                                        "note": body.get("note"), "previousType": current,
                                                        "geometryType": target, "occurredAt": changed_at})
        entity["geometry"] = converted
        entity["centroid"] = geometry_centroid(converted)
        enrich_entity_location(entity)
        entity["updatedAt"] = changed_at
        GeoHandler.store.event("geodata.entity.geometry-type-changed.v1", "entity", entity["id"],
                               {"entityId": entity["id"], "editorId": body["editorId"], "previousType": current,
                                "geometryType": target, "note": body.get("note")})
        return entity

    @staticmethod
    def delete_entity(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        entity = GeoHandler.store.items[p["entityId"]]
        GeoHandler._authorize_gis_admin(p, entity, "geodata.delete")
        authorization = p.get("Authorization", "")
        claims = verify_token(authorization[7:]) if authorization.startswith("Bearer ") else {}
        global_admin = any(role.get("role") in {"GLOBAL_ADMIN", "GLOBAL_OPERATOR"} for role in claims.get("roles", []))
        if not global_admin and entity.get("status") != "REJECTED":
            raise ValueError("only rejected entities can be permanently deleted")
        entity_id = entity["id"]
        for candidate_id, candidate in list(GeoHandler.store.data.setdefault("conflationCandidates", {}).items()):
            if entity_id in (candidate.get("leftEntityId"), candidate.get("rightEntityId")):
                GeoHandler.store.data["conflationCandidates"].pop(candidate_id, None)
        GeoHandler.store.delete_relational(entity_id)
        GeoHandler.store.items.pop(entity_id, None)
        GeoHandler.store.events[:] = [event for event in GeoHandler.store.events
                                      if event.get("aggregate", {}).get("id") != entity_id]
        GeoHandler.store.event("geodata.entity.deleted.v1", "entity", entity_id,
                               {"entityId": entity_id, "deletedBy": p.get("_body", {}).get("deletedBy"), "previousStatus": entity.get("status")})
        return {"entityId": entity_id, "deleted": True, "previousStatus": entity.get("status"), "_status": 204}

    @staticmethod
    def patch_entity_metadata(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        body = dict(p.get("_body") or {})
        editor_id = body.get("editorId")
        require(body, "editorId")
        supported = {"editorId", "name", "location", "manualFields", "note"}
        unknown = set(body) - supported
        if unknown:
            raise ValueError(f"unsupported entity metadata fields: {', '.join(sorted(unknown))}")
        entity = GeoHandler.store.items[p["entityId"]]
        result = entity
        if "name" in body:
            result = GeoHandler.change_entity_name(None, {**p, "_body": {"name": body["name"], "editorId": editor_id, "note": body.get("note")}})
        if "location" in body:
            location_body = {"location": body["location"], "editorId": editor_id, "note": body.get("note")}
            if "manualFields" in body:
                location_body["manualFields"] = body["manualFields"]
            result = GeoHandler.update_location(None, {**p, "_body": location_body})
        if "name" not in body and "location" not in body:
            raise ValueError("at least one editable metadata field is required: name or location")
        return result

    @staticmethod
    def put_entity_categories(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        body = dict(p.get("_body") or {})
        categories = body.get("entityTypes") or body.get("entityTypeCodes") or body.get("entityType")
        return GeoHandler.change_entity_type(None, {**p, "_body": {"entityTypes": categories, "editorId": body.get("editorId"), "note": body.get("note")}})

    @staticmethod
    def review_resource(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        body = dict(p.get("_body") or {})
        status = body.get("status") or body.get("decision")
        if status:
            return GeoHandler.set_status(None, {**p, "_body": {"status": status, "reviewerId": body.get("reviewerId"), "note": body.get("note")}})
        raise ValueError("review status or decision is required")

    @staticmethod
    def _activity_request(p: dict[str, str], path: str, method: str = "GET", payload: dict[str, Any] | None = None) -> dict[str, Any]:
        if not p.get("_http"):
            return {"qsoCount": 0, "activationCount": 0, "awardProgressCount": 0}
        url = os.environ.get("MYOTA_ACTIVITY_URL", "http://activity:8004").rstrip("/") + path
        headers = {"Accept": "application/json", "User-Agent": "MyOTA-geodata-service/1.0"}
        if p.get("Authorization"):
            headers["Authorization"] = p["Authorization"]
        data = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        request = Request(url, data=data, method=method, headers=headers)
        try:
            with urlopen(request, timeout=15) as response:  # nosec B310 - deployment-controlled internal URL
                return json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"activity service request failed: {exc}") from exc

    @staticmethod
    def create_deletion_job(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        body = p.get("_body") or {}
        require(body, "entityId", "requestedBy")
        entity = GeoHandler.store.items[body["entityId"]]
        GeoHandler._authorize_gis_admin(p, entity, "geodata.delete")
        authorization = p.get("Authorization", "")
        claims = verify_token(authorization[7:]) if authorization.startswith("Bearer ") else {}
        global_admin = any(role.get("role") in {"GLOBAL_ADMIN", "GLOBAL_OPERATOR"} for role in claims.get("roles", []))
        if not global_admin and entity.get("status") != "REJECTED":
            raise ValueError("only global administrators can delete non-rejected entities")
        impact = GeoHandler._activity_request(p, f"/v1/activations/entity-deletion-impacts/{entity['id']}")
        job = {"id": new_id(), "entityId": entity["id"], "status": "AWAITING_CONFIRMATION",
               "requestedBy": body["requestedBy"], "impact": impact, "confirmationRequired": True,
               "createdAt": now(), "updatedAt": now()}
        GeoHandler.store.data.setdefault("entityDeletionJobs", {})[job["id"]] = job
        GeoHandler.store.event("geodata.entity-deletion-job.created.v1", "entity_deletion_job", job["id"], job)
        GeoHandler.store.persist()
        return {**job, "_status": 201}

    @staticmethod
    def get_deletion_job(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        return GeoHandler.store.data.setdefault("entityDeletionJobs", {})[p["jobId"]]

    @staticmethod
    def _execute_deletion_job(job_id: str, p: dict[str, str]) -> None:
        jobs = GeoHandler.store.data.setdefault("entityDeletionJobs", {})
        job = jobs.get(job_id)
        if not job:
            return
        job.update({"status": "PROCESSING", "updatedAt": now()})
        GeoHandler.store.persist()
        try:
            cascade = GeoHandler._activity_request(p, "/v1/activations/entity-deletion-cascades", "POST",
                                                    {"entityId": job["entityId"], "deletedBy": p.get("_body", {}).get("deletedBy") or job["requestedBy"]})
            deleted = GeoHandler.delete_entity(None, {**p, "entityId": job["entityId"],
                                                       "_body": {"deletedBy": p.get("_body", {}).get("deletedBy") or job["requestedBy"]}})
            job.update({"status": "COMPLETED", "cascade": cascade, "deleted": deleted,
                        "completedAt": now(), "updatedAt": now()})
            GeoHandler.store.event("geodata.entity-deletion-job.completed.v1", "entity_deletion_job", job_id, job)
        except Exception as exc:  # pragma: no cover - exercised by service integration failures
            job.update({"status": "FAILED", "error": str(exc), "updatedAt": now()})
            GeoHandler.store.event("geodata.entity-deletion-job.failed.v1", "entity_deletion_job", job_id, job)
        GeoHandler.store.persist()

    @staticmethod
    def confirm_deletion_job(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        body = p.get("_body") or {}
        require(body, "confirmation", "deletedBy")
        if str(body["confirmation"]).upper() != "DELETE":
            raise ValueError("confirmation must be DELETE")
        job = GeoHandler.store.data.setdefault("entityDeletionJobs", {})[p["jobId"]]
        if job.get("status") == "COMPLETED":
            return job
        if job.get("status") != "AWAITING_CONFIRMATION":
            raise ValueError("deletion job is not awaiting confirmation")
        job.update({"status": "QUEUED", "confirmedBy": body["deletedBy"], "confirmedAt": now(), "updatedAt": now()})
        request_context = {**p, "_body": body}
        if p.get("_http"):
            GeoHandler.deletion_executor.submit(GeoHandler._execute_deletion_job, job["id"], request_context)
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
    ("GET", "/v1/geodata/imports/{runId}"): GeoHandler.get_import,
    ("GET", "/v1/geodata/imports/{runId}/candidates"): GeoHandler.list_import_candidates,
    ("GET", "/v1/geodata/refresh-schedules"): GeoHandler.list_schedules,
    ("GET", "/v1/geodata/conflation"): GeoHandler.list_conflation,
    ("POST", "/v1/geodata/imports/manual"): GeoHandler.import_manual,
    ("POST", "/v1/geodata/imports"): GeoHandler.enqueue_import,
    ("POST", "/v1/geodata/imports/upload"): GeoHandler.upload_import,
    ("POST", "/v1/geodata/imports/{runId}/candidates/validate"): GeoHandler.validate_import_candidates,
    ("POST", "/v1/geodata/imports/{runId}/process"): GeoHandler.process_import_candidates,
    ("POST", "/v1/geodata/imports/{runId}/processed"): GeoHandler.mark_import_processed,
    ("DELETE", "/v1/geodata/load-test-runs/{testRunId}"): GeoHandler.cleanup_load_test_run,
    ("POST", "/v1/geodata/refresh-schedules"): GeoHandler.create_schedule,
    ("POST", "/v1/geodata/refresh-schedules/{scheduleId}/run"): GeoHandler.refresh_import,
    ("POST", "/v1/geodata/proposals/draw"): GeoHandler.draw_proposal,
    ("POST", "/v1/geodata/proposals"): GeoHandler.draw_proposal,
    ("POST", "/v1/geodata/conflation/{candidateId}/resolve"): GeoHandler.resolve_conflation,
    ("POST", "/v1/geodata/entities/{entityId}/review"): GeoHandler.review,
    ("POST", "/v1/geodata/entities/{entityId}/reviews"): GeoHandler.review_resource,
    ("POST", "/v1/geodata/entities/{entityId}/status"): GeoHandler.set_status,
    ("POST", "/v1/geodata/entities/{entityId}/geometry"): GeoHandler.update_geometry,
    ("PUT", "/v1/geodata/entities/{entityId}/geometry"): GeoHandler.update_geometry,
    ("PATCH", "/v1/geodata/entities/{entityId}"): GeoHandler.patch_entity_metadata,
    ("PUT", "/v1/geodata/entities/{entityId}/categories"): GeoHandler.put_entity_categories,
    ("POST", "/v1/geodata/entities/{entityId}/location"): GeoHandler.update_location,
    ("POST", "/v1/geodata/entities/{entityId}/entity-type"): GeoHandler.change_entity_type,
    ("POST", "/v1/geodata/entities/{entityId}/name"): GeoHandler.change_entity_name,
    ("POST", "/v1/geodata/entities/{entityId}/geometry-type"): GeoHandler.change_geometry_type,
    ("POST", "/v1/geodata/entities/{entityId}/delete"): GeoHandler.delete_entity,
    ("POST", "/v1/geodata/entity-deletion-jobs"): GeoHandler.create_deletion_job,
    ("GET", "/v1/geodata/entity-deletion-jobs/{jobId}"): GeoHandler.get_deletion_job,
    ("POST", "/v1/geodata/entity-deletion-jobs/{jobId}/confirm"): GeoHandler.confirm_deletion_job,
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


if __name__ == "__main__":
    GeoHandler.store.hydrate()
    ThreadingHTTPServer(("0.0.0.0", 8003), GeoHandler).serve_forever()
