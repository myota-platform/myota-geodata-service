from __future__ import annotations

from http.server import ThreadingHTTPServer
from concurrent.futures import ThreadPoolExecutor
import math
import os
import re
from typing import Any
from urllib.parse import parse_qs, urlparse

from common import JsonHandler, new_id, now, page_result, require, verify_token
from geodata_pipeline import (MAX_IMPORT_FEATURES, conflation_score, digest, geometry_bbox, geometry_centroid,
                              geometry_distance_meters,
                              normalize_geometry, source_manifest, validate_attachments)
from geodata_store import GeodataStore
from import_adapters import normalize
from import_formats import SUPPORTED_FORMATS, TEXT_FORMATS, parse_text, parse_uploaded
from location_catalog import build_location_tree, derive_location_codes
from reverse_geocoder import LOCATION_FIELDS, enrich_entity_location


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
    import_executor = ThreadPoolExecutor(max_workers=max(1, int(os.environ.get("MYOTA_IMPORT_WORKERS", "2"))),
                                          thread_name_prefix="geodata-import")

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
        for existing in GeoHandler.store.items.values():
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
                geometry = normalize_geometry(feature["geometry"], props.get("crs"))
                attachments = validate_attachments(raw_feature.get("attachments") or props.get("attachments"))
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
                                         "sourceFeature": raw_feature, "importRunId": run_id, "sourceHash": digest(raw_feature),
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
                import_candidates[candidate_id] = candidate
                preprocessed.append(candidate_id)
                records.append({"sourceRef": source_ref, "sourceHash": entity["provenance"]["sourceHash"]})
            except (TypeError, ValueError) as error:
                errors.append({"index": index, "message": str(error)})
        seen_refs = {record["sourceRef"] for record in records}
        disappeared = GeoHandler._apply_disappearance(programme_slug, source_key, adapter, seen_refs,
                                                       body.get("disappearancePolicy", "REVIEW_REQUIRED")) if body.get("completeSnapshot") else []
        manifest = source_manifest(adapter, source, records, run_id)
        manifest["sourceKey"] = source_key
        manifest["sourceChanged"] = not any(item.get("sourceHash") == manifest["sourceHash"] for item in GeoHandler.store.data.setdefault("sourceManifests", {}).values() if item.get("sourceKey") == source_key)
        GeoHandler.store.data["sourceManifests"][run_id] = manifest
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
        GeoHandler.store.data.setdefault("importRuns", {})[run_id] = record
        GeoHandler.store.event("geodata.import.queued.v1", "import_run", run_id, {"importRunId": run_id,
            "programmeSlug": body.get("programmeSlug"), "adapter": body["adapter"], "format": body.get("format", "GEOJSON").upper(),
            "entityType": body["entityType"], "entityTypes": categories, "filename": filename})
        return run_id, record

    @staticmethod
    def _complete_import_run(run_id: str, result: dict[str, Any]) -> dict[str, Any]:
        run = GeoHandler.store.data.setdefault("importRuns", {})[run_id]
        run.update({"status": "COMPLETED" if not result.get("errors") else "COMPLETED_WITH_ERRORS", "completedAt": now(),
                    "stats": {key: len(result.get(key, [])) for key in ("preprocessed", "created", "updated", "skipped", "errors", "disappeared")},
                    "errors": result.get("errors", []), "manifest": result.get("manifest"),
                    "conflationCandidateCount": len(result.get("conflationCandidates", []))})
        run["status"] = "PREPROCESSED" if not result.get("errors") else "PREPROCESSED_WITH_ERRORS"
        GeoHandler.store.event("geodata.import.preprocessed.v1", "import_run", run_id, result)
        return run

    @staticmethod
    def _fail_import_run(run_id: str, error: Exception) -> dict[str, Any]:
        run = GeoHandler.store.data.setdefault("importRuns", {})[run_id]
        run.update({"status": "FAILED", "completedAt": now(), "errors": [{"message": str(error)}],
                    "stats": {"preprocessed": 0, "created": 0, "updated": 0, "skipped": 0, "errors": 1, "disappeared": 0}})
        GeoHandler.store.event("geodata.import.failed.v1", "import_run", run_id, {"importRunId": run_id, "error": str(error)})
        return run

    @staticmethod
    def _queue_import(body: dict[str, Any], p: dict[str, str], filename: str | None,
                      loader: Any) -> dict[str, Any]:
        run_id, record = GeoHandler._create_import_run(body, filename)

        def process() -> None:
            try:
                with GeoHandler.store.lock:
                    run = GeoHandler.store.data["importRuns"][run_id]
                    run.update({"status": "PROCESSING", "startedAt": now()})
                    GeoHandler.store.persist()
                features = loader()
                prepared = GeoHandler._prepare_import_body(body, features)
                with GeoHandler.store.lock:
                    GeoHandler.store.data["importRuns"][run_id]["featureCount"] = len(features)
                result = GeoHandler._import_features({**prepared, "features": features}, run_id)
                with GeoHandler.store.lock:
                    GeoHandler._complete_import_run(run_id, result)
                    GeoHandler.store.persist()
            except Exception as error:  # imports must report failure in the run, not fail the HTTP request
                with GeoHandler.store.lock:
                    GeoHandler._fail_import_run(run_id, error)
                    GeoHandler.store.persist()

        # Persist the QUEUED record before the worker can finish, preventing a
        # fast worker from being overwritten by the request handler's final save.
        if p.get("_http"):
            GeoHandler.store.persist()
        GeoHandler.import_executor.submit(process)
        return {**record, "queued": True, "_status": 202}

    @staticmethod
    def _start_import(body: dict[str, Any], features: list[dict[str, Any]], p: dict[str, str], filename: str | None = None) -> dict[str, Any]:
        body = GeoHandler._prepare_import_body({**body, "features": features}, features)
        if p.get("_http"):
            return GeoHandler._queue_import(body, p, filename, lambda: features)
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
            prepared, p, body.get("filename"), lambda: parse_text(format_code, content)))

    @staticmethod
    def upload_import(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        GeoHandler._authorize_import(p)
        body = {**p["_body"]}
        require(body, "adapter", "source", "filename", "contentBase64")
        categories = entity_type_codes(body.get("entityTypes") or body.get("entityTypeCodes"), body.get("entityType"))
        if not categories:
            raise ValueError("entityTypes must contain at least one shared entity category code")
        body["entityTypes"], body["entityType"] = categories, categories[0]
        import base64
        try: content = base64.b64decode(body["contentBase64"], validate=True)
        except Exception as exc: raise ValueError("contentBase64 must be valid base64") from exc
        format_code = str(body.get("format") or body["filename"].rsplit(".", 1)[-1]).upper()
        if format_code == "JSON": format_code = "GEOJSON"
        if format_code == "SHP": format_code = "SHAPEFILE"
        try:
            from storage import ObjectStore
            scan = ObjectStore.scan_content(content, body["filename"])
            object_key = f"geodata-imports/{new_id()}-{body['filename'].replace('/', '_')}"
            bucket = "myota-geodata-imports"
            stored = ObjectStore().put(bucket, object_key, content, "application/octet-stream")
            body = {**body, "source": {**body["source"], "objectKey": object_key, "bucket": bucket, "sha256": stored["sha256"], "scan": scan}}
        except ImportError:
            body = {**body, "source": {**body["source"], "sha256": __import__("hashlib").sha256(content).hexdigest()}}
        if format_code in TEXT_FORMATS or format_code in {"WFS", "ARCGIS_FEATURESERVER", "SHAPEFILE", "SHP"}:
            prepared = GeoHandler._prepare_import_body({**body, "format": format_code})
            return GeoHandler.store.once(p.get("Idempotency-Key"), lambda: GeoHandler._queue_import(
                prepared, p, body["filename"], lambda: parse_uploaded(format_code, content, body["filename"])))
        try:
            parse_uploaded(format_code, content, body["filename"])
        except ValueError:
            if format_code not in {"OSM_PBF", "PARKSERVE_US"}:
                raise
            run_id = new_id()
            record = {"id": run_id, "programmeSlug": body.get("programmeSlug"), "adapter": body["adapter"], "format": format_code,
                      "entityType": body["entityType"], "entityTypes": categories, "source": body["source"], "filename": body["filename"], "status": "QUEUED",
                      "queuedAt": now(), "binaryObjectPending": True}
            GeoHandler.store.data.setdefault("importRuns", {})[run_id] = record
            GeoHandler.store.event("geodata.import.queued.v1", "import_run", run_id, {"importRunId": run_id, **record})
            return {**record, "queued": True, "_status": 202}
        raise ValueError(f"unsupported upload format {format_code}")

    @staticmethod
    def import_manual(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        return GeoHandler.enqueue_import(_, p)

    @staticmethod
    def list_imports(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        query = parse_qs(urlparse(p.get("_path", "")).query)
        return page_result(list(GeoHandler.store.data.setdefault("importRuns", {}).values()), query)

    @staticmethod
    def get_import(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        run = GeoHandler.store.data.setdefault("importRuns", {})[p["runId"]]
        candidates = [candidate for candidate in GeoHandler.store.data.setdefault("importCandidates", {}).values()
                      if candidate.get("importRunId") == p["runId"]]
        return {**run, "candidateCounts": {
            "total": len(candidates),
            "pending": sum(candidate.get("validationStatus") == "PENDING" for candidate in candidates),
            "confirmed": sum(candidate.get("validationStatus") == "CONFIRMED" for candidate in candidates),
            "processed": sum(candidate.get("validationStatus") == "PROCESSED" for candidate in candidates),
            "rejected": sum(candidate.get("validationStatus") == "REJECTED" for candidate in candidates),
        }}

    @staticmethod
    def list_import_candidates(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        query = parse_qs(urlparse(p.get("_path", "")).query)
        run_id = p["runId"]
        candidates = [GeoHandler._candidate_view(candidate) for candidate in GeoHandler.store.data.setdefault("importCandidates", {}).values()
                      if candidate.get("importRunId") == run_id]
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
        selected = []
        for candidate_id in candidate_ids:
            candidate = candidates.get(str(candidate_id))
            if not candidate or candidate.get("importRunId") != run_id:
                raise ValueError(f"candidate {candidate_id} does not belong to import run")
            if candidate.get("validationStatus") == "PROCESSED":
                continue
            candidate["validationStatus"] = "CONFIRMED"
            candidate["validationNote"] = body.get("note")
            candidate["validatedBy"] = body["reviewerId"]
            candidate["validatedAt"] = now()
            selected.append(str(candidate_id))
        GeoHandler.store.event("geodata.import.candidates.validated.v1", "import_run", run_id,
                               {"importRunId": run_id, "candidateIds": selected, "reviewerId": body["reviewerId"]})
        if p.get("_http"):
            GeoHandler.store.persist()
        return {"importRunId": run_id, "candidateIds": selected, "validationStatus": "CONFIRMED", "_status": 200}

    @staticmethod
    def _process_import_queue(queue_id: str) -> None:
        with GeoHandler.store.lock:
            queue = GeoHandler.store.data.setdefault("importProcessingQueues", {}).get(queue_id)
            if not queue:
                return
            if queue.get("status") == "COMPLETED":
                return
            queue.update({"status": "PROCESSING", "startedAt": now()})
            GeoHandler.store.persist()
        created, updated, errors = [], [], []
        candidates = GeoHandler.store.data.setdefault("importCandidates", {})
        for candidate_id in queue["candidateIds"]:
            try:
                with GeoHandler.store.lock:
                    candidate = candidates.get(candidate_id)
                    if not candidate:
                        raise ValueError("pre-processed candidate no longer exists")
                    if candidate.get("validationStatus") != "CONFIRMED":
                        raise ValueError("candidate must be confirmed before processing")
                    entity_id = candidate.get("entity", {}).get("id")
                    was_existing = entity_id in GeoHandler.store.items
                    GeoHandler._materialize_import_candidate(candidate, queue["targetStatus"], queue["requestedBy"], queue.get("note"))
                    (updated if was_existing else created).append(entity_id)
                    GeoHandler.store.persist()
            except (TypeError, ValueError) as error:
                errors.append({"candidateId": candidate_id, "message": str(error)})
        with GeoHandler.store.lock:
            queue = GeoHandler.store.data["importProcessingQueues"][queue_id]
            queue.update({"status": "COMPLETED" if not errors else "FAILED", "completedAt": now(),
                          "result": {"created": created, "updated": updated, "errors": errors}})
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
            GeoHandler.store.persist()

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
            if candidate.get("validationStatus") != "CONFIRMED":
                raise ValueError("all selected candidates must be confirmed before processing")
            if target_status == "APPROVED":
                GeoHandler._authorize_review(p, candidate.get("entity") or {})
        queue_id = new_id()
        queue = {"id": queue_id, "importRunId": p["runId"], "candidateIds": [str(value) for value in candidate_ids],
                 "targetStatus": target_status, "requestedBy": body["processorId"], "note": body.get("note"),
                 "status": "QUEUED", "requestedAt": now(), "startedAt": None, "completedAt": None, "result": {}}
        GeoHandler.store.data.setdefault("importProcessingQueues", {})[queue_id] = queue
        GeoHandler.store.event("geodata.import.processing.queued.v1", "import_processing_queue", queue_id,
                               {"queueId": queue_id, "importRunId": p["runId"], "candidateIds": queue["candidateIds"],
                                "targetStatus": target_status, "requestedBy": body["processorId"],
                                "natsSubject": "myota.geodata.import.process.v1"})
        if p.get("_http"):
            GeoHandler.store.persist()
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
        return {"type": "FeatureCollection", "bbox": list(bounds), "features": features[:max_features],
                "count": min(len(features), max_features), "truncated": truncated, "cacheTtlSeconds": 60,
                "cacheKey": digest({"bbox": bounds, "programme": programme, "status": status,
                                     "entityVersions": sorted((entity["id"], entity.get("updatedAt")) for entity in GeoHandler.store.items.values())}),
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
        global_admin = "*" in set(claims.get("scp", [])) or any(role.get("role") in {"GLOBAL_ADMIN", "GLOBAL_OPERATOR"} for role in claims.get("roles", []))
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
    ("POST", "/v1/geodata/refresh-schedules"): GeoHandler.create_schedule,
    ("POST", "/v1/geodata/refresh-schedules/{scheduleId}/run"): GeoHandler.refresh_import,
    ("POST", "/v1/geodata/proposals/draw"): GeoHandler.draw_proposal,
    ("POST", "/v1/geodata/conflation/{candidateId}/resolve"): GeoHandler.resolve_conflation,
    ("POST", "/v1/geodata/entities/{entityId}/review"): GeoHandler.review,
    ("POST", "/v1/geodata/entities/{entityId}/status"): GeoHandler.set_status,
    ("POST", "/v1/geodata/entities/{entityId}/geometry"): GeoHandler.update_geometry,
    ("POST", "/v1/geodata/entities/{entityId}/location"): GeoHandler.update_location,
    ("POST", "/v1/geodata/entities/{entityId}/entity-type"): GeoHandler.change_entity_type,
    ("POST", "/v1/geodata/entities/{entityId}/name"): GeoHandler.change_entity_name,
    ("POST", "/v1/geodata/entities/{entityId}/geometry-type"): GeoHandler.change_geometry_type,
    ("POST", "/v1/geodata/entities/{entityId}/delete"): GeoHandler.delete_entity,
}


def seed() -> None:
    GeoHandler.store.hydrate()
    # These are deliberately small, real-world Sevilla examples for local
    # development. The OSM references and source snapshots remain visible so
    # they can be replaced by a licensed import run before production.
    parks = [
        {"id": "00000000-0000-4000-8000-000000000201", "name": "Parque de María Luisa", "status": "APPROVED",
         "sourceRef": "osm-way-19394336", "osmId": 19394336, "osmUrl": "https://www.openstreetmap.org/way/19394336",
         "centroid": {"lat": 37.374771, "lon": -5.9887943},
         "geometry": {"type": "Polygon", "coordinates": [[[-5.9912281, 37.3759294], [-5.9900663, 37.3733659], [-5.9887632, 37.3712101], [-5.9854955, 37.3738139], [-5.986897, 37.3751501], [-5.9884592, 37.37816], [-5.9895231, 37.3787974], [-5.9912281, 37.3759294]]]}},
        {"id": "00000000-0000-4000-8000-000000000202", "name": "Parque del Alamillo", "status": "APPROVED",
         "sourceRef": "osm-way-39729420", "osmId": 39729420, "osmUrl": "https://www.openstreetmap.org/way/39729420",
         "centroid": {"lat": 37.4183029, "lon": -5.9956268},
         "geometry": {"type": "Polygon", "coordinates": [[[-6.0022151, 37.4188257], [-6.0008522, 37.4138541], [-5.9943133, 37.4123107], [-5.9915312, 37.4130282], [-5.9892589, 37.4199065], [-5.9907927, 37.4233188], [-5.9950954, 37.4253328], [-5.9995267, 37.4220373], [-6.0022151, 37.4188257]]]}},
        {"id": "00000000-0000-4000-8000-000000000203", "name": "Parque de los Príncipes", "status": "CANDIDATE",
         "sourceRef": "osm-way-28604482", "osmId": 28604482, "osmUrl": "https://www.openstreetmap.org/way/28604482",
         "centroid": {"lat": 37.3739359, "lon": -6.006222},
         "geometry": {"type": "Polygon", "coordinates": [[[-6.0084926, 37.3743451], [-6.0070003, 37.3726282], [-6.0038193, 37.3721343], [-6.0036725, 37.3730037], [-6.00429, 37.3741577], [-6.005208, 37.3753131], [-6.0062121, 37.3755272], [-6.0084926, 37.3743451]]]}}
    ]
    for park in parks:
        existing = GeoHandler.store.items.get(park["id"])
        if existing and not str(existing.get("sourceRef", "")).startswith(("demo-", "osm-way-")):
            continue
        retrieved_at = existing.get("provenance", {}).get("source", {}).get("retrievedAt") if existing else None
        source = {"name": "OpenStreetMap", "license": "ODbL 1.0", "retrievedAt": retrieved_at or now(), "url": park["osmUrl"]}
        source_feature = {"type": "Feature", "id": f"way/{park['osmId']}", "properties": {"name": park["name"], "sourceRef": park["sourceRef"], "osmUrl": park["osmUrl"], "leisure": "park"}, "geometry": park["geometry"]}
        GeoHandler.store.items[park["id"]] = {
            "id": park["id"], "programmeSlug": "mpota", "entityType": "MUNICIPAL_PARK", "entityTypes": ["MUNICIPAL_PARK"], "entityTypeCodes": ["MUNICIPAL_PARK"], "name": park["name"],
            "status": park["status"], "sourceState": "CURRENT", "sourceRef": park["sourceRef"], "geometry": park["geometry"], "centroid": park["centroid"],
            "provenance": {"adapter": "OSM", "source": source, "sourceKey": "OpenStreetMap", "sourceFeature": source_feature, "tags": {"leisure": "park"}},
            "review": {"reviewerId": "seed-approver", "reviewedAt": now(), "note": "Seeded verified OSM reference"} if park["status"] == "APPROVED" else None,
            "reviewHistory": [{"action": "APPROVED", "reviewerId": "seed-approver", "note": "Seeded verified OSM reference", "occurredAt": now(), "previousStatus": "CANDIDATE"}] if park["status"] == "APPROVED" else [],
            "geometryHistory": [], "createdAt": existing.get("createdAt", now()) if existing else now(), "updatedAt": now()}
        enrich_entity_location(GeoHandler.store.items[park["id"]])
    for entity in GeoHandler.store.items.values():
        enrich_entity_location(entity)


if __name__ == "__main__":
    seed()
    ThreadingHTTPServer(("0.0.0.0", 8003), GeoHandler).serve_forever()
