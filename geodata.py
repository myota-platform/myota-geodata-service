from __future__ import annotations

from http.server import ThreadingHTTPServer
import math
from typing import Any
from urllib.parse import parse_qs, urlparse

from common import JsonHandler, Store, new_id, now, page_result, require, verify_token
from geodata_pipeline import (MAX_IMPORT_FEATURES, conflation_score, digest, geometry_bbox, geometry_centroid,
                              normalize_geometry, source_manifest, validate_attachments)
from import_adapters import normalize
from location_catalog import build_location_tree, derive_location_codes
from reverse_geocoder import LOCATION_FIELDS, enrich_entity_location


class GeoHandler(JsonHandler):
    service = "geodata-service"
    store = Store("geodata", "GEO_DATABASE_URL")

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
            if role.get("entityType") and role["entityType"] != entity.get("entityType"):
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
        status = query.get("status", [None])[0]
        bounds = GeoHandler._query_bounds(query)
        items = list(GeoHandler.store.items.values())
        if programme:
            items = [i for i in items if i["programmeSlug"] == programme]
        if status:
            items = [i for i in items if i["status"] == status]
        if bounds:
            items = [i for i in items if i.get("geometry") and not (
                (lambda box: box[2] < bounds[0] or box[0] > bounds[2] or box[3] < bounds[1] or box[1] > bounds[3])(geometry_bbox(i["geometry"]))
            )]
        return page_result(items, query)

    @staticmethod
    def adapters(_: JsonHandler, __: dict[str, str]) -> dict[str, Any]:
        return {"adapters": [
            {"code": "PARKSERVE_US", "formats": ["PARKSERVE_US", "GEOJSON"], "requires": ["license", "retrievedAt", "sourceRef"]},
            {"code": "OSM", "formats": ["OSM_PBF", "GEOJSON"], "requiredTags": ["leisure=park", "leisure=nature_reserve", "boundary=protected_area", "landuse=recreation_ground", "highway=path", "highway=footway", "highway=track", "highway=bridleway", "route=hiking"], "attribution": "© OpenStreetMap contributors"},
            {"code": "GOVERNMENT_GIS", "formats": ["WFS", "GEOJSON", "SHAPEFILE", "ARCGIS_FEATURESERVER"], "requires": ["license", "attribution", "sourceFormat"]},
            {"code": "MANUAL", "formats": ["GEOJSON"], "requires": ["programmeSlug", "feature"]}
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
    def _import_features(body: dict[str, Any], run_id: str) -> dict[str, Any]:
        if len(body["features"]) > MAX_IMPORT_FEATURES:
            raise ValueError(f"an import may contain at most {MAX_IMPORT_FEATURES} features")
        adapter = body["adapter"]
        source = dict(body["source"])
        if adapter == "OSM":
            source.setdefault("attribution", "© OpenStreetMap contributors")
        source_key = GeoHandler._source_key(source)
        records = []
        created, updated, skipped, errors, conflation = [], [], [], [], []
        for index, raw_feature in enumerate(body["features"]):
            try:
                feature = normalize(adapter, raw_feature)
                props = feature.get("properties", {})
                source_ref = str(props.get("sourceRef") or props.get("id") or f"record-{index}")
                if props.get("skipReason") == "FILTERED_TAG":
                    skipped.append({"sourceRef": source_ref, "reason": props["skipReason"]})
                    continue
                if not feature.get("geometry"):
                    skipped.append({"sourceRef": source_ref, "reason": "MISSING_GEOMETRY"})
                    continue
                geometry = normalize_geometry(feature["geometry"], props.get("crs"))
                attachments = validate_attachments(raw_feature.get("attachments") or props.get("attachments"))
                existing = next((item for item in GeoHandler.store.items.values()
                                 if source_ref and item.get("sourceRef") == source_ref and item.get("programmeSlug") == body["programmeSlug"]), None)
                occurred_at = now()
                default_entity_type = "TRAIL" if str(props.get("featureType") or "").casefold() == "way" or geometry.get("type") == "LineString" else "MUNICIPAL_PARK"
                entity = {"id": existing["id"] if existing else new_id(), "programmeSlug": body["programmeSlug"],
                          "entityType": props.get("entityType") or default_entity_type, "name": props.get("name", "Unnamed candidate"),
                          "status": existing["status"] if existing else "CANDIDATE", "sourceState": "CURRENT", "geometry": geometry,
                          "centroid": geometry_centroid(geometry), "jurisdiction": props.get("jurisdiction"), "sourceRef": source_ref,
                          "attachments": attachments, "provenance": {"adapter": adapter, "source": source, "sourceKey": source_key,
                                         "sourceFeature": raw_feature, "importRunId": run_id, "sourceHash": digest(raw_feature),
                                         "license": source.get("license"), "attribution": source.get("attribution"),
                                         "retrievedAt": source.get("retrievedAt", occurred_at)},
                          "review": existing.get("review") if existing else None,
                          "reviewHistory": existing.get("reviewHistory", []) if existing else [],
                          "geometryHistory": existing.get("geometryHistory", []) if existing else [],
                          "createdAt": existing.get("createdAt", occurred_at) if existing else occurred_at, "updatedAt": occurred_at}
                GeoHandler._preserve_manual_location(existing, entity)
                enrich_entity_location(entity)
                GeoHandler.store.items[entity["id"]] = entity
                (updated if existing else created).append(entity["id"])
                records.append({"sourceRef": source_ref, "sourceHash": entity["provenance"]["sourceHash"]})
                conflation.extend(GeoHandler._create_conflation_candidates(entity))
            except (TypeError, ValueError) as error:
                errors.append({"index": index, "message": str(error)})
        seen_refs = {record["sourceRef"] for record in records}
        disappeared = GeoHandler._apply_disappearance(body["programmeSlug"], source_key, adapter, seen_refs,
                                                       body.get("disappearancePolicy", "REVIEW_REQUIRED")) if body.get("completeSnapshot") else []
        manifest = source_manifest(adapter, source, records, run_id)
        manifest["sourceKey"] = source_key
        manifest["sourceChanged"] = not any(item.get("sourceHash") == manifest["sourceHash"] for item in GeoHandler.store.data.setdefault("sourceManifests", {}).values() if item.get("sourceKey") == source_key)
        GeoHandler.store.data["sourceManifests"][run_id] = manifest
        result = {"importRunId": run_id, "adapter": adapter, "created": created, "updated": updated, "skipped": skipped,
                  "errors": errors, "disappeared": disappeared, "conflationCandidates": [item["id"] for item in conflation],
                  "manifest": manifest, "_status": 202}
        return result

    @staticmethod
    def import_manual(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        GeoHandler._authorize_import(p)
        body = p["_body"]
        require(body, "programmeSlug", "adapter", "source")
        if "features" not in body or not isinstance(body["features"], list):
            raise ValueError("features must be a list")
        if body["adapter"] not in ("PARKSERVE_US", "OSM", "GOVERNMENT_GIS", "MANUAL"):
            raise ValueError("unsupported adapter")
        def import_run() -> dict[str, Any]:
            run_id = new_id()
            GeoHandler.store.data.setdefault("importRuns", {})[run_id] = {"id": run_id, "programmeSlug": body["programmeSlug"],
                "adapter": body["adapter"], "source": body["source"], "status": "RUNNING", "startedAt": now()}
            result = GeoHandler._import_features(body, run_id)
            GeoHandler.store.data["importRuns"][run_id].update({"status": "COMPLETED" if not result["errors"] else "COMPLETED_WITH_ERRORS",
                "completedAt": now(), "stats": {key: len(result[key]) for key in ("created", "updated", "skipped", "errors", "disappeared")},
                "manifest": result["manifest"]})
            GeoHandler.store.event("geodata.import.accepted.v1", "import_run", run_id, result)
            return result
        return GeoHandler.store.once(p.get("Idempotency-Key"), import_run)

    @staticmethod
    def list_imports(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        query = parse_qs(urlparse(p.get("_path", "")).query)
        return page_result(list(GeoHandler.store.data.setdefault("importRuns", {}).values()), query)

    @staticmethod
    def get_import(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        return GeoHandler.store.data.setdefault("importRuns", {})[p["runId"]]

    @staticmethod
    def create_schedule(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        body = p["_body"]
        require(body, "programmeSlug", "adapter", "source", "intervalSeconds")
        interval = int(body["intervalSeconds"])
        if interval < 300:
            raise ValueError("refresh interval must be at least 300 seconds")
        schedule = {"id": new_id(), "programmeSlug": body["programmeSlug"], "adapter": body["adapter"], "source": body["source"],
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
        body = {**body, "programmeSlug": schedule["programmeSlug"], "adapter": schedule["adapter"], "source": schedule["source"],
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
                                            "entityType": entity.get("entityType"), "sourceRef": entity.get("sourceRef"),
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
        require(body, "programmeSlug", "feature")
        feature = dict(body["feature"])
        feature["attachments"] = body.get("attachments", feature.get("attachments"))
        return GeoHandler.import_manual(None, {"_body": {"programmeSlug": body["programmeSlug"], "adapter": "MANUAL",
            "source": {**(body.get("source") or {}), "name": (body.get("source") or {}).get("name", "Manual proposal"),
                        "license": (body.get("source") or {}).get("license", "programme-supplied")}, "features": [feature]}})

    @staticmethod
    def propose(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        entity = GeoHandler.store.items[p["entityId"]]
        body = p["_body"]
        require(body, "proposerId")
        if entity["status"] not in ("CANDIDATE", "REJECTED"):
            raise ValueError("only candidate or rejected entities can be proposed")
        previous_status = entity["status"]
        entity["status"] = "PROPOSED"
        proposed_at = now()
        entity["review"] = {"proposerId": body["proposerId"], "note": body.get("note"), "proposedAt": proposed_at}
        entity.setdefault("reviewHistory", []).append({"action": "PROPOSED", "proposerId": body["proposerId"],
                                                        "note": body.get("note"), "occurredAt": proposed_at,
                                                        "previousStatus": previous_status})
        entity["updatedAt"] = now()
        GeoHandler.store.event("geodata.entity.proposed.v1", "entity", entity["id"], entity)
        return entity

    @staticmethod
    def review(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        entity = GeoHandler.store.items[p["entityId"]]
        GeoHandler._authorize_review(p, entity)
        body = p["_body"]
        require(body, "decision", "reviewerId")
        if entity["status"] != "PROPOSED":
            raise ValueError("only proposed entities can be reviewed")
        if body["decision"] not in ("APPROVED", "REJECTED"):
            raise ValueError("decision must be APPROVED or REJECTED")
        reviewed_at = now()
        entity["status"] = body["decision"]
        entity["review"] = {**(entity.get("review") or {}), "reviewerId": body["reviewerId"], "note": body.get("note"), "reviewedAt": reviewed_at}
        entity.setdefault("reviewHistory", []).append({"action": body["decision"], "reviewerId": body["reviewerId"],
                                                        "note": body.get("note"), "occurredAt": reviewed_at,
                                                        "previousStatus": "PROPOSED"})
        entity["updatedAt"] = now()
        GeoHandler.store.event("geodata.entity.reviewed.v1", "entity", entity["id"], entity)
        return entity

    @staticmethod
    def set_status(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        entity = GeoHandler.store.items[p["entityId"]]
        GeoHandler._authorize_review(p, entity)
        body = p["_body"]
        require(body, "status", "reviewerId")
        allowed = {"APPROVED", "CANDIDATE", "PROPOSED", "RETIRED", "REJECTED"}
        if body["status"] not in allowed:
            raise ValueError("status must be APPROVED, CANDIDATE, PROPOSED, RETIRED, or REJECTED")
        previous_status = entity["status"]
        target_status = body["status"]
        if previous_status == "APPROVED" and target_status != "RETIRED":
            raise ValueError("approved entities can only be retired to protect historical QSOs")
        if previous_status == "RETIRED" and target_status != "RETIRED":
            raise ValueError("retired entities cannot be reactivated")
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
            raise ValueError("geometry must be a GeoJSON Point, LineString (way), Polygon, or MultiPolygon")
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
    def change_geometry_type(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        entity = GeoHandler.store.items[p["entityId"]]
        GeoHandler._authorize_gis_admin(p, entity, "geodata.geometry.manage")
        body = p["_body"]
        require(body, "geometryType", "editorId")
        target = str(body["geometryType"]).upper()
        target = {"LINESTRING": "WAY"}.get(target, target)
        if target not in {"POINT", "WAY", "POLYGON"}:
            raise ValueError("geometryType must be POINT, WAY, or POLYGON")
        current = str(entity.get("geometry", {}).get("type", "")).upper()
        current = {"LINESTRING": "WAY"}.get(current, current)
        if current == target:
            return entity
        geometry = entity.get("geometry") or {}
        if target == "POINT":
            centre = geometry_centroid(geometry)
            converted = {"type": "Point", "coordinates": [centre["lon"], centre["lat"]]}
        elif target == "WAY":
            if current == "POINT":
                coordinates = geometry.get("coordinates") or []
                if len(coordinates) < 2:
                    raise ValueError("the existing point geometry is invalid")
                lon, lat = float(coordinates[0]), float(coordinates[1])
                delta = 0.0005
                converted = {"type": "LineString", "coordinates": [[lon - delta, lat], [lon + delta, lat]]}
            elif current == "WAY":
                converted = geometry
            else:
                rings = geometry.get("coordinates", [])
                if current == "MULTIPOLYGON":
                    rings = rings[0] if rings else []
                ring = rings[0] if current in {"POLYGON", "MULTIPOLYGON"} and rings else rings
                if len(ring) > 1 and ring[0] == ring[-1]:
                    ring = ring[:-1]
                converted = {"type": "LineString", "coordinates": ring}
        elif current == "POINT":
            coordinates = geometry.get("coordinates") or []
            if len(coordinates) < 2:
                raise ValueError("the existing point geometry is invalid")
            lon, lat = float(coordinates[0]), float(coordinates[1])
            delta = 0.0005
            converted = {"type": "Polygon", "coordinates": [[[lon - delta, lat - delta], [lon + delta, lat - delta],
                                                                  [lon + delta, lat + delta], [lon - delta, lat + delta],
                                                                  [lon - delta, lat - delta]]]}
        elif current == "WAY":
            coordinates = geometry.get("coordinates") or []
            if len(coordinates) < 2:
                raise ValueError("the existing way geometry is invalid")
            longitudes = [float(point[0]) for point in coordinates]
            latitudes = [float(point[1]) for point in coordinates]
            delta = max((max(longitudes) - min(longitudes)) * 0.05, (max(latitudes) - min(latitudes)) * 0.05, 0.0001)
            min_lon, max_lon = min(longitudes) - delta, max(longitudes) + delta
            min_lat, max_lat = min(latitudes) - delta, max(latitudes) + delta
            converted = {"type": "Polygon", "coordinates": [[[min_lon, min_lat], [max_lon, min_lat],
                                                                  [max_lon, max_lat], [min_lon, max_lat],
                                                                  [min_lon, min_lat]]]}
        else:
            rings = geometry.get("coordinates", [])
            if current == "MULTIPOLYGON" and rings and rings[0]:
                rings = rings[0]
            if not rings:
                raise ValueError("the existing polygon geometry is invalid")
            converted = {"type": "Polygon", "coordinates": rings}
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
    def delete_rejected_entity(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        entity = GeoHandler.store.items[p["entityId"]]
        GeoHandler._authorize_gis_admin(p, entity, "geodata.delete")
        if entity.get("status") != "REJECTED":
            raise ValueError("only rejected entities can be permanently deleted")
        entity_id = entity["id"]
        for candidate_id, candidate in list(GeoHandler.store.data.setdefault("conflationCandidates", {}).items()):
            if entity_id in (candidate.get("leftEntityId"), candidate.get("rightEntityId")):
                GeoHandler.store.data["conflationCandidates"].pop(candidate_id, None)
        GeoHandler.store.items.pop(entity_id, None)
        GeoHandler.store.events[:] = [event for event in GeoHandler.store.events
                                      if event.get("aggregate", {}).get("id") != entity_id]
        return {"entityId": entity_id, "deleted": True, "_status": 204}


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
    ("GET", "/v1/geodata/refresh-schedules"): GeoHandler.list_schedules,
    ("GET", "/v1/geodata/conflation"): GeoHandler.list_conflation,
    ("POST", "/v1/geodata/imports/manual"): GeoHandler.import_manual,
    ("POST", "/v1/geodata/imports"): GeoHandler.import_manual,
    ("POST", "/v1/geodata/refresh-schedules"): GeoHandler.create_schedule,
    ("POST", "/v1/geodata/refresh-schedules/{scheduleId}/run"): GeoHandler.refresh_import,
    ("POST", "/v1/geodata/proposals/draw"): GeoHandler.draw_proposal,
    ("POST", "/v1/geodata/conflation/{candidateId}/resolve"): GeoHandler.resolve_conflation,
    ("POST", "/v1/geodata/entities/{entityId}/propose"): GeoHandler.propose,
    ("POST", "/v1/geodata/entities/{entityId}/review"): GeoHandler.review,
    ("POST", "/v1/geodata/entities/{entityId}/status"): GeoHandler.set_status,
    ("POST", "/v1/geodata/entities/{entityId}/geometry"): GeoHandler.update_geometry,
    ("POST", "/v1/geodata/entities/{entityId}/location"): GeoHandler.update_location,
    ("POST", "/v1/geodata/entities/{entityId}/geometry-type"): GeoHandler.change_geometry_type,
    ("POST", "/v1/geodata/entities/{entityId}/delete"): GeoHandler.delete_rejected_entity,
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
            "id": park["id"], "programmeSlug": "mpota", "entityType": "MUNICIPAL_PARK", "name": park["name"],
            "status": park["status"], "sourceState": "CURRENT", "sourceRef": park["sourceRef"], "geometry": park["geometry"], "centroid": park["centroid"],
            "provenance": {"adapter": "OSM", "source": source, "sourceKey": "OpenStreetMap", "sourceFeature": source_feature, "tags": {"leisure": "park"}},
            "review": {"reviewerId": "seed-approver", "reviewedAt": now(), "note": "Seeded verified OSM reference"} if park["status"] == "APPROVED" else None,
            "reviewHistory": [{"action": "APPROVED", "reviewerId": "seed-approver", "note": "Seeded verified OSM reference", "occurredAt": now(), "previousStatus": "PROPOSED"}] if park["status"] == "APPROVED" else [],
            "geometryHistory": [], "createdAt": existing.get("createdAt", now()) if existing else now(), "updatedAt": now()}
        enrich_entity_location(GeoHandler.store.items[park["id"]])
    for entity in GeoHandler.store.items.values():
        enrich_entity_location(entity)


if __name__ == "__main__":
    seed()
    ThreadingHTTPServer(("0.0.0.0", 8003), GeoHandler).serve_forever()
