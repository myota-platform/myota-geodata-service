from __future__ import annotations

from http.server import ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from common import JsonHandler, Store, new_id, now, page_result, require, verify_token
from import_adapters import normalize


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
    def list_entities(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        query = parse_qs(urlparse(p.get("_path", "")).query)
        programme = query.get("programme", [None])[0]
        status = query.get("status", [None])[0]
        items = list(GeoHandler.store.items.values())
        if programme:
            items = [i for i in items if i["programmeSlug"] == programme]
        if status:
            items = [i for i in items if i["status"] == status]
        return page_result(items, query)

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
    def import_manual(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        body = p["_body"]
        require(body, "programmeSlug", "adapter", "source", "features")
        if body["adapter"] not in ("PARKSERVE_US", "OSM", "GOVERNMENT_GIS", "MANUAL"):
            raise ValueError("unsupported adapter")
        def import_run() -> dict[str, Any]:
            run_id = new_id()
            created, updated, skipped = [], [], []
            for raw_feature in body["features"]:
                feature = normalize(body["adapter"], raw_feature)
                props = feature.get("properties", {})
                source_ref = str(props.get("sourceRef") or props.get("id") or "")
                existing = next((e for e in GeoHandler.store.items.values() if source_ref and e.get("sourceRef") == source_ref and e["programmeSlug"] == body["programmeSlug"]), None)
                entity = {"id": existing["id"] if existing else new_id(), "programmeSlug": body["programmeSlug"],
                          "entityType": props.get("entityType", "MUNICIPAL_PARK"), "name": props.get("name", "Unnamed candidate"),
                          "status": existing["status"] if existing else "CANDIDATE", "geometry": feature.get("geometry"),
                          "centroid": props.get("centroid"), "sourceRef": source_ref or None,
                          "provenance": {"adapter": body["adapter"], "source": body["source"], "sourceFeature": raw_feature,
                                         "importRunId": run_id,
                                         "license": body["source"].get("license"), "retrievedAt": body["source"].get("retrievedAt", now())},
                          "review": existing.get("review") if existing else None,
                          "reviewHistory": existing.get("reviewHistory", []) if existing else [],
                          "geometryHistory": existing.get("geometryHistory", []) if existing else [],
                          "createdAt": existing.get("createdAt", now()) if existing else now(), "updatedAt": now()}
                if existing:
                    GeoHandler.store.items[entity["id"]] = entity
                    updated.append(entity["id"])
                else:
                    GeoHandler.store.items[entity["id"]] = entity
                    created.append(entity["id"])
                if not feature.get("geometry"):
                    skipped.append(entity["id"])
            result = {"importRunId": run_id, "adapter": body["adapter"], "created": created, "updated": updated, "skipped": skipped, "_status": 202}
            GeoHandler.store.event("geodata.import.accepted.v1", "import_run", run_id, result)
            return result
        return GeoHandler.store.once(p.get("Idempotency-Key"), import_run)

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
    def update_geometry(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        entity = GeoHandler.store.items[p["entityId"]]
        GeoHandler._authorize_review(p, entity)
        body = p["_body"]
        require(body, "geometry", "editorId")
        geometry = body["geometry"]
        if not isinstance(geometry, dict) or geometry.get("type") not in ("Point", "Polygon", "MultiPolygon") or "coordinates" not in geometry:
            raise ValueError("geometry must be a GeoJSON Point, Polygon, or MultiPolygon")
        previous = entity.get("geometry")
        entity.setdefault("geometryHistory", []).append({"editorId": body["editorId"], "note": body.get("note"),
                                                          "geometry": previous, "editedAt": now()})
        entity["geometry"] = geometry
        entity["updatedAt"] = now()
        GeoHandler.store.event("geodata.entity.geometry-updated.v1", "entity", entity["id"],
                               {"entityId": entity["id"], "editorId": body["editorId"], "note": body.get("note"), "geometry": geometry})
        return entity


GeoHandler.routes = {
    ("GET", "/v1/geodata/entities"): GeoHandler.list_entities,
    ("GET", "/v1/geodata/entities/{entityId}"): GeoHandler.get_entity,
    ("GET", "/v1/geodata/entities/{entityId}/audit"): GeoHandler.audit_entity,
    ("POST", "/v1/geodata/imports/manual"): GeoHandler.import_manual,
    ("POST", "/v1/geodata/entities/{entityId}/propose"): GeoHandler.propose,
    ("POST", "/v1/geodata/entities/{entityId}/review"): GeoHandler.review,
    ("POST", "/v1/geodata/entities/{entityId}/geometry"): GeoHandler.update_geometry,
}


def seed() -> None:
    GeoHandler.store.hydrate()
    if GeoHandler.store.items:
        return
    GeoHandler.store.items["00000000-0000-4000-8000-000000000201"] = {
        "id": "00000000-0000-4000-8000-000000000201", "programmeSlug": "mpota", "entityType": "MUNICIPAL_PARK",
        "name": "Demo Verified Riverside Park", "status": "APPROVED", "sourceRef": "demo-approved-1",
        "geometry": {"type": "Polygon", "coordinates": [[[-3.71, 40.41], [-3.70, 40.41], [-3.70, 40.42], [-3.71, 40.42], [-3.71, 40.41]]]},
        "centroid": {"lat": 40.415, "lon": -3.705}, "provenance": {"adapter": "GOVERNMENT_GIS", "source": {"name": "Demo municipal GIS", "license": "ODbL-compatible demo"}},
        "review": {"reviewerId": "demo-approver", "reviewedAt": now()},
        "reviewHistory": [{"action": "APPROVED", "reviewerId": "demo-approver", "note": "Demo verified reference", "occurredAt": now(), "previousStatus": "PROPOSED"}],
        "geometryHistory": [], "createdAt": now(), "updatedAt": now()}
    GeoHandler.store.items["00000000-0000-4000-8000-000000000202"] = {
        "id": "00000000-0000-4000-8000-000000000202", "programmeSlug": "mpota", "entityType": "MUNICIPAL_PARK",
        "name": "Demo Candidate Neighbourhood Park", "status": "CANDIDATE", "sourceRef": "demo-candidate-1",
        "geometry": {"type": "Point", "coordinates": [-3.69, 40.425]}, "centroid": {"lat": 40.425, "lon": -3.69},
        "provenance": {"adapter": "OSM", "source": {"name": "OpenStreetMap", "license": "ODbL 1.0", "retrievedAt": now()}, "tags": {"leisure": "park"}},
        "review": None, "createdAt": now(), "updatedAt": now()}


if __name__ == "__main__":
    seed()
    ThreadingHTTPServer(("0.0.0.0", 8003), GeoHandler).serve_forever()
