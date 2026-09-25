"""Pure geodata-pipeline primitives shared by the service and import jobs.

The production service keeps source facts separate from programme decisions.  These
helpers deliberately have no database or network dependency so import workers can
use exactly the same validation, manifest and conflation rules as the API.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from difflib import SequenceMatcher
from typing import Any, Iterable


MAX_IMPORT_FEATURES = int(os.environ.get("GEODATA_MAX_IMPORT_FEATURES", "5000"))
MAX_ATTACHMENTS = 20
MAX_ATTACHMENT_BYTES = 25 * 1024 * 1024
MAX_COORDINATES = int(os.environ.get("GEODATA_MAX_COORDINATES", "50000"))
SUPPORTED_CRS = {"EPSG:4326", "CRS84", "urn:ogc:def:crs:OGC:1.3:CRS84"}
GEOMETRY_TYPE_ALIASES = {"way": "LineString", "linestring": "LineString", "multilinestring": "MultiLineString"}


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _walk_coordinates(value: Any) -> Iterable[list[float]]:
    if isinstance(value, (list, tuple)) and len(value) >= 2 and all(isinstance(item, (int, float)) for item in value[:2]):
        yield [float(value[0]), float(value[1])]
        return
    if isinstance(value, (list, tuple)):
        for child in value:
            yield from _walk_coordinates(child)


def coordinate_count(geometry: dict[str, Any] | None) -> int:
    return sum(1 for _ in _walk_coordinates((geometry or {}).get("coordinates")))


def _valid_ring(ring: Any) -> bool:
    if not isinstance(ring, list) or len(ring) < 4:
        return False
    return ring[0] == ring[-1] and all(isinstance(point, list) and len(point) >= 2 for point in ring)


def _valid_line(coordinates: Any) -> bool:
    return isinstance(coordinates, list) and len(coordinates) >= 2 and all(
        isinstance(point, list) and len(point) >= 2 for point in coordinates
    )


def _valid_multiline(coordinates: Any) -> bool:
    return isinstance(coordinates, list) and len(coordinates) >= 1 and all(_valid_line(line) for line in coordinates)


def validate_geometry(geometry: dict[str, Any] | None, *, max_coordinates: int = MAX_COORDINATES) -> dict[str, Any]:
    if not isinstance(geometry, dict):
        raise ValueError("geometry must be a GeoJSON Point, LineString, MultiLineString, Polygon, or MultiPolygon")
    raw_type = str(geometry.get("type") or "")
    geometry_type = GEOMETRY_TYPE_ALIASES.get(raw_type.casefold(), raw_type)
    if geometry_type not in ("Point", "LineString", "MultiLineString", "Polygon", "MultiPolygon"):
        raise ValueError("geometry must be a GeoJSON Point, LineString, MultiLineString, Polygon, or MultiPolygon")
    coordinates = geometry.get("coordinates")
    count = coordinate_count(geometry)
    if count == 0 or count > max_coordinates:
        raise ValueError(f"geometry must contain between 1 and {max_coordinates} coordinates")
    if geometry_type == "Point" and (not isinstance(coordinates, list) or len(coordinates) < 2):
        raise ValueError("Point geometry must contain longitude and latitude")
    if geometry_type == "LineString" and not _valid_line(coordinates):
        raise ValueError("LineString (way) geometry must contain at least two coordinates")
    if geometry_type == "MultiLineString" and not _valid_multiline(coordinates):
        raise ValueError("MultiLineString geometry must contain one or more lines with at least two coordinates")
    if geometry_type == "Polygon" and (not isinstance(coordinates, list) or not all(_valid_ring(ring) for ring in coordinates)):
        raise ValueError("Polygon geometry must contain closed rings with at least four points")
    if geometry_type == "MultiPolygon" and (not isinstance(coordinates, list) or not all(all(_valid_ring(ring) for ring in polygon) for polygon in coordinates)):
        raise ValueError("MultiPolygon geometry must contain closed polygon rings")
    for longitude, latitude in _walk_coordinates(coordinates):
        if not -180 <= longitude <= 180 or not -90 <= latitude <= 90:
            raise ValueError("geometry coordinates must use WGS84 longitude/latitude ranges")
    return {"type": geometry_type, "coordinates": coordinates}


def _mercator_to_wgs84(point: list[float]) -> list[float]:
    x, y = point[:2]
    longitude = x / 20037508.34 * 180
    latitude = y / 20037508.34 * 180
    latitude = 180 / math.pi * (2 * math.atan(math.exp(latitude * math.pi / 180)) - math.pi / 2)
    return [longitude, latitude]


def _map_coordinates(value: Any, mapper: Any) -> Any:
    if isinstance(value, list) and len(value) >= 2 and all(isinstance(item, (int, float)) for item in value[:2]):
        return mapper([float(value[0]), float(value[1])])
    if isinstance(value, list):
        return [_map_coordinates(child, mapper) for child in value]
    return value


def normalize_geometry(geometry: dict[str, Any] | None, crs: str | None = None) -> dict[str, Any]:
    if not geometry:
        raise ValueError("a geometry is required")
    normalized = dict(geometry)
    normalized.pop("crs", None)
    source_crs = str(crs or "EPSG:4326").upper()
    if source_crs in {"EPSG:3857", "EPSG:900913"}:
        normalized["coordinates"] = _map_coordinates(normalized.get("coordinates"), _mercator_to_wgs84)
    elif source_crs not in {value.upper() for value in SUPPORTED_CRS}:
        raise ValueError(f"unsupported CRS {crs}; use EPSG:4326 or EPSG:3857")
    return validate_geometry(normalized)


def geometry_bbox(geometry: dict[str, Any]) -> tuple[float, float, float, float]:
    coordinates = list(_walk_coordinates(geometry.get("coordinates")))
    longitudes = [point[0] for point in coordinates]
    latitudes = [point[1] for point in coordinates]
    return min(longitudes), min(latitudes), max(longitudes), max(latitudes)


def geometry_centroid(geometry: dict[str, Any]) -> dict[str, float]:
    min_lon, min_lat, max_lon, max_lat = geometry_bbox(geometry)
    return {"lon": round((min_lon + max_lon) / 2, 7), "lat": round((min_lat + max_lat) / 2, 7)}


def bbox_intersection(left: tuple[float, float, float, float], right: tuple[float, float, float, float]) -> float:
    min_lon = max(left[0], right[0])
    min_lat = max(left[1], right[1])
    max_lon = min(left[2], right[2])
    max_lat = min(left[3], right[3])
    if min_lon >= max_lon or min_lat >= max_lat:
        return 0.0
    return (max_lon - min_lon) * (max_lat - min_lat)


def _bbox_area(box: tuple[float, float, float, float]) -> float:
    return max(0.0, box[2] - box[0]) * max(0.0, box[3] - box[1])


def _haversine_km(left: dict[str, float], right: dict[str, float]) -> float:
    radius = 6371.0088
    lat1, lat2 = math.radians(left["lat"]), math.radians(right["lat"])
    d_lat = lat2 - lat1
    d_lon = math.radians(right["lon"] - left["lon"])
    a = math.sin(d_lat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(d_lon / 2) ** 2
    return radius * 2 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1 - a)))


def conflation_score(candidate: dict[str, Any], existing: dict[str, Any]) -> dict[str, Any]:
    candidate_name = str(candidate.get("name", "")).casefold().strip()
    existing_name = str(existing.get("name", "")).casefold().strip()
    name_score = SequenceMatcher(None, candidate_name, existing_name).ratio() if candidate_name and existing_name else 0.0
    candidate_ref = str(candidate.get("sourceRef") or "")
    existing_ref = str(existing.get("sourceRef") or "")
    source_identifier = 1.0 if candidate_ref and candidate_ref == existing_ref else 0.0
    candidate_box = geometry_bbox(candidate["geometry"]) if candidate.get("geometry") else None
    existing_box = geometry_bbox(existing["geometry"]) if existing.get("geometry") else None
    overlap = 0.0
    containment = 0.0
    distance_km = None
    if candidate_box and existing_box:
        intersection = bbox_intersection(candidate_box, existing_box)
        union = _bbox_area(candidate_box) + _bbox_area(existing_box) - intersection
        overlap = intersection / union if union else 0.0
        containment = 1.0 if (candidate_box[0] >= existing_box[0] and candidate_box[1] >= existing_box[1] and candidate_box[2] <= existing_box[2] and candidate_box[3] <= existing_box[3]) else 0.0
        distance_km = _haversine_km(geometry_centroid(candidate["geometry"]), geometry_centroid(existing["geometry"]))
    distance_score = max(0.0, 1.0 - (distance_km / 2.0)) if distance_km is not None else 0.0
    jurisdiction_score = 1.0 if candidate.get("jurisdiction") and candidate.get("jurisdiction") == existing.get("jurisdiction") else 0.0
    score = round((source_identifier * 0.30) + (name_score * 0.20) + (containment * 0.20) + (overlap * 0.15) + (distance_score * 0.10) + (jurisdiction_score * 0.05), 6)
    return {"score": score, "signals": {"sourceIdentifier": source_identifier, "name": round(name_score, 6), "containment": containment, "overlap": round(overlap, 6), "distanceKm": round(distance_km, 6) if distance_km is not None else None, "jurisdiction": jurisdiction_score}}


def validate_attachments(attachments: Any) -> list[dict[str, Any]]:
    if attachments is None:
        return []
    if not isinstance(attachments, list) or len(attachments) > MAX_ATTACHMENTS:
        raise ValueError(f"attachments must be a list of at most {MAX_ATTACHMENTS} metadata records")
    result = []
    for attachment in attachments:
        if not isinstance(attachment, dict) or not attachment.get("name") or not attachment.get("mediaType"):
            raise ValueError("each attachment requires name and mediaType")
        size = int(attachment.get("sizeBytes", 0) or 0)
        if size < 0 or size > MAX_ATTACHMENT_BYTES:
            raise ValueError(f"attachment size must be between 0 and {MAX_ATTACHMENT_BYTES} bytes")
        if not attachment.get("uri") and not attachment.get("sha256"):
            raise ValueError("each attachment requires a URI or sha256 content reference")
        result.append({"name": str(attachment["name"]), "mediaType": str(attachment["mediaType"]), "sizeBytes": size,
                       "uri": attachment.get("uri"), "sha256": attachment.get("sha256"), "license": attachment.get("license"),
                       "caption": attachment.get("caption")})
    return result


def source_manifest(adapter: str, source: dict[str, Any], records: list[dict[str, Any]], run_id: str) -> dict[str, Any]:
    record_digests = {str(record.get("sourceRef") or record.get("id") or index): digest(record) for index, record in enumerate(records)}
    return {"runId": run_id, "adapter": adapter, "source": source, "sourceHash": digest(record_digests),
            "recordCount": len(records), "records": record_digests, "retrievedAt": source.get("retrievedAt"),
            "license": source.get("license"), "attribution": source.get("attribution")}
