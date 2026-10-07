"""Canonical geodata adapter contracts.

Adapters deliberately normalize source facts only. Programme eligibility and approval
remain policy owned by the programme/geodata review workflow.
"""

from __future__ import annotations

import re
from typing import Any

from geodata_pipeline import normalize_geometry


OSM_REQUIRED_TAGS = (
    "leisure=park",
    "leisure=nature_reserve",
    "boundary=protected_area",
    "landuse=recreation_ground",
    "highway=path",
    "highway=footway",
    "highway=track",
    "highway=bridleway",
    "route=hiking",
)
GOVERNMENT_GIS_FORMATS = (
    "GEOJSON",
    "WFS",
    "SHAPEFILE",
    "ARCGIS_FEATURESERVER",
)

# GIS providers use a surprisingly wide range of field names for the human-readable
# feature name. Keep the aliases here, at the adapter boundary, so every importer
# produces the same canonical ``properties.name`` value for preprocessing.
NAME_PROPERTY_ALIASES = (
    "name",
    "official_name",
    "site_name",
    "park_name",
    "reserve_name",
    "trail_name",
    "local_name",
    "nombre",
    "denominacion",
    "designation",
    "title",
    "label",
)
_NAME_KEY_TOKENS = ("name", "nombre", "denom", "designation", "title", "label")
_NON_NAME_KEY_TOKENS = (
    "code",
    "id",
    "type",
    "area",
    "hect",
    "lat",
    "lon",
    "lng",
    "region",
    "country",
    "province",
    "county",
    "municip",
    "city",
    "state",
    "admin",
    "source",
    "ref",
    "url",
    "license",
)
_EMPTY_NAME_VALUES = {
    "",
    "-",
    "--",
    "n/a",
    "na",
    "none",
    "null",
    "unknown",
    "unnamed",
}


def _normalise_property_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).casefold())


def _name_value(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = " ".join(value.split())
    if cleaned.casefold() in _EMPTY_NAME_VALUES:
        return None
    # A source attribute containing a whole document is not a useful display name.
    return cleaned[:300] if cleaned else None


def infer_feature_name(properties: dict[str, Any]) -> str | None:
    """Find a display name without treating source codes as names.

    Explicit aliases win over heuristic matches. Nested OSM ``tags`` are inspected
    as well, while the caller retains every original source property unchanged.
    """
    pairs = list(properties.items())
    tags = properties.get("tags")
    if isinstance(tags, dict):
        pairs.extend(tags.items())

    by_key: dict[str, list[Any]] = {}
    for key, value in pairs:
        by_key.setdefault(_normalise_property_key(key), []).append(value)

    for alias in NAME_PROPERTY_ALIASES:
        for value in by_key.get(_normalise_property_key(alias), []):
            candidate = _name_value(value)
            if candidate:
                return candidate

    for key, value in pairs:
        normalized_key = _normalise_property_key(key)
        if not any(token in normalized_key for token in _NAME_KEY_TOKENS):
            continue
        if any(token in normalized_key for token in _NON_NAME_KEY_TOKENS):
            continue
        candidate = _name_value(value)
        if candidate:
            return candidate
    return None


def ensure_feature_name(properties: dict[str, Any]) -> dict[str, Any]:
    """Populate canonical ``name`` while preserving source attributes."""
    if not _name_value(properties.get("name")):
        inferred = infer_feature_name(properties)
        if inferred:
            properties["name"] = inferred
    return properties


class Adapter:
    code = "BASE"

    def normalize(self, feature: dict[str, Any]) -> dict[str, Any]:
        is_way_record = str(feature.get("type") or "").casefold() == "way"
        properties = dict(
            feature.get("properties") or feature.get("tags") or {}
        )
        if is_way_record:
            properties.setdefault("featureType", "way")
            if feature.get("id") is not None:
                properties.setdefault("id", feature["id"])
        ensure_feature_name(properties)
        raw_geometry = feature.get("geometry")
        if (
            not raw_geometry
            and is_way_record
            and feature.get("coordinates") is not None
        ):
            raw_geometry = {
                "type": "LineString",
                "coordinates": feature["coordinates"],
            }
        geometry = (
            normalize_geometry(
                raw_geometry,
                feature.get("crs")
                or feature.get("spatialReference")
                or properties.get("crs"),
            )
            if raw_geometry
            else None
        )
        return {
            "type": "Feature",
            "geometry": geometry,
            "properties": properties,
        }


class ParkServeUSAdapter(Adapter):
    code = "PARKSERVE_US"

    def normalize(self, feature: dict[str, Any]) -> dict[str, Any]:
        value = super().normalize(feature)
        properties = value["properties"]
        properties.setdefault(
            "sourceRef",
            properties.get("parkserve_id")
            or properties.get("park_id")
            or properties.get("id"),
        )
        properties.setdefault(
            "sourceRecordType", "ParkServe US protected/open-space record"
        )
        return value


class OSMAdapter(Adapter):
    code = "OSM"

    def normalize(self, feature: dict[str, Any]) -> dict[str, Any]:
        value = super().normalize(feature)
        tags = value["properties"].get("tags", value["properties"])
        value["properties"]["tags"] = tags
        value["properties"].setdefault(
            "sourceRef",
            value["properties"].get("osm_id") or value["properties"].get("id"),
        )
        value["properties"].setdefault(
            "attribution", "© OpenStreetMap contributors"
        )
        value["properties"]["sourceUrl"] = value["properties"].get(
            "sourceUrl"
        ) or value["properties"].get("osm_url")
        value["properties"]["matchesRequiredTag"] = any(
            tags.get(key) == expected
            for key, expected in (
                tag.split("=", 1) for tag in OSM_REQUIRED_TAGS
            )
        )
        if not value["properties"]["matchesRequiredTag"]:
            value["properties"]["skipReason"] = "FILTERED_TAG"
        return value


class GovernmentGISAdapter(Adapter):
    code = "GOVERNMENT_GIS"

    def normalize(self, feature: dict[str, Any]) -> dict[str, Any]:
        properties = dict(feature.get("properties") or {})
        source_format = str(
            properties.get("sourceFormat")
            or properties.get("format")
            or "GEOJSON"
        ).upper()
        if source_format not in GOVERNMENT_GIS_FORMATS:
            raise ValueError(
                f"unsupported government GIS format {source_format}"
            )
        geometry = feature.get("geometry")
        if source_format in ("ARCGIS", "ARCGIS_FEATURESERVER") and isinstance(
            geometry, dict
        ):
            if "rings" in geometry:
                geometry = {
                    "type": "Polygon",
                    "coordinates": geometry["rings"],
                }
            elif "x" in geometry and "y" in geometry:
                geometry = {
                    "type": "Point",
                    "coordinates": [geometry["x"], geometry["y"]],
                }
        normalized = (
            normalize_geometry(
                geometry,
                feature.get("crs")
                or feature.get("spatialReference")
                or properties.get("crs"),
            )
            if geometry
            else None
        )
        properties["sourceFormat"] = source_format
        properties.setdefault(
            "sourceRef",
            properties.get("objectId")
            or properties.get("OBJECTID")
            or properties.get("id"),
        )
        properties.setdefault(
            "sourceRecordType", "local-government GIS feature"
        )
        ensure_feature_name(properties)
        return {
            "type": "Feature",
            "geometry": normalized,
            "properties": properties,
        }


class ManualAdapter(Adapter):
    code = "MANUAL"


ADAPTERS = {
    c.code: c
    for c in (
        ParkServeUSAdapter,
        OSMAdapter,
        GovernmentGISAdapter,
        ManualAdapter,
    )
}


def normalize(adapter_code: str, feature: dict[str, Any]) -> dict[str, Any]:
    try:
        return ADAPTERS[adapter_code]().normalize(feature)
    except KeyError as exc:
        raise ValueError(f"unsupported adapter: {adapter_code}") from exc
