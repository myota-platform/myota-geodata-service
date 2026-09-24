"""Canonical geodata adapter contracts.

Adapters deliberately normalize source facts only. Programme eligibility and approval
remain policy owned by the programme/geodata review workflow.
"""
from __future__ import annotations

from typing import Any

from geodata_pipeline import normalize_geometry


OSM_REQUIRED_TAGS = ("leisure=park", "leisure=nature_reserve", "boundary=protected_area", "landuse=recreation_ground")
GOVERNMENT_GIS_FORMATS = ("GEOJSON", "WFS", "SHAPEFILE", "ARCGIS_FEATURESERVER")


class Adapter:
    code = "BASE"

    def normalize(self, feature: dict[str, Any]) -> dict[str, Any]:
        properties = dict(feature.get("properties") or {})
        geometry = normalize_geometry(feature.get("geometry"), feature.get("crs") or properties.get("crs")) if feature.get("geometry") else None
        return {"type": "Feature", "geometry": geometry, "properties": properties}


class ParkServeUSAdapter(Adapter):
    code = "PARKSERVE_US"

    def normalize(self, feature: dict[str, Any]) -> dict[str, Any]:
        value = super().normalize(feature)
        properties = value["properties"]
        properties.setdefault("sourceRef", properties.get("parkserve_id") or properties.get("park_id") or properties.get("id"))
        properties.setdefault("sourceRecordType", "ParkServe US protected/open-space record")
        return value


class OSMAdapter(Adapter):
    code = "OSM"

    def normalize(self, feature: dict[str, Any]) -> dict[str, Any]:
        value = super().normalize(feature)
        tags = value["properties"].get("tags", value["properties"])
        value["properties"]["tags"] = tags
        value["properties"].setdefault("sourceRef", value["properties"].get("osm_id") or value["properties"].get("id"))
        value["properties"].setdefault("attribution", "© OpenStreetMap contributors")
        value["properties"]["sourceUrl"] = value["properties"].get("sourceUrl") or value["properties"].get("osm_url")
        value["properties"]["matchesRequiredTag"] = any(tags.get(key) == expected for key, expected in (tag.split("=", 1) for tag in OSM_REQUIRED_TAGS))
        if not value["properties"]["matchesRequiredTag"]:
            value["properties"]["skipReason"] = "FILTERED_TAG"
        return value


class GovernmentGISAdapter(Adapter):
    code = "GOVERNMENT_GIS"

    def normalize(self, feature: dict[str, Any]) -> dict[str, Any]:
        properties = dict(feature.get("properties") or {})
        source_format = str(properties.get("sourceFormat") or properties.get("format") or "GEOJSON").upper()
        if source_format not in GOVERNMENT_GIS_FORMATS:
            raise ValueError(f"unsupported government GIS format {source_format}")
        geometry = feature.get("geometry")
        if source_format in ("ARCGIS", "ARCGIS_FEATURESERVER") and isinstance(geometry, dict):
            if "rings" in geometry:
                geometry = {"type": "Polygon", "coordinates": geometry["rings"]}
            elif "x" in geometry and "y" in geometry:
                geometry = {"type": "Point", "coordinates": [geometry["x"], geometry["y"]]}
        normalized = normalize_geometry(geometry, feature.get("crs") or properties.get("crs")) if geometry else None
        properties["sourceFormat"] = source_format
        properties.setdefault("sourceRef", properties.get("objectId") or properties.get("OBJECTID") or properties.get("id"))
        properties.setdefault("sourceRecordType", "local-government GIS feature")
        return {"type": "Feature", "geometry": normalized, "properties": properties}


class ManualAdapter(Adapter):
    code = "MANUAL"


ADAPTERS = {c.code: c for c in (ParkServeUSAdapter, OSMAdapter, GovernmentGISAdapter, ManualAdapter)}


def normalize(adapter_code: str, feature: dict[str, Any]) -> dict[str, Any]:
    try:
        return ADAPTERS[adapter_code]().normalize(feature)
    except KeyError as exc:
        raise ValueError(f"unsupported adapter: {adapter_code}") from exc
