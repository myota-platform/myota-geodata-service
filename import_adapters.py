"""Canonical geodata adapter contracts.

Adapters deliberately normalize source facts only. Programme eligibility and approval
remain policy owned by the programme/geodata review workflow.
"""
from __future__ import annotations

from typing import Any


class Adapter:
    code = "BASE"

    def normalize(self, feature: dict[str, Any]) -> dict[str, Any]:
        properties = dict(feature.get("properties") or {})
        return {"type": "Feature", "geometry": feature.get("geometry"), "properties": properties}


class ParkServeUSAdapter(Adapter):
    code = "PARKSERVE_US"


class OSMAdapter(Adapter):
    code = "OSM"

    def normalize(self, feature: dict[str, Any]) -> dict[str, Any]:
        value = super().normalize(feature)
        tags = value["properties"].get("tags", value["properties"])
        value["properties"]["tags"] = tags
        value["properties"].setdefault("sourceRef", value["properties"].get("osm_id") or value["properties"].get("id"))
        return value


class GovernmentGISAdapter(Adapter):
    code = "GOVERNMENT_GIS"


class ManualAdapter(Adapter):
    code = "MANUAL"


ADAPTERS = {c.code: c for c in (ParkServeUSAdapter, OSMAdapter, GovernmentGISAdapter, ManualAdapter)}


def normalize(adapter_code: str, feature: dict[str, Any]) -> dict[str, Any]:
    try:
        return ADAPTERS[adapter_code]().normalize(feature)
    except KeyError as exc:
        raise ValueError(f"unsupported adapter: {adapter_code}") from exc

