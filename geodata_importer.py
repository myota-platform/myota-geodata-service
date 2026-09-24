"""Allowlisted source fetcher for scheduled geodata imports.

This worker performs network I/O outside the geodata API process. It accepts
GeoJSON/WFS and ArcGIS FeatureServer responses, preserving the configured source
metadata, then submits a complete snapshot to the service.
"""
from __future__ import annotations

import json
import os
import sys
from urllib.parse import urlparse
from urllib.request import Request, urlopen


def _allowed(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    allowlist = {item.strip().lower() for item in os.environ.get("GEODATA_SOURCE_ALLOWLIST", "").split(",") if item.strip()}
    return bool(host and allowlist and any(host == item or host.endswith("." + item) for item in allowlist))


def fetch_json(url: str) -> dict:
    if not _allowed(url):
        raise RuntimeError("source host is not in GEODATA_SOURCE_ALLOWLIST")
    request = Request(url, headers={"Accept": "application/geo+json, application/json", "User-Agent": "MyOTA-geodata-importer/1.0"})
    with urlopen(request, timeout=30) as response:  # nosec B310 - host is explicitly allowlisted above
        return json.loads(response.read().decode("utf-8"))


def features_from_document(document: dict, source_format: str) -> list[dict]:
    if isinstance(document, dict) and document.get("type") == "FeatureCollection":
        return list(document.get("features") or [])
    if source_format == "ARCGIS_FEATURESERVER" and isinstance(document, dict):
        features = []
        for item in document.get("features") or []:
            geometry = item.get("geometry") or {}
            attributes = dict(item.get("attributes") or {})
            if "rings" in geometry:
                geometry = {"type": "Polygon", "coordinates": geometry["rings"]}
            elif "x" in geometry and "y" in geometry:
                geometry = {"type": "Point", "coordinates": [geometry["x"], geometry["y"]]}
            features.append({"type": "Feature", "properties": attributes, "geometry": geometry})
        return features
    raise ValueError("source response must be GeoJSON FeatureCollection or ArcGIS FeatureServer JSON")


def run(spec: dict) -> dict:
    source = dict(spec["source"])
    document = fetch_json(source["url"])
    source_format = str(source.get("format") or "GEOJSON").upper()
    body = {"programmeSlug": spec["programmeSlug"], "adapter": spec["adapter"], "source": source,
            "features": features_from_document(document, source_format), "completeSnapshot": True,
            "disappearancePolicy": spec.get("disappearancePolicy", "REVIEW_REQUIRED")}
    payload = json.dumps(body).encode("utf-8")
    request = Request(spec["geodataUrl"].rstrip("/") + "/v1/geodata/imports", data=payload, method="POST",
                      headers={"Content-Type": "application/json", "Authorization": "Bearer " + spec["accessToken"],
                               "Idempotency-Key": spec.get("idempotencyKey", source.get("sourceKey", source["url"]))})
    with urlopen(request, timeout=30) as response:  # nosec B310 - geodataUrl is deployment configuration
        return json.loads(response.read().decode("utf-8"))


if __name__ == "__main__":
    spec = json.loads(os.environ.get("GEODATA_IMPORT_SPEC", "{}"))
    spec.setdefault("geodataUrl", os.environ.get("MYOTA_GEODATA_URL", "http://geodata:8003"))
    if not spec:
        raise SystemExit("GEODATA_IMPORT_SPEC is required")
    json.dump(run(spec), sys.stdout)
