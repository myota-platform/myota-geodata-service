"""Text and archive decoders for the geodata intake boundary.

The decoder produces ordinary GeoJSON features.  It deliberately does not
decide lifecycle status or programme eligibility; those decisions belong to
the import service and the review workflow.
"""
from __future__ import annotations

import io
import json
import zipfile
from pathlib import PurePosixPath
from typing import Any
from xml.etree import ElementTree


TEXT_FORMATS = {"GEOJSON", "KML", "GPX"}
BINARY_FORMATS = {"SHAPEFILE", "SHP", "OSM_PBF", "PARKSERVE_US"}
SUPPORTED_FORMATS = TEXT_FORMATS | BINARY_FORMATS | {"WFS", "ARCGIS_FEATURESERVER"}


def _feature(geometry: dict[str, Any] | None, properties: dict[str, Any] | None = None, feature_id: Any = None) -> dict[str, Any]:
    value: dict[str, Any] = {"type": "Feature", "geometry": geometry, "properties": properties or {}}
    if feature_id is not None:
        value["id"] = feature_id
    return value


def _geojson(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return value
    if not isinstance(value, dict):
        raise ValueError("GeoJSON document must be an object")
    kind = value.get("type")
    if kind == "FeatureCollection":
        features = value.get("features")
        if not isinstance(features, list):
            raise ValueError("GeoJSON FeatureCollection must contain a features array")
        return features
    if kind == "Feature":
        return [value]
    if isinstance(value.get("geometry"), dict):
        return [_feature(value["geometry"], value.get("properties"), value.get("id"))]
    if kind in {"Point", "LineString", "MultiLineString", "Polygon", "MultiPolygon"}:
        return [_feature(value)]
    raise ValueError("unsupported GeoJSON document type")


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _kml_geometry(node: ElementTree.Element) -> dict[str, Any] | None:
    points: list[list[float]] = []
    for child in node.iter():
        if _local_name(child.tag) != "coordinates" or not (child.text or "").strip():
            continue
        for token in (child.text or "").replace("\n", " ").split():
            values = token.split(",")
            if len(values) >= 2:
                points.append([float(values[0]), float(values[1])])
    if not points:
        return None
    names = {_local_name(child.tag) for child in node.iter()}
    if "Point" in names:
        return {"type": "Point", "coordinates": points[0]}
    if "Polygon" in names:
        ring = points if points[0] == points[-1] else points + [points[0]]
        return {"type": "Polygon", "coordinates": [ring]}
    if "MultiGeometry" in names:
        lines = []
        for line in node.iter():
            if _local_name(line.tag) != "LineString":
                continue
            geometry = _kml_geometry(line)
            if geometry and geometry["type"] == "LineString":
                lines.append(geometry["coordinates"])
        if lines:
            return {"type": "MultiLineString", "coordinates": lines}
    return {"type": "LineString", "coordinates": points}


def parse_kml(content: str) -> list[dict[str, Any]]:
    try:
        root = ElementTree.fromstring(content)
    except ElementTree.ParseError as exc:
        raise ValueError("KML is not valid XML") from exc
    features = []
    for placemark in root.iter():
        if _local_name(placemark.tag) != "Placemark":
            continue
        name = next((child.text.strip() for child in placemark if _local_name(child.tag) == "name" and child.text), None)
        properties = {"name": name} if name else {}
        geometry = _kml_geometry(placemark)
        if geometry:
            features.append(_feature(geometry, properties))
    if not features:
        raise ValueError("KML contains no supported Placemark geometry")
    return features


def _gpx_point(node: ElementTree.Element) -> list[float]:
    return [float(node.attrib["lon"]), float(node.attrib["lat"])]


def parse_gpx(content: str) -> list[dict[str, Any]]:
    try:
        root = ElementTree.fromstring(content)
    except ElementTree.ParseError as exc:
        raise ValueError("GPX is not valid XML") from exc
    features = []
    for waypoint in root.iter():
        if _local_name(waypoint.tag) not in {"wpt", "rtept"}:
            continue
        properties = {}
        for child in waypoint:
            if _local_name(child.tag) in {"name", "desc", "type"} and child.text:
                properties[_local_name(child.tag)] = child.text.strip()
        features.append(_feature({"type": "Point", "coordinates": _gpx_point(waypoint)}, properties))
    for track in root.iter():
        if _local_name(track.tag) != "trkseg":
            continue
        points = [_gpx_point(node) for node in track if _local_name(node.tag) == "trkpt"]
        if len(points) >= 2:
            features.append(_feature({"type": "LineString", "coordinates": points}, {}))
    if not features:
        raise ValueError("GPX contains no supported waypoint, route, or track geometry")
    return features


def parse_text(format_code: str, content: str) -> list[dict[str, Any]]:
    code = format_code.upper()
    if code == "GEOJSON" or code in {"WFS", "ARCGIS_FEATURESERVER"}:
        try:
            return _geojson(json.loads(content))
        except json.JSONDecodeError as exc:
            raise ValueError("GeoJSON content is not valid JSON") from exc
    if code == "KML":
        return parse_kml(content)
    if code == "GPX":
        return parse_gpx(content)
    raise ValueError(f"{code} is a binary or remote-source format; upload a file or use its source URL")


def parse_uploaded(format_code: str, content: bytes, filename: str = "upload") -> list[dict[str, Any]]:
    code = format_code.upper().replace(".SHP", "SHAPEFILE")
    if code in TEXT_FORMATS or code in {"WFS", "ARCGIS_FEATURESERVER"}:
        return parse_text(code, content.decode("utf-8-sig"))
    if code in {"SHP", "SHAPEFILE"}:
        try:
            import shapefile  # pyshp, optional in the lightweight service image
        except ImportError as exc:
            raise ValueError("Shapefile support requires the pyshp package") from exc
        raw = content
        if filename.lower().endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                shp_name = next((name for name in archive.namelist() if name.lower().endswith(".shp")), None)
                if not shp_name:
                    raise ValueError("shapefile archive does not contain a .shp member")
                raw = archive.read(shp_name)
                # pyshp needs the sibling DBF/SHX streams as well; use an in-memory
                # reader when all sidecars are present.
                stem = str(PurePosixPath(shp_name).with_suffix(""))
                reader = shapefile.Reader(shp=io.BytesIO(raw),
                                          shx=io.BytesIO(archive.read(stem + ".shx")) if stem + ".shx" in archive.namelist() else None,
                                          dbf=io.BytesIO(archive.read(stem + ".dbf")) if stem + ".dbf" in archive.namelist() else None)
        else:
            raise ValueError("upload a .zip containing the .shp, .shx, and .dbf sidecars")
        fields = [field[0] for field in reader.fields[1:]]
        return [_feature(shape.__geo_interface__, dict(zip(fields, record))) for shape, record in zip(reader.shapes(), reader.records())]
    raise ValueError(f"binary format {code} is accepted for queued processing but has no local decoder")
