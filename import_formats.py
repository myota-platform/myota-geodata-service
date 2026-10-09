"""Text and archive decoders for the geodata intake boundary.

The decoder produces ordinary GeoJSON features.  It deliberately does not
decide lifecycle status or programme eligibility; those decisions belong to
the import service and the review workflow.
"""

from __future__ import annotations

import io
import json
import os
import shutil
import struct
import tempfile
import zipfile
from pathlib import Path
from pathlib import PurePosixPath
from typing import Any, Iterator
from xml.parsers import expat
from xml.etree import ElementTree


TEXT_FORMATS = {"GEOJSON", "KML", "GPX"}
BINARY_FORMATS = {"SHAPEFILE", "SHP", "OSM_PBF", "PARKSERVE_US"}
SUPPORTED_FORMATS = (
    TEXT_FORMATS | BINARY_FORMATS | {"WFS", "ARCGIS_FEATURESERVER"}
)
MAX_STREAMED_FEATURE_BYTES = int(
    os.environ.get("MYOTA_IMPORT_MAX_FEATURE_BYTES", str(16 * 1024 * 1024))
)
MAX_STREAMED_FEATURE_VERTICES = int(
    os.environ.get("MYOTA_IMPORT_MAX_FEATURE_VERTICES", "250000")
)
MAX_SHAPEFILE_EXPANDED_BYTES = int(
    os.environ.get("MYOTA_IMPORT_MAX_SHAPEFILE_EXPANDED_BYTES", str(1024**3))
)
MAX_SHAPEFILE_RECORD_BYTES = MAX_STREAMED_FEATURE_BYTES
MAX_SHAPEFILE_ARCHIVE_MEMBERS = 100


def _feature(
    geometry: dict[str, Any] | None,
    properties: dict[str, Any] | None = None,
    feature_id: Any = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "type": "Feature",
        "geometry": geometry,
        "properties": properties or {},
    }
    if feature_id is not None:
        value["id"] = feature_id
    return value


def _geojson(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return value
    if not isinstance(value, dict):
        raise ValueError("GeoJSON document must be an object")
    kind = value.get("type")
    document_crs = value.get("crs") or value.get("spatialReference")

    def with_document_crs(
        features: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not document_crs:
            return features
        result = []
        for feature in features:
            copied = dict(feature)
            copied.setdefault("crs", document_crs)
            result.append(copied)
        return result

    if kind == "FeatureCollection":
        features = value.get("features")
        if not isinstance(features, list):
            raise ValueError(
                "GeoJSON FeatureCollection must contain a features array"
            )
        return with_document_crs(features)
    if kind == "Feature":
        return with_document_crs([value])
    if isinstance(value.get("geometry"), dict):
        return with_document_crs(
            [
                _feature(
                    value["geometry"], value.get("properties"), value.get("id")
                )
            ]
        )
    if kind in {
        "Point",
        "LineString",
        "MultiLineString",
        "Polygon",
        "MultiPolygon",
    }:
        return with_document_crs([_feature(value)])
    raise ValueError("unsupported GeoJSON document type")


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _check_feature_size(feature: dict[str, Any]) -> None:
    encoded_size = len(
        json.dumps(feature, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
    )
    if encoded_size > MAX_STREAMED_FEATURE_BYTES:
        raise ValueError(
            "a source feature exceeds the configured decoded-size limit "
            f"({MAX_STREAMED_FEATURE_BYTES} bytes)"
        )


def _check_vertex_count(geometry: dict[str, Any] | None) -> None:
    if not geometry:
        return
    pending = [geometry.get("coordinates")]
    vertices = 0
    while pending:
        value = pending.pop()
        if not isinstance(value, (list, tuple)):
            continue
        if len(value) >= 2 and all(
            isinstance(item, (int, float)) for item in value[:2]
        ):
            vertices += 1
            if vertices > MAX_STREAMED_FEATURE_VERTICES:
                raise ValueError(
                    "a source geometry exceeds the configured vertex limit "
                    f"({MAX_STREAMED_FEATURE_VERTICES})"
                )
        else:
            pending.extend(value)


def _validate_shapefile_archive(archive: zipfile.ZipFile) -> None:
    members = archive.infolist()
    if len(members) > MAX_SHAPEFILE_ARCHIVE_MEMBERS:
        raise ValueError(
            "shapefile archive contains too many files "
            f"(limit {MAX_SHAPEFILE_ARCHIVE_MEMBERS})"
        )
    expanded_bytes = sum(member.file_size for member in members)
    if expanded_bytes > MAX_SHAPEFILE_EXPANDED_BYTES:
        raise ValueError(
            "shapefile archive exceeds the configured expanded-size limit "
            f"({MAX_SHAPEFILE_EXPANDED_BYTES} bytes)"
        )


def _validate_shapefile_records(
    stream: Any, max_record_bytes: int | None = None
) -> None:
    """Reject oversized or truncated records before pyshp decodes geometry."""
    if max_record_bytes is None:
        max_record_bytes = MAX_SHAPEFILE_RECORD_BYTES
    if len(stream.read(100)) != 100:
        raise ValueError("shapefile header is truncated")
    while True:
        header = stream.read(8)
        if not header:
            return
        if len(header) != 8:
            raise ValueError("shapefile record header is truncated")
        _record_number, content_words = struct.unpack(">II", header)
        record_bytes = content_words * 2
        if record_bytes > max_record_bytes:
            raise ValueError(
                "a shapefile record exceeds the configured decoded-size limit "
                f"({max_record_bytes} bytes)"
            )
        remaining = record_bytes
        while remaining:
            chunk = stream.read(min(64 * 1024, remaining))
            if not chunk:
                raise ValueError("shapefile record data is truncated")
            remaining -= len(chunk)


def _iter_kml(path: Path) -> Iterator[dict[str, Any]]:
    _validate_streaming_xml(path, {"Placemark"})
    stack: list[ElementTree.Element] = []
    for event, element in ElementTree.iterparse(path, events=("start", "end")):
        if event == "start":
            stack.append(element)
            continue
        if _local_name(element.tag) == "Placemark":
            name = next(
                (
                    child.text.strip()
                    for child in element
                    if _local_name(child.tag) == "name" and child.text
                ),
                None,
            )
            geometry = _kml_geometry(element)
            if geometry:
                feature = _feature(geometry, {"name": name} if name else {})
                _check_feature_size(feature)
                _check_vertex_count(geometry)
                yield feature
            element.clear()
            if len(stack) > 1:
                stack[-2].clear()
        if stack:
            stack.pop()


def _iter_gpx(path: Path) -> Iterator[dict[str, Any]]:
    _validate_streaming_xml(path, {"wpt", "rtept", "trkseg"})
    stack: list[ElementTree.Element] = []
    segment_points: list[list[float]] | None = None
    segment_bytes = 0
    segment_vertices = 0
    for event, element in ElementTree.iterparse(path, events=("start", "end")):
        tag = _local_name(element.tag)
        if event == "start":
            stack.append(element)
            if tag == "trkseg":
                segment_points = []
                segment_bytes = 0
                segment_vertices = 0
            elif tag == "trkpt" and segment_points is not None:
                segment_bytes += sum(
                    len(str(key).encode()) + len(str(value).encode())
                    for key, value in element.attrib.items()
                )
                segment_vertices += 1
                if segment_bytes > MAX_STREAMED_FEATURE_BYTES:
                    raise ValueError(
                        "a GPX track segment exceeds the configured decoded-size limit"
                    )
                if segment_vertices > MAX_STREAMED_FEATURE_VERTICES:
                    raise ValueError(
                        "a GPX track segment exceeds the configured vertex limit"
                    )
            continue

        if tag in {"wpt", "rtept"}:
            properties = {}
            for child in element:
                if (
                    _local_name(child.tag) in {"name", "desc", "type"}
                    and child.text
                ):
                    properties[_local_name(child.tag)] = child.text.strip()
            feature = _feature(
                {"type": "Point", "coordinates": _gpx_point(element)},
                properties,
            )
            _check_feature_size(feature)
            yield feature
        elif tag == "trkpt" and segment_points is not None:
            segment_points.append(_gpx_point(element))
        elif tag == "trkseg":
            if segment_points and len(segment_points) >= 2:
                feature = _feature(
                    {"type": "LineString", "coordinates": segment_points},
                    {},
                )
                _check_feature_size(feature)
                _check_vertex_count(feature["geometry"])
                yield feature
            segment_points = None

        if tag in {"wpt", "rtept", "trkseg"}:
            element.clear()
            if len(stack) > 1:
                stack[-2].clear()
        elif tag == "trkpt" and segment_points is not None:
            element.clear()
        elif not any(
            _local_name(parent.tag) in {"wpt", "rtept", "trkseg"}
            for parent in stack[:-1]
        ):
            element.clear()
        if stack:
            stack.pop()


def _validate_streaming_xml(path: Path, feature_tags: set[str]) -> None:
    """Bound XML feature allocation before ElementTree builds feature nodes."""
    parser = expat.ParserCreate(namespace_separator="}")
    parser.SetParamEntityParsing(expat.XML_PARAM_ENTITY_PARSING_NEVER)
    feature_stack: list[dict[str, int]] = []

    def reject_doctype(*_args: Any) -> None:
        raise ValueError("XML imports must not contain a document type")

    def start_element(name: str, attributes: dict[str, str]) -> None:
        local_name = name.rsplit("}", 1)[-1]
        markup_bytes = len(name.encode("utf-8")) + sum(
            len(key.encode("utf-8")) + len(value.encode("utf-8"))
            for key, value in attributes.items()
        )
        if feature_stack:
            feature_stack[-1]["bytes"] += markup_bytes
            feature_stack[-1]["nodes"] += 1
        if local_name in feature_tags:
            feature_stack.append({"bytes": markup_bytes, "nodes": 1})
        if feature_stack:
            _check_xml_feature_limits(feature_stack[-1])

    def character_data(value: str) -> None:
        if feature_stack:
            feature_stack[-1]["bytes"] += len(value.encode("utf-8"))
            _check_xml_feature_limits(feature_stack[-1])

    def end_element(name: str) -> None:
        if name.rsplit("}", 1)[-1] in feature_tags:
            feature_stack.pop()

    parser.StartDoctypeDeclHandler = reject_doctype
    parser.StartElementHandler = start_element
    parser.CharacterDataHandler = character_data
    parser.EndElementHandler = end_element
    try:
        with path.open("rb") as source:
            while chunk := source.read(64 * 1024):
                parser.Parse(chunk, False)
            parser.Parse(b"", True)
    except expat.ExpatError as error:
        raise ValueError(f"invalid XML source: {error}") from error


def _check_xml_feature_limits(feature: dict[str, int]) -> None:
    if feature["bytes"] > MAX_STREAMED_FEATURE_BYTES:
        raise ValueError(
            "an XML feature exceeds the configured decoded-size limit "
            f"({MAX_STREAMED_FEATURE_BYTES} bytes)"
        )
    if feature["nodes"] > MAX_STREAMED_FEATURE_VERTICES:
        raise ValueError(
            "an XML feature exceeds the configured element-count limit "
            f"({MAX_STREAMED_FEATURE_VERTICES})"
        )


def _iter_shapefile(path: Path, filename: str) -> Iterator[dict[str, Any]]:
    try:
        import shapefile
    except ImportError as exc:
        raise ValueError(
            "Shapefile support requires the pyshp package"
        ) from exc
    if not filename.lower().endswith(".zip"):
        raise ValueError(
            "upload a .zip containing the .shp, .shx, and .dbf sidecars"
        )

    with zipfile.ZipFile(path) as archive:
        _validate_shapefile_archive(archive)
        names = archive.namelist()
        shp_name = next(
            (name for name in names if name.lower().endswith(".shp")), None
        )
        if not shp_name:
            raise ValueError(
                "shapefile archive does not contain a .shp member"
            )
        stem = str(PurePosixPath(shp_name).with_suffix(""))
        by_lower = {name.casefold(): name for name in names}
        shx_name = by_lower.get(f"{stem}.shx".casefold())
        dbf_name = by_lower.get(f"{stem}.dbf".casefold())
        if not shx_name or not dbf_name:
            raise ValueError(
                "shapefile archive is missing its .shx or .dbf sidecar"
            )
        prj_name = by_lower.get(f"{stem}.prj".casefold())
        source_crs = None
        if prj_name:
            if archive.getinfo(prj_name).file_size > 1024 * 1024:
                raise ValueError("shapefile projection metadata is too large")
            source_crs = archive.read(prj_name).decode("utf-8", "replace")
        with archive.open(shp_name) as shp_stream:
            _validate_shapefile_records(shp_stream)
        with tempfile.TemporaryDirectory(prefix="myota-shapefile-") as tmp:
            scratch = Path(tmp)
            local_members = {}
            for member in (shp_name, shx_name, dbf_name):
                local_path = scratch / PurePosixPath(member).name
                with (
                    archive.open(member) as source,
                    local_path.open("wb") as destination,
                ):
                    shutil.copyfileobj(source, destination, length=1024 * 1024)
                local_members[member] = local_path

            reader = shapefile.Reader(
                str(local_members[shp_name]),
                str(local_members[shx_name]),
                str(local_members[dbf_name]),
            )
            fields = [field[0] for field in reader.fields[1:]]
            for shape_record in reader.iterShapeRecords():
                geometry = shape_record.shape.__geo_interface__
                properties = dict(zip(fields, shape_record.record))
                feature = _feature(geometry, properties)
                if source_crs:
                    feature["crs"] = source_crs
                _check_feature_size(feature)
                _check_vertex_count(geometry)
                yield feature


def _iter_osm_pbf(path: Path) -> Iterator[dict[str, Any]]:
    try:
        import osmium
    except ImportError as exc:
        raise ValueError(
            "OSM PBF support requires the osmium package"
        ) from exc

    from import_adapters import OSM_REQUIRED_TAGS

    required_tags = {tuple(tag.split("=", 1)) for tag in OSM_REQUIRED_TAGS}
    line_values = {"path", "footway", "track", "bridleway"}
    area_values = {
        ("leisure", "park"),
        ("leisure", "nature_reserve"),
        ("boundary", "protected_area"),
        ("landuse", "recreation_ground"),
    }
    factory = osmium.geom.GeoJSONFactory()

    def make_feature(
        osm_type: str, osm_id: int, tags: Any, geometry: dict[str, Any]
    ) -> dict[str, Any]:
        properties = dict(tags)
        properties.update(
            {
                "id": f"{osm_type}/{osm_id}",
                "osm_id": f"{osm_type}/{osm_id}",
                "sourceRef": f"{osm_type}/{osm_id}",
                "featureType": osm_type,
            }
        )
        feature = _feature(geometry, properties, f"{osm_type}/{osm_id}")
        _check_feature_size(feature)
        _check_vertex_count(geometry)
        return feature

    with tempfile.TemporaryDirectory(prefix="myota-osm-index-") as directory:
        location_index = Path(directory) / "node-locations.store"
        processor = (
            osmium.FileProcessor(path)
            .with_locations(storage=f"sparse_file_array,{location_index}")
            .with_areas()
        )
        for item in processor:
            tags = dict(item.tags)
            if item.is_area():
                if not any(pair in area_values for pair in tags.items()):
                    continue
                vertices = 0
                for outer_ring in item.outer_rings():
                    vertices += len(outer_ring)
                    if vertices > MAX_STREAMED_FEATURE_VERTICES:
                        break
                    vertices += sum(
                        len(inner_ring)
                        for inner_ring in item.inner_rings(outer_ring)
                    )
                    if vertices > MAX_STREAMED_FEATURE_VERTICES:
                        break
                if vertices > MAX_STREAMED_FEATURE_VERTICES:
                    raise ValueError(
                        "an OSM area exceeds the configured vertex limit "
                        f"({MAX_STREAMED_FEATURE_VERTICES})"
                    )
                geometry = json.loads(factory.create_multipolygon(item))
                area_type = "way" if item.from_way() else "relation"
                yield make_feature(area_type, item.orig_id(), tags, geometry)
            elif item.is_node():
                if not any(pair in required_tags for pair in tags.items()):
                    continue
                if not item.location.valid():
                    continue
                yield make_feature(
                    "node",
                    item.id,
                    tags,
                    {
                        "type": "Point",
                        "coordinates": [item.location.lon, item.location.lat],
                    },
                )
            elif item.is_way():
                if (
                    not any(
                        ("highway", value) in tags.items()
                        for value in line_values
                    )
                    and ("route", "hiking") not in tags.items()
                ):
                    continue
                if (
                    item.nodes
                    and len(item.nodes) > MAX_STREAMED_FEATURE_VERTICES
                ):
                    raise ValueError(
                        "an OSM way exceeds the configured vertex limit "
                        f"({MAX_STREAMED_FEATURE_VERTICES})"
                    )
                coordinates = [
                    [node.lon, node.lat]
                    for node in item.nodes
                    if node.location.valid()
                ]
                if len(coordinates) < 2:
                    continue
                yield make_feature(
                    "way",
                    item.id,
                    tags,
                    {"type": "LineString", "coordinates": coordinates},
                )


def _kml_geometry(node: ElementTree.Element) -> dict[str, Any] | None:
    points: list[list[float]] = []
    for child in node.iter():
        if (
            _local_name(child.tag) != "coordinates"
            or not (child.text or "").strip()
        ):
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
        name = next(
            (
                child.text.strip()
                for child in placemark
                if _local_name(child.tag) == "name" and child.text
            ),
            None,
        )
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
            if (
                _local_name(child.tag) in {"name", "desc", "type"}
                and child.text
            ):
                properties[_local_name(child.tag)] = child.text.strip()
        features.append(
            _feature(
                {"type": "Point", "coordinates": _gpx_point(waypoint)},
                properties,
            )
        )
    for track in root.iter():
        if _local_name(track.tag) != "trkseg":
            continue
        points = [
            _gpx_point(node)
            for node in track
            if _local_name(node.tag) == "trkpt"
        ]
        if len(points) >= 2:
            features.append(
                _feature({"type": "LineString", "coordinates": points}, {})
            )
    if not features:
        raise ValueError(
            "GPX contains no supported waypoint, route, or track geometry"
        )
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
    raise ValueError(
        f"{code} is a binary or remote-source format; upload a file or use its source URL"
    )


def parse_uploaded(
    format_code: str, content: bytes, filename: str = "upload"
) -> list[dict[str, Any]]:
    code = format_code.upper().replace(".SHP", "SHAPEFILE")
    if code in TEXT_FORMATS or code in {"WFS", "ARCGIS_FEATURESERVER"}:
        return parse_text(code, content.decode("utf-8-sig"))
    if code in {"SHP", "SHAPEFILE", "PARKSERVE_US"}:
        try:
            import shapefile  # pyshp, optional in the lightweight service image
        except ImportError as exc:
            raise ValueError(
                "Shapefile support requires the pyshp package"
            ) from exc
        raw = content
        if filename.lower().endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                shp_name = next(
                    (
                        name
                        for name in archive.namelist()
                        if name.lower().endswith(".shp")
                    ),
                    None,
                )
                if not shp_name:
                    raise ValueError(
                        "shapefile archive does not contain a .shp member"
                    )
                raw = archive.read(shp_name)
                # pyshp needs the sibling DBF/SHX streams as well; use an in-memory
                # reader when all sidecars are present.
                stem = str(PurePosixPath(shp_name).with_suffix(""))
                reader = shapefile.Reader(
                    shp=io.BytesIO(raw),
                    shx=io.BytesIO(archive.read(stem + ".shx"))
                    if stem + ".shx" in archive.namelist()
                    else None,
                    dbf=io.BytesIO(archive.read(stem + ".dbf"))
                    if stem + ".dbf" in archive.namelist()
                    else None,
                )
                projection_name = next(
                    (
                        name
                        for name in archive.namelist()
                        if str(PurePosixPath(name)).with_suffix("").casefold()
                        == stem.casefold()
                        and name.lower().endswith(".prj")
                    ),
                    None,
                )
                source_crs = (
                    archive.read(projection_name).decode("utf-8", "replace")
                    if projection_name
                    else None
                )
        else:
            raise ValueError(
                "upload a .zip containing the .shp, .shx, and .dbf sidecars"
            )
        fields = [field[0] for field in reader.fields[1:]]
        return [
            {
                **_feature(shape.__geo_interface__, dict(zip(fields, record))),
                **({"crs": source_crs} if source_crs else {}),
            }
            for shape, record in zip(reader.shapes(), reader.records())
        ]
    raise ValueError(
        f"binary format {code} is accepted for queued processing but has no local decoder"
    )


def iter_uploaded_file(
    format_code: str, path: str | Path, filename: str = "upload"
) -> Iterator[dict[str, Any]]:
    """Yield large GeoJSON FeatureCollections one feature at a time.

    Text and shapefile inputs are streamed one feature at a time. A feature is
    rejected before staging if it exceeds the decoded-size or vertex limit.
    """
    code = format_code.upper().replace(".SHP", "SHAPEFILE")
    source = Path(path)
    if code == "KML":
        yield from _iter_kml(source)
        return
    if code == "GPX":
        yield from _iter_gpx(source)
        return
    if code == "OSM_PBF":
        yield from _iter_osm_pbf(source)
        return
    if code in {"SHP", "SHAPEFILE", "PARKSERVE_US"}:
        yield from _iter_shapefile(source, filename)
        return
    if code not in {"GEOJSON", "WFS", "ARCGIS_FEATURESERVER"}:
        yield from parse_uploaded(code, source.read_bytes(), filename)
        return

    try:
        import ijson
        from ijson.common import ObjectBuilder
    except ImportError:
        # Keep a functional fallback for lightweight/test installations. The
        # production image installs ijson and exercises the streaming branch.
        yield from parse_uploaded(code, source.read_bytes(), filename)
        return

    with source.open("rb") as stream:
        first = stream.read(1)
        while first and first.isspace():
            first = stream.read(1)
        stream.seek(0)
        document_crs = next(ijson.items(stream, "crs", use_float=True), None)
        if document_crs is None:
            stream.seek(0)
            document_crs = next(
                ijson.items(stream, "spatialReference", use_float=True), None
            )
        stream.seek(0)
        prefix = "item" if first == b"[" else "features.item"
        builder = None
        feature_size = 0
        found = False
        for event_prefix, event, value in ijson.parse(stream, use_float=True):
            if event_prefix == prefix and event in {
                "start_map",
                "start_array",
            }:
                builder = ObjectBuilder()
                feature_size = 0
            if builder is None:
                continue
            # Count the event's actual key/value bytes plus conservative JSON
            # structure overhead. Repeating the full ijson prefix for every
            # coordinate grossly over-counts large LineStrings and rejects
            # valid features well below the configured decoded-size ceiling.
            feature_size += 8
            if event == "map_key":
                feature_size += len(
                    json.dumps(value, ensure_ascii=False).encode("utf-8")
                )
            elif value is not None:
                feature_size += len(
                    json.dumps(value, ensure_ascii=False).encode("utf-8")
                )
            if feature_size > MAX_STREAMED_FEATURE_BYTES:
                raise ValueError(
                    "a source feature exceeds the configured decoded-size limit "
                    f"({MAX_STREAMED_FEATURE_BYTES} bytes)"
                )
            builder.event(event, value)
            if event_prefix == prefix and event in {"end_map", "end_array"}:
                feature = builder.value
                builder = None
                found = True
                _check_vertex_count(feature.get("geometry"))
                if document_crs and isinstance(feature, dict):
                    feature.setdefault("crs", document_crs)
                _check_feature_size(feature)
                yield feature
        if found:
            return

    # Non-collection GeoJSON values (a Feature, geometry, or an empty array)
    # are uncommon and small in practice; preserve compatibility explicitly.
    yield from parse_uploaded(code, source.read_bytes(), filename)
