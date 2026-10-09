import unittest
import json
import importlib.util
import os
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path
from unittest.mock import patch

from import_formats import (
    iter_uploaded_file,
    parse_gpx,
    parse_kml,
    parse_text,
)
from import_adapters import normalize


class ImportFormatTests(unittest.TestCase):
    def test_geojson_text_is_feature_stream(self):
        features = parse_text(
            "GEOJSON",
            '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[-5.99,37.39]},"properties":{"name":"Sevilla"}}]}',
        )
        self.assertEqual(features[0]["geometry"]["type"], "Point")

    def test_geojson_document_crs_is_carried_to_each_feature(self):
        features = parse_text(
            "GEOJSON",
            '{"type":"FeatureCollection","crs":{"type":"name","properties":{"name":"EPSG:25830"}},"features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[235000,4140000]},"properties":{}}]}',
        )
        self.assertEqual(
            features[0]["crs"]["properties"]["name"], "EPSG:25830"
        )

    def test_uploaded_feature_collection_iterator_preserves_features(self):
        document = {
            "type": "FeatureCollection",
            "crs": {"type": "name", "properties": {"name": "CRS84"}},
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [-5.99, 37.39],
                    },
                    "properties": {"name": f"Feature {index}"},
                }
                for index in range(3)
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "features.geojson"
            path.write_text(json.dumps(document), encoding="utf-8")
            features = list(iter_uploaded_file("GEOJSON", path, path.name))

        self.assertEqual(len(features), 3)
        self.assertEqual(features[2]["properties"]["name"], "Feature 2")
        self.assertEqual(features[0]["crs"], document["crs"])

    @unittest.skipUnless(
        importlib.util.find_spec("ijson"),
        "streaming parser dependency is installed in service CI",
    )
    def test_streaming_parser_does_not_use_whole_document_decoder(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "features.geojson"
            path.write_text(
                '{"type":"FeatureCollection","features":['
                '{"type":"Feature","properties":{},"geometry":'
                '{"type":"Point","coordinates":[-5.99,37.39]}}]}',
                encoding="utf-8",
            )
            features = list(iter_uploaded_file("GEOJSON", path))

        self.assertEqual(len(features), 1)
        self.assertEqual(features[0]["geometry"]["coordinates"][0], -5.99)

    def test_kml_point_and_polygon(self):
        content = '<kml xmlns="http://www.opengis.net/kml/2.2"><Document><Placemark><name>Park</name><Point><coordinates>-5.99,37.39</coordinates></Point></Placemark></Document></kml>'
        features = parse_kml(content)
        self.assertEqual(features[0]["properties"]["name"], "Park")
        self.assertEqual(features[0]["geometry"]["type"], "Point")

    def test_gpx_track_becomes_linestring(self):
        content = '<gpx xmlns="http://www.topografix.com/GPX/1/1"><trk><trkseg><trkpt lat="37.39" lon="-5.99"/><trkpt lat="37.40" lon="-5.98"/></trkseg></trk></gpx>'
        features = parse_gpx(content)
        self.assertEqual(features[0]["geometry"]["type"], "LineString")

    def test_streaming_kml_yields_placemarks_without_whole_parser(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "parks.kml"
            path.write_text(
                "<kml><Document><Placemark><name>A</name><Point>"
                "<coordinates>-5.99,37.39</coordinates></Point></Placemark>"
                "<Placemark><name>B</name><Point>"
                "<coordinates>-5.98,37.40</coordinates></Point></Placemark>"
                "</Document></kml>",
                encoding="utf-8",
            )
            with patch(
                "import_formats.parse_uploaded",
                side_effect=AssertionError("whole-document parser used"),
            ):
                features = list(iter_uploaded_file("KML", path, path.name))

        self.assertEqual(
            [item["properties"]["name"] for item in features], ["A", "B"]
        )

    def test_oversized_xml_feature_is_rejected_before_tree_materialization(
        self,
    ):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "oversized.kml"
            path.write_text(
                "<kml><Placemark><name>" + ("x" * 256) + "</name>"
                "<Point><coordinates>-5.99,37.39</coordinates></Point>"
                "</Placemark></kml>",
                encoding="utf-8",
            )
            with (
                patch("import_formats.MAX_STREAMED_FEATURE_BYTES", 128),
                patch(
                    "import_formats.ElementTree.iterparse",
                    side_effect=AssertionError(
                        "tree parser ran before XML size preflight"
                    ),
                ),
            ):
                with self.assertRaisesRegex(ValueError, "XML feature"):
                    list(iter_uploaded_file("KML", path, path.name))

    def test_xml_doctype_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "doctype.kml"
            path.write_text(
                '<!DOCTYPE kml [<!ENTITY name "Park">]>'
                "<kml><Placemark><name>&name;</name></Placemark></kml>",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "document type"):
                list(iter_uploaded_file("KML", path, path.name))

    def test_streaming_gpx_yields_waypoints_and_tracks(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "routes.gpx"
            path.write_text(
                '<gpx><wpt lat="37.39" lon="-5.99"><name>W</name></wpt>'
                '<trk><trkseg><trkpt lat="37.39" lon="-5.99"/>'
                '<trkpt lat="37.40" lon="-5.98"/></trkseg></trk></gpx>',
                encoding="utf-8",
            )
            with patch(
                "import_formats.parse_uploaded",
                side_effect=AssertionError("whole-document parser used"),
            ):
                features = list(iter_uploaded_file("GPX", path, path.name))

        self.assertEqual(
            [item["geometry"]["type"] for item in features],
            ["Point", "LineString"],
        )

    def test_streaming_shapefile_reads_shape_records_from_zip(self):
        try:
            import shapefile
        except ImportError:
            self.skipTest("pyshp is installed in service CI")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shape_path = root / "parks.shp"
            writer = shapefile.Writer(
                str(shape_path), shapeType=shapefile.POINT
            )
            writer.field("name", "C")
            writer.point(-5.99, 37.39)
            writer.record("Sevilla")
            writer.close()
            archive_path = root / "parks.zip"
            with zipfile.ZipFile(archive_path, "w") as archive:
                for suffix in (".shp", ".shx", ".dbf"):
                    archive.write(root / f"parks{suffix}", f"parks{suffix}")
            features = list(
                iter_uploaded_file(
                    "SHAPEFILE", archive_path, archive_path.name
                )
            )

        self.assertEqual(features[0]["properties"]["name"], "Sevilla")
        self.assertEqual(features[0]["geometry"]["type"], "Point")

    @unittest.skipUnless(
        importlib.util.find_spec("shapefile"),
        "RSS qualification requires the pyshp dependency",
    )
    def test_large_shapefile_streaming_keeps_peak_rss_bounded(self):
        import shapefile

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shape_path = root / "many-parks.shp"
            writer = shapefile.Writer(
                str(shape_path), shapeType=shapefile.POINT
            )
            writer.field("name", "C")
            for index in range(50_000):
                writer.point(-5.99 + (index % 100) / 100_000, 37.39)
                writer.record(f"fixture-{index}")
            writer.close()

            archive_path = root / "many-parks.zip"
            with zipfile.ZipFile(archive_path, "w") as archive:
                for suffix in (".shp", ".shx", ".dbf"):
                    archive.write(
                        root / f"many-parks{suffix}", f"many-parks{suffix}"
                    )

            script = r"""
import resource, sys
from import_formats import iter_uploaded_file
format_code, path = sys.argv[1:]
count = sum(1 for _ in iter_uploaded_file(format_code, path, path))
peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
peak_bytes = peak if sys.platform == "darwin" else peak * 1024
print(f"format={format_code} features={count} peak_rss_bytes={peak_bytes} platform={sys.platform}")
if count != 50000 or peak_bytes > 96 * 1024 * 1024:
    raise SystemExit(1)
"""
            for format_code in ("SHAPEFILE", "PARKSERVE_US"):
                result = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        script,
                        format_code,
                        str(archive_path),
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                    env=os.environ.copy(),
                )

                self.assertEqual(
                    result.returncode,
                    0,
                    f"{format_code} RSS qualification failed:\n"
                    f"{result.stdout}{result.stderr}",
                )
                self.assertIn("features=50000", result.stdout)
                print(result.stdout.strip())

    def test_shapefile_archive_rejects_oversized_record_before_decode(self):
        try:
            import shapefile
        except ImportError:
            self.skipTest("pyshp is installed in service CI")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            writer = shapefile.Writer(
                str(root / "source.shp"), shapeType=shapefile.POINT
            )
            writer.field("name", "C")
            writer.point(-5.99, 37.39)
            writer.record("fixture")
            writer.close()
            path = root / "oversized.zip"
            with zipfile.ZipFile(path, "w") as archive:
                for suffix in (".shp", ".shx", ".dbf"):
                    archive.write(root / f"source{suffix}", f"source{suffix}")

            with patch("import_formats.MAX_SHAPEFILE_RECORD_BYTES", 8):
                with self.assertRaisesRegex(ValueError, "record exceeds"):
                    list(iter_uploaded_file("SHAPEFILE", path, path.name))

    def test_shapefile_archive_rejects_excessive_expansion(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "expanded.zip"
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr("source.shp", b"\0" * 100)
            with patch("import_formats.MAX_SHAPEFILE_EXPANDED_BYTES", 50):
                with self.assertRaisesRegex(ValueError, "expanded-size"):
                    list(iter_uploaded_file("SHAPEFILE", path, path.name))

    def test_streaming_osm_pbf_uses_node_and_way_geometry(self):
        try:
            import osmium
        except ImportError:
            self.skipTest("osmium is installed in service CI")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trails.osm.pbf"
            with osmium.SimpleWriter(path) as writer:
                writer.add_node(
                    osmium.osm.mutable.Node(
                        id=1,
                        location=(-5.99, 37.39),
                        tags={"leisure": "park"},
                    )
                )
                writer.add_node(
                    osmium.osm.mutable.Node(
                        id=2, location=(-5.98, 37.40), tags={}
                    )
                )
                writer.add_way(
                    osmium.osm.mutable.Way(
                        id=10,
                        nodes=[1, 2],
                        tags={"highway": "path", "name": "Trail"},
                    )
                )
            features = list(iter_uploaded_file("OSM_PBF", path, path.name))

        self.assertEqual(len(features), 2)
        self.assertEqual(features[0]["geometry"]["type"], "Point")
        self.assertEqual(features[1]["geometry"]["type"], "LineString")
        self.assertEqual(features[1]["properties"]["sourceRef"], "way/10")

    @unittest.skipUnless(
        importlib.util.find_spec("osmium"),
        "PyOsmium is installed in service CI",
    )
    def test_osm_area_vertex_limit_runs_before_geojson_materialization(self):
        import osmium

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "oversized-area.osm.pbf"
            with osmium.SimpleWriter(path) as writer:
                for node_id, location in enumerate(
                    (
                        (-5.99, 37.39),
                        (-5.98, 37.39),
                        (-5.98, 37.40),
                    ),
                    start=1,
                ):
                    writer.add_node(
                        osmium.osm.mutable.Node(
                            id=node_id, location=location, tags={}
                        )
                    )
                writer.add_way(
                    osmium.osm.mutable.Way(
                        id=10,
                        nodes=[1, 2, 3, 1],
                        tags={"leisure": "park", "area": "yes"},
                    )
                )

            with (
                patch("import_formats.MAX_STREAMED_FEATURE_VERTICES", 3),
                patch.object(
                    osmium.geom.GeoJSONFactory,
                    "create_multipolygon",
                    side_effect=AssertionError(
                        "area was materialized before vertex guard"
                    ),
                ),
            ):
                with self.assertRaisesRegex(ValueError, "OSM area"):
                    list(iter_uploaded_file("OSM_PBF", path, path.name))

    @unittest.skipUnless(
        importlib.util.find_spec("osmium"),
        "RSS qualification requires PyOsmium",
    )
    def test_large_osm_pbf_streaming_keeps_peak_rss_bounded(self):
        import osmium

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "many-parks.osm.pbf"
            with osmium.SimpleWriter(path) as writer:
                for node_id in range(1, 50_001):
                    writer.add_node(
                        osmium.osm.mutable.Node(
                            id=node_id,
                            location=(
                                -5.99 + (node_id % 100) / 100_000,
                                37.39,
                            ),
                            tags={"leisure": "park"},
                        )
                    )

            script = r"""
import resource, sys
from import_formats import iter_uploaded_file
count = sum(1 for _ in iter_uploaded_file("OSM_PBF", sys.argv[1], sys.argv[1]))
peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
peak_bytes = peak if sys.platform == "darwin" else peak * 1024
print(f"features={count} peak_rss_bytes={peak_bytes} platform={sys.platform}")
if count != 50000 or peak_bytes > 96 * 1024 * 1024:
    raise SystemExit(1)
"""
            result = subprocess.run(
                [sys.executable, "-c", script, str(path)],
                check=False,
                capture_output=True,
                text=True,
                env=os.environ.copy(),
            )

        self.assertEqual(
            result.returncode,
            0,
            f"OSM PBF RSS qualification failed:\n{result.stdout}{result.stderr}",
        )
        self.assertIn("features=50000", result.stdout)
        print(result.stdout.strip())

    @unittest.skipUnless(
        importlib.util.find_spec("ijson"),
        "streaming parser dependency is installed in service CI",
    )
    def test_streamed_feature_size_limit_is_enforced(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "large.geojson"
            path.write_text(
                json.dumps(
                    {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "properties": {"description": "x" * 256},
                                "geometry": {
                                    "type": "Point",
                                    "coordinates": [-5.99, 37.39],
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with patch("import_formats.MAX_STREAMED_FEATURE_BYTES", 128):
                with self.assertRaisesRegex(ValueError, "decoded-size limit"):
                    list(iter_uploaded_file("GEOJSON", path, path.name))

    @unittest.skipUnless(
        importlib.util.find_spec("ijson"),
        "RSS qualification requires the streaming parser dependency",
    )
    def test_large_geojson_streaming_keeps_peak_rss_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "large.geojson"
            with path.open("w", encoding="utf-8") as output:
                output.write('{"type":"FeatureCollection","features":[')
                for index in range(300_000):
                    if index:
                        output.write(",")
                    output.write(
                        json.dumps(
                            {
                                "type": "Feature",
                                "properties": {"name": f"fixture-{index}"},
                                "geometry": {
                                    "type": "Point",
                                    "coordinates": [-5.99, 37.39],
                                },
                            },
                            separators=(",", ":"),
                        )
                    )
                output.write("]}")
            script = r"""
import resource, sys
from import_formats import iter_uploaded_file
format_code, path = sys.argv[1:]
count = sum(1 for _ in iter_uploaded_file(format_code, path))
peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
peak_bytes = peak if sys.platform == "darwin" else peak * 1024
print(f"format={format_code} features={count} peak_rss_bytes={peak_bytes} platform={sys.platform}")
if count != 300000 or peak_bytes > 96 * 1024 * 1024:
    raise SystemExit(1)
"""
            for format_code in (
                "GEOJSON",
                "WFS",
                "ARCGIS_FEATURESERVER",
            ):
                result = subprocess.run(
                    [sys.executable, "-c", script, format_code, str(path)],
                    check=False,
                    capture_output=True,
                    text=True,
                    env=os.environ.copy(),
                )

                self.assertEqual(
                    result.returncode,
                    0,
                    f"{format_code} RSS qualification failed:\n"
                    f"{result.stdout}{result.stderr}",
                )
                self.assertIn("features=300000", result.stdout)
                print(result.stdout.strip())

    @unittest.skipUnless(
        importlib.util.find_spec("ijson"),
        "RSS qualification requires the streaming parser dependency",
    )
    def test_max_vertex_geojson_feature_keeps_peak_rss_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "max-vertices.geojson"
            with path.open("w", encoding="utf-8") as output:
                output.write(
                    '{"type":"FeatureCollection","features":[{"type":"Feature",'
                    '"properties":{"name":"max-vertices"},"geometry":'
                    '{"type":"LineString","coordinates":['
                )
                for index in range(250_000):
                    if index:
                        output.write(",")
                    output.write("[-5.99,37.39]")
                output.write("]}}]}")

            script = r"""
import resource, sys
from import_formats import iter_uploaded_file
feature = next(iter_uploaded_file("GEOJSON", sys.argv[1]))
count = len(feature["geometry"]["coordinates"])
peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
peak_bytes = peak if sys.platform == "darwin" else peak * 1024
print(f"vertices={count} peak_rss_bytes={peak_bytes}")
if count != 250000 or peak_bytes > 96 * 1024 * 1024:
    raise SystemExit(1)
"""
            result = subprocess.run(
                [sys.executable, "-c", script, str(path)],
                check=False,
                capture_output=True,
                text=True,
                env=os.environ.copy(),
            )

        self.assertEqual(
            result.returncode,
            0,
            f"maximum-vertex RSS qualification failed:\n"
            f"{result.stdout}{result.stderr}",
        )
        self.assertIn("vertices=250000", result.stdout)
        print(result.stdout.strip())

    def test_kml_and_gpx_streaming_keep_peak_rss_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            kml_path = root / "many-placemarks.kml"
            with kml_path.open("w", encoding="utf-8") as output:
                output.write(
                    '<kml xmlns="http://www.opengis.net/kml/2.2"><Document>'
                )
                for index in range(50_000):
                    output.write(
                        f"<Placemark><name>fixture-{index}</name>"
                        "<Point><coordinates>-5.99,37.39</coordinates>"
                        "</Point></Placemark>"
                    )
                output.write("</Document></kml>")

            gpx_path = root / "many-waypoints.gpx"
            with gpx_path.open("w", encoding="utf-8") as output:
                output.write('<gpx version="1.1" creator="MyOTA">')
                for index in range(50_000):
                    output.write(
                        f'<wpt lat="37.39" lon="-5.99"><name>{index}'
                        "</name></wpt>"
                    )
                output.write("</gpx>")

            script = r"""
import resource, sys
from import_formats import iter_uploaded_file
format_code, path, expected = sys.argv[1], sys.argv[2], int(sys.argv[3])
count = sum(1 for _ in iter_uploaded_file(format_code, path, path))
peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
peak_bytes = peak if sys.platform == "darwin" else peak * 1024
print(f"format={format_code} features={count} peak_rss_bytes={peak_bytes}")
if count != expected or peak_bytes > 96 * 1024 * 1024:
    raise SystemExit(1)
"""
            for format_code, path in (("KML", kml_path), ("GPX", gpx_path)):
                result = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        script,
                        format_code,
                        str(path),
                        "50000",
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                    env=os.environ.copy(),
                )
                self.assertEqual(
                    result.returncode,
                    0,
                    f"{format_code} RSS qualification failed:\n"
                    f"{result.stdout}{result.stderr}",
                )
                print(result.stdout.strip())

    def test_preprocessing_infers_site_name_alias_without_losing_source_property(
        self,
    ):
        feature = normalize(
            "MANUAL",
            {
                "type": "Feature",
                "properties": {
                    "site_code": "ES0000108",
                    "SITE_NAME": "Los Órganos",
                    "TIPO": "B",
                    "HECTAREAS": 152.404570654,
                    "AC": "CANARIAS",
                },
                "geometry": {"type": "Point", "coordinates": [-15.6, 28.3]},
            },
        )

        self.assertEqual(feature["properties"]["name"], "Los Órganos")
        self.assertEqual(feature["properties"]["SITE_NAME"], "Los Órganos")
        self.assertEqual(feature["properties"]["site_code"], "ES0000108")

    def test_government_gis_uses_common_name_aliases(self):
        feature = normalize(
            "GOVERNMENT_GIS",
            {
                "type": "Feature",
                "properties": {
                    "NOMBRE": "Parque del Alamillo",
                    "sourceFormat": "GEOJSON",
                },
                "geometry": {"type": "Point", "coordinates": [-5.99, 37.39]},
            },
        )
        self.assertEqual(feature["properties"]["name"], "Parque del Alamillo")


if __name__ == "__main__":
    unittest.main()
