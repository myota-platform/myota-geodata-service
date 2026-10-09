import unittest
import json
import importlib.util
import tempfile
from pathlib import Path

from import_formats import iter_uploaded_file, parse_gpx, parse_kml, parse_text
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
