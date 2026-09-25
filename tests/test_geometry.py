import unittest

from geodata import GeoHandler
from geodata_pipeline import normalize_geometry
from import_adapters import normalize


class GeometryWayTests(unittest.TestCase):
    def setUp(self):
        self.previous_items = GeoHandler.store.items
        self.previous_events = GeoHandler.store.events
        GeoHandler.store.items = {
            "entity-1": {
                "id": "entity-1", "programmeSlug": "regional-ota", "entityType": "TRAIL",
                "status": "CANDIDATE",
                "geometry": {"type": "Polygon", "coordinates": [[[-5.99, 37.39], [-5.98, 37.39],
                                                                         [-5.98, 37.40], [-5.99, 37.40], [-5.99, 37.39]]]},
                "reviewHistory": [], "geometryHistory": [], "provenance": {},
            },
        }
        GeoHandler.store.events = []

    def tearDown(self):
        GeoHandler.store.items = self.previous_items
        GeoHandler.store.events = self.previous_events

    def test_linestring_is_valid_and_way_alias_is_canonicalized(self):
        self.assertEqual(normalize_geometry({"type": "way", "coordinates": [[-5.99, 37.39], [-5.98, 37.40]]})["type"], "LineString")

    def test_osm_style_way_record_is_importable(self):
        feature = normalize("MANUAL", {"type": "way", "id": "way/42", "coordinates": [[-5.99, 37.39], [-5.98, 37.40]]})
        self.assertEqual(feature["geometry"]["type"], "LineString")
        self.assertEqual(feature["properties"]["featureType"], "way")

        trail = normalize("OSM", {"type": "way", "id": "way/43", "tags": {"highway": "path"},
                                  "coordinates": [[-5.99, 37.39], [-5.98, 37.40]]})
        self.assertEqual(trail["geometry"]["type"], "LineString")
        self.assertNotIn("skipReason", trail["properties"])

    def test_geometry_type_conversion_supports_way_and_round_trips(self):
        result = GeoHandler.change_geometry_type(None, {"entityId": "entity-1", "_body": {"geometryType": "WAY", "editorId": "admin"}})
        self.assertEqual(result["geometry"]["type"], "LineString")
        self.assertGreaterEqual(len(result["geometry"]["coordinates"]), 2)

        result = GeoHandler.change_geometry_type(None, {"entityId": "entity-1", "_body": {"geometryType": "POLYGON", "editorId": "admin"}})
        self.assertEqual(result["geometry"]["type"], "Polygon")


if __name__ == "__main__":
    unittest.main()
