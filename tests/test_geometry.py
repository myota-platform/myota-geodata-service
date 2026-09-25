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

    def test_multilinestring_is_valid(self):
        geometry = normalize_geometry({"type": "MultiLineString", "coordinates": [
            [[-5.99, 37.39], [-5.98, 37.40]], [[-5.97, 37.41], [-5.96, 37.42]]
        ]})
        self.assertEqual(geometry["type"], "MultiLineString")

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

        result = GeoHandler.change_geometry_type(None, {"entityId": "entity-1", "_body": {"geometryType": "MULTILINESTRING", "editorId": "admin"}})
        self.assertEqual(result["geometry"]["type"], "MultiLineString")

    def test_entity_category_change_is_audited(self):
        GeoHandler.store.items["entity-1"]["entityType"] = "MUNICIPAL_PARK"
        result = GeoHandler.change_entity_type(None, {"entityId": "entity-1", "_body": {
            "entityType": "TRAIL", "editorId": "admin", "note": "Reclassified as a trail"
        }})
        self.assertEqual(result["entityType"], "TRAIL")
        self.assertEqual(result["reviewHistory"][-1]["action"], "ENTITY_TYPE_CHANGED")
        self.assertEqual(result["reviewHistory"][-1]["previousEntityType"], "MUNICIPAL_PARK")

    def test_platform_wide_category_and_name_change_are_audited(self):
        GeoHandler.store.items["entity-1"]["programmeSlug"] = None
        category = GeoHandler.change_entity_type(None, {"entityId": "entity-1", "_body": {
            "entityType": "MUNICIPAL_PARK", "editorId": "admin", "note": "Shared category correction"
        }})
        renamed = GeoHandler.change_entity_name(None, {"entityId": "entity-1", "_body": {
            "name": "Sevilla trail", "editorId": "admin", "note": "Corrected display name"
        }})
        self.assertIsNone(category["programmeSlug"])
        self.assertEqual(category["entityType"], "MUNICIPAL_PARK")
        self.assertEqual(renamed["name"], "Sevilla trail")
        self.assertEqual(renamed["reviewHistory"][-1]["action"], "ENTITY_NAME_CHANGED")

    def test_import_can_create_platform_wide_candidate(self):
        result = GeoHandler.import_manual(None, {"_body": {
            "adapter": "MANUAL", "source": {"name": "shared-catalogue-test", "license": "CC0"},
            "entityType": "TRAIL", "features": [{"type": "Feature", "properties": {"name": "Unassigned trail"},
            "geometry": {"type": "LineString", "coordinates": [[-5.99, 37.39], [-5.98, 37.40]]}}]
        }, "Idempotency-Key": "import-unscoped-1"})
        entity = GeoHandler.get_entity(None, {"entityId": result["created"][0]})
        self.assertIsNone(entity["programmeSlug"])
        self.assertEqual(entity["entityType"], "TRAIL")
        self.assertEqual(entity["status"], "CANDIDATE")


if __name__ == "__main__":
    unittest.main()
