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

    def test_manual_draw_can_create_platform_wide_candidate(self):
        result = GeoHandler.draw_proposal(None, {"_body": {
            "source": {"name": "manual-map-test", "license": "CC0"},
            "feature": {"properties": {"name": "Unassigned drawn trail", "entityType": "TRAIL"},
                        "geometry": {"type": "LineString", "coordinates": [[-5.99, 37.39], [-5.98, 37.40]]}}
        }})
        entity = GeoHandler.get_entity(None, {"entityId": result["created"][0]})
        self.assertIsNone(entity["programmeSlug"])
        self.assertEqual(entity["status"], "CANDIDATE")

    def test_review_filters_support_multiple_statuses_location_and_paging(self):
        self.previous_items = GeoHandler.store.items
        GeoHandler.store.items = {
            "a": {"id": "a", "name": "Alpha", "status": "APPROVED", "entityType": "MUNICIPAL_PARK", "programmeSlug": None,
                  "country": "Spain", "region": "Andalucia", "province": "Sevilla", "city": "Sevilla", "geometry": {"type": "Point", "coordinates": [-5.99, 37.39]}},
            "b": {"id": "b", "name": "Bravo", "status": "CANDIDATE", "entityType": "TRAIL", "programmeSlug": None,
                  "country": "Spain", "region": "Andalucia", "province": "Sevilla", "city": "Dos Hermanas", "geometry": {"type": "Point", "coordinates": [-5.95, 37.28]}},
            "c": {"id": "c", "name": "Charlie", "status": "REJECTED", "entityType": "TRAIL", "programmeSlug": None,
                  "country": "Spain", "region": "Andalucia", "province": "Cadiz", "city": "Cadiz", "geometry": {"type": "Point", "coordinates": [-6.29, 36.53]}},
        }
        try:
            result = GeoHandler.list_entities(None, {"_path": "/v1/geodata/entities?status=CANDIDATE&status=APPROVED&country=Spain&region=Andalucia&province=Sevilla&pageSize=1"})
            self.assertEqual(result["total"], 2)
            self.assertEqual(result["items"][0]["name"], "Alpha")
            next_page = GeoHandler.list_entities(None, {"_path": "/v1/geodata/entities?status=CANDIDATE,APPROVED&city=Dos%20Hermanas&pageSize=10"})
            self.assertEqual([item["name"] for item in next_page["items"]], ["Bravo"])
        finally:
            GeoHandler.store.items = self.previous_items


if __name__ == "__main__":
    unittest.main()
