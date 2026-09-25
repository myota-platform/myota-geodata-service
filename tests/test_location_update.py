import unittest
from unittest.mock import patch

from geodata import GeoHandler


class LocationUpdateTests(unittest.TestCase):
    def setUp(self):
        self.previous_items = GeoHandler.store.items
        self.previous_events = GeoHandler.store.events
        GeoHandler.store.items = {
            "entity-1": {
                "id": "entity-1", "programmeSlug": "regional-ota", "entityType": "MUNICIPAL_PARK",
                "status": "CANDIDATE", "geometry": {"type": "Point", "coordinates": [-5.99, 37.39]},
                "centroid": {"lon": -5.99, "lat": 37.39}, "country": "Spain", "countryCode": "ES",
                "manualLocationFields": [], "reviewHistory": [], "provenance": {},
            },
        }
        GeoHandler.store.events = []

    def tearDown(self):
        GeoHandler.store.items = self.previous_items
        GeoHandler.store.events = self.previous_events

    def test_manual_values_are_saved_and_can_be_released(self):
        update = {"entityId": "entity-1", "_body": {
            "location": {"country": "Reino de España", "countryCode": "ES"},
            "manualFields": ["country", "countryCode"], "editorId": "admin-1", "note": "Verified by administrator",
        }}
        with patch("geodata.enrich_entity_location", side_effect=lambda entity, force=False: entity):
            result = GeoHandler.update_location(None, update)
            self.assertEqual(result["country"], "Reino de España")
            self.assertEqual(result["manualLocationFields"], ["country", "countryCode"])

            released = {"entityId": "entity-1", "_body": {
                "location": {}, "manualFields": [], "editorId": "admin-1",
            }}
            result = GeoHandler.update_location(None, released)

        self.assertIsNone(result["country"])
        self.assertEqual(result["manualLocationFields"], [])
        self.assertEqual(result["reviewHistory"][-1]["action"], "LOCATION_UPDATED")
        self.assertEqual(GeoHandler.store.events[-1]["eventType"], "geodata.entity.location-updated.v1")

    def test_manual_edit_reuses_existing_provider_data(self):
        entity = GeoHandler.store.items["entity-1"]
        entity.update({"geocodeStatus": "ENRICHED", "countryCode": "ES", "country": "Spain", "city": "Sevilla"})
        update = {"entityId": "entity-1", "_body": {
            "location": {"country": "Reino de España"}, "manualFields": ["country"], "editorId": "admin-1",
        }}
        with patch("geodata.enrich_entity_location") as enrich:
            GeoHandler.update_location(None, update)
        enrich.assert_called_once_with(entity, force=False)


if __name__ == "__main__":
    unittest.main()
