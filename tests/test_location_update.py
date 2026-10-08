import unittest
from unittest.mock import patch

from geodata import GeoHandler
from relational_state import StateConflict


class LocationUpdateTests(unittest.TestCase):
    def setUp(self):
        self.previous_items = GeoHandler.store.items
        self.previous_events = GeoHandler.store.events
        GeoHandler.store.items = {
            "entity-1": {
                "id": "entity-1",
                "programmeSlug": "regional-ota",
                "entityType": "MUNICIPAL_PARK",
                "status": "CANDIDATE",
                "geometry": {"type": "Point", "coordinates": [-5.99, 37.39]},
                "centroid": {"lon": -5.99, "lat": 37.39},
                "continent": "Europe",
                "continentCode": "EU",
                "country": "Spain",
                "countryCode": "ES",
                "region": "Andalucia",
                "regionCode": "ES-AN",
                "subdivision": "Andalucia",
                "subdivisionCode": "ES-AN",
                "province": "Sevilla",
                "provinceCode": "ES-SE",
                "manualLocationFields": [],
                "reviewHistory": [],
                "provenance": {},
            },
        }
        GeoHandler.store.events = []

    def tearDown(self):
        GeoHandler.store.items = self.previous_items
        GeoHandler.store.events = self.previous_events

    def test_manual_values_are_saved_and_can_be_released(self):
        update = {
            "entityId": "entity-1",
            "_body": {
                "location": {
                    "country": "Reino de España",
                    "countryCode": "ES",
                },
                "manualFields": ["country"],
                "editorId": "admin-1",
                "note": "Verified by administrator",
            },
        }
        with patch.object(GeoHandler.location_executor, "submit") as submit:
            result = GeoHandler.update_location(None, update)
            self.assertEqual(result["country"], "Reino de España")
            self.assertEqual(result["manualLocationFields"], ["country"])

            released = {
                "entityId": "entity-1",
                "_body": {
                    "location": {},
                    "manualFields": [],
                    "editorId": "admin-1",
                },
            }
            result = GeoHandler.update_location(None, released)
        submit.assert_called_once()
        self.assertEqual(result["locationEnrichmentStatus"], "QUEUED")
        self.assertEqual(
            GeoHandler.store.events[-2]["eventType"],
            "geodata.entity.location-enrichment-requested.v1",
        )

        self.assertIsNone(result["country"])
        self.assertEqual(result["manualLocationFields"], [])
        self.assertEqual(
            result["reviewHistory"][-1]["action"], "LOCATION_UPDATED"
        )
        self.assertEqual(
            GeoHandler.store.events[-1]["eventType"],
            "geodata.entity.location-updated.v1",
        )

    def test_manual_edit_reuses_existing_provider_data(self):
        entity = GeoHandler.store.items["entity-1"]
        entity.update(
            {
                "geocodeStatus": "ENRICHED",
                "countryCode": "ES",
                "country": "Spain",
                "city": "Sevilla",
            }
        )
        update = {
            "entityId": "entity-1",
            "_body": {
                "location": {"country": "Reino de España"},
                "manualFields": ["country"],
                "editorId": "admin-1",
            },
        }
        with patch.object(GeoHandler.location_executor, "submit") as submit:
            GeoHandler.update_location(None, update)
        submit.assert_not_called()

    def test_manual_refresh_requires_missing_metadata_and_queues_durable_event(
        self,
    ):
        entity = GeoHandler.store.items["entity-1"]
        entity.update(
            {
                "continent": "Europe",
                "continentCode": "EU",
                "country": "Spain",
                "countryCode": "ES",
                "region": "Andalucia",
                "regionCode": "ES-AN",
                "subdivision": "Andalucia",
                "subdivisionCode": "ES-AN",
                "city": "Sevilla",
            }
        )
        with patch.object(GeoHandler, "_authorize_gis_admin"):
            with self.assertRaisesRegex(
                StateConflict, "location metadata is already complete"
            ):
                GeoHandler.request_location_enrichment(
                    None,
                    {
                        "entityId": "entity-1",
                        "_body": {"editorId": "admin-1"},
                    },
                )

        entity["city"] = None
        with (
            patch.object(GeoHandler, "_authorize_gis_admin"),
            patch.object(GeoHandler.location_executor, "submit") as submit,
        ):
            result = GeoHandler.request_location_enrichment(
                None,
                {"entityId": "entity-1", "_body": {"editorId": "admin-1"}},
            )
        self.assertEqual(result["_status"], 202)
        self.assertEqual(result["status"], "QUEUED")
        self.assertIn("city", result["missingFields"])
        submit.assert_called_once()
        event = GeoHandler.store.events[-1]
        self.assertEqual(
            event["eventType"],
            "geodata.entity.location-enrichment-requested.v1",
        )
        self.assertEqual(
            event["payload"]["natsSubject"],
            "myota.geodata.entity.location-enrichment.v1",
        )

    def test_provider_codes_cannot_be_marked_manual(self):
        with self.assertRaisesRegex(ValueError, "provider-derived"):
            GeoHandler.update_location(
                None,
                {
                    "entityId": "entity-1",
                    "_body": {
                        "location": {"country": "Spain", "countryCode": "ES"},
                        "manualFields": ["country", "countryCode"],
                        "editorId": "admin-1",
                    },
                },
            )


if __name__ == "__main__":
    unittest.main()
