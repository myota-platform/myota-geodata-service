import unittest
from unittest.mock import patch

from geodata import GeoHandler
from geodata_pipeline import digest


class LocationEnrichmentTests(unittest.TestCase):
    def setUp(self):
        self.previous_items = GeoHandler.store.items
        self.previous_events = GeoHandler.store.events
        geometry = {"type": "Point", "coordinates": [-5.99, 37.39]}
        self.geometry_hash = digest(geometry)
        GeoHandler.store.items = {
            "entity-1": {
                "id": "entity-1",
                "name": "Test park",
                "status": "CANDIDATE",
                "geometry": geometry,
                "centroid": {"lon": -5.99, "lat": 37.39},
                "locationEnrichmentRequestId": "request-1",
                "locationEnrichmentGeometryHash": self.geometry_hash,
                "locationEnrichmentStatus": "QUEUED",
                "manualLocationFields": [],
                "provenance": {},
            }
        }
        GeoHandler.store.events = []

    def tearDown(self):
        GeoHandler.store.items = self.previous_items
        GeoHandler.store.events = self.previous_events

    def test_worker_applies_current_lookup_and_replay_does_not_call_provider_twice(
        self,
    ):
        result = {
            "continent": "Europe",
            "continentCode": "EU",
            "country": "Spain",
            "countryCode": "ES",
            "region": "Andalucia",
            "regionCode": "ES-AN",
            "city": "Sevilla",
            "geocodeProvider": "BIGDATACLOUD",
            "geocodeStatus": "ENRICHED",
            "geocodedAt": "2026-10-08T12:00:00Z",
            "geocodePayload": {"countryName": "Spain"},
        }
        with patch(
            "geodata.lookup_entity_location", return_value=result
        ) as lookup:
            self.assertTrue(
                GeoHandler._process_location_enrichment(
                    "entity-1",
                    "request-1",
                    self.geometry_hash,
                    False,
                    "GEOMETRY_CHANGED",
                )
            )
            self.assertTrue(
                GeoHandler._process_location_enrichment(
                    "entity-1",
                    "request-1",
                    self.geometry_hash,
                    False,
                    "GEOMETRY_CHANGED",
                )
            )

        lookup.assert_called_once()
        entity = GeoHandler.store.items["entity-1"]
        self.assertEqual(entity["locationEnrichmentStatus"], "COMPLETED")
        self.assertEqual(entity["location"]["countryCode"], "ES")
        self.assertEqual(
            entity["provenance"]["reverseGeocoding"]["response"],
            {"countryName": "Spain"},
        )
        self.assertEqual(
            GeoHandler.store.events[-1]["eventType"],
            "geodata.entity.location-enriched.v1",
        )

    def test_worker_discards_a_result_if_geometry_changed_while_lookup_ran(
        self,
    ):
        changed_geometry = {
            "type": "Point",
            "coordinates": [-5.98, 37.40],
        }
        entity = GeoHandler.store.items["entity-1"]
        entity["geometry"] = changed_geometry

        with patch("geodata.lookup_entity_location") as lookup:
            result = GeoHandler._process_location_enrichment(
                "entity-1",
                "request-1",
                self.geometry_hash,
                False,
                "GEOMETRY_CHANGED",
            )

        self.assertFalse(result)
        lookup.assert_not_called()
        self.assertEqual(entity["locationEnrichmentStatus"], "QUEUED")
        self.assertNotIn("country", entity)

    def test_geometry_edit_queues_refresh_for_new_geometry(self):
        with patch.object(GeoHandler.location_executor, "submit") as submit:
            result = GeoHandler.update_geometry(
                None,
                {
                    "entityId": "entity-1",
                    "_body": {
                        "editorId": "admin-1",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [-5.98, 37.40],
                        },
                    },
                },
            )

        submit.assert_called_once()
        self.assertEqual(result["locationEnrichmentStatus"], "QUEUED")
        request_event = next(
            event
            for event in GeoHandler.store.events
            if event["eventType"]
            == "geodata.entity.location-enrichment-requested.v1"
        )
        self.assertFalse(request_event["payload"]["onlyMissing"])
        self.assertEqual(
            request_event["payload"]["reason"], "GEOMETRY_CHANGED"
        )

    def test_lookup_never_overwrites_manual_parent_values_or_their_codes(self):
        from reverse_geocoder import apply_location_result

        entity = GeoHandler.store.items["entity-1"]
        entity.update(
            {
                "country": "Reino de España",
                "countryCode": "ES",
                "manualLocationFields": ["country"],
            }
        )
        apply_location_result(
            entity,
            {
                "country": "Portugal",
                "countryCode": "PT",
                "region": "Andalucia",
                "regionCode": "ES-AN",
            },
        )

        self.assertEqual(entity["country"], "Reino de España")
        self.assertEqual(entity["countryCode"], "ES")
        self.assertEqual(entity["region"], "Andalucia")


if __name__ == "__main__":
    unittest.main()
