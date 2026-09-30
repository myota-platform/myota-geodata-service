import unittest
from unittest.mock import patch

from geodata import GeoHandler, seed


class SeedDataTests(unittest.TestCase):
    def test_existing_sample_status_is_not_reset_on_startup(self):
        previous_items = GeoHandler.store.items
        try:
            GeoHandler.store.items = {
                "00000000-0000-4000-8000-000000000203": {
                    "id": "00000000-0000-4000-8000-000000000203",
                    "name": "Parque de los Príncipes",
                    "sourceRef": "osm-way-28604482",
                    "status": "APPROVED",
                }
            }
            with patch.object(GeoHandler.store, "hydrate"), \
                 patch("geodata.enrich_entity_location", side_effect=lambda entity, force=False: entity):
                seed()

            self.assertEqual(
                GeoHandler.store.items["00000000-0000-4000-8000-000000000203"]["status"],
                "APPROVED",
            )
        finally:
            GeoHandler.store.items = previous_items


if __name__ == "__main__":
    unittest.main()
