import unittest

from location_catalog import build_location_tree, derive_location_codes


class LocationCatalogTests(unittest.TestCase):
    def setUp(self):
        self.entities = [{
            "continent": "Europe", "continentCode": "EU", "country": "Spain", "countryCode": "ES",
            "region": "Andalucia", "regionCode": "ES-AN", "subdivision": "Andalucia", "subdivisionCode": "ES-AN",
            "province": "Sevilla", "provinceCode": "ES-SE",
        }, {
            "continent": "Europe", "continentCode": "EU", "country": "Spain", "countryCode": "ES",
            "region": "Madrid", "regionCode": "ES-MD", "subdivision": "Madrid", "subdivisionCode": "ES-MD",
            "province": "Madrid", "provinceCode": "ES-M",
        }]

    def test_builds_provider_derived_tree(self):
        tree = build_location_tree(self.entities)
        self.assertEqual(tree["continents"][0]["code"], "EU")
        self.assertEqual(tree["continents"][0]["countries"][0]["code"], "ES")
        self.assertEqual(len(tree["continents"][0]["countries"][0]["subdivisions"]), 2)

    def test_derives_codes_from_selected_names(self):
        codes = derive_location_codes({"continent": "Europe", "country": "Spain", "region": "Andalucia", "province": "Sevilla"},
                                      {"continent", "country", "region", "province"}, self.entities)
        self.assertEqual(codes, {"continentCode": "EU", "countryCode": "ES", "regionCode": "ES-AN",
                                 "subdivisionCode": "ES-AN", "provinceCode": "ES-SE"})


if __name__ == "__main__":
    unittest.main()
