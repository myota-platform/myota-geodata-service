import unittest

from reverse_geocoder import normalize_response


class ReverseGeocoderTests(unittest.TestCase):
    def test_maps_country_codes_and_administrative_units(self):
        result = normalize_response({
            "continent": "Europe",
            "continentCode": "EU",
            "countryName": "Spain",
            "countryCode": "ES",
            "principalSubdivision": "Andalucia",
            "principalSubdivisionCode": "ES-AN",
            "city": "Sevilla",
            "locality": "El Prado-Parque Maria Luisa",
            "localityInfo": {"administrative": [
                {"name": "Andalucia", "isoCode": "ES-AN", "adminLevel": 4, "order": 1},
                {"name": "Sevilla", "isoCode": "ES-SE", "description": "province", "adminLevel": 6, "order": 2},
            ]},
        })
        self.assertEqual(result["continentCode"], "EU")
        self.assertEqual(result["countryCode"], "ES")
        self.assertEqual(result["regionCode"], "ES-AN")
        self.assertEqual(result["provinceCode"], "ES-SE")
        self.assertEqual(result["city"], "Sevilla")

    def test_does_not_duplicate_province_as_county(self):
        result = normalize_response({
            "countryName": "Spain", "countryCode": "ES", "principalSubdivision": "Andalucia",
            "principalSubdivisionCode": "ES-AN", "city": "Sevilla",
            "localityInfo": {"administrative": [
                {"name": "Sevilla", "description": "province", "isoCode": "ES-SE", "adminLevel": 6, "order": 2},
                {"name": "Sevilla", "adminLevel": 7, "order": 3},
            ]},
        })
        self.assertEqual(result["province"], "Sevilla")
        self.assertIsNone(result["county"])


if __name__ == "__main__":
    unittest.main()
