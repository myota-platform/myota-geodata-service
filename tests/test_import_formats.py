import unittest

from import_formats import parse_gpx, parse_kml, parse_text


class ImportFormatTests(unittest.TestCase):
    def test_geojson_text_is_feature_stream(self):
        features = parse_text("GEOJSON", '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[-5.99,37.39]},"properties":{"name":"Sevilla"}}]}')
        self.assertEqual(features[0]["geometry"]["type"], "Point")

    def test_geojson_document_crs_is_carried_to_each_feature(self):
        features = parse_text("GEOJSON", '{"type":"FeatureCollection","crs":{"type":"name","properties":{"name":"EPSG:25830"}},"features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[235000,4140000]},"properties":{}}]}')
        self.assertEqual(features[0]["crs"]["properties"]["name"], "EPSG:25830")

    def test_kml_point_and_polygon(self):
        content = '<kml xmlns="http://www.opengis.net/kml/2.2"><Document><Placemark><name>Park</name><Point><coordinates>-5.99,37.39</coordinates></Point></Placemark></Document></kml>'
        features = parse_kml(content)
        self.assertEqual(features[0]["properties"]["name"], "Park")
        self.assertEqual(features[0]["geometry"]["type"], "Point")

    def test_gpx_track_becomes_linestring(self):
        content = '<gpx xmlns="http://www.topografix.com/GPX/1/1"><trk><trkseg><trkpt lat="37.39" lon="-5.99"/><trkpt lat="37.40" lon="-5.98"/></trkseg></trk></gpx>'
        features = parse_gpx(content)
        self.assertEqual(features[0]["geometry"]["type"], "LineString")


if __name__ == "__main__":
    unittest.main()
