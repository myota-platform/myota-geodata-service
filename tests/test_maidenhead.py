"""Tests for Maidenhead grid fields derived from catalogue geometry."""

import unittest

from maidenhead import locator_for_point, maidenhead_fields


class MaidenheadLocatorTests(unittest.TestCase):
    def test_known_six_character_locator(self):
        self.assertEqual(locator_for_point(2.2945, 48.8584, 6), "JN18du")

    def test_point_has_one_four_and_six_character_cell(self):
        fields = maidenhead_fields(
            {"type": "Point", "coordinates": [-5.99, 37.4]}
        )

        self.assertEqual(fields["maidenheadGridSquares4"], ["IM77"])
        self.assertEqual(fields["maidenheadLocators6"], ["IM77aj"])

    def test_polygon_inside_one_cell_has_one_value_at_each_precision(self):
        polygon = {
            "type": "Polygon",
            "coordinates": [
                [
                    [-6.02, 37.38],
                    [-6.01, 37.38],
                    [-6.01, 37.39],
                    [-6.02, 37.39],
                    [-6.02, 37.38],
                ]
            ],
        }

        fields = maidenhead_fields(polygon)

        self.assertEqual(fields["maidenheadGridSquares4"], ["IM67"])
        self.assertEqual(fields["maidenheadLocators6"], ["IM67xj"])

    def test_polygon_crossing_grid_boundaries_includes_all_cells(self):
        polygon = {
            "type": "Polygon",
            "coordinates": [
                [
                    [-0.01, -0.01],
                    [0.01, -0.01],
                    [0.01, 0.01],
                    [-0.01, 0.01],
                    [-0.01, -0.01],
                ]
            ],
        }

        fields = maidenhead_fields(polygon)

        self.assertEqual(
            fields["maidenheadGridSquares4"],
            ["II99", "IJ90", "JI09", "JJ00"],
        )
        self.assertEqual(
            fields["maidenheadLocators6"],
            ["II99xx", "IJ90xa", "JI09ax", "JJ00aa"],
        )

    def test_line_spanning_six_character_cells_includes_each_cell(self):
        line = {
            "type": "LineString",
            "coordinates": [[-5.995, 37.401], [-5.9, 37.401]],
        }

        fields = maidenhead_fields(line)

        self.assertEqual(fields["maidenheadGridSquares4"], ["IM77"])
        self.assertEqual(fields["maidenheadLocators6"], ["IM77aj", "IM77bj"])


if __name__ == "__main__":
    unittest.main()
