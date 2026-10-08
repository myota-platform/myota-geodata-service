"""Derive Maidenhead locator cells covered by WGS84 GeoJSON geometries."""

from __future__ import annotations

import math
from typing import Any, Iterable


_EPSILON = 1e-12
_FIELD_LETTERS = "ABCDEFGHIJKLMNOPQR"
_SUBSQUARE_LETTERS = "abcdefghijklmnopqrstuvwx"


def _clamped_index(
    value: float, origin: float, step: float, count: int
) -> int:
    return min(count - 1, max(0, math.floor((value - origin) / step)))


def locator_for_point(
    longitude: float, latitude: float, precision: int
) -> str:
    """Return the standard 4- or 6-character locator for one WGS84 point."""
    if precision not in {4, 6}:
        raise ValueError("Maidenhead precision must be 4 or 6 characters")
    if not -180 <= longitude <= 180 or not -90 <= latitude <= 90:
        raise ValueError(
            "Maidenhead coordinates must be WGS84 longitude/latitude"
        )

    x = _clamped_index(longitude, -180.0, 20.0, 18)
    y = _clamped_index(latitude, -90.0, 10.0, 18)
    result = _FIELD_LETTERS[x] + _FIELD_LETTERS[y]

    longitude_remainder = longitude + 180.0 - x * 20.0
    latitude_remainder = latitude + 90.0 - y * 10.0
    square_x = min(9, max(0, math.floor(longitude_remainder / 2.0)))
    square_y = min(9, max(0, math.floor(latitude_remainder)))
    result += f"{square_x}{square_y}"
    if precision == 4:
        return result

    longitude_remainder -= square_x * 2.0
    latitude_remainder -= square_y
    subsquare_x = min(23, max(0, math.floor(longitude_remainder * 12.0)))
    subsquare_y = min(23, max(0, math.floor(latitude_remainder * 24.0)))
    return (
        result
        + _SUBSQUARE_LETTERS[subsquare_x]
        + _SUBSQUARE_LETTERS[subsquare_y]
    )


def _candidate_indices(
    minimum: float, maximum: float, origin: float, step: float, count: int
) -> range:
    low = (minimum - origin) / step
    high = (maximum - origin) / step
    nearest_low = round(low)
    first = math.floor(low)
    if math.isclose(low, nearest_low, rel_tol=0.0, abs_tol=_EPSILON):
        first = nearest_low - 1
    last = math.floor(high)
    first = min(count - 1, max(0, first))
    last = min(count - 1, max(0, last))
    return range(first, last + 1)


def _point_in_rect(
    point: list[float], bounds: tuple[float, float, float, float]
) -> bool:
    min_x, min_y, max_x, max_y = bounds
    return (
        min_x - _EPSILON <= point[0] <= max_x + _EPSILON
        and min_y - _EPSILON <= point[1] <= max_y + _EPSILON
    )


def _segment_intersects_rect(
    start: list[float],
    end: list[float],
    bounds: tuple[float, float, float, float],
) -> bool:
    """Liang-Barsky clipping, including contact along cell boundaries."""
    min_x, min_y, max_x, max_y = bounds
    delta_x = end[0] - start[0]
    delta_y = end[1] - start[1]
    lower, upper = 0.0, 1.0
    for direction, distance in (
        (-delta_x, start[0] - min_x),
        (delta_x, max_x - start[0]),
        (-delta_y, start[1] - min_y),
        (delta_y, max_y - start[1]),
    ):
        if abs(direction) <= _EPSILON:
            if distance < -_EPSILON:
                return False
            continue
        ratio = distance / direction
        if direction < 0:
            lower = max(lower, ratio)
        else:
            upper = min(upper, ratio)
        if lower > upper + _EPSILON:
            return False
    return True


def _point_in_ring(point: list[float], ring: list[list[float]]) -> bool:
    inside = False
    x, y = point[:2]
    for start, end in zip(ring, ring[1:]):
        if _segment_intersects_rect(start, end, (x, y, x, y)):
            return True
        if (start[1] > y) != (end[1] > y):
            crossing_x = start[0] + (y - start[1]) * (end[0] - start[0]) / (
                end[1] - start[1]
            )
            if x < crossing_x:
                inside = not inside
    return inside


def _point_in_polygon(
    point: list[float], rings: list[list[list[float]]]
) -> bool:
    if not rings or not _point_in_ring(point, rings[0]):
        return False
    for hole in rings[1:]:
        if _point_in_ring(point, hole):
            # Hole boundaries belong to the polygon boundary; their interior does not.
            on_boundary = any(
                _segment_intersects_rect(
                    start, end, (point[0], point[1], point[0], point[1])
                )
                for start, end in zip(hole, hole[1:])
            )
            if on_boundary:
                return True
            return False
    return True


def _line_intersects_rect(
    points: list[list[float]],
    bounds: tuple[float, float, float, float],
) -> bool:
    return any(
        _segment_intersects_rect(start, end, bounds)
        for start, end in zip(points, points[1:])
    )


def _polygon_intersects_rect(
    rings: list[list[list[float]]],
    bounds: tuple[float, float, float, float],
) -> bool:
    for ring in rings:
        if any(_point_in_rect(point, bounds) for point in ring):
            return True
        if _line_intersects_rect(ring, bounds):
            return True
    min_x, min_y, max_x, max_y = bounds
    corners = (
        [min_x, min_y],
        [min_x, max_y],
        [max_x, min_y],
        [max_x, max_y],
    )
    return any(_point_in_polygon(point, rings) for point in corners)


def _component_bounds(
    points: Iterable[list[float]],
) -> tuple[float, float, float, float]:
    coordinates = list(points)
    return (
        min(point[0] for point in coordinates),
        min(point[1] for point in coordinates),
        max(point[0] for point in coordinates),
        max(point[1] for point in coordinates),
    )


def _cells_for_component(
    rings_or_line: list[list[list[float]]] | list[list[float]],
    is_polygon: bool,
    longitude_step: float,
    latitude_step: float,
) -> set[tuple[int, int]]:
    points = (
        [point for ring in rings_or_line for point in ring]
        if is_polygon
        else rings_or_line
    )
    min_x, min_y, max_x, max_y = _component_bounds(points)
    x_indices = _candidate_indices(
        min_x, max_x, -180.0, longitude_step, int(360 / longitude_step)
    )
    y_indices = _candidate_indices(
        min_y, max_y, -90.0, latitude_step, int(180 / latitude_step)
    )
    cells = set()
    for x_index in x_indices:
        cell_min_x = -180.0 + x_index * longitude_step
        cell_max_x = min(180.0, cell_min_x + longitude_step)
        for y_index in y_indices:
            cell_min_y = -90.0 + y_index * latitude_step
            cell_max_y = min(90.0, cell_min_y + latitude_step)
            bounds = (cell_min_x, cell_min_y, cell_max_x, cell_max_y)
            intersects = (
                _polygon_intersects_rect(rings_or_line, bounds)
                if is_polygon
                else _line_intersects_rect(rings_or_line, bounds)
            )
            if intersects:
                cells.add((x_index, y_index))
    return cells


def _covered_cells(geometry: dict[str, Any], precision: int) -> set[str]:
    longitude_step = 2.0 if precision == 4 else 1.0 / 12.0
    latitude_step = 1.0 if precision == 4 else 1.0 / 24.0
    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates")
    if geometry_type == "Point":
        longitude, latitude = coordinates[:2]
        return {locator_for_point(longitude, latitude, precision)}

    if geometry_type == "LineString":
        components = [coordinates]
        polygon_components = []
    elif geometry_type == "MultiLineString":
        components = coordinates
        polygon_components = []
    elif geometry_type == "Polygon":
        components = []
        polygon_components = [coordinates]
    elif geometry_type == "MultiPolygon":
        components = []
        polygon_components = coordinates
    else:
        raise ValueError(f"unsupported GeoJSON geometry type: {geometry_type}")

    indices: set[tuple[int, int]] = set()
    for line in components:
        if len(line) == 1:
            point = line[0]
            indices.add(
                (
                    _clamped_index(
                        point[0],
                        -180.0,
                        longitude_step,
                        int(360 / longitude_step),
                    ),
                    _clamped_index(
                        point[1],
                        -90.0,
                        latitude_step,
                        int(180 / latitude_step),
                    ),
                )
            )
        elif len(line) >= 2:
            for start, end in zip(line, line[1:]):
                indices.update(
                    _cells_for_component(
                        [start, end], False, longitude_step, latitude_step
                    )
                )
    for polygon in polygon_components:
        if polygon and polygon[0]:
            indices.update(
                _cells_for_component(
                    polygon, True, longitude_step, latitude_step
                )
            )
    return {
        locator_for_point(
            -180.0 + (x_index + 0.5) * longitude_step,
            -90.0 + (y_index + 0.5) * latitude_step,
            precision,
        )
        for x_index, y_index in indices
    }


def maidenhead_fields(geometry: dict[str, Any]) -> dict[str, list[str]]:
    """Return sorted unique four- and six-character cells intersected by geometry."""
    return {
        "maidenheadGridSquares4": sorted(_covered_cells(geometry, 4)),
        "maidenheadLocators6": sorted(_covered_cells(geometry, 6)),
    }


def apply_maidenhead_fields(entity: dict[str, Any]) -> dict[str, Any]:
    """Update an entity's derived locator arrays from its current geometry."""
    entity.update(maidenhead_fields(entity.get("geometry") or {}))
    return entity
