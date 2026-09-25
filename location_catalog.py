"""Provider-derived administrative hierarchy for geodata editing.

BigDataCloud's reverse-geocoding API is coordinate based and does not expose a
standalone country/subdivision catalog. This module builds a local catalog from
the provider-derived entity values already stored by the geodata service. That
keeps the admin editor consistent with the provider response without issuing
new remote requests merely to populate a drop-down.
"""
from __future__ import annotations

from typing import Any, Iterable


def _value(entity: dict[str, Any], field: str, *aliases: str) -> Any:
    if field in entity:
        return entity[field]
    location = entity.get("location") or {}
    if field in location:
        return location[field]
    for alias in aliases:
        if alias in entity:
            return entity[alias]
        if alias in location:
            return location[alias]
    return None


def _upsert(container: dict[tuple[str, str], dict[str, Any]], name: Any, code: Any,
            children_key: str) -> dict[str, Any] | None:
    name = str(name or "").strip()
    if not name:
        return None
    code = str(code or "").strip().upper() or None
    key = (code or "", name.casefold())
    if key not in container:
        container[key] = {"name": name, "code": code, children_key: {}}
    elif not container[key].get("code") and code:
        container[key]["code"] = code
    return container[key]


def _sorted_nodes(nodes: dict[tuple[str, str], dict[str, Any]], children_key: str) -> list[dict[str, Any]]:
    result = []
    for node in nodes.values():
        value = {key: item for key, item in node.items() if key != children_key}
        value[children_key] = _sorted_nodes(node[children_key], {
            "countries": "subdivisions", "subdivisions": "provinces", "provinces": "__leaf__"
        }[children_key]) if children_key != "provinces" else [
            {key: item for key, item in child.items() if key != "__leaf__"} for child in sorted(
                node[children_key].values(), key=lambda item: (str(item.get("name") or "").casefold(), str(item.get("code") or ""))
            )
        ]
        result.append(value)
    return sorted(result, key=lambda item: (str(item.get("name") or "").casefold(), str(item.get("code") or "")))


def build_location_tree(entities: Iterable[dict[str, Any]]) -> dict[str, Any]:
    continents: dict[tuple[str, str], dict[str, Any]] = {}
    for entity in entities:
        continent = _upsert(continents, _value(entity, "continent"), _value(entity, "continentCode"), "countries")
        if not continent:
            continue
        countries = continent["countries"]
        country = _upsert(countries, _value(entity, "country"), _value(entity, "countryCode"), "subdivisions")
        if not country:
            continue
        subdivisions = country["subdivisions"]
        subdivision = _upsert(
            subdivisions,
            _value(entity, "region", "subdivision"),
            _value(entity, "regionCode", "subdivisionCode"),
            "provinces",
        )
        if not subdivision:
            continue
        _upsert(subdivision["provinces"], _value(entity, "province"), _value(entity, "provinceCode"), "__leaf__")

    return {
        "provider": "BIGDATACLOUD",
        "codeStandards": {"continent": "provider ISO continent code", "country": "ISO 3166-1", "subdivision": "ISO 3166-2"},
        "continents": _sorted_nodes(continents, "countries"),
    }


def _flatten(nodes: Iterable[dict[str, Any]], child_key: str) -> list[dict[str, Any]]:
    result = []
    for node in nodes:
        result.append(node)
        result.extend(_flatten(node.get(child_key, []), {
            "countries": "subdivisions", "subdivisions": "provinces", "provinces": "__leaf__"
        }[child_key]) if child_key != "provinces" else node.get(child_key, []))
    return result


def _find(nodes: Iterable[dict[str, Any]], name: Any) -> dict[str, Any] | None:
    target = str(name or "").strip().casefold()
    if not target:
        return None
    return next((node for node in nodes if str(node.get("name") or "").casefold() == target), None)


def derive_location_codes(location: dict[str, Any], manual_fields: set[str], entities: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Derive provider codes for manually selected hierarchy names."""
    tree = build_location_tree(entities)
    continents = tree["continents"]
    continent = _find(continents, location.get("continent"))
    result: dict[str, Any] = {}
    if "continent" in manual_fields:
        if not continent:
            raise ValueError("continent must be selected from the provider-derived location tree")
        result["continentCode"] = continent.get("code")

    countries = continent.get("countries", []) if continent else _flatten(continents, "countries")
    country = _find(countries, location.get("country"))
    if "country" in manual_fields:
        if not country:
            raise ValueError("country must be selected from the provider-derived location tree")
        result["countryCode"] = country.get("code")

    subdivisions = country.get("subdivisions", []) if country else _flatten(countries, "subdivisions")
    subdivision_name = location.get("region") or location.get("subdivision")
    subdivision = _find(subdivisions, subdivision_name)
    if manual_fields.intersection({"region", "subdivision"}):
        if not subdivision:
            raise ValueError("region / subdivision must be selected from the provider-derived location tree")
        result["regionCode"] = subdivision.get("code")
        result["subdivisionCode"] = subdivision.get("code")

    provinces = subdivision.get("provinces", []) if subdivision else _flatten(subdivisions, "provinces")
    province = _find(provinces, location.get("province"))
    if "province" in manual_fields:
        if not province:
            raise ValueError("province must be selected from the provider-derived location tree")
        result["provinceCode"] = province.get("code")
    return result
