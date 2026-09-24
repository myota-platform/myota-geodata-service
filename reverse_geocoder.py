"""BigDataCloud reverse-geocoding integration for geodata entities.

The paid/server-side BigDataCloud endpoint is used here because imports and
geometry edits are server-side operations. The client-side free endpoint must
not be used for stored or batch coordinates.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from common import now


def _load_local_env() -> None:
    """Load the service-local .env without overwriting deployment variables."""
    env_path = Path(os.environ.get("MYOTA_GEODATA_ENV_FILE", Path(__file__).with_name(".env")))
    if not env_path.is_file():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip()
        if value[:1] in {"'", '"'} and value[-1:] == value[:1]:
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = value


_load_local_env()


LOCATION_FIELDS = (
    "continent", "continentCode", "country", "countryCode", "region", "regionCode",
    "province", "provinceCode", "county", "countyCode", "city", "locality",
)


def _entry_text(entry: dict[str, Any]) -> str:
    return " ".join(str(entry.get(key) or "") for key in ("name", "description", "isoName")).lower()


def _pick_entry(entries: list[dict[str, Any]], patterns: tuple[str, ...], levels: tuple[int, ...]) -> dict[str, Any] | None:
    for entry in entries:
        if any(pattern in _entry_text(entry) for pattern in patterns):
            return entry
    for entry in entries:
        try:
            if int(entry.get("adminLevel")) in levels:
                return entry
        except (TypeError, ValueError):
            continue
    return None


def normalize_response(payload: dict[str, Any]) -> dict[str, Any]:
    """Map the provider response to the stable MyOTA entity location shape."""
    entries = sorted(payload.get("localityInfo", {}).get("administrative", []) or [],
                     key=lambda entry: (entry.get("order", 999), entry.get("adminLevel", 999)))
    principal_code = payload.get("principalSubdivisionCode")
    principal_name = str(payload.get("principalSubdivision") or "").casefold()
    entries = [entry for entry in entries if not (
        principal_code and entry.get("isoCode") == principal_code
    ) and not (
        principal_name and str(entry.get("name") or "").casefold() == principal_name
    )]
    province_entry = _pick_entry(
        entries,
        ("province", "provincia", "state", "department", "prefecture", "oblast"),
        (6,),
    )
    county_entry = _pick_entry(
        [entry for entry in entries if entry is not province_entry],
        ("county", "comarca", "district", "arrondissement", "municipality"),
        (5, 7),
    )

    def value(entry: dict[str, Any] | None, *keys: str) -> Any:
        if not entry:
            return None
        return next((entry.get(key) for key in keys if entry.get(key)), None)

    region = payload.get("principalSubdivision")
    region_code = payload.get("principalSubdivisionCode")
    province_name = value(province_entry, "name", "isoName")
    county_name = value(county_entry, "name", "isoName")
    city_name = payload.get("city") or payload.get("locality")
    if county_name and str(county_name).casefold() in {str(province_name or "").casefold(), str(city_name or "").casefold()}:
        county_entry = None
        county_name = None
    return {
        "continent": payload.get("continent"),
        "continentCode": payload.get("continentCode"),
        "country": payload.get("countryName"),
        "countryCode": payload.get("countryCode"),
        "region": region,
        "regionCode": region_code,
        # Explicit aliases make the first subdivision after country unambiguous
        # to clients that use ISO terminology rather than programme terminology.
        "subdivision": region,
        "subdivisionCode": region_code,
        "province": province_name,
        "provinceCode": value(province_entry, "isoCode"),
        "county": county_name,
        "countyCode": value(county_entry, "isoCode"),
        "city": city_name,
        "locality": payload.get("locality"),
        "geocodeProvider": "BIGDATACLOUD",
        "geocodeStatus": "ENRICHED",
        "geocodeLookupSource": payload.get("lookupSource"),
        "geocodedAt": now(),
        "geocodePayload": payload,
    }


class ReverseGeocoder:
    endpoint = "https://api-bdc.net/data/reverse-geocode"

    def __init__(self) -> None:
        self.api_key = os.environ.get("BIGDATACLOUD_API_KEY", "").strip()
        self.endpoint = os.environ.get("BIGDATACLOUD_REVERSE_GEOCODE_URL", self.endpoint)
        self.language = os.environ.get("BIGDATACLOUD_LOCALITY_LANGUAGE", "en")
        self.timeout = float(os.environ.get("BIGDATACLOUD_TIMEOUT_SECONDS", "10"))
        self._cache: dict[tuple[float, float, str], dict[str, Any]] = {}

    @property
    def configured(self) -> bool:
        return bool(self.api_key)

    def lookup(self, latitude: float, longitude: float) -> dict[str, Any]:
        key = (round(float(latitude), 5), round(float(longitude), 5), self.language)
        if key in self._cache:
            return self._cache[key]
        if not self.configured:
            return {"geocodeProvider": "BIGDATACLOUD", "geocodeStatus": "NOT_CONFIGURED", "geocodedAt": now()}
        query = urlencode({"latitude": key[0], "longitude": key[1], "localityLanguage": self.language, "key": self.api_key})
        request = Request(f"{self.endpoint}?{query}", headers={"Accept": "application/json", "User-Agent": "MyOTA-geodata-service/1.0"})
        try:
            with urlopen(request, timeout=self.timeout) as response:  # nosec B310 - endpoint is configured by deployment
                payload = json.loads(response.read().decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("reverse-geocoding response was not an object")
            result = normalize_response(payload)
        except Exception as error:  # enrichment must not reject an otherwise valid geodata import
            result = {"geocodeProvider": "BIGDATACLOUD", "geocodeStatus": "FAILED",
                      "geocodeError": str(error), "geocodedAt": now()}
        self._cache[key] = result
        return result


GEOCODER = ReverseGeocoder()


def enrich_entity_location(entity: dict[str, Any], *, force: bool = False) -> dict[str, Any]:
    """Enrich an entity from its centroid while keeping provider provenance."""
    if not force and entity.get("geocodeStatus") == "ENRICHED" and entity.get("countryCode"):
        return entity
    centroid = entity.get("centroid") or {}
    try:
        result = GEOCODER.lookup(float(centroid["lat"]), float(centroid["lon"]))
    except (KeyError, TypeError, ValueError):
        result = {"geocodeProvider": "BIGDATACLOUD", "geocodeStatus": "SKIPPED_NO_CENTROID", "geocodedAt": now()}
    for field in LOCATION_FIELDS:
        if field in result:
            entity[field] = result[field]
    for field in ("subdivision", "subdivisionCode", "geocodeProvider", "geocodeStatus", "geocodeLookupSource", "geocodeError", "geocodedAt"):
        if field in result:
            entity[field] = result[field]
    entity["location"] = {field: entity.get(field) for field in LOCATION_FIELDS}
    provenance = entity.setdefault("provenance", {})
    provenance["reverseGeocoding"] = {key: value for key, value in result.items() if key != "geocodePayload"}
    if "geocodePayload" in result:
        provenance["reverseGeocoding"]["response"] = result["geocodePayload"]
    return entity
