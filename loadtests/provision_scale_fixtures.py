#!/usr/bin/env python3
"""Provision a bounded, permanent synthetic catalogue in Sevilla."""

from __future__ import annotations

import json
import os
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen


API_URL = os.environ.get("MYOTA_API_BASE_URL", "https://api.myota.top").rstrip(
    "/"
)
FIXTURE_COUNT = 10_000
BATCH_SIZE = 5_000
CATEGORY_CODE = "SCALE_TEST_FIXTURE"
FIXTURE_SET = "myota-scale-fixtures-sevilla-v1"
POLL_SECONDS = 600


def fixture_feature(index: int) -> dict[str, object]:
    """Create a deterministic point at least roughly 100 m from neighbors."""
    if not 0 <= index < FIXTURE_COUNT:
        raise ValueError("fixture index is outside the supported range")
    columns = 100
    longitude = -5.99 + (index % columns) * 0.0012
    latitude = 37.34 + (index // columns) * 0.0012
    ordinal = index + 1
    return {
        "type": "Feature",
        "id": f"{FIXTURE_SET}:{ordinal:05d}",
        "properties": {
            "name": f"MyOTA synthetic scale fixture Sevilla {ordinal:05d}",
            "sourceRef": f"{FIXTURE_SET}:{ordinal:05d}",
            "entityTypes": [CATEGORY_CODE],
            "continent": "Europe",
            "continentCode": "EU",
            "country": "Spain",
            "countryCode": "ES",
            "region": "Andalucía",
            "regionCode": "ES-AN",
            "subdivision": "Andalucía",
            "subdivisionCode": "ES-AN",
            "province": "Sevilla",
            "provinceCode": "ES-SE",
            "county": "Sevilla",
            "city": "Sevilla",
            "municipality": "Sevilla",
        },
        "geometry": {
            "type": "Point",
            "coordinates": [round(longitude, 6), round(latitude, 6)],
        },
    }


def require_production_acknowledgement() -> None:
    target = os.environ.get("MYOTA_API_BASE_URL", API_URL).rstrip("/")
    hostname = (urlsplit(target).hostname or "").lower()
    if hostname != "api.myota.top":
        raise RuntimeError(
            "permanent scale fixtures are restricted to https://api.myota.top"
        )
    if os.environ.get("MYOTA_SCALE_FIXTURES_ALLOW_PRODUCTION") != "YES":
        raise RuntimeError(
            "set MYOTA_SCALE_FIXTURES_ALLOW_PRODUCTION=YES to acknowledge "
            "permanent writes to the provisional-production catalogue"
        )
    if os.environ.get("MYOTA_SCALE_FIXTURES_PERMANENT") != "YES":
        raise RuntimeError(
            "set MYOTA_SCALE_FIXTURES_PERMANENT=YES to confirm that these "
            "10,000 synthetic records must not be automatically deleted"
        )


def request_json(
    method: str,
    path: str,
    token: str | None = None,
    body: dict[str, object] | None = None,
) -> dict[str, object]:
    headers = {
        "Accept": "application/json",
        "User-Agent": "MyOTA-Scale-Fixture-Provisioner/1.0",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    data = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body, separators=(",", ":")).encode("utf-8")
    request = Request(
        f"{API_URL}{path}", data=data, headers=headers, method=method
    )
    try:
        with urlopen(request, timeout=120) as response:
            payload = response.read()
    except HTTPError as error:
        safe_detail = ""
        try:
            problem = json.loads(error.read().decode("utf-8"))
            safe_detail = str(
                problem.get("detail") or problem.get("code") or ""
            )
        except (UnicodeDecodeError, json.JSONDecodeError, AttributeError):
            pass
        raise RuntimeError(
            f"{method} {path} failed with HTTP {error.code}: {safe_detail[:500]}"
        ) from None
    except URLError as error:
        raise RuntimeError(f"{method} {path} failed: {error.reason}") from None
    return json.loads(payload.decode("utf-8")) if payload else {}


def login() -> str:
    email = os.environ.get("MYOTA_LOAD_TEST_EMAIL", "")
    password = os.environ.get("MYOTA_LOAD_TEST_PASSWORD", "")
    if not email or not password:
        raise RuntimeError(
            "MYOTA_LOAD_TEST_EMAIL and MYOTA_LOAD_TEST_PASSWORD are required"
        )
    response = request_json(
        "POST",
        "/v1/identity/auth/login",
        body={"email": email, "password": password},
    )
    token = response.get("accessToken")
    if not isinstance(token, str) or not token:
        raise RuntimeError("identity login returned no access token")
    return token


def ensure_category(token: str) -> None:
    result = request_json("GET", "/v1/entity-types", token)
    items = result.get("items", [])
    existing = next(
        (
            item
            for item in items
            if isinstance(item, dict) and item.get("code") == CATEGORY_CODE
        ),
        None,
    )
    if existing:
        types = existing.get("geometryTypes") or []
        if existing.get("active") is False or "POINT" not in types:
            raise RuntimeError(
                f"existing {CATEGORY_CODE} category is inactive or does not allow POINT"
            )
        return
    request_json(
        "POST",
        "/v1/entity-types",
        token,
        {
            "code": CATEGORY_CODE,
            "label": "Synthetic scale-test fixture",
            "description": (
                "Synthetic Sevilla-area points reserved for MyOTA capacity "
                "evidence; not real parks or programme entities."
            ),
            "geometryTypes": ["POINT"],
            "active": True,
        },
    )


def entity_count(token: str) -> int:
    query = urlencode({"entityType": CATEGORY_CODE, "page": 1, "pageSize": 1})
    result = request_json("GET", f"/v1/geodata/entities?{query}", token)
    return int(result.get("total", 0))


def import_batch(token: str, start: int, count: int) -> None:
    features = [
        fixture_feature(index) for index in range(start, start + count)
    ]
    body = {
        "adapter": "MANUAL",
        "format": "GEOJSON",
        "filename": f"{FIXTURE_SET}-{start + 1:05d}-{start + count:05d}.geojson",
        "entityType": CATEGORY_CODE,
        "entityTypes": [CATEGORY_CODE],
        "source": {
            "name": "MyOTA synthetic Sevilla scale fixtures",
            "license": "CC0-1.0",
            "attribution": "Synthetic MyOTA-generated scale-test coordinates",
            "sourceKey": FIXTURE_SET,
        },
        "features": features,
    }
    run = request_json("POST", "/v1/geodata/imports", token, body)
    run_id = run.get("id")
    if not isinstance(run_id, str) or not run_id:
        raise RuntimeError("geodata API accepted no import-run identifier")
    print(f"Import accepted: {run_id}; records={count}", flush=True)

    deadline = time.monotonic() + POLL_SECONDS
    while time.monotonic() < deadline:
        run = request_json("GET", f"/v1/geodata/imports/{run_id}", token)
        status = str(run.get("status", "")).upper()
        if status.startswith("PREPROCESSED"):
            break
        if status == "FAILED":
            raise RuntimeError(f"import {run_id} preprocessing failed")
        time.sleep(3)
    else:
        raise RuntimeError(f"timed out waiting for import {run_id}")

    candidate_ids: list[str] = []
    page = 1
    while True:
        query = urlencode({"page": page, "pageSize": 100})
        result = request_json(
            "GET", f"/v1/geodata/imports/{run_id}/candidates?{query}", token
        )
        candidate_ids.extend(
            item["id"]
            for item in result.get("items", [])
            if isinstance(item, dict) and isinstance(item.get("id"), str)
        )
        next_page = result.get("nextPage")
        if not next_page:
            break
        page = int(next_page)
    if len(candidate_ids) != count:
        raise RuntimeError(
            f"import {run_id} staged {len(candidate_ids)} pending records; expected {count}"
        )

    actor = "myota-scale-fixture-provisioner"
    request_json(
        "POST",
        f"/v1/geodata/imports/{run_id}/candidates/validate",
        token,
        {
            "candidateIds": candidate_ids,
            "reviewerId": actor,
            "validationStatus": "VALID",
            "note": "Permanent synthetic scale-test fixture dataset.",
        },
    )
    request_json(
        "POST",
        f"/v1/geodata/imports/{run_id}/process",
        token,
        {
            "candidateIds": candidate_ids,
            "targetStatus": "APPROVED",
            "processorId": actor,
            "note": "Permanent synthetic scale-test fixture dataset.",
        },
    )

    expected_total = start + count
    deadline = time.monotonic() + POLL_SECONDS
    while time.monotonic() < deadline:
        if entity_count(token) >= expected_total:
            break
        time.sleep(3)
    else:
        raise RuntimeError(
            f"promotion for import {run_id} did not reach {expected_total} entities"
        )

    request_json(
        "POST",
        f"/v1/geodata/imports/{run_id}/processed",
        token,
        {"processedBy": actor},
    )
    print(f"Permanent fixtures promoted: total={expected_total}", flush=True)


def main() -> int:
    try:
        require_production_acknowledgement()
        token = login()
        current = entity_count(token)
        if current == FIXTURE_COUNT:
            print(
                f"Fixture set already exists: {current} entities; no changes made."
            )
            return 0
        if current:
            raise RuntimeError(
                f"found {current} {CATEGORY_CODE} records; refusing partial or duplicate provisioning"
            )
        ensure_category(token)
        for start in range(0, FIXTURE_COUNT, BATCH_SIZE):
            import_batch(token, start, min(BATCH_SIZE, FIXTURE_COUNT - start))
        print(
            f"Provisioned {FIXTURE_COUNT} permanent synthetic fixtures in Sevilla. "
            "The script intentionally provides no cleanup operation."
        )
        return 0
    except (RuntimeError, ValueError, KeyError) as error:
        print(f"Fixture provisioning stopped safely: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
