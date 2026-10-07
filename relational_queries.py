"""Bounded catalogue and import queries over service-owned tables."""

from __future__ import annotations

import time
from typing import Any

from relational_state import PROJECTIONS, canonical


def pagination(query: dict[str, list[str]]) -> tuple[int, int]:
    try:
        return (
            max(1, int(query.get("page", ["1"])[0])),
            min(100, max(1, int(query.get("pageSize", ["50"])[0]))),
        )
    except ValueError as error:
        raise ValueError("page and pageSize must be integers") from error


def location_expression(field: str) -> str:
    return (
        "lower(coalesce(nullif(public_properties->>"
        f"'{field}', ''), public_properties->'location'->>'{field}', ''))"
    )


def page_query(
    store: Any,
    kind: str,
    query: dict[str, list[str]],
    where: str = "true",
    params: tuple = (),
    order: str = "id",
    table: str | None = None,
) -> dict[str, Any]:
    from relational_state import TABLES

    page, size = pagination(query)
    table = table or TABLES[kind]
    with store.transaction() as connection:
        total = connection.execute(
            f"SELECT count(*) FROM {table} WHERE {where}", params
        ).fetchone()[0]
        rows = connection.execute(
            f"SELECT {PROJECTIONS[kind]} FROM {table} WHERE {where} "
            f"ORDER BY {order} LIMIT %s OFFSET %s",
            (*params, size, (page - 1) * size),
        ).fetchall()
    return {
        "items": [canonical(row[0]) for row in rows],
        "page": page,
        "pageSize": size,
        "total": total,
        "nextPage": page + 1 if page * size < total else None,
    }


def catalogue_page(
    store: Any, query: dict[str, list[str]], bounds: tuple | None
) -> dict[str, Any]:
    conditions, params = [], []
    programme = query.get("programme", [None])[0]
    if programme:
        conditions.append("programme_slug=%s")
        params.append(programme)
    statuses = {
        value.strip().upper()
        for raw in query.get("status", [])
        for value in raw.split(",")
        if value.strip()
    }
    if statuses and "ALL" not in statuses:
        conditions.append("lifecycle_status=ANY(%s)")
        params.append(sorted(statuses))
    category = query.get("entityType", [None])[0]
    if category:
        conditions.append(
            "EXISTS (SELECT 1 FROM geodata_entity_category AS category "
            "WHERE category.entity_id=geodata_entity.id "
            "AND lower(category.category_code)=%s)"
        )
        params.append(category.lower())
    for name in ("continent", "country", "region", "province"):
        value = query.get(name, [None])[0]
        if value:
            conditions.append(f"{location_expression(name)}=%s")
            params.append(value.lower())
    city = query.get("city", [None])[0]
    if city:
        conditions.append(
            f"({location_expression('city')}=%s OR "
            f"{location_expression('municipality')}=%s)"
        )
        params.extend([city.lower(), city.lower()])
    if bounds:
        conditions.append("geom && ST_MakeEnvelope(%s,%s,%s,%s,4326)")
        params.extend(bounds)
    started = time.perf_counter()
    try:
        return page_query(
            store,
            "entities",
            query,
            " AND ".join(conditions) or "true",
            tuple(params),
            "lower(name), id",
        )
    finally:
        store._observe_postgis_query("catalogue", started)


def candidate_counts(store: Any, run_id: str) -> dict[str, int]:
    with store.transaction() as connection:
        rows = connection.execute(
            "SELECT validation_status,count(*) FROM geodata_import_candidate "
            "WHERE import_run_id=%s GROUP BY validation_status",
            (run_id,),
        ).fetchall()
    counts = {str(row[0]).lower(): row[1] for row in rows}
    return {
        "total": sum(counts.values()),
        **{
            name: counts.get(name, 0)
            for name in ("pending", "confirmed", "processed", "rejected")
        },
    }


def location_rows(store: Any) -> list[dict[str, Any]]:
    # Distinct metadata only: no geometry/candidate/catalogue hydration.
    fields = (
        "continent",
        "continentCode",
        "country",
        "countryCode",
        "region",
        "regionCode",
        "province",
        "provinceCode",
        "county",
        "city",
        "municipality",
    )
    projection = ",".join(
        f"'{name}',coalesce(nullif(public_properties->>'{name}',''),"
        f"public_properties->'location'->>'{name}')"
        for name in fields
    )
    with store.transaction() as connection:
        rows = connection.execute(
            f"SELECT DISTINCT jsonb_build_object({projection}) "
            "FROM geodata_entity"
        ).fetchall()
    return [row[0] for row in rows]


def audit_rows(store: Any, entity_id: str) -> list[dict[str, Any]]:
    with store.transaction() as connection:
        rows = connection.execute(
            "SELECT event FROM geodata_audit_event WHERE aggregate_type='entity' "
            "AND aggregate_id=%s ORDER BY occurred_at,event_id",
            (entity_id,),
        ).fetchall()
    return [canonical(row[0]) for row in rows]
