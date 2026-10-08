#!/usr/bin/env python3
"""Capture bounded, read-only PostGIS EXPLAIN ANALYZE evidence."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from urllib.parse import urlparse

import psycopg


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bbox",
        default="-5.99,37.37,-5.90,37.43",
        help="minLon,minLat,maxLon,maxLat (default: a Sevilla-area window)",
    )
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--statement-timeout-ms", type=int, default=5000)
    parser.add_argument(
        "--output", help="optional path for the JSON evidence report"
    )
    args = parser.parse_args()

    environment = os.environ.get("MYOTA_ENV", "").lower()
    production = environment == "production"
    if environment not in {"development", "test", "staging", "production"}:
        parser.error("set MYOTA_ENV=development, test, staging, or production")
    if os.environ.get("MYOTA_ALLOW_EXPLAIN_ANALYZE") != "YES":
        parser.error(
            "set MYOTA_ALLOW_EXPLAIN_ANALYZE=YES to acknowledge read-only query execution"
        )
    dsn = os.environ.get("GEO_DATABASE_URL", "")
    if not dsn:
        parser.error("GEO_DATABASE_URL is required")
    hostname = (urlparse(dsn).hostname or "").lower()
    production_hosts = {
        host.strip().lower()
        for host in os.environ.get(
            "MYOTA_PRODUCTION_GEO_DATABASE_HOSTS", ""
        ).split(",")
        if host.strip()
    }
    production_host = any(
        part in hostname
        for part in ("spainip.es", "myota.top", "production", "prod-db")
    )
    if production:
        if os.environ.get("MYOTA_ALLOW_PRODUCTION_EXPLAIN") != "YES":
            parser.error(
                "production EXPLAIN requires MYOTA_ALLOW_PRODUCTION_EXPLAIN=YES"
            )
        if not hostname or hostname not in production_hosts:
            parser.error(
                "production database hostname must exactly match "
                "MYOTA_PRODUCTION_GEO_DATABASE_HOSTS"
            )
    elif production_host:
        parser.error("the evidence tool refuses production database hosts")
    try:
        bounds = [float(value.strip()) for value in args.bbox.split(",")]
    except ValueError:
        parser.error("--bbox must be four comma-separated numbers")
    if len(bounds) != 4 or bounds[0] >= bounds[2] or bounds[1] >= bounds[3]:
        parser.error(
            "--bbox must be minLon,minLat,maxLon,maxLat with increasing bounds"
        )
    if not 1 <= args.limit <= 1000:
        parser.error("--limit must be between 1 and 1000")
    if not 1 <= args.statement_timeout_ms <= 30000:
        parser.error("--statement-timeout-ms must be between 1 and 30000")

    map_sql = """EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT id::text, programme_slug, entity_type_code, name,
               lifecycle_status, ST_AsGeoJSON(geom)::jsonb, public_properties
        FROM geodata_entity
        WHERE geom && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
          AND ST_Intersects(geom, ST_MakeEnvelope(%s, %s, %s, %s, 4326))
          AND (%s::text IS NULL OR programme_slug = %s::text)
          AND (%s::text IS NULL OR lifecycle_status::text = %s::text)
        ORDER BY name, id
        LIMIT %s"""
    catalogue_filter = "geom && ST_MakeEnvelope(%s, %s, %s, %s, 4326)"
    catalogue_count_sql = f"""EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT count(*) FROM geodata_entity WHERE {catalogue_filter}"""
    catalogue_page_sql = f"""EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT public_properties || jsonb_build_object(
            'id', id::text,
            'programmeSlug', programme_slug,
            'entityType', entity_type_code,
            'name', name,
            'status', lifecycle_status,
            'geometry', ST_AsGeoJSON(geom)::jsonb,
            'sourceState', source_state,
            'jurisdiction', jurisdiction,
            'attachments', attachments,
            'version', revision
        )
        FROM geodata_entity
        WHERE {catalogue_filter}
        ORDER BY lower(name), id
        LIMIT %s OFFSET 0"""
    report = {
        "capturedAt": datetime.now(timezone.utc).isoformat(),
        "environment": environment,
        "databaseHost": hostname,
        "bbox": bounds,
        "limit": args.limit,
        "statementTimeoutMs": args.statement_timeout_ms,
    }
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute("SET default_transaction_read_only = on")
        connection.execute(
            "SELECT set_config('statement_timeout', %s, false)",
            (f"{args.statement_timeout_ms}ms",),
        )
        report["serverVersion"] = connection.execute(
            "SHOW server_version"
        ).fetchone()[0]
        report["estimatedEntityRows"] = connection.execute(
            "SELECT COALESCE(reltuples::bigint, 0) FROM pg_class "
            "WHERE oid = 'geodata_entity'::regclass"
        ).fetchone()[0]
        report["spatialIndexes"] = [
            row[0]
            for row in connection.execute(
                "SELECT indexdef FROM pg_indexes WHERE tablename='geodata_entity' AND indexdef ILIKE '%USING gist%' ORDER BY indexname"
            ).fetchall()
        ]
        bbox_params = tuple(bounds)
        report["plans"] = {
            "mapBounds": connection.execute(
                map_sql,
                (
                    *bbox_params,
                    *bbox_params,
                    None,
                    None,
                    None,
                    None,
                    args.limit,
                ),
            ).fetchone()[0],
            "catalogueCount": connection.execute(
                catalogue_count_sql, bbox_params
            ).fetchone()[0],
            "cataloguePage": connection.execute(
                catalogue_page_sql, (*bbox_params, args.limit)
            ).fetchone()[0],
        }
    encoded = json.dumps(report, indent=2, default=str)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as destination:
            destination.write(encoded + "\n")
    print(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
