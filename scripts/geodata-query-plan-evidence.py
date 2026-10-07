#!/usr/bin/env python3
"""Capture repeatable, read-only PostGIS EXPLAIN ANALYZE evidence off production."""

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
    parser.add_argument(
        "--output", help="optional path for the JSON evidence report"
    )
    args = parser.parse_args()

    environment = os.environ.get("MYOTA_ENV", "").lower()
    if environment not in {"development", "test", "staging"}:
        parser.error(
            "set MYOTA_ENV=development, test, or staging; production is refused"
        )
    if os.environ.get("MYOTA_ALLOW_EXPLAIN_ANALYZE") != "YES":
        parser.error(
            "set MYOTA_ALLOW_EXPLAIN_ANALYZE=YES to acknowledge read-only query execution"
        )
    dsn = os.environ.get("GEO_DATABASE_URL", "")
    if not dsn:
        parser.error("GEO_DATABASE_URL is required")
    hostname = (urlparse(dsn).hostname or "").lower()
    if any(
        part in hostname
        for part in ("spainip.es", "myota.top", "production", "prod-db")
    ):
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

    sql = """EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT id, name, lifecycle_status, entity_type_code, ST_AsGeoJSON(geom)
        FROM geodata_entity
        WHERE geom && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
          AND ST_Intersects(geom, ST_MakeEnvelope(%s, %s, %s, %s, 4326))
        ORDER BY name, id
        LIMIT %s"""
    report = {
        "capturedAt": datetime.now(timezone.utc).isoformat(),
        "environment": environment,
        "databaseHost": hostname,
        "bbox": bounds,
        "limit": args.limit,
    }
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute("SET default_transaction_read_only = on")
        report["serverVersion"] = connection.execute(
            "SHOW server_version"
        ).fetchone()[0]
        report["entityRows"] = connection.execute(
            "SELECT count(*) FROM geodata_entity"
        ).fetchone()[0]
        report["spatialIndexes"] = [
            row[0]
            for row in connection.execute(
                "SELECT indexdef FROM pg_indexes WHERE tablename='geodata_entity' AND indexdef ILIKE '%USING gist%' ORDER BY indexname"
            ).fetchall()
        ]
        report["plan"] = connection.execute(
            sql, (*bounds, *bounds, args.limit)
        ).fetchone()[0]
    encoded = json.dumps(report, indent=2, default=str)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as destination:
            destination.write(encoded + "\n")
    print(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
