"""Database aggregates for scrapes, without hydrating entity geometries."""

from typing import Any


def durable_metrics(store: Any) -> dict[str, float]:
    result: dict[str, float] = {}
    with store.operation():
        with store.transaction() as connection:
            for table, column, metric in (
                ("geodata_entity", "lifecycle_status", "entities_by_status"),
                ("import_run", "status", "import_runs_by_status"),
                (
                    "geodata_import_candidate",
                    "validation_status",
                    "import_candidates_by_validation",
                ),
            ):
                rows = connection.execute(
                    f"SELECT {column},count(*) FROM {table} GROUP BY {column}"
                ).fetchall()
                for status, count in rows:
                    result[
                        f'myota_geodata_{metric}_total{{status="{status}"}}'
                    ] = float(count)
                name = {
                    "geodata_entity": "entities",
                    "import_run": "import_runs",
                    "geodata_import_candidate": "import_candidates",
                }[table]
                result[f"myota_geodata_{name}_total"] = float(
                    sum(row[1] for row in rows)
                )
            geometries = connection.execute(
                "SELECT GeometryType(geom),count(*) FROM geodata_entity "
                "GROUP BY GeometryType(geom)"
            ).fetchall()
            names = {
                "POINT": "Point",
                "LINESTRING": "LineString",
                "MULTILINESTRING": "MultiLineString",
                "POLYGON": "Polygon",
                "MULTIPOLYGON": "MultiPolygon",
            }
            for geometry, count in geometries:
                label = names.get(geometry, geometry)
                result[
                    f'myota_geodata_entities_by_geometry_total{{geometry_type="{label}"}}'
                ] = float(count)
            result["myota_geodata_entity_categories_total"] = float(
                connection.execute(
                    "SELECT count(DISTINCT category_code) FROM geodata_entity_category"
                ).fetchone()[0]
            )
            row = connection.execute(
                "SELECT count(*) FILTER (WHERE status IN ('QUEUED','UPLOAD_PENDING')),"
                "count(*) FILTER (WHERE status='PROCESSING'),"
                "coalesce(sum(attempt_count),0),"
                "coalesce(sum((stats->>'preprocessed')::bigint),0),"
                "coalesce(sum(coalesce((stats->>'created')::bigint,0) "
                "+ coalesce((stats->>'updated')::bigint,0)),0),"
                "coalesce(max(extract(epoch FROM now()-started_at)) "
                "FILTER (WHERE status IN ('QUEUED','UPLOAD_PENDING')),0),"
                "coalesce(max(extract(epoch FROM now()-coalesce(heartbeat_at,started_at))) "
                "FILTER (WHERE status='PROCESSING'),0) FROM import_run"
            ).fetchone()
            for index, name in enumerate(
                (
                    "import_queue_depth",
                    "import_processing_runs",
                    "import_attempt_count_sum",
                    "import_features_preprocessed_sum",
                    "import_features_promoted_sum",
                    "import_oldest_queued_age_seconds",
                    "import_oldest_processing_heartbeat_age_seconds",
                )
            ):
                result[f"myota_geodata_{name}"] = float(row[index])
            row = connection.execute(
                "SELECT count(*) FILTER (WHERE state='active' AND pid<>pg_backend_pid()),"
                "count(*) FILTER (WHERE wait_event_type='Lock'),"
                "(SELECT setting::bigint FROM pg_settings WHERE name='max_connections') "
                "FROM pg_stat_activity WHERE datname=current_database()"
            ).fetchone()
            for index, name in enumerate(
                (
                    "active_connections",
                    "lock_waiting_connections",
                    "max_connections",
                )
            ):
                result[f"myota_geodata_postgres_{name}"] = float(row[index])
            row = connection.execute(
                "SELECT count(*),coalesce(extract(epoch FROM now()-min(occurred_at)),0) "
                "FROM outbox_event WHERE published_at IS NULL"
            ).fetchone()
            result["myota_geodata_outbox_pending_events"] = float(row[0])
            result["myota_geodata_outbox_oldest_pending_age_seconds"] = float(
                row[1]
            )
    stats = store._ensure_pool().get_stats()
    for name, field in (
        ("connections", "pool_size"),
        ("available_connections", "pool_available"),
        ("waiting_requests", "requests_waiting"),
        ("wait_milliseconds_total", "requests_wait_ms"),
    ):
        result[f"myota_geodata_postgres_pool_{name}"] = float(
            stats.get(field, 0)
        )
    return result
