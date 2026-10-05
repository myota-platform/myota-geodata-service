"""Retention of finalized geodata imports and their source objects."""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Callable

from storage import ObjectStore

logger = logging.getLogger("myota.geodata.import_retention")
STALE_IMPORT_STATUSES = (
    "'UPLOAD_PENDING', 'QUEUED', 'PROCESSING', 'PREPROCESSED', "
    "'PREPROCESSED_WITH_ERRORS', 'COMPLETED', 'COMPLETED_WITH_ERRORS', 'FAILED'"
)
LAST_ACTIVITY_SQL = (
    "GREATEST(started_at, COALESCE(heartbeat_at, '-infinity'::timestamptz), "
    "COALESCE(completed_at, '-infinity'::timestamptz))"
)
ELIGIBLE_WHERE_SQL = (
    "((status = 'PROCESSED' AND processed_at < now() - (%s * interval '1 day')) "
    f"OR (status IN ({STALE_IMPORT_STATUSES}) AND "
    f"{LAST_ACTIVITY_SQL} < now() - (%s * interval '1 day')))"
)


def source_object(metadata: Any, import_bucket: str) -> tuple[str, str] | None:
    """Return a safe source object reference owned by the import bucket only."""
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except json.JSONDecodeError:
            raise ValueError("import source metadata is not valid JSON")
    if not isinstance(metadata, dict):
        raise ValueError("import source metadata is not an object")
    source = metadata.get("source")
    if not isinstance(source, dict):
        return None
    bucket, key = source.get("bucket"), source.get("objectKey")
    if not bucket and key:
        bucket = import_bucket
    if not key:
        return None
    if bucket != import_bucket:
        raise ValueError("import source points outside the configured import bucket")
    if not isinstance(key, str) or not key.strip():
        raise ValueError("import source has an invalid object key")
    parts = key.split("/")
    if (not import_bucket or "/" in import_bucket or "\\" in import_bucket
            or import_bucket in {".", ".."}):
        raise ValueError("configured import bucket is unsafe")
    if key.startswith("/") or "\\" in key or any(part in {"", ".", ".."} for part in parts):
        raise ValueError("unsafe import object key")
    return bucket, key


def _purge_run(connection: Any, run_id: str, retention_days: int) -> bool:
    """Delete retained import logs and metadata, preserving domain entities."""
    eligible = connection.execute(
        f"SELECT id FROM import_run WHERE id = %s AND {ELIGIBLE_WHERE_SQL} FOR UPDATE",
        (run_id, retention_days, retention_days),
    ).fetchone()
    if not eligible:
        return False
    event_rows = connection.execute(
        "SELECT event_id FROM outbox_event WHERE aggregate_type = 'import_run' "
        "AND aggregate_id = %s AND published_at IS NOT NULL "
        "AND occurred_at < now() - (%s * interval '1 day')",
        (run_id, retention_days),
    ).fetchall()
    event_ids = [row[0] for row in event_rows]
    if event_ids:
        connection.execute(
            "DELETE FROM consumer_processed_event WHERE event_id = ANY(%s)", (event_ids,)
        )
        connection.execute("DELETE FROM dead_letter_event WHERE event_id = ANY(%s)", (event_ids,))
        connection.execute("DELETE FROM outbox_event WHERE event_id = ANY(%s)", (event_ids,))

    # The snapshot manifest FK intentionally has no cascade: remove its
    # import-owned audit data explicitly before deleting the import history.
    connection.execute("DELETE FROM source_snapshot_manifest WHERE import_run_id = %s", (run_id,))
    cursor = connection.execute(
        f"DELETE FROM import_run WHERE id = %s AND {ELIGIBLE_WHERE_SQL}",
        (run_id, retention_days, retention_days),
    )
    if cursor.rowcount != 1:  # defensive; the row is locked and was just verified
        raise RuntimeError("eligible import run disappeared during retention")

    row = connection.execute(
        "SELECT state FROM service_state WHERE service = 'geodata' FOR UPDATE"
    ).fetchone()
    if row:
        state = row[0] if isinstance(row[0], dict) else json.loads(row[0] or "{}")
        data = state.get("data") if isinstance(state.get("data"), dict) else {}
        runs = data.get("importRuns") if isinstance(data.get("importRuns"), dict) else {}
        runs.pop(run_id, None)
        data["importRuns"] = runs
        state["data"] = data
        state["events"] = [
            event for event in state.get("events", [])
            if not (
                (event.get("aggregate") or {}).get("type") == "import_run"
                and str((event.get("aggregate") or {}).get("id")) == run_id
            )
        ]
        connection.execute(
            "UPDATE service_state SET state = %s::jsonb, updated_at = now() WHERE service = 'geodata'",
            (json.dumps(state),),
        )
    return True


def purge_expired_imports(
    *,
    dsn: str | None = None,
    object_store: Any | None = None,
    connection_factory: Callable[..., Any] | None = None,
    retention_days: int | None = None,
    batch_size: int | None = None,
    excluded_run_ids: set[str] | None = None,
) -> dict[str, int]:
    """Purge finalized or inactive import runs older than the retention window.

    Source objects are deleted before database history. This makes retries safe:
    S3 delete is idempotent, and a failed DB transaction leaves the import row
    available for the next run. Finalized runs age from processed_at; queued,
    preprocessed, failed, and stalled runs age from their latest activity.
    """
    dsn = dsn if dsn is not None else os.environ.get("GEO_DATABASE_URL", "")
    if not dsn:
        raise RuntimeError("GEO_DATABASE_URL is required for import retention")
    if connection_factory is None:
        import psycopg
        connection_factory = psycopg.connect
    retention_days = (int(os.environ.get("GEODATA_IMPORT_RETENTION_DAYS", "30"))
                      if retention_days is None else retention_days)
    batch_size = (int(os.environ.get("GEODATA_IMPORT_RETENTION_BATCH_SIZE", "100"))
                  if batch_size is None else batch_size)
    if retention_days < 1 or batch_size < 1:
        raise ValueError("retention days and batch size must be positive")
    import_bucket = os.environ.get("MYOTA_GEODATA_IMPORT_BUCKET", "myota-geodata-imports")
    object_store = object_store or ObjectStore()
    counts: dict[str, Any] = {"eligible": 0, "purged": 0, "failed": 0, "failedRunIds": []}

    # Read a bounded snapshot and close the transaction before touching object
    # storage. A single CronJob/Compose worker is configured, but conditional
    # deletes below also make overlapping runs safe.
    with connection_factory(dsn) as connection:
        query = (
            f"SELECT id::text, source_metadata FROM import_run WHERE {ELIGIBLE_WHERE_SQL} "
            f"{ 'AND NOT (id = ANY(%s::uuid[])) ' if excluded_run_ids else '' }"
            f"ORDER BY CASE WHEN status = 'PROCESSED' THEN processed_at ELSE {LAST_ACTIVITY_SQL} END, id LIMIT %s"
        )
        params = (retention_days, retention_days)
        if excluded_run_ids:
            params += (list(excluded_run_ids),)
        params += (batch_size,)
        rows = connection.execute(query, params).fetchall()
    counts["eligible"] = len(rows)

    for run_id, metadata in rows:
        try:
            object_ref = source_object(metadata, import_bucket)
            if object_ref:
                object_store.delete(*object_ref)
            with connection_factory(dsn) as connection:
                purged = _purge_run(connection, run_id, retention_days)
            if purged:
                counts["purged"] += 1
        except Exception:
            counts["failed"] += 1
            counts["failedRunIds"].append(run_id)
            logger.exception("Failed to purge expired import run %s", run_id)
    logger.info("Import retention finished: %s", counts)
    return counts
