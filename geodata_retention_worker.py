"""Scheduled worker entry point for geodata import retention."""

from __future__ import annotations

import argparse
import logging
import os
import time

from import_retention import purge_expired_imports
from geodata import GeoHandler
from storage import ObjectStore


def purge_sweep() -> dict[str, object]:
    """Drain all eligible rows in bounded batches, stopping safely on errors."""
    batch_size = int(
        os.environ.get("GEODATA_IMPORT_RETENTION_BATCH_SIZE", "100")
    )
    total = {"eligible": 0, "purged": 0, "failed": 0, "failedRunIds": []}
    excluded: set[str] = set()
    while True:
        result = purge_expired_imports(
            batch_size=batch_size, excluded_run_ids=excluded
        )
        for key in ("eligible", "purged", "failed"):
            total[key] += result[key]
        excluded.update(result["failedRunIds"])
        total["failedRunIds"].extend(result["failedRunIds"])
        if result["eligible"] < batch_size or result["purged"] == 0:
            total["uploadsExpired"] = expire_upload_sessions(batch_size)
            return total


def expire_upload_sessions(batch_size: int = 100) -> int:
    """Abort stale S3 multipart sessions and retain only compact history."""
    if not GeoHandler.store.durable:
        return 0
    with GeoHandler.store.transaction() as connection:
        expired = connection.execute(
            "SELECT id::text, bucket, object_key, multipart_upload_id "
            "FROM geodata_upload_session WHERE status IN ('UPLOADING','COMPLETING') "
            "AND expires_at <= now() ORDER BY expires_at LIMIT %s",
            (batch_size,),
        ).fetchall()
    store = ObjectStore()
    completed_ids = []
    for upload_id, bucket, object_key, multipart_id in expired:
        try:
            store.abort_multipart(bucket, object_key, multipart_id)
        except Exception:
            logging.getLogger(__name__).exception(
                "failed to abort expired multipart upload %s", upload_id
            )
            continue
        completed_ids.append(upload_id)
    if completed_ids:
        with GeoHandler.store.transaction() as connection:
            connection.execute(
                "UPDATE geodata_upload_session SET status='EXPIRED', updated_at=now() "
                "WHERE id = ANY(%s::uuid[]) AND status IN ('UPLOADING','COMPLETING')",
                (completed_ids,),
            )
    with GeoHandler.store.transaction() as connection:
        connection.execute(
            "DELETE FROM geodata_upload_session WHERE status IN ('ABORTED','EXPIRED') "
            "AND updated_at < now() - interval '30 days'"
        )
    return len(completed_ids)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--loop", action="store_true", help="run now, then repeat daily"
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=int(
            os.environ.get(
                "GEODATA_IMPORT_RETENTION_INTERVAL_SECONDS", "86400"
            )
        ),
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if args.interval_seconds < 60:
        parser.error("--interval-seconds must be at least 60")
    while True:
        purge_sweep()
        if not args.loop:
            return
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    main()
