"""Exact-tag upload cleanup inside the owning geodata cleanup transaction."""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class UploadFixture:
    upload_id: str
    bucket: str
    object_key: str
    status: str


def tagged_upload_fixtures(store: Any, run_id: str) -> list[UploadFixture]:
    if not store.durable:
        return []
    with store.transaction() as connection:
        rows = connection.execute(
            "SELECT id::text,bucket,object_key,status "
            "FROM geodata_upload_session "
            "WHERE metadata #>> '{source,loadTestRunId}'=%s FOR UPDATE",
            (run_id,),
        ).fetchall()
    fixtures = [UploadFixture(*row) for row in rows]
    terminal = {"COMPLETED", "ABORTED", "FAILED", "EXPIRED"}
    if any(fixture.status not in terminal for fixture in fixtures):
        raise ValueError(
            "load-test uploads must finish or be aborted before cleanup "
            "can remove them"
        )
    return fixtures


def purge_upload_fixtures(
    store: Any, run_id: str, fixtures: list[UploadFixture]
) -> int:
    if not fixtures:
        return 0
    with store.transaction() as connection:
        result = connection.execute(
            "DELETE FROM geodata_upload_session "
            "WHERE id=ANY(%s::uuid[]) "
            "AND metadata #>> '{source,loadTestRunId}'=%s "
            "AND status IN ('COMPLETED','ABORTED','FAILED','EXPIRED')",
            ([fixture.upload_id for fixture in fixtures], run_id),
        )
        # Part records cascade through the service-owned foreign key.
        return result.rowcount
