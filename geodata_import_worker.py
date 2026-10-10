"""Durable JetStream consumers for geodata preprocessing and promotion."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import uuid
from typing import Any, Awaitable, Callable

import psycopg
from nats.aio.client import Client as NATS
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.errors import FetchTimeoutError

from geodata import GeoHandler

LOG = logging.getLogger("geodata.import.worker")
MAX_DELIVERIES = int(os.environ.get("GEODATA_WORKER_MAX_DELIVERIES", "100"))
ACK_WAIT_SECONDS = int(
    os.environ.get("GEODATA_WORKER_ACK_WAIT_SECONDS", "120")
)
MAX_ACK_PENDING = int(os.environ.get("GEODATA_WORKER_MAX_ACK_PENDING", "1"))
RETRY_DELAY_SECONDS = int(
    os.environ.get("GEODATA_WORKER_RETRY_DELAY_SECONDS", "5")
)
CANCELLATION_RECONCILE_SECONDS = int(
    os.environ.get("GEODATA_CANCELLATION_RECONCILE_SECONDS", "30")
)
WORK_RECONCILE_SECONDS = int(
    os.environ.get("GEODATA_WORK_RECONCILE_SECONDS", "60")
)
WORK_RECOVERY_AGE_SECONDS = max(
    300, int(os.environ.get("GEODATA_WORK_RECOVERY_AGE_SECONDS", "300"))
)
WORK_RECOVERY_BATCH_SIZE = 50
WORK_TYPES = {
    "geodata-preprocessing-v1": "geodata.import-preprocess.v1",
    "geodata-import-promotion-v1": "geodata.import-promotion.v1",
    "geodata-entity-deletion-v1": "geodata.entity-delete.v1",
    "geodata-location-enrichment-v1": "geodata.location-enrichment.v1",
}


def _event_key(event: dict[str, Any]) -> tuple[str, str]:
    event_id = str(event.get("workId") or event.get("eventId") or "")
    event_type = str(event.get("workType") or event.get("eventType") or "")
    if not event_id or not event_type.endswith(".v1"):
        raise ValueError("unsupported or malformed geodata work envelope")
    try:
        if str(uuid.UUID(event_id)) != event_id:
            raise ValueError("work ID must use canonical UUID form")
    except (ValueError, AttributeError) as exc:
        raise ValueError("work ID must be a UUID") from exc
    return event_id, event_type


def _already_processed(consumer: str, event_id: str) -> bool:
    with GeoHandler.store.transaction() as connection:
        return bool(
            connection.execute(
                "SELECT 1 FROM consumer_processed_event "
                "WHERE consumer=%s AND event_id=%s",
                (consumer, event_id),
            ).fetchone()
        )


def _record_processed(consumer: str, event: dict[str, Any]) -> None:
    message_id = (
        event.get("causationId") or event.get("workId") or event.get("eventId")
    )
    if not message_id:
        raise ValueError("processed message has no stable work/event ID")
    with GeoHandler.store.transaction() as connection:
        connection.execute(
            "INSERT INTO consumer_processed_event(consumer,event_id) "
            "VALUES (%s,%s) ON CONFLICT DO NOTHING",
            (consumer, message_id),
        )
        connection.execute(
            "INSERT INTO consumer_checkpoint(consumer,last_event_id) "
            "VALUES (%s,%s) ON CONFLICT (consumer) DO UPDATE "
            "SET last_event_id=EXCLUDED.last_event_id, updated_at=now()",
            (consumer, message_id),
        )


def _work_still_pending(consumer: str, event: dict[str, Any]) -> bool:
    payload = event.get("payload") or {}
    aggregate = event.get("aggregate") or {}
    if consumer == "geodata-preprocessing-v1":
        run_id = payload.get("importRunId") or aggregate.get("id")
        key = run_id
        query = "SELECT status FROM import_run WHERE id=%s"
    elif consumer == "geodata-import-promotion-v1":
        queue_id = payload.get("queueId") or aggregate.get("id")
        key = queue_id
        query = (
            "SELECT status FROM geodata_import_processing_queue WHERE id=%s"
        )
    elif consumer == "geodata-entity-deletion-v1":
        job_id = payload.get("jobId") or aggregate.get("id")
        with GeoHandler.store.transaction() as connection:
            row = connection.execute(
                "SELECT payload->>'status' FROM geodata_control_record "
                "WHERE kind=%s AND id=%s",
                ("entityDeletionJobs", job_id),
            ).fetchone()
        status = str(row[0]).upper() if row and row[0] else ""
        return status in {"QUEUED", "PROCESSING"}
    elif consumer == "geodata-location-enrichment-v1":
        entity_id = payload.get("entityId") or aggregate.get("id")
        with GeoHandler.store.transaction() as connection:
            row = connection.execute(
                "SELECT public_properties->>'locationEnrichmentStatus', "
                "public_properties->>'locationEnrichmentRequestId' "
                "FROM geodata_entity WHERE id=%s",
                (entity_id,),
            ).fetchone()
        return bool(
            row
            and str(row[0] or "").upper() == "QUEUED"
            and row[1] == payload.get("requestId")
        )
    else:
        return False
    if not key:
        return False
    with GeoHandler.store.transaction() as connection:
        row = connection.execute(query, (key,)).fetchone()
    status = str(row[0]).upper() if row and row[0] else ""
    active = {"QUEUED", "PROCESSING"}
    if consumer == "geodata-preprocessing-v1":
        active.add("CANCELLING")
    return status in active


def _stale_cancellation_ids() -> list[str]:
    """Return cancellation requests whose worker lease has expired."""
    GeoHandler.store.refresh_import_runs()
    with GeoHandler.store.transaction() as connection:
        rows = connection.execute(
            "SELECT id::text FROM import_run "
            "WHERE status='CANCELLING' "
            "AND (lease_until IS NULL OR lease_until <= now()) "
            "ORDER BY cancellation_requested_at, id"
        ).fetchall()
    return [str(row[0]) for row in rows]


def _insert_recovery_outbox(
    connection,
    event_type: str,
    aggregate_type: str,
    aggregate_id: str,
    payload: dict[str, Any],
) -> None:
    connection.execute(
        "INSERT INTO outbox_event(event_id,event_type,producer,aggregate_type,"
        "aggregate_id,payload,occurred_at) "
        "VALUES (%s,%s,'geodata',%s,%s,%s::jsonb,now())",
        (
            str(uuid.uuid4()),
            event_type,
            aggregate_type,
            aggregate_id,
            json.dumps(payload),
        ),
    )


def _recover_expired_work_dispatches() -> dict[str, int]:
    """Re-enqueue stale commands from database-owned rows, never execute them."""
    recovered = {
        "preprocessing": 0,
        "promotion": 0,
        "deletion": 0,
        "location": 0,
    }
    age = WORK_RECOVERY_AGE_SECONDS
    with psycopg.connect(GeoHandler.store.dsn) as connection:
        connection.execute("SET LOCAL myota.geodata_writer = 'row-v1'")
        runs = connection.execute(
            "SELECT id::text,status FROM import_run WHERE status IN "
            "('QUEUED','PROCESSING') AND work_dispatched_at <= "
            "now()-make_interval(secs => %s) AND "
            "(status='QUEUED' OR lease_until IS NULL OR lease_until<=now()) "
            "ORDER BY work_dispatched_at,id FOR UPDATE SKIP LOCKED LIMIT %s",
            (age, WORK_RECOVERY_BATCH_SIZE),
        ).fetchall()
        for run_id, status in runs:
            if status == "PROCESSING":
                connection.execute(
                    "UPDATE import_run SET status='QUEUED',heartbeat_at=NULL,"
                    "lease_until=NULL,last_error=%s,work_dispatched_at=now() "
                    "WHERE id=%s",
                    ("Expired lease re-dispatched through JetStream", run_id),
                )
            else:
                connection.execute(
                    "UPDATE import_run SET work_dispatched_at=now() WHERE id=%s",
                    (run_id,),
                )
            _insert_recovery_outbox(
                connection,
                "geodata.import.recovered.v1",
                "import_run",
                run_id,
                {
                    "importRunId": run_id,
                    "natsSubject": "myota.geodata.import.preprocess.v1",
                },
            )
            recovered["preprocessing"] += 1

        queues = connection.execute(
            "SELECT id::text,status FROM geodata_import_processing_queue "
            "WHERE status IN ('QUEUED','PROCESSING') AND "
            "work_dispatched_at <= now()-make_interval(secs => %s) AND "
            "(status='QUEUED' OR lease_until IS NULL OR lease_until<=now()) "
            "ORDER BY work_dispatched_at,id FOR UPDATE SKIP LOCKED LIMIT %s",
            (age, WORK_RECOVERY_BATCH_SIZE),
        ).fetchall()
        for queue_id, status in queues:
            if status == "PROCESSING":
                connection.execute(
                    "UPDATE geodata_import_processing_queue SET status='QUEUED',"
                    "heartbeat_at=NULL,lease_until=NULL,error=%s,"
                    "work_dispatched_at=now() WHERE id=%s",
                    (
                        "Expired lease re-dispatched through JetStream",
                        queue_id,
                    ),
                )
            else:
                connection.execute(
                    "UPDATE geodata_import_processing_queue "
                    "SET work_dispatched_at=now() WHERE id=%s",
                    (queue_id,),
                )
            _insert_recovery_outbox(
                connection,
                "geodata.import.processing.recovered.v1",
                "import_processing_queue",
                queue_id,
                {
                    "queueId": queue_id,
                    "natsSubject": "myota.geodata.import.process.v1",
                },
            )
            recovered["promotion"] += 1

        deletions = connection.execute(
            "SELECT id,payload FROM geodata_control_record WHERE kind=%s AND "
            "((payload->>'status'='QUEUED' AND updated_at <= "
            "now()-make_interval(secs => %s)) OR "
            "(payload->>'status'='PROCESSING' AND "
            "(NULLIF(payload->>'leaseUntil','') IS NULL OR "
            "(payload->>'leaseUntil')::timestamptz<=now()) AND updated_at <= "
            "now()-make_interval(secs => %s))) "
            "ORDER BY updated_at,id FOR UPDATE SKIP LOCKED LIMIT %s",
            ("entityDeletionJobs", age, age, WORK_RECOVERY_BATCH_SIZE),
        ).fetchall()
        for job_id, payload in deletions:
            if payload.get("status") == "PROCESSING":
                connection.execute(
                    "UPDATE geodata_control_record SET payload=payload || "
                    "jsonb_build_object('status','QUEUED','leaseUntil',NULL),"
                    "updated_at=now() WHERE kind=%s AND id=%s",
                    ("entityDeletionJobs", job_id),
                )
            else:
                connection.execute(
                    "UPDATE geodata_control_record SET updated_at=now() "
                    "WHERE kind=%s AND id=%s",
                    ("entityDeletionJobs", job_id),
                )
            _insert_recovery_outbox(
                connection,
                "geodata.entity-deletion-job.queued.v1",
                "entity_deletion_job",
                str(job_id),
                {
                    "jobId": str(job_id),
                    "natsSubject": "myota.geodata.entity.delete.v1",
                },
            )
            recovered["deletion"] += 1

        locations = connection.execute(
            "SELECT id::text,public_properties->>'locationEnrichmentRequestId',"
            "public_properties->>'locationEnrichmentGeometryHash',"
            "coalesce((public_properties->>'locationEnrichmentOnlyMissing')::boolean,false),"
            "public_properties->>'locationEnrichmentReason' "
            "FROM geodata_entity WHERE "
            "public_properties->>'locationEnrichmentStatus'='QUEUED' AND "
            "public_properties ? 'locationEnrichmentReason' AND "
            "location_work_dispatched_at <= now()-make_interval(secs => %s) "
            "ORDER BY location_work_dispatched_at,id "
            "FOR UPDATE SKIP LOCKED LIMIT %s",
            (age, WORK_RECOVERY_BATCH_SIZE),
        ).fetchall()
        for (
            entity_id,
            request_id,
            geometry_hash,
            only_missing,
            reason,
        ) in locations:
            if not request_id or not geometry_hash or not reason:
                continue
            connection.execute(
                "UPDATE geodata_entity SET location_work_dispatched_at=now() "
                "WHERE id=%s",
                (entity_id,),
            )
            _insert_recovery_outbox(
                connection,
                "geodata.entity.location-enrichment-requested.v1",
                "entity",
                entity_id,
                {
                    "entityId": entity_id,
                    "requestId": request_id,
                    "geometryHash": geometry_hash,
                    "onlyMissing": only_missing,
                    "reason": reason,
                    "natsSubject": "myota.geodata.entity.location-enrichment.v1",
                },
            )
            recovered["location"] += 1
    return recovered


async def _reconcile_stale_cancellations(stop_event: asyncio.Event) -> None:
    """Finalize cancellations left behind by a stopped or lost worker."""
    interval = max(5, CANCELLATION_RECONCILE_SECONDS)
    while not stop_event.is_set():
        try:
            run_ids = await asyncio.to_thread(_stale_cancellation_ids)
            for run_id in run_ids:
                # Refresh immediately before finalization so the worker uses
                # the authoritative relational status and source metadata.
                await asyncio.to_thread(
                    GeoHandler.store.refresh_import_run, run_id
                )
                finalized = await asyncio.to_thread(
                    GeoHandler._recover_import_run, run_id
                )
                if finalized:
                    LOG.info(
                        "finalized stale import cancellation for run %s",
                        run_id,
                    )
        except Exception:
            LOG.exception("failed to reconcile stale import cancellations")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            continue


async def _reconcile_expired_work_dispatches(
    stop_event: asyncio.Event,
) -> None:
    """Restore aged work commands through the outbox, without executing them."""
    interval = max(15, WORK_RECONCILE_SECONDS)
    while not stop_event.is_set():
        try:
            recovered = await asyncio.to_thread(
                _recover_expired_work_dispatches
            )
            if sum(recovered.values()):
                LOG.warning(
                    "re-enqueued stale work from owning rows: %s", recovered
                )
        except Exception:
            LOG.exception("failed to reconcile expired work dispatches")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            continue


async def _with_ack_heartbeat(
    message: Any, operation: Awaitable[None]
) -> None:
    async def heartbeat() -> None:
        while True:
            await asyncio.sleep(max(15, ACK_WAIT_SECONDS // 3))
            await message.in_progress()

    task = asyncio.create_task(heartbeat())
    try:
        await operation
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def _consume(
    js: Any,
    consumer: str,
    subject: str,
    handler: Callable[[dict[str, Any]], Awaitable[None]],
    stop_event: asyncio.Event,
) -> None:
    # Deployment-owned provisioning controls the durable's ACK/retry limits.
    # Binding without a config avoids client-side attempts to rewrite a shared
    # durable when work kinds intentionally have different limits.
    subscription = await js.pull_subscribe(subject, durable=consumer)
    LOG.info("consumer %s subscribed to %s", consumer, subject)
    while not stop_event.is_set():
        try:
            messages = await subscription.fetch(batch=1, timeout=5)
        except (FetchTimeoutError, NatsTimeoutError):
            continue
        for message in messages:
            event: dict[str, Any] = {}
            try:
                event = json.loads(message.data)
                event_id, event_type = _event_key(event)
                expected_type = WORK_TYPES.get(consumer)
                if expected_type and event_type != expected_type:
                    raise ValueError(
                        f"durable {consumer} received unexpected work type"
                    )
                message_id = str(
                    event.get("causationId") or event.get("workId") or event_id
                )
                if _already_processed(consumer, message_id):
                    if await asyncio.to_thread(
                        _work_still_pending, consumer, event
                    ):
                        LOG.warning(
                            "replaying work %s because its owning row remains active",
                            event_id,
                        )
                        await _with_ack_heartbeat(message, handler(event))
                    else:
                        await message.ack()
                        continue
                else:
                    await _with_ack_heartbeat(message, handler(event))
                _record_processed(consumer, event)
                await message.ack()
            except Exception as error:
                metadata = message.metadata
                max_deliveries = (
                    8
                    if consumer == "geodata-location-enrichment-v1"
                    else MAX_DELIVERIES
                )
                if metadata.num_delivered >= max_deliveries:
                    event_id = str(
                        event.get("causationId")
                        or event.get("workId")
                        or event.get("eventId")
                        or "unknown"
                    )
                    try:
                        uuid.UUID(event_id)
                    except (ValueError, AttributeError):
                        event_id = str(uuid.uuid4())
                    event_type = str(
                        event.get("workType")
                        or event.get("eventType")
                        or "unknown"
                    )
                    aggregate = event.get("aggregate") or {}
                    if not isinstance(aggregate, dict):
                        aggregate = {}
                    aggregate_id = aggregate.get("id")
                    with psycopg.connect(GeoHandler.store.dsn) as connection:
                        connection.execute(
                            "SET LOCAL myota.geodata_writer = 'row-v1'"
                        )
                        connection.execute(
                            "INSERT INTO dead_letter_event(event_id,event_type,payload,attempts,error) "
                            "VALUES (%s,%s,%s::jsonb,%s,%s) ON CONFLICT DO NOTHING",
                            (
                                event_id,
                                event_type,
                                json.dumps(event),
                                metadata.num_delivered,
                                str(error),
                            ),
                        )
                        if (
                            aggregate_id
                            and aggregate.get("type") == "import_run"
                        ):
                            connection.execute(
                                "UPDATE import_run SET status='FAILED', last_error=%s, "
                                "completed_at=now(), heartbeat_at=NULL, lease_until=NULL "
                                "WHERE id=%s AND status IN ('QUEUED','PROCESSING')",
                                (str(error), aggregate_id),
                            )
                        elif (
                            aggregate_id
                            and aggregate.get("type")
                            == "import_processing_queue"
                        ):
                            connection.execute(
                                "UPDATE geodata_import_processing_queue SET status='FAILED', error=%s, "
                                "completed_at=now(), heartbeat_at=NULL, lease_until=NULL "
                                "WHERE id=%s AND status IN ('QUEUED','PROCESSING')",
                                (str(error), aggregate_id),
                            )
                    LOG.exception(
                        "event %s exhausted delivery attempts", event_id
                    )
                    await message.term()
                else:
                    LOG.warning(
                        "event processing failed; NAK for retry: %s", error
                    )
                    await message.nak(
                        delay=min(
                            max(0, RETRY_DELAY_SECONDS)
                            * metadata.num_delivered,
                            60,
                        )
                    )
    await subscription.unsubscribe()


async def run() -> None:
    if not GeoHandler.store.durable:
        raise RuntimeError("GEO_DATABASE_URL is required for import workers")
    await asyncio.to_thread(GeoHandler.store.wait_for_authority_schema)
    GeoHandler.store.hydrate()
    nc = NATS()
    await nc.connect(
        os.environ.get("NATS_URL", "nats://nats:4222"),
        name="myota-geodata-import-worker",
        max_reconnect_attempts=-1,
        reconnect_time_wait=2,
    )
    js = nc.jetstream()
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signal_number in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signal_number, stop_event.set)
    # The deployment-owned topology provisioner is the only authority allowed
    # to mutate durable broker configuration. Remove the retired push durable
    # through a reviewed cutover after its pending-message disposition is known.

    async def preprocess(event: dict[str, Any]) -> None:
        run_id = (event.get("payload") or {}).get("importRunId") or (
            event.get("aggregate") or {}
        ).get("id")
        if run_id:
            await asyncio.to_thread(GeoHandler.store.refresh_import_runs)
            processed = await asyncio.to_thread(
                GeoHandler._recover_import_run, str(run_id)
            )
            if not processed:
                raise RuntimeError(
                    "import lease is active; defer this delivery"
                )

    async def promote(event: dict[str, Any]) -> None:
        queue_id = (event.get("payload") or {}).get("queueId") or (
            event.get("aggregate") or {}
        ).get("id")
        if queue_id:
            await asyncio.to_thread(
                GeoHandler.store.refresh_import_queue, str(queue_id)
            )
            processed = await asyncio.to_thread(
                GeoHandler._process_import_queue, str(queue_id)
            )
            if not processed:
                raise RuntimeError(
                    "promotion lease is active; defer this delivery"
                )

    async def delete_entity(event: dict[str, Any]) -> None:
        job_id = (event.get("payload") or {}).get("jobId") or (
            event.get("aggregate") or {}
        ).get("id")
        if job_id:
            processed = await asyncio.to_thread(
                GeoHandler._execute_deletion_job, str(job_id)
            )
            if not processed:
                raise RuntimeError("deletion lease is active; defer delivery")

    async def enrich_location(event: dict[str, Any]) -> None:
        payload = event.get("payload") or {}
        entity_id = payload.get("entityId") or event.get("aggregate", {}).get(
            "id"
        )
        request_id = payload.get("requestId")
        geometry_hash = payload.get("geometryHash")
        if not entity_id or not request_id or not geometry_hash:
            raise ValueError(
                "location-enrichment event is missing identifiers"
            )
        await asyncio.to_thread(
            GeoHandler._process_location_enrichment,
            str(entity_id),
            str(request_id),
            str(geometry_hash),
            bool(payload.get("onlyMissing", False)),
            str(payload.get("reason") or "UNSPECIFIED"),
        )

    tasks = [
        asyncio.create_task(
            _consume(
                js,
                "geodata-entity-deletion-v1",
                "myota.work.geodata.entity-delete.v1",
                delete_entity,
                stop_event,
            ),
            name="geodata-entity-deletion-consumer",
        ),
        asyncio.create_task(
            _consume(
                js,
                "geodata-preprocessing-v1",
                "myota.work.geodata.import-preprocess.v1",
                preprocess,
                stop_event,
            ),
            name="geodata-preprocessing-consumer",
        ),
        asyncio.create_task(
            _consume(
                js,
                "geodata-import-promotion-v1",
                "myota.work.geodata.import-promotion.v1",
                promote,
                stop_event,
            ),
            name="geodata-promotion-consumer",
        ),
        asyncio.create_task(
            _consume(
                js,
                "geodata-location-enrichment-v1",
                "myota.work.geodata.location-enrichment.v1",
                enrich_location,
                stop_event,
            ),
            name="geodata-location-enrichment-consumer",
        ),
        asyncio.create_task(
            _reconcile_stale_cancellations(stop_event),
            name="geodata-stale-cancellation-reconciler",
        ),
        asyncio.create_task(
            _reconcile_expired_work_dispatches(stop_event),
            name="geodata-expired-work-dispatch-reconciler",
        ),
    ]
    try:
        shutdown = asyncio.create_task(stop_event.wait())
        done, _ = await asyncio.wait(
            [*tasks, shutdown], return_when=asyncio.FIRST_COMPLETED
        )
        if shutdown not in done:
            for task in done:
                task.result()
            raise RuntimeError("a geodata JetStream consumer exited")
        LOG.info("shutdown requested; draining active geodata messages")
        await asyncio.gather(*tasks)
    finally:
        await nc.drain()


if __name__ == "__main__":
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        sys.exit(0)
