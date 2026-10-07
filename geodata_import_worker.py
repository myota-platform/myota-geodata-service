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
from nats.js.api import AckPolicy, ConsumerConfig
from nats.js.errors import FetchTimeoutError

from geodata import GeoHandler

LOG = logging.getLogger("geodata.import.worker")
MAX_DELIVERIES = int(os.environ.get("GEODATA_WORKER_MAX_DELIVERIES", "100"))
ACK_WAIT_SECONDS = int(
    os.environ.get("GEODATA_WORKER_ACK_WAIT_SECONDS", "120")
)
MAX_ACK_PENDING = int(os.environ.get("GEODATA_WORKER_MAX_ACK_PENDING", "1"))


def _event_key(event: dict[str, Any]) -> tuple[str, str]:
    event_id = str(event.get("eventId") or "")
    event_type = str(event.get("eventType") or "")
    if not event_id or not event_type.endswith(".v1"):
        raise ValueError("unsupported or malformed geodata event envelope")
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
    with GeoHandler.store.transaction() as connection:
        connection.execute(
            "INSERT INTO consumer_processed_event(consumer,event_id) "
            "VALUES (%s,%s) ON CONFLICT DO NOTHING",
            (consumer, event["eventId"]),
        )
        connection.execute(
            "INSERT INTO consumer_checkpoint(consumer,last_event_id) "
            "VALUES (%s,%s) ON CONFLICT (consumer) DO UPDATE "
            "SET last_event_id=EXCLUDED.last_event_id, updated_at=now()",
            (consumer, event["eventId"]),
        )


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
    subscription = await js.pull_subscribe(
        subject,
        durable=consumer,
        config=ConsumerConfig(
            durable_name=consumer,
            filter_subject=subject,
            ack_policy=AckPolicy.EXPLICIT,
            ack_wait=ACK_WAIT_SECONDS,
            max_deliver=MAX_DELIVERIES,
            max_ack_pending=MAX_ACK_PENDING,
        ),
    )
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
                event_id, _ = _event_key(event)
                if _already_processed(consumer, event_id):
                    await message.ack()
                    continue
                await _with_ack_heartbeat(message, handler(event))
                _record_processed(consumer, event)
                await message.ack()
            except Exception as error:
                metadata = message.metadata
                if metadata.num_delivered >= MAX_DELIVERIES:
                    event_id = str(event.get("eventId", "unknown"))
                    try:
                        uuid.UUID(event_id)
                    except (ValueError, AttributeError):
                        event_id = str(uuid.uuid4())
                    event_type = str(event.get("eventType", "unknown"))
                    aggregate = event.get("aggregate") or {}
                    if not isinstance(aggregate, dict):
                        aggregate = {}
                    aggregate_id = aggregate.get("id")
                    with psycopg.connect(GeoHandler.store.dsn) as connection:
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
                        delay=min(5 * metadata.num_delivered, 60)
                    )
    await subscription.unsubscribe()


async def run() -> None:
    if not GeoHandler.store.durable:
        raise RuntimeError("GEO_DATABASE_URL is required for import workers")
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
    try:
        await js.delete_consumer("MYOTA_EVENTS", "geodata-import-processing")
        LOG.info("removed the retired push consumer geodata-import-processing")
    except Exception:
        # Fresh installs have no legacy push consumer to remove.
        pass

    async def preprocess(event: dict[str, Any]) -> None:
        run_id = event.get("aggregate", {}).get("id") or (
            event.get("payload") or {}
        ).get("importRunId")
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
        queue_id = event.get("aggregate", {}).get("id") or (
            event.get("payload") or {}
        ).get("queueId")
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

    tasks = [
        asyncio.create_task(
            _consume(
                js,
                "geodata-preprocessing-v1",
                "myota.geodata.import.preprocess.v1",
                preprocess,
                stop_event,
            ),
            name="geodata-preprocessing-consumer",
        ),
        asyncio.create_task(
            _consume(
                js,
                "geodata-import-processing-v2",
                "myota.geodata.import.process.v1",
                promote,
                stop_event,
            ),
            name="geodata-promotion-consumer",
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
