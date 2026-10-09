"""JetStream ACK/retry checks against an isolated disposable broker."""

from __future__ import annotations

import asyncio
import json
import os
import unittest
import uuid
from unittest.mock import patch

from nats.aio.msg import Msg
from nats.js.api import StorageType, StreamConfig

from geodata import GeoHandler
from geodata_import_worker import _consume
from geodata_store import GeodataStore


DATABASE_URL = os.environ.get("GEO_TEST_DATABASE_URL")
NATS_URL = os.environ.get("MYOTA_NATS_TEST_URL")


@unittest.skipUnless(
    DATABASE_URL and NATS_URL,
    "isolated PostGIS and JetStream services are required",
)
class JetStreamWorkerDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        from nats.aio.client import Client as NATS

        self.store = GeodataStore()
        self.store.dsn = str(DATABASE_URL)
        self.store.hydrate()
        self.store_patch = patch.object(GeoHandler, "store", self.store)
        self.store_patch.start()

        self.nc = NATS()
        await self.nc.connect(str(NATS_URL), name="myota-geodata-test")
        self.js = self.nc.jetstream()
        suffix = uuid.uuid4().hex[:16]
        self.stream = f"MYOTA_TEST_{suffix.upper()}"
        self.subject = f"myota.geodata.test.{suffix}"
        self.consumer = f"geodata-test-{suffix}"
        await self.js.add_stream(
            config=StreamConfig(
                name=self.stream,
                subjects=[self.subject],
                storage=StorageType.MEMORY,
            )
        )
        self.stop_event = asyncio.Event()
        self.tasks: list[asyncio.Task[None]] = []

    async def asyncTearDown(self) -> None:
        self.stop_event.set()
        for task in self.tasks:
            if not task.done():
                task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        if self.nc.is_connected:
            try:
                await self.js.delete_stream(self.stream)
            finally:
                await self.nc.drain()
        with self.store.transaction() as connection:
            connection.execute(
                "DELETE FROM consumer_processed_event WHERE consumer=%s",
                (self.consumer,),
            )
            connection.execute(
                "DELETE FROM consumer_checkpoint WHERE consumer=%s",
                (self.consumer,),
            )
        self.store.close()
        self.store_patch.stop()

    def _event(self) -> dict[str, str]:
        return {
            "eventId": str(uuid.uuid4()),
            "eventType": "geodata.test.delivery.v1",
        }

    async def _start_consumer(self, handler) -> None:
        self.tasks.append(
            asyncio.create_task(
                _consume(
                    self.js,
                    self.consumer,
                    self.subject,
                    handler,
                    self.stop_event,
                )
            )
        )
        await asyncio.wait_for(
            self.js.consumer_info(self.stream, self.consumer), timeout=10
        )

    async def test_ack_pending_clears_after_handler_success(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        acked = asyncio.Event()

        async def handler(_event: dict[str, str]) -> None:
            started.set()
            await release.wait()

        original_ack = Msg.ack

        async def tracked_ack(message: Msg, *args, **kwargs):
            result = await original_ack(message, *args, **kwargs)
            acked.set()
            return result

        with patch.object(Msg, "ack", tracked_ack):
            await self._start_consumer(handler)
            await self.js.publish(
                self.subject, json.dumps(self._event()).encode()
            )
            await asyncio.wait_for(started.wait(), timeout=10)

            pending = await self.js.consumer_info(self.stream, self.consumer)
            self.assertEqual(pending.num_ack_pending, 1)

            release.set()
            await asyncio.wait_for(acked.wait(), timeout=10)

        settled = await self.js.consumer_info(self.stream, self.consumer)
        self.assertEqual(settled.num_ack_pending, 0)
        self.assertEqual(settled.num_pending, 0)

    async def test_redelivery_after_commit_before_ack_is_idempotent(
        self,
    ) -> None:
        handler_calls = 0
        acked = asyncio.Event()
        ack_calls = 0
        original_ack = Msg.ack

        async def handler(_event: dict[str, str]) -> None:
            nonlocal handler_calls
            handler_calls += 1

        async def lose_first_ack(message: Msg, *args, **kwargs):
            nonlocal ack_calls
            ack_calls += 1
            if ack_calls == 1:
                raise RuntimeError("simulated lost ACK after durable commit")
            result = await original_ack(message, *args, **kwargs)
            acked.set()
            return result

        with patch.object(Msg, "ack", lose_first_ack):
            with patch("geodata_import_worker.RETRY_DELAY_SECONDS", 0):
                await self._start_consumer(handler)
                await self.js.publish(
                    self.subject, json.dumps(self._event()).encode()
                )
                await asyncio.wait_for(acked.wait(), timeout=10)

        self.assertEqual(handler_calls, 1)
        self.assertEqual(ack_calls, 2)
        info = await self.js.consumer_info(self.stream, self.consumer)
        self.assertEqual(info.num_ack_pending, 0)
        self.assertEqual(info.num_pending, 0)
        with self.store.transaction() as connection:
            processed = connection.execute(
                "SELECT count(*) FROM consumer_processed_event WHERE consumer=%s",
                (self.consumer,),
            ).fetchone()[0]
        self.assertEqual(processed, 1)

    async def test_competing_pull_consumers_process_each_event_once(
        self,
    ) -> None:
        handled = 0
        all_handled = asyncio.Event()

        async def handler(_event: dict[str, str]) -> None:
            nonlocal handled
            handled += 1
            if handled == 8:
                all_handled.set()

        await self._start_consumer(handler)
        await self._start_consumer(handler)
        for _ in range(8):
            await self.js.publish(
                self.subject, json.dumps(self._event()).encode()
            )

        await asyncio.wait_for(all_handled.wait(), timeout=15)
        for _ in range(40):
            info = await self.js.consumer_info(self.stream, self.consumer)
            if info.num_ack_pending == 0 and info.num_pending == 0:
                break
            await asyncio.sleep(0.05)

        self.assertEqual(handled, 8)
        self.assertEqual(info.num_ack_pending, 0)
        self.assertEqual(info.num_pending, 0)
        with self.store.transaction() as connection:
            processed = connection.execute(
                "SELECT count(*) FROM consumer_processed_event WHERE consumer=%s",
                (self.consumer,),
            ).fetchone()[0]
        self.assertEqual(processed, 8)


if __name__ == "__main__":
    unittest.main()
