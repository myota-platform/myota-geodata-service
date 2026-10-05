"""Low-cardinality, read-only JetStream consumer backlog metrics."""
from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any

from metrics import METRICS

METRIC_NAMES = (
    "myota_jetstream_consumer_pending",
    "myota_jetstream_consumer_ack_pending",
    "myota_jetstream_consumer_redeliveries",
    "myota_jetstream_consumer_oldest_message_age_seconds",
    "myota_jetstream_consumer_oldest_message_age_available",
)


def _value(value: Any, name: str, default: Any = 0) -> Any:
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def _subject_matches(subject: str, pattern: str) -> bool:
    subject_parts, pattern_parts = subject.split("."), pattern.split(".")
    index = 0
    for part in pattern_parts:
        if part == ">":
            return index < len(subject_parts)
        if index >= len(subject_parts) or (part != "*" and part != subject_parts[index]):
            return False
        index += 1
    return index == len(subject_parts)


async def collect_stream_metrics(js: Any, nc: Any, stream: str,
                                now: datetime | None = None) -> dict[tuple[str, str, str], float]:
    """Fetch consumer state without consuming messages or changing acknowledgements."""
    current = now or datetime.now(timezone.utc)
    infos = await js.consumers_info(stream)
    maximum = max(1, int(os.environ.get("MYOTA_JETSTREAM_METRICS_MAX_CONSUMERS", "100")))
    result: dict[tuple[str, str, str], float] = {}
    for info in infos[:maximum]:
        consumer = str(_value(info, "name", "unknown"))
        labels = {"stream": stream, "consumer": consumer}
        pending = int(_value(info, "num_pending", 0) or 0)
        ack_pending = int(_value(info, "num_ack_pending", 0) or 0)
        result[(METRIC_NAMES[0], stream, consumer)] = float(pending)
        result[(METRIC_NAMES[1], stream, consumer)] = float(ack_pending)
        result[(METRIC_NAMES[2], stream, consumer)] = float(_value(info, "num_redelivered", 0) or 0)

        age = 0.0
        age_available = 1.0 if pending == 0 and ack_pending == 0 else 0.0
        if pending or ack_pending:
            floor = _value(info, "ack_floor", {})
            delivered = _value(info, "delivered", {})
            sequence = int(_value(floor, "stream_seq", 0) or 0) + 1 if ack_pending else int(
                _value(delivered, "stream_seq", 0) or 0) + 1
            try:
                response = await nc.request(f"$JS.API.STREAM.MSG.GET.{stream}",
                                            json.dumps({"seq": sequence}).encode(), timeout=2)
                stored = json.loads(response.data.decode("utf-8")).get("message") or {}
                config = _value(info, "config", {})
                filters = _value(config, "filter_subjects", []) or []
                single_filter = _value(config, "filter_subject", "")
                if single_filter:
                    filters = [*filters, single_filter]
                stored_subject = stored.get("subject")
                if filters and (not stored_subject or not any(_subject_matches(stored_subject, pattern) for pattern in filters)):
                    raise ValueError("oldest stream sequence does not match the consumer filter")
                timestamp = stored.get("time")
                if isinstance(timestamp, str):
                    timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                if isinstance(timestamp, datetime):
                    timestamp = timestamp.replace(tzinfo=timezone.utc) if timestamp.tzinfo is None else timestamp
                    age = max(0.0, (current - timestamp).total_seconds())
                    age_available = 1.0
            except Exception:
                # Retention/purge may remove a sequence between the info and
                # message lookup. Mark age unavailable rather than inventing it.
                pass
        result[(METRIC_NAMES[3], stream, consumer)] = age
        result[(METRIC_NAMES[4], stream, consumer)] = age_available
    return result


class JetStreamMetricsPoller:
    def __init__(self) -> None:
        self.streams = tuple(filter(None, (part.strip() for part in os.environ.get(
            "MYOTA_JETSTREAM_METRICS_STREAMS", "MYOTA_EVENTS").split(","))))
        self.url = os.environ.get("NATS_URL", "nats://nats:4222")
        self.interval = max(5, int(os.environ.get("MYOTA_JETSTREAM_METRICS_INTERVAL_SECONDS", "15")))
        self._previous: set[tuple[str, str, str]] = set()
        self._lock = threading.Lock()

    def start(self) -> None:
        thread = threading.Thread(target=self._run, name="jetstream-metrics", daemon=True)
        thread.start()

    def _publish(self, samples: dict[tuple[str, str, str], float], successful_streams: set[str]) -> None:
        with self._lock:
            for key in self._previous - samples.keys():
                if key[1] not in successful_streams:
                    continue
                METRICS.set_gauge(key[0], 0, {"stream": key[1], "consumer": key[2]})
                METRICS.set_gauge("myota_jetstream_consumer_oldest_message_age_available", 0,
                                  {"stream": key[1], "consumer": key[2]})
            for (name, stream, consumer), value in samples.items():
                METRICS.set_gauge(name, value, {"stream": stream, "consumer": consumer})
            failed_streams = set(self.streams) - successful_streams
            self._previous = set(samples) | {key for key in self._previous if key[1] in failed_streams}
            poll_ok = bool(self.streams) and not failed_streams
            METRICS.set_gauge("myota_jetstream_metrics_up", 1 if poll_ok else 0)

    def _run(self) -> None:
        asyncio.run(self._poll_forever())

    async def _poll_forever(self) -> None:
        from nats.aio.client import Client as NATS

        while True:
            started = time.monotonic()
            samples: dict[tuple[str, str, str], float] = {}
            successful_streams: set[str] = set()
            try:
                nc = NATS()
                await nc.connect(self.url, name="myota-geodata-metrics", connect_timeout=3,
                                 max_reconnect_attempts=0)
                js = nc.jetstream()
                for stream in self.streams:
                    try:
                        samples.update(await collect_stream_metrics(js, nc, stream))
                        successful_streams.add(stream)
                    except Exception:
                        # An unavailable stream is not a zero backlog. Preserve
                        # its last known sample and report the poll unhealthy.
                        continue
                await nc.drain()
                self._publish(samples, successful_streams)
            except Exception:
                METRICS.set_gauge("myota_jetstream_metrics_up", 0)
            await asyncio.sleep(max(0.1, self.interval - (time.monotonic() - started)))


def start_jetstream_metrics() -> None:
    JetStreamMetricsPoller().start()
