from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from jetstream_observability import collect_stream_metrics
from jetstream_observability import JetStreamMetricsPoller
from metrics import METRICS


class FakeJetStream:
    def __init__(self, consumers):
        self.consumers = consumers

    async def consumers_info(self, _stream):
        return self.consumers


class FakeConnection:
    def __init__(self, timestamps, subjects=None):
        self.timestamps = timestamps
        self.subjects = subjects or {}

    async def request(self, _subject, data, timeout):
        sequence = json.loads(data)["seq"]
        message = {"time": self.timestamps[sequence]}
        if sequence in self.subjects:
            message["subject"] = self.subjects[sequence]
        return SimpleNamespace(data=json.dumps({"message": message}).encode())


class JetStreamMetricsTests(unittest.IsolatedAsyncioTestCase):
    async def test_reports_backlog_redelivery_and_oldest_pending_age_from_jetstream(
        self,
    ):
        now = datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc)
        consumer = {
            "name": "geo-import-worker",
            "num_pending": 7,
            "num_ack_pending": 2,
            "num_redelivered": 3,
            "ack_floor": {"stream_seq": 41},
            "delivered": {"stream_seq": 49},
        }
        nc = FakeConnection({42: (now - timedelta(seconds=73)).isoformat()})

        values = await collect_stream_metrics(
            FakeJetStream([consumer]), nc, "MYOTA_EVENTS", now
        )

        self.assertEqual(
            values[
                (
                    "myota_jetstream_consumer_pending",
                    "MYOTA_EVENTS",
                    "geo-import-worker",
                )
            ],
            7,
        )
        self.assertEqual(
            values[
                (
                    "myota_jetstream_consumer_ack_pending",
                    "MYOTA_EVENTS",
                    "geo-import-worker",
                )
            ],
            2,
        )
        self.assertEqual(
            values[
                (
                    "myota_jetstream_consumer_redeliveries",
                    "MYOTA_EVENTS",
                    "geo-import-worker",
                )
            ],
            3,
        )
        self.assertEqual(
            values[
                (
                    "myota_jetstream_consumer_oldest_message_age_seconds",
                    "MYOTA_EVENTS",
                    "geo-import-worker",
                )
            ],
            73,
        )
        self.assertEqual(
            values[
                (
                    "myota_jetstream_consumer_oldest_message_age_available",
                    "MYOTA_EVENTS",
                    "geo-import-worker",
                )
            ],
            1,
        )

    def test_failed_stream_poll_does_not_turn_last_known_backlog_into_zero(
        self,
    ):
        name = "myota_jetstream_consumer_pending"
        labels = {"stream": "MYOTA_EVENTS", "consumer": "geo-import-worker"}
        METRICS.set_gauge(name, 7, labels)
        poller = JetStreamMetricsPoller()
        poller._previous.add((name, "MYOTA_EVENTS", "geo-import-worker"))

        poller._publish({}, set())

        rendered = METRICS.render()
        self.assertIn(
            'myota_jetstream_consumer_pending{consumer="geo-import-worker",stream="MYOTA_EVENTS"} 7',
            rendered,
        )
        self.assertIn("myota_jetstream_metrics_up 0", rendered)

    def test_successful_poll_with_removed_consumer_sets_backlog_to_zero(self):
        name = "myota_jetstream_consumer_pending"
        labels = {
            "stream": "MYOTA_EVENTS",
            "consumer": "retired-consumer-test",
        }
        METRICS.set_gauge(name, 7, labels)
        poller = JetStreamMetricsPoller()
        poller._previous.add((name, "MYOTA_EVENTS", "retired-consumer-test"))

        poller._publish({}, {"MYOTA_EVENTS"})

        rendered = METRICS.render()
        self.assertIn(
            'myota_jetstream_consumer_pending{consumer="retired-consumer-test",stream="MYOTA_EVENTS"} 0',
            rendered,
        )
        self.assertIn("myota_jetstream_metrics_up 1", rendered)

    async def test_marks_empty_consumer_age_as_available_without_message_lookup(
        self,
    ):
        values = await collect_stream_metrics(
            FakeJetStream(
                [
                    {
                        "name": "notifications",
                        "num_pending": 0,
                        "num_ack_pending": 0,
                        "num_redelivered": 0,
                    }
                ]
            ),
            FakeConnection({}),
            "MYOTA_EVENTS",
        )

        self.assertEqual(
            values[
                (
                    "myota_jetstream_consumer_oldest_message_age_seconds",
                    "MYOTA_EVENTS",
                    "notifications",
                )
            ],
            0,
        )
        self.assertEqual(
            values[
                (
                    "myota_jetstream_consumer_oldest_message_age_available",
                    "MYOTA_EVENTS",
                    "notifications",
                )
            ],
            1,
        )

    async def test_age_is_unavailable_when_message_was_purged_between_queries(
        self,
    ):
        class MissingMessage(FakeConnection):
            async def request(self, *_args, **_kwargs):
                raise RuntimeError("message no longer retained")

        values = await collect_stream_metrics(
            FakeJetStream(
                [
                    {
                        "name": "imports",
                        "num_pending": 1,
                        "num_ack_pending": 0,
                        "num_redelivered": 0,
                        "ack_floor": {"stream_seq": 4},
                        "delivered": {"stream_seq": 6},
                    }
                ]
            ),
            MissingMessage({}),
            "MYOTA_EVENTS",
        )

        self.assertEqual(
            values[
                (
                    "myota_jetstream_consumer_oldest_message_age_available",
                    "MYOTA_EVENTS",
                    "imports",
                )
            ],
            0,
        )

    async def test_filtered_consumer_does_not_report_age_for_an_unmatched_stream_sequence(
        self,
    ):
        now = datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc)
        values = await collect_stream_metrics(
            FakeJetStream(
                [
                    {
                        "name": "geodata-imports",
                        "config": {
                            "filter_subject": "myota.geodata.imports.>"
                        },
                        "num_pending": 1,
                        "num_ack_pending": 0,
                        "ack_floor": {"stream_seq": 4},
                        "delivered": {"stream_seq": 6},
                    }
                ]
            ),
            FakeConnection(
                {7: (now - timedelta(seconds=30)).isoformat()},
                {7: "myota.activity.qso.created"},
            ),
            "MYOTA_EVENTS",
            now,
        )

        self.assertEqual(
            values[
                (
                    "myota_jetstream_consumer_oldest_message_age_available",
                    "MYOTA_EVENTS",
                    "geodata-imports",
                )
            ],
            0,
        )


if __name__ == "__main__":
    unittest.main()
