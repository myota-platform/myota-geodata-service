from __future__ import annotations

import unittest

from otel import Telemetry


class Instrument:
    def __init__(self):
        self.calls = []

    def add(self, value, attributes=None):
        self.calls.append((value, attributes))

    def record(self, value, attributes=None):
        self.calls.append((value, attributes))


class Meter:
    def __init__(self):
        self.instruments = {}

    def create_up_down_counter(self, name, **_kwargs):
        return self.instruments.setdefault(name, Instrument())

    def create_counter(self, name, **_kwargs):
        return self.instruments.setdefault(name, Instrument())

    def create_histogram(self, name, **_kwargs):
        return self.instruments.setdefault(name, Instrument())


class OTelMetricTests(unittest.TestCase):
    def test_request_metrics_balance_active_count_and_record_request_bytes(self):
        meter = Meter()
        telemetry = Telemetry("myota-geodata", meter=meter)

        request = telemetry.start_request("POST", "/v1/geodata/imports/upload", 4096)
        request.finish(202, "/v1/geodata/imports/upload")
        request.finish(202, "/v1/geodata/imports/upload")

        active = meter.instruments["myota.http.server.active_requests"]
        self.assertEqual([call[0] for call in active.calls], [1, -1])
        body_size = meter.instruments["myota.http.server.request.body.size"]
        self.assertEqual([call[0] for call in body_size.calls], [4096])
        self.assertEqual(body_size.calls[0][1]["http.response.status_code"], 202)


if __name__ == "__main__":
    unittest.main()
