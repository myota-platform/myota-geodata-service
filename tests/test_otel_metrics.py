from __future__ import annotations

import os
import subprocess
import sys
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

import otel
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
    def tearDown(self):
        otel._process_telemetry = None

    def test_request_metrics_balance_active_count_and_record_request_bytes(
        self,
    ):
        meter = Meter()
        telemetry = Telemetry("myota-geodata", meter=meter)

        request = telemetry.start_request(
            "POST", "/v1/geodata/imports/upload", 4096
        )
        request.finish(202, "/v1/geodata/imports/upload")
        request.finish(202, "/v1/geodata/imports/upload")

        active = meter.instruments["myota.http.server.active_requests"]
        self.assertEqual([call[0] for call in active.calls], [1, -1])
        body_size = meter.instruments["myota.http.server.request.body.size"]
        self.assertEqual([call[0] for call in body_size.calls], [4096])
        self.assertEqual(
            body_size.calls[0][1]["http.response.status_code"], 202
        )

    def test_disabled_telemetry_is_cached_once_for_the_process(self):
        with patch.dict(os.environ, {"MYOTA_OTEL_ENABLED": "0"}):
            first = otel.telemetry_for("geodata-service")
            second = otel.telemetry_for("another-service-name")

        self.assertIs(first, second)
        self.assertIsInstance(first.requests, otel._NoopInstrument)

    def test_concurrent_access_returns_one_process_instance(self):
        with patch.dict(os.environ, {"MYOTA_OTEL_ENABLED": "0"}):
            with ThreadPoolExecutor(max_workers=12) as pool:
                values = list(
                    pool.map(
                        otel.telemetry_for,
                        [f"service-{index}" for index in range(48)],
                    )
                )

        self.assertTrue(all(value is values[0] for value in values))

    def test_metrics_exporter_setup_failure_is_logged_and_nonfatal(self):
        exporter_module = ModuleType(
            "opentelemetry.exporter.otlp.proto.grpc.metric_exporter"
        )

        class FailingExporter:
            def __init__(self, **_kwargs):
                raise RuntimeError("exporter setup failed")

        exporter_module.OTLPMetricExporter = FailingExporter
        with (
            patch.dict(os.environ, {"MYOTA_OTEL_ENABLED": "1"}),
            patch.dict(
                sys.modules,
                {
                    "opentelemetry.exporter.otlp.proto.grpc.metric_exporter": exporter_module
                },
            ),
            self.assertLogs("otel", level="WARNING") as captured,
        ):
            telemetry = otel.telemetry_for("geodata-service")

        self.assertIsInstance(telemetry.requests, otel._NoopInstrument)
        self.assertIn(
            "OpenTelemetry metrics setup failed (RuntimeError)",
            captured.output[0],
        )

    def test_tracing_and_metrics_share_one_process_provider(self):
        script = """
import logging
from opentelemetry import trace

calls = []
original_set_provider = trace.set_tracer_provider
def count_provider_registration(provider):
    calls.append(provider)
    return original_set_provider(provider)
trace.set_tracer_provider = count_provider_registration

from myota_logging import configure_tracing
from otel import telemetry_for
configure_tracing("myota-geodata")
first = telemetry_for("geodata-service")
second = telemetry_for("another-service-name")
assert first is second
assert len(calls) == 1, len(calls)
assert trace.get_tracer_provider() is calls[0]
"""
        env = os.environ.copy()
        env["MYOTA_OTEL_ENABLED"] = "1"
        env["MYOTA_OTEL_METRIC_INTERVAL_MS"] = "60000"
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=Path(__file__).parents[1],
            env=env,
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn(
            "Overriding of current TracerProvider is not allowed",
            result.stderr,
        )

    def test_tracing_exporter_setup_failure_is_reported_and_nonfatal(self):
        script = """
from opentelemetry.exporter.otlp.proto.grpc import trace_exporter

class FailingExporter:
    def __init__(self, **_kwargs):
        raise RuntimeError("exporter setup failed")

trace_exporter.OTLPSpanExporter = FailingExporter
from myota_logging import configure_tracing
from otel import telemetry_for
configure_tracing("myota-geodata")
assert telemetry_for("geodata-service") is not None
"""
        env = os.environ.copy()
        env["MYOTA_OTEL_ENABLED"] = "1"
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=Path(__file__).parents[1],
            env=env,
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(
            "OpenTelemetry tracing setup failed (RuntimeError)", result.stderr
        )


if __name__ == "__main__":
    unittest.main()
