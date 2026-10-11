"""Best-effort OpenTelemetry HTTP telemetry for the geodata service."""

from __future__ import annotations

import os
import resource
import threading
import time
from dataclasses import dataclass
from typing import Any


class _NoopInstrument:
    def add(self, *_: Any, **__: Any) -> None:
        return

    def record(self, *_: Any, **__: Any) -> None:
        return


@dataclass
class Request:
    telemetry: "Telemetry"
    method: str
    path: str
    started: float
    request_body_size: int = 0
    span: Any = None
    finished: bool = False

    def finish(self, status: int, route: str | None = None) -> None:
        if self.finished:
            return
        self.finished = True
        attrs = {
            "http.request.method": self.method,
            "http.route": route or self.path,
            "http.response.status_code": status,
        }
        try:
            self.telemetry.requests.add(1, attrs)
            self.telemetry.duration.record(
                (time.perf_counter() - self.started) * 1000, attrs
            )
            self.telemetry.active_requests.add(
                -1, {"http.request.method": self.method}
            )
            if self.request_body_size:
                self.telemetry.request_body_size.record(
                    self.request_body_size, attrs
                )
            if self.span:
                for key, value in {
                    "http.request.method": self.method,
                    "url.path": self.path,
                    "http.route": route or self.path,
                    "http.response.status_code": status,
                }.items():
                    self.span.set_attribute(key, value)
                self.span.end()
        except Exception:
            return


class Telemetry:
    def __init__(
        self, service_name: str, tracer: Any = None, meter: Any = None
    ) -> None:
        self.tracer = tracer
        self.active_requests = (
            meter.create_up_down_counter(
                "myota.http.server.active_requests", unit="{request}"
            )
            if meter
            else _NoopInstrument()
        )
        self.requests = (
            meter.create_counter(
                "myota.http.server.requests", unit="{request}"
            )
            if meter
            else _NoopInstrument()
        )
        self.duration = (
            meter.create_histogram("myota.http.server.duration", unit="ms")
            if meter
            else _NoopInstrument()
        )
        self.request_body_size = (
            meter.create_histogram(
                "myota.http.server.request.body.size", unit="By"
            )
            if meter
            else _NoopInstrument()
        )

    def start_request(
        self, method: str, path: str, request_body_size: int = 0,
        create_span: bool = True,
    ) -> Request:
        span = None
        try:
            if self.tracer and create_span:
                span = self.tracer.start_span(f"{method} {path}")
        except Exception:
            pass
        attrs = {"http.request.method": method}
        self.active_requests.add(1, attrs)
        return Request(
            self, method, path, time.perf_counter(), request_body_size, span
        )


def _process_memory_bytes() -> int:
    """Return current RSS in Linux containers, with a portable high-water fallback."""
    try:
        with open("/proc/self/statm", encoding="ascii") as statm:
            resident_pages = int(statm.read().split()[1])
        return resident_pages * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError):
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # macOS reports bytes; Linux reports KiB.
        return int(usage if os.uname().sysname == "Darwin" else usage * 1024)


_lock = threading.Lock()
_instances: dict[str, Telemetry] = {}


def telemetry_for(service_name: str) -> Telemetry:
    with _lock:
        if service_name in _instances:
            return _instances[service_name]
        if os.environ.get("MYOTA_OTEL_ENABLED", "0").strip().lower() in {
            "",
            "0",
            "false",
            "no",
            "off",
        }:
            value = Telemetry(service_name)
        else:
            try:
                from opentelemetry import metrics, trace
                from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
                    OTLPMetricExporter,
                )
                from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                    OTLPSpanExporter,
                )
                from opentelemetry.metrics import Observation
                from opentelemetry.sdk.metrics import MeterProvider
                from opentelemetry.sdk.metrics.export import (
                    PeriodicExportingMetricReader,
                )
                from opentelemetry.sdk.resources import Resource
                from opentelemetry.sdk.trace import TracerProvider
                from opentelemetry.sdk.trace.export import BatchSpanProcessor

                endpoint = os.environ.get(
                    "OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317"
                )
                resolved_service_name = os.environ.get(
                    "OTEL_SERVICE_NAME", service_name
                )
                resource_attributes = {
                    "service.name": resolved_service_name,
                    "service.namespace": "myota",
                    "deployment.environment": os.environ.get(
                        "MYOTA_ENV", "development"
                    ),
                    "service.instance.id": os.environ.get("HOSTNAME", "local"),
                }
                otel_resource = Resource.create(resource_attributes)
                provider = TracerProvider(resource=otel_resource)
                provider.add_span_processor(
                    BatchSpanProcessor(
                        OTLPSpanExporter(
                            endpoint=endpoint,
                            insecure=not endpoint.startswith("https://"),
                        )
                    )
                )
                trace.set_tracer_provider(provider)
                reader = PeriodicExportingMetricReader(
                    OTLPMetricExporter(
                        endpoint=endpoint,
                        insecure=not endpoint.startswith("https://"),
                    ),
                    export_interval_millis=int(
                        os.environ.get(
                            "MYOTA_OTEL_METRIC_INTERVAL_MS", "15000"
                        )
                    ),
                )
                metrics.set_meter_provider(
                    MeterProvider(
                        resource=otel_resource, metric_readers=[reader]
                    )
                )
                meter = metrics.get_meter("myota.http")
                meter.create_observable_gauge(
                    "myota.process.memory.usage",
                    callbacks=[
                        lambda _options: [Observation(_process_memory_bytes())]
                    ],
                    unit="By",
                    description="Current resident memory used by this service process",
                )
                meter.create_observable_gauge(
                    "myota.process.cpu.time",
                    callbacks=[
                        lambda _options: [
                            Observation(
                                resource.getrusage(
                                    resource.RUSAGE_SELF
                                ).ru_utime
                                + resource.getrusage(
                                    resource.RUSAGE_SELF
                                ).ru_stime
                            )
                        ]
                    ],
                    unit="s",
                    description="Cumulative user and system CPU time for this process",
                )
                value = Telemetry(
                    resolved_service_name,
                    trace.get_tracer("myota.http"),
                    meter,
                )
            except Exception:
                value = Telemetry(service_name)
        _instances[service_name] = value
        return value
