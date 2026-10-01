"""Best-effort OpenTelemetry HTTP telemetry for the geodata service."""
from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from typing import Any


class _NoopInstrument:
    def add(self, *_: Any, **__: Any) -> None: return
    def record(self, *_: Any, **__: Any) -> None: return


@dataclass
class Request:
    telemetry: "Telemetry"
    method: str
    path: str
    started: float
    span: Any = None
    finished: bool = False

    def finish(self, status: int, route: str | None = None) -> None:
        if self.finished: return
        self.finished = True
        attrs = {"http.request.method": self.method, "http.route": route or self.path, "http.response.status_code": status}
        try:
            self.telemetry.requests.add(1, attrs)
            self.telemetry.duration.record((time.perf_counter() - self.started) * 1000, attrs)
            if self.span:
                for key, value in {"http.request.method": self.method, "url.path": self.path, "http.route": route or self.path, "http.response.status_code": status}.items():
                    self.span.set_attribute(key, value)
                self.span.end()
        except Exception: return


class Telemetry:
    def __init__(self, service_name: str, tracer: Any = None, meter: Any = None) -> None:
        self.tracer = tracer
        self.requests = meter.create_counter("myota.http.server.requests", unit="{request}") if meter else _NoopInstrument()
        self.duration = meter.create_histogram("myota.http.server.duration", unit="ms") if meter else _NoopInstrument()

    def start_request(self, method: str, path: str) -> Request:
        span = None
        try:
            if self.tracer: span = self.tracer.start_span(f"{method} {path}")
        except Exception: pass
        return Request(self, method, path, time.perf_counter(), span)


_lock = threading.Lock()
_instances: dict[str, Telemetry] = {}


def telemetry_for(service_name: str) -> Telemetry:
    with _lock:
        if service_name in _instances: return _instances[service_name]
        if os.environ.get("MYOTA_OTEL_ENABLED", "0").strip().lower() in {"", "0", "false", "no", "off"}:
            value = Telemetry(service_name)
        else:
            try:
                from opentelemetry import metrics, trace
                from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
                from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
                from opentelemetry.sdk.metrics import MeterProvider
                from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
                from opentelemetry.sdk.resources import Resource
                from opentelemetry.sdk.trace import TracerProvider
                from opentelemetry.sdk.trace.export import BatchSpanProcessor
                endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
                resource = Resource.create({"service.name": service_name, "service.namespace": "myota", "deployment.environment": os.environ.get("MYOTA_ENV", "development")})
                provider = TracerProvider(resource=resource)
                provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=not endpoint.startswith("https://"))))
                trace.set_tracer_provider(provider)
                reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=endpoint, insecure=not endpoint.startswith("https://")), export_interval_millis=int(os.environ.get("MYOTA_OTEL_METRIC_INTERVAL_MS", "15000")))
                metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=[reader]))
                value = Telemetry(service_name, trace.get_tracer("myota.http"), metrics.get_meter("myota.http"))
            except Exception:
                value = Telemetry(service_name)
        _instances[service_name] = value
        return value
