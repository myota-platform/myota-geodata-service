"""Privacy-aware structured logging shared by MyOTA service processes."""

from __future__ import annotations

import contextvars
import json
import logging
import os
import re
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

_context: contextvars.ContextVar[dict[str, str]] = contextvars.ContextVar(
    "myota_log_context", default={}
)
_CONFIGURED = False
_SENSITIVE_KEY = re.compile(
    r"(authorization|password|passwd|secret|token|credential|api.?key|"
    r"signing.?key|dsn|connection.?string|email|profile|request.?body|"
    r"payload|geojson|adif|sql|query.?text)",
    re.IGNORECASE,
)
_BEARER = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+")
_JWT = re.compile(\n    r"\\beyJ[A-Za-z0-9_-]{8,}\\.[A-Za-z0-9_-]{8,}\\.[A-Za-z0-9_-]{8,}\\b"\n)
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(password|passwd|secret|token|api[_-]?key|authorization|"
    r"access[_-]?key|signing[_-]?key)\b(\s*[:=]\s*)([^\s,;]+)"
)
_URL_CREDENTIALS = re.compile(r"(://)[^:/\s]+:[^@/\s]+@")


def redact_text(value: str, limit: int = 1024) -> str:
    text = _BEARER.sub("Bearer [REDACTED]", value)
    text = _JWT.sub("[REDACTED_JWT]", text)
    text = _SECRET_ASSIGNMENT.sub(r"\1\2[REDACTED]", text)
    text = _URL_CREDENTIALS.sub(r"\1[REDACTED]@", text)
    return text[:limit]


def safe_fields(fields: dict[str, Any]) -> dict[str, str | int | float | bool]:
    safe: dict[str, str | int | float | bool] = {}
    for key, value in fields.items():
        if not isinstance(key, str) or _SENSITIVE_KEY.search(key):
            continue
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]{0,63}", key):
            continue
        if isinstance(value, bool | int | float):
            safe[key] = value
        elif isinstance(value, str):
            safe[key] = redact_text(value, 256)
    return safe


@contextmanager
def bind_log_context(**fields: str) -> Iterator[None]:
    prior = _context.get()
    safe = safe_fields(fields)
    token = _context.set({**prior, **{k: str(v) for k, v in safe.items()}})
    try:
        yield
    finally:
        _context.reset(token)


def log_event(
    logger: logging.Logger,
    level: int,
    event: str,
    *,
    component: str,
    **fields: Any,
) -> None:
    logger.log(
        level,
        redact_text(event),
        extra={
            "event": redact_text(event),
            "component": component,
            **safe_fields(fields),
        },
    )


class _SafeFilter(logging.Filter):
    def __init__(self, service_name: str, component: str) -> None:
        super().__init__()
        self.service_name = service_name
        self.component = component
        self._standard = logging.LogRecord("", 0, "", 0, "", (), None).__dict__

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:
            message = "log message formatting failed"
        record.msg, record.args = redact_text(message), ()
        record.exc_info, record.exc_text = None, None
        if not getattr(record, "event", None):
            record.event = record.msg
        else:
            record.event = redact_text(str(record.event))
        if not getattr(record, "component", None):
            record.component = self.component
        for key in list(record.__dict__):
            if key.startswith("_") or key in self._standard:
                continue
            if _SENSITIVE_KEY.search(key):
                delattr(record, key)
                continue
            value = getattr(record, key)
            if isinstance(value, str):
                setattr(record, key, redact_text(value, 256))
            elif not isinstance(value, bool | int | float | type(None)):
                delattr(record, key)
        context = _context.get()
        for key in ("request_id", "correlation_id", "event_id", "job_id"):
            if context.get(key) and not hasattr(record, key):
                setattr(record, key, context[key])
        try:
            from opentelemetry import trace

            span_context = trace.get_current_span().get_span_context()
            if span_context and span_context.is_valid:
                record.trace_id = f"{span_context.trace_id:032x}"
                record.span_id = f"{span_context.span_id:016x}"
        except Exception:
            pass
        record.service_name = self.service_name
        record.service_namespace = "myota"
        record.deployment_environment = os.environ.get(\n            "MYOTA_ENV", "development"\n        )
        record.service_instance_id = os.environ.get("HOSTNAME", "local")
        return True


class _JsonFormatter(logging.Formatter):
    def __init__(self) -> None:
        super().__init__()
        self._standard = logging.LogRecord("", 0, "", 0, "", (), None).__dict__

    def format(self, record: logging.LogRecord) -> str:
        document: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "severity": record.levelname,
            "event": getattr(record, "event", record.getMessage()),
            "component": getattr(record, "component", "service"),
            "service.name": getattr(record, "service_name", "myota"),
            "service.namespace": getattr(record, "service_namespace", "myota"),
            "deployment.environment": getattr(
                record, "deployment_environment", "development"
            ),
            "service.instance.id": getattr(\n                record, "service_instance_id", "local"\n            ),
        }
        for key in (
            "trace_id",
            "span_id",
            "request_id",
            "correlation_id",
            "event_id",
            "job_id",
        ):
            value = getattr(record, key, None)
            if value:
                document[key] = value
        for key, value in record.__dict__.items():
            if key in self._standard or key.startswith("_"):
                continue
            if _SENSITIVE_KEY.search(key) or key in document or value is None:
                continue
            if isinstance(value, bool | int | float | str):
                document[key] = (
                    redact_text(value, 256)\n                    if isinstance(value, str)\n                    else value
                )
        return json.dumps(document, separators=(",", ":"), ensure_ascii=False)


def configure_logging(service_name: str, component: str = "service") -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    root = logging.getLogger()
    root.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
    safe_filter = _SafeFilter(service_name, component)
    stream = logging.StreamHandler()
    stream.setFormatter(_JsonFormatter())
    stream.addFilter(safe_filter)
    root.addHandler(stream)
    if os.environ.get("MYOTA_OTEL_ENABLED", "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        try:
            from opentelemetry._logs import set_logger_provider
            from opentelemetry.exporter.otlp.proto.grpc._log_exporter import (
                OTLPLogExporter,
            )
            from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
            from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
            from opentelemetry.sdk.resources import Resource

            endpoint = os.environ.get(
                "OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317"
            )
            resource = Resource.create(
                {
                    "service.name": os.environ.get(\n                        "OTEL_SERVICE_NAME", service_name\n                    ),
                    "service.namespace": "myota",
                    "deployment.environment": os.environ.get(
                        "MYOTA_ENV", "development"
                    ),
                    "service.instance.id": os.environ.get("HOSTNAME", "local"),
                }
            )
            provider = LoggerProvider(resource=resource)
            provider.add_log_record_processor(
                BatchLogRecordProcessor(
                    OTLPLogExporter(
                        endpoint=endpoint,
                        insecure=not endpoint.startswith("https://"),
                    )
                )
            )
            set_logger_provider(provider)
            otlp = LoggingHandler(\n                level=logging.NOTSET, logger_provider=provider\n            )
            otlp.addFilter(safe_filter)
            root.addHandler(otlp)
        except Exception:
            pass
    _CONFIGURED = True
