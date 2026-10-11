import json
import logging
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

from myota_logging import (
    _JsonFormatter,
    _SafeFilter,
    bind_log_context,
    redact_text,
    safe_fields,
    log_http_completed,
)


def test_structured_record_contains_resource_and_correlation_fields():
    fake_trace = SimpleNamespace(
        get_current_span=lambda: SimpleNamespace(
            get_span_context=lambda: SimpleNamespace(
                is_valid=True,
                trace_id=0x1234567890ABCDEF1234567890ABCDEF,
                span_id=0x1234567890ABCDEF,
            )
        )
    )
    fake_otel = ModuleType("opentelemetry")
    fake_otel.trace = fake_trace
    with bind_log_context(request_id="req-1", correlation_id="flow-1"):
        record = logging.LogRecord(
            "logging-test", logging.INFO, __file__, 1, "work.started", (), None
        )
        record.event = "work.started"
        record.component = "worker"
        with patch.dict(sys.modules, {"opentelemetry": fake_otel}):
            assert _SafeFilter("myota-test", "service").filter(record)
        output = json.loads(_JsonFormatter().format(record))
    assert output["severity"] == "INFO"
    assert output["event"] == "work.started"
    assert output["component"] == "worker"
    assert output["request_id"] == "req-1"
    assert output["correlation_id"] == "flow-1"
    assert output["trace_id"] == "1234567890abcdef1234567890abcdef"
    assert output["span_id"] == "1234567890abcdef"
    assert output["service.name"] == "myota-test"
    assert output["service.namespace"] == "myota"
    assert output["timestamp"].endswith("Z")


def test_sensitive_fields_and_credential_text_are_redacted():
    fields = safe_fields(
        {
            "request_id": "req-1",
            "authorization": "Bearer abc.def",
            "payload": {"private": "value"},
            "safe": "password=hunter2",
        }
    )
    assert fields == {"request_id": "req-1", "safe": "password=[REDACTED]"}
    assert "abc.def" not in redact_text("Authorization: Bearer abc.def")
    assert "[REDACTED]" in redact_text("password=hunter2")


def test_json_formatter_does_not_emit_unbounded_payload_fields():
    record = logging.LogRecord(
        "logging-test", logging.INFO, __file__, 1, "event", (), None
    )
    record.payload = "complete uploaded content"
    record.secret_key = "secret"
    output = json.loads(_JsonFormatter().format(record))
    assert "payload" not in output
    assert "secret_key" not in output


def test_http_completion_is_sanitized_and_classified(caplog):
    logger = logging.getLogger("myota.http-test")
    cases = [(200, logging.INFO, None), (403, logging.WARNING, "client_error"), (500, logging.ERROR, "server_error")]
    with caplog.at_level(logging.DEBUG, logger="myota.http-test"):
        for status, _level, _classification in cases:
            log_http_completed(
                logger,
                method="POST",
                route="/v1/imports/{importId}",
                status=status,
                duration_ms=12.34567,
                request_id="request-1",
                correlation_id="flow-1",
            )
    records = caplog.records[-3:]
    assert [record.levelno for record in records] == [case[1] for case in cases]
    assert [record.__dict__.get("error.classification") for record in records] == [
        case[2] for case in cases
    ]
    assert all(record.getMessage() == "http.request.completed" for record in records)
    assert records[-1].__dict__["http.route"] == "/v1/imports/{importId}"
    assert records[-1].request_id == "request-1"
    assert records[-1].correlation_id == "flow-1"


def test_http_trace_context_is_extracted_and_injected():
    parent = object()
    captured = {}
    trace_id = 0x1234567890ABCDEF1234567890ABCDEF

    class FakeSpan:
        def get_span_context(self):
            return SimpleNamespace(is_valid=True, trace_id=trace_id)

        def update_name(self, name):
            captured["name"] = name

        def set_attribute(self, key, value):
            captured[key] = value

        def end(self):
            captured["ended"] = True

    class FakeScope:
        def __enter__(self):
            return None

        def __exit__(self, *_args):
            captured["scope_closed"] = True

    class FakeTracer:
        def start_span(self, name, *, context, kind):
            captured.update(start_name=name, parent=context, kind=kind)
            return FakeSpan()

    fake_span_kind = SimpleNamespace(SERVER=object())
    fake_trace = ModuleType("opentelemetry.trace")
    fake_trace.SpanKind = fake_span_kind
    fake_trace.get_tracer = lambda _name: FakeTracer()
    fake_trace.use_span = lambda _span, end_on_exit: FakeScope()

    def extract(carrier):
        captured["carrier"] = carrier
        return parent

    def inject(carrier):
        carrier["traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

    fake_propagate = SimpleNamespace(extract=extract, inject=inject)
    fake_otel = ModuleType("opentelemetry")
    fake_otel.trace = fake_trace
    fake_otel.propagate = fake_propagate
    with patch.dict(
        sys.modules,
        {
            "opentelemetry": fake_otel,
            "opentelemetry.trace": fake_trace,
        },
    ):
        request_span = start_http_span(
            "myota-identity",
            "GET",
            {
                "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                "tracestate": "vendor=value",
            },
        )
        request_span.set_result(200, "/v1/identity/me")
        headers = inject_trace_context({})
        request_span.end()

    assert captured["parent"] is parent
    assert captured["carrier"]["tracestate"] == "vendor=value"
    assert captured["name"] == "GET /v1/identity/me"
    assert request_span.trace_id == f"{trace_id:032x}"
    assert headers["traceparent"].startswith("00-")
    assert captured["scope_closed"] is True
    assert captured["ended"] is True
