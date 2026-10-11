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
