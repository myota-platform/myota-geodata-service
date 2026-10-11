from common import Store, bind_correlation_id, normalize_correlation_id


def test_store_event_uses_request_correlation():
    store = Store("geodata")
    with bind_correlation_id("workflow-42"):
        event = store.event("geodata.entity.created.v1", "entity", "id", {})
    assert event["correlationId"] == "workflow-42"


def test_request_correlation_is_bounded_printable_text():
    assert normalize_correlation_id("workflow-42") == "workflow-42"
    assert normalize_correlation_id("x" * 129) is None
    assert normalize_correlation_id("workflow\nsecret") is None
