"""Keep integration/activity framework features when synchronizing geodata."""

import unittest
from unittest.mock import patch

from common import BoundedThreadingHTTPServer, Store
from otel import telemetry_for


class SharedCompatibilityTests(unittest.TestCase):
    def test_relational_activity_can_disable_compatibility_snapshot(self):
        store = Store("activity", persist_state=False)
        store.dsn = "postgresql://unused"
        with patch.object(store, "transaction") as transaction:
            store.hydrate()
            store.persist()
        transaction.assert_not_called()

    def test_bounded_server_is_available_to_integration_runner(self):
        from common import JsonHandler

        server = BoundedThreadingHTTPServer(
            ("127.0.0.1", 0), JsonHandler, bind_and_activate=False
        )
        try:
            self.assertTrue(server.daemon_threads)
        finally:
            server.server_close()

    def test_request_telemetry_accepts_body_size(self):
        request = telemetry_for("compatibility-test").start_request(
            "POST", "/v1/test", 100
        )
        request.finish(200, "/v1/test")
        self.assertTrue(request.finished)
