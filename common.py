"""Shared HTTP, persistence and API-contract primitives."""
from __future__ import annotations

import json
import os
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler
from typing import Any, Callable, Iterator

MAX_BODY_BYTES = int(os.environ.get("MYOTA_MAX_BODY_BYTES", "1048576"))


def now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def new_id() -> str:
    return str(uuid.uuid4())


def page_result(items: list[Any], query: dict[str, list[str]] | None = None) -> dict[str, Any]:
    query = query or {}
    try:
        page = max(1, int(query.get("page", ["1"])[0]))
        page_size = min(100, max(1, int(query.get("pageSize", ["50"])[0])))
    except ValueError as exc:
        raise ValueError("page and pageSize must be integers") from exc
    total = len(items)
    start = (page - 1) * page_size
    return {"items": items[start:start + page_size], "page": page, "pageSize": page_size,
            "total": total, "nextPage": page + 1 if start + page_size < total else None}


class Store:
    """Service-owned state with optional PostgreSQL durability and a durable outbox."""

    def __init__(self, service: str = "service", dsn_env: str | None = None) -> None:
        self.service = service
        self.dsn = os.environ.get(dsn_env or "", "") if dsn_env else ""
        self.items: dict[str, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.idempotency: dict[str, Any] = {}
        self.lock = threading.RLock()
        self._pool: Any = None
        self._hydrated = False

    @property
    def durable(self) -> bool:
        return bool(self.dsn)

    def _ensure_pool(self) -> Any:
        if self._pool is not None:
            return self._pool
        try:
            from psycopg_pool import ConnectionPool
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("PostgreSQL is configured but psycopg[binary,pool] is not installed") from exc
        last: Exception | None = None
        for attempt in range(1, 6):
            try:
                self._pool = ConnectionPool(self.dsn, min_size=1, max_size=10, open=True,
                                            kwargs={"connect_timeout": 5})
                return self._pool
            except Exception as exc:  # pragma: no cover
                last = exc
                time.sleep(min(2 ** (attempt - 1), 8))
        raise RuntimeError(f"unable to connect to PostgreSQL after retries: {last}")

    @contextmanager
    def transaction(self) -> Iterator[Any]:
        if not self.durable:
            yield None
            return
        with self._ensure_pool().connection() as connection:
            try:
                yield connection
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def hydrate(self) -> None:
        if self._hydrated or not self.durable:
            self._hydrated = True
            return
        with self.transaction() as connection:
            row = connection.execute("SELECT state FROM service_state WHERE service = %s", (self.service,)).fetchone()
            if row:
                state = row[0]
                self.items, self.events = state.get("items", {}), state.get("events", [])
            rows = connection.execute("SELECT key, response FROM idempotency_record WHERE service = %s", (self.service,)).fetchall()
            self.idempotency = {key: response for key, response in rows}
        self._hydrated = True

    def persist(self) -> None:
        if not self.durable:
            return
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO service_state(service, state, updated_at) VALUES (%s, %s::jsonb, now()) "
                "ON CONFLICT (service) DO UPDATE SET state = EXCLUDED.state, updated_at = now()",
                (self.service, json.dumps({"items": self.items, "events": self.events})))
            for event in self.events:
                connection.execute(
                    "INSERT INTO outbox_event(event_id, event_type, producer, aggregate_type, aggregate_id, payload, occurred_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s) ON CONFLICT (event_id) DO NOTHING",
                    (event["eventId"], event["eventType"], event["producer"], event["aggregate"]["type"],
                     event["aggregate"]["id"], json.dumps(event["payload"]), event["occurredAt"]))
            for key, response in self.idempotency.items():
                connection.execute(
                    "INSERT INTO idempotency_record(service, key, response) VALUES (%s, %s, %s::jsonb) "
                    "ON CONFLICT (service, key) DO UPDATE SET response = EXCLUDED.response",
                    (self.service, key, json.dumps(response)))

    def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            self._pool = None

    def event(self, event_type: str, aggregate_type: str, aggregate_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        event = {"eventId": new_id(), "eventType": event_type, "occurredAt": now(),
                 "producer": self.service, "aggregate": {"type": aggregate_type, "id": aggregate_id},
                 "correlationId": new_id(), "payload": payload}
        self.events.append(event)
        return event

    def once(self, key: str | None, callback: Callable[[], Any]) -> Any:
        if not key:
            return callback()
        with self.lock:
            if key in self.idempotency:
                return self.idempotency[key]
            result = callback()
            self.idempotency[key] = result
            return result


def read_json(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    try:
        length = int(handler.headers.get("Content-Length", "0"))
    except ValueError as exc:
        raise ValueError("invalid Content-Length") from exc
    if length > MAX_BODY_BYTES:
        raise ValueError(f"request body exceeds {MAX_BODY_BYTES} bytes")
    if length == 0:
        return {}
    try:
        value = json.loads(handler.rfile.read(length).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid JSON body") from exc
    if not isinstance(value, dict):
        raise ValueError("JSON body must be an object")
    return value


class JsonHandler(BaseHTTPRequestHandler):
    service = "myota-service"
    routes: dict[tuple[str, str], Callable[["JsonHandler", dict[str, str]], Any]] = {}
    store = Store()

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _request_id(self) -> str:
        return self.headers.get("X-Request-ID") or new_id()

    def _send(self, status: int, payload: Any) -> None:
        data = b"" if status == 204 else json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("X-Request-ID", self.request_id)
        self.send_header("X-Correlation-ID", self.correlation_id)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, Idempotency-Key, X-Request-ID, X-Correlation-ID")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("API-Version", "v1")
        self.end_headers()
        if data:
            self.wfile.write(data)

    def _error(self, status: int, code: str, detail: str) -> None:
        self._send(status, {"type": f"https://myota.dev/problems/{code}", "title": code.replace("_", " ").title(),
                            "status": status, "code": code, "detail": detail,
                            "requestId": self.request_id, "correlationId": self.correlation_id})

    def do_OPTIONS(self) -> None:
        self.request_id, self.correlation_id = self._request_id(), self.headers.get("X-Correlation-ID") or new_id()
        self._send(204, {})

    def do_GET(self) -> None:
        self._dispatch("GET")

    def do_POST(self) -> None:
        self._dispatch("POST")

    def _dispatch(self, method: str) -> None:
        self.request_id, self.correlation_id = self._request_id(), self.headers.get("X-Correlation-ID") or new_id()
        if self.path == "/healthz":
            self._send(200, {"status": "ok", "service": self.service, "time": now(), "durable": self.store.durable})
            return
        path = self.path.split("?", 1)[0]
        for (route_method, pattern), fn in self.routes.items():
            if route_method != method:
                continue
            parts, actual = pattern.strip("/").split("/"), path.strip("/").split("/")
            if len(parts) != len(actual):
                continue
            params: dict[str, str] = {}
            matched = True
            for wanted, got in zip(parts, actual):
                if wanted.startswith("{") and wanted.endswith("}"):
                    params[wanted[1:-1]] = got
                elif wanted != got:
                    matched = False
                    break
            if matched:
                try:
                    body = read_json(self) if method == "POST" else {}
                    result = fn(self, {**params, "_body": body, "_path": self.path,
                                       "Idempotency-Key": self.headers.get("Idempotency-Key")})
                    status = result.pop("_status", 200) if isinstance(result, dict) else 200
                    self.store.persist()
                    self._send(status, result)
                except ValueError as exc:
                    self._error(400, "invalid_request", str(exc))
                except KeyError as exc:
                    self._error(404, "not_found", str(exc))
                except PermissionError as exc:
                    self._error(403, "forbidden", str(exc))
                except Exception as exc:  # pragma: no cover
                    self._error(500, "internal_error", str(exc))
                return
        self._error(404, "not_found", f"no route for {path}")


def require(body: dict[str, Any], *names: str) -> None:
    missing = [name for name in names if not body.get(name)]
    if missing:
        raise ValueError("missing required fields: " + ", ".join(missing))
