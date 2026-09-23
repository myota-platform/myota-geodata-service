"""Small dependency-free HTTP and persistence primitives for the vertical slice."""
from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler
from typing import Any, Callable


def now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def new_id() -> str:
    return str(uuid.uuid4())


class Store:
    def __init__(self) -> None:
        self.items: dict[str, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.idempotency: dict[str, Any] = {}
        self.lock = threading.RLock()

    def event(self, event_type: str, aggregate_type: str, aggregate_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        event = {"eventId": new_id(), "eventType": event_type, "occurredAt": now(),
                 "producer": self.__class__.__name__, "aggregate": {"type": aggregate_type, "id": aggregate_id},
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
    length = int(handler.headers.get("Content-Length", "0"))
    if length == 0:
        return {}
    raw = handler.rfile.read(length)
    try:
        value = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON: {exc.msg}") from exc
    if not isinstance(value, dict):
        raise ValueError("JSON body must be an object")
    return value


class JsonHandler(BaseHTTPRequestHandler):
    service = "myota-service"
    routes: dict[tuple[str, str], Callable[["JsonHandler", dict[str, str]], Any]] = {}
    store = Store()

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _send(self, status: int, payload: Any) -> None:
        data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, Idempotency-Key")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()
        self.wfile.write(data)

    def do_OPTIONS(self) -> None:
        self._send(204, {})

    def do_GET(self) -> None:
        self._dispatch("GET")

    def do_POST(self) -> None:
        self._dispatch("POST")

    def _dispatch(self, method: str) -> None:
        if self.path == "/healthz":
            self._send(200, {"status": "ok", "service": self.service, "time": now()})
            return
        path = self.path.split("?", 1)[0]
        for (route_method, pattern), fn in self.routes.items():
            if route_method != method:
                continue
            parts = pattern.strip("/").split("/")
            actual = path.strip("/").split("/")
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
                    self._send(status, result)
                except ValueError as exc:
                    self._send(400, {"error": "invalid_request", "message": str(exc)})
                except KeyError as exc:
                    self._send(404, {"error": "not_found", "message": str(exc)})
                except PermissionError as exc:
                    self._send(403, {"error": "forbidden", "message": str(exc)})
                except Exception as exc:  # pragma: no cover - safety net for the HTTP boundary
                    self._send(500, {"error": "internal_error", "message": str(exc)})
                return
        self._send(404, {"error": "not_found", "path": path})


def require(body: dict[str, Any], *names: str) -> None:
    missing = [name for name in names if not body.get(name)]
    if missing:
        raise ValueError("missing required fields: " + ", ".join(missing))
