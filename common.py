"""Shared HTTP, persistence and API-contract primitives."""
from __future__ import annotations

import json
import base64
import errno
import hashlib
import hmac
import io
import os
import secrets
import threading
import tempfile
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from email.parser import BytesParser
from email.policy import default as email_default
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any, Callable, Iterator
from metrics import METRICS
from otel import telemetry_for

# Geodata imports are sent as JSON envelopes and can legitimately contain a
# sizeable pasted FeatureCollection. Deployments may lower this explicitly,
# but the service default must not reject ordinary large dataset intake.
MAX_BODY_BYTES = int(os.environ.get("MYOTA_MAX_BODY_BYTES", str(1024 * 1024 * 1024)))


def require_durable_database(dsn_env: str | None, dsn: str) -> None:
    """Fail fast instead of silently dropping writes into process memory."""
    required = os.environ.get("MYOTA_REQUIRE_DURABILITY", "").strip().lower() in {"1", "true", "yes", "on"}
    if required and dsn_env and not dsn:
        raise RuntimeError(f"{dsn_env} is required when MYOTA_REQUIRE_DURABILITY is enabled")


def now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def json_default(value: Any) -> str:
    """Encode database timestamp values safely at JSON/API boundaries."""
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def new_id() -> str:
    return str(uuid.uuid4())


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _unb64(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def auth_signing_key() -> bytes:
    value = os.environ.get("MYOTA_AUTH_SIGNING_KEY", "myota-development-key-change-me")
    if os.environ.get("MYOTA_ENV", "development").lower() == "production" and value == "myota-development-key-change-me":
        raise RuntimeError("MYOTA_AUTH_SIGNING_KEY must be set outside development")
    return value.encode("utf-8")


def sign_token(claims: dict[str, Any], token_type: str = "access") -> str:
    header = {"alg": "HS256", "typ": "JWT", "tokenType": token_type}
    encoded_header = _b64(json.dumps(header, separators=(",", ":")).encode())
    encoded_claims = _b64(json.dumps(claims, separators=(",", ":")).encode())
    signing_input = f"{encoded_header}.{encoded_claims}".encode()
    signature = hmac.new(auth_signing_key(), signing_input, hashlib.sha256).digest()
    return f"{encoded_header}.{encoded_claims}.{_b64(signature)}"


def verify_token(token: str, expected_type: str | None = None) -> dict[str, Any]:
    try:
        encoded_header, encoded_claims, encoded_signature = token.split(".", 2)
        signing_input = f"{encoded_header}.{encoded_claims}".encode()
        expected = hmac.new(auth_signing_key(), signing_input, hashlib.sha256).digest()
        if not hmac.compare_digest(expected, _unb64(encoded_signature)):
            raise ValueError("invalid token signature")
        header, claims = json.loads(_unb64(encoded_header)), json.loads(_unb64(encoded_claims))
        if header.get("alg") != "HS256" or (expected_type and header.get("tokenType") != expected_type):
            raise ValueError("invalid token type")
        if int(claims.get("exp", 0)) <= int(datetime.now(timezone.utc).timestamp()):
            raise ValueError("token expired")
        return claims
    except (ValueError, TypeError, KeyError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise PermissionError("invalid or expired token") from exc


def hash_secret(value: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.scrypt(value.encode(), salt=salt, n=2**14, r=8, p=1)
    return f"scrypt${_b64(salt)}${_b64(digest)}"


def verify_secret(value: str, encoded: str) -> bool:
    try:
        scheme, salt, expected = encoded.split("$", 2)
        if scheme != "scrypt":
            return False
        actual = hashlib.scrypt(value.encode(), salt=_unb64(salt), n=2**14, r=8, p=1)
        return hmac.compare_digest(actual, _unb64(expected))
    except (ValueError, TypeError):
        return False


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
        require_durable_database(dsn_env, self.dsn)
        self.items: dict[str, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.data: dict[str, Any] = {}
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
                self.data = state.get("data", {})
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
                (self.service, json.dumps({"items": self.items, "events": self.events, "data": self.data}, default=json_default)))
            for event in self.events:
                connection.execute(
                    "INSERT INTO outbox_event(event_id, event_type, producer, aggregate_type, aggregate_id, payload, occurred_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s) ON CONFLICT (event_id) DO NOTHING",
                    (event["eventId"], event["eventType"], event["producer"], event["aggregate"]["type"],
                     event["aggregate"]["id"], json.dumps(event["payload"], default=json_default), event["occurredAt"]))
            for key, response in self.idempotency.items():
                connection.execute(
                    "INSERT INTO idempotency_record(service, key, response) VALUES (%s, %s, %s::jsonb) "
                    "ON CONFLICT (service, key) DO UPDATE SET response = EXCLUDED.response",
                    (self.service, key, json.dumps(response, default=json_default)))

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


def _request_length(handler: BaseHTTPRequestHandler) -> int:
    try:
        length = int(handler.headers.get("Content-Length", "0"))
    except ValueError as exc:
        raise ValueError("invalid Content-Length") from exc
    if length > MAX_BODY_BYTES:
        raise ValueError(f"request body exceeds {MAX_BODY_BYTES} bytes")
    return length


def read_json(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    length = _request_length(handler)
    if length == 0:
        return {}
    try:
        value = json.loads(handler.rfile.read(length).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid JSON body") from exc
    if not isinstance(value, dict):
        raise ValueError("JSON body must be an object")
    return value


def read_multipart(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    """Read metadata and spool a binary file without converting it to Base64."""
    length = _request_length(handler)
    if length == 0:
        raise ValueError("multipart request body must not be empty")
    content_type = handler.headers.get("Content-Type", "")
    content_header = BytesParser(policy=email_default).parsebytes(
        f"Content-Type: {content_type}\r\n\r\n".encode("ascii"))
    boundary = content_header.get_boundary()
    if not boundary:
        raise ValueError("multipart request is missing its boundary")

    class MultipartReader:
        def __init__(self, stream: Any, remaining: int) -> None:
            self.stream, self.buffer, self.remaining = stream, b"", remaining

        def fill(self) -> None:
            if self.remaining <= 0:
                raise ValueError("multipart request ended unexpectedly")
            chunk = self.stream.read(min(8 * 1024 * 1024, self.remaining))
            if not chunk:
                raise ValueError("multipart request ended unexpectedly")
            self.remaining -= len(chunk)
            self.buffer += chunk

        def read(self, size: int) -> bytes:
            while len(self.buffer) < size:
                self.fill()
            value, self.buffer = self.buffer[:size], self.buffer[size:]
            return value

        def readline(self) -> bytes:
            while b"\r\n" not in self.buffer:
                self.fill()
            index = self.buffer.index(b"\r\n")
            value, self.buffer = self.buffer[:index], self.buffer[index + 2:]
            return value

        def read_part(self, delimiter: bytes, sink: Any) -> None:
            keep = len(delimiter) - 1
            while True:
                index = self.buffer.find(delimiter)
                if index >= 0:
                    sink.write(self.buffer[:index])
                    self.buffer = self.buffer[index + len(delimiter):]
                    return
                if len(self.buffer) > keep:
                    sink.write(self.buffer[:-keep])
                    self.buffer = self.buffer[-keep:]
                self.fill()

    reader = MultipartReader(handler.rfile, length)
    if reader.readline() != f"--{boundary}".encode("ascii"):
        raise ValueError("invalid multipart opening boundary")
    delimiter = b"\r\n--" + boundary.encode("ascii")
    result: dict[str, Any] = {}
    while True:
        header_lines = []
        while True:
            line = reader.readline()
            if not line:
                break
            header_lines.append(line)
        headers = BytesParser(policy=email_default).parsebytes(b"\r\n".join(header_lines) + b"\r\n")
        disposition = headers.get("Content-Disposition", "")
        field_name = headers.get_param("name", header="content-disposition")
        filename = headers.get_filename()
        if not field_name:
            raise ValueError("multipart part is missing its field name")
        if filename or field_name == "file":
            spool_dir = os.environ.get("MYOTA_UPLOAD_SPOOL_DIR", "").strip() or None
            if spool_dir:
                Path(spool_dir).mkdir(parents=True, exist_ok=True)
            temporary = tempfile.NamedTemporaryFile(prefix="myota-geodata-upload-", suffix=".part",
                                                     dir=spool_dir, delete=False)
            try:
                with temporary:
                    reader.read_part(delimiter, temporary)
                result["_uploadPath"] = temporary.name
                result["_uploadFilename"] = filename or "upload"
            except Exception:
                os.unlink(temporary.name)
                raise
        else:
            value = io.BytesIO()
            reader.read_part(delimiter, value)
            if value.tell() > 4 * 1024 * 1024:
                raise ValueError("multipart metadata part is too large")
            if field_name == "metadata":
                try:
                    metadata = json.loads(value.getvalue().decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise ValueError("metadata must contain valid JSON") from exc
                if not isinstance(metadata, dict):
                    raise ValueError("metadata must be a JSON object")
                result.update(metadata)
        suffix = reader.read(2)
        if suffix == b"--":
            break
        if suffix != b"\r\n":
            raise ValueError("invalid multipart boundary suffix")
    if "_uploadPath" not in result:
        raise ValueError("multipart request must include a file")
    return result


class JsonHandler(BaseHTTPRequestHandler):
    service = "myota-service"
    routes: dict[tuple[str, str], Callable[["JsonHandler", dict[str, str]], Any]] = {}
    store = Store()
    deprecated_routes: set[tuple[str, str]] = set()

    @classmethod
    def metrics_extra(cls) -> dict[str, float]:
        return {}

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _request_id(self) -> str:
        return self.headers.get("X-Request-ID") or new_id()

    def _send(self, status: int, payload: Any) -> None:
        route = getattr(self, "current_route", None)
        route_name = route[1] if route else getattr(self, "path", "unknown").split("?", 1)[0]
        METRICS.inc("myota_http_requests_total", {"service": self.service, "method": getattr(self, "command", "UNKNOWN"), "route": route_name, "status": status})
        request_telemetry = getattr(self, "_otel_request", None)
        if request_telemetry:
            request_telemetry.finish(status, route_name)
        if route in self.deprecated_routes:
            METRICS.inc("myota_legacy_route_requests_total", {"service": self.service, "method": route[0], "route": route[1]})
        data = b"" if status == 204 else json.dumps(payload, separators=(",", ":"), default=json_default).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("X-Request-ID", self.request_id)
            self.send_header("X-Correlation-ID", self.correlation_id)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, Idempotency-Key, X-Request-ID, X-Correlation-ID")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
            if getattr(self, "current_route", None) in self.deprecated_routes:
                self.send_header("Deprecation", "true")
                self.send_header("Sunset", os.environ.get("MYOTA_LEGACY_ROUTE_SUNSET", "2027-04-01T00:00:00Z"))
            self.send_header("API-Version", "v1")
            self.end_headers()
            if data:
                self.wfile.write(data)
        except OSError as exc:
            # Browser fetch cancellation, navigation, and proxy timeouts can
            # close the socket before a response is fully written. There is
            # no second response to send, so do not turn that into a traceback.
            if exc.errno not in {errno.EPIPE, errno.ECONNRESET, errno.ESHUTDOWN}:
                raise

    def _error(self, status: int, code: str, detail: str) -> None:
        self._send(status, {"type": f"https://myota.dev/problems/{code}", "title": code.replace("_", " ").title(),
                            "status": status, "code": code, "detail": detail,
                            "requestId": self.request_id, "correlationId": self.correlation_id})

    def _send_metrics(self) -> None:
        data = METRICS.render(type(self).metrics_extra()).encode("utf-8")
        request_telemetry = getattr(self, "_otel_request", None)
        if request_telemetry:
            request_telemetry.finish(200, "/metrics")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_OPTIONS(self) -> None:
        self.request_id, self.correlation_id = self._request_id(), self.headers.get("X-Correlation-ID") or new_id()
        self._send(204, {})

    def do_GET(self) -> None:
        self._dispatch("GET")

    def do_POST(self) -> None:
        self._dispatch("POST")

    def do_PUT(self) -> None:
        self._dispatch("PUT")

    def do_PATCH(self) -> None:
        self._dispatch("PATCH")

    def do_DELETE(self) -> None:
        self._dispatch("DELETE")

    def _dispatch(self, method: str) -> None:
        self.request_id, self.correlation_id = self._request_id(), self.headers.get("X-Correlation-ID") or new_id()
        self.command = method
        self._otel_request = telemetry_for(self.service).start_request(method, self.path.split("?", 1)[0])
        if self.path.split("?", 1)[0] == "/metrics":
            self._send_metrics()
            return
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
                    self.current_route = (method, pattern)
                    if method in {"POST", "PUT", "PATCH", "DELETE"}:
                        content_type = self.headers.get("Content-Type", "")
                        body = read_multipart(self) if content_type.lower().startswith("multipart/form-data") else read_json(self)
                    else:
                        body = {}
                    result = fn(self, {**params, "_body": body, "_path": self.path,
                                       "Idempotency-Key": self.headers.get("Idempotency-Key"),
                                       "Authorization": self.headers.get("Authorization", ""),
                                       "User-Agent": self.headers.get("User-Agent", ""),
                                       "Remote-Addr": self.client_address[0], "_http": "1"})
                    status = result.pop("_status", 200) if isinstance(result, dict) else 200
                    if method in {"POST", "PUT", "PATCH", "DELETE"}:
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
