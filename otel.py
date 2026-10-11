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
        sel