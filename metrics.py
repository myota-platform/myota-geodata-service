"""Small dependency-free Prometheus registry for the geodata service."""

from __future__ import annotations

import threading
from collections import defaultdict
from typing import Mapping


def _labels(labels: Mapping[str, object]) -> str:
    if not labels:
        return ""
    parts = []
    for key in sorted(labels):
        value = (
            str(labels[key])
            .replace("\\", "\\\\")
            .replace('"', '\\"')
            .replace("\n", "\\n")
        )
        parts.append(f'{key}="{value}"')
    return "{" + ",".join(parts) + "}"


class MetricsRegistry:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._counters: defaultdict[
            tuple[str, tuple[tuple[str, str], ...]], float
        ] = defaultdict(float)
        self._gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self._histograms: dict[
            tuple[str, tuple[tuple[str, str], ...]], list[float]
        ] = {}

    def inc(
        self, name: str, labels: Mapping[str, object] | None = None
    ) -> None:
        key = (
            name,
            tuple(sorted((str(k), str(v)) for k, v in (labels or {}).items())),
        )
        with self._lock:
            self._counters[key] += 1

    def set_gauge(
        self,
        name: str,
        value: float,
        labels: Mapping[str, object] | None = None,
    ) -> None:
        key = (
            name,
            tuple(sorted((str(k), str(v)) for k, v in (labels or {}).items())),
        )
        with self._lock:
            self._gauges[key] = float(value)

    def observe(
        self,
        name: str,
        value: float,
        labels: Mapping[str, object] | None = None,
    ) -> None:
        key = (
            name,
            tuple(sorted((str(k), str(v)) for k, v in (labels or {}).items())),
        )
        with self._lock:
            values = self._histograms.setdefault(
                key, [0.0, 0.0, *([0.0] * len(_HISTOGRAM_BUCKETS))]
            )
            values[0] += float(value)
            values[1] += 1
            for index, bound in enumerate(_HISTOGRAM_BUCKETS, start=2):
                if value <= bound:
                    values[index] += 1

    def _render_histogram(
        self,
        name: str,
        labels: tuple[tuple[str, str], ...],
        values: list[float],
    ) -> list[str]:
        label_set = dict(labels)
        lines = []
        for index, bound in enumerate(_HISTOGRAM_BUCKETS, start=2):
            lines.append(
                f"{name}_bucket{_labels({**label_set, 'le': bound})} {values[index]:g}"
            )
        lines.append(
            f"{name}_bucket{_labels({**label_set, 'le': '+Inf'})} {values[1]:g}"
        )
        lines.append(f"{name}_sum{_labels(label_set)} {values[0]:g}")
        lines.append(f"{name}_count{_labels(label_set)} {values[1]:g}")
        return lines

    def render(self, extra: Mapping[str, float] | None = None) -> str:
        with self._lock:
            rows = list(self._counters.items())
            gauges = list(self._gauges.items())
            histograms = list(self._histograms.items())
        lines = [
            f"{name}{_labels(dict(labels))} {value:g}"
            for (name, labels), value in sorted(rows)
        ]
        lines += [
            f"{name}{_labels(dict(labels))} {value:g}"
            for (name, labels), value in sorted(gauges)
        ]
        for (name, labels), values in sorted(histograms):
            lines.extend(self._render_histogram(name, labels, values))
        lines += [
            f"{name} {float(value):g}"
            for name, value in sorted((extra or {}).items())
        ]
        return "\n".join(lines) + "\n"


_HISTOGRAM_BUCKETS = (
    0.001,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
)


METRICS = MetricsRegistry()
