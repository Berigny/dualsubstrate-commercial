"""Prometheus metrics for the DualSubstrate gRPC server."""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

SERVICE_LABELS = ("service", "method")

REQUEST_COUNTER = Counter(
    "grpc_server_handled_total",
    "Total RPCs handled",
    SERVICE_LABELS + ("code",),
)

REQUEST_DURATION = Histogram(
    "grpc_server_handling_seconds",
    "RPC latency buckets",
    SERVICE_LABELS,
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0),
)

ANCHOR_ENERGY_TOTAL = Gauge(
    "ledger_anchor_energy_total",
    "Total mixed energy of the most recent anchor per entity.",
    ("entity",),
)

ANCHOR_ENERGY_CONTINUOUS = Gauge(
    "ledger_anchor_energy_continuous",
    "Continuous mismatch contribution of the mixed energy.",
    ("entity",),
)

ANCHOR_ENERGY_DISCRETE = Gauge(
    "ledger_anchor_energy_discrete_weighted",
    "λ-weighted discrete contribution of the mixed energy.",
    ("entity",),
)


def record_ok(service: str, method: str, duration_seconds: float) -> None:
    """Record a successful RPC invocation."""
    REQUEST_COUNTER.labels(service=service, method=method, code="OK").inc()
    REQUEST_DURATION.labels(service=service, method=method).observe(duration_seconds)


def record_err(service: str, method: str, code: str) -> None:
    """Record a failed RPC invocation with the given status code."""
    REQUEST_COUNTER.labels(service=service, method=method, code=code).inc()


def record_anchor_energy(
    entity: str, total: float, continuous: float, discrete_weighted: float
) -> None:
    """Update Prometheus gauges for the mixed energy functional."""

    labels = {"entity": entity}
    ANCHOR_ENERGY_TOTAL.labels(**labels).set(total)
    ANCHOR_ENERGY_CONTINUOUS.labels(**labels).set(continuous)
    ANCHOR_ENERGY_DISCRETE.labels(**labels).set(discrete_weighted)


__all__ = [
    "record_anchor_energy",
    "record_err",
    "record_ok",
]
