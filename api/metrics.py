"""Prometheus metrics for the DualSubstrate gRPC server."""
from __future__ import annotations

from prometheus_client import Counter, Histogram

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


def record_ok(service: str, method: str, duration_seconds: float) -> None:
    """Record a successful RPC invocation."""
    REQUEST_COUNTER.labels(service=service, method=method, code="OK").inc()
    REQUEST_DURATION.labels(service=service, method=method).observe(duration_seconds)


def record_err(service: str, method: str, code: str) -> None:
    """Record a failed RPC invocation with the given status code."""
    REQUEST_COUNTER.labels(service=service, method=method, code=code).inc()
