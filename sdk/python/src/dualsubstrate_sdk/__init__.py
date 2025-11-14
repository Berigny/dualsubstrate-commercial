"""DualSubstrate Python SDK."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__version__ = "1.0.0"

__all__ = [
    "DualSubstrateClient",
    "DualSubstrateError",
    "LedgerClient",
    "MemoryAnchor",
    "RateLimitError",
    "ResponseParseError",
    "ServerError",
    "TraversePath",
    "TraverseResponse",
    "UnexpectedResponseError",
    "ValidationError",
    "__version__",
]

_EXPORTS = {
    "DualSubstrateClient": ("dualsubstrate_sdk.api_client", "DualSubstrateClient"),
    "DualSubstrateError": ("dualsubstrate_sdk.api_client", "DualSubstrateError"),
    "LedgerClient": ("dualsubstrate_sdk.grpc_client", "LedgerClient"),
    "MemoryAnchor": ("dualsubstrate_sdk.qp_memory", "MemoryAnchor"),
    "RateLimitError": ("dualsubstrate_sdk.api_client", "RateLimitError"),
    "ResponseParseError": ("dualsubstrate_sdk.api_client", "ResponseParseError"),
    "ServerError": ("dualsubstrate_sdk.api_client", "ServerError"),
    "TraversePath": ("dualsubstrate_sdk.http_models", "TraversePath"),
    "TraverseResponse": ("dualsubstrate_sdk.http_models", "TraverseResponse"),
    "UnexpectedResponseError": ("dualsubstrate_sdk.api_client", "UnexpectedResponseError"),
    "ValidationError": ("dualsubstrate_sdk.api_client", "ValidationError"),
}


def __getattr__(name: str) -> Any:
    try:
        module_name, attribute = _EXPORTS[name]
    except KeyError as exc:  # pragma: no cover - mirrors default behaviour
        raise AttributeError(f"module 'dualsubstrate_sdk' has no attribute '{name}'") from exc

    module = import_module(module_name)
    value = getattr(module, attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:  # pragma: no cover - introspection helper
    return sorted(__all__)
