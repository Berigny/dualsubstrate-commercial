"""Shared helpers for structured request logging."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from time import perf_counter
from typing import Dict, Generator

from fastapi import Request


def _duration_ms(start: float) -> float:
    return round((perf_counter() - start) * 1000, 3)


@contextmanager
def log_operation(
    logger: logging.Logger,
    operation: str,
    request: Request | None = None,
    **context: object,
) -> Generator[Dict[str, object], None, None]:
    """Emit structured logs around an operation.

    Parameters
    ----------
    logger:
        Logger to emit records to.
    operation:
        Identifier for the operation (e.g. ``"ledger_read"``).
    request:
        Optional request object to capture HTTP context.
    context:
        Additional key/value pairs to include in the log context. The yielded
        mapping can be mutated to add dynamic values before completion.
    """

    start = perf_counter()
    base: Dict[str, object] = {"operation": operation, **context}

    if request is not None:
        base.setdefault("path", request.url.path)
        base.setdefault("method", request.method)

    try:
        yield base
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception(
            "%s failed",
            operation,
            extra={
                **base,
                "status": "error",
                "duration_ms": _duration_ms(start),
                "error": str(exc),
            },
        )
        raise
    else:
        logger.info(
            "%s completed",
            operation,
            extra={**base, "status": "success", "duration_ms": _duration_ms(start)},
        )

