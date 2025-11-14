"""High-level application wrapper around the DualSubstrate HTTP client."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict

from dualsubstrate_sdk.api_client import (
    DualSubstrateClient,
    DualSubstrateError,
    RateLimitError,
    ValidationError,
)
from dualsubstrate_sdk.http_models import TraverseResponse


class ApiServiceError(Exception):
    """Raised when a service-layer action cannot be fulfilled."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        breadcrumbs: Dict[str, Any],
        status_code: int | None = None,
        detail: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.detail = detail
        self.breadcrumbs = dict(breadcrumbs)
        self.cause = cause

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "code": self.code,
            "message": self.message,
            "breadcrumbs": self.breadcrumbs,
        }
        if self.status_code is not None:
            payload["status_code"] = self.status_code
        if self.detail is not None:
            payload["detail"] = self.detail
        return payload


@dataclass
class ApiService:
    """Orchestrates application-specific workflows against the HTTP API."""

    client: DualSubstrateClient
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger(__name__))

    def traverse(
        self,
        start: int,
        depth: int,
        *,
        ledger_id: str | None = None,
    ) -> TraverseResponse:
        """Traverse the DualSubstrate automaton with local validation and logging."""

        breadcrumbs: Dict[str, Any] = {
            "operation": "traverse",
            "start": start,
            "depth": depth,
        }
        if ledger_id:
            breadcrumbs["ledger_id"] = ledger_id

        if not 0 <= start <= 7:
            breadcrumbs["status_code"] = 400
            message = "Start must be between 0 and 7."
            self.logger.warning("Traverse start outside allowed range", extra={"breadcrumbs": breadcrumbs})
            raise ApiServiceError(
                "start_out_of_range",
                message,
                breadcrumbs=breadcrumbs,
                status_code=400,
                detail=message,
            )

        if not 1 <= depth <= 10:
            breadcrumbs["status_code"] = 400
            message = "Depth must be between 1 and 10."
            self.logger.warning("Traverse depth outside allowed range", extra={"breadcrumbs": breadcrumbs})
            raise ApiServiceError(
                "depth_out_of_range",
                message,
                breadcrumbs=breadcrumbs,
                status_code=400,
                detail=message,
            )

        try:
            response = self.client.traverse(start=start, depth=depth, ledger_id=ledger_id)
        except ValidationError as exc:
            breadcrumbs["status_code"] = exc.status_code or 422
            self.logger.info("Traverse validation error", extra={"breadcrumbs": breadcrumbs})
            raise ApiServiceError(
                "validation_error",
                exc.detail or str(exc),
                breadcrumbs=breadcrumbs,
                status_code=exc.status_code or 422,
                detail=exc.detail,
                cause=exc,
            ) from exc
        except RateLimitError as exc:
            breadcrumbs["status_code"] = exc.status_code or 429
            self.logger.warning("Traverse request rate limited", extra={"breadcrumbs": breadcrumbs})
            raise ApiServiceError(
                "rate_limited",
                exc.detail or str(exc),
                breadcrumbs=breadcrumbs,
                status_code=exc.status_code or 429,
                detail=exc.detail,
                cause=exc,
            ) from exc
        except DualSubstrateError as exc:
            breadcrumbs["status_code"] = exc.status_code or 500
            self.logger.error("Traverse request failed", extra={"breadcrumbs": breadcrumbs})
            raise ApiServiceError(
                "backend_error",
                exc.detail or str(exc),
                breadcrumbs=breadcrumbs,
                status_code=exc.status_code,
                detail=exc.detail,
                cause=exc,
            ) from exc
        except Exception as exc:  # pragma: no cover - safety net
            breadcrumbs["exception"] = exc.__class__.__name__
            self.logger.exception("Unexpected traverse failure", extra={"breadcrumbs": breadcrumbs})
            raise ApiServiceError(
                "unexpected_error",
                "Unexpected error during traverse",
                breadcrumbs=breadcrumbs,
                detail=str(exc),
                cause=exc,
            ) from exc

        self.logger.debug("Traverse request succeeded", extra={"breadcrumbs": breadcrumbs})
        return response


__all__ = ["ApiService", "ApiServiceError"]
