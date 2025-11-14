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
        *,
        entity: str | None = None,
        origin: int | None = None,
        limit: int = 8,
        depth: int = 1,
        direction: str = "forward",
        include_metadata: bool = False,
        ledger_id: str | None = None,
    ) -> TraverseResponse:
        """Traverse the DualSubstrate ledger graph with client-side validation."""

        direction_hint = (direction or "forward").strip().lower() or "forward"
        breadcrumbs: Dict[str, Any] = {
            "operation": "traverse",
            "entity": entity,
            "origin": origin,
            "limit": limit,
            "depth": depth,
            "direction": direction_hint,
            "include_metadata": include_metadata,
        }
        if ledger_id:
            breadcrumbs["ledger_id"] = ledger_id

        if not 1 <= limit <= 64:
            breadcrumbs["status_code"] = 400
            message = "Limit must be between 1 and 64."
            self.logger.warning("Traverse limit outside allowed range", extra={"breadcrumbs": breadcrumbs})
            raise ApiServiceError(
                "limit_out_of_range",
                message,
                breadcrumbs=breadcrumbs,
                status_code=400,
                detail=message,
            )

        if not 1 <= depth <= 32:
            breadcrumbs["status_code"] = 400
            message = "Depth must be between 1 and 32."
            self.logger.warning("Traverse depth outside allowed range", extra={"breadcrumbs": breadcrumbs})
            raise ApiServiceError(
                "depth_out_of_range",
                message,
                breadcrumbs=breadcrumbs,
                status_code=400,
                detail=message,
            )

        if direction_hint not in {"forward", "backward", "both"}:
            breadcrumbs["status_code"] = 400
            message = "Direction must be forward, backward, or both."
            self.logger.warning("Traverse direction invalid", extra={"breadcrumbs": breadcrumbs})
            raise ApiServiceError(
                "direction_invalid",
                message,
                breadcrumbs=breadcrumbs,
                status_code=400,
                detail=message,
            )

        try:
            response = self.client.traverse(
                entity=entity,
                origin=origin,
                limit=limit,
                depth=depth,
                direction=direction_hint,
                include_metadata=include_metadata,
                ledger_id=ledger_id,
            )
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
            detail_text = (exc.detail or str(exc) or "").strip()

            endpoint_missing = False
            if exc.status_code in {404, 405}:
                endpoint_missing = True
            elif detail_text:
                lowered = detail_text.lower()
                if "missing authentication token" in lowered:
                    endpoint_missing = True

            if endpoint_missing:
                message = (
                    "Traversal endpoint is unavailable on the backend. "
                    "Redeploy the DualSubstrate API with GET /traverse support."
                )
                self.logger.error(
                    "Traverse endpoint missing on backend", extra={"breadcrumbs": breadcrumbs}
                )
                raise ApiServiceError(
                    "traverse_endpoint_missing",
                    message,
                    breadcrumbs=breadcrumbs,
                    status_code=exc.status_code,
                    detail=detail_text or None,
                    cause=exc,
                ) from exc

            self.logger.error("Traverse request failed", extra={"breadcrumbs": breadcrumbs})
            raise ApiServiceError(
                "backend_error",
                detail_text or str(exc),
                breadcrumbs=breadcrumbs,
                status_code=exc.status_code,
                detail=detail_text or None,
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
