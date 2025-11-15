"""HTTP API client for DualSubstrate REST endpoints."""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Dict

import requests

from .http_models import PayloadValidationError, TraverseResponse

LEDGER_HEADER = "X-Ledger-ID"

logger = logging.getLogger(__name__)

_ALLOWED_S2_PRIMES = {"11", "13", "17", "19"}
S2_PAYLOAD_DIAGNOSTIC = "S2_PAYLOAD"


class DualSubstrateError(Exception):
    """Base exception for HTTP client failures."""

    def __init__(self, message: str, *, status_code: int | None = None, detail: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.detail = detail


class ValidationError(DualSubstrateError):
    """Raised when the API rejects a request due to validation errors."""


class RateLimitError(DualSubstrateError):
    """Raised when the API responds with HTTP 429."""


class ServerError(DualSubstrateError):
    """Raised for HTTP 5xx responses."""


class ResponseParseError(DualSubstrateError):
    """Raised when a successful response cannot be parsed."""


class UnexpectedResponseError(DualSubstrateError):
    """Raised when the server returns an unhandled status code."""


def _extract_detail(response: requests.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return response.text.strip() or f"HTTP {response.status_code}"

    if isinstance(payload, Mapping):
        detail = payload.get("detail")
        if isinstance(detail, list):
            return "; ".join(str(item) for item in detail)
        if isinstance(detail, Mapping):
            try:
                return json.dumps(detail, ensure_ascii=False)
            except (TypeError, ValueError):
                return str(detail)
        if detail is not None:
            return str(detail)
        return json.dumps(payload, ensure_ascii=False)
    return str(payload)


@dataclass
class DualSubstrateClient:
    """Thin wrapper around the DualSubstrate FastAPI surface."""

    base_url: str = "http://localhost:8080"
    api_key: str | None = None
    session: requests.Session | None = None

    def __post_init__(self) -> None:
        self._session = self.session or requests.Session()
        self._base = self.base_url.rstrip("/")
        headers: Dict[str, str] = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        self._default_headers = headers

    # ------------------------------------------------------------------
    def traverse(
        self,
        *,
        entity: str | None = None,
        origin: int | None = None,
        limit: int | None = None,
        depth: int | None = None,
        direction: str | None = None,
        include_metadata: bool | None = None,
        ledger_id: str | None = None,
        timeout: float | None = None,
    ) -> TraverseResponse:
        """Invoke the ``/traverse`` endpoint and parse the response payload."""

        headers = dict(self._default_headers)
        if ledger_id:
            headers[LEDGER_HEADER] = ledger_id

        params: Dict[str, object] = {}
        if entity:
            params["entity"] = entity
        if origin is not None:
            params["origin"] = origin
        if limit is not None:
            params["limit"] = limit
        if depth is not None:
            params["depth"] = depth
        if direction:
            params["direction"] = direction
        if include_metadata is not None:
            params["include_metadata"] = include_metadata

        response = self._session.get(
            f"{self._base}/traverse",
            headers=headers or None,
            params=params or None,
            timeout=timeout or float(os.getenv("DUALSUBSTRATE_HTTP_TIMEOUT", "10")),
        )

        status = response.status_code
        detail = _extract_detail(response)

        if status == 422:
            raise ValidationError("Traverse request rejected", status_code=status, detail=detail)
        if status == 429:
            raise RateLimitError("Traverse request rate limited", status_code=status, detail=detail)
        if 500 <= status <= 599:
            raise ServerError("Server error during traverse", status_code=status, detail=detail)
        if status != 200:
            raise UnexpectedResponseError(
                f"Unexpected status code {status} from traverse", status_code=status, detail=detail
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise ResponseParseError(
                "Traverse response did not contain valid JSON",
                detail=response.text.strip() or None,
            ) from exc

        try:
            return TraverseResponse.from_dict(payload)
        except PayloadValidationError as exc:
            raise ResponseParseError(
                "Traverse response payload was malformed", detail=str(exc)
            ) from exc

    def write_structured_views(
        self,
        *,
        entity: str,
        facets: Mapping[str | int, Mapping[str, Any]] | None = None,
        ledger_id: str | None = None,
        timeout: float | None = None,
    ) -> Dict[str, Any]:
        """Persist S2 facets for ``entity`` using the ``/ledger/s2`` endpoint."""

        if not entity or not entity.strip():
            raise ValueError("entity must be provided")

        headers = dict(self._default_headers)
        if ledger_id:
            headers[LEDGER_HEADER] = ledger_id

        params = {"entity": entity}

        normalised: Dict[str, Dict[str, Any]] = {}
        for prime_key, payload in (facets or {}).items():
            prime_str = str(prime_key)
            if prime_str not in _ALLOWED_S2_PRIMES:
                continue
            if not isinstance(payload, Mapping):
                logger.debug(
                    "%s skipping non-mapping facet for prime %s",
                    S2_PAYLOAD_DIAGNOSTIC,
                    prime_str,
                )
                continue
            normalised[prime_str] = dict(payload)

        if logger.isEnabledFor(logging.DEBUG):
            try:
                logger.debug(
                    "%s %s",
                    S2_PAYLOAD_DIAGNOSTIC,
                    json.dumps(normalised, ensure_ascii=False, sort_keys=True),
                )
            except (TypeError, ValueError):  # pragma: no cover - diagnostics only
                logger.debug("%s %r", S2_PAYLOAD_DIAGNOSTIC, normalised)

        response = self._session.put(
            f"{self._base}/ledger/s2",
            headers=headers or None,
            params=params,
            json=normalised,
            timeout=timeout or float(os.getenv("DUALSUBSTRATE_HTTP_TIMEOUT", "10")),
        )

        status = response.status_code
        detail = _extract_detail(response)

        if status == 422:
            raise ValidationError(
                "Structured views request rejected", status_code=status, detail=detail
            )
        if status == 429:
            raise RateLimitError(
                "Structured views request rate limited", status_code=status, detail=detail
            )
        if 500 <= status <= 599:
            raise ServerError(
                "Server error during structured views write",
                status_code=status,
                detail=detail,
            )
        if status != 200:
            raise UnexpectedResponseError(
                f"Unexpected status code {status} from structured views write",
                status_code=status,
                detail=detail,
            )

        try:
            return response.json()
        except ValueError as exc:
            raise ResponseParseError(
                "Structured views response did not contain valid JSON",
                detail=response.text.strip() or None,
            ) from exc


__all__ = [
    "DualSubstrateClient",
    "DualSubstrateError",
    "RateLimitError",
    "ResponseParseError",
    "ServerError",
    "UnexpectedResponseError",
    "ValidationError",
    "LEDGER_HEADER",
]
