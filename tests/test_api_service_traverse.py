import logging

import pytest

from dualsubstrate_sdk.api_client import DualSubstrateError, RateLimitError, ValidationError
from dualsubstrate_sdk.http_models import TraverseEdge, TraverseResponse

from services.api import ApiService, ApiServiceError


class StubClient:
    def __init__(self, *, response=None, error: Exception | None = None):
        self._response = response
        self._error = error
        self.calls: list[dict[str, object]] = []

    def traverse(self, start: int, depth: int, ledger_id: str | None = None):
        self.calls.append({"start": start, "depth": depth, "ledger_id": ledger_id})
        if self._error is not None:
            raise self._error
        return self._response


def _service(response=None, error: Exception | None = None) -> ApiService:
    client = StubClient(response=response, error=error)
    logger = logging.getLogger("test.api_service")
    return ApiService(client=client, logger=logger), client


def test_traverse_successful_call_returns_payload():
    response = TraverseResponse(
        edges=(TraverseEdge(src=0, dst=1, via_c=False, label="work"),),
        centroid_flips=1,
        final_centroid=0,
    )
    service, client = _service(response=response)

    result = service.traverse(0, 3, ledger_id="ledger-1")

    assert result is response
    assert client.calls == [{"start": 0, "depth": 3, "ledger_id": "ledger-1"}]


def test_traverse_validates_start_range():
    service, client = _service()

    with pytest.raises(ApiServiceError) as excinfo:
        service.traverse(-1, 2)

    err = excinfo.value
    assert err.code == "start_out_of_range"
    assert err.status_code == 400
    assert err.breadcrumbs["start"] == -1
    assert client.calls == []


def test_traverse_validates_depth_range():
    service, client = _service()

    with pytest.raises(ApiServiceError) as excinfo:
        service.traverse(1, 11)

    err = excinfo.value
    assert err.code == "depth_out_of_range"
    assert err.status_code == 400
    assert err.breadcrumbs["depth"] == 11
    assert client.calls == []


def test_traverse_wraps_validation_error():
    error = ValidationError(
        "Traverse request rejected",
        status_code=422,
        detail="No legal outbound edge",
    )
    service, _ = _service(error=error)

    with pytest.raises(ApiServiceError) as excinfo:
        service.traverse(1, 2)

    err = excinfo.value
    assert err.code == "validation_error"
    assert err.status_code == 422
    assert err.detail == "No legal outbound edge"
    assert err.cause is error
    assert err.breadcrumbs["operation"] == "traverse"


def test_traverse_wraps_rate_limit_error():
    error = RateLimitError(
        "Traverse request rate limited",
        status_code=429,
        detail="Too many requests",
    )
    service, _ = _service(error=error)

    with pytest.raises(ApiServiceError) as excinfo:
        service.traverse(1, 2)

    err = excinfo.value
    assert err.code == "rate_limited"
    assert err.status_code == 429
    assert err.detail == "Too many requests"


def test_traverse_wraps_generic_client_error():
    error = DualSubstrateError(
        "Server exploded",
        status_code=503,
        detail="Internal server error",
    )
    service, _ = _service(error=error)

    with pytest.raises(ApiServiceError) as excinfo:
        service.traverse(1, 2)

    err = excinfo.value
    assert err.code == "backend_error"
    assert err.status_code == 503
    assert err.detail == "Internal server error"
