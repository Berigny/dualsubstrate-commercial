import logging

import pytest

from dualsubstrate_sdk.api_client import DualSubstrateError, RateLimitError, ValidationError
from dualsubstrate_sdk.http_models import TraversePath, TraverseResponse

from services.api import ApiService, ApiServiceError


class StubClient:
    def __init__(self, *, response=None, error: Exception | None = None):
        self._response = response
        self._error = error
        self.calls: list[dict[str, object]] = []

    def traverse(self, **kwargs):
        self.calls.append(kwargs)
        if self._error is not None:
            raise self._error
        return self._response


def _service(response=None, error: Exception | None = None) -> ApiService:
    client = StubClient(response=response, error=error)
    logger = logging.getLogger("test.api_service")
    return ApiService(client=client, logger=logger), client


def test_traverse_successful_call_returns_payload():
    response = TraverseResponse(
        origin=23,
        paths=(TraversePath(nodes=(23, 37, 41), weight=0.82, metadata={"prime": 37}),),
        metadata={"tier": "S1"},
        supported=True,
    )
    service, client = _service(response=response)

    result = service.traverse(
        entity="demo",
        origin=23,
        limit=3,
        depth=2,
        direction="forward",
        include_metadata=True,
        ledger_id="ledger-1",
    )

    assert result is response
    assert client.calls == [
        {
            "entity": "demo",
            "origin": 23,
            "limit": 3,
            "depth": 2,
            "direction": "forward",
            "include_metadata": True,
            "ledger_id": "ledger-1",
        }
    ]


def test_traverse_validates_limit_range():
    service, client = _service()

    with pytest.raises(ApiServiceError) as excinfo:
        service.traverse(entity="demo", limit=0)

    err = excinfo.value
    assert err.code == "limit_out_of_range"
    assert err.status_code == 400
    assert client.calls == []


def test_traverse_validates_depth_range():
    service, client = _service()

    with pytest.raises(ApiServiceError) as excinfo:
        service.traverse(entity="demo", depth=64)

    err = excinfo.value
    assert err.code == "depth_out_of_range"
    assert err.status_code == 400
    assert client.calls == []


def test_traverse_validates_direction():
    service, client = _service()

    with pytest.raises(ApiServiceError) as excinfo:
        service.traverse(entity="demo", direction="sideways")

    err = excinfo.value
    assert err.code == "direction_invalid"
    assert err.status_code == 400
    assert client.calls == []


def test_traverse_wraps_validation_error():
    error = ValidationError(
        "Traverse request rejected",
        status_code=422,
        detail="No legal outbound edge",
    )
    service, _ = _service(error=error)

    with pytest.raises(ApiServiceError) as excinfo:
        service.traverse(entity="demo")

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
        service.traverse(entity="demo")

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
        service.traverse(entity="demo")

    err = excinfo.value
    assert err.code == "backend_error"
    assert err.status_code == 503
    assert err.detail == "Internal server error"


@pytest.mark.parametrize(
    "status, detail",
    [
        (404, "Not Found"),
        (405, "Method Not Allowed"),
        (403, "Missing Authentication Token"),
    ],
)
def test_traverse_reports_missing_endpoint(status, detail):
    error = DualSubstrateError(
        "Unexpected status code",
        status_code=status,
        detail=detail,
    )
    service, _ = _service(error=error)

    with pytest.raises(ApiServiceError) as excinfo:
        service.traverse(entity="demo")

    err = excinfo.value
    assert err.code == "traverse_endpoint_missing"
    assert "GET /traverse" in err.message
    assert err.detail == detail
