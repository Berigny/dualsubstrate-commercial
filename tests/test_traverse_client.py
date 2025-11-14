import pytest

from dualsubstrate_sdk.api_client import (
    DualSubstrateClient,
    RateLimitError,
    ResponseParseError,
    ServerError,
    UnexpectedResponseError,
    ValidationError,
)


class DummyResponse:
    def __init__(self, status_code=200, json_data=None, text="", json_exc=None):
        self.status_code = status_code
        self._json_data = json_data
        self._json_exc = json_exc
        self.text = text

    def json(self):
        if self._json_exc is not None:
            raise self._json_exc
        return self._json_data

    def raise_for_status(self):  # pragma: no cover - not used but mimics requests.Response
        if 400 <= self.status_code:
            raise AssertionError("raise_for_status should not be invoked in tests")


class DummySession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append({"url": url, **kwargs})
        try:
            return self._responses.pop(0)
        except IndexError:  # pragma: no cover - defensive
            raise AssertionError("No response queued for POST call")


def test_traverse_successfully_parses_payload():
    payload = {
        "edges": [{"src": 0, "dst": 1, "via_c": False, "label": "work"}],
        "centroid_flips": 2,
        "final_centroid": 1,
    }
    session = DummySession([DummyResponse(200, json_data=payload)])
    client = DualSubstrateClient(base_url="https://api.test", api_key="token", session=session)

    response = client.traverse(0, 3, ledger_id="ledger-1")

    assert response.centroid_flips == 2
    assert response.final_centroid == 1
    assert response.edges[0].label == "work"

    call = session.calls[0]
    assert call["params"] == {"start": 0, "depth": 3}
    assert call["headers"]["Authorization"] == "Bearer token"
    assert call["headers"]["X-Ledger-ID"] == "ledger-1"


def test_traverse_handles_invalid_json_payload():
    session = DummySession(
        [DummyResponse(200, json_exc=ValueError("bad json"), text="not-json")]
    )
    client = DualSubstrateClient(base_url="https://api.test", session=session)

    with pytest.raises(ResponseParseError):
        client.traverse(0, 1)


def test_traverse_handles_malformed_payload_structure():
    payload = {"edges": ["oops"], "centroid_flips": 1, "final_centroid": 0}
    session = DummySession([DummyResponse(200, json_data=payload)])
    client = DualSubstrateClient(base_url="https://api.test", session=session)

    with pytest.raises(ResponseParseError):
        client.traverse(1, 1)


def test_traverse_raises_validation_error():
    payload = {"detail": "No legal outbound edge"}
    session = DummySession([DummyResponse(422, json_data=payload)])
    client = DualSubstrateClient(base_url="https://api.test", session=session)

    with pytest.raises(ValidationError) as excinfo:
        client.traverse(4, 1)

    assert excinfo.value.detail == "No legal outbound edge"
    assert excinfo.value.status_code == 422


def test_traverse_raises_rate_limit_error():
    payload = {"detail": "Too many requests"}
    session = DummySession([DummyResponse(429, json_data=payload)])
    client = DualSubstrateClient(base_url="https://api.test", session=session)

    with pytest.raises(RateLimitError) as excinfo:
        client.traverse(2, 2)

    assert excinfo.value.status_code == 429


def test_traverse_raises_server_error_for_5xx():
    payload = {"detail": "Internal server error"}
    session = DummySession([DummyResponse(503, json_data=payload)])
    client = DualSubstrateClient(base_url="https://api.test", session=session)

    with pytest.raises(ServerError) as excinfo:
        client.traverse(1, 2)

    assert excinfo.value.status_code == 503


def test_traverse_raises_unexpected_status():
    session = DummySession([DummyResponse(404, json_data={"detail": "not found"})])
    client = DualSubstrateClient(base_url="https://api.test", session=session)

    with pytest.raises(UnexpectedResponseError) as excinfo:
        client.traverse(1, 2)

    assert excinfo.value.status_code == 404
