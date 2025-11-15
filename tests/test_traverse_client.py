import json
from pathlib import Path

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

    def raise_for_status(self):  # pragma: no cover - mirrors requests.Response API
        if 400 <= self.status_code:
            raise AssertionError("raise_for_status should not be invoked in tests")


class DummySession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append({"method": "GET", "url": url, **kwargs})
        try:
            return self._responses.pop(0)
        except IndexError:  # pragma: no cover - defensive
            raise AssertionError("No response queued for GET call")

    def put(self, url, **kwargs):
        self.calls.append({"method": "PUT", "url": url, **kwargs})
        try:
            return self._responses.pop(0)
        except IndexError:  # pragma: no cover - defensive
            raise AssertionError("No response queued for PUT call")


def test_traverse_successfully_parses_payload():
    payload = {
        "origin": 23,
        "paths": [
            {"nodes": [23, 37, 41], "weight": 0.82, "metadata": {"prime": 37}},
        ],
        "metadata": {"tier": "S1"},
        "supported": True,
    }
    session = DummySession([DummyResponse(200, json_data=payload)])
    client = DualSubstrateClient(base_url="https://api.test", api_key="token", session=session)

    response = client.traverse(
        entity="demo",
        origin=23,
        limit=3,
        depth=2,
        direction="forward",
        include_metadata=True,
        ledger_id="ledger-1",
    )

    assert response.origin == 23
    assert response.paths[0].nodes == (23, 37, 41)
    assert response.paths[0].weight == pytest.approx(0.82)
    assert response.paths[0].metadata["prime"] == 37
    assert response.metadata["tier"] == "S1"

    call = session.calls[0]
    assert call["params"] == {
        "entity": "demo",
        "origin": 23,
        "limit": 3,
        "depth": 2,
        "direction": "forward",
        "include_metadata": True,
    }
    assert call["headers"]["Authorization"] == "Bearer token"
    assert call["headers"]["X-Ledger-ID"] == "ledger-1"


def test_traverse_handles_invalid_json_payload():
    session = DummySession(
        [DummyResponse(200, json_exc=ValueError("bad json"), text="not-json")]
    )
    client = DualSubstrateClient(base_url="https://api.test", session=session)

    with pytest.raises(ResponseParseError):
        client.traverse()


def test_traverse_handles_malformed_payload_structure():
    payload = {"paths": ["oops"], "origin": 23, "metadata": {}, "supported": True}
    session = DummySession([DummyResponse(200, json_data=payload)])
    client = DualSubstrateClient(base_url="https://api.test", session=session)

    with pytest.raises(ResponseParseError):
        client.traverse()


def test_traverse_raises_validation_error():
    payload = {"detail": "Traversal unsupported"}
    session = DummySession([DummyResponse(422, json_data=payload)])
    client = DualSubstrateClient(base_url="https://api.test", session=session)

    with pytest.raises(ValidationError) as excinfo:
        client.traverse()

    assert excinfo.value.detail == "Traversal unsupported"
    assert excinfo.value.status_code == 422


def test_traverse_raises_rate_limit_error():
    payload = {"detail": "Too many requests"}
    session = DummySession([DummyResponse(429, json_data=payload)])
    client = DualSubstrateClient(base_url="https://api.test", session=session)

    with pytest.raises(RateLimitError) as excinfo:
        client.traverse()

    assert excinfo.value.status_code == 429


def test_traverse_raises_server_error_for_5xx():
    payload = {"detail": "Internal server error"}
    session = DummySession([DummyResponse(503, json_data=payload)])
    client = DualSubstrateClient(base_url="https://api.test", session=session)

    with pytest.raises(ServerError) as excinfo:
        client.traverse()

    assert excinfo.value.status_code == 503


def test_traverse_raises_unexpected_status():
    session = DummySession([DummyResponse(404, json_data={"detail": "not found"})])
    client = DualSubstrateClient(base_url="https://api.test", session=session)

    with pytest.raises(UnexpectedResponseError) as excinfo:
        client.traverse()

    assert excinfo.value.status_code == 404


def test_write_structured_views_matches_contract(tmp_path):
    payload_path = Path(__file__).with_name("contracts") / "s2_payload.json"
    expected_payload = json.loads(payload_path.read_text())

    facets_input = {int(key): value for key, value in expected_payload.items()}
    facets_input["views"] = {"ignored": True}
    facets_input["entity"] = "aurora"  # legacy field that must be excluded

    session = DummySession([DummyResponse(200, json_data={"ok": True})])
    client = DualSubstrateClient(base_url="https://api.test", api_key="token", session=session)

    response = client.write_structured_views(
        entity="aurora",
        facets=facets_input,
        ledger_id="ledger-1",
    )

    assert response == {"ok": True}
    assert session.calls, "expected HTTP call to be recorded"

    call = session.calls[0]
    assert call["method"] == "PUT"
    assert call["url"] == "https://api.test/ledger/s2"
    assert call["params"] == {"entity": "aurora"}
    assert call["headers"]["Authorization"] == "Bearer token"
    assert call["headers"]["X-Ledger-ID"] == "ledger-1"
    assert call["json"] == expected_payload
    assert "entity" not in call["json"]
    assert "views" not in call["json"]
