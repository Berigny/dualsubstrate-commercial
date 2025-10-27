import json
from typing import Any

import pytest
from fastapi.testclient import TestClient

from api.main import app


class _FixedSalience:
    def __init__(self, value: float) -> None:
        self._value = value

    def score(self, _: str) -> float:
        return self._value


@pytest.fixture(name="client")
def fixture_client() -> TestClient:
    return TestClient(app)


def _set_salience(monkeypatch: pytest.MonkeyPatch, value: float) -> None:
    monkeypatch.setattr("api.main._SALIENCE_MODEL", _FixedSalience(value))
    monkeypatch.setattr("api.main.SALIENT_THRESHOLD", 0.5)


def test_salience_stores_and_exact_returns_payload(monkeypatch: pytest.MonkeyPatch, client: TestClient):
    _set_salience(monkeypatch, 0.9)

    payload = {"utterance": "Remember to call Alice tomorrow", "timestamp": 123.0}
    response = client.post("/salience", json=payload, headers={"x-api-key": "mvp-secret"})
    assert response.status_code == 200
    data: dict[str, Any] = response.json()
    assert data["stored"] is True
    assert len(data["key"]) == 32
    assert data["len"] == len(payload["utterance"])
    assert pytest.approx(data["score"], rel=1e-3) == 0.9
    assert data["threshold"] == 0.5

    exact = client.get(f"/exact/{data['key']}", headers={"x-api-key": "mvp-secret"})
    assert exact.status_code == 200
    exact_payload = exact.json()
    assert exact_payload["text"] == payload["utterance"]
    assert exact_payload["t"] == payload["timestamp"]
    assert exact_payload["score"] == data["score"]


def test_salience_below_threshold(monkeypatch: pytest.MonkeyPatch, client: TestClient):
    _set_salience(monkeypatch, 0.2)

    payload = {"utterance": "Too dull", "timestamp": 456.0}
    response = client.post("/salience", json=payload, headers={"x-api-key": "mvp-secret"})
    assert response.status_code == 200
    data = response.json()
    assert data == {"stored": False, "score": 0.2, "text": payload["utterance"], "threshold": 0.5}


def test_exact_invalid_key(client: TestClient):
    response = client.get("/exact/not-a-key", headers={"x-api-key": "mvp-secret"})
    assert response.status_code == 422
