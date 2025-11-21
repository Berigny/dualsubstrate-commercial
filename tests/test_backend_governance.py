import shutil

import pytest
from fastapi.testclient import TestClient

from backend.main import app


@pytest.fixture()
def temp_backend_db(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path))
    yield tmp_path
    monkeypatch.delenv("DB_PATH", raising=False)
    shutil.rmtree(tmp_path, ignore_errors=True)


@pytest.fixture()
def backend_client(temp_backend_db):
    with TestClient(app) as client:
        yield client


def test_ledger_persists_across_clients(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path))
    payload = {
        "key": {"namespace": "ns", "identifier": "persist"},
        "state": {"coordinates": {"x": 1.0}, "phase": None, "metadata": {}},
        "notes": "first",
    }

    with TestClient(app) as client:
        resp = client.post("/ledger/write", json=payload)
        assert resp.status_code == 200

    with TestClient(app) as client:
        read = client.get("/ledger/read/ns:persist")
        assert read.status_code == 200
        body = read.json()
        assert body["key"]["identifier"] == "persist"
        assert body["state"]["coordinates"] == {"x": 1.0}

    monkeypatch.delenv("DB_PATH", raising=False)
    shutil.rmtree(tmp_path, ignore_errors=True)


def test_ethics_missing_ledger_entry_returns_404(backend_client):
    payload = {
        "actor": "tester",
        "action": "deploy",
        "key": {"namespace": "ns", "identifier": "missing"},
        "parameters": {"foo": 1.0},
    }

    resp = backend_client.post("/ethics/evaluate", json=payload)
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Ledger entry not found"


def test_coherence_deterministic_parameter_order(backend_client):
    base = {
        "actor": "tester",
        "action": "move",
    }
    payload_a = {**base, "parameters": {"b": 2.0, "a": 1.0}}
    payload_b = {**base, "parameters": {"a": 1.0, "b": 2.0}}

    first = backend_client.post("/coherence/evaluate", json=payload_a)
    second = backend_client.post("/coherence/evaluate", json=payload_b)

    assert first.status_code == second.status_code == 200
    assert first.json() == second.json()
