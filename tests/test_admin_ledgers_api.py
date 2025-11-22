import os
import uuid

import pytest
from starlette.testclient import TestClient

from backend.main import app as backend_app


@pytest.fixture()
def ledger_client(tmp_path):
    original_path = os.environ.get("DB_PATH")
    os.environ["DB_PATH"] = str(tmp_path)

    try:
        with TestClient(backend_app) as client:
            yield client
    finally:
        if original_path is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = original_path


def _write_entry(client: TestClient, namespace: str) -> None:
    identifier = f"entry-{uuid.uuid4().hex[:8]}"
    payload = {
        "key": {"namespace": namespace, "identifier": identifier},
        "state": {"coordinates": {}, "phase": "test", "metadata": {"note": "ping"}},
    }
    resp = client.post("/ledger/write", json=payload)
    assert resp.status_code == 200, resp.text


def test_list_ledgers_includes_defaults_and_discovered(ledger_client):
    resp = ledger_client.get("/admin/ledgers")
    assert resp.status_code == 200, resp.text
    ledgers = resp.json().get("ledgers", [])
    assert "default" in ledgers

    _write_entry(ledger_client, namespace="fresh-ledger")
    resp = ledger_client.get("/admin/ledgers")
    assert resp.status_code == 200, resp.text
    ledgers = resp.json().get("ledgers", [])
    assert "fresh-ledger" in ledgers


def test_create_ledger_registers_namespace(ledger_client):
    resp = ledger_client.post("/admin/ledgers", json={"name": "ui-ledger"})
    assert resp.status_code == 200, resp.text

    payload = resp.json()
    assert payload.get("ledger") == "ui-ledger"
    assert "ui-ledger" in payload.get("ledgers", [])

    follow_up = ledger_client.get("/admin/ledgers")
    assert follow_up.status_code == 200, follow_up.text
    assert "ui-ledger" in follow_up.json().get("ledgers", [])
