import os

import pytest
from starlette.testclient import TestClient

from backend.main import app as backend_app


@pytest.fixture()
def compat_client(tmp_path):
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


def test_anchor_roundtrip(compat_client: TestClient):
    payload = {
        "entity": "test",
        "factors": [{"prime": 3, "delta": 1}],
        "text": "hello world",
    }
    resp = compat_client.post("/anchor", json=payload)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["status"] == "anchored"
    assert body["edges"], "edges should mirror legacy payload"

    entry_id = f"test:{body['timestamp']}"
    read_resp = compat_client.get(f"/ledger/read/{entry_id}")
    assert read_resp.status_code == 200, read_resp.text

    entry = read_resp.json()
    assert entry["key"]["namespace"] == "test"
    assert entry["key"]["identifier"] == str(body["timestamp"])
    assert entry["state"]["metadata"].get("text") == "hello world"
    assert entry["state"]["coordinates"].get("3") == 1.0


def test_ledger_metrics(compat_client: TestClient):
    # populate a couple of entries to ensure metrics aggregate
    for idx in range(2):
        payload = {
            "entity": "test",
            "factors": [{"prime": 5, "delta": idx}],
            "text": f"note-{idx}",
        }
        compat_client.post("/anchor", json=payload)

    metrics_resp = compat_client.get("/ledger", params={"entity": "test"})
    assert metrics_resp.status_code == 200, metrics_resp.text
    metrics = metrics_resp.json()

    assert metrics["entity"] == "test"
    assert metrics["entry_count"] >= 2
    assert metrics["last_updated"] is not None


def test_rotate_stub(compat_client: TestClient):
    payload = {"entity": "demo", "axis": [0, 0, 1], "angle": 1.57}
    resp = compat_client.post("/rotate", json=payload)
    assert resp.status_code == 200, resp.text

    data = resp.json()
    assert data == {"status": "skipped", "reason": "rotation-disabled"}
