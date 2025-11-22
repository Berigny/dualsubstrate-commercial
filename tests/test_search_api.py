import os
import uuid

import pytest
from starlette.testclient import TestClient

from backend.main import app as backend_app


def _write_entry(
    client: TestClient,
    *,
    text: str,
    namespace: str = "default",
    metadata: dict | None = None,
) -> str:
    identifier = f"entry-{uuid.uuid4().hex[:8]}"
    entry_id = f"{namespace}:{identifier}"
    payload_metadata = {"summary": text}
    if metadata:
        payload_metadata.update(metadata)
    payload = {
        "key": {"namespace": namespace, "identifier": identifier},
        "state": {
            "coordinates": {},
            "phase": "test",
            "metadata": payload_metadata,
        },
    }

    resp = client.post("/ledger/write", json=payload)
    assert resp.status_code == 200, resp.text
    return entry_id


@pytest.fixture()
def search_client(tmp_path):
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


def test_search_indexes_written_entry(search_client):
    entry_id = _write_entry(
        search_client, text="Dual Substrate ledger entry with explicit metadata"
    )

    resp = search_client.get(
        "/search", params={"entity": "default", "q": "Dual Substrate"}
    )
    assert resp.status_code == 200, resp.text

    payload = resp.json()
    assert payload["query"] == "Dual Substrate"
    assert payload["mode"] == "any"

    matches = {
        row["entry_id"]: row
        for row in payload.get("results", [])
        if row.get("entry_id")
    }

    assert entry_id in matches
    result = matches[entry_id]
    assert result["score"] > 0
    assert "dual substrate" in result["snippet"].lower()
    assert result["entry"]["key"]["identifier"] in entry_id


def test_search_mode_all_requires_all_tokens(search_client):
    entry_with_all = _write_entry(
        search_client, text="Dual Substrate signal includes both tokens"
    )
    entry_partial = _write_entry(search_client, text="Dual channel only")

    any_resp = search_client.get(
        "/search",
        params={"entity": "default", "q": "Dual Substrate", "mode": "any"},
    )
    assert any_resp.status_code == 200, any_resp.text
    any_ids = [row.get("entry_id") for row in any_resp.json().get("results", [])]
    assert entry_with_all in any_ids
    assert entry_partial in any_ids
    scores = [row["score"] for row in any_resp.json()["results"] if row.get("entry_id") in {entry_with_all, entry_partial}]
    assert scores == sorted(scores, reverse=True)

    all_resp = search_client.get(
        "/search",
        params={"entity": "default", "q": "Dual Substrate", "mode": "all"},
    )
    assert all_resp.status_code == 200, all_resp.text
    all_ids = [row.get("entry_id") for row in all_resp.json().get("results", [])]
    assert entry_with_all in all_ids
    assert entry_partial not in all_ids


def test_search_indexes_body_metadata(search_client):
    entry_id = _write_entry(
        search_client,
        text="Annotation body mentions Dual Substrate in nested structures",
        metadata={"body": {"text": "Dual Substrate narrative in body content"}},
    )

    resp = search_client.get(
        "/search", params={"entity": "default", "q": "Dual Substrate"}
    )
    assert resp.status_code == 200, resp.text
    results = resp.json().get("results", [])
    assert any(row.get("entry_id") == entry_id and row.get("score", 0) > 0 for row in results)


def test_search_rejects_unknown_mode(search_client):
    _write_entry(search_client, text="Dual Substrate fallback")

    resp = search_client.get(
        "/search",
        params={"entity": "default", "q": "Dual Substrate", "mode": "unsupported"},
    )
    assert resp.status_code == 422


def test_search_returns_latest_for_entity_when_query_missing(search_client):
    resp = search_client.get("/search", params={"entity": "no-matches", "limit": 50})

    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload.get("query") == ""
    assert payload.get("mode") == "all" or payload.get("mode") == "any"
    assert payload.get("results") == []


def test_search_filters_results_by_entity(search_client):
    entry_default = _write_entry(
        search_client, text="Shared topic across entities", namespace="default"
    )
    _write_entry(
        search_client, text="Shared topic across entities", namespace="other-entity"
    )

    resp = search_client.get(
        "/search", params={"entity": "default", "q": "Shared topic"}
    )

    assert resp.status_code == 200, resp.text
    payload = resp.json()
    result_ids = [row.get("entry_id") for row in payload.get("results", [])]
    assert entry_default in result_ids
    assert all("other-entity" not in (row.get("entry", {}).get("key", {}).get("namespace", "") or "") for row in payload.get("results", []))


def test_search_accepts_compatibility_flags(search_client):
    entry_id = _write_entry(
        search_client,
        text="Compatibility parameters should not block search",
        namespace="default",
    )

    resp = search_client.get(
        "/search",
        params={
            "entity": "default",
            "q": "Compatibility",
            "fuzzy": False,
            "semantic_weight": 0.9,
            "delta": 5,
        },
    )
    assert resp.status_code == 200, resp.text

    payload = resp.json()
    assert payload.get("query") == "Compatibility"
    assert payload.get("mode") == "any"
    assert any(row.get("entry_id") == entry_id for row in payload.get("results", []))
