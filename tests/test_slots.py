import json

import pytest


API_KEY = "mvp-secret"
HEADERS = {"x-api-key": API_KEY, "X-Ledger-ID": "spec-ledger"}


def _create_ledger(client):
    resp = client.post("/admin/ledgers", headers={"x-api-key": API_KEY}, json={"ledger_id": "spec-ledger"})
    assert resp.status_code == 200


def test_upsert_s1_body_and_fetch(client):
    _create_ledger(client)
    entity = "berigny-1863"

    s1_payload = {
        "2": {"what_new": "definition vs conception", "write_primes": [23, 29]},
        "3": {"title": "Light for the Million", "write_primes": [23]},
    }
    resp = client.put(f"/ledger/s1?entity={entity}", headers=HEADERS, json=s1_payload)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["slots"]["S1"]["2"]["write_primes"] == [23, 29]

    body_payload = {"content_type": "text/plain", "text": "Sample body content."}
    resp = client.put(f"/ledger/body?entity={entity}&prime=23", headers=HEADERS, json=body_payload)
    assert resp.status_code == 200
    data = resp.json()
    assert "23" in data["slots"]["body"]
    assert data["slots"]["body"]["23"]["hash"].startswith("sha256:")

    resp = client.get(f"/ledger?entity={entity}", headers=HEADERS)
    assert resp.status_code == 200
    doc = resp.json()
    assert doc["slots"]["S1"]["3"]["title"] == "Light for the Million"
    assert "factors" in doc


def test_invalid_body_prime_rejected(client):
    _create_ledger(client)
    entity = "body-test"

    payload = {"content_type": "text/plain", "text": "Body"}
    resp = client.put(f"/ledger/body?entity={entity}&prime=19", headers=HEADERS, json=payload)
    assert resp.status_code == 422


def test_invalid_s2_key(client):
    _create_ledger(client)
    entity = "s2-test"

    resp = client.put(
        f"/ledger/s2?entity={entity}",
        headers=HEADERS,
        json={"12": {"summary": "invalid prime"}},
    )
    assert resp.status_code == 422


def test_anchor_populates_body_slots_from_write_primes(client):
    _create_ledger(client)
    entity = "anchor-body"

    s1_payload = {
        "2": {"headline": "Aurora beacon", "write_primes": [23, 29]},
        "3": {"summary": "Aurora insight", "write_primes": [31]},
    }

    resp = client.put(
        f"/ledger/s1?entity={entity}",
        headers=HEADERS,
        json=s1_payload,
    )
    assert resp.status_code == 200, resp.text

    anchor_payload = {
        "entity": entity,
        "factors": [{"prime": 2, "delta": 1}],
        "text": "Aurora will anchor body slots.",
    }

    resp = client.post("/anchor", headers=HEADERS, json=anchor_payload)
    assert resp.status_code == 200, resp.text

    resp = client.get(f"/ledger?entity={entity}", headers=HEADERS)
    assert resp.status_code == 200
    doc = resp.json()
    body_slots = doc["slots"]["body"]
    for prime in ("23", "29", "31"):
        assert prime in body_slots
        assert body_slots[prime]["text"] == anchor_payload["text"]

    fallback_entity = "anchor-fallback"
    fallback_payload = {
        "entity": fallback_entity,
        "factors": [{"prime": 2, "delta": 1}],
        "text": "Fallback body prime.",
    }

    resp = client.post("/anchor", headers=HEADERS, json=fallback_payload)
    assert resp.status_code == 200, resp.text

    resp = client.get(f"/ledger?entity={fallback_entity}", headers=HEADERS)
    assert resp.status_code == 200
    fallback_doc = resp.json()
    fallback_body = fallback_doc["slots"]["body"]
    assert fallback_body["23"]["text"] == fallback_payload["text"]
