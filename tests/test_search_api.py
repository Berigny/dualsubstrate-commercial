import uuid

API_KEY = "mvp-secret"


def _create_ledger(client, ledger_id: str):
    resp = client.post(
        "/admin/ledgers",
        headers={"x-api-key": API_KEY},
        json={"ledger_id": ledger_id},
    )
    assert resp.status_code == 200, resp.text


def _seed_search_content(client):
    ledger_id = f"spec-ledger-{uuid.uuid4().hex[:8]}"
    headers = {"x-api-key": API_KEY, "X-Ledger-ID": ledger_id}
    _create_ledger(client, ledger_id)

    # --- S1 slots -----------------------------------------------------
    entity_primary = "s1-alpha"
    entity_secondary = "s1-beta"
    resp = client.put(
        f"/ledger/s1?entity={entity_primary}",
        headers=headers,
        json={"2": {"title": "Aurora aurora beacon", "write_primes": [23]}},
    )
    assert resp.status_code == 200, resp.text
    resp = client.put(
        f"/ledger/s1?entity={entity_secondary}",
        headers=headers,
        json={"5": {"summary": "Aurora insight", "write_primes": [29]}},
    )
    assert resp.status_code == 200, resp.text

    # --- S2 slots -----------------------------------------------------
    s2_primary = "s2-alpha"
    s2_secondary = "s2-beta"
    metrics_payload = {"dE": -1.0, "dDrift": -1.5, "dRetention": 1.1, "K": 0.0}
    for entity in (s2_primary, s2_secondary):
        resp = client.patch(
            f"/ledger/metrics?entity={entity}", headers=headers, json=metrics_payload
        )
        assert resp.status_code == 200, resp.text
    resp = client.put(
        f"/ledger/s2?entity={s2_primary}",
        headers=headers,
        json={"11": {"overview": "Aurora evidence"}},
    )
    assert resp.status_code == 200, resp.text
    resp = client.put(
        f"/ledger/s2?entity={s2_secondary}",
        headers=headers,
        json={"19": {"notes": ["Aurora log"]}},
    )
    assert resp.status_code == 200, resp.text

    # --- body slots ---------------------------------------------------
    body_primary = "body-alpha"
    body_secondary = "body-beta"
    resp = client.put(
        f"/ledger/body?entity={body_primary}&prime=23",
        headers=headers,
        json={"content_type": "text/plain", "text": "Aurora aurora luminous arc"},
    )
    assert resp.status_code == 200, resp.text
    resp = client.put(
        f"/ledger/body?entity={body_secondary}&prime=29",
        headers=headers,
        json={"content_type": "text/plain", "text": "Aurora sketch"},
    )
    assert resp.status_code == 200, resp.text

    return {
        "s1_primary": entity_primary,
        "s1_secondary": entity_secondary,
        "s2_primary": s2_primary,
        "s2_secondary": s2_secondary,
        "body_primary": body_primary,
        "body_secondary": body_secondary,
    }, headers


def test_search_s1_mode_ranking(client):
    entities, headers = _seed_search_content(client)

    resp = client.get(
        "/search",
        headers=headers,
        params={"q": "aurora", "mode": "s1"},
    )
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    results = payload.get("results", [])
    assert len(results) >= 2
    assert results[0]["entity"] == entities["s1_primary"]
    assert results[0]["prime"] == 2
    assert results[0]["score"] >= results[1]["score"]
    assert "aurora" in results[0]["snippet"].lower()
    assert results[1]["entity"] == entities["s1_secondary"]
    assert results[1]["prime"] == 5


def test_search_s2_mode_ranking(client):
    entities, headers = _seed_search_content(client)

    resp = client.get(
        "/search",
        headers=headers,
        params={"q": "aurora", "mode": "s2"},
    )
    assert resp.status_code == 200, resp.text
    results = resp.json().get("results", [])
    assert len(results) >= 2
    assert results[0]["entity"] == entities["s2_primary"]
    assert results[0]["prime"] == 11
    assert results[1]["entity"] == entities["s2_secondary"]
    assert results[1]["prime"] == 19
    scores = [row["score"] for row in results[:2]]
    assert scores == sorted(scores, reverse=True)


def test_search_body_mode_ranking(client):
    entities, headers = _seed_search_content(client)

    resp = client.get(
        "/search",
        headers=headers,
        params={"q": "aurora", "mode": "body"},
    )
    assert resp.status_code == 200, resp.text
    results = resp.json().get("results", [])
    assert len(results) >= 2
    assert results[0]["entity"] == entities["body_primary"]
    assert results[0]["prime"] == 23
    assert results[1]["entity"] == entities["body_secondary"]
    assert results[1]["prime"] == 29
    assert results[0]["score"] > results[1]["score"]


def test_search_all_mode_includes_all_slots(client):
    entities, headers = _seed_search_content(client)

    resp = client.get(
        "/search",
        headers=headers,
        params={"q": "aurora", "mode": "all"},
    )
    assert resp.status_code == 200, resp.text
    results = resp.json().get("results", [])
    assert len(results) >= 4
    entities_seen = {row["entity"] for row in results}
    assert entities["s1_primary"] in entities_seen
    assert entities["s2_primary"] in entities_seen
    assert entities["body_primary"] in entities_seen
    scores = [row["score"] for row in results[:5]]
    assert scores == sorted(scores, reverse=True)


def test_search_rejects_unknown_mode(client):
    _, headers = _seed_search_content(client)

    resp = client.get(
        "/search",
        headers=headers,
        params={"q": "aurora", "mode": "unknown"},
    )
    assert resp.status_code == 422
