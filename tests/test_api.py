import math
import textwrap

import pytest

from core.ledger import PRIME_ARRAY


def test_health_check(client):
    """
    Test the health check endpoint.
    """
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_anchor_updates_inference_lane(client):
    payload = {
        "entity": "demo",
        "factors": [{"prime": PRIME_ARRAY[0], "delta": 2}],
    }
    response = client.post(
        "/anchor",
        headers={"Authorization": "Bearer mvp-secret"},
        json=payload,
    )
    assert response.status_code == 200

    state_response = client.get(
        "/inference/state",
        headers={"Authorization": "Bearer mvp-secret"},
        params={"entity": "demo"},
    )
    assert state_response.status_code == 200

    body = state_response.json()
    assert len(body.get("x", [])) == len(PRIME_ARRAY)
    norm = math.sqrt(sum(component * component for component in body["x"]))
    assert norm == pytest.approx(1.0)

    key = str(PRIME_ARRAY[0])
    assert key in body["R"]
    first_row = body["R"][key]
    row_norm = math.sqrt(sum(component * component for component in first_row))
    assert row_norm == pytest.approx(1.0)


def test_anchor_cycle_report(client):
    payload = {
        "entity": "cycle-demo",
        "factors": [
            {"prime": PRIME_ARRAY[0], "delta": 1},
            {"prime": PRIME_ARRAY[1], "delta": 1},
        ],
    }
    response = client.post(
        "/anchor",
        headers={"Authorization": "Bearer mvp-secret"},
        json=payload,
    )
    assert response.status_code == 200

    body = response.json()
    cycle = body.get("cycle")
    assert cycle is not None
    assert len(cycle.get("steps", [])) == 2
    assert cycle["flips"] >= 1
    last_step = cycle["steps"][1]
    assert last_step["prime"] == PRIME_ARRAY[1]
    assert last_step["permutation"] == "swap_pair"
    assert last_step["rotor"] == "quarter_turn"


def test_anchor_persists_full_memory_text(client):
    text = textwrap.dedent(
        """
        Light for the million – Religion of God and not of man.
        Philosophy demands honest doubt before belief can carry authority.
        Knowledge gathered by experiment restores harmony; superstition corrodes it.
        """
    ).strip()
    payload = {
        "entity": "berigny",
        "factors": [{"prime": PRIME_ARRAY[0], "delta": 3}],
        "text": text,
    }

    response = client.post(
        "/anchor",
        headers={"Authorization": "Bearer mvp-secret"},
        json=payload,
    )
    assert response.status_code == 200

    mem_response = client.get(
        "/memories",
        headers={"Authorization": "Bearer mvp-secret"},
        params={"entity": "berigny", "limit": 1},
    )
    assert mem_response.status_code == 200
    history = mem_response.json()
    assert history, "expected the transcript to be persisted"

    latest = history[0]
    assert latest["text"] == text
    assert latest["timestamp"] > 0
    assert latest["prime_annotations"][0]["prime"] == PRIME_ARRAY[0]


def test_retrieve_prefers_persisted_body(client):
    entity = "ledger-backed"
    memory_text = "memory fallback"
    body_text = "ledger body content"

    mem_response = client.post(
        "/memories",
        headers={"Authorization": "Bearer mvp-secret"},
        json={
            "entity": entity,
            "factors": [{"prime": PRIME_ARRAY[0], "delta": 1}],
            "text": memory_text,
        },
    )
    assert mem_response.status_code == 200

    body_response = client.put(
        "/ledger/body",
        headers={"Authorization": "Bearer mvp-secret"},
        params={"entity": entity, "prime": 29},
        json={"text": body_text},
    )
    assert body_response.status_code == 200

    retrieve_response = client.get(
        "/retrieve",
        headers={"Authorization": "Bearer mvp-secret"},
        params={"entity": entity},
    )
    assert retrieve_response.status_code == 200
    payload = retrieve_response.json()
    assert payload == {"entity": entity, "text": body_text}


def test_traverse_get_returns_paths(client):
    entity = "traverse-demo"
    payload = {
        "entity": entity,
        "factors": [{"prime": PRIME_ARRAY[0], "delta": 2}],
    }

    anchor_response = client.post(
        "/anchor",
        headers={"Authorization": "Bearer mvp-secret"},
        json=payload,
    )
    assert anchor_response.status_code == 200

    response = client.get(
        "/traverse",
        headers={"Authorization": "Bearer mvp-secret"},
        params={"entity": entity, "include_metadata": True, "limit": 4},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["supported"] is True
    assert body["origin"] == PRIME_ARRAY[0]
    assert isinstance(body.get("metadata"), dict)
    paths = body.get("paths", [])
    assert paths, "Expected at least one traversal path"
    first = paths[0]
    assert first["nodes"], "expected traversal nodes list"
    assert first["nodes"][0] == body["origin"]
    assert 0.0 <= first["weight"] <= 1.0
    assert first["metadata"]["prime"] == PRIME_ARRAY[0]
    assert first["metadata"]["direction"] == "forward"


def test_inference_state_with_history(client):
    entity = "history-demo"
    for delta in (1, 2):
        response = client.post(
            "/anchor",
            headers={"Authorization": "Bearer mvp-secret"},
            json={
                "entity": entity,
                "factors": [{"prime": PRIME_ARRAY[0], "delta": delta}],
            },
        )
        assert response.status_code == 200

    response = client.get(
        "/inference/state",
        headers={"Authorization": "Bearer mvp-secret"},
        params={"entity": entity, "include_history": True, "limit": 2},
    )

    assert response.status_code == 200
    payload = response.json()
    history = payload.get("history")
    assert isinstance(history, list)
    assert 1 <= len(history) <= 2
    assert history[0]["e"] == entity


def test_inference_state_supports_header_fallback(client):
    entity = "header-fallback"
    headers = {"Authorization": "Bearer mvp-secret", "X-Ledger-ID": entity}
    response = client.post(
        "/anchor",
        headers=headers,
        json={
            "entity": entity,
            "factors": [{"prime": PRIME_ARRAY[0], "delta": 1}],
        },
    )
    assert response.status_code == 200

    state_response = client.get("/inference/state", headers=headers)
    assert state_response.status_code == 200
    payload = state_response.json()
    assert isinstance(payload.get("x"), list)


def test_search_allows_entity_parameter(client):
    entity = "search-demo"
    body_payload = {
        "entity": entity,
        "prime": 29,
        "body": "Meeting recap for entity search-demo",
        "metadata": {"kind": "note"},
    }

    put_response = client.put(
        "/ledger/body",
        headers={"Authorization": "Bearer mvp-secret"},
        json=body_payload,
    )
    assert put_response.status_code == 200

    response = client.get(
        "/search",
        headers={"Authorization": "Bearer mvp-secret"},
        params={"entity": entity, "q": "meeting", "mode": "body", "limit": 5},
    )

    assert response.status_code == 200
    data = response.json()
    assert data.get("entity") == entity
    results = data.get("results", [])
    assert any(row.get("entity") == entity for row in results)


def test_search_supports_recall_mode(client):
    entity = "recall-demo"
    body_payload = {
        "entity": entity,
        "prime": 31,
        "body": "Recall specific meeting summary",
        "metadata": {"kind": "memory"},
    }

    put_response = client.put(
        "/ledger/body",
        headers={"Authorization": "Bearer mvp-secret"},
        json=body_payload,
    )
    assert put_response.status_code == 200

    response = client.get(
        "/search",
        headers={"Authorization": "Bearer mvp-secret"},
        params={"entity": entity, "q": "meeting", "mode": "recall", "limit": 3},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload.get("entity") == entity
    hits = payload.get("results", [])
    assert any(row.get("entity") == entity for row in hits)
