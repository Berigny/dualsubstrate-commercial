import math

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
