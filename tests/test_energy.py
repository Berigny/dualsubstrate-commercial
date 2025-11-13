import pytest

from core.ledger import PRIME_ARRAY
from core.valuation import mixed_energy


def _auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer mvp-secret"}


def test_anchor_reports_energy(client):
    prime = PRIME_ARRAY[0]
    pre_state_resp = client.get(
        "/inference/state",
        headers=_auth_headers(),
        params={"entity": "energy-demo"},
    )
    assert pre_state_resp.status_code == 200
    pre_state = pre_state_resp.json()
    readouts = {int(k): v for k, v in pre_state["R"].items()}
    expected = mixed_energy(
        pre_state["x"],
        readouts,
        [(prime, 2)],
        lambda_weight=0.5,
    )
    response = client.post(
        "/anchor",
        headers=_auth_headers(),
        json={
            "entity": "energy-demo",
            "factors": [{"prime": prime, "delta": 2}],
        },
    )
    assert response.status_code == 200
    body = response.json()
    energy = body.get("energy")
    assert energy is not None
    assert energy["entity"] == "energy-demo"
    assert energy["lambda_weight"] == pytest.approx(0.5)
    assert energy["total"] == pytest.approx(expected.total)
    assert energy["continuous"] == pytest.approx(expected.continuous)
    assert energy["discrete"] == pytest.approx(expected.discrete)
    assert energy["discrete_weighted"] == pytest.approx(expected.weighted_discrete)


def test_metrics_expose_latest_energy(client):
    prime = PRIME_ARRAY[1]
    # first update establishes state
    client.post(
        "/anchor",
        headers=_auth_headers(),
        json={
            "entity": "energy-metrics",
            "factors": [{"prime": prime, "delta": 1}],
        },
    )
    # capture state before the second update to compute expected energy
    pre_state_resp = client.get(
        "/inference/state",
        headers=_auth_headers(),
        params={"entity": "energy-metrics"},
    )
    assert pre_state_resp.status_code == 200
    pre_state = pre_state_resp.json()
    readouts = {int(k): v for k, v in pre_state["R"].items()}
    expected = mixed_energy(
        pre_state["x"],
        readouts,
        [(prime, 1)],
        lambda_weight=0.5,
    )
    # second update exercises different delta size
    response = client.post(
        "/anchor",
        headers=_auth_headers(),
        json={
            "entity": "energy-metrics",
            "factors": [{"prime": prime, "delta": 1}],
        },
    )
    assert response.status_code == 200
    metrics_response = client.get("/metrics", headers=_auth_headers())
    assert metrics_response.status_code == 200
    metrics = metrics_response.json()
    energy = metrics.get("last_anchor_energy")
    assert energy is not None
    assert energy["entity"] == "energy-metrics"
    assert energy["total"] == pytest.approx(expected.total)
    assert energy["continuous"] == pytest.approx(expected.continuous, abs=1e-6)
    assert energy["discrete_weighted"] == pytest.approx(expected.weighted_discrete)
