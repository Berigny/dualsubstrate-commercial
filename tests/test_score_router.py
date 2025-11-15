from __future__ import annotations


def test_score_s2_metrics_structure(client):
    payload = {
        "facets": {
            "11": {
                "summary": "Aurora evidence trail",
                "scope": "global",
                "ontology_refs": ["aurora:scope"],
            },
            "13": {
                "owner": "OpsReview",
                "definitions": [
                    {"term": "Aurora", "text": "Natural light display"},
                    {"term": "Salience", "text": "Perceived significance"},
                ],
            },
        }
    }

    response = client.post(
        "/score/s2",
        headers={"Authorization": "Bearer mvp-secret"},
        json=payload,
    )

    assert response.status_code == 200
    body = response.json()
    assert "metrics" in body
    metrics = body["metrics"]

    for key in ("dE", "dDrift", "dRetention", "K", "coverage", "token_total", "facets_scored"):
        assert key in metrics

    assert metrics["coverage"] == len(payload["facets"])
    assert metrics["token_total"] > 0
    assert metrics["dE"] < 0
    assert metrics["dDrift"] < 0
    assert metrics["dRetention"] > 0
    assert metrics["K"] >= 0

    breakdown = metrics["facets_scored"]
    assert set(breakdown.keys()) == set(payload["facets"].keys())
    for entry in breakdown.values():
        assert entry["tokens"] >= 0
        assert entry["fields"] >= 0
        assert "density" in entry
