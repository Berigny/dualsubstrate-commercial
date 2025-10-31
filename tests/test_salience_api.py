from unittest.mock import patch
import pytest

def test_salience_stores_and_exact_returns_payload(client):
    """
    Test that a salient utterance is stored and can be retrieved with all expected fields.
    """
    with patch("api.main.S1Salience.score", return_value=0.9):
        payload = {"utterance": "Remember to call Alice tomorrow", "timestamp": 123.0}
        response = client.post("/salience", json=payload, headers={"x-api-key": "mvp-secret"})

        assert response.status_code == 200
        data = response.json()
        assert data["stored"] is True
        assert len(data["key"]) == 32
        assert data["len"] == len(payload["utterance"])
        assert pytest.approx(data["score"], rel=1e-3) == 0.9
        assert data["threshold"] == 0.7

        exact_response = client.get(f"/exact/{data['key']}", headers={"x-api-key": "mvp-secret"})
        assert exact_response.status_code == 200
        exact_payload = exact_response.json()
        assert exact_payload["text"] == payload["utterance"]
        assert exact_payload["t"] == payload["timestamp"]
        assert exact_payload["score"] == data["score"]

def test_salience_below_threshold(client):
    """
    Test that a non-salient utterance is not stored and returns the correct payload.
    """
    with patch("api.main.S1Salience.score", return_value=0.2):
        payload = {"utterance": "Too dull", "timestamp": 456.0}
        response = client.post("/salience", json=payload, headers={"x-api-key": "mvp-secret"})

        assert response.status_code == 200
        data = response.json()
        assert data == {
            "stored": False,
            "score": 0.2,
            "text": payload["utterance"],
            "threshold": 0.7,
        }

def test_exact_invalid_key(client):
    """
    Test that an invalid key returns a 422 error.
    """
    response = client.get("/exact/not-a-key", headers={"x-api-key": "mvp-secret"})
    assert response.status_code == 422
