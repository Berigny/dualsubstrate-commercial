import pytest
from fastapi.testclient import TestClient
from api.main import app

@pytest.fixture(name="client")
def fixture_client() -> TestClient:
    return TestClient(app)

def test_qp_put_and_get(client: TestClient):
    key = "a" * 32
    value = "test_value"

    # Test PUT
    response_put = client.post(f"/qp/{key}", json={"value": value}, headers={"x-api-key": "mvp-secret"})
    assert response_put.status_code == 200
    assert response_put.json() == {"status": "ok"}

    # Test GET
    response_get = client.get(f"/qp/{key}", headers={"x-api-key": "mvp-secret"})
    assert response_get.status_code == 200
    assert response_get.json() == {"key": key, "value": value}

def test_qp_get_not_found(client: TestClient):
    key = "b" * 32
    response = client.get(f"/qp/{key}", headers={"x-api-key": "mvp-secret"})
    assert response.status_code == 404

def test_qp_invalid_key(client: TestClient):
    key = "invalid_key"
    response = client.get(f"/qp/{key}", headers={"x-api-key": "mvp-secret"})
    assert response.status_code == 422
