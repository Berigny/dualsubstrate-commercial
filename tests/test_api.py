"""Placeholder API tests using FastAPI's TestClient."""

import pytest
from fastapi.testclient import TestClient

from api.main import app


@pytest.fixture(name="client")
def fixture_client() -> TestClient:
    return TestClient(app)


@pytest.mark.skip(reason="MVP placeholder - flesh out API behaviour")
def test_health_endpoint(client: TestClient) -> None:
    response = client.get("/health", headers={"x-api-key": "mvp-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
