def test_qp_put_and_get(client):
    """
    Test storing and retrieving a value from the Qp database via the REST API.
    """
    key_hex = "8fc7d0d1add48e145f0430dfc381196c"
    value = "hello ledger"
    response = client.post(f"/qp/{key_hex}", json={"value": value}, headers={"x-api-key": "mvp-secret"})
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

    response = client.get(f"/qp/{key_hex}", headers={"x-api-key": "mvp-secret"})
    assert response.status_code == 200
    assert response.json() == {"key": key_hex, "value": value}


def test_qp_get_not_found(client):
    """
    Test that the API returns a 404 error when a key is not found in the Qp database.
    """
    key_hex = "deadbeefdeadbeefdeadbeefdeadbeef"
    response = client.get(f"/qp/{key_hex}", headers={"x-api-key": "mvp-secret"})
    assert response.status_code == 404


def test_qp_invalid_key(client):
    """
    Test that the API returns a 422 error when an invalid key is provided.
    """
    key_hex = "invalid"
    response = client.get(f"/qp/{key_hex}", headers={"x-api-key": "mvp-secret"})
    assert response.status_code == 422
