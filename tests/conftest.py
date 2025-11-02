import os
import tempfile
import shutil
import pytest
from starlette.testclient import TestClient
from backend.main import app as real_app

# static test secrets
os.environ["DUALSUBSTRATE_API_KEY"] = "mvp-secret"
os.environ["FASTAPI_ROOT"] = "http://localhost:8080"
os.environ["TESTING"] = "True"

@pytest.fixture(scope="function")
def temp_db():
    """Unique empty directory per test; deleted afterwards."""
    tmp = tempfile.mkdtemp(prefix="pytest_ledger_")
    # tell the app to use this directory
    os.environ["DB_PATH"] = tmp
    yield tmp
    # cleanup
    shutil.rmtree(tmp, ignore_errors=True)
    os.environ.pop("DB_PATH", None)   # remove so next test starts clean

@pytest.fixture(scope="function")
def client(temp_db):
    """
    Single TestClient whose app opens/closes RocksDB
    inside the same context – guarantees isolation.
    """
    with TestClient(real_app) as c:
        yield c