import os
import tempfile
import shutil
import pytest
from contextlib import asynccontextmanager
from fastapi import FastAPI
from rocksdict import Rdict

# Set environment variables that are static for all tests
os.environ["DUALSUBSTRATE_API_KEY"] = "mvp-secret"
os.environ["FASTAPI_ROOT"] = "http://localhost:8080"
os.environ["TESTING"] = "True"

@pytest.fixture(scope="function")
def temp_db():
    """
    Create a unique, empty RocksDB directory for a single test function.
    Yields the absolute path and deletes the directory after the test.
    """
    tmp = tempfile.mkdtemp(prefix="test_ledger_")
    # Set the environment variables for the database paths
    os.environ["LEDGER_DATA_PATH"] = tmp
    yield tmp
    shutil.rmtree(tmp, ignore_errors=True)

@pytest.fixture(scope="function")
def app(temp_db):
    """
    Builds a new FastAPI instance for each test function, whose lifespan opens
    a unique Rdict instance on the provided temp_db path, and closes it
    reliably on shutdown.
    """
    from api.main import lifespan as api_lifespan
    from api.main import app as api_app

    test_app = FastAPI(lifespan=api_lifespan)
    test_app.include_router(api_app.router)

    return test_app

@pytest.fixture(scope="function")
def client(app):
    """
    Creates a TestClient for each test function. The app, and therefore the DB,
    is created and torn down within this context, guaranteeing isolation.
    """
    from fastapi.testclient import TestClient
    with TestClient(app) as c:
        yield c
