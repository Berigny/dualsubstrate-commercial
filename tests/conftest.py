import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest
from starlette.testclient import TestClient

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# static test secrets
os.environ["DUALSUBSTRATE_API_KEY"] = "mvp-secret"
os.environ["FASTAPI_ROOT"] = "http://localhost:8080"
os.environ["TESTING"] = "True"
os.environ["API_KEYS"] = "mvp-secret"

from api.main import app as real_app  # noqa: E402  (import after sys.path tweak)
from core import ledger as core_ledger

@pytest.fixture(scope="function")
def temp_db():
    """Unique empty directory per test; deleted afterwards."""
    tmp = tempfile.mkdtemp(prefix="pytest_ledger_")
    # configure core ledger paths to the temp directory
    path_tmp = Path(tmp)
    os.environ["LEDGER_DATA_PATH"] = tmp
    os.environ["EVENT_LOG_PATH"] = str(path_tmp / "event.log")
    os.environ["FACTORS_DB_PATH"] = str(path_tmp / "factors")
    os.environ["POSTINGS_DB_PATH"] = str(path_tmp / "postings")
    os.environ["SLOTS_DB_PATH"] = str(path_tmp / "slots")

    core_ledger.DATA_ROOT = path_tmp
    core_ledger.EVENT_LOG = os.environ["EVENT_LOG_PATH"]
    core_ledger.FACTORS_DB = os.environ["FACTORS_DB_PATH"]
    core_ledger.POSTINGS_DB = os.environ["POSTINGS_DB_PATH"]
    core_ledger.SLOTS_DB = os.environ["SLOTS_DB_PATH"]
    yield tmp
    # cleanup
    shutil.rmtree(tmp, ignore_errors=True)
    os.environ.pop("LEDGER_DATA_PATH", None)
    os.environ.pop("EVENT_LOG_PATH", None)
    os.environ.pop("FACTORS_DB_PATH", None)
    os.environ.pop("POSTINGS_DB_PATH", None)
    os.environ.pop("SLOTS_DB_PATH", None)

@pytest.fixture(scope="function")
def client(temp_db):
    """
    Single TestClient whose app opens/closes RocksDB
    inside the same context – guarantees isolation.
    """
    with TestClient(real_app) as c:
        yield c
