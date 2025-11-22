import os
from typing import Iterable, Tuple

import pytest
from starlette.testclient import TestClient

from backend.fieldx_kernel.models import ContinuousState, LedgerEntry, LedgerKey
from backend.fieldx_kernel.substrate.ledger_store_v2 import LedgerStoreV2
from backend.main import app as backend_app
from backend.search.token_index import TokenPrimeIndex


def _write_without_index(app, entries: Iterable[Tuple[str, str]]) -> None:
    """Persist ledger entries without populating the token index."""

    store = LedgerStoreV2(app.state.db, token_index=None)
    for identifier, text in entries:
        store.write(
            LedgerEntry(
                key=LedgerKey(namespace="default", identifier=identifier),
                state=ContinuousState(metadata={"summary": text}),
            )
        )


@pytest.fixture()
def admin_client(tmp_path):
    original_path = os.environ.get("DB_PATH")
    os.environ["DB_PATH"] = str(tmp_path)

    try:
        with TestClient(backend_app) as client:
            yield client
    finally:
        if original_path is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = original_path


def test_admin_reindex_rebuilds_index(admin_client):
    entries = [
        ("first", "Dual substrate index rebuild path"),
        ("second", "Another ledger fragment for rebuild"),
    ]

    _write_without_index(admin_client.app, entries)

    response = admin_client.get("/admin/reindex")
    assert response.status_code == 200, response.text

    payload = response.json()
    assert payload["entries_reindexed"] == len(entries)
    assert payload["tokens_indexed"] >= 2
    assert payload["postings_updated"] >= len(entries)

    token_index = TokenPrimeIndex(admin_client.app)
    store = LedgerStoreV2(admin_client.app.state.db, token_index=token_index)
    for identifier, _ in entries:
        entry_id = f"default:{identifier}"
        entry = store.read(entry_id)
        assert entry is not None

        primes = entry.state.metadata.get("token_primes") or []
        assert primes, f"Missing primes for {entry_id}"

        prime_key = token_index._prime_key(primes[0])
        assert prime_key in admin_client.app.state.db
