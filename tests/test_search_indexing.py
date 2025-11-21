import json
from fastapi import FastAPI

from backend.fieldx_kernel.models import ContinuousState, LedgerEntry, LedgerKey
from backend.fieldx_kernel.substrate.ledger_store_v2 import LedgerStoreV2
from backend.search import service as search_service
from backend.search.token_index import TokenPrimeIndex


def _build_entry(identifier: str, text: str, namespace: str = "default") -> LedgerEntry:
    return LedgerEntry(
        key=LedgerKey(namespace=namespace, identifier=identifier),
        state=ContinuousState(metadata={"summary": text}),
    )


def test_reindex_then_query_returns_results():
    app = FastAPI()
    app.state.db = {}

    # Store entries without a token index to simulate historical data requiring reindexing.
    store_without_index = LedgerStoreV2(app.state.db, token_index=None)
    entry = _build_entry("reindex", "Dual substrate archival fragment")
    store_without_index.write(entry)

    token_index = TokenPrimeIndex(app)
    store_with_index = LedgerStoreV2(app.state.db, token_index=token_index)

    # Reindex the existing entries to populate postings lists.
    for raw_key, raw_entry in list(app.state.db.items()):
        if not isinstance(raw_entry, (bytes, bytearray)):
            continue

        decoded_entry = store_with_index._decode(raw_entry)
        store_with_index.write(decoded_entry)

    results = search_service.search(
        "dual substrate", store=store_with_index, token_index=token_index
    )

    assert any(row.get("entry_id") == "default:reindex" for row in results)


def test_stopwords_excluded_in_all_mode():
    app = FastAPI()
    app.state.db = {}

    token_index = TokenPrimeIndex(app)
    store = LedgerStoreV2(app.state.db, token_index=token_index)

    entry = _build_entry("stopwords", "Dual substrate signal without conjunction")
    store.write(entry)

    results = search_service.search(
        "dual substrate and", store=store, token_index=token_index, mode="all"
    )

    assert any(row.get("entry_id") == "default:stopwords" for row in results)


def test_search_falls_back_all_to_any_then_scan():
    app = FastAPI()
    app.state.db = {}

    token_index = TokenPrimeIndex(app)
    store = LedgerStoreV2(app.state.db, token_index=token_index)

    entry_dual = _build_entry("dual", "Dual channel data")
    entry_substrate = _build_entry("substrate", "Substrate layer only")
    store.write(entry_dual)
    store.write(entry_substrate)

    # No entry contains both tokens so mode="all" should fallback to the union behaviour.
    initial_results = search_service.search(
        "dual substrate", store=store, token_index=token_index, mode="all"
    )
    initial_ids = {row.get("entry_id") for row in initial_results}
    assert initial_ids == {"default:dual", "default:substrate"}

    # Remove postings to force a subsequent fallback to the linear scan.
    for key in list(app.state.db.keys()):
        if isinstance(key, str) and key.startswith("ix:prime:"):
            app.state.db.pop(key)

    scan_results = search_service.search(
        "dual substrate", store=store, token_index=token_index, mode="all"
    )
    scan_ids = {row.get("entry_id") for row in scan_results}
    assert scan_ids == {"default:dual", "default:substrate"}


def test_postings_lists_accumulate_entry_ids():
    app = FastAPI()
    app.state.db = {}

    token_index = TokenPrimeIndex(app)
    prime = token_index.get_or_assign_prime("dual")
    token_index.update_inverted_index([prime], "ns:first")
    token_index.update_inverted_index([prime], "ns:second")

    raw_postings = app.state.db[token_index._prime_key(prime)]
    postings = set(json.loads(raw_postings))

    assert postings == {"ns:first", "ns:second"}


def test_token_prime_mapping_persists_across_instances():
    app = FastAPI()
    app.state.db = {}

    first_index = TokenPrimeIndex(app)
    prime_dual = first_index.get_or_assign_prime("Dual")
    prime_substrate = first_index.get_or_assign_prime("substrate")

    second_app = FastAPI()
    second_app.state.db = app.state.db
    second_index = TokenPrimeIndex(second_app)

    assert second_index.get_or_assign_prime("dual") == prime_dual
    assert second_index.get_or_assign_prime("substrate") == prime_substrate

    next_prime = second_index.get_or_assign_prime("fresh-token")
    assert next_prime not in {prime_dual, prime_substrate}
