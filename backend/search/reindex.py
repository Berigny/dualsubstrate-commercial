"""Utilities for rebuilding the token index from existing ledger entries."""

from __future__ import annotations

import logging
import math
from typing import Iterable

from fastapi import FastAPI

from backend.fieldx_kernel.substrate.ledger_store_v2 import (
    LedgerStoreV2,
    _collect_text_fragments,
)
from backend.search.token_index import TokenPrimeIndex, normalise_text


logger = logging.getLogger(__name__)


def _decode_key(raw_key: object) -> str:
    if isinstance(raw_key, (bytes, bytearray)):
        return raw_key.decode()
    return str(raw_key)


def _is_index_key(key: str) -> bool:
    return key.startswith("tp:") or key.startswith("ix:")


def _full_text_from_metadata(metadata: dict | None) -> str:
    if not metadata:
        return ""

    filtered = {
        key: value
        for key, value in metadata.items()
        if key not in {"full_text", "token_primes", "token_prime_product"}
    }
    fragments: Iterable[str] = _collect_text_fragments(filtered)
    return " ".join(str(fragment) for fragment in fragments)


def reindex_all(app: FastAPI, *, entity: str | None = None) -> dict:
    """
    Walk existing ledger entries and rebuild token primes + inverted index.

    Returns a summary dictionary with counts describing the work performed.
    """

    db = getattr(app.state, "db", None)
    if db is None:
        raise RuntimeError("Database not initialized on application state")

    token_index = TokenPrimeIndex(app)
    store = LedgerStoreV2(db, token_index=token_index)

    ledger_rows: list[tuple[str, object]] = []
    index_keys: list[object] = []

    # Snapshot existing rows so we can safely mutate the DB afterwards.
    with store._lock:  # type: ignore[attr-defined]
        for raw_key, raw_value in db.items():
            decoded_key = _decode_key(raw_key)
            if _is_index_key(decoded_key):
                index_keys.append(raw_key)
                continue

            ledger_rows.append((decoded_key, raw_value))

        for idx_key in index_keys:
            db.pop(idx_key, None)

    logger.info("Cleared %s index keys", len(index_keys))

    tokens_seen: set[str] = set()
    postings_written = 0
    entries_reindexed = 0

    for entry_id, raw_value in ledger_rows:
        try:
            entry = store._decode(raw_value)  # type: ignore[attr-defined]
        except Exception:
            logger.exception("Skipping malformed ledger row", extra={"entry_id": entry_id})
            continue

        full_text = _full_text_from_metadata(entry.state.metadata)
        tokens = normalise_text(full_text)
        primes = token_index.primes_for_tokens(tokens) if tokens else []

        metadata = dict(entry.state.metadata)
        metadata["full_text"] = full_text
        metadata["token_primes"] = primes
        metadata["token_prime_product"] = math.prod(primes) if primes else None
        entry.state.metadata = metadata

        encoded_entry = store._encode(entry)  # type: ignore[attr-defined]
        with store._lock:  # type: ignore[attr-defined]
            db[entry.key.as_path().encode()] = encoded_entry

        if primes:
            token_index.update_inverted_index(primes, entry.key.as_path())
            postings_written += len(primes)
        tokens_seen.update(tokens)
        entries_reindexed += 1

    summary = {
        "entity": entity,
        "entries_reindexed": entries_reindexed,
        "tokens_indexed": len(tokens_seen),
        "postings_updated": postings_written,
        "cleared_index_keys": len(index_keys),
    }

    logger.info("Reindex complete", extra=summary)
    return summary


__all__ = ["reindex_all"]
