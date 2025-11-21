"""Persistent ledger storage backed by RocksDB."""

from __future__ import annotations

import json
import math
import re
from datetime import datetime
from threading import RLock
from typing import Any, Iterable, Mapping, MutableMapping, Optional

from backend.fieldx_kernel.models import ContinuousState, LedgerEntry, LedgerKey
from backend.search.token_index import TokenPrimeIndex, normalise_text


def _collect_text_fragments(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for item in value.values():
            yield from _collect_text_fragments(item)
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            yield from _collect_text_fragments(item)


def _full_text_for_entry(entry: LedgerEntry) -> str:
    text = getattr(entry, "text", None)
    if text:
        return str(text)

    body = getattr(entry, "body", None)
    if body is not None:
        return str(body)

    metadata = entry.state.metadata or {}
    fragments = list(_collect_text_fragments(metadata))
    if fragments:
        return " ".join(str(fragment) for fragment in fragments)

    return ""


class LedgerStoreV2:
    """Ledger storage that persists entries in a RocksDB dictionary."""

    def __init__(self, db: MutableMapping[bytes, bytes], token_index: TokenPrimeIndex | None = None):
        self._db = db
        self._lock = RLock()
        self._token_index = token_index

    def _encode(self, entry: LedgerEntry) -> bytes:
        payload = {
            "key": {"namespace": entry.key.namespace, "identifier": entry.key.identifier},
            "state": {
                "coordinates": dict(entry.state.coordinates),
                "phase": entry.state.phase,
                "metadata": dict(entry.state.metadata),
            },
            "created_at": entry.created_at.isoformat(),
            "notes": entry.notes,
        }
        return json.dumps(payload).encode()

    def _decode(self, payload: bytes) -> LedgerEntry:
        data = json.loads(payload)
        key_data = data["key"]
        state_data = data["state"]
        created_at = datetime.fromisoformat(data["created_at"])

        return LedgerEntry(
            key=LedgerKey(namespace=key_data["namespace"], identifier=key_data["identifier"]),
            state=ContinuousState(
                coordinates=dict(state_data.get("coordinates", {})),
                phase=state_data.get("phase"),
                metadata=dict(state_data.get("metadata", {})),
            ),
            created_at=created_at,
            notes=data.get("notes"),
        )

    def write(self, entry: LedgerEntry) -> None:
        """Persist the ledger entry using its path as the key."""

        entry_id = entry.key.as_path()
        full_text = _full_text_for_entry(entry)
        primes = self._index_entry(entry, full_text)

        encoded_key = entry_id.encode()
        encoded_entry = self._encode(entry)
        with self._lock:
            self._db[encoded_key] = encoded_entry
            if primes:
                self._token_index.update_inverted_index(primes, entry_id)

    def read(self, ledger_id: str) -> Optional[LedgerEntry]:
        """Retrieve a ledger entry by its encoded identifier path."""

        with self._lock:
            encoded = self._db.get(ledger_id.encode())
        if encoded is None:
            return None

        return self._decode(encoded)

    # Compatibility helpers for existing callers expecting the v1 API
    def upsert(self, entry: LedgerEntry) -> None:  # pragma: no cover - thin wrapper
        self.write(entry)

    def get(self, key: LedgerKey) -> Optional[LedgerEntry]:  # pragma: no cover - thin wrapper
        return self.read(key.as_path())

    def _index_entry(self, entry: LedgerEntry, full_text: str) -> list[int]:
        metadata = dict(entry.state.metadata)
        metadata["full_text"] = full_text

        if self._token_index is None:
            entry.state.metadata = metadata
            return []

        normalised_text = normalise_text(full_text)
        tokens = re.findall(r"[a-z0-9]+", normalised_text)
        if not tokens:
            entry.state.metadata = metadata
            return []

        unique_tokens = list(dict.fromkeys(tokens))
        primes = self._token_index.primes_for_tokens(unique_tokens)

        metadata["token_primes"] = primes
        metadata["token_prime_product"] = math.prod(primes) if primes else None
        entry.state.metadata = metadata

        return primes
