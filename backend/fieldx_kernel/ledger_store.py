"""In-memory placeholder for ledger storage operations."""

from __future__ import annotations

from threading import RLock
from typing import Dict, Iterable, List, Optional

from .models import LedgerEntry, LedgerKey


class LedgerStore:
    """Simple ledger registry using a thread-safe dictionary."""

    def __init__(self) -> None:
        self._entries: Dict[LedgerKey, LedgerEntry] = {}
        self._lock = RLock()

    def upsert(self, entry: LedgerEntry) -> None:
        """Insert or update a ledger entry for the provided key."""

        with self._lock:
            self._entries[entry.key] = entry

    def get(self, key: LedgerKey) -> Optional[LedgerEntry]:
        """Retrieve an entry by key if it exists."""

        with self._lock:
            return self._entries.get(key)

    def list_namespace(self, namespace: str) -> List[LedgerEntry]:
        """Return all entries belonging to a namespace."""

        with self._lock:
            return [entry for key, entry in self._entries.items() if key.namespace == namespace]

    def iter_entries(self) -> Iterable[LedgerEntry]:
        """Iterate over a snapshot of current entries."""

        with self._lock:
            return list(self._entries.values())
