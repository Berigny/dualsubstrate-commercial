"""Append-only ledger backed by RocksDB."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional


@dataclass
class LedgerEvent:
    payload: bytes
    offset: int


class Ledger:
    """Minimal append-only ledger abstraction with RocksDB integration hooks."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self._db = None  # Placeholder for the RocksDB handle

    def open(self) -> None:
        """Initialise the RocksDB connection."""
        raise NotImplementedError("TODO: wire up RocksDB persistence")

    def append(self, event: bytes) -> LedgerEvent:
        """Append an event to the ledger and return the stored record."""
        raise NotImplementedError("TODO: implement append semantics")

    def stream(self, *, start: Optional[int] = None) -> Iterator[LedgerEvent]:
        """Stream events from the ledger starting from an optional offset."""
        raise NotImplementedError("TODO: implement event streaming")

