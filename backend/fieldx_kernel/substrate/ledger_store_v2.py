"""Persistent ledger storage backed by RocksDB."""

from __future__ import annotations

import json
from datetime import datetime
from threading import RLock
from typing import MutableMapping, Optional

from backend.fieldx_kernel.models import ContinuousState, LedgerEntry, LedgerKey


class LedgerStoreV2:
    """Ledger storage that persists entries in a RocksDB dictionary."""

    def __init__(self, db: MutableMapping[bytes, bytes]):
        self._db = db
        self._lock = RLock()

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

        encoded_key = entry.key.as_path().encode()
        encoded_entry = self._encode(entry)
        with self._lock:
            self._db[encoded_key] = encoded_entry

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
