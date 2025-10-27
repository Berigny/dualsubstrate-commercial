"""RocksDB-backed storage primitives for the dual-substrate ledger."""

from __future__ import annotations

import json
import os
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple, Any, Callable

try:  # pragma: no cover - import guard
    import rocksdict as _rocksdict  # type: ignore[import]
except ImportError:  # pragma: no cover - optional dependency
    _rocksdict = None  # type: ignore[assignment]

AccessType = getattr(_rocksdict, "AccessType", None)
Options = getattr(_rocksdict, "Options", None)
Rdict = getattr(_rocksdict, "Rdict", None)
MergeOperator = getattr(_rocksdict, "MergeOperator", None)
WriteBatch = getattr(_rocksdict, "WriteBatch", None)


ColumnFamily = str
Key = bytes
Value = bytes


DEFAULT_COLUMN_FAMILIES: Tuple[ColumnFamily, ...] = (
    "default",
    "meta",
    "R",
    "Qp",
    "bridge",
    "index",
    "ethics",
)


def rocksdb_available() -> bool:
    """Return True when the optional ``rocksdict`` dependency is importable."""

    return Rdict is not None


@dataclass(frozen=True)
class LedgerKeys:
    """Convenience helpers for namespaced ledger key prefixes."""

    entity: str
    timestamp: int

    def timestamp_bytes(self) -> bytes:
        return to_big_endian_timestamp(self.timestamp)

    def r(self) -> Key:
        return compose_key(b"r", self.entity, self.timestamp_bytes())

    def qp(self) -> Key:
        return compose_key(b"p", self.entity, self.timestamp_bytes())

    def bridge(self) -> Key:
        return compose_key(b"b", self.entity, self.timestamp_bytes())

    def index_prefix(self, prefix: bytes) -> Key:
        return compose_index_key(b"ix", b"prefix", prefix, self.entity, self.timestamp_bytes())

    def index_hash(self, r_hash: bytes) -> Key:
        return compose_index_key(b"ix", b"hash", r_hash, self.entity, self.timestamp_bytes())

    def ethics(self) -> Key:
        return compose_key(b"e", self.entity)


class RocksLedgerStorage:
    """Thin convenience wrapper around ``Rdict`` for ledger operations."""

    def __init__(self, db: "Rdict") -> None:
        if not rocksdb_available():  # pragma: no cover - defensive guard
            raise RuntimeError("rocksdict must be installed to use RocksLedgerStorage")

        self._db = db

    @property
    def db(self) -> "Rdict":
        return self._db

    def put_ledger_entry(
        self,
        entity: str,
        timestamp: int,
        r_payload: bytes,
        qp_payload: bytes,
        bridge_payload: bytes,
        p_prefix: bytes,
        r_hash: bytes,
        ethics_delta: bytes,
    ) -> None:
        """Atomically persist a ledger entry to all column families."""

        keys = LedgerKeys(entity=entity, timestamp=timestamp)
        if hasattr(self._db, "write_batch"):
            wb = self._db.write_batch()
            wb.put(("R", keys.r()), r_payload)
            wb.put(("Qp", keys.qp()), qp_payload)
            wb.put(("bridge", keys.bridge()), bridge_payload)
            wb.put(("index", keys.index_prefix(p_prefix)), b"")
            wb.put(("index", keys.index_hash(r_hash)), b"")
            if hasattr(wb, "merge"):
                wb.merge(("ethics", keys.ethics()), ethics_delta)
            self._db.write(wb)
            return

        # Fallback path when write batches are unavailable
        self._db.put(keys.r(), r_payload)
        self._db.put(keys.qp(), qp_payload)
        self._db.put(keys.bridge(), bridge_payload)
        self._db.put(keys.index_prefix(p_prefix), b"")
        self._db.put(keys.index_hash(r_hash), b"")
        self._db.put(keys.ethics(), ethics_delta)


def to_big_endian_timestamp(timestamp: int) -> bytes:
    """Encode ``timestamp`` as an unsigned 64-bit big-endian integer."""

    if timestamp < 0:
        raise ValueError("timestamp must be non-negative")
    return struct.pack(">Q", timestamp)


def compose_key(prefix: bytes, entity: str, timestamp: Optional[bytes] = None) -> Key:
    parts = [prefix, b"/", entity.encode("utf-8")]
    if timestamp is not None:
        parts.extend([b"/", timestamp])
    return b"".join(parts)


def compose_index_key(
    ix_prefix: bytes,
    ix_type: bytes,
    qualifier: bytes,
    entity: str,
    timestamp: bytes,
) -> Key:
    return b"".join(
        [
            ix_prefix,
            b"/",
            ix_type,
            b"/",
            qualifier,
            b"/",
            entity.encode("utf-8"),
            b"/",
            timestamp,
        ]
    )


def _merge_ethics(existing_value: Optional[bytes], operands: Iterable[bytes]) -> bytes:
    """Merge operator that keeps cumulative credits/debits and the max timestamp."""

    state = _decode_ethics(existing_value)
    for operand in operands:
        delta = _decode_ethics(operand)
        state["credits"] += delta.get("credits", 0)
        state["debits"] += delta.get("debits", 0)
        state["last_ts"] = max(state["last_ts"], delta.get("last_ts", 0))
    return _encode_ethics(state)


def _decode_ethics(value: Optional[bytes]) -> dict:
    if not value:
        return {"credits": 0, "debits": 0, "last_ts": 0}
    if isinstance(value, (bytes, bytearray)):
        return json.loads(value.decode("utf-8"))
    return dict(value)


def _encode_ethics(value: dict) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")


def open_rocksdb(
    path: os.PathLike[str] | str,
    *,
    column_families: Iterable[ColumnFamily] | None = None,
    create_if_missing: bool = True,
    prefix_extractor: Optional[str] = None,
) -> RocksLedgerStorage:
    """Open a RocksDB instance configured for the ledger schema."""

    if not rocksdb_available():  # pragma: no cover - dependency guard
        raise RuntimeError(
            "rocksdict is required to open RocksDB storage. "
            "Install it with `pip install rocksdict`."
        )

    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    options = Options() if Options is not None else None
    if options is not None and hasattr(options, "create_if_missing"):
        options.create_if_missing(create_if_missing)
    if options is not None and prefix_extractor and hasattr(options, "set_prefix_extractor"):
        options.set_prefix_extractor(prefix_extractor)

    kwargs: dict[str, Any] = {}
    if options is not None:
        kwargs["options"] = options

    db = Rdict(str(db_path), **kwargs)
    if MergeOperator is not None and hasattr(db, "set_merge_operator"):
        db.set_merge_operator("ethics", MergeOperator(merge_fn=_merge_ethics))
    return RocksLedgerStorage(db)


def open_db(
    path: os.PathLike[str] | str = "./data/ledger",
    *,
    column_families: Iterable[ColumnFamily] | None = None,
    create_if_missing: bool = True,
    prefix_extractor: Optional[str] = None,
):
    """Convenience wrapper that returns the raw ``Rdict`` handle for local use."""

    storage = open_rocksdb(
        path,
        column_families=column_families,
        create_if_missing=create_if_missing,
        prefix_extractor=prefix_extractor,
    )
    return storage.db


__all__ = [
    "DEFAULT_COLUMN_FAMILIES",
    "LedgerKeys",
    "RocksLedgerStorage",
    "compose_index_key",
    "compose_key",
    "open_db",
    "open_rocksdb",
    "rocksdb_available",
    "to_big_endian_timestamp",
]
