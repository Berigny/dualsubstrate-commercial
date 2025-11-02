"""RocksDB-backed storage primitives for the dual-substrate ledger."""

from __future__ import annotations

import json
import os
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

try:  # pragma: no cover - import guard
    import rocksdict as _rocksdict  # type: ignore[import]
except ImportError:  # pragma: no cover - optional dependency
    _rocksdict = None  # type: ignore[assignment]

_rocksdb = None
if _rocksdict is None:  # pragma: no cover - optional dependency
    try:
        import rocksdb as _rocksdb  # type: ignore[import]
    except ImportError:
        _rocksdb = None  # type: ignore[assignment]

_BACKEND: str
if _rocksdict is not None:
    _BACKEND = "rocksdict"
elif _rocksdb is not None:
    _BACKEND = "python-rocksdb"
else:
    _BACKEND = "none"


if _BACKEND == "rocksdict":  # pragma: no branch - explicit aliasing
    AccessType = getattr(_rocksdict, "AccessType", None)
    Options = getattr(_rocksdict, "Options", None)
    Rdict = getattr(_rocksdict, "Rdict", None)
    MergeOperator = getattr(_rocksdict, "MergeOperator", None)
    WriteBatch = getattr(_rocksdict, "WriteBatch", None)
else:
    AccessType = None

    class Options:  # pragma: no cover - thin adapter
        """Compatibility shim that mirrors ``rocksdict.Options``."""

        def __init__(self) -> None:
            self._create_if_missing: bool = True
            self._prefix_spec: Optional[str] = None

        def create_if_missing(self, flag: bool) -> None:
            self._create_if_missing = bool(flag)

        def set_prefix_extractor(self, spec: str) -> None:
            self._prefix_spec = spec

        def build(self):
            opts = _rocksdb.Options()
            opts.create_if_missing = self._create_if_missing
            prefix_len = _parse_prefix_length(self._prefix_spec)
            if prefix_len is not None and hasattr(_rocksdb, "FixedPrefixTransform"):
                opts.prefix_extractor = _rocksdb.FixedPrefixTransform(prefix_len)
            return opts

    class MergeOperator:  # pragma: no cover - adapter for merge semantics
        """Minimal wrapper storing the merge callback for later use."""

        def __init__(self, *, merge_fn: Callable[[Optional[bytes], Iterable[bytes]], bytes], name: str = "merge") -> None:
            self.merge_fn = merge_fn
            self.name = name

    class PyRocksCompatDB:  # pragma: no cover - behaviour exercised via tests
        """Lightweight facade that mimics the subset of ``rocksdict.Rdict`` we rely on."""

        _CF_SEPARATOR = b"\x00"

        def __init__(
            self,
            path: str,
            *,
            options: Options | None = None,
            column_families: Iterable[str] | None = None,
            create_if_missing: bool = True,
            prefix_extractor: Optional[str] = None,
        ) -> None:
            opts = options.build() if isinstance(options, Options) else _rocksdb.Options()
            opts.create_if_missing = getattr(opts, "create_if_missing", True) and create_if_missing
            if prefix_extractor and not isinstance(options, Options):
                prefix_len = _parse_prefix_length(prefix_extractor)
                if prefix_len is not None and hasattr(_rocksdb, "FixedPrefixTransform"):
                    opts.prefix_extractor = _rocksdb.FixedPrefixTransform(prefix_len)
            self._db = _rocksdb.DB(str(path), opts)
            self._merge_operators: Dict[str, Callable[[Optional[bytes], Iterable[bytes]], bytes]] = {}
            self._column_families = tuple(column_families or DEFAULT_COLUMN_FAMILIES)

        # Mapping protocol -------------------------------------------------
        def __setitem__(self, key: bytes, value: bytes) -> None:
            self.put(key, value)

        def __getitem__(self, key: bytes) -> bytes:
            result = self.get(key)
            if result is None:
                raise KeyError(key)
            return result

        # Basic primitives -------------------------------------------------
        def put(self, key: bytes | Tuple[str, bytes], value: bytes) -> None:
            cf, user_key = self._normalise_key(key)
            self._db.put(self._encode_key(cf, user_key), value)

        def get(self, key: bytes | Tuple[str, bytes]) -> Optional[bytes]:
            cf, user_key = self._normalise_key(key)
            return self._db.get(self._encode_key(cf, user_key))

        def delete(self, key: bytes | Tuple[str, bytes]) -> None:
            cf, user_key = self._normalise_key(key)
            self._db.delete(self._encode_key(cf, user_key))

        # Iteration --------------------------------------------------------
        def items(self):  # noqa: D401 - mimic ``dict.items`` signature
            """Yield ``(key, value)`` pairs stored in the database."""

            it = self._db.iteritems()
            it.seek_to_first()
            for raw_key, value in it:
                yield self._decode_key(raw_key), value

        # Merge support ----------------------------------------------------
        def set_merge_operator(self, column_family: str, operator: MergeOperator) -> None:
            self._merge_operators[column_family] = operator.merge_fn

        def merge(self, key: Tuple[str, bytes], value: bytes) -> None:
            cf, user_key = self._normalise_key(key)
            merge_fn = self._merge_operators.get(cf)
            if merge_fn is None:
                raise RuntimeError(f"no merge operator registered for column family '{cf}'")
            existing = self.get((cf, user_key))
            merged = merge_fn(existing, [value])
            self.put((cf, user_key), merged)

        def close(self) -> None:
            del self._db

        # Internal helpers -------------------------------------------------
        def _normalise_key(self, key: bytes | Tuple[str, bytes]) -> Tuple[str, bytes]:
            if isinstance(key, tuple):
                cf, user_key = key
                return cf, user_key
            return "default", key

        def _encode_key(self, column_family: str, user_key: bytes) -> bytes:
            return column_family.encode("utf-8") + self._CF_SEPARATOR + user_key

        def _decode_key(self, raw_key: bytes) -> bytes:
            if self._CF_SEPARATOR in raw_key:
                _, user_key = raw_key.split(self._CF_SEPARATOR, 1)
                return user_key
            return raw_key

    Rdict = PyRocksCompatDB
    MergeOperator = MergeOperator
    WriteBatch = None


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
    """Return True when either ``rocksdict`` or ``python-rocksdb`` is importable."""

    return _BACKEND != "none"


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
            raise RuntimeError("A RocksDB binding must be installed to use RocksLedgerStorage")

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
        existing_ethics = getattr(self._db, "get", lambda *_: None)(keys.ethics())
        merged = _merge_ethics(existing_ethics, [ethics_delta])
        self._db.put(keys.ethics(), merged)


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


def _parse_prefix_length(spec: Optional[str]) -> Optional[int]:
    if not spec:
        return None
    if ":" in spec:
        prefix, _, length = spec.partition(":")
        if prefix in {"fixed", "fixed_prefix"}:
            try:
                return int(length)
            except ValueError:
                return None
    try:
        return int(spec)
    except (TypeError, ValueError):
        return None


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
            "A RocksDB binding is required to open RocksDB storage. "
            "Install it with `pip install rocksdict` or `pip install python-rocksdb`."
        )

    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    options = Options() if Options is not None else None
    if options is not None and hasattr(options, "create_if_missing"):
        options.create_if_missing(create_if_missing)
    if options is not None and prefix_extractor and hasattr(options, "set_prefix_extractor"):
        options.set_prefix_extractor(prefix_extractor)

    if _BACKEND == "rocksdict":
        kwargs: Dict[str, Any] = {}
        if options is not None:
            kwargs["options"] = options
        db = Rdict(str(db_path), **kwargs)
    else:
        db = Rdict(
            str(db_path),
            options=options,
            column_families=tuple(column_families or DEFAULT_COLUMN_FAMILIES),
            create_if_missing=create_if_missing,
            prefix_extractor=prefix_extractor,
        )

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
