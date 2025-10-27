"""Append-only event log + RocksDB indices
Events: (entity_id, prime, delta_k, timestamp)."""
import json
import os
import tempfile
import time
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from checksum import merkle_root
from .flow_rule_bridge import validate_prime_sequence
from core.storage import open_db as open_rocksdb, rocksdb_available

DATA_ROOT = Path(os.getenv("LEDGER_DATA_PATH", "./data"))
EVENT_LOG = os.getenv("EVENT_LOG_PATH", str(DATA_ROOT / "event.log"))
FACTORS_DB = os.getenv("FACTORS_DB_PATH", str(DATA_ROOT / "factors"))
POSTINGS_DB = os.getenv("POSTINGS_DB_PATH", str(DATA_ROOT / "postings"))
PRIME_ARRAY: Tuple[int, ...] = (2, 3, 5, 7, 11, 13, 17, 19)
QP_PREFIX = b"qp:"


HAS_ROCKS = rocksdb_available()


class _InMemoryIterator:
    def __init__(self, data: Dict[bytes, bytes]):
        self._data = data
        self._keys = sorted(data.keys())
        self._index = 0

    def seek(self, prefix: bytes) -> None:
        self._index = 0
        while self._index < len(self._keys) and self._keys[self._index] < prefix:
            self._index += 1

    def __iter__(self):
        return self

    def __next__(self):
        if self._index >= len(self._keys):
            raise StopIteration
        key = self._keys[self._index]
        self._index += 1
        return key, self._data[key]


class _InMemoryDB:
    def __init__(self) -> None:
        self._data: Dict[bytes, bytes] = {}

    def get(self, key: bytes):  # type: ignore[override]
        return self._data.get(key)

    def put(self, key: bytes, value: bytes) -> None:
        self._data[key] = value

    def iteritems(self) -> _InMemoryIterator:
        return _InMemoryIterator(self._data)

    def items(self):
        return self._data.items()


def _open_db(path: str):
    if HAS_ROCKS:
        db_path = Path(path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            return open_rocksdb(db_path)
        except Exception:  # pragma: no cover - fallback when RocksDB unavailable
            return _InMemoryDB()
    return _InMemoryDB()


def _iter_prefix(db, prefix: bytes):
    if HAS_ROCKS:
        for key, value in db.items():
            key_bytes = key.encode() if isinstance(key, str) else key
            if key_bytes.startswith(prefix):
                yield key_bytes, value
    else:
        it = db.iteritems()
        it.seek(prefix)
        for k, v in it:
            if not k.startswith(prefix):
                break
            yield k, v


class Ledger:
    def __init__(self):
        self.fdb = _open_db(FACTORS_DB)
        self.pdb = _open_db(POSTINGS_DB)
        self.log = self._open_event_log()

    @staticmethod
    def _qp_key(key: bytes) -> bytes:
        return QP_PREFIX + key

    def qp_put(self, key: bytes, value: str) -> None:
        """Store a value in the Qp namespace."""
        self.fdb.put(self._qp_key(key), value.encode())

    def qp_get(self, key: bytes) -> str | None:
        """Retrieve a value from the Qp namespace."""
        val = self.fdb.get(self._qp_key(key))
        return val.decode() if val else None

    @staticmethod
    def _open_event_log():
        log_path = Path(EVENT_LOG)
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            return open(log_path, "ab", buffering=0)
        except (OSError, ValueError):  # pragma: no cover - fallback path
            fallback = Path(tempfile.gettempdir()) / "dualsubstrate" / "event.log"
            fallback.parent.mkdir(parents=True, exist_ok=True)
            return open(fallback, "ab", buffering=0)

    def anchor(self, entity: str, factors: List[Tuple[int,int]]):
        ts = int(time.time()*1000)
        check = validate_prime_sequence([p for p, _ in factors]) if factors else None
        via_flags = check.via_centroid if check else []
        for idx, (p, dk) in enumerate(factors):
            # 1) append event
            via_c = via_flags[idx] if idx < len(via_flags) else False
            evt = json.dumps({"e": entity, "p": p, "d": dk, "ts": ts, "via_c": via_c})
            self.log.write((evt+"\n").encode())
            # 2) update entity→factors
            old = self._get_factor(entity, p)
            new = old + dk
            self.fdb.put(f"{entity}:{p}".encode(), str(new).encode())
            # 3) update prime→postings
            self.pdb.put(f"{p}:{entity}".encode(), str(new).encode())

    def _get_factor(self, entity: str, p: int) -> int:
        v = self.fdb.get(f"{entity}:{p}".encode())
        if v is None:
            return 0
        if isinstance(v, bytes):
            return int(v.decode())
        return int(v)

    def factors(self, entity: str) -> List[Tuple[int, int]]:
        """Return the eight-prime exponent vector for ``entity``."""
        return [(p, self._get_factor(entity, p)) for p in PRIME_ARRAY]

    def anchor_batch(self, entity: str, commands: List[Tuple[int, int]]):
        """Set absolute exponents for ``entity`` via batch update."""
        deltas: List[Tuple[int, int]] = []
        for prime, target in commands:
            current = self._get_factor(entity, prime)
            delta = int(target) - current
            if delta != 0:
                deltas.append((prime, delta))
        if deltas:
            self.anchor(entity, deltas)

    def query(self, primes: List[int]) -> List[Tuple[str,int]]:
        """return (entity, weight) pairs that divide ALL primes"""
        from functools import reduce
        sets = []
        for p in primes:
            ents = []
            prefix = f"{p}:".encode()
            for k, v in _iter_prefix(self.pdb, prefix):
                ent = k.decode().split(":")[1]
                value = v.decode() if isinstance(v, bytes) else str(v)
                ents.append((ent, int(value)))
            sets.append(dict(ents))
        # intersect
        common = reduce(lambda a,b: a.keys() & b.keys(), sets)
        out = []
        for e in common:
            w = min(sets[i][e] for i in range(len(primes)))
            out.append((e, w))
        return out

    def checksum(self, entity: str) -> str:
        prefix = f"{entity}:".encode()
        leaves = []
        for k, v in _iter_prefix(self.fdb, prefix):
            value = v if isinstance(v, bytes) else str(v).encode()
            leaves.append(k + value)
        return merkle_root(leaves)


_INMEM_APPEND_LOG: List[Tuple[str, int, bytes, bytes, Dict[str, str]]] = []


def append_ledger(
    *,
    entity: str,
    r: bytes,
    p: bytes,
    ts: int | None = None,
    meta: Dict[str, str] | None = None,
    idem_key: str | None = None,
) -> Tuple[int, str]:
    """Lightweight append helper used by the gRPC facade."""

    timestamp = int(ts) if ts is not None else int(time.time() * 1000)
    commit_id = idem_key or f"{entity}/{timestamp}"
    _INMEM_APPEND_LOG.append((entity, timestamp, bytes(r), bytes(p), dict(meta or {})))
    return timestamp, commit_id


def scan_p_prefix(
    *, prefix: bytes, limit: int = 100, reverse: bool = False
) -> Iterable[Tuple[str, int, bytes, bytes]]:
    """Iterate over in-memory append log entries filtered by ``prefix``."""

    if limit <= 0:
        return []

    filtered = [row for row in _INMEM_APPEND_LOG if row[3].startswith(prefix)] if prefix else list(_INMEM_APPEND_LOG)
    filtered.sort(key=lambda row: row[1], reverse=reverse)
    for entity, ts, r_val, p_val, _ in filtered[:limit]:
        yield (entity, ts, r_val, p_val)
