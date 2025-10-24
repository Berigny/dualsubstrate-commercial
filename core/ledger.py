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

try:  # pragma: no cover - optional dependency
    import rocksdb  # type: ignore
except ImportError:  # pragma: no cover - fallback to in-memory shim
    rocksdb = None

EVENT_LOG = os.getenv("EVENT_LOG_PATH", "/data/event.log")
FACTORS_DB = "/data/factors"
POSTINGS_DB = "/data/postings"
PRIME_ARRAY: Tuple[int, ...] = (2, 3, 5, 7, 11, 13, 17, 19)

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


class _InMemoryWriteBatch:
    def __init__(self, target: "_InMemoryDB") -> None:
        self._target = target
        self._puts: List[Tuple[bytes, bytes]] = []

    def put(self, key: bytes, value: bytes) -> None:
        self._puts.append((key, value))

    def apply(self) -> None:
        for key, value in self._puts:
            self._target.put(key, value)


class _InMemoryDB:
    def __init__(self) -> None:
        self._data: Dict[bytes, bytes] = {}

    def get(self, key: bytes):  # type: ignore[override]
        return self._data.get(key)

    def put(self, key: bytes, value: bytes) -> None:
        self._data[key] = value

    def iteritems(self) -> _InMemoryIterator:
        return _InMemoryIterator(self._data)


def _open_db(path, cf_names):
    if rocksdb is None:
        return _InMemoryDB()
    opts = rocksdb.Options(create_if_missing=True)
    return rocksdb.DB(
        path,
        opts,
        column_families={name: rocksdb.ColumnFamilyOptions() for name in cf_names},
    )


def _new_write_batch(db):
    if rocksdb is None:
        return _InMemoryWriteBatch(db)
    return rocksdb.WriteBatch()


def _write_batch(db, batch) -> None:
    if rocksdb is None:
        batch.apply()
    else:
        db.write(batch)

class Ledger:
    def __init__(self):
        self.fdb = _open_db(FACTORS_DB, ["default"])
        self.pdb = _open_db(POSTINGS_DB, ["default"])
        self.log = self._open_event_log()

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
        batch_f = _new_write_batch(self.fdb)
        batch_p = _new_write_batch(self.pdb)
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
            batch_f.put(f"{entity}:{p}".encode(), str(new).encode())
            # 3) update prime→postings
            batch_p.put(f"{p}:{entity}".encode(), str(new).encode())
        _write_batch(self.fdb, batch_f)
        _write_batch(self.pdb, batch_p)

    def _get_factor(self, entity: str, p: int) -> int:
        v = self.fdb.get(f"{entity}:{p}".encode())
        return int(v.decode()) if v else 0

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
            it = self.pdb.iteritems()
            it.seek(f"{p}:".encode())
            ents = []
            for k, v in it:
                if not k.decode().startswith(f"{p}:"):
                    break
                ent = k.decode().split(":")[1]
                ents.append((ent, int(v.decode())))
            sets.append(dict(ents))
        # intersect
        common = reduce(lambda a,b: a.keys() & b.keys(), sets)
        out = []
        for e in common:
            w = min(sets[i][e] for i in range(len(primes)))
            out.append((e, w))
        return out

    def checksum(self, entity: str) -> str:
        it = self.fdb.iteritems()
        it.seek(f"{entity}:".encode())
        leaves = []
        for k, v in it:
            if not k.decode().startswith(f"{entity}:"):
                break
            leaves.append(k+v)
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
