"""Append-only event log + RocksDB indices
Events: (entity_id, prime, delta_k, timestamp)."""
import json
import hashlib
import os
import tempfile
import time
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from checksum import merkle_root
from .flow_rule_bridge import validate_prime_sequence
from core.storage import open_db as open_rocksdb, rocksdb_available

DATA_ROOT = Path(os.getenv("LEDGER_DATA_PATH", "./data"))
EVENT_LOG = Path(os.getenv("EVENT_LOG_PATH", str(DATA_ROOT / "event.log")))
FACTORS_DB = Path(os.getenv("FACTORS_DB_PATH", str(DATA_ROOT / "factors")))
POSTINGS_DB = Path(os.getenv("POSTINGS_DB_PATH", str(DATA_ROOT / "postings")))
SLOTS_DB = Path(os.getenv("SLOTS_DB_PATH", str(DATA_ROOT / "slots")))
PRIME_ARRAY: Tuple[int, ...] = (2, 3, 5, 7, 11, 13, 17, 19)
QP_PREFIX = b"qp:"
SLOTS_PREFIX = b"slots:"
DEFAULT_LAWFULNESS = int(os.getenv("LEDGER_LAWFULNESS_DEFAULT", "3"))


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

    def close(self):
        pass


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
    def __init__(
        self,
        *,
        event_log_path: str | os.PathLike[str] | None = None,
        factors_path: str | os.PathLike[str] | None = None,
        postings_path: str | os.PathLike[str] | None = None,
        slots_path: str | os.PathLike[str] | None = None,
    ):
        self.event_log_path = Path(event_log_path or EVENT_LOG)
        self.factors_path = Path(factors_path or FACTORS_DB)
        self.postings_path = Path(postings_path or POSTINGS_DB)
        self.slots_path = Path(slots_path or SLOTS_DB)
        self.fdb = _open_db(str(self.factors_path))
        self.pdb = _open_db(str(self.postings_path))
        self.sdb = _open_db(str(self.slots_path))
        self.log = self._open_event_log()

    def close(self):
        """Close the database connections."""
        self.fdb.close()
        self.pdb.close()
        self.sdb.close()
        self.log.close()

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

    def qp_iter(self, prefix: bytes):
        """Iterate over Qp namespace entries with the provided prefix."""
        stored_prefix = self._qp_key(prefix)
        for key, value in _iter_prefix(self.fdb, stored_prefix):
            key_bytes = key[len(QP_PREFIX):] if key.startswith(QP_PREFIX) else key
            yield key_bytes, value

    # ---------- structured slots ----------
    @staticmethod
    def _slots_key(entity: str) -> bytes:
        return SLOTS_PREFIX + entity.encode()

    @staticmethod
    def _default_slots_doc(entity: str) -> Dict:
        return {
            "entity": entity,
            "version": "1.1",
            "tier": "S1",
            "lawfulness": DEFAULT_LAWFULNESS,
            "meta": {},
            "slots": {
                "S1": {},
                "S2": {},
                "body": {},
            },
            "r_metrics": {
                "dE": 0,
                "dDrift": 0,
                "dRetention": 0,
                "K": 0.0,
            },
        }

    def _load_slots_doc(self, entity: str) -> Dict:
        raw = self.sdb.get(self._slots_key(entity))
        if not raw:
            return self._default_slots_doc(entity)
        if isinstance(raw, bytes):
            return json.loads(raw.decode())
        return json.loads(raw)

    def _store_slots_doc(self, entity: str, doc: Dict) -> None:
        payload = json.dumps(doc, separators=(",", ":")).encode()
        self.sdb.put(self._slots_key(entity), payload)

    def entity_document(self, entity: str) -> Dict:
        """Return the structured S1/S2/body document for ``entity``."""
        return self._load_slots_doc(entity)

    @staticmethod
    def _ensure_prime(value: int) -> bool:
        if value < 2:
            return False
        if value in (2, 3):
            return True
        if value % 2 == 0:
            return False
        d = 3
        while d * d <= value:
            if value % d == 0:
                return False
            d += 2
        return True

    def update_s1_slots(self, entity: str, facets: Dict[str, Dict]) -> Dict:
        doc = self._load_slots_doc(entity)
        if doc.get("lawfulness", DEFAULT_LAWFULNESS) < 1:
            raise ValueError("Entity lawfulness forbids S1 updates (requires >=1).")
        allowed_primes = {"2", "3", "5", "7"}
        slots = doc["slots"].setdefault("S1", {})
        for prime_key, payload in facets.items():
            if prime_key not in allowed_primes:
                raise ValueError(f"Prime {prime_key} is not a valid S1 slot.")
            if not isinstance(payload, dict):
                raise ValueError(f"S1 facet for prime {prime_key} must be an object.")
            write_targets = payload.get("write_primes") or payload.get("write_primes".upper())
            if not write_targets:
                raise ValueError(f"S1 facet {prime_key} requires write_primes.")
            cleaned_targets = []
            for candidate in write_targets:
                try:
                    prime = int(candidate)
                except (TypeError, ValueError):
                    raise ValueError(f"write_primes must be integers (got {candidate!r}).") from None
                if prime < 23 or not self._ensure_prime(prime):
                    raise ValueError(f"write_primes entries must be primes >=23 (got {prime}).")
                cleaned_targets.append(prime)
            payload["write_primes"] = cleaned_targets
            slots[prime_key] = payload
        self._store_slots_doc(entity, doc)
        return doc

    def update_body_slot(self, entity: str, prime: int, body: Dict) -> Dict:
        doc = self._load_slots_doc(entity)
        if doc.get("lawfulness", DEFAULT_LAWFULNESS) < 2:
            raise ValueError("Entity lawfulness forbids body updates (requires >=2).")
        if prime < 23 or not self._ensure_prime(prime):
            raise ValueError("Body writes must target primes >=23.")
        if not isinstance(body, dict):
            raise ValueError("Body payload must be an object.")
        content = body.get("text") or body.get("value")
        if not isinstance(content, str) or not content.strip():
            raise ValueError("Body payload requires non-empty text.")
        digest = hashlib.sha256(content.encode()).hexdigest()
        slot = {
            "content_type": body.get("content_type", "text/plain"),
            "text": content,
            "hash": f"sha256:{digest}",
            "updated_at": int(time.time() * 1000),
        }
        doc["slots"].setdefault("body", {})[str(prime)] = slot
        self._store_slots_doc(entity, doc)
        return doc

    def update_s2_slots(self, entity: str, facets: Dict[str, Dict]) -> Dict:
        doc = self._load_slots_doc(entity)
        if doc.get("lawfulness", DEFAULT_LAWFULNESS) < 3:
            raise ValueError("Entity lawfulness forbids S2 updates (requires >=3).")
        allowed_primes = {"11", "13", "17", "19"}
        slots = doc["slots"].setdefault("S2", {})
        for prime_key, payload in facets.items():
            if prime_key not in allowed_primes:
                raise ValueError(f"Prime {prime_key} is not a valid S2 slot.")
            if not isinstance(payload, dict):
                raise ValueError(f"S2 facet for prime {prime_key} must be an object.")
            slots[prime_key] = payload
        doc["tier"] = "S2" if facets else doc.get("tier", "S1")
        self._store_slots_doc(entity, doc)
        return doc

    def update_lawfulness(self, entity: str, value: int) -> Dict:
        if value < 0 or value > 3:
            raise ValueError("Lawfulness must be between 0 and 3.")
        doc = self._load_slots_doc(entity)
        doc["lawfulness"] = int(value)
        self._store_slots_doc(entity, doc)
        return doc

    def update_r_metrics(self, entity: str, metrics: Dict[str, float]) -> Dict:
        doc = self._load_slots_doc(entity)
        doc.setdefault("r_metrics", {}).update(metrics)
        self._store_slots_doc(entity, doc)
        return doc

    def _open_event_log(self):
        log_path = self.event_log_path
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
