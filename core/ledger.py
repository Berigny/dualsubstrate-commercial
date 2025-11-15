"""Append-only event log + RocksDB indices
Events: (entity_id, prime, delta_k, timestamp)."""
import json
import hashlib
import os
import tempfile
import time
from collections import deque
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Tuple

from checksum import merkle_root
from .automorphism import CycleAutomorphismService, CycleResult
from .flow_rule_bridge import FlowRuleViolation, validate_prime_sequence
from .inference import InferenceStore
from .valuation import EnergyBreakdown, mixed_energy
from core.storage import open_db as open_rocksdb, rocksdb_available

DATA_ROOT = Path(os.getenv("LEDGER_DATA_PATH", "./data"))
EVENT_LOG = Path(os.getenv("EVENT_LOG_PATH", str(DATA_ROOT / "event.log")))
FACTORS_DB = Path(os.getenv("FACTORS_DB_PATH", str(DATA_ROOT / "factors")))
POSTINGS_DB = Path(os.getenv("POSTINGS_DB_PATH", str(DATA_ROOT / "postings")))
SLOTS_DB = Path(os.getenv("SLOTS_DB_PATH", str(DATA_ROOT / "slots")))
INFERENCE_DB = Path(os.getenv("INFERENCE_DB_PATH", str(DATA_ROOT / "inference")))
PRIME_ARRAY: Tuple[int, ...] = (2, 3, 5, 7, 11, 13, 17, 19)
QP_PREFIX = b"qp:"
SLOTS_PREFIX = b"slots:"
DEFAULT_LAWFULNESS = int(os.getenv("LEDGER_LAWFULNESS_DEFAULT", "3"))


HAS_ROCKS = rocksdb_available()


_SEARCH_MODE_CONFIG: Dict[str, Dict[str, object]] = {
    "s1": {
        "slots": ("S1",),
        "prime_weights": {2: 1.0, 3: 0.95, 5: 0.9, 7: 0.85},
        "default_weight": 0.75,
    },
    "s2": {
        "slots": ("S2",),
        "prime_weights": {11: 0.8, 13: 0.75, 17: 0.7, 19: 0.65},
        "default_weight": 0.6,
    },
    "body": {
        "slots": ("body",),
        "prime_weights": {},
        "default_weight": 0.7,
    },
    "recall": {
        "slots": ("body",),
        "prime_weights": {},
        "default_weight": 0.75,
    },
    "all": {
        "slots": ("S1", "S2", "body"),
        "prime_weights": {
            2: 1.0,
            3: 0.95,
            5: 0.9,
            7: 0.85,
            11: 0.8,
            13: 0.75,
            17: 0.7,
            19: 0.65,
        },
        "default_weight": 0.6,
    },
}


def _iter_strings(value) -> Iterator[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for item in value.values():
            yield from _iter_strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _iter_strings(item)


def _make_snippet(text: str, needle: str, radius: int = 40) -> str:
    lowered = text.lower()
    idx = lowered.find(needle)
    if idx == -1:
        excerpt = text[: radius * 2]
        if len(text) > radius * 2:
            return f"{excerpt}…"
        return excerpt
    start = max(0, idx - radius)
    end = min(len(text), idx + len(needle) + radius)
    prefix = "…" if start > 0 else ""
    suffix = "…" if end < len(text) else ""
    return f"{prefix}{text[start:end]}{suffix}"


def _extract_entity_from_slot_key(raw_key: bytes | str) -> str:
    if isinstance(raw_key, bytes):
        suffix = raw_key[len(SLOTS_PREFIX) :]
        return suffix.decode(errors="ignore")
    if isinstance(raw_key, str):
        prefix = SLOTS_PREFIX.decode()
        if raw_key.startswith(prefix):
            return raw_key[len(prefix) :]
        return raw_key
    return str(raw_key)


def _decode_slots_document(raw_value) -> Dict | None:
    if isinstance(raw_value, bytes):
        payload = raw_value.decode()
    elif isinstance(raw_value, str):
        payload = raw_value
    else:
        return None
    try:
        doc = json.loads(payload)
    except json.JSONDecodeError:
        return None
    if isinstance(doc, dict):
        return doc
    return None


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
        inference_path: str | os.PathLike[str] | None = None,
    ):
        self.event_log_path = Path(event_log_path or EVENT_LOG)
        self.factors_path = Path(factors_path or FACTORS_DB)
        self.postings_path = Path(postings_path or POSTINGS_DB)
        self.slots_path = Path(slots_path or SLOTS_DB)
        self.inference_path = Path(inference_path or INFERENCE_DB)
        self.fdb = _open_db(str(self.factors_path))
        self.pdb = _open_db(str(self.postings_path))
        self.sdb = _open_db(str(self.slots_path))
        self.idb = _open_db(str(self.inference_path))
        self.log = self._open_event_log()
        self.inference_store = InferenceStore(self.idb, primes=PRIME_ARRAY)
        self._energy_lambda = float(os.getenv("LEDGER_ENERGY_LAMBDA", "0.5"))
        self._last_energy: Dict[str, EnergyBreakdown] = {}
        self.automorphism = CycleAutomorphismService(
            self.inference_store, primes=PRIME_ARRAY
        )

    def close(self):
        """Close the database connections."""
        self.fdb.close()
        self.pdb.close()
        self.sdb.close()
        self.idb.close()
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

    def search_slots(self, query: str, mode: str, *, limit: int = 50) -> List[Dict[str, object]]:
        """Return ranked slot/body matches for ``query`` using ``mode`` weights."""

        normalized_query = (query or "").strip()
        if not normalized_query:
            return []

        normalized_mode = (mode or "").strip().lower()
        config = _SEARCH_MODE_CONFIG.get(normalized_mode)
        if config is None:
            raise ValueError(f"Unsupported search mode: {mode}")

        needle = normalized_query.lower()
        slots_to_scan = tuple(config.get("slots", ()))
        prime_weights = config.get("prime_weights", {})
        default_weight = float(config.get("default_weight", 0.5))
        results: Dict[Tuple[str, int], Dict[str, object]] = {}

        for raw_key, raw_value in _iter_prefix(self.sdb, SLOTS_PREFIX):
            entity = _extract_entity_from_slot_key(raw_key)
            doc = _decode_slots_document(raw_value)
            if not doc:
                continue
            # Search is open to every entity regardless of lawfulness.
            slots = doc.get("slots")
            if not isinstance(slots, dict):
                continue

            for slot_name in slots_to_scan:
                bucket = slots.get(slot_name, {})
                if slot_name == "body":
                    if not isinstance(bucket, dict):
                        continue
                    for prime_key, body_payload in bucket.items():
                        try:
                            prime = int(prime_key)
                        except (TypeError, ValueError):
                            continue
                        if not isinstance(body_payload, dict):
                            continue
                        text_value = body_payload.get("text") or body_payload.get("value")
                        if not isinstance(text_value, str):
                            continue
                        lowered = text_value.lower()
                        matches = lowered.count(needle)
                        if matches == 0:
                            continue
                        weight = float(prime_weights.get(prime, default_weight))
                        score = weight * matches
                        snippet = _make_snippet(text_value, needle)
                        key = (entity, prime)
                        payload = {
                            "entity": entity,
                            "prime": prime,
                            "score": round(score, 6),
                            "snippet": snippet,
                        }
                        existing = results.get(key)
                        if not existing or payload["score"] > existing["score"]:
                            results[key] = payload
                        elif existing and payload["score"] == existing["score"] and len(snippet) < len(existing["snippet"]):
                            results[key] = payload
                else:
                    if not isinstance(bucket, dict):
                        continue
                    for prime_key, slot_payload in bucket.items():
                        try:
                            prime = int(prime_key)
                        except (TypeError, ValueError):
                            continue
                        weight = float(prime_weights.get(prime, default_weight))
                        for candidate in _iter_strings(slot_payload):
                            text_value = candidate.strip()
                            if not text_value:
                                continue
                            lowered = text_value.lower()
                            matches = lowered.count(needle)
                            if matches == 0:
                                continue
                            score = weight * matches
                            snippet = _make_snippet(text_value, needle)
                            key = (entity, prime)
                            payload = {
                                "entity": entity,
                                "prime": prime,
                                "score": round(score, 6),
                                "snippet": snippet,
                            }
                            existing = results.get(key)
                            if not existing or payload["score"] > existing["score"]:
                                results[key] = payload
                            elif existing and payload["score"] == existing["score"] and len(snippet) < len(existing["snippet"]):
                                results[key] = payload

        ranked = sorted(results.values(), key=lambda row: row["score"], reverse=True)
        if limit and limit > 0:
            return ranked[:limit]
        return ranked

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

    def write_s1_slots(self, entity: str, slots: List[Dict[str, Any]]) -> int:
        doc = self._load_slots_doc(entity)
        if doc.get("lawfulness", DEFAULT_LAWFULNESS) < 1:
            raise ValueError("Entity lawfulness forbids S1 updates (requires >=1).")

        allowed_primes = {2, 3, 5, 7}
        bucket = doc["slots"].setdefault("S1", {})
        updated = 0

        for slot in slots:
            if not isinstance(slot, dict):
                raise ValueError("Each S1 slot must be an object.")

            try:
                prime = int(slot.get("prime"))
            except (TypeError, ValueError) as exc:
                raise ValueError("prime must be an integer") from exc
            if prime not in allowed_primes:
                raise ValueError(f"Prime {prime} is not a valid S1 slot.")

            try:
                body_prime = int(slot.get("body_prime"))
            except (TypeError, ValueError) as exc:
                raise ValueError("body_prime must be an integer") from exc
            if body_prime < 23 or not self._ensure_prime(body_prime):
                raise ValueError("body_prime must be a prime >=23.")

            value_raw = slot.get("value", 1)
            try:
                numeric_value = float(value_raw)
            except (TypeError, ValueError) as exc:
                raise ValueError("value must be numeric.") from exc
            if isinstance(value_raw, int) and not isinstance(value_raw, bool):
                value = value_raw
            elif isinstance(value_raw, float):
                value = value_raw
            else:
                value = numeric_value

            title = slot.get("title")
            if title is not None and not isinstance(title, str):
                raise ValueError("title must be a string.")

            tags = slot.get("tags")
            if tags is not None:
                if not isinstance(tags, list) or any(not isinstance(item, str) for item in tags):
                    raise ValueError("tags must be a list of strings.")

            metadata = slot.get("metadata")
            if metadata is not None and not isinstance(metadata, dict):
                raise ValueError("metadata must be an object.")

            score_raw = slot.get("score")
            if score_raw is not None:
                try:
                    score_value = float(score_raw)
                except (TypeError, ValueError) as exc:
                    raise ValueError("score must be numeric.") from exc
            else:
                score_value = None

            timestamp_raw = slot.get("timestamp")
            if timestamp_raw is not None:
                try:
                    timestamp_value = int(timestamp_raw)
                except (TypeError, ValueError) as exc:
                    raise ValueError("timestamp must be an integer.") from exc
            else:
                timestamp_value = None

            write_targets = slot.get("write_primes")
            if write_targets is None:
                cleaned_targets = [body_prime]
            else:
                if not isinstance(write_targets, list):
                    raise ValueError("write_primes must be a list of integers.")
                cleaned_targets = []
                for candidate in write_targets:
                    try:
                        candidate_prime = int(candidate)
                    except (TypeError, ValueError) as exc:
                        raise ValueError(
                            f"write_primes must be integers (got {candidate!r})."
                        ) from exc
                    if candidate_prime < 23 or not self._ensure_prime(candidate_prime):
                        raise ValueError(
                            f"write_primes entries must be primes >=23 (got {candidate_prime})."
                        )
                    cleaned_targets.append(candidate_prime)
                if body_prime not in cleaned_targets:
                    cleaned_targets.append(body_prime)
            slot_payload: Dict[str, Any] = {
                "prime": prime,
                "value": value,
                "body_prime": body_prime,
                "write_primes": cleaned_targets,
            }
            if title is not None:
                slot_payload["title"] = title
            if tags is not None:
                slot_payload["tags"] = list(tags)
            if metadata is not None:
                slot_payload["metadata"] = dict(metadata)
            if score_value is not None:
                slot_payload["score"] = score_value
            if timestamp_value is not None:
                slot_payload["timestamp"] = timestamp_value

            bucket[str(prime)] = slot_payload
            updated += 1

        if updated:
            doc["tier"] = "S1"
            self._store_slots_doc(entity, doc)
        return updated

    def update_s1_slots(self, entity: str, facets: Dict[str, Dict]) -> Dict:
        slots: List[Dict[str, Any]] = []
        for prime_key, payload in facets.items():
            if not isinstance(payload, dict):
                raise ValueError(f"S1 facet for prime {prime_key} must be an object.")
            write_targets = payload.get("write_primes") or payload.get("write_primes".upper())
            if not write_targets:
                raise ValueError(f"S1 facet {prime_key} requires write_primes.")
            cleaned_targets: List[int] = []
            for candidate in write_targets:
                try:
                    prime = int(candidate)
                except (TypeError, ValueError):
                    raise ValueError(f"write_primes must be integers (got {candidate!r}).") from None
                if prime < 23 or not self._ensure_prime(prime):
                    raise ValueError(f"write_primes entries must be primes >=23 (got {prime}).")
                cleaned_targets.append(prime)
            try:
                prime_int = int(prime_key)
            except (TypeError, ValueError):
                raise ValueError(f"Prime {prime_key} is not a valid S1 slot.") from None
            slots.append(
                {
                    "prime": prime_int,
                    "body_prime": cleaned_targets[0],
                    "value": payload.get("value", 1),
                    "title": payload.get("title"),
                    "tags": payload.get("tags"),
                    "metadata": payload.get("metadata"),
                    "score": payload.get("score"),
                    "timestamp": payload.get("timestamp"),
                    "write_primes": cleaned_targets,
                }
            )
        self.write_s1_slots(entity, slots)
        return self._load_slots_doc(entity)

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
        body_slots = doc["slots"].setdefault("body", {})
        prime_key = str(prime)
        existing_slot = body_slots.get(prime_key, {})
        merged_slot = dict(existing_slot)

        for key, value in body.items():
            if key == "value":
                continue
            if key == "provenance" and value is None:
                merged_slot.pop("provenance", None)
            else:
                merged_slot[key] = value

        merged_slot["content_type"] = merged_slot.get("content_type", "text/plain")
        merged_slot["text"] = content
        merged_slot["hash"] = f"sha256:{digest}"
        merged_slot["updated_at"] = int(time.time() * 1000)
        body_slots[prime_key] = merged_slot
        self._store_slots_doc(entity, doc)
        return doc

    def update_s2_slots(self, entity: str, facets: Dict[str, Dict]) -> Dict:
        doc = self._load_slots_doc(entity)
        if doc.get("lawfulness", DEFAULT_LAWFULNESS) < 3:
            raise ValueError("Entity lawfulness forbids S2 updates (requires >=3).")
        metrics = doc.get("r_metrics") or {}
        required_metrics = ("dE", "dDrift", "dRetention", "K")
        missing = [name for name in required_metrics if metrics.get(name) is None]
        if missing:
            missing_list = ", ".join(sorted(missing))
            raise ValueError(
                "S2 updates require r_metrics values for: "
                f"{missing_list}. Provide metrics before writing facets."
            )
        try:
            delta_e = float(metrics["dE"])
            delta_drift = float(metrics["dDrift"])
            delta_retention = float(metrics["dRetention"])
            delta_k = float(metrics["K"])
        except (TypeError, ValueError) as exc:
            raise ValueError("S2 updates require numeric r_metrics values.") from exc
        if delta_e >= 0:
            raise ValueError(f"S2 updates require ΔE < 0 (got {delta_e}).")
        if delta_drift >= 0:
            raise ValueError(f"S2 updates require ΔDrift < 0 (got {delta_drift}).")
        if delta_retention <= 0:
            raise ValueError(f"S2 updates require ΔRetention > 0 (got {delta_retention}).")
        if delta_k < 0:
            raise ValueError(f"S2 updates require ΔK ≥ 0 (got {delta_k}).")
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

    def anchor(
        self,
        entity: str,
        factors: List[Tuple[int,int]],
        *,
        update_inference: bool = True,
    ) -> CycleResult:
        ts = int(time.time()*1000)
        primes_only = [p for p, _ in factors]
        via_flags: List[bool] = []
        if primes_only:
            try:
                check = validate_prime_sequence(primes_only)
            except FlowRuleViolation:
                check = None
                via_flags = self.automorphism.derive_via_flags(primes_only)
            else:
                via_flags = check.via_centroid
        cycle = (
            self.automorphism.enforce(
                entity,
                primes_only,
                via_flags,
                mutate_state=update_inference,
            )
            if factors
            else self.automorphism.empty_cycle()
        )
        if factors:
            self._last_energy[entity] = self._compute_energy(entity, factors)
        else:
            self._last_energy[entity] = EnergyBreakdown(
                total=0.0,
                continuous=0.0,
                discrete=0.0,
                lambda_weight=self._energy_lambda,
            )
        if update_inference and factors:
            self.inference_store.update(entity, [(p, dk) for p, dk in factors])
        for idx, (p, dk) in enumerate(factors):
            # 1) append event
            via_c = via_flags[idx] if idx < len(via_flags) else False
            centroid_digit = (
                cycle.steps[idx].centroid
                if idx < len(cycle.steps)
                else cycle.final_centroid
            )
            evt = json.dumps(
                {
                    "e": entity,
                    "p": p,
                    "d": dk,
                    "ts": ts,
                    "via_c": via_c,
                    "c": centroid_digit,
                    "cycle_index": cycle.steps[idx].cycle_index
                    if idx < len(cycle.steps)
                    else cycle.flips,
                }
            )
            self.log.write((evt+"\n").encode())
            # 2) update entity→factors
            old = self._get_factor(entity, p)
            new = old + dk
            self.fdb.put(f"{entity}:{p}".encode(), str(new).encode())
            # 3) update prime→postings
            self.pdb.put(f"{p}:{entity}".encode(), str(new).encode())
        return cycle

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
            return self.anchor(entity, deltas, update_inference=True)
        return self.automorphism.empty_cycle()

    def _compute_energy(
        self, entity: str, deltas: List[Tuple[int, int]]
    ) -> EnergyBreakdown:
        """Evaluate ``E_t`` for ``entity`` using the current inference state."""

        snapshot = self.inference_store.snapshot(entity)
        return mixed_energy(
            snapshot.x,
            snapshot.readouts,
            deltas,
            lambda_weight=self._energy_lambda,
        )

    def last_energy(self, entity: str) -> EnergyBreakdown | None:
        """Return the most recent energy measurement for ``entity``."""

        return self._last_energy.get(entity)

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

    def inference_snapshot(self, entity: str) -> Dict[str, object]:
        """Return the persisted inference state for ``entity``."""

        return self.inference_store.snapshot(entity).as_dict()

    def inference_history(self, entity: str, *, limit: int = 10) -> List[Dict[str, object]]:
        """Return the most recent inference events for ``entity`` from the log."""

        if limit <= 0:
            return []

        history: "deque[Dict[str, object]]" = deque(maxlen=int(limit))

        try:
            with open(self.event_log_path, "r", encoding="utf-8") as handle:
                for line in handle:
                    record_raw = line.strip()
                    if not record_raw:
                        continue
                    try:
                        payload = json.loads(record_raw)
                    except json.JSONDecodeError:
                        continue
                    if payload.get("e") != entity:
                        continue
                    history.append(payload)
        except FileNotFoundError:
            return []
        except OSError:
            return []

        return list(reversed(history))


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
