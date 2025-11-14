"""
DualSubstrate API – ledger + Metatron-star flow-rule enforcement
"""
from fastapi import FastAPI, HTTPException, Depends, Query, Request, WebSocket, WebSocketDisconnect, Body
from pydantic import BaseModel, conint
from typing import Any, Callable, Dict, List, Literal, Set, Tuple
import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager

# ---------- imports ----------
import flow_rule  # our Rust wheel
from core import core as core_rs  # PyO3 bindings (quaternion pack/rotate)
from core.ledger import Ledger, PRIME_ARRAY  # the RocksDB wrapper we wrote earlier
from deps import require_key, limiter
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi import _rate_limit_exceeded_handler
from s1_s2_memory import S1Salience, deterministic_key
from api.prime_schema import annotate_factors, annotate_prime_list, get_schema_response
from api.ledger_manager import get_ledger, close_all, list_ledgers
from api.metrics import record_anchor_energy

# ---------- flow-rule bridge ----------
_ALLOWED_DIRECT = {(1, 2), (5, 6), (3, 0), (7, 4), (1, 0)}


def _python_transition_allowed(src: int, dst: int) -> bool:
    if src == dst:
        return True
    if (src, dst) in _ALLOWED_DIRECT:
        return True
    if src % 2 == 0 and dst % 2 == 1:
        return False
    return (src % 2) == (dst % 2)


_transition_allowed: Callable[[int, int], bool]
if hasattr(flow_rule, "py_transition_allowed"):
    _transition_allowed = getattr(flow_rule, "py_transition_allowed")  # type: ignore[assignment]
else:  # pragma: no cover - defensive fallback when Rust wheel missing
    _transition_allowed = _python_transition_allowed


# ---------- demo metrics ----------
_metrics_lock = threading.Lock()
tokens_saved = 0
total_calls = 0
duplicate_calls = 0
_seen_signatures: Set[str] = set()
_last_anchor_energy: Dict[str, object] | None = None


logger = logging.getLogger(__name__)


# ---------- models ----------
Prime = conint(ge=2, le=19)  # S0 primes only for MVP


class Factor(BaseModel):
    prime: Prime
    delta: int  # can be negative


class AnchorReq(BaseModel):
    entity: str
    factors: List[Factor]
    text: str | None = None


def _persist_memory_entry(
    req: AnchorReq, ledger: Ledger, *, timestamp: int | None = None
) -> dict | None:
    """Persist the transcript payload into the Qp namespace."""
    if not req.text:
        return None

    ts_ms = timestamp or int(time.time() * 1000)
    key = f"{req.entity}:{ts_ms}".encode()
    payload = json.dumps(
        {
            "text": req.text,
            "timestamp": ts_ms,
            "primes": [int(f.prime) for f in req.factors],
        }
    )
    ledger.qp_put(key, payload)
    return {"stored": True, "key": key.decode(), "timestamp": ts_ms}


def _factor_signature(entity: str, factors: List[Factor]) -> str:
    payload = {
        "entity": entity,
        "factors": [(f.prime, f.delta) for f in factors],
    }
    return json.dumps(payload, sort_keys=True)


def _record_metrics(entity: str, factors: List[Factor]) -> bool:
    """
    Update the demo counters and report whether this anchor matches a prior entry.
    """
    global tokens_saved, total_calls, duplicate_calls
    signature = _factor_signature(entity, factors)
    with _metrics_lock:
        total_calls += 1
        if signature in _seen_signatures:
            duplicate_calls += 1
            tokens_saved += len(factors)
            return True
        _seen_signatures.add(signature)
    return False


class QueryReq(BaseModel):
    primes: List[Prime]


class RotateReq(BaseModel):
    entity: str
    axis: Tuple[float, float, float] = (0.0, 0.0, 1.0)
    angle: float


class RotateResp(BaseModel):
    original_checksum: str
    rotated_checksum: str
    energy_cycles: int


class QpPut(BaseModel):
    value: str


class SalienceReq(BaseModel):
    utterance: str
    timestamp: float | None = None
    threshold: float | None = None


class Edge(BaseModel):
    src: int
    dst: int
    via_c: bool
    label: str


class TraverseResp(BaseModel):
    edges: List[Edge]
    centroid_flips: int
    final_centroid: Literal[0, 1]


class LedgerCreate(BaseModel):
    ledger_id: str


class LedgerSlotPayload(BaseModel):
    prime: int
    value: float | int | None = None
    body_prime: int
    title: str | None = None
    tags: List[str] | None = None
    metadata: Dict[str, Any] | None = None
    score: float | None = None
    timestamp: int | None = None


class LedgerSlotsPayload(BaseModel):
    entity: str | None = None
    slots: List[LedgerSlotPayload]

class LawfulnessUpdate(BaseModel):
    value: conint(ge=0, le=3)


class MetricsUpdate(BaseModel):
    dE: float | None = None
    dDrift: float | None = None
    dRetention: float | None = None
    K: float | None = None


# ---------- helpers ----------
Node = Literal[0, 1, 2, 3, 4, 5, 6, 7]
PRIME_IDX = {p: idx for idx, p in enumerate(PRIME_ARRAY)}
LEDGER_HEADER = "X-Ledger-ID"
DEFAULT_LEDGER_ID = os.getenv("DEFAULT_LEDGER_ID", "default")


def _ledger_id(request: Request) -> str:
    return request.headers.get(LEDGER_HEADER, DEFAULT_LEDGER_ID)


def _entity_from_request(
    entity_param: str | None,
    request: Request,
    *,
    allow_default: bool = False,
) -> str:
    """
    Resolve an entity identifier from query parameters or the X-Ledger-ID header.
    When ``allow_default`` is true we fall back to ``DEFAULT_LEDGER_ID`` so legacy
    clients that omitted the entity parameter continue to function.
    """

    candidate = (entity_param or request.headers.get(LEDGER_HEADER) or "").strip()
    if candidate:
        return candidate
    if allow_default:
        return DEFAULT_LEDGER_ID
    raise HTTPException(
        422, "entity must be provided via query parameter or X-Ledger-ID header"
    )


def _prime_to_node(p: Prime) -> Node:
    """core registry: 2→0, 3→1, 5→2, 7→3, 11→4, 13→5, 17→6, 19→7"""
    mapping = {2: 0, 3: 1, 5: 2, 7: 3, 11: 4, 13: 5, 17: 6, 19: 7}
    return mapping[p]  # type: ignore


def _centroid_now() -> Literal[0, 1]:
    return 0 if int(time.time() * 1000) % 2 == 0 else 1


def _label(src: int, dst: int) -> str:
    if src == dst:
        return "persistence"
    if (src, dst) in ((1, 2), (5, 6)):
        return "work"
    if (src, dst) in ((3, 0), (7, 4)):
        return "heat-dump"
    if (src, dst) == (1, 0):
        return "electric-dissipation"
    return "mediated"


def _legalise_transition(src_node: int, dst_node: int) -> Tuple[bool, bool]:
    """
    returns (allowed, via_c)
    if native transition forbidden -> force via_c=True and still allow
    """
    allowed = _transition_allowed(src_node, dst_node)
    if allowed:
        via_c = (
            (src_node % 2 == 0 and dst_node % 2 == 1)
            and (src_node, dst_node) not in _ALLOWED_DIRECT
        )
        return (True, via_c)
    # illegal -> must go through C (we still store the delta, but flag via_c)
    return (True, True)


def _ledger_response(ledger: Ledger, entity: str) -> Dict[str, Any]:
    doc = ledger.entity_document(entity)
    return {
        "entity": entity,
        "version": doc.get("version", "1.0"),
        "tier": doc.get("tier", "S1"),
        "lawfulness": doc.get("lawfulness"),
        "meta": doc.get("meta", {}),
        "slots": doc.get("slots", {}),
        "r_metrics": doc.get("r_metrics", {}),
        "factors": annotate_factors(ledger.factors(entity)),
    }


# ---------- FastAPI ----------
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.s1_salience = S1Salience()
    app.state.recall_store: Dict[str, str] = {}
    try:
        yield
    finally:
        close_all()

app = FastAPI(title="DualSubstrate – Flow-Rule Ledger", version="0.3.0", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)
SALIENT_THRESHOLD = 0.7


@app.get("/")
def root():
    return {"message": "DualSubstrate /traverse ready"}


@app.get("/schema", include_in_schema=False)
def prime_schema():
    """Expose canonical prime + modifier schema for agents and clients."""
    return get_schema_response()


@app.get("/admin/ledgers", include_in_schema=False)
def list_ledger_mounts(_: str = Depends(require_key)):
    return {"ledgers": list_ledgers()}


@app.post("/admin/ledgers", include_in_schema=False)
def create_ledger(payload: LedgerCreate, _: str = Depends(require_key)):
    ledger_id = payload.ledger_id.strip()
    if not ledger_id:
        raise HTTPException(422, "ledger_id must not be empty")
    get_ledger(ledger_id)
    return {"ledger_id": ledger_id}


@app.put("/ledger/s1")
def put_ledger_s1(
    payload: LedgerSlotsPayload,
    request: Request,
    _: str = Depends(require_key),
):
    entity = (payload.entity or "").strip()
    if not entity:
        fallback_header = request.headers.get("X-Entity", "").strip()
        fallback_query = request.query_params.get("entity", "").strip()
        entity = fallback_header or fallback_query
    if not entity:
        raise HTTPException(status_code=422, detail="entity is required")

    slots = payload.slots or []
    if not slots:
        return {"updated": 0}

    normalized: List[Dict[str, Any]] = []
    for slot in slots:
        data = slot.model_dump(exclude_none=True)
        prime = data.get("prime")
        body_prime = data.get("body_prime")
        if not isinstance(prime, int) or not isinstance(body_prime, int):
            raise HTTPException(status_code=422, detail="prime and body_prime are required")
        value = data.get("value")
        if value is None:
            data["value"] = 1
        normalized.append(data)

    ledger = get_ledger(_ledger_id(request))
    try:
        updated = ledger.write_s1_slots(entity, normalized)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"updated": updated}


@app.put("/ledger/body")
def upsert_body_slot(
    request: Request,
    entity: str | None = Query(None, description="Entity identifier"),
    prime: int | None = Query(None, ge=2, description="Target prime (>=23)"),
    payload: Dict[str, Any] = Body(...),
    _: str = Depends(require_key),
):
    ledger = get_ledger(_ledger_id(request))

    body_entity = (payload.get("entity") or "").strip()
    query_entity = (entity or "").strip()
    resolved_entity = body_entity or query_entity
    if not resolved_entity:
        raise HTTPException(422, "entity must be provided in the query or body payload")
    if body_entity and query_entity and body_entity != query_entity:
        raise HTTPException(422, "entity in body must match the query parameter")

    prime_value = payload.get("prime", prime)
    if prime_value is None:
        raise HTTPException(422, "prime must be provided in the query or body payload")
    try:
        prime_int = int(prime_value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(422, "prime must be an integer") from exc
    if prime is not None and payload.get("prime") is not None and int(prime) != prime_int:
        raise HTTPException(422, "prime in body must match the query parameter")

    text_value: str | None = None
    for field in ("body", "text", "value"):
        candidate = payload.get(field)
        if isinstance(candidate, str) and candidate.strip():
            text_value = candidate
            break
    if text_value is None:
        raise HTTPException(422, "Body payload requires non-empty text.")

    metadata_raw = payload.get("metadata")
    if metadata_raw is None:
        metadata_obj: Dict[str, Any] | None = None
    elif isinstance(metadata_raw, dict):
        metadata_obj = metadata_raw
    else:
        raise HTTPException(422, "metadata must be an object when provided")

    slot_payload: Dict[str, Any] = {}
    for key, value in payload.items():
        if key in {"entity", "prime", "body", "text", "value", "metadata"}:
            continue
        slot_payload[key] = value

    slot_payload["text"] = text_value
    if metadata_obj is not None:
        slot_payload["metadata"] = metadata_obj

    try:
        ledger.update_body_slot(resolved_entity, prime_int, slot_payload)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    return {"ok": True, "entity": resolved_entity, "prime": prime_int}


@app.put("/ledger/s2")
def upsert_s2_slots(
    request: Request,
    entity: str = Query(...),
    payload: Dict[str, Dict[str, Any]] = Body(...),
    _: str = Depends(require_key),
):
    ledger = get_ledger(_ledger_id(request))
    try:
        ledger.update_s2_slots(entity, payload)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    return _ledger_response(ledger, entity)


# ---------- traversal facade ----------
@app.get("/traverse")
@limiter.limit("300/minute")
def traverse_paths(
    request: Request,
    entity: str | None = Query(None, description="Entity identifier to traverse"),
    origin: int | None = Query(None, ge=2, description="Optional origin prime"),
    limit: int = Query(8, ge=1, le=64, description="Maximum traversal records to return"),
    depth: int = Query(1, ge=1, le=32, description="Traversal depth hint"),
    direction: str | None = Query(
        None,
        description="Traversal direction preference (forward, backward, or both)",
    ),
    include_metadata: bool = Query(False, description="Include entity metadata block"),
    _: str = Depends(require_key),
):
    ledger = get_ledger(_ledger_id(request))
    target_entity = _entity_from_request(entity, request, allow_default=False)

    direction_hint = (direction or "forward").strip().lower() or "forward"
    if direction_hint not in {"forward", "backward", "both"}:
        raise HTTPException(422, "direction must be forward, backward, or both")

    factor_pairs = [
        (prime, weight) for prime, weight in ledger.factors(target_entity) if weight != 0
    ]
    annotations = annotate_prime_list([prime for prime, _ in factor_pairs])
    annotation_map = {item.get("prime"): item for item in annotations if isinstance(item, dict)}
    total_weight = sum(abs(weight) for _, weight in factor_pairs) or 1.0

    paths: List[Dict[str, Any]] = []
    for idx, (prime, weight) in enumerate(factor_pairs):
        if len(paths) >= limit:
            break
        window = [p for p, _ in factor_pairs[idx : idx + depth]]
        nodes = list(window)
        if origin is not None and (not nodes or nodes[0] != origin):
            nodes.insert(0, origin)
        metadata_record: Dict[str, Any] = {
            "prime": prime,
            "delta": weight,
            "direction": direction_hint,
            "entity": target_entity,
        }
        annotation = annotation_map.get(prime)
        if annotation:
            metadata_record["annotation"] = annotation
        paths.append(
            {
                "nodes": nodes or ([origin] if origin is not None else []),
                "weight": round(abs(weight) / total_weight, 6),
                "metadata": metadata_record,
            }
        )

    metadata_block: Dict[str, Any] = {}
    if include_metadata:
        doc = ledger.entity_document(target_entity)
        meta = doc.get("meta") if isinstance(doc, dict) else None
        metadata_block = meta if isinstance(meta, dict) else {}

    origin_prime = origin
    if origin_prime is None and paths:
        first_nodes = paths[0].get("nodes") or []
        if first_nodes:
            origin_prime = first_nodes[0]

    return {
        "origin": origin_prime,
        "paths": paths,
        "metadata": metadata_block,
        "supported": True,
    }


@app.get("/search")
def search(
    request: Request,
    q: str = Query(..., description="Query string to match across ledger slots."),
    mode: str = Query(
        "all",
        description="Search scope: s1, s2, body, slots, recall, or all",
    ),
    limit: int = Query(50, ge=1, le=200, description="Maximum number of results to return."),
    entity: str | None = Query(None, description="Optional entity scope override."),
    _: str = Depends(require_key),
):
    ledger = get_ledger(_ledger_id(request))
    target_entity = (entity or request.headers.get(LEDGER_HEADER) or "").strip()
    try:
        results = ledger.search_slots(q, mode, limit=limit)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc

    payload = {"query": q, "mode": mode, "results": results}
    if target_entity:
        payload["entity"] = target_entity
    return payload


@app.patch("/ledger/lawfulness")
def patch_lawfulness(
    request: Request,
    payload: LawfulnessUpdate,
    entity: str = Query(...),
    _: str = Depends(require_key),
):
    ledger = get_ledger(_ledger_id(request))
    try:
        ledger.update_lawfulness(entity, payload.value)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    return _ledger_response(ledger, entity)


@app.patch("/ledger/metrics")
def patch_metrics(
    request: Request,
    entity: str = Query(...),
    payload: MetricsUpdate = Body(...),
    _: str = Depends(require_key),
):
    ledger = get_ledger(_ledger_id(request))
    metrics = {k: v for k, v in payload.dict().items() if v is not None}
    if not metrics:
        raise HTTPException(422, "Provide at least one metric field.")
    ledger.update_r_metrics(entity, metrics)
    return _ledger_response(ledger, entity)


@app.get("/inference/state")
def get_inference_state(
    request: Request,
    entity: str | None = Query(None, description="Entity identifier"),
    include_history: bool = Query(False, description="Include recent inference history."),
    limit: int = Query(10, ge=1, le=100, description="Maximum history items to return."),
    _: str = Depends(require_key),
):
    ledger = get_ledger(_ledger_id(request))
    target_entity = _entity_from_request(entity, request, allow_default=True)

    snapshot = ledger.inference_snapshot(target_entity)
    if include_history:
        snapshot["history"] = ledger.inference_history(target_entity, limit=limit)
    return snapshot


# ---------- existing endpoints ----------
@app.post("/anchor")
@limiter.limit("100/minute")
def anchor(req: AnchorReq, request: Request, _: str = Depends(require_key)):
    """
    1. map primes → nodes
    2. enforce flow-rules (auto-route via C if needed)
    3. write only lawful deltas
    """
    ledger = get_ledger(_ledger_id(request))
    global _last_anchor_energy
    ts = int(time.time() * 1000)
    centroid = _centroid_now()
    edges: List[Edge] = []
    lawful_factors: List[Tuple[int, int]] = []  # (prime, delta)

    _record_metrics(req.entity, req.factors)

    for f in req.factors:
        src = _prime_to_node(f.prime)
        dst = _prime_to_node(f.prime)  # self-loop for persistence
        # if delta !=0 we treat as *intent* to move; here we simplify to self
        # real use-case: user supplies *target* node and we compute delta
        allowed, via_c = _legalise_transition(src, dst)
        if not allowed:
            raise HTTPException(422, f"Transition {src}→{dst} never allowed")
        edges.append(Edge(src=src, dst=dst, via_c=via_c, label=_label(src, dst)))
        lawful_factors.append((f.prime, f.delta))

    # write to ledger
    cycle = ledger.anchor(req.entity, lawful_factors)

    if req.text:
        doc = ledger.entity_document(req.entity)
        slots = doc.get("slots") if isinstance(doc, dict) else None
        s1_slots = slots.get("S1") if isinstance(slots, dict) else None
        target_primes: Set[int] = set()
        if isinstance(s1_slots, dict):
            for payload in s1_slots.values():
                if not isinstance(payload, dict):
                    continue
                body_candidate = payload.get("body_prime")
                if body_candidate is not None:
                    try:
                        normalized_body = int(body_candidate)
                    except (TypeError, ValueError):
                        normalized_body = None
                    else:
                        if normalized_body >= 23:
                            target_primes.add(normalized_body)
                write_targets = payload.get("write_primes")
                if not isinstance(write_targets, list):
                    continue
                for candidate in write_targets:
                    try:
                        prime = int(candidate)
                    except (TypeError, ValueError):
                        continue
                    target_primes.add(prime)
        if not target_primes:
            target_primes.add(23)
        body_payload = {"content_type": "text/plain", "text": req.text}
        for prime in sorted(target_primes):
            try:
                ledger.update_body_slot(req.entity, prime, body_payload)
            except ValueError as exc:
                raise HTTPException(422, str(exc)) from exc
    energy = ledger.last_energy(req.entity)
    energy_payload: Dict[str, object] | None = None
    if energy is not None:
        energy_payload = {"entity": req.entity, **energy.as_payload()}
        logger.info(
            "E_t computed",
            extra={
                "entity": req.entity,
                "E_t": energy.total,
                "continuous": energy.continuous,
                "discrete_weighted": energy.weighted_discrete,
                "lambda": energy.lambda_weight,
            },
        )
        record_anchor_energy(
            req.entity, energy.total, energy.continuous, energy.weighted_discrete
        )
        with _metrics_lock:
            _last_anchor_energy = energy_payload
    else:
        with _metrics_lock:
            _last_anchor_energy = None
    _persist_memory_entry(req, ledger, timestamp=ts)
    if req.text:
        request.app.state.recall_store[req.entity] = req.text
    return {
        "status": "anchored",
        "edges": edges,
        "centroid_at_write": centroid,
        "timestamp": ts,
        "cycle": cycle.as_dict() if cycle else None,
        "energy": energy_payload,
    }


@app.post("/query")
@limiter.limit("200/minute")
def query(req: QueryReq, request: Request, _: str = Depends(require_key)):
    ledger = get_ledger(_ledger_id(request))
    hits = ledger.query(req.primes)
    return {"results": [{"entity": e, "weight": w} for e, w in hits]}


@app.post("/rotate", response_model=RotateResp)
def rotate(req: RotateReq, request: Request, _: str = Depends(require_key)):
    """Rotate the eight-prime exponent lattice via quaternion conjugation."""
    ledger = get_ledger(_ledger_id(request))
    original_checksum = ledger.checksum(req.entity)

    exps = [0] * len(PRIME_ARRAY)
    for prime, exp in ledger.factors(req.entity):
        idx = PRIME_IDX.get(prime)
        if idx is not None:
            exps[idx] = exp

    q1, q2, norm1, norm2 = core_rs.py_pack_quaternion(exps)
    cycles_before = core_rs.py_energy_proxy()
    q1_new, q2_new = core_rs.py_rotate_quaternion(q1, q2, req.axis, req.angle)
    cycles_after = core_rs.py_energy_proxy()
    new_exps = core_rs.py_unpack_quaternion(q1_new, q2_new, norm1, norm2)

    cmds = list(zip(PRIME_ARRAY, new_exps))
    ledger.anchor_batch(req.entity, cmds)

    rotated_checksum = ledger.checksum(req.entity)
    return RotateResp(
        original_checksum=original_checksum,
        rotated_checksum=rotated_checksum,
        energy_cycles=int(cycles_after - cycles_before),
    )


def _pick_latest_body_text(doc: Dict[str, Any]) -> str | None:
    slots = doc.get("slots")
    if not isinstance(slots, dict):
        return None
    body_slots = slots.get("body")
    if not isinstance(body_slots, dict):
        return None

    newest_text: str | None = None
    newest_updated_at = -1

    for payload in body_slots.values():
        if not isinstance(payload, dict):
            continue
        text = payload.get("text")
        if not isinstance(text, str) or not text.strip():
            continue
        updated_at_raw = payload.get("updated_at")
        updated_at = 0
        if isinstance(updated_at_raw, (int, float)):
            updated_at = int(updated_at_raw)
        else:
            try:
                updated_at = int(updated_at_raw)
            except (TypeError, ValueError):
                updated_at = 0
        if updated_at > newest_updated_at:
            newest_text = text
            newest_updated_at = updated_at

    return newest_text


def _latest_memory_text(ledger: Ledger, entity: str) -> str | None:
    prefix = f"{entity}:".encode()
    latest_text: str | None = None
    latest_timestamp = -1

    for _, raw_value in ledger.qp_iter(prefix):
        try:
            decoded = json.loads(raw_value.decode())
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue

        if not isinstance(decoded, dict):
            continue

        text = decoded.get("text")
        if not isinstance(text, str) or not text.strip():
            continue

        ts_raw = decoded.get("timestamp")
        ts = 0
        if isinstance(ts_raw, (int, float)):
            ts = int(ts_raw)
        else:
            try:
                ts = int(ts_raw)
            except (TypeError, ValueError):
                ts = 0

        if ts > latest_timestamp:
            latest_timestamp = ts
            latest_text = text

    return latest_text


@app.get("/retrieve")
def recall_last(entity: str, request: Request, _: str = Depends(require_key)):
    """Return the most recently anchored raw text for ``entity``."""

    ledger = get_ledger(_ledger_id(request))
    doc = ledger.entity_document(entity)
    text = _pick_latest_body_text(doc)

    if text is None:
        text = _latest_memory_text(ledger, entity)

    if text is None:
        text = request.app.state.recall_store.get(entity)

    if text is None:
        raise HTTPException(404, detail="Not Found")

    request.app.state.recall_store[entity] = text
    return {"entity": entity, "text": text}


# ----------  persistent memory log  ----------
@app.post("/memories", include_in_schema=False)
def persist_memory(req: AnchorReq, request: Request, _: str = Depends(require_key)):
    """
    Store every transcript in Qp column family keyed by entity|timestamp_ms.
    """
    ledger = get_ledger(_ledger_id(request))
    result = _persist_memory_entry(req, ledger)
    if result is None:
        return {"stored": False}
    return result


@app.get("/memories", include_in_schema=False)
def memories(
    request: Request,
    entity: str = Query("demo_user"),
    since: int = Query(0, ge=0),
    until: int | None = Query(None, ge=0),
    limit: int = Query(10, ge=1, le=100),
    _: str = Depends(require_key),
):
    """
    Return chronologically descending list of memories for entity in [since, until].
    """
    ledger = get_ledger(_ledger_id(request))
    prefix = f"{entity}:".encode()
    entries: List[dict] = []
    upper_bound = until or int(time.time() * 1000)

    for raw_key, raw_value in ledger.qp_iter(prefix):
        try:
            _, ts_part = raw_key.split(b":", 1)
            ts = int(ts_part)
        except (ValueError, IndexError):
            continue
        if ts < since or ts > upper_bound:
            continue
        try:
            decoded = json.loads(raw_value.decode())
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(decoded, dict) and "primes" in decoded:
            decoded["prime_annotations"] = annotate_prime_list(decoded.get("primes", []))
        entries.append(decoded)

    entries.sort(key=lambda item: item.get("timestamp", 0), reverse=True)
    return entries[:limit]


@app.get("/checksum")
def checksum(entity: str, request: Request, _: str = Depends(require_key)):
    ledger = get_ledger(_ledger_id(request))
    return {"entity": entity, "checksum": ledger.checksum(entity)}


@app.get("/ledger")
def ledger_snapshot(entity: str, request: Request, _: str = Depends(require_key)):
    """Return the persisted exponent vector for ``entity``."""

    ledger = get_ledger(_ledger_id(request))
    return _ledger_response(ledger, entity)


# ---------- new traverse endpoint (unchanged logic) ----------
@app.post("/traverse", response_model=TraverseResp)
@limiter.limit("300/minute")
def traverse(
    request: Request,
    start: int = Query(..., ge=0, le=7),
    depth: int = Query(3, ge=1, le=10),
):
    centroid = _centroid_now()
    flips = 0
    current = start
    path: List[Edge] = []

    for _ in range(depth):
        # pick first legal outbound edge (deterministic)
        dst = next(
            (
                dst
                for src, dst in [(current, d) for d in range(8)]
                if _transition_allowed(src, dst)
            ),
            None,
        )
        if dst is None:
            raise HTTPException(422, f"No legal outbound edge from node {current}")
        via_c = (
            (current % 2 == 0 and dst % 2 == 1)
            and (current, dst) not in _ALLOWED_DIRECT
        )
        path.append(Edge(src=current, dst=dst, via_c=via_c, label=_label(current, dst)))
        if via_c:
            centroid ^= 1
            flips += 1
        current = dst

    return TraverseResp(edges=path, centroid_flips=flips, final_centroid=centroid)


# ---------- health ----------
@app.get("/health", include_in_schema=False)
def health() -> dict[str, str]:
    """Report a simple ready status for HTTP health checks."""

    return {"status": "ok"}


@app.get("/centroid")
def centroid_now():
    return {"centroid": _centroid_now()}


@app.get("/metrics")
def metrics(_: str = Depends(require_key)):
    """Expose live demo counters for the Streamlit chassis."""

    with _metrics_lock:
        saved = tokens_saved
        total = total_calls
        dup = duplicate_calls
        last_energy = dict(_last_anchor_energy) if _last_anchor_energy else None

    integrity = 1.0 if total == 0 else max(0.0, 1 - (dup / total))
    return {
        "tokens_deduped": saved,
        "ledger_integrity": integrity,
        "last_anchor_energy": last_energy,
    }


@app.post("/qp/{key}")
def qp_put(key: str, req: QpPut, request: Request, _: str = Depends(require_key)):
    """Store a value in the Qp column family."""
    try:
        key_bytes = bytes.fromhex(key)
        if len(key_bytes) != 16:
            raise ValueError
    except ValueError:
        raise HTTPException(422, "Key must be a 16-byte hex string.")
    ledger = get_ledger(_ledger_id(request))
    ledger.qp_put(key_bytes, req.value)
    return {"status": "ok"}


@app.get("/qp/{key}")
def qp_get(key: str, request: Request, _: str = Depends(require_key)):
    """Retrieve a value from the Qp column family."""
    try:
        key_bytes = bytes.fromhex(key)
        if len(key_bytes) != 16:
            raise ValueError
    except ValueError:
        raise HTTPException(422, "Key must be a 16-byte hex string.")
    ledger = get_ledger(_ledger_id(request))
    value = ledger.qp_get(key_bytes)
    if value is None:
        raise HTTPException(404, "Key not found.")
    return {"key": key, "value": value}


@app.post("/salience")
@limiter.limit("200/minute")
def store_if_salient(req: SalienceReq, request: Request, _: str = Depends(require_key)):
    """Score ``utterance`` and persist to Qp when salient."""

    utterance = req.utterance.strip()
    if not utterance:
        raise HTTPException(422, "Utterance must not be empty.")

    score = float(request.app.state.s1_salience.score(utterance))
    threshold = SALIENT_THRESHOLD if req.threshold is None else float(req.threshold)
    threshold = max(0.0, min(1.0, threshold))
    if score > threshold:
        key_bytes = deterministic_key(utterance)
        payload = json.dumps({
            "text": utterance,
            "t": req.timestamp or time.time(),
            "score": score,
        })
        ledger = get_ledger(_ledger_id(request))
        ledger.qp_put(key_bytes, payload)
        return {
            "stored": True,
            "key": key_bytes.hex(),
            "len": len(utterance),
            "score": score,
            "text": utterance,
            "threshold": threshold,
        }

    return {"stored": False, "score": score, "text": utterance, "threshold": threshold}


@app.get("/exact/{key}")
@limiter.limit("300/minute")
def exact_memory(key: str, request: Request, _: str = Depends(require_key)):
    """Return the stored JSON payload for ``key`` from the Qp column family."""

    try:
        key_bytes = bytes.fromhex(key)
        if len(key_bytes) != 16:
            raise ValueError
    except ValueError:
        raise HTTPException(422, "Key must be a 16-byte hex string.")

    ledger = get_ledger(_ledger_id(request))
    value = ledger.qp_get(key_bytes)
    if value is None:
        raise HTTPException(404, "Key not found.")

    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return {"text": value}


@app.websocket("/pcm")
async def pcm_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_bytes()
            await websocket.send_bytes(data) # echo for spectrogram
    except WebSocketDisconnect:
        pass
