"""
DualSubstrate API – ledger + Metatron-star flow-rule enforcement
"""
from fastapi import FastAPI, HTTPException, Depends, Query, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, conint
from typing import Callable, Dict, List, Literal, Set, Tuple
import json
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


# ---------- models ----------
Prime = conint(ge=2, le=19)  # S0 primes only for MVP


class Factor(BaseModel):
    prime: Prime
    delta: int  # can be negative


class AnchorReq(BaseModel):
    entity: str
    factors: List[Factor]
    text: str | None = None


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


# ---------- helpers ----------
Node = Literal[0, 1, 2, 3, 4, 5, 6, 7]
PRIME_IDX = {p: idx for idx, p in enumerate(PRIME_ARRAY)}


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


# ---------- FastAPI ----------
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.ledger = Ledger()
    app.state.s1_salience = S1Salience()
    app.state.recall_store: Dict[str, str] = {}
    yield
    app.state.ledger.close()

app = FastAPI(title="DualSubstrate – Flow-Rule Ledger", version="0.3.0", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)
SALIENT_THRESHOLD = 0.7


@app.get("/")
def root():
    return {"message": "DualSubstrate /traverse ready"}


# ---------- existing endpoints ----------
@app.post("/anchor")
@limiter.limit("100/minute")
def anchor(req: AnchorReq, request: Request, _: str = Depends(require_key)):
    """
    1. map primes → nodes
    2. enforce flow-rules (auto-route via C if needed)
    3. write only lawful deltas
    """
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
    request.app.state.ledger.anchor(req.entity, lawful_factors)
    if req.text:
        request.app.state.recall_store[req.entity] = req.text
    return {
        "status": "anchored",
        "edges": edges,
        "centroid_at_write": centroid,
        "timestamp": ts,
    }


@app.post("/query")
@limiter.limit("200/minute")
def query(req: QueryReq, request: Request, _: str = Depends(require_key)):
    hits = request.app.state.ledger.query(req.primes)
    return {"results": [{"entity": e, "weight": w} for e, w in hits]}


@app.post("/rotate", response_model=RotateResp)
def rotate(req: RotateReq, request: Request, _: str = Depends(require_key)):
    """Rotate the eight-prime exponent lattice via quaternion conjugation."""
    original_checksum = request.app.state.ledger.checksum(req.entity)

    exps = [0] * len(PRIME_ARRAY)
    for prime, exp in request.app.state.ledger.factors(req.entity):
        idx = PRIME_IDX.get(prime)
        if idx is not None:
            exps[idx] = exp

    q1, q2, norm1, norm2 = core_rs.py_pack_quaternion(exps)
    cycles_before = core_rs.py_energy_proxy()
    q1_new, q2_new = core_rs.py_rotate_quaternion(q1, q2, req.axis, req.angle)
    cycles_after = core_rs.py_energy_proxy()
    new_exps = core_rs.py_unpack_quaternion(q1_new, q2_new, norm1, norm2)

    cmds = list(zip(PRIME_ARRAY, new_exps))
    request.app.state.ledger.anchor_batch(req.entity, cmds)

    rotated_checksum = request.app.state.ledger.checksum(req.entity)
    return RotateResp(
        original_checksum=original_checksum,
        rotated_checksum=rotated_checksum,
        energy_cycles=int(cycles_after - cycles_before),
    )


@app.get("/retrieve")
def recall_last(entity: str, request: Request, _: str = Depends(require_key)):
    """Return the most recently anchored raw text for ``entity``."""

    text = request.app.state.recall_store.get(entity)
    if text is None:
        raise HTTPException(404, detail="Not Found")
    return {"entity": entity, "text": text}


@app.get("/checksum")
def checksum(entity: str, request: Request, _: str = Depends(require_key)):
    return {"entity": entity, "checksum": request.app.state.ledger.checksum(entity)}


@app.get("/ledger")
def ledger_snapshot(entity: str, request: Request, _: str = Depends(require_key)):
    """Return the persisted exponent vector for ``entity``."""

    factors = request.app.state.ledger.factors(entity)
    return {"entity": entity, "factors": [{"prime": p, "value": v} for p, v in factors]}


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

    integrity = 1.0 if total == 0 else max(0.0, 1 - (dup / total))
    return {"tokens_deduped": saved, "ledger_integrity": integrity}


@app.post("/qp/{key}")
def qp_put(key: str, req: QpPut, request: Request, _: str = Depends(require_key)):
    """Store a value in the Qp column family."""
    try:
        key_bytes = bytes.fromhex(key)
        if len(key_bytes) != 16:
            raise ValueError
    except ValueError:
        raise HTTPException(422, "Key must be a 16-byte hex string.")
    request.app.state.ledger.qp_put(key_bytes, req.value)
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
    value = request.app.state.ledger.qp_get(key_bytes)
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
        request.app.state.ledger.qp_put(key_bytes, payload)
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

    value = request.app.state.ledger.qp_get(key_bytes)
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
