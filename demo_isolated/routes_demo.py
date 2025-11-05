from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse
from demo_isolated.token_prime import hash_sentence

try:  # pragma: no cover - optional dependency bridge
    from ledger import anchor_events
except ImportError:  # pragma: no cover - fallback for local dev
    from core.ledger import Ledger

    _DEMO_LEDGER = Ledger()

    def anchor_events(entity: str, factors):
        commands = [(factor["prime"], factor.get("k", 0)) for factor in factors]
        if commands:
            _DEMO_LEDGER.anchor(entity, commands)

try:  # pragma: no cover - optional dependency bridge
    from checksum import merkle_leaf
except ImportError:  # pragma: no cover - fallback for local dev
    from core.checksum import merkle_root

    def merkle_leaf(factors):
        leaves = [f"{f['prime']}:{f.get('k', 0)}".encode() for f in factors]
        return merkle_root(leaves) if leaves else ""

router = APIRouter(prefix="/demo", tags=["demo"])

_STATIC_DIR = Path(__file__).resolve().parent / "static"
_LAST_SENTENCE: dict[str, dict] = {}


@router.post("/anchor")
def demo_anchor(req: dict):
    factors = hash_sentence(req["text"])
    anchor_events("demo_user", factors)
    record = {"stored": True, "key": merkle_leaf(factors), "tokens": len(factors), "text": req["text"]}
    _LAST_SENTENCE["demo_user"] = record
    return record


@router.get("/metrics")
def demo_metrics():
    # dummy numbers for now – will read from Prometheus later
    return {"tokens_deduped": 42, "ledger_integrity": 0.996}


@router.get("/static/stt_min.js", include_in_schema=False)
def demo_stt_js():
    return FileResponse(_STATIC_DIR / "stt_min.js", media_type="application/javascript")


@router.get("/retrieve")
def demo_retrieve(entity: str):
    return _LAST_SENTENCE.get(entity, {"stored": False, "text": None})
