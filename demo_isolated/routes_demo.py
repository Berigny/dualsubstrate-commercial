from fastapi import APIRouter, Request, Depends
from demo_isolated.token_prime import hash_sentence
from typing import List, Tuple, Optional
from core.ledger import Ledger

router = APIRouter(prefix="/demo", tags=["demo"])

# In-memory store for "last item" logic, as the main ledger doesn't support "last".
DEMO_MEMORY = {}

# This dependency provides the ledger. It uses the app's instance if available,
# otherwise it creates a fallback singleton on the first request.
# This avoids the module-level instantiation that caused the build to fail.
_fallback_ledger: Optional[Ledger] = None
def get_ledger_dependency(request: Request) -> Ledger:
    if hasattr(request.app.state, "ledger") and request.app.state.ledger:
        return request.app.state.ledger

    # Fallback for standalone execution
    global _fallback_ledger
    if _fallback_ledger is None:
        _fallback_ledger = Ledger()
    return _fallback_ledger

def merkle_leaf(factors: List[Tuple[int, int]]) -> str:
    return "dummy_merkle_leaf"

@router.post("/anchor")
def demo_anchor(req: dict, ledger: Ledger = Depends(get_ledger_dependency)):
    factors_list_of_dicts = hash_sentence(req["text"])
    factors: List[Tuple[int, int]] = [(d['prime'], d['k']) for d in factors_list_of_dicts]

    ledger.anchor("demo_user", factors)

    # Store last text/factors for the retrieve endpoint
    DEMO_MEMORY["demo_user_last_text"] = req["text"]
    DEMO_MEMORY["demo_user_last_factors"] = factors

    return {"stored": True, "key": merkle_leaf(factors), "tokens": len(factors)}

@router.get("/metrics")
def demo_metrics():
    # These are dummy values as per the user's instructions.
    return {"tokens_deduped": 42, "ledger_integrity": 0.996}

@router.get("/retrieve")
def demo_retrieve(entity: str = "demo_user"):
    # The user's plan was to retrieve the *last* sentence. The ledger itself
    # doesn't have a simple "last" query. We'll use our in-memory cache for this.
    return {
        "entity": entity,
        "text": DEMO_MEMORY.get(f"{entity}_last_text", ""),
        "factors": DEMO_MEMORY.get(f"{entity}_last_factors", [])
    }
