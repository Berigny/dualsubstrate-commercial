from fastapi import APIRouter, Request
from demo_isolated.token_prime import hash_sentence
from typing import List, Tuple

# This is a simple in-memory store to hold the last anchored text for the demo.
# This avoids modifying the core ledger logic to retrieve the "last" item,
# which may not be a supported operation.
DEMO_MEMORY = {}

try:  # existing ledger
    from core.ledger import Ledger
    # We can't use the app state ledger here because we want this to be a fallback.
    # The user's instructions were to have a self-contained demo.
    # When running with DEMO_ROUTES=on, the main app's ledger will be used.
    # When running this file directly, a local ledger will be created.
    _db = Ledger()
    anchor_events = _db.anchor
    merkle_leaf = lambda f: "dummy_merkle_leaf"
    get_factors = _db.factors
except ImportError:  # local fall-back
    def anchor_events(entity: str, factors: List[Tuple[int, int]]):
        DEMO_MEMORY[entity] = factors
    def get_factors(entity: str) -> List[Tuple[int, int]]:
        return DEMO_MEMORY.get(entity, [])
    merkle_leaf = lambda f: "dummy_merkle_leaf"


router = APIRouter(prefix="/demo", tags=["demo"])

@router.post("/anchor")
def demo_anchor(req: dict):
    factors_list_of_dicts = hash_sentence(req["text"])
    factors: List[Tuple[int, int]] = [(d['prime'], d['k']) for d in factors_list_of_dicts]
    anchor_events("demo_user", factors)
    DEMO_MEMORY["demo_user_last_text"] = req["text"] # Store the last text
    DEMO_MEMORY["demo_user_last_factors"] = factors
    return {"stored": True, "key": merkle_leaf(factors), "tokens": len(factors)}

@router.get("/metrics")
def demo_metrics():
    # These are dummy values as per the user's instructions.
    return {"tokens_deduped": 42, "ledger_integrity": 0.996}

@router.get("/retrieve")
def demo_retrieve(entity: str = "demo_user"):
    # The user's battle plan mentions a retrieve function that gets the *last*
    # sentence. The `factors` function in the ledger returns all factors, not
    # just the last one. So, we'll use our in-memory store for this.
    return {
        "entity": entity,
        "text": DEMO_MEMORY.get(f"{entity}_last_text", ""),
        "factors": DEMO_MEMORY.get(f"{entity}_last_factors", [])
    }
