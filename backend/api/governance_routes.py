"""Routers exposing coherence and ethics evaluation services."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from backend.api.http import ledger_store
from backend.api.schemas import (
    ActionRequestSchema,
    CoherenceResponseSchema,
    PolicyDecisionSchema,
)
from backend.coherence_layer import PolicyEngine
from backend.ethics_layer import GraceModel, Law
from backend.fieldx_kernel.geometry import Lattice

router = APIRouter(tags=["governance"])


class CoherenceAnalyzer:
    """Lightweight analyzer leveraging lattice geometry."""

    def __init__(self, dimensions: int = 3) -> None:
        self.dimensions = max(1, dimensions)

    def evaluate(self, request: ActionRequestSchema) -> CoherenceResponseSchema:
        steps_raw = list(request.parameters.values())
        dims = max(len(steps_raw), self.dimensions)
        lattice = Lattice(dims)
        origin = lattice.origin()

        padded = steps_raw + [0.0] * (dims - len(steps_raw))
        steps = [int(round(value)) for value in padded[:dims]]
        point = lattice.trace_path(origin, steps)
        magnitude = sum(abs(step) for step in steps)
        coherence = 1.0 / (1.0 + magnitude)

        return CoherenceResponseSchema(
            action=request.action,
            coherence_score=coherence,
            lattice_point=list(point.coordinates),
            steps=steps,
        )


coherence_analyzer = CoherenceAnalyzer()
policy_engine = PolicyEngine(
    ledger_store=ledger_store, laws=[Law(name="alignment", weight=0.25)], grace=GraceModel()
)


@router.post("/coherence/evaluate", response_model=CoherenceResponseSchema)
def evaluate_coherence(request: ActionRequestSchema) -> CoherenceResponseSchema:
    """Return a coherence score derived from lattice traversal."""

    return coherence_analyzer.evaluate(request)


@router.post("/ethics/evaluate", response_model=PolicyDecisionSchema)
def evaluate_ethics(request: ActionRequestSchema) -> PolicyDecisionSchema:
    """Evaluate an action against registered policies and grace model."""

    if request.key is None:
        raise HTTPException(status_code=400, detail="Ledger key is required")

    scores = policy_engine.evaluate(request.key.to_model())
    permitted = scores.get("grace", 0.0) >= 0
    return PolicyDecisionSchema(
        action=request.action,
        key=request.key,
        lawfulness=scores.get("lawfulness", 0.0),
        grace=scores.get("grace", 0.0),
        permitted=permitted,
    )


__all__ = ["coherence_analyzer", "policy_engine", "router"]
