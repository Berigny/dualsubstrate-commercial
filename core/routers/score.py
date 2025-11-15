"""Routers for scoring S2 facet payloads."""

from __future__ import annotations

from typing import Any, Dict, Mapping

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from deps import require_key

router = APIRouter(prefix="/score", tags=["score"])


class ScoreS2Request(BaseModel):
    """Request body accepted by the ``POST /score/s2`` endpoint."""

    model_config = ConfigDict(extra="ignore")
    facets: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    entity: str | None = Field(default=None)


def _token_weight(value: Any) -> int:
    """Return a lightweight token estimate for ``value``."""

    if value is None:
        return 0
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return 0
        return len([token for token in stripped.replace("\n", " ").split(" ") if token])
    if isinstance(value, (int, float, bool)):
        return 1
    if isinstance(value, Mapping):
        return sum(_token_weight(v) for v in value.values())
    if isinstance(value, (list, tuple, set)):
        return sum(_token_weight(v) for v in value)
    return 1


def score_s2_facets(facets: Mapping[str, Mapping[str, Any]]) -> Dict[str, Any]:
    """Score a batch of S2 facets and produce heuristic metrics."""

    if not isinstance(facets, Mapping):
        raise HTTPException(422, "facets must be an object")

    breakdown: Dict[str, Dict[str, Any]] = {}
    total_tokens = 0
    total_fields = 0

    for prime_key, payload in facets.items():
        if not isinstance(payload, Mapping):
            raise HTTPException(422, f"Facet {prime_key!r} must be an object")
        tokens = _token_weight(payload)
        fields = len(payload)
        total_tokens += tokens
        total_fields += fields
        density = float(tokens) / float(fields or 1)
        breakdown[str(prime_key)] = {
            "tokens": tokens,
            "fields": fields,
            "density": round(density, 6),
        }

    coverage = len(breakdown)
    avg_tokens = float(total_tokens) / float(coverage or 1) if coverage else 0.0

    if coverage == 0:
        metrics = {
            "dE": -0.1,
            "dDrift": -0.05,
            "dRetention": 0.0,
            "K": 0.0,
            "coverage": 0,
            "token_total": 0,
            "facets_scored": breakdown,
        }
        return {"metrics": metrics}

    d_e = -0.12 * max(total_tokens, 1)
    d_drift = -0.08 * max(total_fields, 1)
    d_retention = 0.2 * coverage + 0.04 * avg_tokens
    k_complexity = 0.1 * total_fields + 0.05 * coverage

    metrics = {
        "dE": round(d_e, 6),
        "dDrift": round(d_drift, 6),
        "dRetention": round(d_retention, 6),
        "K": round(k_complexity, 6),
        "coverage": coverage,
        "token_total": total_tokens,
        "facets_scored": breakdown,
    }
    return {"metrics": metrics}


@router.post("/s2")
def score_s2(payload: ScoreS2Request, _: str = Depends(require_key)) -> Dict[str, Any]:
    """API entrypoint delegating to :func:`score_s2_facets`."""

    return score_s2_facets(payload.facets)


__all__ = ["router", "score_s2_facets"]
