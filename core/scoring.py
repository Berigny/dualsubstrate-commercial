"""S2 facet scoring utilities backed by ℝ-substrate heuristics."""

from __future__ import annotations

from typing import Dict, List

try:  # pragma: no cover - exercised only when helpers are missing at runtime
    from r_substrate import (  # type: ignore
        compute_coherence_delta,
        compute_drift_delta,
        compute_energy_delta,
        compute_retention_delta,
    )
except ImportError:  # pragma: no cover - fallback for test environments
    def _missing_helper(*_args: object, **_kwargs: object) -> float:
        raise RuntimeError("ℝ-substrate delta helpers are unavailable")

    compute_energy_delta = _missing_helper
    compute_drift_delta = _missing_helper
    compute_retention_delta = _missing_helper
    compute_coherence_delta = _missing_helper


def _facet_summaries(facets: Dict[str, Dict]) -> List[str]:
    """Return the non-empty ``summary`` fields from ``facets``."""

    summaries: List[str] = []
    for payload in facets.values():
        if not isinstance(payload, dict):
            continue
        raw = payload.get("summary")
        if not isinstance(raw, str):
            continue
        text = raw.strip()
        if text:
            summaries.append(text)
    return summaries


def score_s2_facets(entity: str, facets: Dict[str, Dict]) -> Dict[str, float]:
    """Compute ℝ-substrate deltas for ``entity`` based on ``facets``."""

    summaries = _facet_summaries(facets or {})

    delta_e = float(compute_energy_delta(entity, summaries))
    delta_drift = float(compute_drift_delta(entity, summaries))
    delta_retention = float(compute_retention_delta(entity, summaries))
    k_delta = float(compute_coherence_delta(entity, summaries))

    return {
        "ΔE": delta_e,
        "ΔDrift": delta_drift,
        "ΔRetention": delta_retention,
        "K": abs(k_delta),
    }


__all__ = ["score_s2_facets"]

