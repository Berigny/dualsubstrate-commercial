from __future__ import annotations

from typing import Any, Dict

import pytest

from core import scoring


def test_score_s2_facets_delegates_to_r_substrate(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: Dict[str, Dict[str, Any]] = {}

    def _stub(name: str, value: float):
        def _inner(entity: str, summaries: list[str]) -> float:
            calls[name] = {"entity": entity, "summaries": summaries}
            return value

        return _inner

    monkeypatch.setattr(scoring, "compute_energy_delta", _stub("ΔE", -0.5))
    monkeypatch.setattr(scoring, "compute_drift_delta", _stub("ΔDrift", -0.25))
    monkeypatch.setattr(scoring, "compute_retention_delta", _stub("ΔRetention", 0.75))
    monkeypatch.setattr(scoring, "compute_coherence_delta", _stub("K", -1.2))

    facets = {
        "11": {"summary": "  Primary summary  "},
        "13": {"summary": "Secondary summary", "owner": "ops"},
        "17": {"scope": "none"},
        "19": {"summary": None},
    }

    result = scoring.score_s2_facets("aurora", facets)

    assert result == {
        "ΔE": -0.5,
        "ΔDrift": -0.25,
        "ΔRetention": 0.75,
        "K": 1.2,
    }

    assert set(calls.keys()) == {"ΔE", "ΔDrift", "ΔRetention", "K"}
    for payload in calls.values():
        assert payload["entity"] == "aurora"
        assert payload["summaries"] == ["Primary summary", "Secondary summary"]


def test_score_s2_facets_handles_missing_summaries(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: Dict[str, list[str]] = {}

    def _capture(name: str):
        def _inner(entity: str, summaries: list[str]) -> float:
            captured[name] = summaries
            return {"ΔE": -0.1, "ΔDrift": -0.05, "ΔRetention": 0.2, "K": 0.0}[name]

        return _inner

    monkeypatch.setattr(scoring, "compute_energy_delta", _capture("ΔE"))
    monkeypatch.setattr(scoring, "compute_drift_delta", _capture("ΔDrift"))
    monkeypatch.setattr(scoring, "compute_retention_delta", _capture("ΔRetention"))
    monkeypatch.setattr(scoring, "compute_coherence_delta", _capture("K"))

    result = scoring.score_s2_facets("aurora", {"11": {"owner": "ops"}})

    assert result == {
        "ΔE": -0.1,
        "ΔDrift": -0.05,
        "ΔRetention": 0.2,
        "K": 0.0,
    }

    for summaries in captured.values():
        assert summaries == []
