from pathlib import Path

import pytest

from core.ledger import Ledger


def _make_ledger(tmp_path: Path) -> Ledger:
    return Ledger(
        event_log_path=tmp_path / "event.log",
        factors_path=tmp_path / "factors",
        postings_path=tmp_path / "postings",
        slots_path=tmp_path / "slots",
        inference_path=tmp_path / "inference",
    )


def test_update_s2_slots_rejects_when_metrics_missing(tmp_path):
    ledger = _make_ledger(tmp_path)
    entity = "metricless"
    doc = ledger._default_slots_doc(entity)
    doc["r_metrics"].pop("dRetention")
    ledger._store_slots_doc(entity, doc)
    with pytest.raises(ValueError, match="r_metrics values for: dRetention"):
        ledger.update_s2_slots(entity, {"11": {"summary": "test"}})
    ledger.close()


def test_update_s2_slots_rejects_when_thresholds_not_met(tmp_path):
    ledger = _make_ledger(tmp_path)
    entity = "thresholds"
    ledger.update_r_metrics(
        entity,
        {
            "dE": -1.0,
            "dDrift": -0.2,
            "dRetention": 0.0,
            "K": 0.5,
        },
    )
    with pytest.raises(ValueError, match="ΔRetention > 0"):
        ledger.update_s2_slots(entity, {"11": {"summary": "test"}})
    ledger.close()


def test_update_s2_slots_accepts_when_metrics_pass(tmp_path):
    ledger = _make_ledger(tmp_path)
    entity = "passing"
    ledger.update_r_metrics(
        entity,
        {
            "dE": -1.2,
            "dDrift": -0.4,
            "dRetention": 0.8,
            "K": 0.0,
        },
    )
    facets = {"11": {"summary": "approved"}}
    doc = ledger.update_s2_slots(entity, facets)
    assert doc["slots"]["S2"]["11"]["summary"] == "approved"
    assert doc["tier"] == "S2"
    stored = ledger.entity_document(entity)
    assert stored["slots"]["S2"] == facets
    ledger.close()
