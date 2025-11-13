"""Lightweight registry for per-ID Ledger instances."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict

from core.ledger import Ledger

BASE_LEDGER_ROOT = Path(os.getenv("LEDGER_ROOT", "./data/ledgers")).resolve()
_LEDGERS: Dict[str, Ledger] = {}


def get_ledger(ledger_id: str) -> Ledger:
    """Return (and cache) a Ledger bound to ``ledger_id``."""
    if ledger_id in _LEDGERS:
        return _LEDGERS[ledger_id]

    base = BASE_LEDGER_ROOT / ledger_id
    base.mkdir(parents=True, exist_ok=True)
    factors = base / "factors"
    postings = base / "postings"
    slots = base / "slots"
    inference = base / "inference"
    factors.parent.mkdir(parents=True, exist_ok=True)
    postings.parent.mkdir(parents=True, exist_ok=True)
    slots.parent.mkdir(parents=True, exist_ok=True)
    inference.parent.mkdir(parents=True, exist_ok=True)
    ledger = Ledger(
        event_log_path=base / "event.log",
        factors_path=factors,
        postings_path=postings,
        slots_path=slots,
        inference_path=inference,
    )
    _LEDGERS[ledger_id] = ledger
    return ledger


def list_ledgers() -> Dict[str, str]:
    """Return the currently opened ledger IDs."""
    return {ledger_id: str(BASE_LEDGER_ROOT / ledger_id) for ledger_id in _LEDGERS}


def close_all() -> None:
    """Close every cached Ledger."""
    for ledger in _LEDGERS.values():
        ledger.close()
    _LEDGERS.clear()
