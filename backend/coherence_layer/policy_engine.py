"""Placeholder policy engine coordinating ledger and ethics layers."""

from __future__ import annotations

from typing import Callable, Dict, List, Optional

from backend.ethics_layer.law import Law
from backend.ethics_layer.grace import GraceModel
from backend.fieldx_kernel import LedgerEntry, LedgerKey, LedgerStore


class PolicyEngine:
    """Coordinates policy checks before storing or acting on entries."""

    def __init__(
        self,
        ledger_store: LedgerStore,
        laws: Optional[List[Law]] = None,
        grace: Optional[GraceModel] = None,
    ) -> None:
        self.ledger_store = ledger_store
        self.laws = laws or []
        self.grace = grace or GraceModel()
        self._callbacks: List[Callable[[LedgerEntry], None]] = []

    def register_callback(self, callback: Callable[[LedgerEntry], None]) -> None:
        """Register a callback to be invoked after successful evaluation."""

        self._callbacks.append(callback)

    def evaluate(self, key: LedgerKey) -> Dict[str, float]:
        """Score the provided key against known laws with grace applied."""

        entry = self.ledger_store.get(key)
        if entry is None:
            return {"lawfulness": 0.0, "grace": 0.0}

        lawfulness = sum(law.evaluate(entry.state) for law in self.laws)
        grace_score = self.grace.mediate(lawfulness)
        return {"lawfulness": lawfulness, "grace": grace_score}

    def record(self, entry: LedgerEntry) -> None:
        """Store the entry after running policy callbacks."""

        self.ledger_store.upsert(entry)
        for callback in self._callbacks:
            callback(entry)
