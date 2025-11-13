"""Stateful inference utilities for maintaining continuous ledger projections."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Sequence, Tuple


@dataclass
class InferenceSnapshot:
    """Container representing the latent state ``x`` and readout matrices ``R_i``."""

    x: List[float]
    readouts: Dict[int, List[float]]

    def as_dict(self) -> Dict[str, object]:
        """Return a JSON-serialisable mapping."""

        return {
            "x": list(self.x),
            "R": {str(prime): list(row) for prime, row in self.readouts.items()},
        }


class InferenceStore:
    """Persistence-aware helper that tracks per-entity inference state.

    The implementation mirrors the gradient update rule described in the
    dual-substrate patent: for each observation ``(prime, delta)`` we minimise
    ``0.5 * ||R_i x - delta||^2`` via first-order updates of the latent state
    ``x`` and the corresponding readout row ``R_i``. The normalisation step keeps
    both vectors numerically stable between successive anchors.
    """

    KEY_PREFIX = b"inf:"

    def __init__(
        self,
        db: Any,
        *,
        primes: Sequence[int],
        dimension: int | None = None,
        learning_rate: float = 0.05,
    ) -> None:
        self._db = db
        self._primes = tuple(int(p) for p in primes)
        self._dimension = int(dimension or len(self._primes))
        if self._dimension <= 0:
            raise ValueError("InferenceStore dimension must be positive")
        self._learning_rate = float(learning_rate)
        self._prime_index = {prime: idx for idx, prime in enumerate(self._primes)}

    # ------------------------------------------------------------------
    # public API
    def snapshot(self, entity: str) -> InferenceSnapshot:
        """Return the current latent state for ``entity`` without mutation."""

        state = self._load(entity)
        return InferenceSnapshot(list(state.x), {k: list(v) for k, v in state.readouts.items()})

    def update(self, entity: str, observations: Sequence[Tuple[int, float]]) -> InferenceSnapshot:
        """Apply gradient updates for ``observations`` and persist the new state."""

        if not observations:
            return self.snapshot(entity)

        state = self._load(entity)
        for prime, value in observations:
            idx = self._prime_index.get(int(prime))
            if idx is None:
                continue  # ignore primes outside the configured lattice
            row = state.readouts.setdefault(int(prime), self._unit_row(idx))
            prediction = _dot(row, state.x)
            error = prediction - float(value)
            lr = self._learning_rate

            # Gradient for the latent state x: (R_i^T (R_i x - delta))
            grad_x = [error * coeff for coeff in row]
            # Gradient for the readout row R_i: ((R_i x - delta) * x^T)
            grad_r = [error * coord for coord in state.x]

            for j in range(self._dimension):
                state.x[j] -= lr * grad_x[j]
                row[j] -= lr * grad_r[j]

            state.readouts[int(prime)] = row
            _normalise(row)
        _normalise(state.x)
        self._store(entity, state)
        return state

    # ------------------------------------------------------------------
    # internal helpers
    def _load(self, entity: str) -> InferenceSnapshot:
        key = self.KEY_PREFIX + entity.encode("utf-8")
        raw = self._db.get(key)
        if raw is None:
            return InferenceSnapshot(self._zero_vector(), self._default_readouts())
        if isinstance(raw, str):
            payload = raw
        else:
            payload = raw.decode("utf-8")
        data = json.loads(payload)
        x = [float(v) for v in data.get("x", [])][: self._dimension]
        readouts = {
            int(prime): [float(v) for v in row][: self._dimension]
            for prime, row in data.get("R", {}).items()
        }
        if len(x) < self._dimension:
            x.extend(0.0 for _ in range(self._dimension - len(x)))
        for prime, idx in self._prime_index.items():
            readouts.setdefault(prime, self._unit_row(idx))
        return InferenceSnapshot(x, readouts)

    def _store(self, entity: str, state: InferenceSnapshot) -> None:
        payload = json.dumps(state.as_dict(), separators=(",", ":"), sort_keys=True)
        self._db.put(self.KEY_PREFIX + entity.encode("utf-8"), payload.encode("utf-8"))

    def _zero_vector(self) -> List[float]:
        return [0.0] * self._dimension

    def _unit_row(self, idx: int) -> List[float]:
        row = [0.0] * self._dimension
        if 0 <= idx < self._dimension:
            row[idx] = 1.0
        return row

    def _default_readouts(self) -> Dict[int, List[float]]:
        return {prime: self._unit_row(idx) for prime, idx in self._prime_index.items()}


def _dot(vec_a: Sequence[float], vec_b: Sequence[float]) -> float:
    return sum(float(a) * float(b) for a, b in zip(vec_a, vec_b))


def _normalise(vector: List[float] | Dict[int, float]) -> None:
    if isinstance(vector, list):
        norm = math.sqrt(sum(component * component for component in vector))
        if norm == 0.0:
            return
        for idx in range(len(vector)):
            vector[idx] /= norm
    else:
        norm = math.sqrt(sum(value * value for value in vector.values()))
        if norm == 0.0:
            return
        for key in list(vector.keys()):
            vector[key] /= norm


__all__ = ["InferenceSnapshot", "InferenceStore"]
