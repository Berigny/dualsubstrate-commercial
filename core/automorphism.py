"""Cycle automorphism helpers for enforcing Even→C→Odd stability."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Sequence, Tuple

from .flow_rule_bridge import node_for_prime
from .inference import InferenceSnapshot, InferenceStore


def _default_centroid_provider() -> int:
    """Return the ambient centroid digit based on wall-clock time."""

    return 0 if int(time.time() * 1000) % 2 == 0 else 1


@dataclass
class CycleStep:
    """Describe a single step within the Even→C→Odd automorphism cycle."""

    prime: int
    node: str
    parity: str
    centroid: int
    via_centroid: bool
    permutation: str | None
    rotor: str | None
    cycle_index: int

    def as_dict(self) -> Dict[str, object]:
        return {
            "prime": self.prime,
            "node": self.node,
            "parity": self.parity,
            "centroid": self.centroid,
            "via_centroid": self.via_centroid,
            "permutation": self.permutation,
            "rotor": self.rotor,
            "cycle_index": self.cycle_index,
        }


@dataclass
class CycleResult:
    """Summary of the automorphism cycle enforced for a batch of factors."""

    initial_centroid: int
    final_centroid: int
    flips: int
    steps: List[CycleStep]

    def as_dict(self) -> Dict[str, object]:
        return {
            "initial_centroid": self.initial_centroid,
            "final_centroid": self.final_centroid,
            "flips": self.flips,
            "steps": [step.as_dict() for step in self.steps],
        }


class CycleAutomorphismService:
    """Enforce Even→C→Odd automorphisms on the continuous inference state."""

    _PERMUTATION_LABEL = "swap_pair"
    _ROTOR_LABEL = "quarter_turn"

    def __init__(
        self,
        store: InferenceStore,
        *,
        primes: Sequence[int],
        centroid_provider: Callable[[], int] | None = None,
    ) -> None:
        self._store = store
        self._prime_index = {int(prime): idx for idx, prime in enumerate(primes)}
        self._centroid_provider = centroid_provider or _default_centroid_provider

    # ------------------------------------------------------------------
    # public API
    def empty_cycle(self) -> CycleResult:
        centroid = self._centroid_provider()
        return CycleResult(
            initial_centroid=centroid,
            final_centroid=centroid,
            flips=0,
            steps=[],
        )

    def derive_via_flags(self, primes: Sequence[int]) -> List[bool]:
        """Return parity-based centroid flags for ``primes``."""

        ordered = [int(p) for p in primes]
        flags = [False] * len(ordered)
        for idx in range(1, len(ordered)):
            prev = node_for_prime(ordered[idx - 1])
            curr = node_for_prime(ordered[idx])
            flags[idx] = prev.is_even != curr.is_even
        return flags

    def enforce(
        self,
        entity: str,
        primes: Sequence[int],
        via_flags: Sequence[bool],
        *,
        mutate_state: bool = True,
    ) -> CycleResult:
        """Apply automorphisms for ``primes`` and return the resulting cycle."""

        ordered_primes = [int(p) for p in primes]
        if not ordered_primes:
            return self.empty_cycle()

        initial_centroid = self._centroid_provider()
        centroid = initial_centroid
        steps: List[CycleStep] = []
        operations: List[Tuple[int, int]] = []
        flips = 0
        cycle_index = 0

        for idx, prime in enumerate(ordered_primes):
            node = node_for_prime(prime)
            via_centroid = via_flags[idx] if idx < len(via_flags) else False
            applied = False

            if via_centroid and not node.is_even:
                flips += 1
                cycle_index += 1
                centroid ^= 1
                pair = self._pair_indices(prime)
                if pair is not None:
                    operations.append(pair)
                applied = True

            steps.append(
                CycleStep(
                    prime=prime,
                    node=node.name,
                    parity="even" if node.is_even else "odd",
                    centroid=centroid,
                    via_centroid=via_centroid,
                    permutation=self._PERMUTATION_LABEL if applied else None,
                    rotor=self._ROTOR_LABEL if applied else None,
                    cycle_index=cycle_index,
                )
            )

        if mutate_state and operations:
            def mutator(snapshot: InferenceSnapshot) -> None:
                for even_idx, odd_idx in operations:
                    self._apply_permutation(snapshot, even_idx, odd_idx)
                    self._apply_rotor(snapshot, even_idx, odd_idx)

            self._store.mutate_state(entity, mutator)

        return CycleResult(
            initial_centroid=initial_centroid,
            final_centroid=centroid,
            flips=flips,
            steps=steps,
        )

    # ------------------------------------------------------------------
    # internal helpers
    def _pair_indices(self, prime: int) -> Tuple[int, int] | None:
        idx = self._prime_index.get(prime)
        if idx is None:
            return None
        if idx % 2 == 0:
            partner = idx + 1
        else:
            partner = idx - 1
        if partner < 0 or partner >= len(self._prime_index):
            return None
        even_idx = min(idx, partner)
        odd_idx = max(idx, partner)
        return even_idx, odd_idx

    def _apply_permutation(self, snapshot: InferenceSnapshot, even_idx: int, odd_idx: int) -> None:
        snapshot.x[even_idx], snapshot.x[odd_idx] = snapshot.x[odd_idx], snapshot.x[even_idx]
        for row in snapshot.readouts.values():
            row[even_idx], row[odd_idx] = row[odd_idx], row[even_idx]

    def _apply_rotor(self, snapshot: InferenceSnapshot, even_idx: int, odd_idx: int) -> None:
        even_val, odd_val = snapshot.x[even_idx], snapshot.x[odd_idx]
        snapshot.x[even_idx] = -odd_val
        snapshot.x[odd_idx] = even_val
        for row in snapshot.readouts.values():
            even_coeff, odd_coeff = row[even_idx], row[odd_idx]
            row[even_idx] = -odd_coeff
            row[odd_idx] = even_coeff


__all__ = [
    "CycleAutomorphismService",
    "CycleResult",
    "CycleStep",
]
