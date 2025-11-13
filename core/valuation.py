"""Valuation utilities for p-adic style weighting and energy tracking."""

from __future__ import annotations

from dataclasses import dataclass
from math import inf
from typing import Dict, Iterable, Mapping, Sequence, Tuple


def v_p(n: int, p: int) -> int:
    """Return the exponent of prime p dividing n."""
    if p <= 1:
        raise ValueError("p must be a prime greater than 1")
    if n == 0:
        return inf
    n = abs(n)
    exponent = 0
    while n % p == 0:
        n //= p
        exponent += 1
    return exponent


def is_divisible(n: int, p: int) -> bool:
    """Check whether n is divisible by p."""
    return v_p(n, p) > 0 if n != 0 else True


def weighted_norm(digits: Iterable[int], p: int) -> float:
    """Compute a placeholder weighted norm for a digit stream."""
    raise NotImplementedError("TODO: define valuation weighting scheme")


@dataclass(frozen=True)
class EnergyBreakdown:
    """Container summarising the mixed energy functional ``E_t``."""

    total: float
    continuous: float
    discrete: float
    lambda_weight: float

    @property
    def weighted_discrete(self) -> float:
        """Return the λ-weighted discrete contribution."""

        return self.discrete * self.lambda_weight

    def as_payload(self) -> Dict[str, float]:
        """Return a JSON-serialisable payload describing the energy."""

        return {
            "total": self.total,
            "continuous": self.continuous,
            "discrete": self.discrete,
            "lambda_weight": self.lambda_weight,
            "discrete_weighted": self.weighted_discrete,
        }


def _dot(vec_a: Sequence[float], vec_b: Sequence[float]) -> float:
    return sum(float(a) * float(b) for a, b in zip(vec_a, vec_b))


def continuous_mismatch(
    state_vector: Sequence[float],
    readouts: Mapping[int, Sequence[float]],
    deltas: Iterable[Tuple[int, float]],
) -> float:
    """Return the continuous mismatch term ``0.5 Σ‖R_i x - Δ_i‖²``.

    The expression mirrors the quadratic error from the mixed substrate patent: for
    each ledger update we project the latent state ``x`` through the readout row
    ``R_i`` and measure the squared deviation from the observed exponent delta.
    """

    mismatch = 0.0
    for prime, delta in deltas:
        row = readouts.get(int(prime))
        if row is None:
            continue
        residual = _dot(row, state_vector) - float(delta)
        mismatch += 0.5 * residual * residual
    return mismatch


def discrete_exponent_cost(deltas: Iterable[Tuple[int, float]]) -> float:
    """Return the L¹ norm of the exponent updates used in ``E_t``."""

    return sum(abs(float(delta)) for _, delta in deltas)


def mixed_energy(
    state_vector: Sequence[float],
    readouts: Mapping[int, Sequence[float]],
    deltas: Iterable[Tuple[int, float]],
    *,
    lambda_weight: float = 1.0,
) -> EnergyBreakdown:
    """Compute the mixed energy functional ``E_t`` for a batch of updates.

    ``E_t`` combines the continuous mismatch with a λ-weighted discrete penalty
    on exponent changes, matching the mixed substrate formulation described in the
    patent documentation.
    """

    materialised = [(int(prime), float(delta)) for prime, delta in deltas]
    mismatch = continuous_mismatch(state_vector, readouts, materialised)
    discrete = discrete_exponent_cost(materialised)
    total = mismatch + lambda_weight * discrete
    return EnergyBreakdown(
        total=total,
        continuous=mismatch,
        discrete=discrete,
        lambda_weight=lambda_weight,
    )


__all__ = [
    "EnergyBreakdown",
    "continuous_mismatch",
    "discrete_exponent_cost",
    "is_divisible",
    "mixed_energy",
    "v_p",
    "weighted_norm",
]

