"""Valuation utilities for p-adic style weighting."""

from __future__ import annotations

from math import inf
from typing import Iterable


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

