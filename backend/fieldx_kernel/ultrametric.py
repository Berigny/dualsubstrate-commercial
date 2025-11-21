"""Ultrametric utilities for comparing hierarchical states."""

from __future__ import annotations

from typing import Iterable


def ultrametric_distance(a: Iterable[float], b: Iterable[float]) -> float:
    """Return a max-difference ultrametric distance between two vectors."""

    differences = [abs(x - y) for x, y in zip(a, b)]
    if not differences:
        return 0.0
    return max(differences)
