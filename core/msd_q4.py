"""Carry-free base-4 most-significant-digit arithmetic primitives."""

from __future__ import annotations

from typing import Iterable, List

Digit = int


def normalise_msd_q4(digits: Iterable[Digit]) -> List[Digit]:
    """Normalise a base-4 MSD digit sequence into canonical form."""
    raise NotImplementedError("TODO: implement carry-free normalisation")


def add_msd_q4(lhs: Iterable[Digit], rhs: Iterable[Digit]) -> List[Digit]:
    """Add two MSD digit sequences without propagating carries."""
    raise NotImplementedError("TODO: implement MSD base-4 addition")

