"""Lattice utilities for mapping high dimensional states to a grid."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Tuple


@dataclass(frozen=True)
class LatticePoint:
    """Represents a discrete point on an n-dimensional lattice."""

    coordinates: Tuple[int, ...]

    def neighbors(self) -> Iterable["LatticePoint"]:
        """Yield adjacent lattice points using Manhattan distance."""

        for idx, value in enumerate(self.coordinates):
            delta = list(self.coordinates)
            delta[idx] = value + 1
            yield LatticePoint(tuple(delta))
            delta[idx] = value - 1
            yield LatticePoint(tuple(delta))


class Lattice:
    """Minimal lattice wrapper to construct and trace coordinates."""

    def __init__(self, dimensions: int) -> None:
        if dimensions < 1:
            raise ValueError("dimensions must be positive")
        self.dimensions = dimensions

    def origin(self) -> LatticePoint:
        """Return the zeroed origin for the lattice size."""

        return LatticePoint(tuple(0 for _ in range(self.dimensions)))

    def trace_path(self, start: LatticePoint, steps: List[int]) -> LatticePoint:
        """Walk along a path of increments from a start point."""

        if len(steps) != self.dimensions:
            raise ValueError("step count must match lattice dimensions")
        coords = tuple(coord + step for coord, step in zip(start.coordinates, steps))
        return LatticePoint(coords)
