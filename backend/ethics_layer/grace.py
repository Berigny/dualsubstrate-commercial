"""Grace model softening rigid law evaluations."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class GraceModel:
    """Simple grace model that dampens penalties and rewards alignment."""

    forgiveness_factor: float = 0.2

    def mediate(self, lawfulness_score: float) -> float:
        """Reduce the raw score to simulate forgiveness."""

        return lawfulness_score * (1.0 - self.forgiveness_factor)
