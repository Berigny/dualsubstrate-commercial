"""
Memory governance rules inspired by brand consilience controls.

Maps a continuous consilience score into governance zones and applies
action-specific policies (store vs inject) with audit notes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import List, Optional


class MemoryZone(Enum):
    """Governance zones derived from the current consilience score."""

    FLEXIBLE = "flexible"   # diverse / novel contexts
    ADAPTIVE = "adaptive"   # moderate overlap with established themes
    PROTECTED = "protected" # high overlap – potential redundancy


class MemoryAction(Enum):
    """Actions subject to governance."""

    STORE = auto()
    INJECT = auto()


@dataclass
class GovernanceDecision:
    allowed: bool
    zone: MemoryZone
    reason: str = ""
    requires_audit: bool = False
    requires_confirmation: bool = False
    notes: List[str] = field(default_factory=list)
    max_daily: Optional[int] = None


def consilience_zone(consilience_score: float) -> MemoryZone:
    """
    Map consilience score ([0, 1]) into a governance zone.

    High consilience → Protected (strong alignment with recent themes).
    Moderate consilience → Adaptive.
    Low consilience → Flexible (encourage exploration).
    """

    score = max(0.0, min(1.0, consilience_score))
    if score >= 0.7:
        return MemoryZone.PROTECTED
    if score >= 0.4:
        return MemoryZone.ADAPTIVE
    return MemoryZone.FLEXIBLE


def evaluate_memory_action(
    action: MemoryAction,
    consilience_score: float,
    daily_count: int,
) -> GovernanceDecision:
    """
    Evaluate whether a memory action is permitted under current governance rules.
    """

    zone = consilience_zone(consilience_score)
    notes: List[str] = []
    max_daily: Optional[int] = None
    requires_audit = False
    requires_confirmation = False

    if zone is MemoryZone.PROTECTED:
        notes.append("Protected memory zone: high thematic overlap.")
        if action is MemoryAction.STORE:
            max_daily = 3
            requires_audit = True
            notes.append("Limit storing near-duplicate memories; audit before committing.")
        else:  # INJECT
            max_daily = 2
            requires_confirmation = True
            notes.append("Confirm before injecting recurring memory into dialogue.")
    elif zone is MemoryZone.ADAPTIVE:
        notes.append("Adaptive memory zone: mixed novelty.")
        if action is MemoryAction.STORE:
            max_daily = 6
            requires_audit = True
            notes.append("Track adaptive stores to watch for drift.")
        else:
            max_daily = 4
            requires_audit = True
            notes.append("Inject with lightweight audit (log the event).")
    else:  # FLEXIBLE
        notes.append("Flexible memory zone: high novelty, exploration encouraged.")
        max_daily = None

    allowed = True
    reason = ""
    if max_daily is not None and daily_count >= max_daily:
        allowed = False
        reason = f"Daily limit reached for {zone.value} zone (max {max_daily})."
        notes.append(reason)

    return GovernanceDecision(
        allowed=allowed,
        zone=zone,
        reason=reason,
        requires_audit=requires_audit,
        requires_confirmation=requires_confirmation,
        notes=notes,
        max_daily=max_daily,
    )
