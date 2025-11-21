"""Unity metric placeholder capturing binary distance behavior."""

from __future__ import annotations


def unity_metric(value: float) -> float:
    """Collapse a signal to unit magnitude if it is non-zero."""

    return 1.0 if abs(value) > 0 else 0.0
