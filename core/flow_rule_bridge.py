"""Python bridge for the Metatron-star flow-rule static library."""
from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import List, Sequence, Tuple

try:
    from flow_rule import py_batch_allowed as _batch_allowed  # type: ignore
except ImportError:  # pragma: no cover - exercised when the Rust lib is absent
    _batch_allowed = None


class Node(IntEnum):
    S0 = 0
    S1 = 1
    S2 = 2
    S3 = 3
    S4 = 4
    S5 = 5
    S6 = 6
    S7 = 7

    @property
    def is_even(self) -> bool:
        return (int(self) % 2) == 0


# First eight primes map to S0-only nodes.
PRIME_TO_NODE = {
    2: Node.S0,
    3: Node.S1,
    5: Node.S2,
    7: Node.S3,
    11: Node.S4,
    13: Node.S5,
    17: Node.S6,
    19: Node.S7,
}


class FlowRuleViolation(ValueError):
    """Raised when a transition path violates the flow rule."""

    def __init__(self, offending: Tuple[Node, Node]):
        src, dst = offending
        super().__init__(f"flow-rule violation: {src.name}->{dst.name}")
        self.offending = offending


def _python_batch_allowed(edges: Sequence[Tuple[Node, Node]]) -> List[bool]:
    def allowed_direct(src: Node, dst: Node) -> bool:
        return (src, dst) in {
            (Node.S1, Node.S2),
            (Node.S5, Node.S6),
            (Node.S3, Node.S0),
            (Node.S7, Node.S4),
            (Node.S1, Node.S0),
        }

    def transition_allowed(src: Node, dst: Node) -> bool:
        if src == dst:
            return True
        if src.is_even and (not dst.is_even) and not allowed_direct(src, dst):
            return False
        return allowed_direct(src, dst) or (src.is_even == dst.is_even)

    return [transition_allowed(s, d) for s, d in edges]


def _batch(edges: Sequence[Tuple[Node, Node]]) -> List[bool]:
    if not edges:
        return []
    if _batch_allowed is not None:
        return _batch_allowed([(int(s), int(d)) for s, d in edges])
    return _python_batch_allowed(edges)


def node_for_prime(prime: int) -> Node:
    try:
        return PRIME_TO_NODE[prime]
    except KeyError as exc:  # pragma: no cover - guard rails
        raise ValueError(f"unsupported prime for S0 flow: {prime}") from exc


@dataclass
class TransitionCheck:
    transitions: List[Tuple[Node, Node]]
    via_centroid: List[bool]


def validate_prime_sequence(primes: Sequence[int]) -> TransitionCheck:
    """Validate ordered primes according to the flow rule."""
    nodes = [node_for_prime(p) for p in primes]
    transitions: List[Tuple[Node, Node]] = []
    via_flags: List[bool] = [False] * len(nodes)

    for i in range(1, len(nodes)):
        src, dst = nodes[i - 1], nodes[i]
        transitions.append((src, dst))
        via_flags[i] = src.is_even != dst.is_even

    results = _batch(transitions)
    for (src, dst), ok in zip(transitions, results):
        if not ok:
            raise FlowRuleViolation((src, dst))

    return TransitionCheck(transitions=transitions, via_centroid=via_flags)
