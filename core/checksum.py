"""Merkle checksum utilities for (prime, digits) tuples."""

from __future__ import annotations

import hashlib
from typing import Iterable, Tuple


def merkle_leaf(prime: int, digits: Iterable[int]) -> bytes:
    """Compute a deterministic leaf hash for a prime and digit payload."""
    digest = hashlib.sha256()
    digest.update(str(prime).encode("ascii"))
    digest.update(b":")
    digest.update(",".join(str(d) for d in digits).encode("ascii"))
    return digest.digest()


def merkle_root(items: Iterable[Tuple[int, Iterable[int]]]) -> bytes:
    """Compute a binary Merkle root over the provided items."""
    leaves = [merkle_leaf(prime, digits) for prime, digits in items]
    if not leaves:
        return hashlib.sha256(b"").digest()
    layer = leaves
    while len(layer) > 1:
        next_layer = []
        for index in range(0, len(layer), 2):
            left = layer[index]
            right = layer[index + 1] if index + 1 < len(layer) else left
            digest = hashlib.sha256()
            digest.update(left)
            digest.update(right)
            next_layer.append(digest.digest())
        layer = next_layer
    return layer[0]

