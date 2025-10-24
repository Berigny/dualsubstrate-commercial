"""Shared quaternion helpers for REST and gRPC entrypoints."""

from __future__ import annotations

import math
from typing import Iterable, Sequence

_DEFAULT_VECTOR = (1.0, 0.0, 0.0)


def _normalize(q: Sequence[float]) -> tuple[float, float, float, float]:
    if len(q) != 4:
        raise ValueError("Quaternion must have four components (w, x, y, z)")
    w, x, y, z = (float(part) for part in q)
    norm = math.sqrt(w * w + x * x + y * y + z * z)
    if norm == 0:
        raise ValueError("Quaternion norm cannot be zero")
    return w / norm, x / norm, y / norm, z / norm


def _quat_multiply(a: Sequence[float], b: Sequence[float]) -> tuple[float, float, float, float]:
    aw, ax, ay, az = a
    bw, bx, by, bz = b
    return (
        aw * bw - ax * bx - ay * by - az * bz,
        aw * bx + ax * bw + ay * bz - az * by,
        aw * by - ax * bz + ay * bw + az * bx,
        aw * bz + ax * by - ay * bx + az * bw,
    )


def rotate(q: Sequence[float], vec: Iterable[float] | None = None) -> list[float]:
    """Rotate ``vec`` by quaternion ``q`` and return the rotated vector."""

    qw, qx, qy, qz = _normalize(q)
    vector = tuple(float(v) for v in (vec or _DEFAULT_VECTOR))
    if len(vector) != 3:
        raise ValueError("Vector must have exactly three components")

    pure = (0.0, vector[0], vector[1], vector[2])
    conj = (qw, -qx, -qy, -qz)
    rotated = _quat_multiply(_quat_multiply((qw, qx, qy, qz), pure), conj)
    return [rotated[1], rotated[2], rotated[3]]


__all__ = ["rotate"]
