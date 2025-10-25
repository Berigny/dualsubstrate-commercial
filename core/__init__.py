"""Core logic modules for the DualSubstrate MVP."""

from __future__ import annotations

from importlib import import_module
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

__all__ = ["msd_q4", "ledger", "valuation", "checksum", "rotate", "storage", "core"]

if TYPE_CHECKING:  # pragma: no cover - for static analyzers only
    from . import checksum, ledger, msd_q4, rotate, valuation


def _build_core_fallback() -> Any:
    """Provide a lightweight shim when the Rust extension is unavailable."""

    def _ensure_eight(values: list[int]) -> list[int]:
        padded = list(values)[:8]
        if len(padded) < 8:
            padded.extend([0] * (8 - len(padded)))
        return padded

    def py_pack_quaternion(exps: list[int]) -> tuple[tuple[float, ...], tuple[float, ...], float, float]:
        padded = _ensure_eight([int(v) for v in exps])
        q1 = tuple(float(v) for v in padded[:4])
        q2 = tuple(float(v) for v in padded[4:])
        return q1, q2, 1.0, 1.0

    def py_rotate_quaternion(
        q1: tuple[float, ...],
        q2: tuple[float, ...],
        _axis: tuple[float, float, float],
        _angle: float,
    ) -> tuple[tuple[float, ...], tuple[float, ...]]:
        return q1, q2

    def py_unpack_quaternion(
        q1: tuple[float, ...],
        q2: tuple[float, ...],
        _norm1: float,
        _norm2: float,
    ) -> list[int]:
        values = list(q1) + list(q2)
        return [int(round(v)) for v in values]

    def py_energy_proxy() -> float:
        return 0.0

    return SimpleNamespace(
        py_pack_quaternion=py_pack_quaternion,
        py_rotate_quaternion=py_rotate_quaternion,
        py_unpack_quaternion=py_unpack_quaternion,
        py_energy_proxy=py_energy_proxy,
    )


def __getattr__(name: str) -> Any:
    """Dynamically import submodules on first access."""

    if name in {"msd_q4", "ledger", "valuation", "checksum", "rotate", "storage"}:
        module = import_module(f".{name}", __name__)
        globals()[name] = module
        return module
    if name == "core":
        try:
            module = import_module(f".{name}", __name__)
        except ModuleNotFoundError:  # pragma: no cover - fallback path
            module = _build_core_fallback()
        globals()[name] = module
        return module
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
