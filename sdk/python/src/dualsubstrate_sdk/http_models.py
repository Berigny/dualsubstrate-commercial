"""Typed HTTP response models for DualSubstrate REST helpers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, NoReturn


class PayloadValidationError(ValueError):
    """Raised when a JSON payload cannot be coerced into the expected schema."""


def _coerce_int(value: Any, *, field: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise PayloadValidationError(f"Field '{field}' must be an integer") from exc


def _coerce_bool(value: Any, *, field: str) -> bool:
    if isinstance(value, bool):
        return value
    if value in {0, 1}:
        return bool(value)
    raise PayloadValidationError(f"Field '{field}' must be a boolean")


def _coerce_str(value: Any, *, field: str) -> str:
    if isinstance(value, str):
        return value
    raise PayloadValidationError(f"Field '{field}' must be a string")


def _raise_edge_error(index: int) -> NoReturn:
    raise PayloadValidationError(
        f"Edge at position {index} must be an object with 'src', 'dst', 'via_c', and 'label'"
    )


@dataclass(frozen=True)
class TraverseEdge:
    """Single outbound edge returned by the ``/traverse`` endpoint."""

    src: int
    dst: int
    via_c: bool
    label: str

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TraverseEdge":
        src = _coerce_int(data.get("src"), field="src")
        dst = _coerce_int(data.get("dst"), field="dst")
        if not 0 <= src <= 7:
            raise PayloadValidationError("Field 'src' must be between 0 and 7")
        if not 0 <= dst <= 7:
            raise PayloadValidationError("Field 'dst' must be between 0 and 7")
        via_c = _coerce_bool(data.get("via_c"), field="via_c")
        label = _coerce_str(data.get("label"), field="label")
        return cls(src=src, dst=dst, via_c=via_c, label=label)

    def to_dict(self) -> dict[str, Any]:
        return {
            "src": self.src,
            "dst": self.dst,
            "via_c": self.via_c,
            "label": self.label,
        }


@dataclass(frozen=True)
class TraverseResponse:
    """Structured response payload returned by ``/traverse``."""

    edges: tuple[TraverseEdge, ...]
    centroid_flips: int
    final_centroid: Literal[0, 1]

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TraverseResponse":
        try:
            raw_edges_obj = data.get("edges", [])
        except AttributeError as exc:
            raise PayloadValidationError("Field 'edges' must be iterable") from exc

        if not isinstance(raw_edges_obj, Iterable):
            raise PayloadValidationError("Field 'edges' must be iterable")

        edges = tuple(
            TraverseEdge.from_dict(edge) if isinstance(edge, Mapping) else _raise_edge_error(idx)
            for idx, edge in enumerate(raw_edges_obj)
        )
        centroid_flips = _coerce_int(data.get("centroid_flips"), field="centroid_flips")
        final_centroid_raw = _coerce_int(data.get("final_centroid"), field="final_centroid")
        if final_centroid_raw not in (0, 1):
            raise PayloadValidationError("Field 'final_centroid' must be either 0 or 1")

        final_centroid: Literal[0, 1] = 0 if final_centroid_raw == 0 else 1

        return cls(edges=edges, centroid_flips=centroid_flips, final_centroid=final_centroid)

    def to_dict(self) -> dict[str, Any]:
        return {
            "edges": [edge.to_dict() for edge in self.edges],
            "centroid_flips": self.centroid_flips,
            "final_centroid": self.final_centroid,
        }


__all__ = ["PayloadValidationError", "TraverseEdge", "TraverseResponse"]
