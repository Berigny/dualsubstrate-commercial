"""Typed HTTP response models for DualSubstrate REST helpers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, NoReturn


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


def _coerce_float(value: Any, *, field: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise PayloadValidationError(f"Field '{field}' must be a float") from exc


def _coerce_metadata(value: Any, *, field: str) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    raise PayloadValidationError(f"Field '{field}' must be an object")


@dataclass(frozen=True)
class TraversePath:
    """Traversal path returned by the ``/traverse`` endpoint."""

    nodes: tuple[int, ...]
    weight: float
    metadata: dict[str, Any]

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TraversePath":
        try:
            raw_nodes = data.get("nodes", [])
        except AttributeError as exc:
            raise PayloadValidationError("Field 'nodes' must be iterable") from exc

        if not isinstance(raw_nodes, Iterable):
            raise PayloadValidationError("Field 'nodes' must be iterable")

        nodes: list[int] = []
        for idx, node in enumerate(raw_nodes):
            nodes.append(_coerce_int(node, field=f"nodes[{idx}]"))

        weight = _coerce_float(data.get("weight"), field="weight")
        metadata = _coerce_metadata(data.get("metadata"), field="metadata")
        return cls(nodes=tuple(nodes), weight=weight, metadata=metadata)

    def to_dict(self) -> dict[str, Any]:
        return {
            "nodes": list(self.nodes),
            "weight": self.weight,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class TraverseResponse:
    """Structured response payload returned by ``/traverse``."""

    origin: int | None
    paths: tuple[TraversePath, ...]
    metadata: dict[str, Any]
    supported: bool

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TraverseResponse":
        if not isinstance(data, Mapping):
            raise PayloadValidationError("Traverse response must be an object")

        try:
            raw_paths = data.get("paths", [])
        except AttributeError as exc:
            raise PayloadValidationError("Field 'paths' must be iterable") from exc

        if not isinstance(raw_paths, Iterable):
            raise PayloadValidationError("Field 'paths' must be iterable")

        paths: tuple[TraversePath, ...] = tuple(
            TraversePath.from_dict(path)
            if isinstance(path, Mapping)
            else _raise_path_error(idx)
            for idx, path in enumerate(raw_paths)
        )

        origin_raw = data.get("origin")
        origin = None if origin_raw is None else _coerce_int(origin_raw, field="origin")
        metadata = _coerce_metadata(data.get("metadata"), field="metadata")
        supported = _coerce_bool(data.get("supported", True), field="supported")

        return cls(origin=origin, paths=paths, metadata=metadata, supported=supported)

    def to_dict(self) -> dict[str, Any]:
        return {
            "origin": self.origin,
            "paths": [path.to_dict() for path in self.paths],
            "metadata": dict(self.metadata),
            "supported": self.supported,
        }


def _raise_path_error(index: int) -> NoReturn:
    raise PayloadValidationError(
        f"Path at position {index} must be an object with 'nodes', 'weight', and 'metadata'"
    )


__all__ = ["PayloadValidationError", "TraversePath", "TraverseResponse"]
