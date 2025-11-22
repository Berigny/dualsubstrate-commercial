"""Fuzzy retrieval that blends p-adic overlap with semantic similarity."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Mapping, MutableMapping, Protocol, Sequence

import numpy as np

logger = logging.getLogger(__name__)


class MemoryService(Protocol):
    """Protocol describing the memory service abstraction used by routers."""

    def get_all_memories(self, entity: str | None = None) -> Sequence[Mapping[str, object]]:
        ...

    def anchor(self, text: str, entity: str | None = None) -> Mapping[str, object] | Sequence[Mapping[str, object]]:  # pragma: no cover - optional
        ...


@dataclass(frozen=True)
class MemoryCandidate:
    """Lightweight wrapper for memory payloads used during ranking."""

    text: str
    factors: Sequence[Mapping[str, object]]
    payload: Mapping[str, object]


DEFAULT_SEMANTIC_MODEL = os.getenv("SENTENCE_TRANSFORMER_MODEL", "all-MiniLM-L6-v2")
DEFAULT_SEMANTIC_WEIGHT = 0.45


@lru_cache(maxsize=1)
def _get_encoder(model_name: str = DEFAULT_SEMANTIC_MODEL):
    """Lazily load the shared sentence transformer encoder."""

    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(model_name)
    logger.info("SentenceTransformer model loaded", extra={"model_name": model_name})
    return model


def _resolve_semantic_weight(value: float | None) -> float:
    """Resolve the semantic weight from a parameter or environment variable."""

    if value is None:
        env_value = os.getenv("SEMANTIC_WEIGHT") or os.getenv("DUALSUBSTRATE_SEMANTIC_WEIGHT")
        try:
            value = float(env_value) if env_value is not None else DEFAULT_SEMANTIC_WEIGHT
        except ValueError:
            logger.warning("Invalid semantic weight in environment; using default", extra={"env_value": env_value})
            value = DEFAULT_SEMANTIC_WEIGHT

    if not 0.0 <= value <= 1.0:
        raise ValueError("semantic_weight must be between 0 and 1")

    return value


def _normalize_embedding(vec: np.ndarray) -> np.ndarray:
    norm = np.linalg.norm(vec)
    if norm == 0:
        return vec
    return vec / norm


def _semantic_similarity(
    query_vec: np.ndarray, candidate_vec: np.ndarray, *, normalized: bool = False
) -> float:
    if query_vec.size == 0 or candidate_vec.size == 0:
        return 0.0
    if not normalized:
        query_vec = _normalize_embedding(query_vec)
        candidate_vec = _normalize_embedding(candidate_vec)
    return float(np.dot(query_vec, candidate_vec))


def _extract_factors(value: Mapping[str, object] | None) -> Sequence[Mapping[str, object]]:
    if not value:
        return []

    factors = value.get("factors") if isinstance(value, Mapping) else None
    if isinstance(factors, Sequence):
        return [factor for factor in factors if isinstance(factor, Mapping)]
    if isinstance(value, Sequence):  # type: ignore[redundant-expr]
        return [factor for factor in value if isinstance(factor, Mapping)]
    return []


def p_adic_distance(
    a_factors: Sequence[Mapping[str, object]],
    b_factors: Sequence[Mapping[str, object]],
    *,
    max_delta: int = 2,
    min_overlap: int = 1,
) -> tuple[float, int]:
    """
    Return an average delta-based distance and overlap count for two factor sets.

    The inputs are sequences of mapping objects that include at least a ``prime``
    key and optionally a ``delta``. Distances are capped by ``max_delta`` to
    reduce the influence of outliers. If the overlap cardinality is below
    ``min_overlap`` the distance defaults to ``float('inf')``.
    """

    if not a_factors or not b_factors:
        return float("inf"), 0

    a_map: MutableMapping[int, float] = {}
    b_map: MutableMapping[int, float] = {}
    for item in a_factors:
        try:
            a_map[int(item.get("prime"))] = float(item.get("delta", 0.0))
        except (TypeError, ValueError, AttributeError):
            continue
    for item in b_factors:
        try:
            b_map[int(item.get("prime"))] = float(item.get("delta", 0.0))
        except (TypeError, ValueError, AttributeError):
            continue

    shared = set(a_map).intersection(b_map)
    overlap = len(shared)
    if overlap < min_overlap:
        return float("inf"), overlap

    deltas = [min(abs(a_map[prime] - b_map[prime]), float(max_delta)) for prime in shared]
    average_delta = float(sum(deltas)) / overlap if deltas else float("inf")
    return average_delta, overlap


def _prepare_candidates(memories: Sequence[Mapping[str, object]]) -> list[MemoryCandidate]:
    candidates: list[MemoryCandidate] = []
    for memory in memories:
        text = ""
        if isinstance(memory, Mapping):
            raw_text = memory.get("text") or memory.get("body") or memory.get("value")
            text = str(raw_text) if raw_text is not None else ""
            factors = _extract_factors(memory)
            candidates.append(MemoryCandidate(text=text, factors=factors, payload=memory))
    return candidates


def fuzzy_retrieve(
    query: str,
    *,
    entity: str | None = None,
    memory_service: MemoryService,
    top_k: int = 5,
    semantic_weight: float | None = None,
    max_delta: int = 2,
    min_overlap: int = 1,
) -> list[Mapping[str, object]]:
    """
    Retrieve the top ``top_k`` memories blending p-adic overlap and semantics.

    ``semantic_weight`` interpolates cosine similarity and p-adic similarity.
    """

    semantic_weight = _resolve_semantic_weight(semantic_weight)

    memories = list(memory_service.get_all_memories(entity))
    candidates = _prepare_candidates(memories)
    if not candidates:
        return []

    anchor_payload = getattr(memory_service, "anchor", None)
    query_factors: Sequence[Mapping[str, object]] = []
    if callable(anchor_payload):
        try:
            anchored = anchor_payload(query, entity=entity)
            query_factors = _extract_factors(anchored) or _extract_factors(getattr(anchored, "metadata", None))
        except Exception:  # pragma: no cover - defensive
            logger.exception("Failed to anchor query text; continuing without p-adic context")

    encoder = _get_encoder()
    query_vec = encoder.encode(query, normalize_embeddings=True)

    ranked: list[tuple[float, Mapping[str, object]]] = []
    for candidate in candidates:
        embedding = candidate.payload.get("embedding") if isinstance(candidate.payload, Mapping) else None
        if embedding is None:
            embedding_vec = np.array(encoder.encode(candidate.text, normalize_embeddings=True))
        else:
            embedding_vec = _normalize_embedding(np.array(embedding))

        semantic_sim = _semantic_similarity(query_vec, embedding_vec, normalized=True)

        distance, overlap = p_adic_distance(
            query_factors,
            candidate.factors,
            max_delta=max_delta,
            min_overlap=min_overlap,
        )
        p_adic_sim = 0.0 if distance == float("inf") else 1.0 / (1.0 + distance)

        combined = (semantic_weight * semantic_sim) + ((1.0 - semantic_weight) * p_adic_sim)

        enriched = dict(candidate.payload)
        enriched.update(
            {
                "semantic_similarity": semantic_sim,
                "p_adic_similarity": p_adic_sim,
                "p_adic_overlap": overlap,
                "score": combined,
            }
        )
        ranked.append((combined, enriched))

    ranked.sort(key=lambda pair: pair[0], reverse=True)
    return [payload for _, payload in ranked[:top_k]]


__all__ = ["MemoryService", "MemoryCandidate", "fuzzy_retrieve", "p_adic_distance"]
