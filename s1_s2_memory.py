"""
Heuristic S1/S2 memory pipeline for the Streamlit demo.

S1 (salience) scores incoming utterances and decides when to store them.
S2 (consolidation) periodically chooses which stored facts to surface.

This module deliberately keeps the models tiny and dependency-free so the
Streamlit app can run without heavyweight ML frameworks.
"""

from __future__ import annotations

import json
import math
import re
import time
import hashlib
from dataclasses import dataclass, field
from typing import Callable, Deque, Iterable, List, Optional
from collections import deque


def _normalise_text(text: str) -> str:
    """Canonicalise text for hashing and scoring."""

    return re.sub(r"\s+", " ", text.strip())


def deterministic_key(text: str) -> bytes:
    """Return a deterministic 16-byte key for ``text``."""

    norm = re.sub(r"\W+", "", text.lower().strip())
    return hashlib.blake2b(norm.encode("utf-8"), digest_size=16).digest()


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


class S1Salience:
    """
    Extremely small salience model:
    - hash 4-gram characters into a 16-d bag
    - apply a fixed weight vector + sigmoid
    """

    def __init__(self, *, seed: int = 1337) -> None:
        import random

        rnd = random.Random(seed)
        self._weights = [rnd.uniform(-0.4, 0.4) for _ in range(16)]

    @staticmethod
    def _features(text: str) -> List[float]:
        grams = [text[i : i + 4] for i in range(max(len(text) - 3, 1))]
        vec = [0.0] * 16
        for gram in grams:
            idx = hash(gram) % len(vec)
            vec[idx] += 1.0
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        return [v / norm for v in vec]

    def score(self, text: str) -> float:
        feats = self._features(text)
        dot = sum(f * w for f, w in zip(feats, self._weights))
        return _sigmoid(dot)


@dataclass
class MemoryEvent:
    key_hex: str
    text: str
    score: float
    timestamp: float


class S2Consolidator:
    """Rank recent events by combined salience, length, and recency."""

    def __init__(self, *, window: int = 8) -> None:
        self.window = window

    def rank(self, events: Iterable[MemoryEvent]) -> List[MemoryEvent]:
        recent = list(events)[-self.window :]
        scored: List[tuple[float, MemoryEvent]] = []
        for idx, event in enumerate(reversed(recent)):
            recency = 1.0 / (idx + 1.0)
            length_factor = min(len(event.text) / 160.0, 1.0)
            score = (0.5 * event.score) + (0.3 * length_factor) + (0.2 * recency)
            scored.append((score, event))
        scored.sort(key=lambda item: item[0], reverse=True)
        return [event for _, event in scored]


def parse_payload(value: str) -> str:
    """Extract the stored text from a Qp payload."""

    try:
        data = json.loads(value)
        if isinstance(data, dict) and "text" in data:
            return str(data["text"])
    except (json.JSONDecodeError, TypeError):
        pass
    return value


@dataclass
class MemoryResponse:
    fact: Optional[str]
    stored: bool
    score: Optional[float] = None


@dataclass
class MemoryLoop:
    """
    Coordinator for the S1/S2 pipeline.

    Parameters
    ----------
    threshold: float
        Salience threshold for S1 (default 0.7).
    max_events: int
        Trigger consolidation after this many salient events.
    silence_window: float
        Trigger consolidation after this many idle seconds.
    """

    threshold: float = 0.7
    max_events: int = 6
    silence_window: float = 30.0
    _events: Deque[MemoryEvent] = field(default_factory=lambda: deque(maxlen=32))
    _events_since_consolidate: int = 0
    _last_salient_at: Optional[float] = None
    _last_consolidated_at: Optional[float] = None
    _injected_keys: set[str] = field(default_factory=set)
    _s1: S1Salience = field(default_factory=S1Salience)
    _s2: S2Consolidator = field(default_factory=S2Consolidator)

    def reset(self) -> None:
        self._events.clear()
        self._events_since_consolidate = 0
        self._last_salient_at = None
        self._last_consolidated_at = None
        self._injected_keys.clear()

    def process(
        self,
        text: str,
        *,
        store_fn: Callable[[bytes, str], None],
        fetch_fn: Callable[[bytes], Optional[str]],
        now: Optional[float] = None,
    ) -> MemoryResponse:
        """
        Process an utterance, storing it when salient and optionally returning a
        fact to inject.
        """

        now = now or time.time()
        normalised = _normalise_text(text)
        if not normalised:
            return MemoryResponse(fact=None, stored=False, score=None)

        score = self._s1.score(normalised)
        stored = False
        fact: Optional[str] = None

        if score >= self.threshold:
            key_bytes = deterministic_key(normalised)
            key_hex = key_bytes.hex()
            payload = json.dumps(
                {"text": normalised, "score": score, "timestamp": now}, separators=(",", ":")
            )
            store_fn(key_bytes, payload)
            self._events.append(MemoryEvent(key_hex, normalised, score, now))
            self._events_since_consolidate += 1
            self._last_salient_at = now
            stored = True

            if self._events_since_consolidate >= self.max_events:
                fact = self._consolidate(fetch_fn, now)
        else:
            fact = self._maybe_consolidate_on_timeout(fetch_fn, now)

        return MemoryResponse(fact=fact, stored=stored, score=score if stored else None)

    def force_consolidate(
        self,
        fetch_fn: Callable[[bytes], Optional[str]],
        *,
        now: Optional[float] = None,
    ) -> Optional[str]:
        """Force a consolidation pass."""

        now = now or time.time()
        return self._consolidate(fetch_fn, now)

    # ---------- helpers ----------
    def _maybe_consolidate_on_timeout(
        self,
        fetch_fn: Callable[[bytes], Optional[str]],
        now: float,
    ) -> Optional[str]:
        if (
            self._events_since_consolidate > 0
            and self._last_salient_at is not None
            and (now - self._last_salient_at) >= self.silence_window
        ):
            return self._consolidate(fetch_fn, now)
        return None

    def _consolidate(
        self,
        fetch_fn: Callable[[bytes], Optional[str]],
        now: float,
    ) -> Optional[str]:
        ranked = self._s2.rank(self._events)
        for event in ranked:
            if event.key_hex in self._injected_keys:
                continue
            raw = fetch_fn(bytes.fromhex(event.key_hex))
            text = parse_payload(raw) if raw is not None else event.text
            if not text:
                continue
            self._events_since_consolidate = 0
            self._last_consolidated_at = now
            self._injected_keys.add(event.key_hex)
            return text
        return None
