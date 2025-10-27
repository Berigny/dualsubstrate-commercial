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
from typing import Callable, Deque, Iterable, List, Optional, Tuple, Set, Dict
from collections import deque

from memory_governance import (
    GovernanceDecision,
    MemoryAction,
    MemoryZone,
    evaluate_memory_action,
)


def _normalise_text(text: str) -> str:
    """Canonicalise text for hashing and scoring."""

    return re.sub(r"\s+", " ", text.strip())


def deterministic_key(text: str) -> bytes:
    """Return a deterministic 16-byte key for ``text``."""

    norm = re.sub(r"\W+", "", text.lower().strip())
    return hashlib.blake2b(norm.encode("utf-8"), digest_size=16).digest()


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def _tokenise(text: str) -> List[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


class S1Salience:
    """
    Extremely small salience model:
    - hash 4-gram characters into a 16-d bag
    - compare against a running baseline and gate via sigmoid
    """

    def __init__(self, *, alpha: float = 0.12, sensitivity: float = 8.0, offset: float = 0.25) -> None:
        self._baseline = [0.0] * 16
        self._alpha = alpha
        self._sensitivity = sensitivity
        self._offset = offset

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
        diff = math.sqrt(sum((f - b) ** 2 for f, b in zip(feats, self._baseline)))
        score = _sigmoid(self._sensitivity * (diff - self._offset))
        self._baseline = [
            (1.0 - self._alpha) * b + self._alpha * f for b, f in zip(self._baseline, feats)
        ]
        return score


@dataclass
class MemoryEvent:
    key_hex: str
    text: str
    score: float
    timestamp: float
    consilience: float
    zone: MemoryZone
    tokens: Set[str]


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
class ConsolidationResult:
    fact: Optional[str]
    decision: Optional[GovernanceDecision]
    event: Optional[MemoryEvent]


@dataclass
class MemoryResponse:
    stored: bool
    salience_score: Optional[float] = None
    consilience: Optional[float] = None
    zone: Optional[MemoryZone] = None
    storage_notes: List[str] = field(default_factory=list)
    injected_fact: Optional[str] = None
    injection_notes: List[str] = field(default_factory=list)


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
    max_events: int = 3
    silence_window: float = 18.0
    _events: Deque[MemoryEvent] = field(default_factory=lambda: deque(maxlen=32))
    _events_since_consolidate: int = 0
    _last_salient_at: Optional[float] = None
    _last_consolidated_at: Optional[float] = None
    _injected_keys: set[str] = field(default_factory=set)
    _s1: S1Salience = field(default_factory=S1Salience)
    _s2: S2Consolidator = field(default_factory=S2Consolidator)
    _daily_counts: Dict[MemoryAction, Tuple[str, int]] = field(default_factory=dict)

    def reset(self) -> None:
        self._events.clear()
        self._events_since_consolidate = 0
        self._last_salient_at = None
        self._last_consolidated_at = None
        self._injected_keys.clear()
        self._daily_counts.clear()

    def _get_daily_count(self, action: MemoryAction, now: float) -> int:
        date = time.strftime("%Y-%m-%d", time.localtime(now))
        stored_date, count = self._daily_counts.get(action, (date, 0))
        if stored_date != date:
            count = 0
        self._daily_counts[action] = (date, count)
        return count

    def _increment_daily_count(self, action: MemoryAction, now: float) -> None:
        date = time.strftime("%Y-%m-%d", time.localtime(now))
        stored_date, count = self._daily_counts.get(action, (date, 0))
        if stored_date != date:
            count = 0
        self._daily_counts[action] = (date, count + 1)

    def _consilience_score(self, tokens: Set[str]) -> float:
        if not self._events or not tokens:
            return 0.0
        scores: List[float] = []
        for event in self._events:
            union = len(tokens | event.tokens)
            if union == 0:
                continue
            intersection = len(tokens & event.tokens)
            scores.append(intersection / union)
        return sum(scores) / len(scores) if scores else 0.0

    def process(
        self,
        text: str,
        *,
        store_fn: Callable[[bytes, str], None],
        fetch_fn: Callable[[bytes], Optional[str]],
        now: Optional[float] = None,
    ) -> MemoryResponse:
        """Process an utterance and run governance-aware S1/S2 logic."""

        now = now or time.time()
        normalised = _normalise_text(text)
        if not normalised:
            return MemoryResponse(stored=False)

        tokens = set(_tokenise(normalised))
        salience = self._s1.score(normalised)
        consilience = self._consilience_score(tokens)

        store_count = self._get_daily_count(MemoryAction.STORE, now)
        store_decision = evaluate_memory_action(MemoryAction.STORE, consilience, store_count)

        response = MemoryResponse(
            stored=False,
            salience_score=salience,
            consilience=consilience,
            zone=store_decision.zone,
            storage_notes=list(store_decision.notes),
        )

        if salience < self.threshold:
            response.storage_notes.append(
                f"Salience {salience:.2f} below threshold {self.threshold:.2f}; not stored."
            )
            consolidation = self._maybe_consolidate_on_timeout(fetch_fn, now)
            if consolidation and consolidation.fact:
                response.injected_fact = consolidation.fact
                if consolidation.decision:
                    response.zone = consolidation.decision.zone
                    response.injection_notes = list(consolidation.decision.notes)
                    if consolidation.decision.requires_confirmation:
                        response.injection_notes.append("Injection required confirmation (auto accepted).")
            return response

        self._last_salient_at = now

        if not store_decision.allowed:
            if store_decision.reason:
                response.storage_notes.append(store_decision.reason)
            # Even if not stored, allow time-based consolidation to proceed.
            consolidation = self._maybe_consolidate_on_timeout(fetch_fn, now)
            if consolidation and consolidation.fact:
                response.injected_fact = consolidation.fact
                if consolidation.decision:
                    response.zone = consolidation.decision.zone
                    response.injection_notes = list(consolidation.decision.notes)
                    if consolidation.decision.requires_confirmation:
                        response.injection_notes.append("Injection required confirmation (auto accepted).")
            return response

        # Store in Qp and update local memory event log.
        key_bytes = deterministic_key(normalised)
        key_hex = key_bytes.hex()
        payload = json.dumps(
            {
                "text": normalised,
                "score": salience,
                "timestamp": now,
                "consilience": consilience,
                "zone": store_decision.zone.value,
            },
            separators=(",", ":"),
        )
        store_fn(key_bytes, payload)
        self._increment_daily_count(MemoryAction.STORE, now)

        event = MemoryEvent(
            key_hex=key_hex,
            text=normalised,
            score=salience,
            timestamp=now,
            consilience=consilience,
            zone=store_decision.zone,
            tokens=tokens,
        )
        self._events.append(event)
        self._events_since_consolidate += 1
        response.stored = True
        if store_decision.requires_audit:
            response.storage_notes.append("Audit recommended for stored memory.")

        consolidation: Optional[ConsolidationResult] = None
        if self._events_since_consolidate >= self.max_events:
            consolidation = self._consolidate(fetch_fn, now)
        else:
            consolidation = self._maybe_consolidate_on_timeout(fetch_fn, now)

        if consolidation and consolidation.fact:
            response.injected_fact = consolidation.fact
            if consolidation.decision:
                response.zone = consolidation.decision.zone
                response.injection_notes = list(consolidation.decision.notes)
                if consolidation.decision.requires_confirmation:
                    response.injection_notes.append("Injection required confirmation (auto accepted).")

        return response

    def force_consolidate(
        self,
        fetch_fn: Callable[[bytes], Optional[str]],
        *,
        now: Optional[float] = None,
    ) -> Optional[ConsolidationResult]:
        """Force a consolidation pass."""

        now = now or time.time()
        return self._consolidate(fetch_fn, now)

    # ---------- helpers ----------
    def _maybe_consolidate_on_timeout(
        self,
        fetch_fn: Callable[[bytes], Optional[str]],
        now: float,
    ) -> Optional[ConsolidationResult]:
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
    ) -> Optional[ConsolidationResult]:
        ranked = self._s2.rank(self._events)
        for event in ranked:
            if event.key_hex in self._injected_keys:
                continue
            inject_count = self._get_daily_count(MemoryAction.INJECT, now)
            decision = evaluate_memory_action(MemoryAction.INJECT, event.consilience, inject_count)
            if not decision.allowed:
                continue
            raw = fetch_fn(bytes.fromhex(event.key_hex))
            text = parse_payload(raw) if raw is not None else event.text
            if not text:
                continue
            self._events_since_consolidate = 0
            self._last_consolidated_at = now
            self._injected_keys.add(event.key_hex)
            self._increment_daily_count(MemoryAction.INJECT, now)
            return ConsolidationResult(fact=text, decision=decision, event=event)
        return None
