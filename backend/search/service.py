"""Search helpers for ledger entries indexed by token primes."""

from __future__ import annotations

import json
import re
from typing import Iterable, List, Sequence

from backend.fieldx_kernel.substrate.ledger_store_v2 import _collect_text_fragments
from backend.search.token_index import TokenPrimeIndex, normalise_text


def _load_index_entries(index: TokenPrimeIndex, prime: int) -> set[str]:
    """Return entry identifiers associated with ``prime`` from the inverted index."""

    raw = index.db.get(index._prime_key(prime))
    if raw is None:
        return set()

    try:
        decoded = raw.decode() if isinstance(raw, (bytes, bytearray)) else raw
        payload = json.loads(decoded)
        return {str(item) for item in payload}
    except (TypeError, json.JSONDecodeError):  # pragma: no cover - defensive
        return set()


def search_by_primes(
    primes: Sequence[int], index: TokenPrimeIndex, mode: str = "any"
) -> List[str]:
    """
    Collect candidate entry identifiers for the provided ``primes``.

    ``mode`` controls set combination:
    * ``"any"`` (default) unions all postings lists.
    * ``"all"`` intersects postings lists to require every token.
    """

    cleaned_mode = (mode or "any").strip().lower()
    if cleaned_mode not in {"any", "all"}:
        raise ValueError("mode must be 'any' or 'all'")

    postings: list[set[str]] = []
    for prime in primes:
        entries = _load_index_entries(index, int(prime))
        if entries:
            postings.append(entries)

    if not postings:
        return []

    if cleaned_mode == "all":
        candidates = set.intersection(*postings)
    else:
        candidates = set().union(*postings)

    return sorted(candidates)


def _combine_text_fragments(metadata: dict | None) -> str:
    if not metadata:
        return ""

    if full_text := metadata.get("full_text"):
        return str(full_text)

    fragments: Iterable[str] = _collect_text_fragments(metadata)
    return " ".join(str(fragment) for fragment in fragments)


def full_text_score(text: str, tokens: Sequence[str]) -> tuple[float, str]:
    """Return a tuple of ``(score, snippet)`` for ``text`` against ``tokens``."""

    if not text:
        return 0.0, ""

    cleaned_tokens = [token for token in tokens if token]
    if not cleaned_tokens:
        return 0.0, ""

    lowered_text = normalise_text(text)
    score = float(sum(lowered_text.count(token) for token in cleaned_tokens))
    if score == 0:
        return 0.0, ""

    # Identify the first occurrence to build a short snippet around the match.
    first_hit = None
    for token in cleaned_tokens:
        position = lowered_text.find(token)
        if position != -1 and (first_hit is None or position < first_hit):
            first_hit = position

    window = 48
    if first_hit is None:
        snippet = text.strip()
    else:
        start = max(0, first_hit - window)
        end = min(len(text), first_hit + window)
        snippet = text[start:end].strip()
        if start > 0:
            snippet = "... " + snippet
        if end < len(text):
            snippet = snippet + " ..."

    return score, snippet


def search(
    query: str,
    *,
    store,
    token_index: TokenPrimeIndex,
    mode: str = "any",
    limit: int = 50,
) -> List[dict]:
    """Search ledger entries using the inverted token index and full-text overlap."""

    normalised_query = normalise_text(query)
    tokens = re.findall(r"[a-z0-9]+", normalised_query)
    if not tokens:
        return []

    token_primes = token_index.primes_for_tokens(tokens)
    candidate_ids = search_by_primes(token_primes, token_index, mode=mode)

    results: list[dict] = []
    for entry_id in candidate_ids:
        entry = store.read(entry_id)
        if entry is None:
            continue

        text = _combine_text_fragments(entry.state.metadata)
        score, snippet = full_text_score(text, tokens)
        if score == 0:
            continue

        result = {
            "entry": {
                "key": {
                    "namespace": entry.key.namespace,
                    "identifier": entry.key.identifier,
                },
                "state": {
                    "coordinates": entry.state.coordinates,
                    "phase": entry.state.phase,
                    "metadata": entry.state.metadata,
                },
                "created_at": entry.created_at.isoformat(),
                "notes": entry.notes,
            },
            "score": score,
            "snippet": snippet,
            "entry_id": entry_id,
        }
        results.append(result)

    ranked = sorted(results, key=lambda row: row["score"], reverse=True)
    if limit and limit > 0:
        return ranked[:limit]
    return ranked


__all__ = [
    "full_text_score",
    "search",
    "search_by_primes",
]
