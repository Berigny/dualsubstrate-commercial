"""Search helpers for ledger entries indexed by token primes."""

from __future__ import annotations

import json
import logging
from typing import Iterable, List, Sequence

from backend.fieldx_kernel.substrate.ledger_store_v2 import _collect_text_fragments
from backend.search.token_index import TokenPrimeIndex, normalise_text


logger = logging.getLogger(__name__)

STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "into",
    "is",
    "of",
    "on",
    "or",
    "that",
    "the",
    "to",
    "was",
    "were",
    "with",
}


def _preview_text(text: str, limit: int = 160) -> str:
    """Return a short, single-line preview for result snippets."""

    cleaned = (text or "").strip()
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[: limit - 1]}…"


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
    posting_sizes: list[int] = []
    for prime in primes:
        entries = _load_index_entries(index, int(prime))
        postings.append(entries)
        posting_sizes.append(len(entries))

    logger.debug(
        "Loaded postings for primes",
        extra={"mode": cleaned_mode, "primes": list(primes), "posting_sizes": posting_sizes},
    )

    if not postings:
        return []

    if cleaned_mode == "all":
        candidates = set.intersection(*postings)
    else:
        candidates = set().union(*postings)

    logger.debug(
        "Combined postings",
        extra={
            "mode": cleaned_mode,
            "candidate_count": len(candidates),
            "posting_sizes": posting_sizes,
        },
    )

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

    lowered_text = text.lower()
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


def _scan_all_entries(store, tokens: Sequence[str], *, limit: int) -> list[dict]:
    """Scan all ledger entries when the inverted index yields no candidates."""

    try:
        with store._lock:  # type: ignore[attr-defined]
            snapshots = list(store._db.items())  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover - defensive fallback if store internals change
        return []

    results: list[dict] = []
    for raw_key, raw_entry in snapshots:
        try:
            entry_id = raw_key.decode() if isinstance(raw_key, (bytes, bytearray)) else str(raw_key)
            entry = store._decode(raw_entry)  # type: ignore[attr-defined]
        except Exception:  # pragma: no cover - skip malformed rows defensively
            continue

        text = _combine_text_fragments(entry.state.metadata)
        score, snippet = full_text_score(text, tokens)
        if score == 0:
            continue

        results.append(
            {
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
        )

    ranked = sorted(results, key=lambda row: row["score"], reverse=True)
    final_results = ranked[:limit] if limit and limit > 0 else ranked
    logger.debug(
        "Linear scan results prepared",
        extra={"result_count": len(final_results), "scanned_entries": len(results)},
    )
    return final_results


def list_recent_entries(store, *, entity: str, limit: int = 50) -> list[dict]:
    """Return the most recent ledger entries for ``entity``."""

    try:
        with store._lock:  # type: ignore[attr-defined]
            snapshots = list(store._db.items())  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover - defensive fallback
        logger.warning("Unable to collect ledger snapshots for recent listing")
        return []

    entries: list[tuple[float, dict]] = []
    for raw_key, raw_entry in snapshots:
        try:
            entry_id = raw_key.decode() if isinstance(raw_key, (bytes, bytearray)) else str(raw_key)
            entry = store._decode(raw_entry)  # type: ignore[attr-defined]
        except Exception:  # pragma: no cover - skip malformed rows defensively
            continue

        if entry.key.namespace != entity:
            continue

        snippet_source = _combine_text_fragments(entry.state.metadata)
        snippet = _preview_text(snippet_source)
        entries.append(
            (
                entry.created_at.timestamp(),
                {
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
                    "score": 0.0,
                    "snippet": snippet,
                    "entry_id": entry_id,
                },
            )
        )

    ranked = sorted(entries, key=lambda row: row[0], reverse=True)
    trimmed = ranked[:limit] if limit and limit > 0 else ranked
    results = [row[1] for row in trimmed]

    logger.debug(
        "Prepared recent entries listing",
        extra={"entity": entity, "result_count": len(results), "scanned_entries": len(entries)},
    )
    return results


def search(
    query: str,
    *,
    store,
    token_index: TokenPrimeIndex,
    mode: str = "any",
    limit: int = 50,
) -> List[dict]:
    """Search ledger entries using the inverted token index and full-text overlap."""

    raw_tokens = normalise_text(query)
    tokens = [token for token in raw_tokens if token and token not in STOPWORDS]
    logger.debug(
        "Normalised search tokens",
        extra={"raw_tokens": raw_tokens, "filtered_tokens": tokens},
    )
    if not tokens:
        return []

    token_primes = token_index.primes_for_tokens(tokens)
    logger.debug(
        "Resolved token primes",
        extra={"tokens": tokens, "token_primes": token_primes},
    )

    cleaned_mode = (mode or "any").strip().lower()
    if cleaned_mode not in {"any", "all"}:
        raise ValueError("mode must be 'any' or 'all'")

    candidate_ids = search_by_primes(token_primes, token_index, mode=cleaned_mode)
    effective_mode = cleaned_mode
    if cleaned_mode == "all" and not candidate_ids and token_primes:
        logger.debug(
            "Retrying search with mode='any' after empty intersection",
            extra={"requested_mode": cleaned_mode, "token_primes": token_primes},
        )
        candidate_ids = search_by_primes(token_primes, token_index, mode="any")
        effective_mode = "any"

    logger.debug(
        "Search candidates collected",
        extra={"mode": effective_mode, "candidate_count": len(candidate_ids)},
    )

    if not candidate_ids:
        logger.debug(
            "No candidates from index; falling back to linear scan",
            extra={"mode": effective_mode, "token_primes": token_primes},
        )
        return _scan_all_entries(store, tokens, limit=limit)

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
    final_results = ranked[:limit] if limit and limit > 0 else ranked
    logger.debug(
        "Returning ranked search results",
        extra={"mode": effective_mode, "result_count": len(final_results)},
    )
    return final_results


__all__ = [
    "full_text_score",
    "list_recent_entries",
    "search",
    "search_by_primes",
]
