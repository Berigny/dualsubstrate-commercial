"""Canonical prime schema + helpers shared by API endpoints and agents."""
from __future__ import annotations

from typing import Iterable, List, Tuple

PRIME_SCHEMA: dict[int, dict[str, str]] = {
    2: {
        "name": "Novelty",
        "tier": "S1",
        "mnemonic": "spark",
        "description": "New angles, surprising facts, reframings, first-time signals.",
    },
    3: {
        "name": "Uniqueness",
        "tier": "S1",
        "mnemonic": "spec",
        "description": "Definitions, canonical schemas, identifiers, invariants.",
    },
    5: {
        "name": "Connection",
        "tier": "S1",
        "mnemonic": "stitch",
        "description": "Cross references, dependency graphs, coherence statements.",
    },
    7: {
        "name": "Action",
        "tier": "S1",
        "mnemonic": "step",
        "description": "Behaviours, operations, runnable steps, commands.",
    },
    11: {
        "name": "Potential",
        "tier": "S2",
        "mnemonic": "seed",
        "description": "Ideation, speculative options, branching design spaces.",
    },
    13: {
        "name": "Autonomy",
        "tier": "S2",
        "mnemonic": "silo",
        "description": "Well-bounded modules, ownership, clear interfaces.",
    },
    17: {
        "name": "Relatedness",
        "tier": "S2",
        "mnemonic": "system",
        "description": "Integration plans, stakeholder alignment, multi-actor weaving.",
    },
    19: {
        "name": "Mastery",
        "tier": "S2",
        "mnemonic": "standard",
        "description": "Repeatable practice, SOPs, acceptance criteria, benchmarks.",
    },
}

MODIFIER_SCHEMA: dict[int, dict[str, str]] = {
    23: {"name": "Evidence", "description": "Citations, datasets, measured results."},
    29: {"name": "Clarity", "description": "Lucid prose, diagrams, high readability."},
    31: {"name": "Time-Sensitivity", "description": "Incidents, announcements, decaying info."},
    37: {"name": "Risk/Ethics", "description": "Harm, fairness, consent, safety concerns."},
    41: {"name": "Reuse", "description": "Templates, libraries, reusable patterns."},
}

PRIME_ORDER: Tuple[int, ...] = tuple(PRIME_SCHEMA.keys())


def get_schema_response() -> dict:
    """Return a serialisable schema payload for /schema."""
    return {
        "primes": [
            {
                "prime": prime,
                "name": info["name"],
                "tier": info["tier"],
                "mnemonic": info["mnemonic"],
                "description": info["description"],
            }
            for prime, info in PRIME_SCHEMA.items()
        ],
        "modifiers": [
            {"prime": prime, "name": info["name"], "description": info["description"]}
            for prime, info in MODIFIER_SCHEMA.items()
        ],
    }


def annotate_factors(pairs: Iterable[Tuple[int, int]]) -> List[dict]:
    """Attach symbolic metadata to (prime, value) rows for responses."""
    annotated: List[dict] = []
    for prime, value in pairs:
        info = PRIME_SCHEMA.get(prime, {})
        annotated.append(
            {
                "prime": prime,
                "value": value,
                "symbol": info.get("name", f"Prime {prime}"),
                "tier": info.get("tier"),
                "mnemonic": info.get("mnemonic"),
            }
        )
    return annotated


def annotate_prime_list(primes: Iterable[int]) -> List[dict]:
    """Return prime labels for list responses (e.g., memories)."""
    out: List[dict] = []
    for prime in primes:
        info = PRIME_SCHEMA.get(prime, {})
        out.append(
            {
                "prime": prime,
                "symbol": info.get("name", f"Prime {prime}"),
                "tier": info.get("tier"),
            }
        )
    return out
