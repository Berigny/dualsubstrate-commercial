"""Run a retrieval benchmark against the DualSubstrate memory stack."""

from __future__ import annotations

import argparse
import json
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, MutableMapping, Sequence

from backend.retrieval import fuzzy_retrieve
from backend.search.token_index import normalise_text

try:  # Production prime set used by the ledger and inference lane
    from core.ledger import PRIME_ARRAY as PRIMES
except Exception:  # pragma: no cover - fallback to a small deterministic prime list
    from backend.search.token_index import _generate_primes as _gen

    PRIMES = tuple(_gen(16))


@dataclass
class QuerySpec:
    entity: str
    query: str
    relevant_texts: set[str]


class BenchmarkMemoryService:
    """Minimal in-memory adapter that mirrors the backend memory surface area."""

    def __init__(self) -> None:
        self._entries: list[dict[str, object]] = []
        self._token_map: MutableMapping[str, int] = {}

    def _token_prime(self, token: str) -> int:
        token = token.strip().lower()
        if token in self._token_map:
            return self._token_map[token]

        index = len(self._token_map)
        prime = PRIMES[index % len(PRIMES)]
        self._token_map[token] = prime
        return prime

    def _factors_for_text(self, text: str) -> list[dict[str, object]]:
        tokens = normalise_text(text)
        factors = []
        for token in tokens:
            prime = self._token_prime(token)
            factors.append({"prime": prime, "delta": 1})
        return factors

    # --- production-style memory hooks ---------------------------------
    def anchor_memory(self, *, entity: str, text: str) -> Mapping[str, object]:
        factors = self._factors_for_text(text)
        entry = {"entity": entity, "text": text, "factors": factors}
        self._entries.append(entry)
        return entry

    def clear_entity(self, entity: str) -> None:
        self._entries = [row for row in self._entries if row.get("entity") != entity]

    # --- protocol support for fuzzy_retrieve ----------------------------
    def get_all_memories(self, entity: str | None = None) -> Sequence[Mapping[str, object]]:
        if entity is None:
            return list(self._entries)
        return [row for row in self._entries if row.get("entity") == entity]

    def anchor(self, text: str, entity: str | None = None) -> Mapping[str, object]:
        return self.anchor_memory(entity=entity or "", text=text)


@dataclass
class BenchmarkResult:
    recall_at_10: float
    mrr: float
    avg_latency_ms: float
    queries: int

    def as_dict(self) -> dict[str, object]:
        return {
            "recall_at_10": self.recall_at_10,
            "mrr": self.mrr,
            "avg_latency_ms": self.avg_latency_ms,
            "queries": self.queries,
        }


def load_dataset(path: Path) -> tuple[list[str], list[QuerySpec]]:
    entities: set[str] = set()
    queries: list[QuerySpec] = []

    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            payload = json.loads(line)
            entity = str(payload.get("entity", "")) or "default"
            entities.add(entity)
            for query in payload.get("queries", []):
                text = str(query.get("query", "")).strip()
                if not text:
                    continue
                relevant = query.get("relevant") or query.get("answers") or []
                queries.append(
                    QuerySpec(
                        entity=entity,
                        query=text,
                        relevant_texts={str(item) for item in relevant},
                    )
                )
    return sorted(entities), queries


def seed_memories(service: BenchmarkMemoryService, dataset_path: Path) -> None:
    with dataset_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            payload = json.loads(line)
            entity = str(payload.get("entity", "")) or "default"
            service.clear_entity(entity)
            for memory in payload.get("memories", []):
                if isinstance(memory, str):
                    text = memory
                else:
                    text = str(memory.get("text", ""))
                if text:
                    service.anchor_memory(entity=entity, text=text)


def evaluate(service: BenchmarkMemoryService, specs: Iterable[QuerySpec]) -> BenchmarkResult:
    hits = 0
    rr_total = 0.0
    latencies: list[float] = []
    query_count = 0

    for spec in specs:
        start = time.perf_counter()
        results = fuzzy_retrieve(
            spec.query,
            entity=spec.entity,
            memory_service=service,
            top_k=10,
        )
        duration_ms = (time.perf_counter() - start) * 1000.0
        latencies.append(duration_ms)
        query_count += 1

        rank_hit = None
        for idx, row in enumerate(results):
            text = str(row.get("text") or row.get("body") or row.get("value") or "")
            if text in spec.relevant_texts:
                rank_hit = idx
                break

        if rank_hit is not None:
            hits += 1
            rr_total += 1.0 / float(rank_hit + 1)

    recall = float(hits) / query_count if query_count else 0.0
    mrr = rr_total / query_count if query_count else 0.0
    avg_latency = statistics.mean(latencies) if latencies else 0.0
    return BenchmarkResult(recall_at_10=recall, mrr=mrr, avg_latency_ms=avg_latency, queries=query_count)


def print_summary(result: BenchmarkResult) -> None:
    targets = {"recall_at_10": 0.6, "mrr": 0.5, "avg_latency_ms": 800.0}
    print("Dual Retrieval Benchmark")
    print("========================")
    print(f"Queries        : {result.queries}")
    print(f"Recall@10      : {result.recall_at_10:.3f} (target >= {targets['recall_at_10']:.2f})")
    print(f"MRR            : {result.mrr:.3f} (target >= {targets['mrr']:.2f})")
    print(f"Avg latency ms : {result.avg_latency_ms:.2f} (target <= {targets['avg_latency_ms']:.0f})")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path(__file__).with_name("benchmark_dataset.jsonl"),
        help="Path to the benchmark dataset JSONL",
    )
    args = parser.parse_args()

    dataset_path = args.dataset
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset not found: {dataset_path}")

    service = BenchmarkMemoryService()
    seed_memories(service, dataset_path)
    _, specs = load_dataset(dataset_path)
    result = evaluate(service, specs)
    print_summary(result)


if __name__ == "__main__":
    main()
