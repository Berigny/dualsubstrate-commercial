"""Token-to-prime index stored in RocksDB."""

from __future__ import annotations

import json
import math
import re
from typing import Iterable, List

from fastapi import FastAPI

# Deterministic list of prime numbers for token assignment.
# Generated at import time but deterministic because no randomness is used.

def _generate_primes(count: int) -> List[int]:
    primes: List[int] = []
    candidate = 2
    while len(primes) < count:
        is_prime = True
        limit = int(math.sqrt(candidate)) + 1
        for prime in primes:
            if prime > limit:
                break
            if candidate % prime == 0:
                is_prime = False
                break
        if is_prime:
            primes.append(candidate)
        candidate += 1
    return primes


_PRIME_LIST = _generate_primes(10_000)


def normalise_text(text: str) -> str:
    """Lowercase and collapse whitespace to normalise a token."""

    cleaned = re.sub(r"\s+", " ", text.strip().lower())
    return cleaned


class TokenPrimeIndex:
    """Manage token→prime assignments and an inverted prime index."""

    def __init__(self, app: FastAPI):
        self.app = app
        self.db = app.state.db

    @staticmethod
    def _token_key(token: str) -> str:
        return f"tp:token:{token}"

    @staticmethod
    def _prime_key(prime: int) -> str:
        return f"ix:prime:{prime}"

    @staticmethod
    def _next_index_key() -> str:
        return "tp:next_index"

    def get_or_assign_prime(self, token: str) -> int:
        """Return the assigned prime for ``token`` or allocate a new one."""

        normalised_token = normalise_text(token)
        token_key = self._token_key(normalised_token)
        existing = self.db.get(token_key)
        if existing is not None:
            return int(existing)

        next_index_raw = self.db.get(self._next_index_key())
        next_index = int(next_index_raw) if next_index_raw is not None else 0
        if next_index >= len(_PRIME_LIST):
            raise IndexError("Prime list exhausted")

        prime = _PRIME_LIST[next_index]
        self.db[token_key] = str(prime)
        self.db[self._next_index_key()] = str(next_index + 1)
        return prime

    def primes_for_tokens(self, tokens: Iterable[str]) -> List[int]:
        """Return primes for ``tokens``, allocating new ones as needed."""

        return [self.get_or_assign_prime(token) for token in tokens]

    def update_inverted_index(self, primes: Iterable[int], entry_id: str) -> None:
        """Add ``entry_id`` to the inverted index for each ``prime``."""

        for prime in primes:
            key = self._prime_key(prime)
            existing_raw = self.db.get(key)
            if existing_raw:
                try:
                    entries = set(json.loads(existing_raw))
                except (TypeError, json.JSONDecodeError):
                    entries = set()
            else:
                entries = set()

            if entry_id not in entries:
                entries.add(entry_id)
                self.db[key] = json.dumps(sorted(entries))
