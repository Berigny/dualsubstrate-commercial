"""HTTP helpers and adapter utilities for DualSubstrate memory endpoints."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Iterable, Sequence

import requests


@dataclass
class MemoryAnchor:
    """Thin wrapper around the HTTP memory projection endpoints.

    Parameters
    ----------
    api_key:
        Bearer token used for authentication with the HTTP gateway.
    base_url:
        Base URL of the grpc-gateway deployment (e.g. ``https://api.example.com``).
    session:
        Optional :class:`requests.Session` to reuse connections across calls.
    """

    api_key: str
    base_url: str = "http://localhost:8080"
    session: requests.Session | None = None

    def __post_init__(self) -> None:
        self._session = self.session or requests.Session()
        self._headers = {"Authorization": f"Bearer {self.api_key}"}
        self._base = self.base_url.rstrip("/")

    # --- raw helpers -----------------------------------------------------
    def anchor(self, entity: str, factors: Sequence[tuple[int, int]]) -> dict:
        payload = {
            "entity": entity,
            "factors": [
                {"prime": prime, "delta": delta}
                for prime, delta in factors
            ],
        }
        response = self._session.post(
            f"{self._base}/anchor",
            headers=self._headers,
            json=payload,
            timeout=float(os.getenv("DUALSUBSTRATE_HTTP_TIMEOUT", "10")),
        )
        response.raise_for_status()
        return response.json()

    def query(self, primes: Sequence[int]) -> list[tuple[str, int]]:
        response = self._session.post(
            f"{self._base}/query",
            headers=self._headers,
            json={"primes": list(primes)},
            timeout=float(os.getenv("DUALSUBSTRATE_HTTP_TIMEOUT", "10")),
        )
        response.raise_for_status()
        body = response.json()
        return [
            (row["entity"], int(row["weight"]))
            for row in body.get("results", [])
        ]

    def checksum(self, entity: str) -> str:
        response = self._session.get(
            f"{self._base}/checksum",
            headers=self._headers,
            params={"entity": entity},
            timeout=float(os.getenv("DUALSUBSTRATE_HTTP_TIMEOUT", "10")),
        )
        response.raise_for_status()
        body = response.json()
        return body.get("checksum", "")

    # --- framework adapters ---------------------------------------------
    def as_langchain_tool(self):
        """Expose a LangChain Tool for querying entity projections."""

        from langchain.tools import Tool

        def _invoke(primes: Sequence[int] | str) -> str:
            vector: Sequence[int]
            if isinstance(primes, str):
                data = json.loads(primes)
                vector = data.get("primes", []) if isinstance(data, dict) else data
            else:
                vector = primes
            rows = self.query(vector)
            return json.dumps(
                {"results": [
                    {"entity": entity, "weight": weight} for entity, weight in rows
                ]},
                ensure_ascii=False,
            )

        return Tool(
            name="dualsubstrate_query",
            description=(
                "Query DualSubstrate entity weights for a set of primes. "
                "Input can be a JSON string or sequence of integers."
            ),
            func=_invoke,
        )

    def as_llamaindex_tool(self):
        """Expose a LlamaIndex FunctionTool for querying entity weights."""

        from llama_index.core.tools import FunctionTool

        def _invoke(primes: Iterable[int]) -> list[tuple[str, int]]:
            return self.query(list(primes))

        return FunctionTool.from_defaults(
            fn=_invoke,
            name="dualsubstrate_query",
            description="Query DualSubstrate entity weights for the provided primes.",
        )
