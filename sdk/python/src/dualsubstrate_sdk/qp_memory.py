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
    def anchor(
        self,
        entity: str,
        factors: Sequence[tuple[int, int]],
        *,
        text: str | None = None,
    ) -> dict:
        """Append ``factors`` for ``entity`` and optionally persist ``text``.

        The FastAPI ``/anchor`` endpoint accepts an optional transcript payload in
        addition to the exponent deltas.  When ``text`` is provided we forward it
        so the server can mirror the request into the Qp column family.  Existing
        callers that do not supply ``text`` continue to behave identically.
        """

        payload = {
            "entity": entity,
            "factors": [
                {"prime": prime, "delta": delta}
                for prime, delta in factors
            ],
        }
        if text is not None:
            payload["text"] = text
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

    def search(self, query: str, mode: str = "all") -> list[dict[str, object]]:
        """Query structured slots/body shards for ``query`` using ``mode`` weights."""

        response = self._session.get(
            f"{self._base}/search",
            headers=self._headers,
            params={"q": query, "mode": mode},
            timeout=float(os.getenv("DUALSUBSTRATE_HTTP_TIMEOUT", "10")),
        )
        response.raise_for_status()
        payload = response.json()
        results: list[dict[str, object]] = []
        for row in payload.get("results", []):
            if not isinstance(row, dict):
                continue
            try:
                prime = int(row.get("prime"))
            except (TypeError, ValueError):
                continue
            snippet = row.get("snippet", "")
            score_raw = row.get("score", 0.0)
            try:
                score = float(score_raw)
            except (TypeError, ValueError):
                score = 0.0
            results.append(
                {
                    "entity": str(row.get("entity", "")),
                    "prime": prime,
                    "score": score,
                    "snippet": str(snippet),
                }
            )
        return results

    def inference_state(self, entity: str) -> dict[str, object]:
        """Return the latent state vector and readout rows for ``entity``."""

        response = self._session.get(
            f"{self._base}/inference/state",
            headers=self._headers,
            params={"entity": entity},
            timeout=float(os.getenv("DUALSUBSTRATE_HTTP_TIMEOUT", "10")),
        )
        response.raise_for_status()
        payload = response.json()
        rows = {
            int(prime): [float(value) for value in vector]
            for prime, vector in payload.get("R", {}).items()
        }
        return {
            "x": [float(v) for v in payload.get("x", [])],
            "R": rows,
        }

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
