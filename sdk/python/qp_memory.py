"""Lightweight Python wrapper for the DualSubstrate MVP API."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional
from urllib.request import Request, urlopen


@dataclass
class QPMemoryClient:
    """Simple JSON-over-HTTP client."""

    base_url: str
    api_key: str

    def _request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Any:
        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        request = Request(url, data=data, method=method)
        request.add_header("Content-Type", "application/json")
        request.add_header("x-api-key", self.api_key)
        with urlopen(request) as response:
            body = response.read()
        return json.loads(body.decode("utf-8")) if body else None

    def append_event(self, payload: str) -> Dict[str, Any]:
        return self._request("POST", "/events", {"payload": payload})

    def get_event(self, offset: int) -> Dict[str, Any]:
        return self._request("GET", f"/events/{offset}")

    def get_head(self) -> Dict[str, Any]:
        return self._request("GET", "/ledger/head")

    def compute_checksum(self, items: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
        return self._request("POST", "/checksum", {"items": list(items)})

    def health(self) -> Dict[str, Any]:
        return self._request("GET", "/health")

