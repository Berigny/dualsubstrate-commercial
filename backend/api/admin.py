"""Administrative endpoints for managing search indexes and ledgers."""

import json
import logging
from typing import Iterable, Set

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from backend.api.http import get_db
from backend.api.logging_utils import log_operation
from backend.search.reindex import reindex_all


router = APIRouter(prefix="/admin", tags=["admin"])
LOGGER = logging.getLogger(__name__)

LEDGER_REGISTRY_KEY = b"__ledgers__"


class LedgerCreateRequest(BaseModel):
    """Accept lightweight creation payloads from the Streamlit UI."""

    model_config = ConfigDict(extra="ignore")

    name: str | None = Field(None, description="Human-friendly ledger name")
    namespace: str | None = Field(None, description="Namespace used when writing entries")

    def resolved_name(self) -> str:
        candidate = (self.namespace or self.name or "").strip()
        return candidate or "default"


def _load_registered_ledgers(db) -> Set[str]:
    raw = db.get(LEDGER_REGISTRY_KEY)
    if raw is None:
        return set()

    try:
        payload = json.loads(raw.decode() if isinstance(raw, (bytes, bytearray)) else raw)
    except Exception:  # pragma: no cover - defensive guard
        return set()

    return {str(item) for item in payload if str(item).strip()}


def _persist_registered_ledgers(db, ledgers: Iterable[str]) -> list[str]:
    cleaned = sorted({ledger.strip() for ledger in ledgers if ledger and ledger.strip()})
    db[LEDGER_REGISTRY_KEY] = json.dumps(cleaned).encode()
    return cleaned


def _discover_ledgers(db) -> Set[str]:
    namespaces: set[str] = set()
    try:
        with db.iter() as iterator:  # type: ignore[attr-defined]
            for raw_key, _ in iterator:
                try:
                    decoded = raw_key.decode() if isinstance(raw_key, (bytes, bytearray)) else str(raw_key)
                except Exception:  # pragma: no cover - defensive
                    continue

                if decoded == LEDGER_REGISTRY_KEY.decode():
                    continue

                namespace = decoded.split(":", 1)[0]
                if namespace:
                    namespaces.add(namespace)
    except Exception:  # pragma: no cover - fallback if iterator unavailable
        try:
            for raw_key in db.keys():  # type: ignore[attr-defined]
                decoded = raw_key.decode() if isinstance(raw_key, (bytes, bytearray)) else str(raw_key)
                if decoded == LEDGER_REGISTRY_KEY.decode():
                    continue
                namespace = decoded.split(":", 1)[0]
                if namespace:
                    namespaces.add(namespace)
        except Exception:
            namespaces = set()

    return namespaces


@router.get("/reindex")
def trigger_reindex(
    request: Request,
    entity: str | None = Query(None, description="Optional logical entity context."),
    db=Depends(get_db),
):
    """Rebuild the token index and refreshed metadata for all ledger entries."""

    with log_operation(
        logger=LOGGER,
        operation="admin_reindex",
        request=request,
        entity=entity,
    ) as ctx:
        try:
            result = reindex_all(request.app, entity=entity)
        except Exception as exc:  # pragma: no cover - defensive guardrail
            raise HTTPException(status_code=500, detail=str(exc)) from exc

        ctx.update(result)
        return result


@router.get("/ledgers")
def list_ledgers(request: Request, db=Depends(get_db)):
    """Return known ledger namespaces for the UI sidebar."""

    with log_operation(LOGGER, "admin_list_ledgers", request=request) as ctx:
        registered = _load_registered_ledgers(db)
        discovered = _discover_ledgers(db)
        ledgers = sorted(registered | discovered | {"default"})

        ctx.update({"ledger_count": len(ledgers)})
        return {"ledgers": ledgers}


@router.post("/ledgers")
def create_ledger(request: Request, payload: LedgerCreateRequest, db=Depends(get_db)):
    """Record a ledger namespace for clients that create ledgers dynamically."""

    with log_operation(LOGGER, "admin_create_ledger", request=request) as ctx:
        name = payload.resolved_name()
        registered = _load_registered_ledgers(db)
        discovered = _discover_ledgers(db)
        ledgers = registered | discovered | {name or "default"}
        persisted = _persist_registered_ledgers(db, ledgers)

        ctx.update({"ledger": name, "ledger_count": len(persisted)})
        return {"status": "ok", "ledger": name, "ledgers": persisted}


__all__ = ["router"]
