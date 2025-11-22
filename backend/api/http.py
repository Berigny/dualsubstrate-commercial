"""REST endpoints for ledger interactions backed by the Field-X kernel."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from backend.api.schemas import LedgerEntrySchema
from backend.fieldx_kernel import LedgerKey, LedgerStore
from backend.fieldx_kernel.substrate import LedgerStoreV2
from backend.search import service as search_service
from backend.search.token_index import TokenPrimeIndex

router = APIRouter(prefix="/ledger", tags=["ledger"])
search_router = APIRouter(tags=["search"])

logger = logging.getLogger(__name__)

debug_ledger_store = LedgerStore()


def parse_key(key_path: str) -> LedgerKey:
    """Parse a ledger key path of the form ``namespace:identifier``."""

    if ":" in key_path:
        namespace, identifier = key_path.split(":", 1)
    else:
        namespace, identifier = "default", key_path

    if not namespace or not identifier:
        raise HTTPException(status_code=400, detail="Invalid ledger key")

    return LedgerKey(namespace=namespace, identifier=identifier)



def get_db(request: Request):
    db = getattr(request.app.state, "db", None)
    if db is None:
        raise HTTPException(status_code=500, detail="Database not initialized")
    return db


def get_ledger_store(request: Request, db=Depends(get_db)) -> LedgerStoreV2:
    return LedgerStoreV2(db, token_index=TokenPrimeIndex(request.app))


@router.post("/write", response_model=LedgerEntrySchema)
def write_entry(entry: LedgerEntrySchema, store: LedgerStoreV2 = Depends(get_ledger_store)) -> LedgerEntrySchema:
    """Persist a ledger entry in the shared store."""

    model_entry = entry.to_model()
    store.write(model_entry)
    return LedgerEntrySchema.from_model(model_entry)


@router.get("/read/{entry_id}", response_model=LedgerEntrySchema)
def read_entry(entry_id: str, store: LedgerStoreV2 = Depends(get_ledger_store)) -> LedgerEntrySchema:
    """Return a ledger entry for the provided identifier."""

    key = parse_key(entry_id)
    record = store.read(key.as_path())
    if record is None:
        raise HTTPException(status_code=404, detail="Entry not found")

    return LedgerEntrySchema.from_model(record)


@router.post("/debug/ledger/write", response_model=LedgerEntrySchema)
def debug_write_entry(entry: LedgerEntrySchema) -> LedgerEntrySchema:
    """Persist a ledger entry using the in-memory store for debugging."""

    model_entry = entry.to_model()
    debug_ledger_store.upsert(model_entry)
    return LedgerEntrySchema.from_model(model_entry)


@search_router.get("/search")
def search_entries(
    request: Request,
    entity: str = Query(..., description="Logical entity context."),
    q: str | None = Query(None, description="Query string to match against metadata."),
    mode: str = Query(
        "any",
        description="Set combination strategy: 'any' (union) or 'all' (intersection).",
    ),
    limit: int = Query(50, ge=1, le=200, description="Maximum number of results."),
    store: LedgerStoreV2 = Depends(get_ledger_store),
):
    token_index = TokenPrimeIndex(request.app)

    cleaned_query = (q or "").strip()
    cleaned_mode = (mode or "any").strip()
    logger.info(
        "Handling /search request",
        extra={"entity": entity, "query": cleaned_query, "mode": cleaned_mode, "limit": limit},
    )

    try:
        if cleaned_query:
            results = search_service.search(
                cleaned_query,
                store=store,
                token_index=token_index,
                mode=cleaned_mode,
                limit=limit,
            )
            results = search_service.filter_results_for_entity(results, entity)
        else:
            results = search_service.list_recent_entries(
                store, entity=entity, limit=limit
            )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    payload = {"query": cleaned_query, "mode": cleaned_mode, "results": results, "entity": entity}
    logger.info(
        "Search results prepared",
        extra={"entity": entity, "query": cleaned_query, "mode": cleaned_mode, "limit": limit, "result_count": len(results)},
    )
    return payload


__all__ = [
    "debug_ledger_store",
    "get_ledger_store",
    "parse_key",
    "router",
    "search_router",
]
