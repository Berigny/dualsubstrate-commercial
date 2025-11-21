"""Administrative endpoints for managing search indexes."""

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from backend.api.http import get_db
from backend.search.reindex import reindex_all


router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/reindex")
def trigger_reindex(
    request: Request,
    entity: str | None = Query(None, description="Optional logical entity context."),
    db=Depends(get_db),
):
    """Rebuild the token index and refreshed metadata for all ledger entries."""

    try:
        return reindex_all(request.app, entity=entity)
    except Exception as exc:  # pragma: no cover - defensive guardrail
        raise HTTPException(status_code=500, detail=str(exc)) from exc


__all__ = ["router"]
