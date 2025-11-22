"""Administrative endpoints for managing search indexes."""

import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from backend.api.http import get_db
from backend.api.logging_utils import log_operation
from backend.search.reindex import reindex_all


router = APIRouter(prefix="/admin", tags=["admin"])
LOGGER = logging.getLogger(__name__)


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


__all__ = ["router"]
