"""FastAPI application exposing ledger and checksum endpoints."""

from __future__ import annotations

from typing import List

from fastapi import Depends, FastAPI, HTTPException, status

from core import checksum as checksum_core
from . import models
from .deps import get_current_user, rate_limiter

app = FastAPI(
    title="DualSubstrate MVP",
    version="0.1.0",
    description="Prototype API surface for carry-free arithmetic services.",
)

_EVENTS: List[models.EventOut] = []


@app.get("/health")
async def health() -> dict[str, str]:
    """Simple health check endpoint."""
    return {"status": "ok"}


@app.post(
    "/events",
    response_model=models.EventOut,
    status_code=status.HTTP_202_ACCEPTED,
)
async def append_event(
    event: models.EventIn,
    _: str = Depends(get_current_user),
    __: None = Depends(rate_limiter),
) -> models.EventOut:
    """Append an event to the in-memory ledger."""
    offset = len(_EVENTS)
    record = models.EventOut(offset=offset, payload=event.payload)
    _EVENTS.append(record)
    return record


@app.get("/events/{offset}", response_model=models.EventOut)
async def get_event(
    offset: int,
    _: str = Depends(get_current_user),
) -> models.EventOut:
    """Fetch a single event by its offset."""
    try:
        return _EVENTS[offset]
    except IndexError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Event not found"
        ) from exc


@app.get("/ledger/head", response_model=models.LedgerHead)
async def ledger_head(_: str = Depends(get_current_user)) -> models.LedgerHead:
    """Return summary details for the in-memory ledger head."""
    return models.LedgerHead(length=len(_EVENTS))


@app.post("/checksum", response_model=models.ChecksumResponse)
async def compute_checksum(
    request: models.ChecksumRequest,
    _: str = Depends(get_current_user),
) -> models.ChecksumResponse:
    """Compute a Merkle checksum over supplied (prime, digits) tuples."""
    items = [(item.prime, item.digits) for item in request.items]
    digest = checksum_core.merkle_root(items)
    return models.ChecksumResponse(root=digest.hex())


@app.delete("/events", status_code=status.HTTP_204_NO_CONTENT)
async def reset_events(_: str = Depends(get_current_user)) -> None:
    """Reset the in-memory event ledger."""
    _EVENTS.clear()
