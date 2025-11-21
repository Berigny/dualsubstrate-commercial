"""REST endpoints for ledger interactions backed by the Field-X kernel."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from backend.api.schemas import LedgerEntrySchema
from backend.fieldx_kernel import LedgerKey, LedgerStore

router = APIRouter(prefix="/ledger", tags=["ledger"])

# Simple in-memory store used by both ledger and governance endpoints.
ledger_store = LedgerStore()


def parse_key(key_path: str) -> LedgerKey:
    """Parse a ledger key path of the form ``namespace:identifier``."""

    if ":" in key_path:
        namespace, identifier = key_path.split(":", 1)
    else:
        namespace, identifier = "default", key_path

    if not namespace or not identifier:
        raise HTTPException(status_code=400, detail="Invalid ledger key")

    return LedgerKey(namespace=namespace, identifier=identifier)


@router.post("/write", response_model=LedgerEntrySchema)
def write_entry(entry: LedgerEntrySchema) -> LedgerEntrySchema:
    """Persist a ledger entry in the shared store."""

    model_entry = entry.to_model()
    ledger_store.upsert(model_entry)
    return LedgerEntrySchema.from_model(model_entry)


@router.get("/read/{entry_id}", response_model=LedgerEntrySchema)
def read_entry(entry_id: str) -> LedgerEntrySchema:
    """Return a ledger entry for the provided identifier."""

    key = parse_key(entry_id)
    record = ledger_store.get(key)
    if record is None:
        raise HTTPException(status_code=404, detail="Entry not found")

    return LedgerEntrySchema.from_model(record)


__all__ = ["ledger_store", "parse_key", "router"]
