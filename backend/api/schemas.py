"""Pydantic schemas bridging API requests to Field-X models."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field

from backend.fieldx_kernel.models import ContinuousState, LedgerEntry, LedgerKey


class LedgerKeySchema(BaseModel):
    """API representation of a ledger key."""

    model_config = ConfigDict(extra="ignore")

    namespace: str
    identifier: str

    def to_model(self) -> LedgerKey:
        """Convert the schema to the internal dataclass."""

        return LedgerKey(namespace=self.namespace, identifier=self.identifier)


class ContinuousStateSchema(BaseModel):
    """API payload describing a continuous state."""

    model_config = ConfigDict(extra="ignore")

    coordinates: Dict[str, float] = Field(default_factory=dict)
    phase: str | None = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def to_model(self) -> ContinuousState:
        """Convert the schema into a ``ContinuousState`` instance."""

        state = ContinuousState(
            coordinates=dict(self.coordinates),
            phase=self.phase,
            metadata=dict(self.metadata),
        )
        return state


class LedgerEntrySchema(BaseModel):
    """Schema capturing a full ledger entry with metadata."""

    model_config = ConfigDict(extra="ignore")

    key: LedgerKeySchema
    state: ContinuousStateSchema
    created_at: datetime | None = None
    notes: str | None = None

    def to_model(self) -> LedgerEntry:
        """Convert the schema into a ``LedgerEntry`` record."""

        created = self.created_at or datetime.utcnow()
        return LedgerEntry(
            key=self.key.to_model(),
            state=self.state.to_model(),
            created_at=created,
            notes=self.notes,
        )

    @classmethod
    def from_model(cls, entry: LedgerEntry) -> "LedgerEntrySchema":
        """Build the schema from a ledger dataclass."""

        return cls(
            key=LedgerKeySchema(**entry.key.__dict__),
            state=ContinuousStateSchema(
                coordinates=entry.state.coordinates,
                phase=entry.state.phase,
                metadata=entry.state.metadata,
            ),
            created_at=entry.created_at,
            notes=entry.notes,
        )
