"""Pydantic request and response models for the public API."""

from __future__ import annotations

from datetime import datetime
from typing import Iterable, List

from pydantic import BaseModel, Field


class EventIn(BaseModel):
    payload: str = Field(..., description="Base64 or hex encoded payload")


class EventOut(BaseModel):
    offset: int
    payload: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class LedgerHead(BaseModel):
    length: int


class ChecksumItem(BaseModel):
    prime: int
    digits: List[int]


class ChecksumRequest(BaseModel):
    items: List[ChecksumItem] = Field(default_factory=list)


class ChecksumResponse(BaseModel):
    root: str
