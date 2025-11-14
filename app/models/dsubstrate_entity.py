"""Pydantic schema for Dual-Substrate ledger entities (v1.1)."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, root_validator, validator


def _is_prime(value: int) -> bool:
    if value < 2:
        return False
    if value in (2, 3):
        return True
    if value % 2 == 0:
        return False
    factor = 3
    while factor * factor <= value:
        if value % factor == 0:
            return False
        factor += 2
    return True


class RelatedSlot(BaseModel):
    entity: str
    prime: int

    @validator("prime")
    def _prime_non_negative(cls, value: int) -> int:
        if value < 2:
            raise ValueError("related_slots.prime must be >=2")
        return value

    class Config:
        extra = "forbid"


class VectorDescriptor(BaseModel):
    momentum: Optional[str] = None
    bound_with: Optional[List[str]] = None
    strength: Optional[float] = None

    class Config:
        extra = "forbid"


class S1Facet(BaseModel):
    what_new: Optional[str] = None
    title: Optional[str] = None
    key_tags: Optional[List[str]] = None
    related_tags: Optional[List[str]] = None
    related_slots: Optional[List[RelatedSlot]] = None
    timestamp: Optional[str] = None
    location: Optional[str] = None
    vector: Optional[VectorDescriptor] = None
    write_primes: List[int] = Field(default_factory=list)

    @validator("write_primes", each_item=True)
    def _ensure_large_primes(cls, value: int) -> int:
        if value < 23 or not _is_prime(value):
            raise ValueError("write_primes entries must be primes >=23")
        return value

    @validator("key_tags", "related_tags", each_item=True)
    def _strip_strings(cls, value: str) -> str:
        return value.strip()

    class Config:
        extra = "allow"


class Definition(BaseModel):
    term: str
    text: str

    class Config:
        extra = "forbid"


class TestSpec(BaseModel):
    type: str
    must_have: Optional[List[str]] = None

    class Config:
        extra = "allow"


class S2Facet(BaseModel):
    summary: Optional[str] = None
    summary_ref: Optional[int] = None
    owner: Optional[str] = None
    scope: Optional[str] = None
    ontology_refs: Optional[List[str]] = None
    definitions: Optional[List[Definition]] = None
    tests: Optional[List[TestSpec]] = None
    refs: Optional[List[int]] = None

    @validator("summary_ref")
    def _validate_summary_ref(cls, value: Optional[int]) -> Optional[int]:
        if value is None:
            return value
        if value < 23 or not _is_prime(value):
            raise ValueError("summary_ref must reference a prime >=23")
        return value

    @validator("refs", each_item=True)
    def _validate_refs(cls, value: int) -> int:
        if value < 23 or not _is_prime(value):
            raise ValueError("refs entries must be primes >=23")
        return value

    class Config:
        extra = "allow"


class QuoteDescriptor(BaseModel):
    span: Optional[List[int]] = None
    text: str
    source_url: Optional[str] = None

    @validator("span")
    def _validate_span(cls, value: Optional[List[int]]) -> Optional[List[int]]:
        if value is None:
            return value
        if len(value) != 2:
            raise ValueError("quote span must contain exactly two integers")
        start, end = value
        if start < 0 or end < 0 or end < start:
            raise ValueError("quote span must be non-negative and ordered")
        return value

    class Config:
        extra = "forbid"


class BodyNormalised(BaseModel):
    title: Optional[str] = None
    author: Optional[str] = None
    year: Optional[int] = None
    topics: Optional[List[str]] = None
    quotes: Optional[List[QuoteDescriptor]] = None

    class Config:
        extra = "allow"


class BodyProvenance(BaseModel):
    ingested_at: Optional[str] = None
    by: Optional[str] = None

    class Config:
        extra = "allow"


class BodyShard(BaseModel):
    content_type: str = "text/plain"
    text: str
    hash: Optional[str] = None
    norm: Optional[BodyNormalised] = None
    provenance: Optional[BodyProvenance] = None
    updated_at: Optional[int] = None
    kind: Optional[str] = None
    version: Optional[str] = None
    lawfulness_level: Optional[int] = None

    @validator("text")
    def _ensure_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("body text must not be empty")
        return value

    @validator("lawfulness_level")
    def _validate_lawfulness_level(cls, value: Optional[int]) -> Optional[int]:
        if value is None:
            return value
        if not 0 <= value <= 3:
            raise ValueError("lawfulness_level must be between 0 and 3")
        return value

    class Config:
        extra = "allow"


class SlotsPayload(BaseModel):
    S1: Dict[str, S1Facet] = Field(default_factory=dict)
    S2: Dict[str, S2Facet] = Field(default_factory=dict)
    body: Dict[str, BodyShard] = Field(default_factory=dict)

    @validator("S1", pre=True)
    def _ensure_dict(cls, value: Any) -> Dict[str, Any]:
        return value or {}

    @validator("S2", pre=True)
    def _ensure_dict_s2(cls, value: Any) -> Dict[str, Any]:
        return value or {}

    @validator("body", pre=True)
    def _ensure_dict_body(cls, value: Any) -> Dict[str, Any]:
        return value or {}

    @validator("S1")
    def _validate_s1_keys(cls, value: Dict[str, S1Facet]) -> Dict[str, S1Facet]:
        allowed = {"2", "3", "5", "7"}
        for key in value.keys():
            if key not in allowed:
                raise ValueError("S1 slots must be keyed by primes 2,3,5,7")
        return value

    @validator("S2")
    def _validate_s2_keys(cls, value: Dict[str, S2Facet]) -> Dict[str, S2Facet]:
        allowed = {"11", "13", "17", "19"}
        for key in value.keys():
            if key not in allowed:
                raise ValueError("S2 slots must be keyed by primes 11,13,17,19")
        return value

    @validator("body")
    def _validate_body_keys(cls, value: Dict[str, BodyShard]) -> Dict[str, BodyShard]:
        for key in value.keys():
            try:
                numeric = int(key)
            except (TypeError, ValueError) as exc:
                raise ValueError("body slot keys must be prime integers") from exc
            if numeric < 23 or not _is_prime(numeric):
                raise ValueError("body slot keys must be primes >=23")
        return value

    class Config:
        extra = "forbid"


class RMetrics(BaseModel):
    dE: float
    dDrift: float
    dRetention: float
    K: float

    class Config:
        extra = "forbid"


class MetaPayload(BaseModel):
    source: Optional[str] = None
    provenance: Optional[List[Any]] = None
    schema: Optional[str] = None

    class Config:
        extra = "allow"


class Factor(BaseModel):
    prime: int
    value: float
    symbol: str
    tier: str
    mnemonic: str

    @validator("prime")
    def _validate_prime(cls, value: int) -> int:
        if value < 2 or not _is_prime(value):
            raise ValueError("factor primes must be >=2 and prime")
        return value

    @validator("tier")
    def _validate_tier(cls, value: str) -> str:
        if value not in {"S1", "S2"}:
            raise ValueError("factor tier must be either 'S1' or 'S2'")
        return value

    class Config:
        extra = "allow"


class DSubstrateEntity(BaseModel):
    entity: str
    version: str = "1.1"
    tier: str = "S1"
    lawfulness: int = 1
    created_at: str
    updated_at: str
    factors: List[Factor] = Field(default_factory=list)
    meta: MetaPayload = Field(default_factory=MetaPayload)
    slots: SlotsPayload = Field(default_factory=SlotsPayload)
    r_metrics: RMetrics = Field(default_factory=lambda: RMetrics(dE=0.0, dDrift=0.0, dRetention=0.0, K=0.0))

    @validator("tier")
    def _validate_entity_tier(cls, value: str) -> str:
        if value not in {"S1", "S2"}:
            raise ValueError("tier must be either 'S1' or 'S2'")
        return value

    @validator("lawfulness")
    def _validate_lawfulness(cls, value: int) -> int:
        if value < 0 or value > 3:
            raise ValueError("lawfulness must be between 0 and 3")
        return value

    @root_validator
    def _ensure_version(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        version = values.get("version")
        if version != "1.1":
            raise ValueError("version must be '1.1' for Dual-Substrate entities")
        return values

    class Config:
        extra = "allow"
