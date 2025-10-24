from pydantic import BaseModel
from typing import List, Tuple

class Factor(BaseModel):
    prime: int
    delta: int

class AnchorReq(BaseModel):
    entity: str
    factors: List[Factor]

class QueryReq(BaseModel):
    primes: List[int]
