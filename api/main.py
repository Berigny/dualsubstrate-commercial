from fastapi import FastAPI, Depends, HTTPException
from models import AnchorReq, QueryReq
from core.ledger import Ledger
from deps import require_key, limiter

app = FastAPI(title="DualSubstrate MVP", version="0.1.0")
ledger = Ledger()

@app.post("/anchor")
@limiter.limit("100/minute")
def anchor(req: AnchorReq, _: str = Depends(require_key)):
    ledger.anchor(req.entity, [(f.prime, f.delta) for f in req.factors])
    return {"status": "anchored"}

@app.post("/query")
@limiter.limit("200/minute")
def query(req: QueryReq, _: str = Depends(require_key)):
    hits = ledger.query(req.primes)
    return {"results": [{"entity": e, "weight": w} for e, w in hits]}

@app.get("/checksum")
def checksum(entity: str, _: str = Depends(require_key)):
    return {"entity": entity, "checksum": ledger.checksum(entity)}
