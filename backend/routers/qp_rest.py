from fastapi import APIRouter, HTTPException, Depends, Request
from pydantic import BaseModel

router = APIRouter(prefix="/qp")

class StoreReq(BaseModel):
    value: str

def get_db(request: Request):
    return request.app.state.db   # lifespan opened it

@router.post("/{key_hex}")
def store(key_hex: str, payload: StoreReq, db=Depends(get_db)):
    key = bytes.fromhex(key_hex)
    db[key] = payload.value.encode()
    return {"status": "ok"}

@router.get("/{key_hex}")
def retrieve(key_hex: str, db=Depends(get_db)):
    key = bytes.fromhex(key_hex)
    val = db.get(key)
    if val is None:
        raise HTTPException(status_code=404, detail="Not found")
    return {"key": key_hex, "value": val.decode()}