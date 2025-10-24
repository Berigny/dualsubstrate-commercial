import os, time
from fastapi import HTTPException, Depends
from slowapi import Limiter
from slowapi.util import get_remote_address

KEYS = set(os.getenv("API_KEYS", "demo-key").split(","))
limiter = Limiter(key_func=get_remote_address)

def require_key(key: str = Depends(lambda: None)):
    # FastAPI header dependency
    from fastapi.security import HTTPBearer
    scheme = HTTPBearer()
    def checker(tok = Depends(scheme)):
        if tok.credentials not in KEYS:
            raise HTTPException(401, "Invalid key")
    return Depends(checker)
