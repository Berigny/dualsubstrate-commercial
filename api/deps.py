import os
from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from slowapi import Limiter
from slowapi.util import get_remote_address

KEYS = {key.strip() for key in os.getenv("API_KEYS", "demo-key").split(",") if key.strip()}
limiter = Limiter(key_func=get_remote_address)
_bearer_scheme = HTTPBearer(auto_error=False)


def require_key(
    credentials: HTTPAuthorizationCredentials | None = Security(_bearer_scheme),
) -> None:
    """Validate incoming Authorization header against configured API keys."""

    token = credentials.credentials if credentials else None
    if token not in KEYS:
        raise HTTPException(status_code=401, detail="Invalid key")
    return None
