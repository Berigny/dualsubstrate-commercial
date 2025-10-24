"""Authentication and rate limiting dependencies for the API."""

from __future__ import annotations

import asyncio

from fastapi import Header, HTTPException, status

API_KEY_HEADER = "x-api-key"
DUMMY_API_KEY = "mvp-secret"


async def get_current_user(api_key: str = Header(default="")) -> str:
    """Validate the provided API key header."""
    if not api_key or api_key != DUMMY_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )
    return "mvp-user"


async def rate_limiter() -> None:
    """Async placeholder for future rate limiting controls."""
    await asyncio.sleep(0)

