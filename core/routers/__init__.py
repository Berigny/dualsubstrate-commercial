"""FastAPI routers for the DualSubstrate API."""

from .score import router as score_router

__all__ = ["score_router"]
