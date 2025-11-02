"""FastAPI backend for Dual-Substrate live audio memory."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
import threading
import time
from contextlib import asynccontextmanager
from io import BytesIO
from typing import Any, Dict

import requests
from fastapi import FastAPI, HTTPException, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.websockets import WebSocketDisconnect
from openai import OpenAI
from prometheus_client import make_asgi_app
from rocksdict import Rdict

from backend.routers import qp_rest


LOGGER = logging.getLogger(__name__)

WS_ROUTE = "/ws"
PCM_ROUTE = "/pcm"

DB_PATH_ENV = os.getenv("DB_PATH", "./data")
DB_FILE = "ledger.db"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Open RocksDB on application startup and close it on shutdown.
    Tests can override DB_PATH via environment to isolate state.
    """

    db_base = DB_PATH_ENV or tempfile.gettempdir()
    db_full = os.path.join(db_base, DB_FILE)
    os.makedirs(os.path.dirname(db_full), exist_ok=True)
    db = Rdict(db_full)
    app.state.db = db
    LOGGER.info("RocksDB opened at %s", db_full)
    try:
        yield
    finally:
        db.close()
        LOGGER.info("RocksDB closed")


_STATE_LOCK = threading.Lock()
_STATE: Dict[str, Any] = {
    "backend": None,
    "headers": {},
    "threshold": 0.7,
    "baseline": False,
    "client": None,
}


def set_state(**updates: Any) -> None:
    """Merge ``updates`` into the shared backend state."""
    with _STATE_LOCK:
        _STATE.update(updates)


def get_state() -> Dict[str, Any]:
    """Return a shallow copy of the backend state."""
    with _STATE_LOCK:
        return dict(_STATE)


def configure(
    backend: str | None = None,
    api_key: str | None = None,
    openai_key: str | None = None,
    threshold: float | None = None,
    baseline: bool | None = None,
) -> None:
    """Configure runtime state for the websocket + proxy service."""

    updates: Dict[str, Any] = {}

    if backend:
        updates["backend"] = backend.rstrip("/")
    if api_key:
        updates["headers"] = {"Authorization": f"Bearer {api_key}"}
    if threshold is not None:
        updates["threshold"] = float(threshold)
    if baseline is not None:
        updates["baseline"] = bool(baseline)

    if openai_key:
        updates["client"] = OpenAI(api_key=openai_key)
    elif openai_key == "":
        updates["client"] = None

    if updates:
        set_state(**updates)


def configure_from_env() -> None:
    """Load configuration from environment variables if present."""

    backend = os.getenv("FASTAPI_ROOT")
    api_key = os.getenv("DUALSUBSTRATE_API_KEY")
    openai_key = os.getenv("OPENAI_API_KEY")

    threshold_raw = os.getenv("SALIENCE_THRESHOLD")
    baseline_raw = os.getenv("BASELINE_MODE")

    threshold = float(threshold_raw) if threshold_raw else None
    baseline = None
    if baseline_raw:
        baseline = baseline_raw.strip().lower() in {"1", "true", "yes", "on"}

    configure(
        backend=backend,
        api_key=api_key,
        openai_key=openai_key,
        threshold=threshold,
        baseline=baseline,
    )

    state = get_state()
    if state.get("client") is None:
        LOGGER.warning("OPENAI_API_KEY not configured – transcription disabled")
    if not state.get("backend"):
        LOGGER.warning("FASTAPI_ROOT not configured – /salience proxy disabled")


def _transcribe_chunk(data: bytes) -> str:
    state = get_state()
    client: OpenAI | None = state.get("client")
    if client is None:
        raise RuntimeError("OpenAI client not configured")

    audio = BytesIO(data)
    audio.name = "chunk.webm"
    transcript = client.audio.transcriptions.create(model="whisper-1", file=audio)
    return (transcript.text or "").strip()


def _call_salience(
    backend: str,
    headers: Dict[str, str],
    text: str,
    threshold: float,
    timestamp: float,
) -> Dict[str, Any]:
    payload = {"utterance": text, "timestamp": timestamp, "threshold": threshold}
    try:
        response = requests.post(
            f"{backend}/salience", json=payload, headers=headers, timeout=15
        )
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as exc:
        return {
            "stored": False,
            "text": text,
            "error": str(exc),
            "score": None,
            "timestamp": timestamp,
        }
    except ValueError as exc:
        return {
            "stored": False,
            "text": text,
            "error": f"Invalid JSON from backend: {exc}",
            "score": None,
            "timestamp": timestamp,
        }

    data.setdefault("timestamp", timestamp)
    data.setdefault("text", text)
    return data


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)
app.include_router(qp_rest.router)


@app.get("/health", include_in_schema=False)
def health() -> Dict[str, str]:
    """Return a simple status payload for health checks."""

    return {"status": "ok"}


@app.websocket(WS_ROUTE)
async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    loop = asyncio.get_running_loop()
    try:
        while True:
            try:
                payload = await websocket.receive_bytes()
            except WebSocketDisconnect:
                break

            try:
                text = await loop.run_in_executor(None, _transcribe_chunk, payload)
            except Exception as exc:  # pragma: no cover - defensive logging
                await websocket.send_text(json.dumps({"stored": False, "error": str(exc)}))
                continue

            if not text:
                continue

            state = get_state()
            backend = state.get("backend")
            headers = state.get("headers", {})
            threshold = float(state.get("threshold", 0.7))
            baseline = bool(state.get("baseline", False))
            timestamp = time.time()

            if baseline or not backend:
                await websocket.send_text(
                    json.dumps(
                        {
                            "stored": False,
                            "text": text,
                            "score": None,
                            "timestamp": timestamp,
                            "baseline": baseline,
                            "reason": "baseline" if baseline else "backend-unset",
                        }
                    )
                )
                continue

            result = await loop.run_in_executor(
                None, _call_salience, backend, headers, text, threshold, timestamp
            )
            await websocket.send_text(json.dumps(result))
    finally:  # pragma: no cover - best-effort close
        await websocket.close()


@app.websocket(PCM_ROUTE)
async def pcm_endpoint(websocket: WebSocket) -> None:
    """Relay raw PCM frames back to the browser for visualisation."""
    await websocket.accept()
    try:
        while True:
            try:
                data = await websocket.receive_bytes()
            except WebSocketDisconnect:
                break
            await websocket.send_bytes(data)
    finally:  # pragma: no cover - best-effort close
        await websocket.close()


@app.get("/exact/{key_hex}")
def proxy_exact(key_hex: str) -> JSONResponse:
    state = get_state()
    backend = state.get("backend")
    headers = state.get("headers", {})
    if not backend:
        raise HTTPException(status_code=503, detail="Backend not configured")
    try:
        response = requests.get(f"{backend}/exact/{key_hex}", headers=headers, timeout=10)
    except requests.RequestException as exc:  # pragma: no cover - network failure
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    try:
        data = response.json()
    except ValueError as exc:  # pragma: no cover - backend bug guard
        raise HTTPException(status_code=502, detail=f"Invalid JSON from backend: {exc}") from exc
    return JSONResponse(data)


configure_from_env()

__all__ = [
    "PCM_ROUTE",
    "WS_ROUTE",
    "app",
    "configure",
    "configure_from_env",
    "get_state",
    "set_state",
]
