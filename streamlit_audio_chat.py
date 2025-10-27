"""Streamlit UI for Dual-Substrate live memory demo."""

import asyncio
import json
import os
import threading
import time
from io import BytesIO
from pathlib import Path
from typing import Any, Dict

import requests
import streamlit as st
import streamlit.components.v1 as components
from fastapi import FastAPI, HTTPException, WebSocket
from fastapi.responses import JSONResponse
from fastapi.websockets import WebSocketDisconnect
from openai import OpenAI
import uvicorn


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

WS_PORT = int(os.getenv("STREAMLIT_WS_PORT", "8765"))
WS_ROUTE = "/ws"
PCM_ROUTE = "/pcm"

STATE_LOCK = threading.Lock()
WS_STATE: Dict[str, Any] = {
    "backend": None,
    "headers": {},
    "threshold": 0.7,
    "baseline": False,
    "client": None,
}


def _set_state(**updates: Any) -> None:
    with STATE_LOCK:
        WS_STATE.update(updates)


def _get_state() -> Dict[str, Any]:
    with STATE_LOCK:
        return dict(WS_STATE)


def _clean_secret(value: str) -> str:
    return "".join(ch for ch in value if not ch.isspace())


# ---------------------------------------------------------------------------
# Live console backend (WebSocket + /exact proxy)
# ---------------------------------------------------------------------------

def _transcribe_chunk(data: bytes) -> str:
    state = _get_state()
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


app = FastAPI()


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

            state = _get_state()
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
    state = _get_state()
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


def _run_ws_server() -> None:
    asyncio.set_event_loop(asyncio.new_event_loop())

    def _pcm_sender() -> None:
        """Optional helper that forwards local PCM frames to the visualiser."""
        try:
            import pyaudio  # type: ignore
            import websocket  # type: ignore
        except ImportError:
            return

        PCM_WS = f"ws://localhost:{WS_PORT}{PCM_ROUTE}"
        CHUNK = 1024
        FORMAT = pyaudio.paInt16
        CHANNELS = 1
        RATE = 16000

        try:
            ws = websocket.create_connection(PCM_WS, timeout=5)
        except Exception:
            return

        pa = pyaudio.PyAudio()
        try:
            stream = pa.open(
                format=FORMAT,
                channels=CHANNELS,
                rate=RATE,
                input=True,
                frames_per_buffer=CHUNK,
            )
        except Exception:
            ws.close()
            pa.terminate()
            return

        try:
            while True:
                pcm = stream.read(CHUNK, exception_on_overflow=False)
                try:
                    ws.send(pcm, opcode=websocket.ABNF.OPCODE_BINARY)
                except Exception:
                    break
        finally:
            try:
                stream.stop_stream()
                stream.close()
            finally:
                pa.terminate()
            try:
                ws.close()
            except Exception:
                pass

    threading.Timer(1.0, _pcm_sender).start()
    uvicorn.run(app, host="0.0.0.0", port=WS_PORT, log_level="error")


# ---------------------------------------------------------------------------
# Streamlit front-end
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Dual-Substrate Audio Memory", layout="wide")
st.title("Dual-Substrate Audio Memory")
st.caption(
    "Grant the microphone once, then watch the live console stream salient fragments into the substrate."
)

with st.sidebar:
    st.header("Configuration")
    default_backend = os.getenv("FASTAPI_ROOT", "http://localhost:8000")
    backend_input = st.text_input(
        "DualSubstrate API base URL",
        value=default_backend,
        help="Local development defaults to http://localhost:8000",
    )
    dualsubstrate_key_input = st.text_input(
        "DualSubstrate API key",
        type="password",
        value=os.getenv("DUALSUBSTRATE_API_KEY", ""),
    )
    openai_key_input = st.text_input(
        "OpenAI API key",
        type="password",
        value=os.getenv("OPENAI_API_KEY", ""),
    )
    salience_threshold = st.slider("Salience threshold (S₁ – websocket)", 0.3, 0.95, 0.7, 0.05)
    vad_threshold = st.slider("VAD energy threshold", 5.0, 40.0, 12.0, 0.5)

backend_base = backend_input.rstrip("/") or default_backend
dualsubstrate_key = _clean_secret(dualsubstrate_key_input)
openai_key = _clean_secret(openai_key_input)

if not dualsubstrate_key:
    st.warning("Enter a DualSubstrate API key to continue.")
    st.stop()

if not openai_key:
    st.warning("Enter an OpenAI API key to continue.")
    st.stop()

API_HEADERS = {"Authorization": f"Bearer {dualsubstrate_key}"}
openai_client = OpenAI(api_key=openai_key)

_set_state(
    backend=backend_base,
    headers=API_HEADERS,
    threshold=float(salience_threshold),
    baseline=False,
    client=openai_client,
)

if "ws_thread" not in st.session_state or st.session_state.ws_thread is None or not st.session_state.ws_thread.is_alive():
    ws_thread = threading.Thread(target=_run_ws_server, daemon=True)
    ws_thread.start()
    st.session_state.ws_thread = ws_thread

st.markdown(
    """
**Workflow**

1. Grant microphone access (one tap). The recording is discarded – we only need the permission prompt.
2. Watch the live console render energy + spectrogram while the browser streams 250 ms chunks to Streamlit.
3. Streamlit forwards each chunk to the websocket service, which transcribes, hashes, and stores it via `/salience`.
4. Stored keys appear immediately; click any badge to fetch `/exact/{key}` and play back the stored text.
5. Export the session as JSON – the SHA-256 checksum matches the values persisted inside Qp.
    """
)

st.subheader("Step 1 – Grant microphone access")
warmup = st.audio_input(
    "Click once to allow microphone access (the sample is discarded).",
    key="warmup",
)
if warmup:
    st.success("Microphone access granted. The live console below can now stream audio.")
else:
    st.info(
        "If the live console stays on 'Waiting for microphone…', click the recorder above to trigger the browser permission dialog."
    )

st.subheader("Step 2 – Live console")
component_js = (
    Path(__file__).parent
    / "streamlit"
    / "components"
    / "live_memory_v2.js"
).read_text(
    encoding="utf-8"
)
component_config = {
    "wsPort": WS_PORT,
    "wsRoute": WS_ROUTE,
    "pcmRoute": PCM_ROUTE,
    "exactBase": "/exact",
    "vadThreshold": float(vad_threshold),
    "baseline": False,
}

styles = """
<style>
.live-memory {font-family: 'Inter', sans-serif; color: #212529;}
.live-memory .status {display: flex; align-items: center; gap: 1rem; margin-bottom: 0.5rem;}
.live-memory #badge {padding: 0.35rem 0.7rem; border-radius: 999px; background: #f1f3f5; font-weight: 600;}
.live-memory #badge.active {background: #e6fcf5; color: #087f5b;}
.live-memory #vad {font-size: 0.9rem; color: #868e96;}
.live-memory canvas {width: 100%; max-width: 720px; background: #000; border-radius: 4px; box-shadow: inset 0 0 12px rgba(0,0,0,0.4); margin-bottom: 0.75rem;}
.live-memory .controls {display: flex; align-items: center; gap: 1rem; margin-bottom: 0.75rem;}
.live-memory #export-json {background: #1864ab; color: #fff; border: none; padding: 0.5rem 1rem; border-radius: 6px; cursor: pointer;}
.live-memory #export-json:hover {background: #1c7ed6;}
.live-memory #export-hash {font-family: 'Fira Mono', monospace; font-size: 0.85rem; color: #495057;}
.live-memory ul {list-style: none; padding: 0; margin: 0; display: flex; flex-direction: column; gap: 0.5rem;}
.live-memory .memory-item {display: flex; justify-content: space-between; align-items: center; gap: 1rem; padding: 0.5rem 0.75rem; border: 1px solid #e9ecef; border-radius: 6px; background: #fff; box-shadow: 0 1px 3px rgba(0,0,0,0.05);}
.live-memory .memory-btn {background: transparent; border: none; color: #0b7285; font-weight: 600; cursor: pointer; text-align: left;}
.live-memory .memory-btn:hover {text-decoration: underline;}
.live-memory .meta {font-family: 'Fira Mono', monospace; font-size: 0.75rem; color: #868e96;}
.live-memory #log {margin-top: 1rem; max-height: 220px; overflow-y: auto; font-family: 'Fira Mono', monospace; font-size: 0.75rem; background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 6px; padding: 0.75rem; display: flex; flex-direction: column-reverse; gap: 0.35rem;}
</style>
"""

html = f"""
<div class=\"live-memory\">
  <div class=\"status\">
    <span id=\"badge\">Waiting for microphone…</span>
    <span id=\"vad\">Energy: 0.0</span>
  </div>
  <canvas id=\"spectrogram\" width=\"720\" height=\"160\"></canvas>
  <div class=\"controls\">
    <button id=\"export-json\">Export session JSON</button>
    <span id=\"export-hash\"></span>
  </div>
  <ul id=\"keys\"></ul>
  <div id=\"log\"></div>
</div>
<script>window.liveMemoryConfig = {json.dumps(component_config)};</script>
{styles}
<script src="https://cdnjs.cloudflare.com/ajax/libs/jsSHA/3.3.1/sha.min.js"></script>
<script>{component_js}</script>
"""

components.html(html, height=640)

st.markdown(
    """
**Step 3 – Stream & store**: Speak naturally; the console streams chunks, hashes each fragment, and writes through `/salience`.

**Step 4 – Inspect memories**: Stored keys appear as badges. Click any badge to fetch `/exact/{key}` and hear the bit-exact payload.

**Step 5 – Export session**: Use “Export session JSON” to download all captured fragments – the SHA-256 digest shown matches the persisted Qp payloads.
    """
)
