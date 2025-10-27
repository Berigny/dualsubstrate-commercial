"""Streamlit UI for Dual-Substrate live memory demo."""

import asyncio
import json
import logging
import os
import threading
from pathlib import Path
from urllib.parse import urlparse

import streamlit as st
import streamlit.components.v1 as components
import uvicorn

from backend import PCM_ROUTE, WS_ROUTE, app as backend_app, configure as configure_backend


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)

WS_PORT = int(os.getenv("STREAMLIT_WS_PORT", "8765"))

try:  # pragma: no cover - optional local dependencies
    import pyaudio  # type: ignore
    import websocket  # type: ignore
except ImportError:  # pragma: no cover - optional local dependencies
    pyaudio = None  # type: ignore[assignment]
    websocket = None  # type: ignore[assignment]


def _clean_secret(value: str) -> str:
    return "".join(ch for ch in value if not ch.isspace())


def _ws_host_from_backend(base: str) -> str | None:
    parsed = urlparse(base if "://" in base else f"http://{base}")
    hostname = parsed.hostname
    if not hostname:
        return None
    if hostname in {"localhost", "127.0.0.1"}:
        port = parsed.port or WS_PORT
        return f"{hostname}:{port}"
    if parsed.port:
        return f"{hostname}:{parsed.port}"
    return hostname


def _run_ws_server() -> None:
    asyncio.set_event_loop(asyncio.new_event_loop())

    def _pcm_sender() -> None:
        """Optional helper that forwards local PCM frames to the visualiser."""
        if pyaudio is None or websocket is None:
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

    if pyaudio:
        threading.Timer(1.0, _pcm_sender).start()
    else:
        logger.info("pyaudio unavailable – PCM visualisation disabled")

    uvicorn.run(backend_app, host="0.0.0.0", port=WS_PORT, log_level="error")


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
if not backend_base.startswith(("http://", "https://")):
    backend_base = f"http://{backend_base}"
dualsubstrate_key = _clean_secret(dualsubstrate_key_input)
openai_key = _clean_secret(openai_key_input)

if not dualsubstrate_key:
    st.warning("Enter a DualSubstrate API key to continue.")
    st.stop()

if not openai_key:
    st.warning("Enter an OpenAI API key to continue.")
    st.stop()

configure_backend(
    backend=backend_base,
    api_key=dualsubstrate_key,
    openai_key=openai_key,
    threshold=float(salience_threshold),
    baseline=False,
)

parsed_backend = urlparse(backend_base if "://" in backend_base else f"http://{backend_base}")
should_run_local_backend = (
    (parsed_backend.hostname in {"localhost", "127.0.0.1"})
    and ((parsed_backend.port or WS_PORT) == WS_PORT)
)

if should_run_local_backend:
    ws_thread = st.session_state.get("ws_thread")
    if ws_thread is None or not ws_thread.is_alive():
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
ws_host = _ws_host_from_backend(backend_base)
component_config = {
    "wsHost": ws_host,
    "wsPort": WS_PORT,
    "wsRoute": WS_ROUTE,
    "pcmRoute": PCM_ROUTE,
    "exactBase": f"{backend_base}/exact",
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
