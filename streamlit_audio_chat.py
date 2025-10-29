"""Streamlit UI for Dual-Substrate live memory demo."""

import os
from pathlib import Path

import streamlit as st


def _clean_secret(value: str) -> str:
    return "".join(ch for ch in value if not ch.isspace())


def _host_from_url(url: str) -> str:
    return (
        url.replace("https://", "")
        .replace("http://", "")
        .rstrip("/")
    )


st.set_page_config(page_title="Dual-Substrate Audio Memory", layout="wide")
st.title("Dual-Substrate Audio Memory")
st.caption("Real-time salience over Fly-hosted Dual-Substrate APIs.")

fastapi_root = _clean_secret(
    st.secrets.get("FASTAPI_ROOT", "") or os.getenv("FASTAPI_ROOT", "")
)
dualsubstrate_key = _clean_secret(
    st.secrets.get("DUALSUBSTRATE_API_KEY", "")
    or os.getenv("DUALSUBSTRATE_API_KEY", "")
)

if not fastapi_root or not dualsubstrate_key:
    st.error(
        "Set FASTAPI_ROOT and DUALSUBSTRATE_API_KEY secrets to stream live audio via Fly."
    )
    st.stop()

ws_host = _host_from_url(fastapi_root)

st.markdown(
    """
- Press **▶️ Start streaming** to open the WebSocket to the Fly deployment.
- Grant microphone access when prompted.
- Stored badges appear instantly; click any badge to fetch `/exact/{key}`.
    """
)

if st.button("▶️ Start streaming", key="start_stream"):
    st.session_state.started = True

if st.session_state.get("started"):
    component_js = (
        Path(__file__).parent / "streamlit" / "components" / "live_minimal.js"
    ).read_text(encoding="utf-8")
    html = f"""
    <div>
      <span id=\"badge\">Waiting for mic…</span> | <span id=\"vad\">Energy: 0.0</span>
      <ul id=\"keys\"></ul>
    </div>
    <script>window.WS_HOST = "{ws_host}";</script>
    <script>{component_js}</script>
    """
    st.components.v1.html(html, height=300)
else:
    st.info("Press ▶️ Start streaming to begin streaming audio to Fly.")
