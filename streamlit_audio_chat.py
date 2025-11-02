"""Streamlit UI for Dual-Substrate live memory demo."""

import json
import os
from pathlib import Path

import streamlit as st
from streamlit.errors import StreamlitSecretNotFoundError


def _clean_secret(value: str) -> str:
    return "".join(ch for ch in value if not ch.isspace())


def _host_from_url(url: str) -> str:
    return (
        url.replace("https://", "")
        .replace("http://", "")
        .rstrip("/")
    )


def _secret_or_env(key: str) -> str:
    """Read a secret from Streamlit or environment, returning an empty string on failure."""

    try:
        value = st.secrets[key]
    except (KeyError, StreamlitSecretNotFoundError):
        value = os.getenv(key, "")
    return _clean_secret(value or "")


st.set_page_config(page_title="Dual-Substrate Audio Memory", layout="wide")
st.title("Dual-Substrate Audio Memory")
st.caption("Real-time salience over Fly-hosted Dual-Substrate APIs.")


with st.sidebar:
    st.subheader("Connection")
    fastapi_default = _secret_or_env("FASTAPI_ROOT")
    dualsubstrate_default = _secret_or_env("DUALSUBSTRATE_API_KEY")
    fastapi_root = _clean_secret(
        st.text_input("FASTAPI root", value=fastapi_default or "https://")
    )
    dualsubstrate_key = _clean_secret(
        st.text_input(
            "DualSubstrate API key",
            value=dualsubstrate_default,
            type="password",
        )
    )

if not fastapi_root or not dualsubstrate_key:
    st.error(
        "Provide FASTAPI_ROOT and DUALSUBSTRATE_API_KEY to stream live audio via Fly."
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
    config_payload = json.dumps(
        {
            "wsHost": ws_host,
            "apiKey": dualsubstrate_key,
            "exactBase": f"{fastapi_root.rstrip('/')}/exact",
        }
    )
    html = f"""
    <div>
      <span id=\"badge\">Waiting for mic…</span> | <span id=\"vad\">Energy: 0.0</span>
      <ul id=\"keys\"></ul>
    </div>
    <script>
      window.liveMemoryConfig = {config_payload};
    </script>
    <script>{component_js}</script>
    """
    st.components.v1.html(html, height=320)
else:
    st.info("Press ▶️ Start streaming to begin streaming audio to Fly.")
