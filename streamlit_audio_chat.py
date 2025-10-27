import os
import time

import requests
import streamlit as st
from openai import OpenAI
from io import BytesIO
from s1_s2_memory import MemoryLoop

# ---------- config ----------
FASTAPI_ROOT_DEFAULT = os.getenv("FASTAPI_ROOT", "http://localhost:8000")
FASTAPI_ROOT = FASTAPI_ROOT_DEFAULT  # will be overwritten by sidebar input
# ----------------------------

st.set_page_config(page_title="Dual-Substrate Audio Memory", layout="centered")
st.title("🎙️ Dual-Substrate Audio Memory Demo")

API_HEADERS: dict[str, str] = {}
openai_client: OpenAI | None = None

# ---------- helpers ----------
def qp_put(key: bytes, value: str):
    url = f"{FASTAPI_ROOT}/qp/{key.hex()}"
    payload = {"value": value}
    headers = API_HEADERS or {}
    r = requests.post(url, json=payload, headers=headers)
    r.raise_for_status()

def qp_get(key: bytes) -> str | None:
    url = f"{FASTAPI_ROOT}/qp/{key.hex()}"
    headers = API_HEADERS or {}
    r = requests.get(url, headers=headers)
    if r.status_code == 404:
        return None
    return r.json()["value"]

# ---------- sidebar ----------
with st.sidebar:
    default_openai_key = os.getenv("OPENAI_API_KEY", "")
    api_key = st.text_input(
        "OpenAI API key",
        type="password",
        value=default_openai_key,
        help="Required to use Whisper and chat completions.",
    )
    fastapi_root_input = st.text_input(
        "DualSubstrate API base URL",
        value=FASTAPI_ROOT_DEFAULT,
        help="Local dev default is http://localhost:8000",
    )
    FASTAPI_ROOT = fastapi_root_input.rstrip("/") or FASTAPI_ROOT_DEFAULT
    default_fastapi_key = os.getenv("DUALSUBSTRATE_API_KEY", "")
    fastapi_api_key = st.text_input(
        "DualSubstrate API key",
        type="password",
        value=default_fastapi_key,
        help="Matches the API_KEYS value used when starting uvicorn.",
    )
    if not fastapi_api_key:
        st.info("Enter a DualSubstrate API key to continue.")
        st.stop()
    API_HEADERS = {"Authorization": f"Bearer {fastapi_api_key}"}
    if not api_key:
        st.stop()
    openai_client = OpenAI(api_key=api_key)
    salience_threshold = st.slider("Salience threshold (S₁)", 0.3, 0.95, 0.7, 0.05)

# ---------- state ----------
if "messages" not in st.session_state:
    st.session_state.messages = []
if "facts" not in st.session_state:
    st.session_state.facts = []
if "start_time" not in st.session_state:
    st.session_state.start_time = None
if "memory_loop" not in st.session_state:
    st.session_state.memory_loop = MemoryLoop()

st.session_state.memory_loop.threshold = salience_threshold

# ---------- UI ----------
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])

def add_fact(fact):
    st.session_state.facts.append(fact)
    st.info(f"Memory injected: {fact}")

# ---------- audio recorder ----------
audio_input = st.audio_input("Press to talk", key="audio")
if audio_input and st.session_state.start_time is None:
    st.session_state.start_time = time.time()


def require_openai() -> OpenAI:
    if openai_client is None:
        st.error("OpenAI client not configured.")
        st.stop()
    return openai_client


def transcribe(audio_bytes: bytes) -> str:
    client = require_openai()
    audio_io = BytesIO(audio_bytes)
    audio_io.name = "audio.webm"
    transcript = client.audio.transcriptions.create(
        model="whisper-1",
        file=audio_io,
    )
    return transcript.text

def reply(user_text: str) -> str:
    st.session_state.messages.append({"role": "user", "content": user_text})
    client = require_openai()
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=st.session_state.messages,
        temperature=0.2,
        max_tokens=150,
    )
    bot_text = response.choices[0].message.content or ""
    st.session_state.messages.append({"role": "assistant", "content": bot_text})
    return bot_text

memory_loop = st.session_state.memory_loop

if audio_input:
    raw_audio = audio_input.getvalue()
    text = transcribe(raw_audio)
    st.write("Transcribed:", text)

    now = time.time()

    try:
        outcome = memory_loop.process(
            text,
            store_fn=qp_put,
            fetch_fn=qp_get,
            now=now,
        )
    except requests.RequestException as exc:
        st.warning(f"Memory pipeline error: {exc}")
        outcome = None

    user_text = text
    if outcome:
        if outcome.stored and outcome.score is not None:
            st.caption(f"Salience score {outcome.score:.2f} → stored in Qp")
        if outcome.fact:
            add_fact(outcome.fact)
            user_text = f"{user_text}\n[Memory: {outcome.fact}]"

    bot = reply(user_text)
    with st.chat_message("assistant"):
        st.write(bot)

# ---------- controls ----------
if st.button("Stop talking"):
    st.session_state.start_time = None
    try:
        fact = memory_loop.force_consolidate(qp_get)
    except requests.RequestException as exc:
        st.warning(f"Memory consolidation error: {exc}")
        fact = None
    if fact:
        add_fact(fact)

if st.session_state.facts:
    with st.expander("Retrieved facts"):
        for f in st.session_state.facts:
            st.write("- ", f)
