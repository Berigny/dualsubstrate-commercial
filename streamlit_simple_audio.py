"""Standalone Streamlit demo for the Dual-Substrate audio memory flow."""

from __future__ import annotations

import os
import time
from io import BytesIO
from typing import Optional

import requests
import streamlit as st
from openai import OpenAI


def _clean_secret(value: str) -> str:
    return "".join(ch for ch in value if not ch.isspace())


def _default_fastapi_root() -> str:
    secret_value = ""
    try:
        secret_value = _clean_secret(st.secrets["FASTAPI_ROOT"])
    except Exception:
        secret_value = ""

    if secret_value:
        return secret_value

    env_value = _clean_secret(os.getenv("FASTAPI_ROOT", ""))
    return env_value or "http://localhost:8000"


FASTAPI_ROOT = _default_fastapi_root()

st.set_page_config(page_title="Dual-Substrate Audio Memory", layout="centered")
st.title("🎙️ Dual-Substrate Audio Memory Demo")


def qp_put(key_hex: str, value: str) -> None:
    url = f"{FASTAPI_ROOT}/qp/{key_hex}"
    payload = {"value": value}
    response = requests.post(url, json=payload, timeout=10)
    response.raise_for_status()


def qp_get(key_hex: str) -> Optional[str]:
    url = f"{FASTAPI_ROOT}/qp/{key_hex}"
    response = requests.get(url, timeout=10)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    data = response.json()
    return data.get("value")


def deterministic_key(text: str) -> bytes:
    import hashlib
    import re

    norm = re.sub(r"\W+", "", text.lower().strip())
    return hashlib.blake2b(norm.encode(), digest_size=16).digest()


def _format_timestamp(seconds: int) -> str:
    return f"{seconds // 60}:{seconds % 60:02}"


with st.sidebar:
    st.markdown("Configure the OpenAI key to enable transcription + chat.")
    input_key = st.text_input("OpenAI API key", type="password", key="openai_api_key")
    if not input_key:
        st.stop()
    openai_key = _clean_secret(input_key)

    fastapi_input = st.text_input("FASTAPI root", value=FASTAPI_ROOT)
    FASTAPI_ROOT = _clean_secret(fastapi_input or FASTAPI_ROOT)
    if not FASTAPI_ROOT:
        st.error("FASTAPI root required to reach the Dual-Substrate backend.")
        st.stop()

    minutes = st.selectbox("Conversation length (minutes)", [1, 2, 3, 5, 8, 13, 21, 34, 55])
    quarter = minutes * 15  # seconds
    inject_points = [quarter, 2 * quarter, 3 * quarter]
    st.write("Facts will be injected at:", [_format_timestamp(t) for t in inject_points])

client = OpenAI(api_key=openai_key)

if "messages" not in st.session_state:
    st.session_state.messages = []
if "facts" not in st.session_state:
    st.session_state.facts = []
if "start_time" not in st.session_state:
    st.session_state.start_time = None

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])


def add_fact(fact: str) -> None:
    st.session_state.facts.append(fact)
    st.info(f"Memory injected: {fact}")


audio_bytes = st.audio_input("Press to talk", key="audio")
if audio_bytes and st.session_state.start_time is None:
    st.session_state.start_time = time.time()


def transcribe(buffer: bytes) -> str:
    audio_io = BytesIO(buffer)
    audio_io.name = "audio.webm"
    transcript = client.audio.transcriptions.create(model="whisper-1", file=audio_io)
    return (transcript.text or "").strip()


def reply(user_text: str) -> str:
    key_hex = deterministic_key(user_text).hex()
    try:
        qp_put(key_hex, user_text)
    except requests.RequestException as exc:
        st.error(f"Failed to store utterance: {exc}")
        return ""

    elapsed = time.time() - st.session_state.start_time
    for point in inject_points:
        if point <= elapsed <= point + 5:
            try:
                fact = qp_get(key_hex)
            except requests.RequestException as exc:
                st.warning(f"Memory fetch failed: {exc}")
                fact = None
            if fact:
                add_fact(fact)
                user_text = f"{user_text}\n[Memory: {fact}]"

    st.session_state.messages.append({"role": "user", "content": user_text})
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=st.session_state.messages,
        temperature=0.2,
        max_tokens=150,
    )
    bot_text = response.choices[0].message.content or ""
    st.session_state.messages.append({"role": "assistant", "content": bot_text})
    return bot_text


if audio_bytes:
    try:
        transcript_text = transcribe(audio_bytes)
    except Exception as exc:  # pragma: no cover - defensive UX
        st.error(f"Transcription failed: {exc}")
        transcript_text = ""
    if transcript_text:
        assistant_reply = reply(transcript_text)
        if assistant_reply:
            with st.chat_message("assistant"):
                st.write(assistant_reply)


if st.session_state.start_time:
    elapsed = time.time() - st.session_state.start_time
    if elapsed > minutes * 60:
        st.warning("Time limit reached – session ended.")
        st.session_state.start_time = None
        with st.expander("Retrieved facts"):
            for fact in st.session_state.facts:
                st.write("- ", fact)


if st.button("Stop talking"):
    st.session_state.start_time = None
    with st.expander("Retrieved facts"):
        for fact in st.session_state.facts:
            st.write("- ", fact)
