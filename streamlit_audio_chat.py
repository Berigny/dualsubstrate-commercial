import streamlit as st
import openai
import requests, time, datetime, base64, json, os
from io import BytesIO

# ---------- config ----------
FASTAPI_ROOT = "http://localhost:8000" # existing dualsubstrate-commercial
QP_CF = "Qp"
# ----------------------------

st.set_page_config(page_title="Dual-Substrate Audio Memory", layout="centered")
st.title("🎙️ Dual-Substrate Audio Memory Demo")

# ---------- helpers ----------
def qp_put(key: bytes, value: str):
    url = f"{FASTAPI_ROOT}/qp/{key.hex()}"
    payload = {"value": value}
    r = requests.post(url, json=payload)
    r.raise_for_status()

def qp_get(key: bytes) -> str:
    url = f"{FASTAPI_ROOT}/qp/{key.hex()}"
    r = requests.get(url)
    if r.status_code == 404:
        return None
    return r.json()["value"]

def deterministic_key(text: str) -> bytes:
    import hashlib, re
    norm = re.sub(r'\W+', '', text.lower().strip())
    return hashlib.blake2b(norm.encode(), digest_size=16).digest()

# ---------- sidebar ----------
with st.sidebar:
    api_key = st.text_input("OpenAI API key", type="password")
    if not api_key:
        st.stop()
    openai.api_key = api_key
    minutes = st.selectbox("Conversation length", [1,2,3,5,8,13,21,34,55])
    quarter = minutes * 15 # seconds
    inject_points = [quarter, 2*quarter, 3*quarter] # 1st 5 s of each quarter
    st.write("Facts will be injected at:", [f"{t//60}:{t%60:02}" for t in inject_points])

# ---------- state ----------
if "messages" not in st.session_state:
    st.session_state.messages = []
if "facts" not in st.session_state:
    st.session_state.facts = []
if "start_time" not in st.session_state:
    st.session_state.start_time = None

# ---------- UI ----------
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])

def add_fact(fact):
    st.session_state.facts.append(fact)
    st.info(f"Memory injected: {fact}")

# ---------- audio recorder ----------
audio_bytes = st.audio_input("Press to talk", key="audio")
if audio_bytes and st.session_state.start_time is None:
    st.session_state.start_time = time.time()

def transcribe(audio_bytes) -> str:
    audio_io = BytesIO(audio_bytes)
    audio_io.name = "audio.webm"
    transcript = openai.Audio.transcribe("whisper-1", audio_io)
    return transcript["text"]

def reply(user_text: str) -> str:
    # 1. store user utterance in Qp (deterministic key)
    k = deterministic_key(user_text)
    qp_put(k, user_text)

    # 2. check if we need to inject a fact
    elapsed = time.time() - st.session_state.start_time
    for t in inject_points:
        if t <= elapsed <= t+5:
            # fetch a *previous* memory (here we just demo with user text)
            fact = qp_get(k)
            if fact:
                add_fact(fact)
                user_text = f"{user_text}\n[Memory: {fact}]"

    # 3. LLM call
    st.session_state.messages.append({"role": "user", "content": user_text})
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=st.session_state.messages,
        temperature=0.2,
        max_tokens=150
    )
    bot_text = response.choices[0].message.content
    st.session_state.messages.append({"role": "assistant", "content": bot_text})
    return bot_text

if audio_bytes:
    text = transcribe(audio_bytes)
    bot = reply(text)
    with st.chat_message("assistant"):
        st.write(bot)

# ---------- auto stop ----------
if st.session_state.start_time:
    if time.time() - st.session_state.start_time > minutes*60:
        st.warning("Time limit reached – session ended.")
        st.session_state.start_time = None
        with st.expander("Retrieved facts"):
            for f in st.session_state.facts:
                st.write("- ", f)

if st.button("Stop talking"):
    st.session_state.start_time = None
    with st.expander("Retrieved facts"):
        for f in st.session_state.facts:
            st.write("- ", f)
