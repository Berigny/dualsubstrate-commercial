import streamlit as st
import requests, json

st.set_page_config(page_title="DualSubstrate Live Demo", layout="centered")
st.title("🎤 Live Prime-Ledger Demo (Browser STT)")
st.info("Works in Chrome/Edge.  No audio leaves your machine.")

# embed the JS component
html = """
<script src="/demo/static/stt_min.js"></script>
<button onclick="startSTT()">Start talking</button>
"""
st.components.v1.html(html, height=100)

# recall button
if st.button("Recall last sentence"):
    resp = requests.get("http://localhost:8000/demo/retrieve?entity=demo_user").json()
    st.write(resp)

# live metrics
metrics = requests.get("http://localhost:8000/demo/metrics").json()
st.metric("Tokens saved", metrics["tokens_deduped"])
st.metric("Ledger integrity %", f"{metrics['ledger_integrity']*100:.1f} %")
