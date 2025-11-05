import streamlit as st
import requests
import streamlit.components.v1 as components
import os

st.set_page_config(page_title="DualSubstrate Live Demo", layout="centered")
st.title("🎤 Live Prime-Ledger Demo")

# Get the API URL from an environment variable, with a default for local dev
API_URL = os.getenv("API_URL", "http://localhost:8000")

with open("demo_isolated/static/stt_min.js", "r") as f:
    js_code = f.read().replace("FETCH_URL", f"{API_URL}/demo/anchor")


html = f"""
<script>
{js_code}
</script>
<button onclick="startSTT()">Start Talking</button>
"""

stt_response = components.html(html, height=100)

if 'stt_response' not in st.session_state:
    st.session_state['stt_response'] = None

if stt_response:
    st.session_state['stt_response'] = stt_response

if st.session_state['stt_response']:
    response_dict = st.session_state['stt_response']
    st.write(f"Last spoken text: {response_dict.get('text', '')}")
    st.write(f"Tokens stored: {response_dict.get('tokens', '')}")
    st.write(f"Merkle leaf: {response_dict.get('key', '')}")


if st.button("Recall last"):
    try:
        r = requests.get(f"{API_URL}/demo/retrieve?entity=demo_user").json()
        st.write(r)
    except requests.exceptions.RequestException as e:
        st.error(f"Could not connect to the demo server at {API_URL}. Please ensure it is running. Details: {e}")

try:
    m = requests.get(f"{API_URL}/demo/metrics").json()
    col1, col2 = st.columns(2)
    col1.metric("Tokens saved", m["tokens_deduped"])
    col2.metric("Ledger integrity %", f"{m['ledger_integrity']*100:.1f} %")
except requests.exceptions.RequestException as e:
    st.error(f"Could not connect to the demo server at {API_URL}. Please ensure it is running. Details: {e}")
