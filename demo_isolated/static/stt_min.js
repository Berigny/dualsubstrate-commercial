function startSTT() {
  const r = new (window.SpeechRecognition || webkitSpeechRecognition)();
  r.interimResults = false;
  r.lang = 'en-US';
  r.onresult = e => {
    const text = e.results[0][0].transcript;
    fetch("/demo/anchor", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({text})
    }).then(res => res.json()).then(data => {
      window.parent.postMessage({type: 'streamlit:setComponentValue', value: {text, ...data}}, "*");
    });
  };
  r.start();
}
