(() => {
const ws = new WebSocket("wss://{{WS_HOST}}/ws");
const badgeEl = document.getElementById("badge");
const vadEl   = document.getElementById("vad");
const keysEl  = document.getElementById("keys");
const apiKey  = "{{API_KEY}}";

ws.onopen = () => {
  badgeEl.innerText = "Connected – speak now";
  navigator.mediaDevices.getUserMedia({audio:true})
  .then(stream => {
    const ctx   = new AudioContext({sampleRate:16000});
    const src   = ctx.createMediaStreamSource(stream);
    const proc  = ctx.createScriptProcessor(1024,1,1);
    proc.onaudioprocess = e => {
        const pcm16 = new Int16Array(e.inputBuffer.length);
        const data  = e.inputBuffer.getChannelData(0);
        for(let i=0;i<data.length;i++) pcm16[i]=data[i]*0x7FFF;
        ws.send(pcm16.buffer);
        const energy = Math.sqrt(pcm16.reduce((s,v)=>s+v*v,0)/pcm16.length)/32768;
        vadEl.innerText = `Energy: ${energy.toFixed(2)}`;
    };
    src.connect(proc);
    proc.connect(ctx.destination);
  })
  .catch(() => { badgeEl.innerText = "Microphone permission denied"; });
};

ws.onmessage = ev => {
  const msg = JSON.parse(ev.data);
  if(msg.stored){
    badgeEl.innerText = `Stored 0x${msg.key.slice(0,8)}…`;
    const li = document.createElement("li");
    const button = document.createElement("button");
    button.textContent = msg.text || msg.key;
    button.onclick = () => {
      fetch(`https://{{WS_HOST}}/exact/${msg.key}`, {
        headers: {Authorization: `Bearer ${apiKey}`}
      })
      .then(r => r.json())
      .then(d => alert(d.text || "(no text)"));
    };
    li.appendChild(button);
    keysEl.prepend(li);
  }
};
})();
