(() => {
  const CFG = window.liveMemoryConfig || {};
  const badgeEl = document.getElementById("badge");
  const vadEl = document.getElementById("vad");
  const keysList = document.getElementById("keys");

  if (!badgeEl || !keysList) {
    console.warn("Live memory components missing.");
    return;
  }

  const wsRoute = CFG.wsRoute || "/ws";
  const wsHost = CFG.wsHost || window.location.host;
  const wsProtocol =
    CFG.wsProtocol ||
    (window.location.protocol === "https:" ? "wss:" : "ws:");
  const wsUrl = `${wsProtocol}//${wsHost.replace(/\/$/, "")}${wsRoute}`;
  const apiKey = CFG.apiKey || "";
  const exactBase = (CFG.exactBase || "/exact").replace(/\/$/, "");

  const fetchHeaders = apiKey
    ? {
        Authorization: `Bearer ${apiKey}`,
      }
    : undefined;

  const ws = new WebSocket(wsUrl);

  ws.onopen = () => {
    badgeEl.innerText = "Connected – speak now";
    navigator.mediaDevices
      .getUserMedia({ audio: true })
      .then((stream) => {
        const ctx = new AudioContext({ sampleRate: 16000 });
        const src = ctx.createMediaStreamSource(stream);
        const proc = ctx.createScriptProcessor(1024, 1, 1);
        proc.onaudioprocess = (e) => {
          const pcm16 = new Int16Array(e.inputBuffer.length);
          const data = e.inputBuffer.getChannelData(0);
          for (let i = 0; i < data.length; i += 1) {
            pcm16[i] = data[i] * 0x7fff;
          }
          ws.send(pcm16.buffer);
          if (vadEl) {
            const energy =
              Math.sqrt(
                pcm16.reduce((sum, value) => sum + value * value, 0) /
                  pcm16.length,
              ) / 32768;
            vadEl.innerText = `Energy: ${energy.toFixed(2)}`;
          }
        };
        src.connect(proc);
        proc.connect(ctx.destination);
      })
      .catch((err) => {
        console.error("Microphone access denied:", err);
        badgeEl.innerText = "Microphone access denied";
      });
  };

  ws.onmessage = (ev) => {
    const msg = JSON.parse(ev.data);
    if (!msg || !msg.stored) {
      return;
    }

    badgeEl.innerText = `Stored 0x${msg.key.slice(0, 8)}…`;
    const li = document.createElement("li");
    const button = document.createElement("button");
    button.type = "button";
    button.textContent = msg.text;
    button.addEventListener("click", () => {
      const target = `${exactBase}/${msg.key}`;
      fetch(target, {
        headers: fetchHeaders,
      })
        .then((resp) => resp.json())
        .then((payload) => {
          const text = payload && payload.text ? payload.text : JSON.stringify(payload);
          alert(text);
        })
        .catch((err) => {
          console.error("Exact fetch failed:", err);
          alert("Failed to fetch exact memory.");
        });
    });
    li.prepend(button);
    keysList.prepend(li);
  };
})();
