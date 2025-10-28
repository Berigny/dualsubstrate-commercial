(() => {
  'use strict';

  const config = window.liveMemoryConfig || {};
  const wsRoute = config.wsRoute || '/ws';
  const pcmRoute = config.pcmRoute || null;
  const exactBase = config.exactBase || '/exact';
  const vadThreshold = typeof config.vadThreshold === 'number' ? config.vadThreshold : 12;

  const host = config.wsHost
    ? config.wsHost
    : config.wsPort
    ? `${window.location.hostname}:${config.wsPort}`
    : window.location.host;
  const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';

  const apiSocket = new WebSocket(`${protocol}://${host}${wsRoute}`);
  const pcmSocket = pcmRoute ? new WebSocket(`${protocol}://${host}${pcmRoute}`) : null;

  const badgeEl = document.getElementById('badge');
  const vadEl = document.getElementById('vad');
  const spectrogram = document.getElementById('spectrogram');
  const listEl = document.getElementById('keys');
  const logEl = document.getElementById('log');
  const exportBtn = document.getElementById('export-json');
  const exportHashEl = document.getElementById('export-hash');
  const storedItems = [];

  if (!badgeEl || !vadEl || !spectrogram || !listEl) {
    console.warn('Live memory DOM nodes missing.');
    return;
  }

  if (config.baseline) {
    badgeEl.textContent = 'Baseline mode – substrate disabled';
    badgeEl.classList.remove('active');
  }

  if (pcmSocket) {
    pcmSocket.addEventListener('open', () => updateLog('PCM WebSocket connected.'));
    pcmSocket.addEventListener('close', () => updateLog('PCM WebSocket disconnected.'));
  }

  apiSocket.addEventListener('open', () => updateLog('WebSocket connected.'));
  apiSocket.addEventListener('close', () => updateLog('WebSocket disconnected.'));
  apiSocket.addEventListener('message', (event) => {
    try {
      const data = JSON.parse(event.data);
      if (data.stored && data.key) {
        addBadge({
          key: data.key,
          text: data.text,
          score: data.score || 0,
          timestamp: data.timestamp || Date.now() / 1000,
        });
      } else if (!data.stored && data.text) {
        updateLog(`Not stored (score=${(data.score || 0).toFixed(2)}): ${data.text}`);
      }
    } catch (err) {
      updateLog(`Malformed event: ${err}`);
    }
  });

  if (exportBtn) {
    exportBtn.addEventListener('click', () => {
      if (!storedItems.length) {
        updateLog('Nothing to export yet.');
        return;
      }
      exportBadges();
    });
  }

  let currentEnergy = 0;

  navigator.mediaDevices
    .getUserMedia({ audio: true })
    .then((stream) => {
      badgeEl.textContent = 'Microphone active';
      badgeEl.classList.add('active');

      const mediaRecorder = new MediaRecorder(stream, { mimeType: 'audio/webm' });

      const audioContext = new (window.AudioContext || window.webkitAudioContext)();
      const analyser = audioContext.createAnalyser();
      analyser.fftSize = 1024;
      const source = audioContext.createMediaStreamSource(stream);
      source.connect(analyser);

      const frequencyData = new Uint8Array(analyser.frequencyBinCount);
      const timeData = new Uint8Array(analyser.fftSize);
      const ctx = spectrogram.getContext('2d');
      const width = spectrogram.width;
      const height = spectrogram.height;

      function drawSpectrogram() {
        requestAnimationFrame(drawSpectrogram);
        analyser.getByteFrequencyData(frequencyData);
        const imageData = ctx.getImageData(1, 0, width - 1, height);
        ctx.putImageData(imageData, 0, 0);
        for (let y = 0; y < height; y += 1) {
          const idx = Math.floor((frequencyData.length * y) / height);
          const value = frequencyData[idx];
          const hue = 240 - (value / 255) * 240;
          ctx.fillStyle = `hsl(${hue}, 90%, ${(value / 255) * 60 + 20}%)`;
          ctx.fillRect(width - 1, height - 1 - y, 1, 1);
        }

        analyser.getByteTimeDomainData(timeData);
        let sum = 0;
        for (let i = 0; i < timeData.length; i += 1) {
          const sample = (timeData[i] - 128) / 128;
          sum += sample * sample;
        }
        currentEnergy = Math.sqrt(sum / timeData.length) * 100;
        vadEl.textContent = `Energy: ${currentEnergy.toFixed(1)}`;
        vadEl.style.color = currentEnergy > vadThreshold ? '#12b886' : '#adb5bd';
      }

      drawSpectrogram();

      const pending = [];
      mediaRecorder.ondataavailable = (event) => {
        if (event.data.size > 0) {
          pending.push(event.data);
        }
        if (pending.length >= 4) {
          const blob = new Blob(pending.splice(0, pending.length), { type: 'audio/webm' });
          if (currentEnergy > vadThreshold) {
            blob.arrayBuffer().then((buffer) => {
              if (apiSocket.readyState === WebSocket.OPEN) {
                apiSocket.send(buffer);
              }
              if (pcmSocket && pcmSocket.readyState === WebSocket.OPEN) {
                try {
                  pcmSocket.send(buffer);
                } catch (err) {
                  updateLog(`PCM send error: ${err}`);
                }
              }
            });
          }
        }
      };

      mediaRecorder.start(250);
      updateLog('Microphone capture started.');
    })
    .catch((err) => {
      updateLog(`Microphone error: ${err.message}`);
      badgeEl.textContent = 'Microphone permission denied.';
      badgeEl.classList.remove('active');
    });

  function updateLog(message) {
    if (!logEl) return;
    const stamp = new Date().toLocaleTimeString();
    const entry = document.createElement('div');
    entry.textContent = `[${stamp}] ${message}`;
    logEl.prepend(entry);
  }

  function addBadge(item) {
    storedItems.push(item);
    badgeEl.textContent = `Stored: 0x${item.key}`;
    badgeEl.classList.add('active');

    const li = document.createElement('li');
    li.className = 'memory-item';

    const anchor = document.createElement('button');
    anchor.type = 'button';
    anchor.className = 'memory-btn';
    anchor.textContent = `${item.text} (0x${item.key.slice(0, 8)}…)`;
    anchor.addEventListener('click', () => playback(item.key));

    const meta = document.createElement('span');
    meta.className = 'meta';
    meta.textContent = `score=${item.score.toFixed(2)} t=${new Date(item.timestamp * 1000).toLocaleTimeString()}`;

    li.appendChild(anchor);
    li.appendChild(meta);
    listEl.prepend(li);
  }

  function playback(keyHex) {
    fetch(`${exactBase}/${keyHex}`)
      .then((resp) => resp.json())
      .then((payload) => {
        const utterance = new SpeechSynthesisUtterance(payload.text || payload.value || '');
        speechSynthesis.speak(utterance);
        updateLog(`Playback 0x${keyHex}`);
      })
      .catch((err) => {
        updateLog(`Playback failed for ${keyHex}: ${err}`);
      });
  }

  async function exportBadges() {
    const jsonText = JSON.stringify(storedItems, null, 2);
    const encoded = new TextEncoder().encode(jsonText);
    const hashBuffer = await crypto.subtle.digest('SHA-256', encoded);
    const hashHex = Array.from(new Uint8Array(hashBuffer))
      .map((b) => b.toString(16).padStart(2, '0'))
      .join('');
    exportHashEl.textContent = `SHA-256: ${hashHex}`;

    const blob = new Blob([jsonText], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = 'qp_badges.json';
    link.click();
    URL.revokeObjectURL(url);
  }
})();
