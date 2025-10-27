(function () {
  'use strict';

  const CFG = window.liveMemoryConfig || {};
  const wsRoute = CFG.wsRoute || '/ws';
  const pcmRoute = CFG.pcmRoute || '/pcm';
  const exactBase = CFG.exactBase || '/exact';
  const vadThreshold = typeof CFG.vadThreshold === 'number' ? CFG.vadThreshold : 12;

  const badgeEl = document.getElementById('badge');
  const listEl = document.getElementById('keys');
  const logEl = document.getElementById('log');
  const vadEl = document.getElementById('vad');
  const exportBtn = document.getElementById('export-json');
  const exportHashEl = document.getElementById('export-hash');
  const canvas = document.getElementById('spectrogram');

  if (!badgeEl || !listEl || !canvas) {
    console.warn('Live memory DOM nodes missing.');
    return;
  }

  const ctx = canvas.getContext('2d');
  const WIDTH = canvas.width;
  const HEIGHT = canvas.height;
  const ENERGY_H = 20;
  const SPEC_HEIGHT = HEIGHT - ENERGY_H;

  const energyHist = new Float32Array(WIDTH).fill(0);
  const storedItems = [];
  const shaObj = typeof jsSHA === 'function' ? new jsSHA('SHA-256', 'TEXT') : null;

  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const host = CFG.wsHost || window.location.host;

  const apiSocket = new WebSocket(`${protocol}//${host}${wsRoute}`);
  const pcmSocket = new WebSocket(`${protocol}//${host}${pcmRoute}`);
  pcmSocket.binaryType = 'arraybuffer';

  let captureStarted = false;
  let mediaRecorder = null;
  let audioStream = null;
  let currentEnergy = 0;
  const pendingWebm = [];
  const pendingPcmFrames = [];

  function updateLog(message) {
    if (!logEl) {
      return;
    }
    const stamp = new Date().toLocaleTimeString();
    const entry = document.createElement('div');
    entry.textContent = `[${stamp}] ${message}`;
    logEl.prepend(entry);
  }

  function hslToRgb(h, s, l) {
    h = ((h % 360) + 360) % 360;
    s = Math.max(0, Math.min(1, s));
    l = Math.max(0, Math.min(1, l));

    const c = (1 - Math.abs(2 * l - 1)) * s;
    const hp = h / 60;
    const x = c * (1 - Math.abs((hp % 2) - 1));
    let r = 0;
    let g = 0;
    let b = 0;

    if (hp >= 0 && hp < 1) {
      r = c;
      g = x;
    } else if (hp < 2) {
      r = x;
      g = c;
    } else if (hp < 3) {
      g = c;
      b = x;
    } else if (hp < 4) {
      g = x;
      b = c;
    } else if (hp < 5) {
      r = x;
      b = c;
    } else {
      r = c;
      b = x;
    }
    const m = l - c / 2;
    return {
      r: Math.round((r + m) * 255),
      g: Math.round((g + m) * 255),
      b: Math.round((b + m) * 255),
    };
  }

  function fftRadix2(int16) {
    const N = 256;
    const result = new Float32Array(N);
    if (!int16 || !int16.length) {
      return result.subarray(0, N / 2);
    }

    const step = Math.max(1, Math.floor(int16.length / N));
    const real = new Float32Array(N);
    const imag = new Float32Array(N);

    for (let i = 0; i < N; i += 1) {
      const idx = Math.min(int16.length - 1, i * step);
      const sample = int16[idx] / 32768;
      const window = 0.54 - 0.46 * Math.cos((2 * Math.PI * i) / (N - 1));
      real[i] = sample * window;
    }

    let j = 0;
    for (let i = 0; i < N; i += 1) {
      if (i < j) {
        const tr = real[i];
        const ti = imag[i];
        real[i] = real[j];
        imag[i] = imag[j];
        real[j] = tr;
        imag[j] = ti;
      }
      let m = N >> 1;
      while (j >= m && m >= 2) {
        j -= m;
        m >>= 1;
      }
      j += m;
    }

    for (let size = 2; size <= N; size <<= 1) {
      const half = size >> 1;
      const tableStep = (2 * Math.PI) / size;
      for (let start = 0; start < N; start += size) {
        for (let k = 0; k < half; k += 1) {
          const angle = tableStep * k;
          const wr = Math.cos(angle);
          const wi = -Math.sin(angle);
          const i1 = start + k;
          const i2 = i1 + half;
          const tr = wr * real[i2] - wi * imag[i2];
          const ti = wr * imag[i2] + wi * real[i2];
          real[i2] = real[i1] - tr;
          imag[i2] = imag[i1] - ti;
          real[i1] += tr;
          imag[i1] += ti;
        }
      }
    }

    const magnitudes = new Float32Array(N / 2);
    for (let i = 0; i < N / 2; i += 1) {
      magnitudes[i] = Math.sqrt(real[i] * real[i] + imag[i] * imag[i]);
    }
    return magnitudes;
  }

  function drawEnergy() {
    const barY = HEIGHT - ENERGY_H;
    ctx.fillStyle = '#000';
    ctx.fillRect(0, barY, WIDTH, ENERGY_H);
    ctx.fillStyle = '#0f0';
    for (let x = 0; x < WIDTH; x += 1) {
      const h = Math.min(ENERGY_H - 2, Math.floor(energyHist[x] * (ENERGY_H - 2)));
      if (h <= 0) {
        continue;
      }
      ctx.fillRect(WIDTH - 1 - x, barY + ENERGY_H - h, 1, h);
    }
  }

  function drawSpectrogram(int16) {
    const magnitudes = fftRadix2(int16);
    if (SPEC_HEIGHT <= 1) {
      return;
    }
    ctx.drawImage(canvas, 0, 0, WIDTH, SPEC_HEIGHT - 1, 0, 0, WIDTH, SPEC_HEIGHT - 1);
    for (let x = 0; x < WIDTH; x += 1) {
      const bin = Math.min(magnitudes.length - 1, Math.floor((x * 64) / WIDTH));
      const mag = magnitudes[bin];
      const norm = Math.min(1, Math.log10(1 + mag) / 2.5);
      const color = hslToRgb(240 - norm * 240, 1, 0.5);
      ctx.fillStyle = `rgb(${color.r}, ${color.g}, ${color.b})`;
      ctx.fillRect(x, SPEC_HEIGHT - 1, 1, 1);
    }
  }

  function updateEnergyDisplay(energy) {
    const scaled = energy * 100;
    currentEnergy = scaled;
    if (vadEl) {
      vadEl.textContent = `Energy: ${scaled.toFixed(1)}`;
      vadEl.style.color = scaled > vadThreshold ? '#12b886' : '#adb5bd';
    }
  }

  function handlePcmFrame(int16) {
    if (!int16 || !int16.length) {
      return;
    }
    let sumSquares = 0;
    for (let i = 0; i < int16.length; i += 1) {
      const value = int16[i] / 32768;
      sumSquares += value * value;
    }
    const energy = Math.sqrt(sumSquares / int16.length);
    energyHist.copyWithin(1, 0, WIDTH - 1);
    energyHist[0] = energy;
    drawSpectrogram(int16);
    drawEnergy();
    updateEnergyDisplay(energy);
  }

  function addBadge(item) {
    storedItems.unshift(item);
    badgeEl.classList.add('active');
    badgeEl.textContent = `Stored 0x${item.key.slice(0, 8)}… (${item.len} chars)`;

    const li = document.createElement('li');
    li.className = 'memory-item';

    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'memory-btn';
    button.textContent = item.text;
    button.addEventListener('click', () => playback(item.key));

    const meta = document.createElement('span');
    meta.className = 'meta';
    meta.textContent = `score ${item.score.toFixed(2)}`;

    li.appendChild(button);
    li.appendChild(meta);
    listEl.prepend(li);

    if (shaObj) {
      shaObj.update(item.text || '');
      const digest = shaObj.getHash('HEX');
      exportHashEl.textContent = `SHA-256: ${digest.slice(0, 16)}…`;
    }
  }

  function playback(keyHex) {
    fetch(`${exactBase}/${keyHex}`)
      .then((resp) => resp.json())
      .then((payload) => {
        const utterance = new SpeechSynthesisUtterance(payload.text || payload.value || '');
        speechSynthesis.speak(utterance);
        updateLog(`Playback 0x${keyHex}`);
      })
      .catch((err) => updateLog(`Playback failed for ${keyHex}: ${err}`));
  }

  window.playback = playback;

  function exportMemories() {
    if (!storedItems.length) {
      updateLog('Nothing to export yet.');
      return;
    }
    const jsonText = JSON.stringify(storedItems, null, 2);
    const blob = new Blob([jsonText], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'qp_memories.json';
    a.click();
    URL.revokeObjectURL(url);
  }

  if (exportBtn) {
    exportBtn.addEventListener('click', exportMemories);
  }

  apiSocket.addEventListener('open', () => {
    updateLog('API socket connected.');
    flushWebmQueue();
  });

  apiSocket.addEventListener('close', () => {
    updateLog('API socket disconnected.');
  });

  apiSocket.addEventListener('message', (event) => {
    try {
      const data = JSON.parse(event.data);
      if (data.stored && data.key) {
        addBadge({
          key: data.key,
          text: data.text || '',
          score: typeof data.score === 'number' ? data.score : 0,
          timestamp: data.timestamp || Date.now() / 1000,
          len: data.text ? data.text.length : 0,
        });
      } else if (!data.stored && data.text) {
        updateLog(`Not stored (score=${(data.score || 0).toFixed(2)}): ${data.text}`);
      }
    } catch (err) {
      updateLog(`Malformed event: ${err}`);
    }
  });

  pcmSocket.addEventListener('open', () => {
    updateLog('PCM socket connected.');
    flushPendingPcm();
    startCapture();
  });

  pcmSocket.addEventListener('close', () => {
    updateLog('PCM socket disconnected.');
  });

  pcmSocket.addEventListener('message', (event) => {
    const buffer = event.data;
    if (!(buffer instanceof ArrayBuffer)) {
      return;
    }
    const pcm = new Int16Array(buffer);
    handlePcmFrame(pcm);
  });

  function flushPendingPcm() {
    if (pcmSocket.readyState !== WebSocket.OPEN) {
      return;
    }
    while (pendingPcmFrames.length) {
      const frame = pendingPcmFrames.shift();
      if (frame) {
        pcmSocket.send(frame.buffer);
      }
    }
  }

  function enqueuePcmFrame(frame) {
    if (pcmSocket.readyState === WebSocket.OPEN) {
      pcmSocket.send(frame.buffer);
    } else {
      pendingPcmFrames.push(frame);
      handlePcmFrame(frame);
    }
  }

  function flushWebmQueue() {
    if (!pendingWebm.length || apiSocket.readyState !== WebSocket.OPEN) {
      return;
    }
    const chunk = new Blob(pendingWebm.splice(0, pendingWebm.length), { type: 'audio/webm' });
    chunk.arrayBuffer().then((buffer) => {
      if (currentEnergy > vadThreshold && apiSocket.readyState === WebSocket.OPEN) {
        apiSocket.send(buffer);
      }
    });
  }

  function startCapture() {
    if (captureStarted) {
      return;
    }
    captureStarted = true;
    navigator.mediaDevices
      .getUserMedia({ audio: true })
      .then((stream) => {
        audioStream = stream;
        badgeEl.textContent = 'Listening…';
        setupMediaRecorder(stream);
        setupPcmPipeline(stream);
        updateLog('Microphone capture started.');
      })
      .catch((err) => {
        badgeEl.textContent = 'Microphone permission denied.';
        badgeEl.classList.remove('active');
        updateLog(`Microphone error: ${err.message}`);
      });
  }

  function setupMediaRecorder(stream) {
    try {
      mediaRecorder = new MediaRecorder(stream, { mimeType: 'audio/webm;codecs=opus' });
    } catch (err) {
      mediaRecorder = new MediaRecorder(stream);
    }

    mediaRecorder.ondataavailable = (event) => {
      if (event.data && event.data.size > 0) {
        pendingWebm.push(event.data);
      }
      flushWebmQueue();
    };

    mediaRecorder.start(250);
  }

  function setupPcmPipeline(stream) {
    const audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    const source = audioCtx.createMediaStreamSource(stream);
    const processor = audioCtx.createScriptProcessor(2048, 1, 1);
    const targetRate = 16000;
    const phaseStep = targetRate;
    let phase = 0;
    let acc = 0;
    let accCount = 0;
    const pcmFloats = [];

    processor.onaudioprocess = (event) => {
      const input = event.inputBuffer.getChannelData(0);
      const sampleRate = event.inputBuffer.sampleRate;
      for (let i = 0; i < input.length; i += 1) {
        acc += input[i];
        accCount += 1;
        phase += phaseStep;
        if (phase >= sampleRate) {
          const value = acc / accCount;
          pcmFloats.push(Math.max(-1, Math.min(1, value)));
          phase -= sampleRate;
          acc = 0;
          accCount = 0;
        }
      }

      const frameSize = targetRate / 4; // 250 ms
      while (pcmFloats.length >= frameSize) {
        const frame = pcmFloats.splice(0, frameSize);
        const buffer = new Int16Array(frameSize);
        for (let j = 0; j < frameSize; j += 1) {
          buffer[j] = Math.max(-32768, Math.min(32767, Math.round(frame[j] * 32767)));
        }
        enqueuePcmFrame(buffer);
      }
    };

    source.connect(processor);
    processor.connect(audioCtx.destination);
  }

  window.addEventListener('beforeunload', () => {
    if (mediaRecorder && mediaRecorder.state !== 'inactive') {
      mediaRecorder.stop();
    }
    if (audioStream) {
      audioStream.getTracks().forEach((track) => track.stop());
    }
  });
})();
