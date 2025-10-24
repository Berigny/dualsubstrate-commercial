# DualSubstrate MVP – 5 Minute Quick Start

## Prerequisites
- Python 3.11+
- Optional: Docker & Docker Compose for container based runs

## 1. Create a Virtual Environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install fastapi "uvicorn[standard]" pydantic pytest
```

## 2. Launch the API
```bash
uvicorn api.main:app --reload
```
The API expects the header `x-api-key: mvp-secret` on protected endpoints.

## 3. Smoke Test
```bash
curl -s http://127.0.0.1:8000/health | jq
curl -s -X POST http://127.0.0.1:8000/events \
  -H "Content-Type: application/json" \
  -H "x-api-key: mvp-secret" \
  -d '{"payload": "hello"}' | jq
```

## 4. Run the Placeholders
```bash
pytest
```
Tests are currently marked as skipped—they document the intended API surface.

## 5. Docker Workflow
```bash
cd ops
docker compose up --build
```
This starts the API on port 8000 and a Grafana stub on port 3000.

## Repository Layout
- `core/` – number theory, ledger glue, checksum primitives
- `api/` – FastAPI service with basic dependencies and models
- `sdk/python/` – lightweight client wrapper
- `ops/` – Docker, docker-compose, Grafana stub dashboard
- `tests/` – pytest skeletons for core logic and API layer

## Storage Layer (RocksDB)
To experiment with the dual-substrate ledger storage module, install the optional
`rocksdict` dependency and run the unit suite:

```bash
pip install rocksdict
pytest -q
```

The storage module will create a RocksDB instance with column families for
`meta`, `R`, `Qp`, `bridge`, `index`, and `ethics`, and sets up a merge operator
for the ethics counters.

## Pitch
> “We gave AI memory a **gyroscope**: 8 primes → 2 quaternions → **rotation-invariant**, **4× smaller**, **35 % less energy** on ARM.  
> Try it: `curl -X POST api.dualsubstrate.ai/rotate`”
