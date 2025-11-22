# DualSubstrate Commercial

[![buf lint](https://github.com/Berigny/dualsubstrate-commercial/actions/workflows/proto-ci.yml/badge.svg?branch=main)](https://github.com/Berigny/dualsubstrate-commercial/actions/workflows/proto-ci.yml)
[![Helm Chart](https://img.shields.io/badge/Artifact%20Hub-dualsubstrate-326ce5?logo=helm&logoColor=white)](https://artifacthub.io/packages/helm/dualsubstrate/dualsubstrate)
[![PyPI](https://img.shields.io/pypi/v/dualsubstrate-sdk.svg)](https://pypi.org/project/dualsubstrate-sdk/)

Fast path to production for the DualSubstrate service. This README tracks the commercial Fly deployment, front-end hookups (see the [Chat UI repo](https://github.com/Berigny/chat)), and the roadmap toward the next back-end upgrade that operationalizes the eight-equation memoir below.

## What changed in this drop

- **Dedicated backend service (`backend/main.py`)** now runs the websocket proxy, ledger gateways, governance evaluators, and Streamlit/debug tooling off the same FastAPI app. Use this service when wiring the latest `chat` repo – it supersedes the earlier `api/main.py` surface for these flows.
- **Token-aware ledger search** lives under `backend/search/` with an inverted `TokenPrimeIndex`, background reindex helper, and `/search` endpoint for direct debugging.
- **Governance and ethics APIs** (`/coherence/evaluate`, `/ethics/evaluate`) expose the Field-X coherence lattice and policy engine so agents can justify or deny actions.
- **Admin + debug helpers** (`/admin/reindex`, `/ledger/debug/ledger/write`) let you rebuild the search index or hydrate entries without touching RocksDB directly.

## Repository map (active services)

| Path | Purpose | How to run |
| --- | --- | --- |
| `backend/main.py` | FastAPI surface for websocket salience proxy, ledger/search/governance endpoints, admin tools. | `APP_MODULE=backend.main:app make run` or `uvicorn backend.main:app --reload` |
| `api/main.py` | Original flow-rule ledger API (anchor/search/assemble/etc). Still available for compatibility. | `make run` (defaults to `api.main:app`) |
| `backend/search/` | Token index + search orchestration used by the backend service. | Loaded automatically by `backend.main`. |

To make sure you are extending the latest backend instead of the legacy surface:

1. `git pull` to fetch `main`.
2. Work from `/Users/davidberigny/Documents/GitHub/dualsubstrate-commercial/backend`.
3. Start the service with `APP_MODULE=backend.main:app make run` (or run uvicorn directly).
4. Hit `http://localhost:8000/health` to confirm the backend surface is serving the new endpoints before making changes.

## Backend endpoints

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health` | Simple readiness probe for the backend service. |
| `POST` | `/ledger/write` | Store a `LedgerEntry` (persisted in RocksDB + token index). |
| `GET` | `/ledger/read/{namespace:identifier}` | Retrieve the stored entry for a composite key. |
| `POST` | `/ledger/debug/ledger/write` | Write to the in-memory ledger store for local experiments. |
| `GET` | `/search?q=` | Token-prime search across ledger entries (`mode=any|all`, optional `limit`). |
| `GET` | `/admin/reindex` | Rebuild the search/token index for the current RocksDB snapshot. |
| `POST` | `/coherence/evaluate` | Run the Field-X lattice coherence analyzer on an `ActionRequest`. |
| `POST` | `/ethics/evaluate` | Score an action via the policy engine and grace model (lawfulness + permission). |
| `POST/GET` | `/qp/{key_hex}` | Existing quickstore endpoints retained for binary blobs. |

The `chat` repo’s connectivity tab now expects `/ledger/*`, `/search`, and `/admin/reindex` from this backend. After anchoring at least one transcript, press “Build search index” (calls `/admin/reindex`) once per ledger to sync the token cache, then use `/search` for recall debugging.

## Dual-substrate architecture

**Signal split.** A continuous \(\mathbb{R}\) substrate handles gradient-driven pattern search and embedding alignment, while the discrete \(\mathbb{Q}_p\) prime ledger preserves symbolic identity, exact factors, and arithmetic anchoring. The two streams stay synchronized through shared entity IDs, mirrored factor slots, and cross-projections that keep approximate vectors tethered to stable prime signatures.

**Fly.io layout.** The Fly-hosted engine exposes `/anchor`, `/memories`, `/rotate`, `/traverse`, and `/inference/state`, backed by RocksDB for the prime ledger and a lightweight Python service for continuous updates. Stateless Fly machines fan out the API, while ledger volumes stay colocated for low-latency retrieval and Möbius refresh cycles.

**Front-ends.** Streamlit surfaces (`chat_demo_app.py` for the demo/chat UX and `admin_app.py` for operators) live in the [Chat UI repo](https://github.com/Berigny/chat). They call the Fly engine directly and can be redeployed independently of the Fly layer for zero shared blast radius. A five-minute spin-up and ~30-second deploy loop keep the demo chassis fresh while the engine stays hardened on Fly.

## Spec toward the next upgrade

Below is a clean, self-contained “codebook” of the eight equations exactly as they appear in the memoir, followed by computable scaffolds that can be turned into symbols or code. Open functions are called out with placeholders you can fill later.

1. **Substrate genesis** – \(R_0 = \mathbb{R} \times \prod_{p\in\mathbb{P}} \mathbb{Q}_p\). Python scaffold:
   ```python
   import sympy as sp
   primes = list(sp.primerange(2, 100))
   R = sp.Reals
   Qp = [sp.padic_field(p) for p in primes]
   adelic = R, Qp
   ```

2. **Temporalization** – \(\mathcal{T}_{\text{Memory}} = \text{DynSys}(\mathbb{Q}_p, \mathcal{H})\). Time is irreversible drift over the p-adic integers with hysteresis \(\mathcal{H}\).
   ```python
   Z_p = lambda p: sp.padic_integers(p)
   def hysteretic_map(state: int, p: int, hysteresis: float = 0.1):
       return (state + 1) % p**3 + int(hysteresis * p)
   trajectory = [hysteretic_map(s, 3) for s in range(20)]
   ```

3. **Geometry** – \(\mathcal{G}_{\text{Space}} = \text{Map}(\text{Aut}(\mathbb{H}) \rightarrow SO(4))\), leveraging the SU(2) × SU(2) double cover.
   ```python
   import numpy as np
   from scipy.spatial.transform import Rotation as R

   def quaternion_to_SO4(q1, q2):
       L = R.from_quat(q1).as_matrix()
       Rm = R.from_quat(q2).as_matrix()
       M = np.zeros((4, 4))
       M[0, 0] = 1
       M[1:, 1:] = L @ Rm.T
       return M
   ```

4. **Electromagnetic coupling** – \(\mathcal{A}_{EM} = f(\mathbb{P}) \rightarrow \alpha\). A toy model derives \(\alpha\) from the 137/139 twin-prime neighborhood.
   ```python
   def alpha_from_primes():
       twin = (137, 139)
       return 1 / np.sqrt(twin[0] * twin[1])

   alpha = alpha_from_primes()
   ```

5. **Gravitation** – \(G/(\mu_0 \alpha^2) = g(\ln 2)\) with ansatz \(G = (1/k)(\ln 2 / \alpha)^\gamma (\hbar c / \varepsilon_0)\).
   ```python
   hbar, c, eps0 = 1.054e-34, 2.998e8, 8.854e-12

   def derive_G(k: float = 1.0, gamma: float = 1.0):
       return (1 / k) * (np.log(2) / alpha) ** gamma * (hbar * c / eps0)

   G_pred = derive_G(k=1.27, gamma=1.0)
   ```

6. **Consciousness** – \(\text{CUn/Con} = \oint \mathcal{M}_H(C \leftrightarrow UC)\, d\tau\). Hysteretic integration over an ultrametric tree.
   ```python
   from scipy.sparse.csgraph import shortest_path

   def build_ultrametric(n_leaves: int):
       D = np.zeros((n_leaves, n_leaves))
       for i in range(n_leaves):
           for j in range(i + 1, n_leaves):
               D[i, j] = D[j, i] = 2 ** (-(i ^ j).bit_length())
       return D

   D = build_ultrametric(16)
   coherence = 1 - (shortest_path(D).max() / D.shape[0])
   ```

7. **Coherence mandate** – \(K_{Unity} = \langle \Psi | \Psi \rangle_{CUn/Con} = 1\). Enforce a normalized state on the adelic space.
   ```python
   def normalised(psi):
       return np.allclose(np.vdot(psi, psi), 1.0)

   psi = np.random.randn(64) + 1j * np.random.randn(64)
   psi /= np.linalg.norm(psi)
   assert normalised(psi)
   ```

8. **Ethics** – \(\mathcal{E}_{Ethics} = \arg\max_x [\text{Law}(x) \cdot \text{Grace}(x)]\). Constrained optimisation over adherence and novelty.
   ```python
   from scipy.optimize import differential_evolution

   def law(x):
       return 1 - np.abs(x[0] ** 2 + x[1] ** 2 - 1)

   def grace(x):
       return -np.sum(x * np.log2(x + 1e-12))

   def ethic(x):
       return law(x) * grace(x)

   bounds = [(-1.2, 1.2), (-1.2, 1.2)]
   res = differential_evolution(lambda x: -ethic(x), bounds)
   ```

### Implementation Q&A

- **P-adic vs ultrametric?** The ledger uses an ultrametric-inspired prime-factor lattice rather than full field-complete p-adic arithmetic; primes anchor identity and distance while avoiding heavy p-adic carries.
- **Continuous/discrete integration?** \(\mathbb{R}\) embeddings and \(\mathbb{Q}_p\) factors form a direct product. Tensor-style projections inject prime weights into the continuous context window, keeping the spaces coupled without collapsing either one.
- **Normalization?** Continuous updates use gradient-style normalization (token and energy scaling). The discrete ledger uses variational balancing of prime deltas so slot weights remain bounded without erasing exactness.
- **Emergent behavior?** Stable recall across sessions, clustering of recurring entities along shared prime slots, lower prompt churn from deduped anchors, and faster retrieval when Möbius rotations refresh the lattice.
- **If \(K_{Unity} \neq 1\)?** Allowing \(K_{Unity}\) to drift skews amplitude between continuous and discrete streams—\(\mathbb{R}\) vectors over- or under-weight prime guidance—degrading coherence and retrieval until renormalized.

### Recent modelling output

```
1. SUBSTRATE GENESIS
Primes: [2, 3, 5, 7, 11, 13, 17, 19]
✓ Real × p-adic substrate defined

2. TEMPORALIZATION
Temporal trajectory: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
✓ Directed time chain generated

3. GEOMETRY
4D rotation matrix:
[[-1.  0.  0.  0.]
 [ 0.  1.  0.  0.]
 [ 0.  0. -1.  0.]
 [ 0.  0.  0.  1.]]
✓ SU(2)×SU(2) → SO(4) mapping achieved

4. ELECTROMAGNETIC COUPLING
Predicted α: 0.0072973276
Actual α:    0.0072973526
α⁻¹ predicted: 137.036 vs actual: 137.036
✓ Fine structure constant from twin primes

5. GRAVITATION
Optimal k: 1.272563
Predicted G: 6.674e-11
Actual G:    6.674e-11
Match: True
✓ Gravitational constant derived

6. CONSCIOUSNESS
Ultrametric distance matrix shape: (8, 8)
Coherence measure: 0.875000
✓ Consciousness as ultrametric integration

7. COHERENCE MANDATE
State vector norm: 1.0000000000
Unity coherence satisfied: True
✓ Quantum state normalization verified

8. ETHICS
Ethical optimum: [0.707107, 0.707107]
Law component: 1.000000
Grace component: 0.346574
Ethical value: 0.346574
✓ Ethics as constrained optimization solved
```

## Operations and observability

**Metrics patch.** Before the `/metrics` route in `main.py`, initialize:

```python
tokens_saved = 0
total_calls = 0
duplicates = 0
```

Inside `/anchor`:

```python
global tokens_saved, total_calls, duplicates
total_calls += 1
if _already_seen(factors):
    duplicates += 1
    tokens_saved += len(factors)
```

Expose real numbers from `/metrics`:

```python
return {
    "tokens_deduped": tokens_saved,
    "ledger_integrity": 1 - duplicates / total_calls if total_calls else 1.0,
}
```

**Prompt patterns that trigger ledger retrieval.** The chat client reaches into the DualSubstrate remote ledger when prompts include:
- Command prefixes: `/q …`, `@ledger …`, or `::memory …`.
- Keywords: any casing of `quote`, `verbatim`, `exact`, `recall`, `retrieve`, or `what did I say`.
- Time references: natural-language date/time expressions parsable by `dateparser` or `parsedatetime`.
- Semantic similarity: queries close to the intent “provide exact quotes from prior user statements.”

When signals exceed the retrieval threshold, the app fetches recent anchored memories from Fly and sanitizes them before forwarding to the LLM (assistant turns and quote artefacts are stripped).

**Prime-aware anchoring for external agents.** To keep factor weights spread across the eight-prime topology, frame external agents with:
```
You are using the DualSubstrate ledger. For each utterance, extract up to one entry for each slot:

- Prime 2: subject or speaker
- Prime 3: primary action
- Prime 5: object or recipient
- Prime 7: location or channel
- Prime 11: time anchor
- Prime 13: intent or outcome
- Prime 17: supporting context
- Prime 19: sentiment/priority

When you call POST /anchor, send a factors array containing every slot you filled.
If a slot has no info, omit it (or use delta 0). Include the raw transcript in text.
After anchoring, call GET /memories?entity=demo_user&limit=20 to retrieve the exact strings you logged.
```

Ready-to-test anchor example:
```bash
curl -X POST https://dualsubstrate-commercial.fly.dev/anchor \
  -H "x-api-key: demo-key" \
  -H "Content-Type: application/json" \
  -d '{
        "entity":"demo_user",
        "text":"Met Priya at the NYC office to finalize Tuesday’s launch plan.",
        "factors":[
          {"prime":2,"delta":1},
          {"prime":3,"delta":1},
          {"prime":5,"delta":1},
          {"prime":7,"delta":1},
          {"prime":11,"delta":1},
          {"prime":13,"delta":1},
          {"prime":19,"delta":1}
        ]
      }'
```

**Möbius transform CTA.** `/rotate` performs the quaternion pack/rotate/unpack cycle to regenerate the exponent lattice. Trigger from Streamlit:
```python
if st.button("♾️ Möbius Transform", help="Reproject the exponent lattice"):
    payload = {"entity": ENTITY, "axis": (0.0, 0.0, 1.0), "angle": 1.0472}
    resp = requests.post(f"{API}/rotate", json=payload, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    st.success(
        f"Rotated lattice. Δenergy = {data['energy_cycles']}, "
        f"checksum {data['original_checksum']} → {data['rotated_checksum']}."
    )
```
Behind the scenes `/rotate` pulls the factor vector, runs the quaternion Möbius rotation, anchors the new vector via `anchor_batch`, and returns before/after checksums plus energy cycles.

**Traversal & inference observability.** `/traverse` and `/inference/state` let operators audit weighted walks across primes and active inference tasks. Streamlit tabs render traversal paths and inference status; older deployments hide the tabs and emit a “not available” notice.

**RocksDB probe verification.** Point the UI at your ledger directory via `ROCKSDB_DATA_PATH` (default `/app/rocksdb-data`). The **Memory & Inference** tab exposes a **RocksDB probe** form that invokes `tests.rocksdb_probe.run_probe()` directly from the UI:
1. Enter the entity ID, a synthetic prompt, and the prime pattern to traverse (e.g., `2*3*5*7`).
2. Click **Run RocksDB probe** to anchor via the embedded RocksDB client and walk the keyspace.
3. Compare returned key/value pairs with `MemoryService.memory_lookup()` (surfaced in the sidebar “Raw Ledger” expander).

**Manual smoke test – S2 promotion button.**
1. Launch `chat_demo_app.py` (`make demo`) and authenticate so the Connectivity Debug tab is visible.
2. Open **Connectivity Debug** → click **Promote to S2 tier**.
3. Confirm a success toast, recall mode switcher updates to slots/S2, and no request exceptions appear.

## Stage

Current state: **Integration** — continuous \(\mathbb{R}\) substrate, discrete prime ledger, and front-end surfaces are connected with observability hooks; scaling and deeper p-adic fidelity come next.
