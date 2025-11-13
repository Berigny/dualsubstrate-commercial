import math

import pytest

from core.inference import InferenceStore
from core.ledger import PRIME_ARRAY


class _MemoryDB:
    def __init__(self):
        self._store: dict[bytes, bytes] = {}

    def get(self, key: bytes):
        return self._store.get(key)

    def put(self, key: bytes, value: bytes) -> None:
        self._store[key] = value


def test_inference_update_normalises_state():
    store = InferenceStore(_MemoryDB(), primes=PRIME_ARRAY, learning_rate=0.1)
    state = store.update("demo", [(PRIME_ARRAY[0], 3.0), (PRIME_ARRAY[1], -1.5)])

    norm = math.sqrt(sum(component * component for component in state.x))
    assert norm == pytest.approx(1.0)

    first_row = state.readouts[PRIME_ARRAY[0]]
    row_norm = math.sqrt(sum(component * component for component in first_row))
    assert row_norm == pytest.approx(1.0)

    snapshot = store.snapshot("demo")
    assert snapshot.x == pytest.approx(state.x)
    assert snapshot.readouts[PRIME_ARRAY[1]] == pytest.approx(state.readouts[PRIME_ARRAY[1]])
