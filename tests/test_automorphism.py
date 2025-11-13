import pytest

from core.automorphism import CycleAutomorphismService
from core.inference import InferenceStore
from core.ledger import PRIME_ARRAY


class _MemoryDB:
    def __init__(self):
        self._store: dict[bytes, bytes] = {}

    def get(self, key: bytes):
        return self._store.get(key)

    def put(self, key: bytes, value: bytes) -> None:
        self._store[key] = value


def test_cycle_automorphism_rotates_even_odd_pairs():
    store = InferenceStore(_MemoryDB(), primes=PRIME_ARRAY)

    def initialise(snapshot):
        for idx in range(len(snapshot.x)):
            snapshot.x[idx] = float(idx + 1)
        for row in snapshot.readouts.values():
            for idx in range(len(row)):
                row[idx] = float(idx + 1)

    store.mutate_state("alice", initialise)

    service = CycleAutomorphismService(
        store, primes=PRIME_ARRAY, centroid_provider=lambda: 0
    )
    primes = [PRIME_ARRAY[0], PRIME_ARRAY[1], PRIME_ARRAY[2], PRIME_ARRAY[3]]
    flags = service.derive_via_flags(primes)
    result = service.enforce("alice", primes, flags, mutate_state=True)

    assert result.initial_centroid == 0
    assert result.flips == 2
    assert result.final_centroid == 0
    assert len(result.steps) == len(primes)

    first_transition = result.steps[1]
    assert first_transition.prime == PRIME_ARRAY[1]
    assert first_transition.via_centroid is True
    assert first_transition.permutation == "swap_pair"
    assert first_transition.rotor == "quarter_turn"
    assert first_transition.cycle_index == 1

    second_transition = result.steps[2]
    assert second_transition.permutation is None
    assert second_transition.via_centroid is True

    final_transition = result.steps[3]
    assert final_transition.permutation == "swap_pair"
    assert final_transition.cycle_index == 2

    snapshot = store.snapshot("alice")
    assert snapshot.x[:4] == pytest.approx([-1.0, 2.0, -3.0, 4.0])
    for row in snapshot.readouts.values():
        assert row[:4] == pytest.approx([-1.0, 2.0, -3.0, 4.0])


def test_cycle_automorphism_skips_state_mutation_when_disabled():
    store = InferenceStore(_MemoryDB(), primes=PRIME_ARRAY)

    def initialise(snapshot):
        for idx in range(len(snapshot.x)):
            snapshot.x[idx] = float(idx + 1)

    store.mutate_state("bob", initialise)

    service = CycleAutomorphismService(
        store, primes=PRIME_ARRAY, centroid_provider=lambda: 1
    )
    primes = [PRIME_ARRAY[0], PRIME_ARRAY[1]]
    flags = service.derive_via_flags(primes)
    before = store.snapshot("bob")
    result = service.enforce("bob", primes, flags, mutate_state=False)
    after = store.snapshot("bob")

    assert before.x == pytest.approx(after.x)
    assert result.initial_centroid == 1
    assert result.final_centroid == 0
    assert result.steps[1].permutation == "swap_pair"
