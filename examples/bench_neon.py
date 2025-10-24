"""ARM NEON vs quaternion packing benchmark stub."""
from __future__ import annotations

import numpy as np
from tqdm import tqdm

from core import core as core_rs
from core.ledger import Ledger, PRIME_ARRAY


def main() -> None:
    ledger = Ledger()
    baseline: list[int] = []
    quat: list[int] = []

    for _ in tqdm(range(10_000), desc="samples"):
        exps = np.random.randint(-8, 9, size=8, dtype=np.int32)

        # baseline float vector path (anchor absolute exponents)
        t0 = core_rs.py_energy_proxy()
        ledger.anchor_batch("42", [(p, int(e)) for p, e in zip(PRIME_ARRAY, exps)])
        t1 = core_rs.py_energy_proxy()
        baseline.append(int(t1 - t0))

        # quaternion path
        t0 = core_rs.py_energy_proxy()
        q1, q2, _ = core_rs.py_pack_quaternion([int(e) for e in exps])
        core_rs.py_rotate_quaternion(q1, q2, (0.0, 0.0, 1.0), 0.1)
        t1 = core_rs.py_energy_proxy()
        quat.append(int(t1 - t0))

    print("Baseline avg cycles", float(np.mean(baseline)))
    print("Quaternion avg cycles", float(np.mean(quat)))
    print(
        "Energy saving ≈",
        (1.0 - float(np.mean(quat)) / float(np.mean(baseline))) * 100.0,
        "%",
    )


if __name__ == "__main__":
    main()
