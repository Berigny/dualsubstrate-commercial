"""
Modified Signed-Digit radix-4 (digits ∈ {-2,-1,0,1,2})
carry-free add, deferred normalise.
"""
from typing import List

DIGIT_SET = {-2, -1, 0, 1, 2}

def add_msds(a: List[int], b: List[int]) -> List[int]:
    """digit-wise add; outputs may overflow digit set."""
    la, lb = len(a), len(b)
    out = [0] * max(la, lb)
    for i, da in enumerate(a): out[i] += da
    for i, db in enumerate(b): out[i] += db
    return out

def normalise(msd: List[int]) -> List[int]:
    """in-place bound into DIGIT_SET; single left ripple."""
    carry = 0
    for i in range(len(msd)):
        total = msd[i] + carry
        if total > 2:
            msd[i] = total - 4
            carry = 1
        elif total < -2:
            msd[i] = total + 4
            carry = -1
        else:
            msd[i] = total
            carry = 0
    if carry:
        msd.append(carry)
    # trim trailing zeros
    while len(msd) > 1 and msd[-1] == 0:
        msd.pop()
    return msd

def int_to_msd(n: int) -> List[int]:
    if n == 0:
        return [0]
    neg = n < 0
    n = abs(n)
    out = []
    while n:
        rem = n & 3          # low 2 bits
        n = n >> 2
        if rem > 2:          # choose representation to keep digits small
            rem -= 4
            n += 1
        out.append(rem)
    if neg:
        out = [-d for d in out]
    return normalise(out)

def msd_to_int(digits: List[int]) -> int:
    return sum(d * (4 ** i) for i, d in enumerate(digits))
