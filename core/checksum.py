import hashlib
from typing import List

def merkle_root(leaves: List[bytes]) -> str:
    if not leaves:
        return hashlib.sha256(b"").hexdigest()
    h = [hashlib.sha256(leaf).digest() for leaf in leaves]
    while len(h) > 1:
        if len(h) % 2:
            h.append(h[-1])
        h = [hashlib.sha256(h[i] + h[i+1]).digest() for i in range(0, len(h), 2)]
    return h[0].hex()
