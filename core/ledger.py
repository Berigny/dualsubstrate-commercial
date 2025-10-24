"""
Append-only event log + RocksDB indices
Events: (entity_id, prime, delta_k, timestamp)
"""
import rocksdb, json, time, os
from typing import Dict, List, Tuple
from checksum import merkle_root
from .flow_rule_bridge import validate_prime_sequence

EVENT_LOG = os.getenv("EVENT_LOG_PATH", "/data/event.log")
FACTORS_DB = "/data/factors"
POSTINGS_DB = "/data/postings"

def _open_db(path, cf_names):
    opts = rocksdb.Options(create_if_missing=True)
    return rocksdb.DB(path, opts, column_families={name: rocksdb.ColumnFamilyOptions()
                                                   for name in cf_names})

class Ledger:
    def __init__(self):
        self.fdb = _open_db(FACTORS_DB, ["default"])
        self.pdb = _open_db(POSTINGS_DB, ["default"])
        self.log = open(EVENT_LOG, "ab", buffering=0)

    def anchor(self, entity: str, factors: List[Tuple[int,int]]):
        ts = int(time.time()*1000)
        batch_f = rocksdb.WriteBatch()
        batch_p = rocksdb.WriteBatch()
        check = validate_prime_sequence([p for p, _ in factors]) if factors else None
        via_flags = check.via_centroid if check else []
        for idx, (p, dk) in enumerate(factors):
            # 1) append event
            via_c = via_flags[idx] if idx < len(via_flags) else False
            evt = json.dumps({"e": entity, "p": p, "d": dk, "ts": ts, "via_c": via_c})
            self.log.write((evt+"\n").encode())
            # 2) update entity→factors
            old = self._get_factor(entity, p)
            new = old + dk
            batch_f.put(f"{entity}:{p}".encode(), str(new).encode())
            # 3) update prime→postings
            batch_p.put(f"{p}:{entity}".encode(), str(new).encode())
        self.fdb.write(batch_f)
        self.pdb.write(batch_p)

    def _get_factor(self, entity: str, p: int) -> int:
        v = self.fdb.get(f"{entity}:{p}".encode())
        return int(v.decode()) if v else 0

    def query(self, primes: List[int]) -> List[Tuple[str,int]]:
        """return (entity, weight) pairs that divide ALL primes"""
        from functools import reduce
        sets = []
        for p in primes:
            it = self.pdb.iteritems()
            it.seek(f"{p}:".encode())
            ents = []
            for k, v in it:
                if not k.decode().startswith(f"{p}:"):
                    break
                ent = k.decode().split(":")[1]
                ents.append((ent, int(v.decode())))
            sets.append(dict(ents))
        # intersect
        common = reduce(lambda a,b: a.keys() & b.keys(), sets)
        out = []
        for e in common:
            w = min(sets[i][e] for i in range(len(primes)))
            out.append((e, w))
        return out

    def checksum(self, entity: str) -> str:
        it = self.fdb.iteritems()
        it.seek(f"{entity}:".encode())
        leaves = []
        for k, v in it:
            if not k.decode().startswith(f"{entity}:"):
                break
            leaves.append(k+v)
        return merkle_root(leaves)
