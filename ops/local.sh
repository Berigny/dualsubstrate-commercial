#!/usr/bin/env bash
set -euo pipefail

DB_PATH=${DB_PATH:-./data/ledger}

echo "[local] opening RocksDB at ${DB_PATH}"

python - "$DB_PATH" <<'PYCODE'
import sys
from pathlib import Path

from core.storage.rocksdb import open_db

db_path = Path(sys.argv[1])
db = open_db(db_path)

# Seed a few handy prefixes for local smoke tests
db[b"case:1"] = b"\x01"
db[b"R:seed"] = b"\x9f\x03\xaa"
db[b"meta:ready"] = b"ok"

print("[local] rows:")
for key in (b"case:1", b"R:seed", b"meta:ready"):
    print(f"  {key!r} -> {db[key]!r}")

db.close()
print("[local] ready/ok")
PYCODE
