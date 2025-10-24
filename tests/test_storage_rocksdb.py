import json
import types

import pytest

from core.storage import rocksdb_available, to_big_endian_timestamp
from core.storage.rocksdb import (
    LedgerKeys,
    _encode_ethics,
    _merge_ethics,
    compose_index_key,
    compose_key,
)


def test_big_endian_timestamp_ordering():
    assert to_big_endian_timestamp(1) < to_big_endian_timestamp(2)
    with pytest.raises(ValueError):
        to_big_endian_timestamp(-1)


def test_compose_keys():
    ts = to_big_endian_timestamp(42)
    key = compose_key(b"r", "alice", ts)
    assert key.startswith(b"r/alice/")
    assert key.endswith(ts)


def test_compose_index_key():
    ts = to_big_endian_timestamp(42)
    prefix = compose_index_key(b"ix", b"prefix", b"abc", "alice", ts)
    assert prefix.startswith(b"ix/prefix/abc/alice/")
    assert prefix.endswith(ts)


def test_ledger_keys_helpers():
    keys = LedgerKeys(entity="alice", timestamp=42)
    ts = to_big_endian_timestamp(42)
    assert keys.r().endswith(ts)
    assert keys.qp().startswith(b"p/alice")
    assert keys.bridge().startswith(b"b/alice")
    assert keys.index_prefix(b"xyz").startswith(b"ix/prefix/xyz/alice/")
    assert keys.index_hash(b"hash").startswith(b"ix/hash/hash/alice/")
    assert keys.ethics() == b"e/alice"


def test_ethics_merge_operator_is_associative():
    existing = _encode_ethics({"credits": 10, "debits": 3, "last_ts": 5})
    operands = types.SimpleNamespace(__iter__=lambda self: iter(self.values))
    merge_operands = operands()
    merge_operands.values = [
        _encode_ethics({"credits": 2, "debits": 1, "last_ts": 8}),
        _encode_ethics({"credits": 5, "debits": 0, "last_ts": 4}),
    ]
    merged = _merge_ethics(existing, merge_operands)
    payload = json.loads(merged)
    assert payload["credits"] == 17
    assert payload["debits"] == 4
    assert payload["last_ts"] == 8


@pytest.mark.skipif(not rocksdb_available(), reason="rocksdict is not installed")
def test_open_database(tmp_path):
    from core.storage import open_rocksdb

    storage = open_rocksdb(tmp_path / "ledger.db")
    assert storage.db is not None
