import argparse
import asyncio
import logging
import os
import sys
from contextlib import suppress
from pathlib import Path
from time import perf_counter
from typing import Iterable, Optional

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection

GEN_PATH = Path(__file__).resolve().parent / "gen"
if str(GEN_PATH) not in sys.path:
    sys.path.insert(0, str(GEN_PATH))
from api.gen.dualsubstrate.v1 import health_pb2 as ds_health_pb
from api.gen.dualsubstrate.v1 import health_pb2_grpc as ds_health_rpc
from api.gen.dualsubstrate.v1 import ledger_pb2 as pb
from api.gen.dualsubstrate.v1 import ledger_pb2_grpc as rpc
from api.metrics import record_err, record_ok
from api.metrics_http import metrics_server

# --- wire-up to your existing core ---
# Expect these functions to exist or be easy to add:
# - core.rotate_quaternion(q: list[float], vec: list[float]|None) -> list[float]
# - core.append_ledger(entity:str, r:bytes, p:bytes, ts:int, meta:dict, idem_key:str|None) -> tuple[int,str]
# - core.scan_p_prefix(prefix:bytes, limit:int, reverse:bool) -> Iterable[tuple[str,int,bytes,bytes]]

from core import rotate as core_rotate   # e.g., your /rotate logic wrapper
from core import ledger as core_ledger   # add thin wrappers if needed


class DualSubstrateHealthService(ds_health_rpc.HealthServicer):
    """Simple health responder mirroring the gRPC health status."""

    def __init__(self) -> None:
        self._status = ds_health_pb.HealthResponse.Status.UNKNOWN

    def set_status(self, status: int) -> None:
        self._status = status