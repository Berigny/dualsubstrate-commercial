import os
import asyncio
import logging
from typing import Iterable

import grpc

from api.gen.dualsubstrate.v1 import ledger_pb2 as pb
from api.gen.dualsubstrate.v1 import ledger_pb2_grpc as rpc

# --- wire-up to your existing core ---
# Expect these functions to exist or be easy to add:
# - core.rotate_quaternion(q: list[float], vec: list[float]|None) -> list[float]
# - core.append_ledger(entity:str, r:bytes, p:bytes, ts:int, meta:dict, idem_key:str|None) -> tuple[int,str]
# - core.scan_p_prefix(prefix:bytes, limit:int, reverse:bool) -> Iterable[tuple[str,int,bytes,bytes]]

from core import rotate as core_rotate   # e.g., your /rotate logic wrapper
from core import ledger as core_ledger   # add thin wrappers if needed


class DualSubstrateService(rpc.DualSubstrateServicer):
    async def Health(self, request: pb.HealthRequest, context):
        return pb.HealthResponse(status="SERVING")

    async def Rotate(self, request: pb.QuaternionRequest, context):
        q = list(request.q)
        vec = list(request.vec) if request.vec else None
        rotated = core_rotate.rotate(q, vec)
        return pb.QuaternionResponse(vec=rotated)

    async def Append(self, request: pb.AppendRequest, context):
        e = request.entry
        ts, commit_id = core_ledger.append_ledger(
            entity=e.entity,
            r=bytes(e.r),
            p=bytes(e.p),
            ts=int(e.ts) if e.ts else None,
            meta=dict(e.meta),
            idem_key=request.idem_key or None,
        )
        return pb.AppendResponse(ts=ts, commit_id=commit_id)

    async def ScanPrefix(self, request: pb.ScanRequest, context):
        rows: Iterable[tuple[str, int, bytes, bytes]] = core_ledger.scan_p_prefix(
            prefix=bytes(request.p_prefix),
            limit=int(request.limit or 100),
            reverse=bool(request.reverse),
        )
        out_rows = [
            pb.LedgerRow(entity=entity, ts=ts, r=r, p=p)
            for entity, ts, r, p in rows
        ]
        return pb.ScanResponse(rows=out_rows)


async def serve() -> None:
    server = grpc.aio.server(
        options=[
            ("grpc.max_send_message_length", 64 * 1024 * 1024),
            ("grpc.max_receive_message_length", 64 * 1024 * 1024),
        ]
    )
    rpc.add_DualSubstrateServicer_to_server(DualSubstrateService(), server)
    port = int(os.environ.get("GRPC_PORT", "50051"))
    server.add_insecure_port(f"[::]:{port}")
    logging.info("gRPC listening on :%s", port)
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(serve())
