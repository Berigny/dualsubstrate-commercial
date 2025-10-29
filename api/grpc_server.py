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

    async def Check(self, request: ds_health_pb.HealthRequest, context):  # type: ignore[override]
        return ds_health_pb.HealthResponse(status=self._status)


class DualSubstrateService(rpc.DualSubstrateServicer):
    _SERVICE = "dualsubstrate.v1.DualSubstrate"

    async def Rotate(self, request: pb.QuaternionRequest, context):
        start = perf_counter()
        method = "Rotate"
        try:
            q = list(request.q)
            vec = list(request.vec) if request.vec else None
            rotated = core_rotate.rotate(q, vec)
            response = pb.QuaternionResponse(vec=rotated)
        except grpc.RpcError as exc:
            record_err(self._SERVICE, method, exc.code().name)
            raise
        except Exception:
            record_err(self._SERVICE, method, grpc.StatusCode.UNKNOWN.name)
            raise
        else:
            duration = perf_counter() - start
            record_ok(self._SERVICE, method, duration)
            return response

    async def Append(self, request: pb.AppendRequest, context):
        start = perf_counter()
        method = "Append"
        try:
            e = request.entry
            ts, commit_id = core_ledger.append_ledger(
                entity=e.entity,
                r=bytes(e.r),
                p=bytes(e.p),
                ts=int(e.ts) if e.ts else None,
                meta=dict(e.meta),
                idem_key=request.idem_key or None,
            )
            response = pb.AppendResponse(ts=ts, commit_id=commit_id)
        except grpc.RpcError as exc:
            record_err(self._SERVICE, method, exc.code().name)
            raise
        except Exception:
            record_err(self._SERVICE, method, grpc.StatusCode.UNKNOWN.name)
            raise
        else:
            duration = perf_counter() - start
            record_ok(self._SERVICE, method, duration)
            return response

    async def ScanPrefix(self, request: pb.ScanRequest, context):
        start = perf_counter()
        method = "ScanPrefix"
        try:
            rows: Iterable[tuple[str, int, bytes, bytes]] = core_ledger.scan_p_prefix(
                prefix=bytes(request.p_prefix),
                limit=int(request.limit or 100),
                reverse=bool(request.reverse),
            )
            out_rows = [
                pb.LedgerRow(entity=entity, ts=ts, r=r, p=p)
                for entity, ts, r, p in rows
            ]
            response = pb.ScanResponse(rows=out_rows)
        except grpc.RpcError as exc:
            record_err(self._SERVICE, method, exc.code().name)
            raise
        except Exception:
            record_err(self._SERVICE, method, grpc.StatusCode.UNKNOWN.name)
            raise
        else:
            duration = perf_counter() - start
            record_ok(self._SERVICE, method, duration)
            return response


def _resolve_tls_paths(
    tls_dir: Optional[str], tls_cert: Optional[str], tls_key: Optional[str]
) -> tuple[Optional[Path], Optional[Path]]:
    cert_path = Path(tls_cert).expanduser() if tls_cert else None
    key_path = Path(tls_key).expanduser() if tls_key else None
    if tls_dir:
        base = Path(tls_dir).expanduser()
        cert_path = cert_path or base / "tls.crt"
        key_path = key_path or base / "tls.key"
    return cert_path, key_path


def _load_server_credentials(
    cert_path: Optional[Path], key_path: Optional[Path]
) -> Optional[grpc.ServerCredentials]:
    if not cert_path or not key_path:
        return None

    if not cert_path.exists() or not key_path.exists():
        missing = []
        if not cert_path.exists():
            missing.append(str(cert_path))
        if not key_path.exists():
            missing.append(str(key_path))
        logging.error("TLS requested but certificate/key missing: %s", ", ".join(missing))
        return None

    try:
        certificate_chain = cert_path.read_bytes()
        private_key = key_path.read_bytes()
    except OSError as exc:  # pragma: no cover - unexpected I/O failure
        logging.error("Failed reading TLS assets: %s", exc)
        raise

    return grpc.ssl_server_credentials(((private_key, certificate_chain),))


async def serve() -> None:
    parser = argparse.ArgumentParser(description="DualSubstrate gRPC server")
    parser.add_argument("--host", default=os.environ.get("GRPC_HOST", "[::]"))
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("GRPC_PORT", "50051"))
    )
    parser.add_argument("--tls-dir", default=os.environ.get("GRPC_TLS_DIR"))
    parser.add_argument("--tls-cert", default=os.environ.get("GRPC_TLS_CERT"))
    parser.add_argument("--tls-key", default=os.environ.get("GRPC_TLS_KEY"))
    args = parser.parse_args()

    server = grpc.aio.server(
        options=[
            ("grpc.max_send_message_length", 64 * 1024 * 1024),
            ("grpc.max_receive_message_length", 64 * 1024 * 1024),
        ]
    )

    rpc.add_DualSubstrateServicer_to_server(DualSubstrateService(), server)

    health_servicer = health.aio.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

    dualsubstrate_health = DualSubstrateHealthService()
    ds_health_rpc.add_HealthServicer_to_server(dualsubstrate_health, server)

    service_names = [
        pb.DESCRIPTOR.services_by_name["DualSubstrate"].full_name,
        ds_health_pb.DESCRIPTOR.services_by_name["Health"].full_name,
    ]
    reflection.enable_server_reflection(
        service_names + [reflection.SERVICE_NAME], server
    )

    for service_name in service_names:
        await health_servicer.set(service_name, health_pb2.HealthCheckResponse.SERVING)
    await health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    await health_servicer.set(
        reflection.SERVICE_NAME, health_pb2.HealthCheckResponse.SERVING
    )
    dualsubstrate_health.set_status(ds_health_pb.HealthResponse.Status.SERVING)

    cert_path, key_path = _resolve_tls_paths(args.tls_dir, args.tls_cert, args.tls_key)
    credentials = _load_server_credentials(cert_path, key_path)
    address = f"{args.host}:{args.port}"

    if credentials:
        server.add_secure_port(address, credentials)
        logging.info("gRPC listening with TLS on %s", address)
    else:
        if any([args.tls_dir, args.tls_cert, args.tls_key]):
            raise RuntimeError(
                f"TLS configuration requested but not available (cert={cert_path}, key={key_path})"
            )
        server.add_insecure_port(address)
        logging.info("gRPC listening without TLS on %s", address)

    http_host = os.getenv("HTTP_HOST") or os.getenv("METRICS_HOST")
    http_port_raw = os.getenv("HTTP_PORT") or os.getenv("METRICS_PORT")
    http_port = int(http_port_raw) if http_port_raw else None

    metrics_task = asyncio.create_task(metrics_server(http_host, http_port))

    await server.start()

    try:
        await server.wait_for_termination()
    finally:
        dualsubstrate_health.set_status(ds_health_pb.HealthResponse.Status.NOT_SERVING)
        await health_servicer.enter_graceful_shutdown()
        metrics_task.cancel()
        with suppress(asyncio.CancelledError):
            await metrics_task


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(serve())
