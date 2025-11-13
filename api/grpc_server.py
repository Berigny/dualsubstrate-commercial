import argparse
import asyncio
import logging
import os
import sys
import types
from contextlib import suppress
from pathlib import Path
from time import perf_counter
from typing import Any, Iterable, Optional, cast

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection
from google.protobuf import descriptor_pb2

GEN_PATH = Path(__file__).resolve().parent / "gen"
if str(GEN_PATH) not in sys.path:
    sys.path.insert(0, str(GEN_PATH))


def _inject_openapiv2_placeholder() -> None:
    """Inject a minimal placeholder module for protoc-gen-openapiv2 annotations."""

    pkg_root = "protoc_gen_openapiv2"
    module_name = f"{pkg_root}.options.annotations_pb2"

    if module_name in sys.modules:
        return

    file_proto = descriptor_pb2.FileDescriptorProto()
    file_proto.name = "protoc-gen-openapiv2/options/annotations.proto"
    file_proto.package = "protoc_gen_openapiv2.options"

    module = types.ModuleType(module_name)
    module.DESCRIPTOR = file_proto  # type: ignore[attr-defined]

    root_mod = sys.modules.get(pkg_root)
    if root_mod is None:
        root_mod = types.ModuleType(pkg_root)
        sys.modules[pkg_root] = root_mod
    options_mod = sys.modules.get(f"{pkg_root}.options")
    if options_mod is None:
        options_mod = types.ModuleType(f"{pkg_root}.options")
        sys.modules[f"{pkg_root}.options"] = options_mod

    sys.modules[module_name] = module
    setattr(root_mod, "options", options_mod)
    setattr(options_mod, "annotations_pb2", module)


_inject_openapiv2_placeholder()

from api.gen.dualsubstrate.v1 import health_pb2 as ds_health_pb
from api.gen.dualsubstrate.v1 import health_pb2_grpc as ds_health_rpc
from api.gen.dualsubstrate.v1 import ledger_pb2 as pb
from api.gen.dualsubstrate.v1 import ledger_pb2_grpc as rpc
from api.metrics import record_err, record_ok
from api.metrics_http import metrics_server

ds_health_pb = cast(Any, ds_health_pb)
ds_health_rpc = cast(Any, ds_health_rpc)
pb = cast(Any, pb)
rpc = cast(Any, rpc)

# --- wire-up to your existing core ---
# Expect these functions to exist or be easy to add:
# - core.rotate_quaternion(q: list[float], vec: list[float]|None) -> list[float]
# - core.append_ledger(entity:str, r:bytes, p:bytes, ts:int, meta:dict, idem_key:str|None) -> tuple[int,str]
# - core.scan_p_prefix(prefix:bytes, limit:int, reverse:bool) -> Iterable[tuple[str,int,bytes,bytes]]

from core import rotate as core_rotate  # e.g., your /rotate logic wrapper
from core import ledger as core_ledger  # add thin wrappers if needed

_HealthServiceBase = getattr(ds_health_rpc, "HealthServicer", None)
_add_health_to_server = getattr(ds_health_rpc, "add_HealthServicer_to_server", None)

if _HealthServiceBase is None or _add_health_to_server is None:  # pragma: no cover - defensive
    _HealthServiceBase = getattr(ds_health_rpc, "HealthServiceServicer", None) or getattr(
        ds_health_rpc, "HealthService", None
    )
    _add_health_to_server = getattr(
        ds_health_rpc, "add_HealthServiceServicer_to_server", None
    ) or getattr(ds_health_rpc, "add_HealthService_to_server", None)

if _HealthServiceBase is None or _add_health_to_server is None:  # pragma: no cover - defensive
    raise ImportError("Unsupported grpcio health stub variant")


class DualSubstrateHealthService(_HealthServiceBase):
    """Simple health responder mirroring the gRPC health status."""

    def __init__(self) -> None:
        self._status = ds_health_pb.CheckResponse.Status.STATUS_UNKNOWN_UNSPECIFIED

    def set_status(self, status: int) -> None:
        self._status = status

    async def Check(self, request: ds_health_pb.CheckRequest, context):  # type: ignore[override]
        return ds_health_pb.CheckResponse(status=self._status)


class DualSubstrateService(rpc.DualSubstrateServiceServicer):
    _SERVICE = "dualsubstrate.v1.DualSubstrateService"

    async def Rotate(self, request: pb.RotateRequest, context):
        start = perf_counter()
        method = "Rotate"
        try:
            q = list(request.q)
            vec = list(request.vec) if request.vec else None
            rotated = core_rotate.rotate(q, vec)
            response = pb.RotateResponse(vec=rotated)
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

    async def ScanPrefix(self, request: pb.ScanPrefixRequest, context):
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
            response = pb.ScanPrefixResponse(rows=out_rows)
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

    return grpc.ssl_server_credentials([(private_key, certificate_chain)])


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

    rpc.add_DualSubstrateServiceServicer_to_server(DualSubstrateService(), server)

    health_servicer = health.aio.HealthServicer()  # type: ignore[attr-defined]
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

    dualsubstrate_health = DualSubstrateHealthService()
    _add_health_to_server(dualsubstrate_health, server)  # type: ignore[arg-type]

    # Backwards compatibility: earlier releases exported the health service as
    # ``dualsubstrate.v1.Health`` instead of ``HealthService``.  The generated
    # helpers no longer register that alias, so add a manual generic handler to
    # keep the legacy probe working (used by fly checks and CI pipelines).
    legacy_health_handler = grpc.unary_unary_rpc_method_handler(
        dualsubstrate_health.Check,
        request_deserializer=ds_health_pb.CheckRequest.FromString,
        response_serializer=ds_health_pb.CheckResponse.SerializeToString,
    )
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "dualsubstrate.v1.Health", {"Check": legacy_health_handler}
            ),
        )
    )

    service_names = [
        pb.DESCRIPTOR.services_by_name["DualSubstrateService"].full_name,
        ds_health_pb.DESCRIPTOR.services_by_name["HealthService"].full_name,
        "dualsubstrate.v1.Health",
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
    dualsubstrate_health.set_status(ds_health_pb.CheckResponse.Status.STATUS_SERVING)

    credentials = _load_server_credentials(*_resolve_tls_paths(args.tls_dir, args.tls_cert, args.tls_key))
    if credentials:
        server.add_secure_port(f"{args.host}:{args.port}", credentials)
    else:
        server.add_insecure_port(f"{args.host}:{args.port}")

    await server.start()
    logging.info("gRPC server listening on %s:%d", args.host, args.port)

    metrics_task = asyncio.create_task(metrics_server())
    try:
        await server.wait_for_termination()
    finally:
        metrics_task.cancel()
        with suppress(asyncio.CancelledError):
            await metrics_task


if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        asyncio.run(serve())
