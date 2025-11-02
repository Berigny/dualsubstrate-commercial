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
@@ -193,51 +170,51 @@ def _load_server_credentials(

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
  import argparse
import asyncio
import logging
import os
import sys
import types
from contextlib import suppress
from pathlib import Path
from time import perf_counter
from typing import Iterable, Optional

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

# --- wire-up to your existing core ---
# Expect these functions to exist or be easy to add:
# - core.rotate_quaternion(q: list[float], vec: list[float]|None) -> list[float]
# - core.append_ledger(entity:str, r:bytes, p:bytes, ts:int, meta:dict, idem_key:str|None) -> tuple[int,str]
# - core.scan_p_prefix(prefix:bytes, limit:int, reverse:bool) -> Iterable[tuple[str,int,bytes,bytes]]

from core import rotate as core_rotate   # e.g., your /rotate logic wrapper
from core import ledger as core_ledger   # add thin wrappers if needed
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


class DualSubstrateHealthService(ds_health_rpc.HealthServicer):
class DualSubstrateHealthService(_HealthServiceBase):
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
@@ -193,51 +170,51 @@ def _load_server_credentials(

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
    _add_health_to_server(dualsubstrate_health, server)

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
buf.work.yaml
Deleted
