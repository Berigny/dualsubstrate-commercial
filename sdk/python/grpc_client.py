import grpc
from api.gen.dualsubstrate.v1 import ledger_pb2 as pb
from api.gen.dualsubstrate.v1 import ledger_pb2_grpc as rpc
from api.gen.dualsubstrate.v1 import health_pb2 as ds_health_pb
from api.gen.dualsubstrate.v1 import health_pb2_grpc as ds_health_rpc
from grpc_health.v1 import health_pb2 as grpc_health_pb2, health_pb2_grpc as grpc_health_rpc


class LedgerClient:
    def __init__(self, target: str = "localhost:50051"):
        self.channel = grpc.insecure_channel(target)
        self.stub = rpc.DualSubstrateStub(self.channel)
        self.health_stub = ds_health_rpc.HealthStub(self.channel)
        self.grpc_health_stub = grpc_health_rpc.HealthStub(self.channel)

    def health(self) -> str:
        response = self.health_stub.Check(ds_health_pb.HealthRequest())
        return ds_health_pb.HealthResponse.Status.Name(response.status)

    def grpc_health(self, service: str = "dualsubstrate.v1.DualSubstrate") -> str:
        response = self.grpc_health_stub.Check(
            grpc_health_pb2.HealthCheckRequest(service=service)
        )
        return grpc_health_pb2.HealthCheckResponse.ServingStatus.Name(response.status)

    def rotate(self, q, vec=None):
        req = pb.QuaternionRequest(q=q, vec=vec or [])
        return list(self.stub.Rotate(req).vec)

    def append(
        self,
        entity,
        r: bytes,
        p: bytes,
        ts: int | None = None,
        meta: dict | None = None,
        idem_key: str | None = None,
    ):
        e = pb.LedgerEntry(entity=entity, r=r, p=p, ts=ts or 0, meta=meta or {})
        response = self.stub.Append(pb.AppendRequest(entry=e, idem_key=idem_key or ""))
        return response.commit_id

    def scan_prefix(self, prefix: bytes, limit: int = 50, reverse: bool = False):
        resp = self.stub.ScanPrefix(pb.ScanRequest(p_prefix=prefix, limit=limit, reverse=reverse))
        return [(row.entity, row.ts, bytes(row.r), bytes(row.p)) for row in resp.rows]


if __name__ == "__main__":
    client = LedgerClient()
    print("health:", client.health())
