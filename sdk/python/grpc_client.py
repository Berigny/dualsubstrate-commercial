import grpc
from api.gen.dualsubstrate.v1 import ledger_pb2 as pb
from api.gen.dualsubstrate.v1 import ledger_pb2_grpc as rpc


class LedgerClient:
    def __init__(self, target: str = "localhost:50051"):
        self.channel = grpc.insecure_channel(target)
        self.stub = rpc.DualSubstrateStub(self.channel)

    def health(self) -> str:
        return self.stub.Health(pb.HealthRequest()).status

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
