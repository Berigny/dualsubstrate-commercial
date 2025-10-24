import requests, os

class QpMemory:
    def __init__(self, api_key: str, base_url: str = "http://localhost:8000"):
        self.h = {"Authorization": f"Bearer {api_key}"}
        self.url = base_url.rstrip("/")

    def anchor(self, entity: str, factors: list[tuple[int,int]]):
        payload = {"entity": entity, "factors": [{"prime": p, "delta": d} for p, d in factors]}
        r = requests.post(f"{self.url}/anchor", json=payload, headers=self.h)
        r.raise_for_status()

    def query(self, primes: list[int]) -> list[tuple[str,int]]:
        payload = {"primes": primes}
        r = requests.post(f"{self.url}/query", json=payload, headers=self.h)
        r.raise_for_status()
        return [(h["entity"], h["weight"]) for h in r.json()["results"]]

    def checksum(self, entity: str) -> str:
        r = requests.get(f"{self.url}/checksum", params={"entity": entity}, headers=self.h)
        r.raise_for_status()
        return r.json()["checksum"]
