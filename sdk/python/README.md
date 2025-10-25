# DualSubstrate Python SDK

This package provides a lightweight client for the DualSubstrate gRPC API
alongside helper adapters for LangChain and LlamaIndex pipelines.

## Quickstart

```bash
pip install dualsubstrate-sdk
```

```python
from dualsubstrate_sdk import LedgerClient

client = LedgerClient()
print(client.health())
```
