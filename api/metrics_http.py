"""HTTP exposition of Prometheus metrics for the gRPC server."""
from __future__ import annotations

import asyncio
import logging

from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest


async def _handle_metrics(_: web.Request) -> web.Response:
    payload = generate_latest()
    return web.Response(body=payload, content_type=CONTENT_TYPE_LATEST)


async def metrics_server(host: str = "0.0.0.0", port: int = 9090) -> None:
    """Run an aiohttp server that exposes Prometheus metrics."""
    app = web.Application()
    app.router.add_get("/metrics", _handle_metrics)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    logging.info("Prometheus metrics server listening on %s:%d", host, port)

    stop_event = asyncio.Event()
    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        # Normal shutdown path triggered by the caller.
        raise
    finally:
        await runner.cleanup()
