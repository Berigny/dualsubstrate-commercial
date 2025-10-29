"""HTTP exposition of Prometheus metrics for the gRPC server."""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest


async def _handle_metrics(_: web.Request) -> web.Response:
    payload = generate_latest()
    return web.Response(body=payload, content_type=CONTENT_TYPE_LATEST)


async def _handle_health(_: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


def _resolve_host_port(
    host_override: Optional[str],
    port_override: Optional[int],
) -> tuple[str, int]:
    """Resolve listening address using overrides and environment variables."""

    env_host = os.getenv("HTTP_HOST") or os.getenv("METRICS_HOST")
    env_port = os.getenv("HTTP_PORT") or os.getenv("METRICS_PORT")

    host = host_override or env_host or "0.0.0.0"
    if port_override is not None:
        port = port_override
    elif env_port:
        port = int(env_port)
    else:
        port = 8080
    return host, port


async def metrics_server(
    host: Optional[str] = None,
    port: Optional[int] = None,
) -> None:
    """Run an aiohttp server that exposes Prometheus metrics and health."""

    listen_host, listen_port = _resolve_host_port(host, port)

    app = web.Application()
    app.router.add_get("/metrics", _handle_metrics)
    app.router.add_get("/health", _handle_health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, listen_host, listen_port)
    await site.start()
    logging.info(
        "Prometheus metrics server listening on %s:%d", listen_host, listen_port
    )

    stop_event = asyncio.Event()
    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        # Normal shutdown path triggered by the caller.
        raise
    finally:
        await runner.cleanup()
