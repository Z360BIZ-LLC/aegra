"""Decoupled liveness probe served from a Python thread.

This module provides a minimal HTTP server bound on a dedicated port
(default 8001) that responds to ``GET /live``. The server runs in a
plain ``threading.Thread`` — completely independent of the asyncio
event loop that powers the FastAPI app on port 8000.

The motivation: ALB / ECS health checks must succeed even when graph
execution briefly blocks the asyncio loop (long synchronous LLM tool
calls, sync DB calls inside nodes, GIL contention spikes, etc.). The
in-loop ``/live`` endpoint in ``core/health.py`` cannot serve a
response if the loop is wedged, even momentarily — so ALB sees a
timeout and marks the target unhealthy.

The sidecar is intentionally tiny: a ``ThreadingHTTPServer`` from
``http.server`` answering a single path. It additionally consults a
``LoopHeartbeat`` updated by an asyncio task, so it can return 503 if
the loop is *genuinely* dead for more than
``SIDECAR_STALE_THRESHOLD_SECONDS`` (default 30). Short blips are
tolerated; sustained loop failure correctly fails the probe.

Configuration via env vars:
    SIDECAR_LIVE_PORT (int, default 8001) — port to bind.
    SIDECAR_STALE_THRESHOLD_SECONDS (float, default 30) — lag threshold
        beyond which /live returns 503.
"""

from __future__ import annotations

import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import structlog

logger = structlog.get_logger("aegra.sidecar")

DEFAULT_PORT: int = 8001
DEFAULT_STALE_THRESHOLD_SECONDS: float = 30.0
DEFAULT_HEARTBEAT_INTERVAL_SECONDS: float = 0.1


def _env_int(name: str, default: int) -> int:
    """Read an int from env, falling back to ``default`` if unset/invalid."""
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("sidecar.env_invalid_int", var=name, value=raw, default=default)
        return default


def _env_float(name: str, default: float) -> float:
    """Read a float from env, falling back to ``default`` if unset/invalid."""
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("sidecar.env_invalid_float", var=name, value=raw, default=default)
        return default


class LoopHeartbeat:
    """Tracks the last time the asyncio event loop ticked.

    Construction primes ``last_tick_monotonic`` to ``time.monotonic()``
    so a fresh heartbeat is not immediately reported as stale.
    """

    __slots__ = ("last_tick_monotonic",)

    def __init__(self) -> None:
        self.last_tick_monotonic: float = time.monotonic()

    def bump(self) -> None:
        """Mark the loop as having just ticked."""
        self.last_tick_monotonic = time.monotonic()

    def lag_seconds(self) -> float:
        """Seconds since the loop last bumped the heartbeat."""
        return time.monotonic() - self.last_tick_monotonic


async def heartbeat_task(hb: LoopHeartbeat, interval: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS) -> None:
    """Continuously bump the heartbeat from inside the asyncio loop.

    Self-healing: any non-cancellation exception is logged and the task
    backs off for a second before resuming. ``CancelledError`` is
    re-raised so normal shutdown still works.
    """
    import asyncio  # local import keeps module import-time light

    while True:
        try:
            hb.bump()
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("sidecar.heartbeat_task_error")
            await asyncio.sleep(1)


def _make_handler_class(hb: LoopHeartbeat, stale_threshold_seconds: float) -> type[BaseHTTPRequestHandler]:
    """Build a ``BaseHTTPRequestHandler`` subclass closed over ``hb``.

    Using a factory avoids globals and lets multiple sidecars (e.g., in
    tests) coexist with independent heartbeats.
    """

    class SidecarHandler(BaseHTTPRequestHandler):
        # Disable HTTP/1.1 keep-alive negotiation noise; not needed for ALB.
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:  # noqa: N802 — BaseHTTPRequestHandler API
            if self.path != "/live":
                self._write_json(404, {"error": "not_found", "path": self.path})
                return

            lag = hb.lag_seconds()
            if lag < stale_threshold_seconds:
                self._write_json(
                    200,
                    {"status": "alive", "loop_lag_ms": int(lag * 1000)},
                )
            else:
                self._write_json(
                    503,
                    {
                        "status": "stale",
                        "loop_lag_ms": int(lag * 1000),
                        "stale_threshold_ms": int(stale_threshold_seconds * 1000),
                    },
                )

        def _write_json(self, status: int, body: dict[str, Any]) -> None:
            payload = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 — stdlib signature
            """Route handler logs through structlog; suppress 2xx success noise.

            The stdlib default writes a line to stderr for every request,
            which floods CloudWatch when ALB polls /live every few seconds.
            We only emit when the response wasn't successful.
            """
            # args[1] is the status code as a string for BaseHTTPRequestHandler;
            # guard defensively in case the call shape differs.
            status_str = args[1] if len(args) >= 2 else ""
            if status_str.startswith("2"):
                return
            logger.info(
                "sidecar.request",
                client=self.address_string(),
                message=format % args,
            )

        def log_request(self, code: int | str = "-", size: int | str = "-") -> None:
            """Suppress per-request success logs; let log_message handle non-2xx."""
            try:
                code_int = int(code)
            except (TypeError, ValueError):
                code_int = 0
            if 200 <= code_int < 300:
                return
            super().log_request(code, size)

        def log_error(self, format: str, *args: Any) -> None:  # noqa: A002 — stdlib signature
            logger.warning(
                "sidecar.error",
                client=self.address_string(),
                message=format % args,
            )

    return SidecarHandler


def start_sidecar(
    hb: LoopHeartbeat,
    port: int | None = None,
    host: str = "0.0.0.0",  # nosec B104 — bind-all is intentional for container probes
    stale_threshold_seconds: float | None = None,
) -> threading.Thread:
    """Bind the sidecar HTTP server and run it on a daemon thread.

    Returns the spawned thread. The bind happens synchronously before
    this function returns, so callers know the port is live (or get an
    immediate ``OSError`` if the port is taken).

    The thread is a daemon — process exit kills it cleanly without
    needing explicit shutdown.
    """
    resolved_port = port if port is not None else _env_int("SIDECAR_LIVE_PORT", DEFAULT_PORT)
    resolved_threshold = (
        stale_threshold_seconds
        if stale_threshold_seconds is not None
        else _env_float("SIDECAR_STALE_THRESHOLD_SECONDS", DEFAULT_STALE_THRESHOLD_SECONDS)
    )

    handler_cls = _make_handler_class(hb, resolved_threshold)
    server = ThreadingHTTPServer((host, resolved_port), handler_cls)

    thread = threading.Thread(
        target=server.serve_forever,
        name="aegra-sidecar-live",
        daemon=True,
    )
    thread.start()
    logger.info(
        "sidecar.started",
        host=host,
        port=resolved_port,
        stale_threshold_seconds=resolved_threshold,
    )
    return thread
