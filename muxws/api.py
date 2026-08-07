"""`connect`, `accept` and `serve` - the module-level factories (§9.3).

The codec is resolved **before** any socket is touched. Putting the lookup after the upgrade is the
single most likely wrong implementation of WSM-CDC-016, and it is what makes a misconfigured
deployment fail as a puzzling decode error on the tenth frame rather than as a named startup error.
"""

from __future__ import annotations

import asyncio

from typing import Any

from muxws.codecs import Codec, get_codec
from muxws.conf import settings
from muxws.errors import ConnectionClosed
from muxws.fragment import MAX_FRAME_BYTES
from muxws.peer import ErrorSerializer, Peer, StreamHandler
from muxws.subprotocol import mismatch_error, offer, PREFIX
from muxws.transports import SocketAdapter


def resolve_codec(override: Codec | None = None) -> Codec:
    """An explicit `codec=` wins (WSM-CDC-012); otherwise the configured name (WSM-CDC-010/016).

    Never falls back to JSON. A deployment that believes it is running msgpack, is not, and finds
    out from neither end is the failure WSM-INV-015 names.
    """
    if override is not None:
        return override
    return get_codec(settings.codec)


async def connect(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    subprotocols: list[str] | None = None,
    hello: Any = None,
    hello_headers: dict[str, Any] | None = None,
    reconnect: Any = None,
    ping_interval: float = 20.0,
    ping_timeout: float = 10.0,
    hello_timeout: float = 10.0,
    max_payload_bytes: int = 67_108_864,
    max_concurrent_streams: int = 100,
    error_serializer: ErrorSerializer | None = None,
    codec: Codec | None = None,
    max_frame_bytes: int = MAX_FRAME_BYTES,
) -> Peer:
    """Dial `url` and return a serving peer.

    Raises if the **first** attempt fails, with the underlying error, whatever `reconnect` says
    (WSM-RCN-006). Reconnection applies to connections that were established and then lost; a peer
    that retried its first dial forever would turn a typo in the URL into silence.

    `hello`, `reconnect`, `ping_interval`, `ping_timeout` and `hello_timeout` are accepted and stored
    here but acted on in M5b. Accepting them now keeps the signature stable.
    """
    import websockets

    from muxws.transports.websockets_ import verify_negotiated, WebsocketsSocket

    resolved = resolve_codec(codec)
    offered = offer(resolved.name, subprotocols)

    try:
        connection = await websockets.connect(
            url,
            subprotocols=offered,  # type: ignore[arg-type]
            additional_headers=headers,
        )
    except Exception as exc:
        if _looks_like_a_refused_handshake(exc):
            raise mismatch_error(resolved.name) from exc
        raise

    negotiated = getattr(connection, "subprotocol", None)
    try:
        verify_negotiated(negotiated, resolved.name)
    except Exception:
        from muxws.transports.websockets_ import POLICY_VIOLATION

        await connection.close(code=POLICY_VIOLATION, reason="codec mismatch")
        raise

    peer = Peer(
        WebsocketsSocket(connection),
        codec=resolved,
        is_dialer=True,
        error_serializer=error_serializer,
        max_frame_bytes=max_frame_bytes,
    )
    peer._pending_options = {  # type: ignore[attr-defined]
        "hello": hello,
        "hello_headers": hello_headers,
        "reconnect": reconnect,
        "ping_interval": ping_interval,
        "ping_timeout": ping_timeout,
        "hello_timeout": hello_timeout,
        "max_payload_bytes": max_payload_bytes,
        "max_concurrent_streams": max_concurrent_streams,
        "url": url,
    }
    peer._serve_task = asyncio.create_task(peer.serve())  # type: ignore[attr-defined]
    return peer


def _looks_like_a_refused_handshake(exc: BaseException) -> bool:
    """A 400 on the upgrade is what an acceptor answers a codec it does not speak (WSM-CDC-022)."""
    status = getattr(getattr(exc, "response", None), "status_code", None)
    return status == 400 or "400" in str(exc)


async def accept(
    socket: SocketAdapter | Any,
    *,
    max_payload_bytes: int = 67_108_864,
    max_concurrent_streams: int = 100,
    error_serializer: ErrorSerializer | None = None,
    codec: Codec | None = None,
    max_frame_bytes: int = MAX_FRAME_BYTES,
) -> Peer:
    """Accept an inbound connection and return a peer that is not yet serving.

    A Starlette `WebSocket` is upgraded here, because `accept()` is the only party that knows which
    subprotocol to select (WSM-CDC-026). Anything already implementing `SocketAdapter` is taken as
    it is.
    """
    resolved = resolve_codec(codec)
    adapter = await _adapt(socket, resolved.name)
    peer = Peer(
        adapter,
        codec=resolved,
        is_dialer=False,
        error_serializer=error_serializer,
        max_frame_bytes=max_frame_bytes,
    )
    peer._pending_options = {  # type: ignore[attr-defined]
        "max_payload_bytes": max_payload_bytes,
        "max_concurrent_streams": max_concurrent_streams,
    }
    return peer


async def _adapt(socket: Any, codec_name: str) -> SocketAdapter:
    if isinstance(socket, SocketAdapter):
        return socket
    if _is_starlette_websocket(socket):
        from muxws.transports.starlette import perform_upgrade

        return await perform_upgrade(socket, codec_name)
    from muxws.transports.websockets_ import verify_negotiated, WebsocketsSocket

    verify_negotiated(getattr(socket, "subprotocol", None), codec_name)
    return WebsocketsSocket(socket)


def _is_starlette_websocket(socket: Any) -> bool:
    return hasattr(socket, "scope") and hasattr(socket, "client_state")


async def serve(socket: SocketAdapter | Any, *, handler: StreamHandler, **peer_options: Any) -> None:
    """Accept, register `handler`, and run the read loop until the socket closes."""
    peer = await accept(socket, **peer_options)
    peer.on_stream(handler)
    try:
        await peer.serve()
    except ConnectionClosed:
        return


def select_subprotocol(connection: Any, subprotocols: list[str]) -> str | None:
    """Re-exported handshake hook for the `websockets` library (WSM-CDC-027)."""
    from muxws.transports.websockets_ import select_subprotocol as hook

    return hook(connection, subprotocols)


__all__ = ["PREFIX", "accept", "connect", "resolve_codec", "select_subprotocol", "serve"]
