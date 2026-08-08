"""`connect`, `accept` and `serve` - the module-level factories (§9.3).

The codec is resolved **before** any socket is touched. Putting the lookup after the upgrade is the
single most likely wrong implementation of WSM-CDC-016, and it is what makes a misconfigured
deployment fail as a puzzling decode error on the tenth frame rather than as a named startup error.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from muxws.codecs import Codec, get_codec
from muxws.conf import settings
from muxws.errors import CodecMismatch, ConnectionClosed, ProtocolError
from muxws.fragment import MAX_FRAME_BYTES
from muxws.peer import ErrorSerializer, Peer, StreamHandler
from muxws.reconnect import ConnectionLoop, Hello, Reconnect
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
    reconnect: Reconnect | None = None,
    ping_interval: float = 20.0,
    ping_timeout: float = 10.0,
    hello_timeout: float = 10.0,
    max_payload_bytes: int = 67_108_864,
    max_concurrent_streams: int = 100,
    error_serializer: ErrorSerializer | None = None,
    codec: Codec | None = None,
    max_frame_bytes: int = MAX_FRAME_BYTES,
    on_stream: StreamHandler | None = None,
    on_close: Callable[[Any], None] | None = None,
    on_reconnect: Callable[[int, Any], None] | None = None,
) -> Peer:
    """Dial `url` and return a serving peer.

    Raises if the **first** attempt fails, with the underlying error, whatever `reconnect` says
    (WSM-RCN-006). Reconnection applies to connections that were established and then lost; a peer
    that retried its first dial forever would turn a typo in the URL into silence. There is
    deliberately no option that changes this (WSM-INV-018): a caller who wants the first dial retried
    writes that loop itself, where it can decide what a permanent failure looks like.

    Everything after that first dial belongs to the reconnect helper: the backoff schedule, the
    heartbeat, and the hello replayed verbatim on every connection this peer ever makes.

    `on_stream`, `on_close` and `on_reconnect` are the same handlers `peer.on_stream(...)` and
    friends register, and they are registered **before** the hello goes out. That is the whole reason
    they are parameters rather than a line the caller writes afterwards: the acceptor may push a
    stream the instant it sees the hello, and a peer whose handler is registered one await later
    answers that push `reset(REFUSED, "no on_stream handler")` (WSM-STM-033).
    """
    resolved = resolve_codec(codec)
    dial = _websocket_dialer(url, resolved, headers=headers, subprotocols=subprotocols)

    socket = await dial()
    peer = Peer(
        socket,
        codec=resolved,
        is_dialer=True,
        error_serializer=error_serializer,
        max_frame_bytes=max_frame_bytes,
        max_payload_bytes=max_payload_bytes,
        max_concurrent_streams=max_concurrent_streams,
    )
    if on_stream is not None:
        peer.on_stream(on_stream)
    if on_close is not None:
        peer.on_close(on_close)
    if on_reconnect is not None:
        peer.on_reconnect(on_reconnect)
    loop = ConnectionLoop(
        peer,
        dial,
        options=reconnect or Reconnect(),
        # Captured **here**, once, by value: replayed verbatim on every later connection and never
        # re-read from the application's object (WSM-RCN-020).
        hello=Hello(payload=hello, headers=hello_headers, timeout=hello_timeout),
        ping_interval=ping_interval,
        ping_timeout=ping_timeout,
    )
    await loop.establish()
    loop.start()
    # Held so the garbage collector cannot collect the supervisor out from under the peer.
    peer._connection_loop = loop
    return peer


def _websocket_dialer(
    url: str,
    resolved: Codec,
    *,
    headers: dict[str, str] | None,
    subprotocols: list[str] | None,
) -> Callable[[], Awaitable[SocketAdapter]]:
    """One closure that dials, verifies the subprotocol, and hands back a `SocketAdapter`.

    The helper is given this rather than a URL because it knows nothing about transports: every
    reconnect is the same dial as the first one, down to the offered subprotocol list, so there is
    exactly one place where a connection is made and no way for a reconnect to negotiate something
    the first connection did not (WSM-CDC-016/020).
    """
    offered = offer(resolved.name, subprotocols)

    async def dial() -> SocketAdapter:
        import websockets

        from muxws.transports.websockets_ import verify_negotiated, WebsocketsSocket

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

        try:
            verify_negotiated(getattr(connection, "subprotocol", None), resolved.name)
        except Exception:
            from muxws.transports.websockets_ import POLICY_VIOLATION

            await connection.close(code=POLICY_VIOLATION, reason="codec mismatch")
            raise

        return WebsocketsSocket(connection)

    return dial


#: What an acceptor answers a codec it does not speak (WSM-CDC-022), and the only status this reads
#: as a refusal. Anything else is a server that failed for its own reasons and must surface as itself.
_REFUSED = 400


def _looks_like_a_refused_handshake(exc: BaseException) -> bool:
    """A 400 on the upgrade is a refused muxws handshake, to be reported as `CodecMismatch`.

    The status comes from `InvalidStatus.response`, never from the message. `"400" in str(exc)` also
    matches the `OSError` for a connection refused on **port** 400, and matching a status code out of
    prose is what let a cross-language dial - where the message wording differs - miss a real refusal
    entirely (WSM-CDC-024). The string branch survives only as a fallback for a `websockets` release
    that reports the status without attaching the response, and is narrowed to the phrase it uses.
    """
    from websockets.exceptions import InvalidStatus

    if isinstance(exc, InvalidStatus):
        return exc.response.status_code == _REFUSED
    return f"HTTP {_REFUSED}" in str(exc)


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
        max_payload_bytes=max_payload_bytes,
        max_concurrent_streams=max_concurrent_streams,
    )
    return peer


async def _adapt(socket: Any, codec_name: str) -> SocketAdapter:
    """Wrap whatever the application handed us, upgrading it first when that is still to be done.

    The framework checks come **before** the `SocketAdapter` one. `SocketAdapter` is a
    `runtime_checkable` Protocol, and `isinstance` against one of those tests only that the four
    method *names* exist - which Starlette's `WebSocket` happens to satisfy. Checking it first meant
    a Starlette socket was taken as an already-adapted one and the upgrade never happened, which
    surfaces only against a real ASGI server.
    """
    if _is_starlette_websocket(socket):
        from muxws.transports.starlette import perform_upgrade

        return await perform_upgrade(socket, codec_name)

    if _is_websockets_connection(socket):
        from muxws.transports.websockets_ import verify_negotiated, WebsocketsSocket

        verify_negotiated(getattr(socket, "subprotocol", None), codec_name)
        return WebsocketsSocket(socket)

    if isinstance(socket, SocketAdapter):
        return socket

    raise ProtocolError(
        f"{type(socket).__name__} is neither a SocketAdapter nor a socket muxws knows how to "
        f"upgrade; wrap it in an adapter (WSM-API-021)"
    )


def _is_starlette_websocket(socket: Any) -> bool:
    return hasattr(socket, "scope") and hasattr(socket, "client_state")


def _is_websockets_connection(socket: Any) -> bool:
    """A `websockets` connection, which has already handshaken by the time we see it."""
    return hasattr(socket, "recv") and hasattr(socket, "send") and hasattr(socket, "subprotocol")


async def serve(socket: SocketAdapter | Any, *, handler: StreamHandler, **peer_options: Any) -> None:
    """Accept, register `handler`, and run the read loop until the socket closes."""
    try:
        peer = await accept(socket, **peer_options)
    except CodecMismatch:
        # The upgrade was refused and the 400 has already been sent (WSM-CDC-022). There is no peer
        # and nothing further to do; raising here would turn an ordinary misconfiguration into a
        # traceback out of the application's endpoint.
        return
    peer.on_stream(handler)
    try:
        await peer.serve()
    except ConnectionClosed:
        return


def select_subprotocol(connection: Any, subprotocols: list[str]) -> str:
    """Re-exported handshake hook for the `websockets` library (WSM-CDC-027).

    Raises `NegotiationError` rather than returning None on a mismatch; see the hook itself for why
    that is the only shape `websockets` turns into the 400 WSM-CDC-022 requires.
    """
    from muxws.transports.websockets_ import select_subprotocol as hook

    return hook(connection, subprotocols)


__all__ = ["PREFIX", "accept", "connect", "resolve_codec", "select_subprotocol", "serve"]
