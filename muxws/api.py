"""`connect`, `accept` and `serve` - the module-level factories (§9.3).

The codec is resolved before any socket is opened (WSM-CDC-016), so a misconfigured codec raises
`CodecNotRegistered` at startup instead of failing later as a decode error.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from muxws.codecs import Codec, get_codec
from muxws.conf import settings
from muxws.errors import CodecMismatch, ConnectionClosed, ProtocolError
from muxws.fragment import MAX_FRAME_BYTES
from muxws.peer import (
    ErrorSerializer,
    IncomingHeadersInterceptor,
    OpenInterceptor,
    Peer,
    ReplyHeadersInterceptor,
    StreamHandler,
)
from muxws.reconnect import ConnectionLoop, Hello, Reconnect
from muxws.subprotocol import mismatch_error, offer, PREFIX
from muxws.transports import SocketAdapter
from muxws.transports.unix import parse_unix_url
from muxws.transports.websockets_ import verify_dialable_url


def resolve_codec(override: Codec | None = None) -> Codec:
    """An explicit `codec=` wins (WSM-CDC-012); otherwise the configured name (WSM-CDC-010/016).

    Never falls back to JSON (WSM-INV-015). Raises `CodecNotRegistered` for an unregistered name.
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
    open_interceptors: Sequence[OpenInterceptor] = (),
    reply_headers_interceptors: Sequence[ReplyHeadersInterceptor] = (),
    incoming_headers_interceptors: Sequence[IncomingHeadersInterceptor] = (),
) -> Peer:
    """Dial `url` and return a serving peer.

    `url` is `ws://`, `wss://`, or `ws+unix:///path/to.sock:/route` for a Unix domain socket. A
    `ws+unix:` URL uses the same handshake (an HTTP upgrade offering `muxws.v1.<codec>`, answered 101
    or 400) over a socket file, and every parameter below has the same meaning as over TCP.

    If the first attempt fails, the underlying error is raised regardless of `reconnect`
    (WSM-RCN-006). Reconnection applies only to connections that were established and then lost.
    No option changes this (WSM-INV-018); a caller that wants the first dial retried writes its own
    loop.

    After the first connection, the reconnect helper handles the backoff schedule, the heartbeat, and
    the hello, which is replayed unchanged on every connection.

    `on_stream`, `on_close` and `on_reconnect` register the same handlers as `peer.on_stream(...)`,
    `peer.on_close(...)` and `peer.on_reconnect(...)`, before the hello is sent. The acceptor may
    push a stream as soon as it receives the hello; without a registered handler the peer answers it
    with `reset(REFUSED, "no on_stream handler")` (WSM-STM-033).

    `open_interceptors`, `reply_headers_interceptors` and `incoming_headers_interceptors` are also
    registered before the hello is sent, so a stream the acceptor pushes after the hello passes the
    incoming interceptors (WSM-API-026).
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
    _register_interceptors(peer, open_interceptors, reply_headers_interceptors, incoming_headers_interceptors)
    loop = ConnectionLoop(
        peer,
        dial,
        options=reconnect or Reconnect(),
        # Captured once, by value, and replayed unchanged on every later connection (WSM-RCN-020).
        hello=Hello(payload=hello, headers=hello_headers, timeout=hello_timeout),
        ping_interval=ping_interval,
        ping_timeout=ping_timeout,
    )
    await loop.establish()
    loop.start()
    # Lets `close()` stop the helper, and keeps the supervisor task referenced.
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

    The reconnect helper receives this closure instead of a URL and has no transport knowledge. Every
    reconnect performs the same dial as the first connection, with the same offered subprotocol list
    (WSM-CDC-016/020).
    """
    offered = offer(resolved.name, subprotocols)
    # Parsed here, not inside `dial()`: a malformed `ws+unix:` URL, a `wss+unix:` URL and a platform
    # without AF_UNIX raise from `connect()` before any socket is opened, and every reconnect dials
    # the same socket file.
    unix = parse_unix_url(url)
    # `parse_unix_url` is stdlib-only, so a malformed `ws+unix:` URL raises `UnixUrlError` even where
    # `websockets` is absent. `verify_dialable_url` then checks the dependency, which supplies the URL
    # parser, and then the URL.
    #
    # Both checks run outside `dial()`. The `except` in `dial()` answers a refused upgrade with
    # `CodecMismatch` (WSM-CDC-022/024), and its string fallback matches `HTTP 400` in an
    # `InvalidURI` that quotes the URL, so `connect("ws:/HTTP 400")` would be reported as a codec
    # mismatch (WSM-ERR-016).
    verify_dialable_url(url, uri=unix.uri if unix is not None else None)

    async def dial() -> SocketAdapter:
        import websockets

        from muxws.transports.websockets_ import verify_negotiated, WebsocketsSocket

        # Both arms share one `try`, so the `except` below translates a 400 into `CodecMismatch` for
        # both (WSM-CDC-022/024).
        try:
            if unix is None:
                connection = await websockets.connect(
                    url,
                    subprotocols=offered,  # type: ignore[arg-type]
                    additional_headers=headers,
                )
            else:
                # Imported only in this branch, so a TCP dial does not import it. `websockets.asyncio`
                # is available at the minimum supported `websockets` version, since the top-level
                # `connect` above resolves to it.
                from websockets.asyncio.client import unix_connect

                # `uri=` is the logical URL: the HTTP handshake needs a request target and a `Host`,
                # which a filesystem path does not provide. The other arguments are the same as in the
                # TCP arm.
                connection = await unix_connect(
                    unix.path,
                    uri=unix.uri,
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


#: The status an acceptor answers for a codec it does not speak (WSM-CDC-022), and the only status
#: treated as a codec refusal. Any other failure is raised as it is.
_REFUSED = 400


def _looks_like_a_refused_handshake(exc: BaseException) -> bool:
    """A 400 on the upgrade is a refused muxws handshake, to be reported as `CodecMismatch`.

    The status is read from `InvalidStatus.response`, not from the message: `"400" in str(exc)` also
    matches the `OSError` for a refused connection on port 400, and message wording differs between
    the two ports (WSM-CDC-024). For any other exception type, the message is matched against the
    exact phrase `HTTP 400`, for `websockets` releases that report the status without the response.
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
    open_interceptors: Sequence[OpenInterceptor] = (),
    reply_headers_interceptors: Sequence[ReplyHeadersInterceptor] = (),
    incoming_headers_interceptors: Sequence[IncomingHeadersInterceptor] = (),
) -> Peer:
    """Accept an inbound connection and return a peer that is not yet serving.

    A Starlette `WebSocket` is upgraded here, because `accept()` selects the subprotocol
    (WSM-CDC-026). A `websockets` connection has its negotiated subprotocol checked. Any other
    `SocketAdapter` is used as it is. The three interceptor sequences are registered on the peer in
    order (WSM-API-026).
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
    _register_interceptors(peer, open_interceptors, reply_headers_interceptors, incoming_headers_interceptors)
    return peer


def _register_interceptors(
    peer: Peer,
    open_interceptors: Sequence[OpenInterceptor],
    reply_headers_interceptors: Sequence[ReplyHeadersInterceptor],
    incoming_headers_interceptors: Sequence[IncomingHeadersInterceptor],
) -> None:
    """Register each sequence on `peer` in its order, as the matching `peer.intercept_*()` calls would."""
    for open_interceptor in open_interceptors:
        peer.intercept_open(open_interceptor)
    for reply_interceptor in reply_headers_interceptors:
        peer.intercept_reply_headers(reply_interceptor)
    for incoming_interceptor in incoming_headers_interceptors:
        peer.intercept_incoming_headers(incoming_interceptor)


async def _adapt(socket: Any, codec_name: str) -> SocketAdapter:
    """Wrap the socket the application passed, upgrading it first when it is not yet accepted.

    The framework checks run before the `SocketAdapter` check. `SocketAdapter` is a
    `runtime_checkable` Protocol, so `isinstance` tests only that the four method names exist, and
    Starlette's `WebSocket` has them; checked first, a Starlette socket would be used without the
    upgrade.

    Raises `ProtocolError` for an object that is none of the three.
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
    """True for a `websockets` connection; its handshake is already complete."""
    return hasattr(socket, "recv") and hasattr(socket, "send") and hasattr(socket, "subprotocol")


async def serve(socket: SocketAdapter | Any, *, handler: StreamHandler, **peer_options: Any) -> None:
    """Accept, register `handler`, and run the read loop until the socket closes.

    `peer_options` are `accept()`'s keyword arguments, the three interceptor sequences included.
    """
    try:
        peer = await accept(socket, **peer_options)
    except CodecMismatch:
        # The upgrade was refused and the 400 has already been sent (WSM-CDC-022). There is no peer;
        # the function returns without raising, and the refusal was logged by `select`.
        return
    peer.on_stream(handler)
    try:
        await peer.serve()
    except ConnectionClosed:
        return


def select_subprotocol(connection: Any, subprotocols: list[str]) -> str:
    """Re-exported handshake hook for the `websockets` library (WSM-CDC-027).

    Raises `NegotiationError` on a mismatch, which `websockets` answers with the 400 that
    WSM-CDC-022 requires.

    """
    from muxws.transports.websockets_ import select_subprotocol as hook

    return hook(connection, subprotocols)


__all__ = ["PREFIX", "accept", "connect", "resolve_codec", "select_subprotocol", "serve"]
