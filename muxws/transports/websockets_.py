"""The `websockets` library: a dialer, and the acceptor's handshake hook.

The module name has a trailing underscore because a `websockets.py` inside this package would shadow
the library it imports. `websockets` is imported inside the functions, so the package has no required
runtime dependencies (WSM-PKG-002).
"""

from __future__ import annotations

import logging

from typing import Any

from muxws.errors import ConnectionClosed, TransportUnsupportedError, TransportUrlError
from muxws.subprotocol import find_offer, PREFIX, select

logger = logging.getLogger("muxws.codec")

#: The WebSocket close code for a policy violation, used when a transport offers no handshake hook
#: at all and the mismatch can only be discovered on an already-open socket (WSM-CDC-028).
POLICY_VIOLATION = 1008

#: The install command for this transport's dependency, quoted in `WebsocketsNotInstalledError`
#: (WSM-ERR-016).
INSTALL_HINT = "pip install muxws[websockets]"


class WebsocketUrlError(TransportUrlError):
    """A `ws:`/`wss:` URL the `websockets` library cannot turn into something to dial.

    No hostname (`ws:/nohost`), a scheme that is neither `ws` nor `wss`, a port that is not an
    integer, or a string that is not a URL. A `TransportUrlError`, and so a `MuxwsError` and a
    `ValueError`, so `except MuxwsError` around `connect()` catches an unusable URL of any scheme
    (WSM-ERR-016).

    The message quotes the `websockets` diagnostic, which names the offending part of the URL, and
    the original exception is chained with `from exc`.
    """


class WebsocketsNotInstalledError(TransportUnsupportedError):
    """`connect()` was called and `import websockets` failed; the message names the extra.

    Raised for both `ws://` and `ws+unix://` dials, which import the same package.

    The condition is local and permanent; retrying or changing the URL does not help. The base is
    therefore `TransportUnsupportedError` (a `RuntimeError`), not `TransportUrlError`.

    Raised only when the package is absent. A `websockets` that is present but fails to import keeps
    its own `ImportError`; see `require_websockets`, and `ts/node.ts::WsNotInstalledError`.
    """


def require_websockets() -> Any:
    """Import `websockets`, or raise `WebsocketsNotInstalledError` naming the extra.

    Used by both dial arms. The package has no required runtime dependencies (WSM-PKG-002), so the
    import happens here and its absence is reported as a muxws error naming the install command
    (WSM-ERR-016).

    Called from `verify_dialable_url` before the URL is parsed, because `websockets` supplies the URL
    parser.

    Only a `ModuleNotFoundError` whose `name` is exactly `"websockets"` becomes
    `WebsocketsNotInstalledError`. A missing submodule (`name` of `"websockets.asyncio"`) or another
    `ImportError` means the package is present and failed to import, and is re-raised unchanged.
    `ts/node.ts::requireWs` applies the same rule to `ERR_MODULE_NOT_FOUND`.
    """
    try:
        import websockets
    except ImportError as exc:
        if not isinstance(exc, ModuleNotFoundError) or exc.name != "websockets":
            raise
        raise WebsocketsNotInstalledError(
            f"muxws needs the `websockets` package to dial a ws:, wss: or ws+unix: URL, and importing "
            f"it failed ({exc}); install it with `{INSTALL_HINT}`"
        ) from exc
    return websockets


def verify_dialable_url(url: str, *, uri: str | None = None) -> None:
    """Refuse a URL `websockets` cannot dial, before anything opens a socket.

    `uri` is the logical `ws://` URL that a `ws+unix:` dial passes to `unix_connect(uri=...)`; that
    URL is parsed by the handshake, so it is the one checked. For a TCP dial `url` is checked.

    Called outside `connect()`'s dial closure. The closure's exception handler translates a refused
    upgrade into `CodecMismatch` (WSM-CDC-022/024), and its string fallback matches `HTTP 400`
    anywhere in the message, including an `InvalidURI` that quotes the URL: `connect("ws:/HTTP 400")`
    would be reported as a codec mismatch. A real 400 requires a URL that parsed, so checking here
    does not change how a real refusal is reported (WSM-ERR-016).

    `parse_uri` raises `InvalidURI` for a rejected scheme or hostname and a `ValueError` from
    `urllib.parse` for a non-integer port; `InvalidURI` is not a `ValueError`, so both are caught.
    Only URL parsing runs inside the `try`, so no other `ValueError` is caught.

    Raises `WebsocketsNotInstalledError` when `websockets` is absent and `WebsocketUrlError` when the
    URL cannot be parsed.
    """
    require_websockets()

    from websockets.exceptions import InvalidURI
    from websockets.uri import parse_uri

    dialable = url if uri is None else uri
    try:
        parse_uri(dialable)
    except (InvalidURI, ValueError) as exc:
        if uri is None:
            raise WebsocketUrlError(f"{url!r} is not a URL this transport can dial: {exc}") from exc
        raise WebsocketUrlError(
            f"{url!r} is not a URL this transport can dial: the handshake it would send is for {uri!r}, and {exc}"
        ) from exc


class WebsocketsSocket:
    """`SocketAdapter` over a `websockets` connection, either direction."""

    __slots__ = ("_connection",)

    def __init__(self, connection: Any) -> None:
        self._connection = connection

    async def send_text(self, text: str) -> None:
        await self._send(text)

    async def send_bytes(self, data: bytes) -> None:
        await self._send(data)

    async def _send(self, message: str | bytes) -> None:
        from websockets.exceptions import ConnectionClosed as WsClosed

        try:
            await self._connection.send(message)
        except WsClosed as exc:
            raise ConnectionClosed("websocket closed while sending", code=_code_of(exc)) from exc

    async def receive(self) -> str | bytes:
        from websockets.exceptions import ConnectionClosed as WsClosed

        try:
            return await self._connection.recv()
        except WsClosed as exc:
            raise ConnectionClosed("websocket closed", code=_code_of(exc), was_clean=_was_clean(exc)) from exc

    async def close(self, code: int = 1000, reason: str = "") -> None:
        await self._connection.close(code=code, reason=reason)


def _code_of(exc: Any) -> int:
    close = getattr(exc, "rcvd", None) or getattr(exc, "sent", None)
    return getattr(close, "code", 1006)


def _was_clean(exc: Any) -> bool:
    return _code_of(exc) == 1000


def select_subprotocol(connection: Any, subprotocols: list[str]) -> str:
    """Installable as `websockets.serve(..., select_subprotocol=muxws.select_subprotocol)`.

    The `websockets` library completes the handshake before calling the handler, so the decision is
    handed to it up front (WSM-CDC-027).

    A refusal raises `NegotiationError`. Returning None would answer 101 with no
    `Sec-WebSocket-Protocol` header and leave the mismatch to be detected on an open socket, which
    WSM-CDC-022 forbids where the transport offers a choice. `ServerProtocol.accept` turns an
    `InvalidHandshake` raised by this hook into HTTP 400 and any other exception into 500.
    """
    _ = connection
    from websockets.exceptions import NegotiationError

    from muxws.conf import settings

    selected = select(list(subprotocols), settings.codec)
    if selected is None:
        # `select` has already logged the offered and the configured codec (WSM-CDC-029). This
        # message becomes the 400 response body and contains nothing the dialer sent.
        raise NegotiationError(f"muxws requires the {PREFIX}{settings.codec} subprotocol")
    return selected


def verify_negotiated(negotiated: str | None, configured: str) -> None:
    """Check the subprotocol on an already-open socket (WSM-CDC-028).

    For a transport that offers neither a selection hook nor a way to deny the upgrade. Raises
    `CodecMismatch` when `negotiated` is not `muxws.v1.<configured>`; the caller then closes with 1008.

    """
    from muxws.subprotocol import mismatch_error

    if negotiated != f"{PREFIX}{configured}":
        logger.error(
            "muxws negotiated subprotocol is %r, expected %r; closing with %d",
            negotiated,
            f"{PREFIX}{configured}",
            POLICY_VIOLATION,
        )
        raise mismatch_error(configured)


__all__ = [
    "INSTALL_HINT",
    "POLICY_VIOLATION",
    "WebsocketUrlError",
    "WebsocketsNotInstalledError",
    "WebsocketsSocket",
    "find_offer",
    "require_websockets",
    "select_subprotocol",
    "verify_dialable_url",
    "verify_negotiated",
]
