"""The `websockets` library: a dialer, and the acceptor's handshake hook.

The module keeps its trailing underscore: a `websockets.py` inside this package would shadow the
library it imports. `websockets` is imported inside the functions, so the package keeps zero
required runtime dependencies (WSM-PKG-002).
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

#: The extra that installs this transport's dependency, spelled the way a reader can paste it. Named
#: once, here, because the whole value of `WebsocketsNotInstalledError` over a bare `ImportError` is
#: that it carries the remedy, and a remedy that drifts out of date is worse than none (WSM-ERR-016).
INSTALL_HINT = "pip install muxws[websockets]"


class WebsocketUrlError(TransportUrlError):
    """A `ws:`/`wss:` URL the `websockets` library cannot turn into something to dial.

    No hostname (`ws:/nohost`), a scheme that is neither `ws` nor `wss`, a port that is not an
    integer, or a string that is not a URL at all. It is a `TransportUrlError` - and so a
    `MuxwsError` and a `ValueError` - so that one `except MuxwsError` around `connect()` catches an
    unusable URL whichever scheme it names (WSM-ERR-016).

    The library's own diagnostic is not replaced, only framed: it is quoted in the message and the
    original exception is chained with `from exc`, because `websockets` says *which* part of the URL
    it objected to and this class does not.
    """


class WebsocketsNotInstalledError(TransportUnsupportedError):
    """`connect()` was called and `import websockets` failed; the message names the extra.

    One class for both dial arms: a `ws://` dial and a `ws+unix://` dial fail on the identical
    `import websockets`, because it is the identical dependency.

    `NotInstalled` rather than `Unavailable`: "unavailable" in a traceback out of a dial reads as a
    transient endpoint failure a caller may retry, and this is a permanent local condition no retry
    and no different URL can fix. That is also why the base is `TransportUnsupportedError` (a
    `RuntimeError`) and not `TransportUrlError`.

    Only the package being absent becomes this class. A `websockets` that is present but broken keeps
    its own `ImportError` - see `require_websockets`, and the same narrowing in
    `ts/node.ts::WsNotInstalledError`.
    """


def require_websockets() -> Any:
    """Import `websockets`, or raise `WebsocketsNotInstalledError` naming the extra.

    The gate for both dial arms, and why this module has no module-scope `import websockets`: the
    package has zero required runtime dependencies (WSM-PKG-002), so the absence can only be detected
    where it is needed and reported as a muxws error that says what to install (WSM-ERR-016).

    Called from `verify_dialable_url` before the URL is parsed, because the URL parser is supplied by
    the dependency: a caller with neither gets the failure that blocks the other, rather than an
    `ImportError` out of the middle of a URL check.

    Only the package actually being absent becomes this class. `ModuleNotFoundError` with `name`
    exactly `"websockets"` is the one shape that means absent; a missing *sub*module (`name` of
    `"websockets.asyncio"`) or a plain `ImportError` means the package was found and something inside
    it failed, and answering that with `pip install muxws[websockets]` would hide the only message
    naming the real fault. `ts/node.ts::requireWs` narrows the same way on `ERR_MODULE_NOT_FOUND`.
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

    `uri` is the *logical* `ws://` URL a `ws+unix:` dial synthesises and hands to
    `unix_connect(uri=...)`; it is what that arm's handshake actually parses, so it is what gets
    checked. For a TCP dial there is nothing to synthesise and `url` is checked as it stands. Both
    arms go through here because both arms parse a URL with the same parser from the same package.

    Called outside `connect()`'s dial closure, never inside the closure's `except`. That handler
    translates a refused upgrade into `CodecMismatch` (WSM-CDC-022/024) by reading a 400 out of the
    exception, and its string fallback matches `HTTP 400` anywhere in the message - including in an
    `InvalidURI` that is merely quoting the URL back, so `connect("ws:/HTTP 400")` would be reported
    as a codec mismatch on a connection that was never made. An unparseable URL never reaches the
    closure, and no real refusal changes: a real 400 requires a dial that reached a server, which
    requires a URL that parsed (WSM-ERR-016).

    `parse_uri` raises `InvalidURI` for a scheme or hostname it rejects and a plain `ValueError` out
    of `urllib.parse` for a port that is not an integer; `InvalidURI` is not a `ValueError`, so
    catching either alone misses half the shapes. Catching `ValueError` this broadly is safe only
    because nothing but URL parsing runs inside the `try` - the same catch inside the dial's handler
    would also see a `ValueError` raised while building headers.
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

    The refusal is **raised, not returned**. Returning None here answers 101 with no
    `Sec-WebSocket-Protocol` header and leaves the mismatch to be found on an already-open socket -
    precisely the "complete the handshake and close afterwards" that WSM-CDC-022 forbids wherever
    the transport offers a choice. `ServerProtocol.accept` turns an `InvalidHandshake` out of this
    hook into HTTP 400 and anything else into 500, so `NegotiationError` is the one exception that
    produces the status the rule names.
    """
    _ = connection
    from websockets.exceptions import NegotiationError

    from muxws.conf import settings

    selected = select(list(subprotocols), settings.codec)
    if selected is None:
        # `select` has already logged which codec was offered against which is configured
        # (WSM-CDC-029); this message is only what fits in the 400's body, and it deliberately
        # reflects nothing the dialer sent.
        raise NegotiationError(f"muxws requires the {PREFIX}{settings.codec} subprotocol")
    return selected


def verify_negotiated(negotiated: str | None, configured: str) -> None:
    """Check the subprotocol on an already-open socket (WSM-CDC-028).

    The last resort, for a transport that offers neither a selection hook nor a way to deny the
    upgrade. The caller closes with 1008 when this raises.
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
