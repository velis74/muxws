"""The `websockets` library: a dialer, and the acceptor's handshake hook.

The module keeps its trailing underscore: a `websockets.py` inside this package would shadow the
library it imports. `websockets` is imported inside the functions, so the package keeps zero
required runtime dependencies (WSM-PKG-002).
"""

from __future__ import annotations

import logging

from typing import Any

from muxws.errors import ConnectionClosed
from muxws.subprotocol import find_offer, PREFIX, select

logger = logging.getLogger("muxws.codec")

#: The WebSocket close code for a policy violation, used when a transport offers no handshake hook
#: at all and the mismatch can only be discovered on an already-open socket (WSM-CDC-028).
POLICY_VIOLATION = 1008


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


__all__ = ["POLICY_VIOLATION", "WebsocketsSocket", "find_offer", "select_subprotocol", "verify_negotiated"]
