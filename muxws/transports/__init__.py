"""The socket adapter port.

WSM-API-021: this is the **only** place transport-specific code lives. Text and binary sends are
separate methods, never one polymorphic `send` - the peer picks between them from `codec.binary`,
which is declared rather than sniffed (WSM-CDC-002).
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class SocketAdapter(Protocol):
    """One WebSocket, seen the only way the peer is allowed to see it."""

    async def send_text(self, text: str) -> None:
        """Put one text message on the wire. Called only under a text codec (WSM-CDC-002)."""
        ...

    async def send_bytes(self, data: bytes) -> None:
        """Put one binary message on the wire. Called only under a binary codec."""
        ...

    async def receive(self) -> str | bytes:
        """The next inbound message, waiting for one.

        **Raises `ConnectionClosed` when the socket ends**, and that is how the peer learns the
        connection died - it is the only signal. An adapter that returned a sentinel, or that blocked
        forever after the socket closed, would leave the read loop parked and every pending await
        hanging with no error anywhere (WSM-INV-011).
        """
        ...

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """Close this side.

        `code` defaults to 1000 and MUST NOT be 1006: that code means "closed abnormally" and a peer
        may never send it - `websockets` rejects it outright, and an adapter that swallowed the
        rejection would leave the socket open while the peer believed it closed. Idempotent: closing
        an already-closed socket is not an error.
        """
        ...
