"""The socket adapter port.

WSM-API-021: transport-specific code lives only behind this interface. Text and binary sends are
separate methods; the peer chooses between them from the declared `codec.binary` (WSM-CDC-002).
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class SocketAdapter(Protocol):
    """One WebSocket, as the peer accesses it."""

    async def send_text(self, text: str) -> None:
        """Put one text message on the wire. Called only under a text codec (WSM-CDC-002)."""
        ...

    async def send_bytes(self, data: bytes) -> None:
        """Put one binary message on the wire. Called only under a binary codec."""
        ...

    async def receive(self) -> str | bytes:
        """The next inbound message, waiting for one.

        **Raises `ConnectionClosed` when the socket ends**; this is the only way the peer learns that
        the connection died. An adapter that returned a sentinel, or blocked after the socket closed,
        would leave the read loop and every pending await waiting indefinitely (WSM-INV-011).
        """
        ...

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """Close this side.

        `code` defaults to 1000 and MUST NOT be 1006, which means "closed abnormally" and may not be
        sent; `websockets` raises on it, and an adapter that suppressed that error would leave the
        socket open. Idempotent: closing an already-closed socket is not an error.

        """
        ...
