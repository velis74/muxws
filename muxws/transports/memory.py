"""Two socket adapters wired to each other, with no socket anywhere.

Part of the installed package rather than the test tree: the conformance runner and any application
test that wants a peer pair without a listening socket import it as public API.
"""

from __future__ import annotations

import asyncio

from muxws.errors import ConnectionClosed


class MemorySocket:
    """One end of an in-memory pair. Implements `SocketAdapter`."""

    __slots__ = ("_inbox", "_outbox", "_closed", "_peer_socket", "sent")

    def __init__(self) -> None:
        self._inbox: asyncio.Queue[str | bytes | None] = asyncio.Queue()
        self._outbox: asyncio.Queue[str | bytes | None] | None = None
        self._closed = False
        self._peer_socket: MemorySocket | None = None
        #: Every message this side put on the wire, in order. The conformance runner reads it.
        self.sent: list[str | bytes] = []

    @property
    def is_closed(self) -> bool:
        return self._closed

    async def send_text(self, text: str) -> None:
        await self._deliver(text)

    async def send_bytes(self, data: bytes) -> None:
        await self._deliver(data)

    async def _deliver(self, message: str | bytes) -> None:
        if self._closed:
            raise ConnectionClosed("socket is closed", code=1006)
        self.sent.append(message)
        if self._outbox is not None:
            await self._outbox.put(message)

    async def receive(self) -> str | bytes:
        message = await self._inbox.get()
        if message is None:
            raise ConnectionClosed("socket closed while receiving", code=1006)
        return message

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """Close this side cleanly, waking both ends' pending receives."""
        _ = (code, reason)
        if self._closed:
            return
        self._closed = True
        await self._inbox.put(None)
        if self._peer_socket is not None and not self._peer_socket._closed:
            self._peer_socket._closed = True
            await self._peer_socket._inbox.put(None)

    async def drop(self) -> None:
        """Simulate socket death: no close frame, no warning, both ends simply stop."""
        await self.close(code=1006, reason="dropped")

    def inject(self, message: str | bytes) -> None:
        """Push a raw message into this side's inbox, bypassing the other peer.

        The conformance runner uses it to deliver frames no correct implementation would send.
        """
        self._inbox.put_nowait(message)


def memory_pair() -> tuple[MemorySocket, MemorySocket]:
    """Two adapters wired to each other. `drop()` on either simulates socket death."""
    left = MemorySocket()
    right = MemorySocket()
    left._outbox = right._inbox
    right._outbox = left._inbox
    left._peer_socket = right
    right._peer_socket = left
    return left, right
