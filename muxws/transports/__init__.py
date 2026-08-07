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

    async def send_text(self, text: str) -> None: ...

    async def send_bytes(self, data: bytes) -> None: ...

    async def receive(self) -> str | bytes: ...

    async def close(self, code: int = 1000, reason: str = "") -> None: ...
