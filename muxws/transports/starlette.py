"""Starlette / FastAPI acceptor.

Imports `starlette` inside the functions that need it, so the package has no required runtime
dependencies (WSM-PKG-002).
"""

from __future__ import annotations

from typing import Any

from muxws.errors import CodecMismatch, ProtocolError
from muxws.subprotocol import PREFIX, select


class StarletteSocket:
    """`SocketAdapter` over `starlette.websockets.WebSocket`."""

    __slots__ = ("_websocket",)

    def __init__(self, websocket: Any) -> None:
        self._websocket = websocket

    async def send_text(self, text: str) -> None:
        await self._websocket.send_text(text)

    async def send_bytes(self, data: bytes) -> None:
        await self._websocket.send_bytes(data)

    async def receive(self) -> str | bytes:
        from starlette.websockets import WebSocketDisconnect

        from muxws.errors import ConnectionClosed

        try:
            message = await self._websocket.receive()
        except WebSocketDisconnect as exc:
            raise ConnectionClosed("websocket disconnected", code=exc.code, reason="") from exc

        if message["type"] == "websocket.disconnect":
            raise ConnectionClosed("websocket disconnected", code=message.get("code", 1006), reason="")
        if message.get("text") is not None:
            return message["text"]
        if message.get("bytes") is not None:
            return message["bytes"]
        raise ProtocolError(f"unexpected websocket message {message['type']!r}")

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """Close this side, tolerating a socket that is already disconnected.

        Starlette raises on a second close, so the state is checked first: `close` must be idempotent,
        and the peer may close a socket the client has already disconnected.
        """
        from starlette.websockets import WebSocketState

        if self._websocket.client_state is not WebSocketState.DISCONNECTED:
            await self._websocket.close(code=code, reason=reason)


def is_unaccepted(websocket: Any) -> bool:
    """True while the application has neither accepted nor closed `websocket`."""
    from starlette.websockets import WebSocketState

    return websocket.client_state is WebSocketState.CONNECTING


async def deny(websocket: Any, status: int) -> None:
    """Answer the upgrade with the ASGI denial response `status` and an empty body instead of accepting."""
    await websocket.send({"type": "websocket.http.response.start", "status": status, "headers": []})
    await websocket.send({"type": "websocket.http.response.body", "body": b""})


async def perform_upgrade(websocket: Any, configured: str) -> StarletteSocket:
    """Accept the upgrade, or **deny** it with HTTP 400 (WSM-CDC-022/026).

    `accept()` performs the WebSocket accept itself because it selects the subprotocol; a socket the
    application has already accepted raises `ProtocolError`.

    On a mismatch the ASGI denial response is sent instead of an accept, and `CodecMismatch` is
    raised. `websocket.close()` before accept would answer 403; WSM-CDC-022 requires a 400 with no
    subprotocol selected.
    """
    if not is_unaccepted(websocket):
        raise ProtocolError(
            "the websocket was already accepted; muxws.accept() performs the upgrade itself because "
            "it is the only party that knows which subprotocol to select (WSM-CDC-026)"
        )

    offered = list(websocket.scope.get("subprotocols") or [])
    selected = select(offered, configured)
    if selected is None:
        await deny(websocket, 400)
        # CodecMismatch, the same class the dialer raises for this failure (WSM-CDC-024/029).
        # `serve()` returns without raising on it, because the 400 has already been sent.

        raise CodecMismatch(
            f"refused the upgrade: offered {offered!r}, this acceptor speaks {PREFIX}{configured}",
            configured=configured,
        )

    await websocket.accept(subprotocol=selected)
    return StarletteSocket(websocket)
