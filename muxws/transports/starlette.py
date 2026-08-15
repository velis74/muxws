"""Starlette / FastAPI acceptor.

Imports `starlette` inside the functions that need it, so the package keeps zero required runtime
dependencies (WSM-PKG-002): installing muxws does not install a web framework, and importing this
module is how an application says it wants one.
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

        Starlette raises on a second close, so the state is checked first: the protocol requires
        `close` to be idempotent, and the peer legitimately closes a socket the client has already
        walked away from.
        """
        from starlette.websockets import WebSocketState

        if self._websocket.client_state is not WebSocketState.DISCONNECTED:
            await self._websocket.close(code=code, reason=reason)


async def perform_upgrade(websocket: Any, configured: str) -> StarletteSocket:
    """Accept the upgrade, or **deny** it with HTTP 400 (WSM-CDC-022/026).

    `accept()` performs the WebSocket accept itself because it is the only party that knows which
    subprotocol to select; an application that accepted first has taken that decision away.

    On a mismatch the ASGI denial response goes out *before* any accept. `websocket.close()` before
    accept renders 403, which is not what WSM-CDC-022 asks for: the upgrade must be refused with no
    subprotocol selected and a 400.
    """
    from starlette.websockets import WebSocketState

    if websocket.client_state is not WebSocketState.CONNECTING:
        raise ProtocolError(
            "the websocket was already accepted; muxws.accept() performs the upgrade itself because "
            "it is the only party that knows which subprotocol to select (WSM-CDC-026)"
        )

    offered = list(websocket.scope.get("subprotocols") or [])
    selected = select(offered, configured)
    if selected is None:
        await websocket.send({"type": "websocket.http.response.start", "status": 400, "headers": []})
        await websocket.send({"type": "websocket.http.response.body", "body": b""})
        # CodecMismatch rather than a bare ProtocolError: this is the acceptor's half of the same
        # failure the dialer composes for itself (WSM-CDC-024/029), and `serve()` knows to answer it
        # with silence because the 400 has already gone out.
        raise CodecMismatch(
            f"refused the upgrade: offered {offered!r}, this acceptor speaks {PREFIX}{configured}",
            configured=configured,
        )

    await websocket.accept(subprotocol=selected)
    return StarletteSocket(websocket)
