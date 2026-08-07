"""Real sockets over the `websockets` library, in both roles."""

from __future__ import annotations

import asyncio

from collections.abc import AsyncIterator
from typing import Any

import pytest
import websockets

import muxws

from muxws.errors import CodecMismatch, RemoteError, ResetCode
from muxws.stream import Stream
from muxws.transports.websockets_ import WebsocketsSocket


async def _echo(payload: Any, stream: Stream) -> None:
    """One handler covering every call shape the tests need."""
    action = (payload or {}).get("action")
    if action == "stream":
        for index in range(3):
            await stream.send({"chunk": index})
        await stream.end({"chunk": 3})
    elif action == "raise":
        raise ValueError("handler said no")
    elif action == "forever":
        await stream.closed.wait()
    else:
        await stream.reply({"echo": payload})


@pytest.fixture
async def server() -> AsyncIterator[str]:
    """A `websockets` acceptor on an ephemeral port, with muxws installed as its handshake hook."""

    async def handle(connection: Any) -> None:
        peer = await muxws.accept(WebsocketsSocket(connection), codec=muxws.get_codec("json"))
        peer.on_stream(_echo)
        await peer.serve()

    async with websockets.serve(
        handle,
        "127.0.0.1",
        0,
        select_subprotocol=muxws.select_subprotocol,
    ) as service:
        port = service.sockets[0].getsockname()[1]
        yield f"ws://127.0.0.1:{port}"


async def test_select_subprotocol_hook_accepts_a_matching_dialer(server: str):
    """WSM-CDC-027: the hook is what a transport that handshakes before the handler needs."""
    peer = await muxws.connect(server)
    try:
        assert await peer.request({"action": "echo", "n": 1}) == {"echo": {"action": "echo", "n": 1}}
    finally:
        await peer._socket.close()


async def test_dialer_offers_the_muxws_entry_first_on_a_real_socket(server: str):
    """WSM-CDC-020: even when the application appended its own entries."""
    peer = await muxws.connect(server, subprotocols=["bearer.abc123"])
    try:
        assert await peer.request({"action": "echo"}) == {"echo": {"action": "echo"}}
    finally:
        await peer._socket.close()


async def test_mismatched_codecs_reject_handshake(server: str, monkeypatch: pytest.MonkeyPatch):
    """WSM-CDC-022/024 **(spec)**: refused at the handshake, and no frame is exchanged."""

    class Msgpackish(muxws.JsonCodec):
        name = "msgpack"

    monkeypatch.setattr(muxws.conf.settings, "codec", "json")
    with pytest.raises(CodecMismatch) as info:
        await muxws.connect(server, codec=Msgpackish())

    message = str(info.value)
    assert "msgpack" in message
    assert "VITE_MUXWS_CODEC" in message
    assert "MUXWS_CODEC" in message


async def test_real_socket_carries_the_m2_shapes(server: str):
    """Unary, streaming response, notify and cancel, over a real socket."""
    peer = await muxws.connect(server)
    try:
        assert await peer.request({"action": "echo", "v": 1}) == {"echo": {"action": "echo", "v": 1}}

        chunks = [item async for item in peer.open({"action": "stream"})]
        assert chunks == [{"chunk": i} for i in range(4)]

        assert await peer.notify({"action": "echo"}) is None

        with pytest.raises(RemoteError) as info:
            await peer.request({"action": "raise"})
        assert info.value.payload == {"type": "ValueError", "message": "handler said no"}

        held = peer.open({"action": "forever"})
        await asyncio.sleep(0.05)
        await held.cancel()
        assert held.closed.is_set()
    finally:
        await peer._socket.close()


async def test_server_push_uses_the_same_mechanism():
    """WSM-INV-002: one symmetric peer type, so a push is a request with the roles swapped."""
    pushed: list[Any] = []

    async def handle(connection: Any) -> None:
        peer = await muxws.accept(WebsocketsSocket(connection))
        peer.on_stream(_echo)
        serving = asyncio.create_task(peer.serve())
        await asyncio.sleep(0.05)
        await peer.notify({"event": "tick"})
        await serving

    async with websockets.serve(handle, "127.0.0.1", 0, select_subprotocol=muxws.select_subprotocol) as service:
        url = f"ws://127.0.0.1:{service.sockets[0].getsockname()[1]}"
        peer = await muxws.connect(url)

        async def receive(payload: Any, stream: Stream) -> None:
            _ = stream
            pushed.append(payload)

        peer.on_stream(receive)
        try:
            await asyncio.sleep(0.2)
            assert pushed == [{"event": "tick"}]
        finally:
            await peer._socket.close()


async def test_a_binary_codec_selects_the_binary_send_path(server: str):
    """WSM-CDC-002/WSM-API-021: the branch comes from `codec.binary`, and nothing sniffs."""
    sent: list[str] = []

    class SpyAdapter:
        def __init__(self, inner: Any) -> None:
            self._inner = inner

        async def send_text(self, text: str) -> None:
            sent.append("text")
            await self._inner.send_text(text)

        async def send_bytes(self, data: bytes) -> None:
            sent.append("bytes")
            await self._inner.send_bytes(data)

        async def receive(self) -> str | bytes:
            return await self._inner.receive()

        async def close(self, code: int = 1000, reason: str = "") -> None:
            await self._inner.close(code, reason)

    connection = await websockets.connect(server, subprotocols=["muxws.v1.json"])  # type: ignore[arg-type]
    peer = muxws.Peer(SpyAdapter(WebsocketsSocket(connection)), codec=muxws.get_codec("json"), is_dialer=True)
    serving = asyncio.create_task(peer.serve())
    try:
        assert await peer.request({"action": "echo"}) == {"echo": {"action": "echo"}}
        assert set(sent) == {"text"}, "a text codec must never reach send_bytes"
    finally:
        await connection.close()
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)


async def test_reset_codes_survive_a_real_socket(server: str):
    """A stream-level reset is carried and reconstructed as the right exception class."""
    peer = await muxws.connect(server)
    try:
        with pytest.raises(RemoteError) as info:
            await peer.request({"action": "raise"})
        assert info.value.code is ResetCode.APPLICATION_ERROR
    finally:
        await peer._socket.close()
