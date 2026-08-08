"""Real sockets over the `websockets` library, in both roles."""

from __future__ import annotations

import asyncio
import logging
import socket as socketlib

from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import urlsplit

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


class Acceptor:
    """A running acceptor, plus the one thing a refusal test has to count.

    `handshakes` is incremented by the connection handler, which `websockets` calls only after the
    upgrade has been answered 101. A refusal must leave it at zero: WSM-CDC-022 forbids completing
    the handshake and closing afterwards, so an acceptor that got as far as its handler has already
    broken the rule whatever it does next.
    """

    def __init__(self) -> None:
        self.url = ""
        self.handshakes = 0


@pytest.fixture
async def acceptor() -> AsyncIterator[Acceptor]:
    """A `websockets` acceptor on an ephemeral port, with muxws installed as its handshake hook."""
    running = Acceptor()

    async def handle(connection: Any) -> None:
        running.handshakes += 1
        peer = await muxws.accept(WebsocketsSocket(connection), codec=muxws.get_codec("json"))
        peer.on_stream(_echo)
        await peer.serve()

    async with websockets.serve(
        handle,
        "127.0.0.1",
        0,
        select_subprotocol=muxws.select_subprotocol,
    ) as service:
        running.url = f"ws://127.0.0.1:{service.sockets[0].getsockname()[1]}"
        yield running


@pytest.fixture
def server(acceptor: Acceptor) -> str:
    """The same acceptor for the tests that only need somewhere to dial."""
    return acceptor.url


async def _upgrade(url: str, offered: str) -> tuple[int, dict[str, str]]:
    """Send a real WebSocket upgrade by hand and read the HTTP status line back.

    No muxws peer can be the witness for WSM-CDC-022. The rule is entirely about *which status code*
    comes back, and a dial in this language never sees one: `connect()` recovers through the
    WSM-CDC-028 check on the open socket and reports `CodecMismatch` whether the acceptor answered
    400 or 101-with-no-subprotocol. That blind spot is exactly how an acceptor answering 101 passed
    three milestones of green suites, so the request is spoken here at the HTTP level instead.
    """
    parts = urlsplit(url)
    host, port = parts.hostname or "127.0.0.1", parts.port or 80
    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(
            "GET / HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            # A fixed key is fine: nothing here verifies `Sec-WebSocket-Accept`, and a constant makes
            # the request byte-identical between runs.
            "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            f"Sec-WebSocket-Protocol: {offered}\r\n"
            "\r\n".encode()
        )
        await writer.drain()
        status_line = await asyncio.wait_for(reader.readline(), timeout=5)
        headers: dict[str, str] = {}
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=5)
            if line in (b"\r\n", b"\n", b""):
                break
            name, _, value = line.decode().partition(":")
            headers[name.strip().lower()] = value.strip()
        return int(status_line.split()[1]), headers
    finally:
        writer.close()


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


async def test_a_mismatched_codec_offer_is_answered_with_http_400(acceptor: Acceptor):
    """WSM-CDC-022 **(spec)**: refuse the upgrade - no subprotocol, and status 400.

    Answering 101 with no `Sec-WebSocket-Protocol` and leaving the mismatch to the post-handshake
    check is the "complete the handshake and close afterwards" the rule forbids wherever the
    transport gives a choice; `websockets` gives one.
    """
    status, headers = await _upgrade(acceptor.url, "muxws.v1.msgpack")

    assert status == 400
    assert "sec-websocket-protocol" not in headers
    assert acceptor.handshakes == 0, "a refused upgrade must never reach the connection handler"


async def test_a_matching_codec_offer_is_answered_with_http_101(acceptor: Acceptor):
    """The other half of WSM-CDC-022, without which the 400 above is also what a broken acceptor
    answers everyone: the offered value must come back as the selected subprotocol."""
    status, headers = await _upgrade(acceptor.url, "muxws.v1.json")

    assert status == 101
    assert headers["sec-websocket-protocol"] == "muxws.v1.json"


@pytest.mark.parametrize(
    "offered",
    ["muxws.v2.json", "bearer.abc123"],
    ids=["a-later-generation", "no-muxws-entry-at-all"],
)
async def test_an_unusable_offer_is_answered_with_http_400(acceptor: Acceptor, offered: str):
    """WSM-CDC-025 and WSM-CDC-022's third case, refused at the handshake and not one step later.

    A generation this acceptor cannot speak is rejected *here*, which is what makes the version
    component of the subprotocol name a real gate rather than a label (WSM-CON-009).
    """
    status, _ = await _upgrade(acceptor.url, offered)

    assert status == 400
    assert acceptor.handshakes == 0


async def test_mismatched_codecs_reject_handshake(
    acceptor: Acceptor,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    """WSM-CDC-022/024 **(spec)**: refused at the handshake, and no frame is exchanged.

    The frame count is the negative witness the rule has always named and nothing has ever asserted.
    Both peers would live in this process, so the `muxws.frames` logger sees every frame either of
    them sends or receives, and `hello=` guarantees there would be one to see: a dialer that got a
    socket puts its hello on the wire immediately (WSM-RCN-021).
    """

    class Msgpackish(muxws.JsonCodec):
        name = "msgpack"

    monkeypatch.setattr(muxws.conf.settings, "codec", "json")
    with caplog.at_level(logging.DEBUG, logger="muxws.frames"), pytest.raises(CodecMismatch) as info:
        await muxws.connect(acceptor.url, codec=Msgpackish(), hello={"session": "abc"})

    message = str(info.value)
    assert "msgpack" in message
    assert "VITE_MUXWS_CODEC" in message
    assert "MUXWS_CODEC" in message

    # The frame count comes first because it is the stronger statement: an implementation that
    # noticed the mismatch only *after* saying hello would still leave `handshakes` looking wrong,
    # but this is the line that names what actually went out.
    exchanged = [record.getMessage() for record in caplog.records if record.name == "muxws.frames"]
    assert exchanged == [], f"a refused handshake exchanged {len(exchanged)} muxws frame(s)"
    assert acceptor.handshakes == 0, "the upgrade must have been refused, not completed and closed"


async def test_a_101_that_negotiated_something_else_is_caught_on_the_open_socket():
    """WSM-CDC-028: the last resort, for the one case the handshake cannot refuse.

    This acceptor selects the application's own entry instead of the muxws one, and `websockets`
    completes the handshake because that value *was* offered (WSM-CDC-021). Nothing about the
    upgrade looks wrong from the dialer's side, so the check on the already-open socket is all there
    is - and it is the only route a browser ever has. It is not a substitute for the 400: it is what
    made the missing 400 invisible for three milestones.
    """

    def select_the_wrong_one(_connection: Any, _subprotocols: list[str]) -> str:
        return "bearer.abc123"

    async def handle(connection: Any) -> None:
        await connection.wait_closed()

    async with websockets.serve(handle, "127.0.0.1", 0, select_subprotocol=select_the_wrong_one) as service:
        url = f"ws://127.0.0.1:{service.sockets[0].getsockname()[1]}"
        with pytest.raises(CodecMismatch):
            await muxws.connect(url, subprotocols=["bearer.abc123"])


def _a_closed_port_whose_number_contains_400() -> int:
    """A port nothing is listening on, chosen so the connection error names "400" in its text."""
    for candidate in (14000, 24000, 34000, 44000):
        with socketlib.socket() as probe:
            try:
                probe.bind(("127.0.0.1", candidate))
            except OSError:
                continue
        return candidate
    pytest.skip("no candidate port containing 400 was free")


async def test_an_unreachable_acceptor_is_not_reported_as_a_codec_mismatch():
    """WSM-CDC-024 is about a *refused upgrade*, and only about that.

    Reading the status out of the exception's prose - `"400" in str(exc)` - also matches
    `[Errno 111] Connect call failed ('127.0.0.1', 14000)`, which would send the reader off to check
    MUXWS_CODEC on both ends of a connection where nothing is listening at all. The status is read
    from `InvalidStatus.response`, so a dead port stays an `OSError`.
    """
    port = _a_closed_port_whose_number_contains_400()

    # `CodecMismatch` is not an `OSError`, so naming the expected type here is the whole assertion:
    # the old heuristic raised `CodecMismatch` and this line failed.
    with pytest.raises(ConnectionRefusedError):
        await muxws.connect(f"ws://127.0.0.1:{port}")


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
