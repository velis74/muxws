"""Real sockets over the `websockets` library, in both roles."""

from __future__ import annotations

import asyncio
import importlib.util
import logging
import socket as socketlib
import sys
import tempfile

from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType
from typing import Any
from urllib.parse import urlsplit

import pytest
import websockets

from websockets.asyncio.server import unix_serve
from websockets.exceptions import InvalidURI

import muxws

from muxws.errors import CodecMismatch, MuxwsError, RemoteError, ResetCode, TransportUnsupportedError, TransportUrlError
from muxws.stream import Stream
from muxws.transports.unix import parse_unix_url
from muxws.transports.websockets_ import (
    INSTALL_HINT,
    WebsocketsNotInstalledError,
    WebsocketsSocket,
    WebsocketUrlError,
)


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
    return await _speak_the_upgrade(reader, writer, host=f"{host}:{port}", request_target="/", offered=offered)


async def _speak_the_upgrade(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    host: str,
    request_target: str,
    offered: str,
) -> tuple[int, dict[str, str]]:
    """The upgrade request itself, byte for byte, over whatever stream pair the caller opened.

    Shared with the Unix-socket twin, whose claim is that WSM-CDC-022 is answered *identically* over
    a filesystem socket: two hand-written request builders drifting apart by one header would turn a
    difference in the question into a difference in the answer. `asyncio.open_connection` and
    `asyncio.open_unix_connection` return the same pair of objects, so the transport is all that
    differs between the callers.
    """
    try:
        writer.write(
            f"GET {request_target} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
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

    The last assertion is the other half of WSM-ERR-016's layering: a real refusal reaches a server,
    so its URL parsed, so the check ahead of the dial let it through. A URL check that claimed a
    dialable address would show up here as a `TransportUrlError` in place of the `CodecMismatch`.
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
    assert not isinstance(info.value, TransportUrlError), "a refused upgrade is not a bad address"


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


# --------------------------------------------------------------------------- the same, over AF_UNIX
#
# `ws+unix:///path/to.sock:/route` dials a filesystem socket. Nothing below is a new protocol: each
# test is the twin of one above it, because a dial has four transport-shaped ways to go wrong - the
# request target and `Host` a filesystem path cannot supply, the subprotocol offer, the 400 that must
# still be translated, and the socket file a reconnect has to re-open.

#: AF_UNIX does not exist on Windows, so every test that opens a socket file carries this. The
#: grammar itself is tested in `unix_test.py`, which needs no socket and therefore never skips: that
#: split is what keeps the portability guard honest on a CI that only ever runs Linux.
requires_af_unix = pytest.mark.skipif(
    not hasattr(socketlib, "AF_UNIX"),
    reason="this platform has no AF_UNIX, so no ws+unix: URL can be dialled here",
)


@pytest.fixture
async def unix_acceptor() -> AsyncIterator[Acceptor]:
    """The `acceptor` fixture's twin, listening on a socket file instead of an ephemeral port.

    `tempfile.TemporaryDirectory`, not pytest's `tmp_path`: `sun_path` holds about 108 bytes and
    pytest's per-test directory is already ~74 of them on Linux and ~120 under a macOS `TMPDIR`, so
    the natural choice is the one that fails there with `AF_UNIX path too long` and nowhere else.
    """
    running = Acceptor()

    async def handle(connection: Any) -> None:
        running.handshakes += 1
        peer = await muxws.accept(WebsocketsSocket(connection), codec=muxws.get_codec("json"))
        peer.on_stream(_echo)
        await peer.serve()

    with tempfile.TemporaryDirectory(prefix="muxws-") as directory:
        path = str(Path(directory) / "s.sock")
        async with unix_serve(handle, path, select_subprotocol=muxws.select_subprotocol):
            running.url = f"ws+unix://{path}:/ws"
            yield running


@pytest.fixture
def unix_server(unix_acceptor: Acceptor) -> str:
    """The same acceptor for the tests that only need a `ws+unix:` URL to dial."""
    return unix_acceptor.url


async def _upgrade_over_unix(url: str, offered: str) -> tuple[int, dict[str, str]]:
    """`_upgrade`, over a socket file: the same request, sent down `open_unix_connection` instead.

    The URL is resolved with the library's own parser rather than by pulling the path out of the
    fixture, because that makes this the one test that proves the two halves of the feature meet: the
    grammar really does name the file the acceptor is listening on. A broken parser cannot make this
    pass quietly - there is nothing to connect to - it can only make it fail.
    """
    target = parse_unix_url(url)
    assert target is not None, f"{url!r} must parse as a ws+unix: URL"
    logical = urlsplit(target.uri)
    reader, writer = await asyncio.open_unix_connection(target.path)
    return await _speak_the_upgrade(
        reader,
        writer,
        host=logical.netloc,
        request_target=logical.path,
        offered=offered,
    )


@requires_af_unix
async def test_a_mismatched_codec_offer_over_a_unix_socket_is_answered_with_http_400(unix_acceptor: Acceptor):
    """WSM-CDC-022 over AF_UNIX, at the only level that can see it.

    The refusal is HTTP, and HTTP is exactly as present over a socket file as over a port - so an
    implementation that reached for a shortcut here, "it is a local socket, the peer is trusted, let
    the post-handshake check catch it", would be completing the handshake and closing afterwards.
    `handshakes == 0` is the assertion that says it did not.
    """
    status, headers = await _upgrade_over_unix(unix_acceptor.url, "muxws.v1.msgpack")

    assert status == 400
    assert "sec-websocket-protocol" not in headers
    assert unix_acceptor.handshakes == 0, "a refused upgrade must never reach the connection handler"


@requires_af_unix
async def test_a_matching_codec_offer_over_a_unix_socket_is_answered_with_http_101(unix_acceptor: Acceptor):
    """The other half, without which the 400 above is also what an acceptor answers everyone.

    It doubles as the proof that the request target and `Host` this transport has to invent out of
    the URL are ones a real acceptor accepts: `websockets` answers 400 to a malformed request line
    just as readily as to a codec it does not speak, and the pair of tests would then agree for
    entirely the wrong reason.
    """
    status, headers = await _upgrade_over_unix(unix_acceptor.url, "muxws.v1.json")

    assert status == 101
    assert headers["sec-websocket-protocol"] == "muxws.v1.json"


@requires_af_unix
async def test_a_unix_socket_carries_the_m2_shapes(unix_server: str):
    """Unary, streaming response, notify, a remote failure and cancel, over a socket file.

    The whole feature in one test: `connect()` given a `ws+unix:` URL returns a peer that is not
    distinguishable from a TCP one by anything an application can do to it. `ping()` is here too, and
    is not decoration - the heartbeat is what declares a socket dead (WSM-RCN-010/011), and it runs
    over the frame layer, which means over whatever `WebsocketsSocket` was handed.
    """
    peer = await muxws.connect(unix_server)
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

        assert await peer.ping() >= 0.0, "the heartbeat's round trip has to complete over AF_UNIX too"
    finally:
        await peer._socket.close()


@requires_af_unix
async def test_a_mismatched_codec_over_a_unix_socket_reaches_the_caller_as_a_codec_mismatch(
    unix_acceptor: Acceptor,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    """WSM-CDC-022/024 through the public entry point, which is where the translation can be lost.

    The 400 an acceptor sends is only useful if the dialer turns it into `CodecMismatch`, and that
    translation lives in one `except` around the dial. A Unix dial written in a `try` of its own
    still connects, still gets its 400, and still raises - as `InvalidStatus`, a `websockets` type
    naming a status code, to an application that was told to catch `CodecMismatch`. Nothing else in
    the suite goes red when that happens, which is why this test exists.

    The empty frame log is the stronger of the two assertions, exactly as in the TCP twin: `hello=`
    guarantees a dialer that got a socket would have put something on the wire immediately.
    """

    class Msgpackish(muxws.JsonCodec):
        name = "msgpack"

    monkeypatch.setattr(muxws.conf.settings, "codec", "json")
    with caplog.at_level(logging.DEBUG, logger="muxws.frames"), pytest.raises(CodecMismatch) as info:
        await muxws.connect(unix_acceptor.url, codec=Msgpackish(), hello={"session": "abc"})

    assert "msgpack" in str(info.value)

    exchanged = [record.getMessage() for record in caplog.records if record.name == "muxws.frames"]
    assert exchanged == [], f"a refused handshake exchanged {len(exchanged)} muxws frame(s)"
    assert unix_acceptor.handshakes == 0, "the upgrade must have been refused, not completed and closed"


@requires_af_unix
async def test_a_missing_socket_file_is_not_reported_as_a_codec_mismatch():
    """The UDS twin of the dead-port test: nothing listening is not a misconfigured codec.

    A socket file that is not there is the commonest failure of this transport by a wide margin - the
    service has not started yet, or writes its socket somewhere else - and it has to arrive as the
    `FileNotFoundError` naming the path. Reporting it as `CodecMismatch` would send the reader off to
    compare MUXWS_CODEC on both ends of a connection that was never made.
    """
    with tempfile.TemporaryDirectory(prefix="muxws-") as directory:
        url = f"ws+unix://{Path(directory) / 'absent.sock'}:/ws"

        # `CodecMismatch` is not an `OSError`, so naming the expected class here is the assertion.
        with pytest.raises(FileNotFoundError):
            await muxws.connect(url)


@requires_af_unix
async def test_a_peer_reconnects_across_a_re_created_socket_file():
    """WSM-RCN-020/030 over AF_UNIX: the dial closure re-opens the path, and gets the new inode.

    A socket file is not a port. `unix_serve` leaves the file behind when it closes, and the next
    server unlinks that stale entry and binds a **new** inode at the same path - so a dialer holding
    anything resolved once, a file descriptor or an inode, reconnects to a socket nobody is listening
    on and hangs. Re-opening the path on every attempt is the only thing that works, and it is what
    parsing the URL outside the closure and passing only the path into it buys.

    The hello is asserted twice because that is the rule: replayed verbatim on the new connection
    (WSM-RCN-020), and `on_reconnect` fires after it is acknowledged and not before (WSM-RCN-030).
    """
    hellos: list[Any] = []

    async def handle(connection: Any) -> None:
        peer = await muxws.accept(WebsocketsSocket(connection))
        peer.on_stream(lambda payload, _stream: hellos.append(payload))
        await peer.serve()

    def listen(path: str) -> Any:
        return unix_serve(handle, path, select_subprotocol=muxws.select_subprotocol)

    with tempfile.TemporaryDirectory(prefix="muxws-") as directory:
        path = str(Path(directory) / "s.sock")
        first = await listen(path)
        peer = await muxws.connect(
            f"ws+unix://{path}:/ws",
            hello={"tab": "abc"},
            reconnect=muxws.Reconnect(initial_delay=0.01, max_delay=0.05),
            ping_interval=0.0,
        )
        reconnected: list[int] = []
        again = asyncio.Event()
        peer.on_reconnect(lambda attempt, _peer: (reconnected.append(attempt), again.set()))
        try:
            assert hellos == [{"tab": "abc"}], "the hello goes out on the first connection too"

            first.close()
            await first.wait_closed()
            second = await listen(path)
            try:
                await asyncio.wait_for(again.wait(), 5.0)

                assert reconnected == [1]
                assert hellos == [{"tab": "abc"}, {"tab": "abc"}], "replayed verbatim on the new socket"
                assert peer.is_open is True

                await peer.notify({"after": "the reconnect"})
                await asyncio.sleep(0.1)
                assert hellos[-1] == {"after": "the reconnect"}, "and the new socket carries traffic"
            finally:
                second.close()
                await second.wait_closed()
        finally:
            await peer.close()


# ------------------------------------------------------- the address and the dependency (WSM-ERR-016)
#
# Nothing below reaches a socket, and none of it can be witnessed by the tests above, every one of
# which has an acceptor. Two failures live here - an address `websockets` cannot parse, and a
# `websockets` that is not installed at all - and each must arrive as a `MuxwsError` under a shared
# transport base, so that one `except MuxwsError` around `connect()` catches a malformed address
# whichever scheme it named.


@pytest.mark.parametrize(
    "url",
    [
        "ws:/nohost",
        "wss:/nohost",
        "ws://",
        "http://example.com/x",
        "not a url at all",
        "ws://host:notaport/x",
    ],
    ids=[
        "ws-with-no-hostname",
        "wss-with-no-hostname",
        "an-authority-that-is-empty",
        "a-scheme-that-is-not-ws-or-wss",
        "a-string-that-is-not-a-url",
        "a-port-that-is-not-a-number",
    ],
)
async def test_a_url_websockets_cannot_parse_arrives_as_a_transport_url_error(url: str):
    """WSM-ERR-016: a `ws:`/`wss:` address this transport cannot open is a `TransportUrlError`.

    Both schemes are here because the translation is one call and a rewrite that special-cased `ws:`
    would still pass a `ws:`-only table. The last two rows are the ones a narrower implementation
    misses: `parse_uri` answers a bad port with a plain `ValueError` out of `urllib.parse`, and
    `InvalidURI` is not a `ValueError`, so an `except InvalidURI` alone lets that shape through
    unchanged - while a string that is not a URL at all is the shape a configuration file produces
    when a variable was never substituted.

    The three `isinstance` assertions are the rule itself rather than three ways of saying one thing:
    `MuxwsError` is what an application-wide handler names, `ValueError` is what a caller who never
    heard of muxws already catches around a URL it typed, and `TransportUrlError` is what a caller
    that wants "this address is wrong, whatever transport it named" writes without importing this
    module (it is not importable at all on a machine with no `websockets`).
    """
    with pytest.raises(WebsocketUrlError) as info:
        await muxws.connect(url)

    assert isinstance(info.value, TransportUrlError)
    assert isinstance(info.value, MuxwsError)
    assert isinstance(info.value, ValueError)
    assert url in str(info.value), "the message has to name the URL that was rejected"


@pytest.mark.parametrize(
    ("url", "underlying"),
    [("ws:/nohost", InvalidURI), ("ws://host:notaport/x", ValueError)],
    ids=["invalid-uri", "a-port-that-is-not-a-number"],
)
async def test_the_librarys_own_diagnostic_survives_the_translation(url: str, underlying: type[BaseException]):
    """The original is chained with `from exc`, so the traceback still says *what* was wrong.

    `WebsocketUrlError` knows that a URL was refused; only `websockets` knows whether the hostname was
    missing, the scheme was wrong or the port was not a number. A translation that swallowed that
    would turn a one-glance diagnosis into a puzzle, so the class frames the library's wording and
    never replaces it - the message quotes it and `__cause__` still holds the exception object.
    """
    with pytest.raises(WebsocketUrlError) as info:
        await muxws.connect(url)

    cause = info.value.__cause__
    assert isinstance(cause, underlying), f"__cause__ is {cause!r}"
    assert str(cause) in str(info.value), "the library's own words are quoted, not paraphrased"


async def test_a_url_containing_the_refusal_status_is_not_a_codec_mismatch():
    """The layering, and the defect it exists to prevent (WSM-ERR-016, WSM-CDC-024).

    `_looks_like_a_refused_handshake` falls back to matching `HTTP 400` in the exception's prose and
    an `InvalidURI` quotes the offending URL, so a URL check made inside the dial closure reports
    `connect("ws:/HTTP 400")` as a `CodecMismatch` on a connection that was never made.

    This is what makes "before the dial, not inside the failed-dial handler" normative rather than
    advisory: moving the check into `dial()`'s `except` leaves every other test in this file green and
    fails only this one. `not isinstance` is asserted as well as the positive class, because
    `CodecMismatch` and `WebsocketUrlError` are unrelated branches of the tree.
    """
    with pytest.raises(WebsocketUrlError) as info:
        await muxws.connect("ws:/HTTP 400")

    assert not isinstance(info.value, CodecMismatch)


@requires_af_unix
async def test_a_ws_unix_url_with_an_unparseable_authority_is_a_transport_url_error():
    """The `ws+unix:` arm goes through the same URL check, on the URI it synthesises.

    A `ws+unix:` URL carries an optional authority, which becomes the `Host` header of a handshake
    that is still HTTP; `parse_unix_url` copies it through without looking at it, so a port that is
    not a number survives the grammar and dies in `websockets` when the *logical* URI is parsed,
    which without this check reaches the caller as a bare `ValueError` naming neither muxws nor the
    URL. Checking `unix.uri` rather than `url` puts it under the same base as the TCP shapes, so
    `except TransportUrlError` covers a mistyped address over either transport, and the message names
    both spellings: the caller typed one of them and the diagnostic is about the other.
    """
    with pytest.raises(WebsocketUrlError) as info:
        await muxws.connect("ws+unix://host:notaport/tmp/p.sock:/r")

    assert isinstance(info.value, TransportUrlError)
    message = str(info.value)
    assert "ws+unix://host:notaport/tmp/p.sock:/r" in message, "the URL the caller typed"
    assert "ws://host:notaport/r" in message, "and the logical URI the handshake would have asked for"


class _RefuseWebsockets:
    """A `sys.meta_path` finder that makes `import websockets` fail without uninstalling anything.

    Raising from `find_spec` rather than returning `None` produces the `ModuleNotFoundError` an absent
    package produces, and it covers the submodules too: `websockets.uri` and
    `websockets.asyncio.client` are imported by name elsewhere in this transport, so hiding only the
    top-level package would leave a half-usable dependency no real machine has.
    """

    def find_spec(self, name: str, path: Any = None, target: Any = None) -> None:
        _ = path, target
        if name == "websockets" or name.startswith("websockets."):
            raise ModuleNotFoundError(f"No module named {name!r}", name=name)
        return None


#: The name of the transitive dependency a partially-installed `websockets` is missing. Any name will
#: do; what matters is only that it is *not* `websockets`, because that difference is the whole of
#: what `require_websockets` narrows on (WSM-ERR-016).
LOST_TRANSITIVE_DEPENDENCY = "websockets_internal_helper_that_does_not_exist"


class _BreakWebsockets:
    """A finder that makes `websockets` present but unimportable - the broken install, not the absent one.

    `find_spec` raises a `ModuleNotFoundError` naming a *different* module, which is what an
    interrupted install or a version skew inside the package really produces: the package is found and
    executing its `__init__` reaches for something that is gone. `_RefuseWebsockets` raises for the
    same statement with `name="websockets"`, and those two names are the only thing telling the two
    situations apart, which is exactly why this test exists.
    """

    def find_spec(self, name: str, path: Any = None, target: Any = None) -> None:
        _ = path, target
        if name == "websockets" or name.startswith("websockets."):
            missing = LOST_TRANSITIVE_DEPENDENCY
            raise ModuleNotFoundError(f"No module named {missing!r}", name=missing)
        return None


@contextmanager
def _websockets_import_broken_by(blocker: Any) -> Iterator[None]:
    """Install `blocker` on `sys.meta_path` for the body, and leave the interpreter exactly as found.

    Every `websockets` module is taken out of `sys.modules` as well as blocked, because an import of
    an already-imported package never reaches a finder at all and the fixture would then prove
    nothing. Both halves are put back in `finally`: the module objects are restored by identity, so
    the classes the rest of this file already holds - `websockets.serve`, `unix_serve`, `InvalidURI` -
    are the same objects afterwards as before, and a test ordered after this one still dials a real
    socket. A fixture that leaked its blocker would take every remaining test in the session with it.
    """
    saved = {name: module for name, module in sys.modules.items() if name.partition(".")[0] == "websockets"}
    for name in saved:
        del sys.modules[name]
    sys.meta_path.insert(0, blocker)
    try:
        yield
    finally:
        sys.meta_path.remove(blocker)
        sys.modules.update(saved)


@pytest.fixture
def websockets_uninstalled() -> Iterator[None]:
    """Make this interpreter look like one where `pip install muxws[websockets]` was never run."""
    with _websockets_import_broken_by(_RefuseWebsockets()):
        yield


@pytest.fixture
def websockets_installed_but_broken() -> Iterator[None]:
    """Make this interpreter look like one where `websockets` is installed and does not import."""
    with _websockets_import_broken_by(_BreakWebsockets()):
        yield


@pytest.mark.parametrize(
    "url",
    [
        pytest.param("ws://127.0.0.1:9/x", id="over-tcp"),
        # Guarded, and the TCP row deliberately is not. `_websocket_dialer` parses the `ws+unix:` URL
        # before it checks the dependency (`api.py`, and the ordering there is load-bearing for a
        # different reason), so on a platform with no `AF_UNIX` this URL raises
        # `UnixSocketsUnsupportedError` and never reaches `require_websockets` at all. The TCP row
        # must keep running everywhere, because it is the one that proves the class exists on a
        # platform that cannot dial a socket file.
        pytest.param("ws+unix:///tmp/muxws-absent.sock:/ws", id="over-a-socket-file", marks=requires_af_unix),
    ],
)
async def test_a_missing_websockets_package_names_the_extra_that_installs_it(
    websockets_uninstalled: None,
    url: str,
):
    """WSM-ERR-016: a missing optional dependency is a `TransportUnsupportedError` with the remedy.

    A bare `ModuleNotFoundError: No module named 'websockets'` out of a `connect()` names neither the
    library that needed it nor the command that fixes it, and it is not a `MuxwsError`, so an
    application that handles every muxws failure in one place sees it as a crash. GAPS.md records the
    same shape one layer down: an install with no WebSocket implementation answered every upgrade 404
    and was read as a muxws defect for want of a message naming the package.

    Both arms are here because they share the dependency and must therefore share the class: a
    `ws+unix:` dial fails on the identical `import websockets` a `ws://` dial does, and inventing a
    second class for it would tell the reader there were two things to install. `RuntimeError` rather
    than `ValueError` is the other half of the sentence - the URL is fine and no retry will help.
    """
    _ = websockets_uninstalled

    with pytest.raises(WebsocketsNotInstalledError) as info:
        await muxws.connect(url)

    assert isinstance(info.value, TransportUnsupportedError)
    assert isinstance(info.value, MuxwsError)
    assert isinstance(info.value, RuntimeError)
    assert "pip install muxws[websockets]" in str(info.value)
    assert not isinstance(info.value, TransportUrlError), "the address was never the problem"


async def test_a_websockets_that_is_installed_but_broken_keeps_its_own_import_error(
    websockets_installed_but_broken: None,
):
    """WSM-ERR-016: an import failure that is not the dependency being absent is re-raised untouched.

    The control on the test above: a handler that catches every `ImportError` answers a corrupt
    install with `WebsocketsNotInstalledError` and the remedy `pip install muxws[websockets]`, which
    tells a reader who already has the package to install it again and swallows the
    `ModuleNotFoundError` naming the module that is really missing.

    The twin of `ts/transport-errors.spec.ts` *"rethrows a broken install untouched, so it is never
    reported as an absent one"*, which narrows on `ERR_MODULE_NOT_FOUND` for the same reason.
    """
    _ = websockets_installed_but_broken

    with pytest.raises(ModuleNotFoundError) as info:
        await muxws.connect("ws://127.0.0.1:9/x")

    assert info.value.name == LOST_TRANSITIVE_DEPENDENCY, "the module that is actually missing"
    assert not isinstance(info.value, MuxwsError), "muxws must not claim a failure it cannot remedy"
    assert INSTALL_HINT not in str(info.value), "the package is installed; suggesting the install is wrong"


def test_this_transport_module_still_imports_with_its_dependency_blocked(websockets_uninstalled: None):
    """`except WebsocketsNotInstalledError` must not itself raise the `ImportError` it replaces.

    The class only helps a caller who can name it, and naming it means importing this module on the
    machine that has no `websockets` - so the module must have no `websockets` at import time, which
    is also what WSM-PKG-002 asks of every adapter. The module is executed from its own file under a
    throwaway name rather than reloaded in place, because rebinding the real module's classes
    mid-session would leave `api.py` holding a `WebsocketsNotInstalledError` that no longer matches
    the one this file imported.
    """
    _ = websockets_uninstalled
    source = Path(str(sys.modules[WebsocketsSocket.__module__].__file__))

    spec = importlib.util.spec_from_file_location("muxws_websockets_probe", source)
    assert spec is not None
    assert spec.loader is not None
    module: ModuleType = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert issubclass(module.WebsocketsNotInstalledError, TransportUnsupportedError)
