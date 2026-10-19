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
    """A running acceptor and its count of completed handshakes.

    `handshakes` is incremented by the connection handler, which `websockets` calls only after the
    upgrade has been answered 101. A refusal leaves it at zero: WSM-CDC-022 forbids completing the
    handshake and closing afterwards, so reaching the handler already violates the rule.
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

    WSM-CDC-022 specifies the status code, which a muxws dial does not expose: `connect()` falls back
    to the WSM-CDC-028 check on the open socket and reports `CodecMismatch` whether the acceptor
    answered 400 or 101 with no subprotocol. The request is therefore sent at the HTTP level.
    """
    parts = urlsplit(url)
    host, port = parts.hostname or "127.0.0.1", parts.port or 80
    reader, writer = await asyncio.open_connection(host, port)
    return await _send_upgrade_request(reader, writer, host=f"{host}:{port}", request_target="/", offered=offered)


async def _send_upgrade_request(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    host: str,
    request_target: str,
    offered: str,
) -> tuple[int, dict[str, str]]:
    """The upgrade request itself, byte for byte, over whatever stream pair the caller opened.

    Shared with the Unix-socket variant, which checks that WSM-CDC-022 is answered identically over a
    filesystem socket; one request builder guarantees both send the same request.
    `asyncio.open_connection` and `asyncio.open_unix_connection` return the same pair of objects, so
    only the transport differs between the callers.
    """
    try:
        writer.write(
            f"GET {request_target} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            # A fixed key: nothing here verifies `Sec-WebSocket-Accept`, and a constant makes the
            # request byte-identical between runs.
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
    """WSM-CDC-027: the handshake hook is for transports that complete the handshake before the handler runs."""
    peer = await muxws.connect(server)
    try:
        assert await peer.request({"action": "echo", "n": 1}) == {"echo": {"action": "echo", "n": 1}}
    finally:
        await peer._socket.close()


async def test_dialer_offers_the_muxws_entry_first_on_a_real_socket(server: str):
    """WSM-CDC-020: the muxws subprotocol is offered first, also when the application appended its own entries."""
    peer = await muxws.connect(server, subprotocols=["bearer.abc123"])
    try:
        assert await peer.request({"action": "echo"}) == {"echo": {"action": "echo"}}
    finally:
        await peer._socket.close()


async def test_a_mismatched_codec_offer_is_answered_with_http_400(acceptor: Acceptor):
    """WSM-CDC-022 **(spec)**: refuse the upgrade - no subprotocol, and status 400.

    Answering 101 with no `Sec-WebSocket-Protocol` and leaving the mismatch to the post-handshake
    check is the "complete the handshake and close afterwards" the rule forbids when the transport
    allows refusing the upgrade; `websockets` allows it.
    """
    status, headers = await _upgrade(acceptor.url, "muxws.v1.msgpack")

    assert status == 400
    assert "sec-websocket-protocol" not in headers
    assert acceptor.handshakes == 0, "a refused upgrade must never reach the connection handler"


async def test_a_matching_codec_offer_is_answered_with_http_101(acceptor: Acceptor):
    """WSM-CDC-022, matching case: the offered value is returned as the selected subprotocol.

    This is the control for the 400 test above, which an acceptor refusing every upgrade would pass."""
    status, headers = await _upgrade(acceptor.url, "muxws.v1.json")

    assert status == 101
    assert headers["sec-websocket-protocol"] == "muxws.v1.json"


@pytest.mark.parametrize(
    "offered",
    ["muxws.v2.json", "bearer.abc123"],
    ids=["a-later-generation", "no-muxws-entry-at-all"],
)
async def test_an_unusable_offer_is_answered_with_http_400(acceptor: Acceptor, offered: str):
    """WSM-CDC-025 and WSM-CDC-022's third case: an unsupported version is refused at the handshake.

    The acceptor rejects a protocol version it does not implement during the handshake, so the version
    component of the subprotocol name determines whether a connection is made (WSM-CON-009).
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

    The rule requires that no frame is exchanged. Both peers run in this process, so the
    `muxws.frames` logger records every frame either sends or receives, and `hello=` ensures a dialer
    that got a socket sends a frame immediately (WSM-RCN-021).

    The last assertion covers WSM-ERR-016's layering: a refusal comes from a server, so the URL check
    before the dial accepted the URL. A URL check that rejected a dialable address would raise
    `TransportUrlError` here instead of `CodecMismatch`.
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

    # The frame count is asserted first: it lists the frames that were sent, which `handshakes` does
    # not.
    exchanged = [record.getMessage() for record in caplog.records if record.name == "muxws.frames"]
    assert exchanged == [], f"a refused handshake exchanged {len(exchanged)} muxws frame(s)"
    assert acceptor.handshakes == 0, "the upgrade must have been refused, not completed and closed"
    assert not isinstance(info.value, TransportUrlError), "a refused upgrade is not a bad address"


async def test_a_101_that_negotiated_something_else_is_caught_on_the_open_socket():
    """WSM-CDC-028: the post-handshake check catches the case the handshake cannot refuse.

    This acceptor selects the application's own entry instead of the muxws one, and `websockets`
    completes the handshake because that value was offered (WSM-CDC-021). The upgrade is valid from
    the dialer's side, so the check on the open socket is the only detection, and it is the only one
    a browser has. It does not replace the 400: it reports a mismatch in both cases, so a dialer
    cannot tell whether the acceptor sent 400.
    """

    def select_the_wrong_one(_connection: Any, _subprotocols: list[str]) -> str:
        return "bearer.abc123"

    async def handle(connection: Any) -> None:
        await connection.wait_closed()

    async with websockets.serve(handle, "127.0.0.1", 0, select_subprotocol=select_the_wrong_one) as service:
        url = f"ws://127.0.0.1:{service.sockets[0].getsockname()[1]}"
        with pytest.raises(CodecMismatch):
            await muxws.connect(url, subprotocols=["bearer.abc123"])


async def test_accepting_a_websockets_connection_with_another_subprotocol_closes_it_with_1008(
    caplog: pytest.LogCaptureFixture,
):
    """WSM-CDC-028/029: `accept()` of an open `websockets` connection whose negotiated subprotocol is
    not `muxws.v1.<codec>` logs one `muxws.codec` ERROR naming the negotiated and the expected
    subprotocol and both environment variables, closes the socket with 1008 and raises `CodecMismatch`."""
    raised: list[BaseException] = []

    async def handle(connection: Any) -> None:
        try:
            await muxws.accept(connection, codec=muxws.get_codec("json"))
        except CodecMismatch as exc:
            raised.append(exc)
        await connection.wait_closed()

    # No `select_subprotocol` hook: the handshake completes with no subprotocol selected.
    async with websockets.serve(handle, "127.0.0.1", 0) as service:
        url = f"ws://127.0.0.1:{service.sockets[0].getsockname()[1]}"
        with caplog.at_level(logging.ERROR, logger="muxws.codec"):
            async with websockets.connect(url, subprotocols=["muxws.v1.json"]) as connection:  # type: ignore[list-item]
                await asyncio.wait_for(connection.wait_closed(), timeout=5)

    assert connection.close_code == 1008
    assert len(raised) == 1
    errors = [record for record in caplog.records if record.levelno == logging.ERROR]
    assert [record.name for record in errors] == ["muxws.codec"]
    message = errors[0].getMessage()
    assert "None" in message
    assert "'muxws.v1.json'" in message
    assert "VITE_MUXWS_CODEC" in message
    assert "MUXWS_CODEC on the server" in message


class _HandshakeCounter:
    """A `websockets` acceptor that counts completed handshakes; `closed` is set when a connection closes."""

    def __init__(self) -> None:
        self.url = ""
        self.handshakes = 0
        self.closed = asyncio.Event()


@pytest.fixture
async def counter() -> AsyncIterator[_HandshakeCounter]:
    running = _HandshakeCounter()

    async def handle(connection: Any) -> None:
        running.handshakes += 1
        await connection.wait_closed()
        running.closed.set()

    async with websockets.serve(handle, "127.0.0.1", 0, select_subprotocol=muxws.select_subprotocol) as service:
        running.url = f"ws://127.0.0.1:{service.sockets[0].getsockname()[1]}"
        yield running


async def test_connect_rejects_a_frame_cap_that_cannot_hold_a_frame_before_dialing(counter: _HandshakeCounter):
    """WSM-FRG-034: `connect()` raises `ProtocolError` for an unusable `max_frame_bytes` without dialing."""
    with pytest.raises(muxws.ProtocolError, match="WSM-FRG-034"):
        await muxws.connect(counter.url, max_frame_bytes=8)
    assert counter.handshakes == 0


async def test_connect_rejects_a_hello_that_cannot_be_copied_before_dialing(counter: _HandshakeCounter):
    """WSM-RCN-027: `connect()` raises the `deepcopy` error for an uncopyable hello without dialing."""
    with pytest.raises(TypeError):
        await muxws.connect(counter.url, hello={"pending": (value for value in ())})
    assert counter.handshakes == 0


async def test_connect_closes_the_dialled_socket_when_building_the_peer_fails(
    counter: _HandshakeCounter, monkeypatch: pytest.MonkeyPatch
):
    """WSM-RCN-006: an error raised after the dial and before the hello reaches the caller, and the
    dialled socket is closed."""

    def refuse(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("peer construction failed")

    monkeypatch.setattr(muxws.api, "Peer", refuse)
    with pytest.raises(RuntimeError, match="peer construction failed"):
        await muxws.connect(counter.url)
    assert counter.handshakes == 1
    await asyncio.wait_for(counter.closed.wait(), timeout=5)


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
    """WSM-CDC-024 applies only to a refused upgrade; a closed port raises `OSError`.

    Matching the status in the exception text (`"400" in str(exc)`) also matches
    `[Errno 111] Connect call failed ('127.0.0.1', 14000)`, which would be reported as a codec
    mismatch. The status is read from `InvalidStatus.response`, so a closed port stays an `OSError`.
    """
    port = _a_closed_port_whose_number_contains_400()

    # `CodecMismatch` is not an `OSError`, so the expected type is the assertion.
    with pytest.raises(ConnectionRefusedError):
        await muxws.connect(f"ws://127.0.0.1:{port}")


async def test_real_socket_carries_unary_streaming_notify_and_cancel(server: str):
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
# `ws+unix:///path/to.sock:/route` dials a filesystem socket. The protocol is the same; each test
# repeats one above over AF_UNIX. The transport-specific parts are the request target and `Host`
# (which a filesystem path does not supply), the subprotocol offer, the translation of the 400, and
# the socket file a reconnect re-opens.

#: AF_UNIX does not exist on Windows, so every test that opens a socket file carries this. The URL
#: grammar is tested in `unix_test.py`, which needs no socket and does not skip, so the portability
#: guard is tested on Linux as well.
requires_af_unix = pytest.mark.skipif(
    not hasattr(socketlib, "AF_UNIX"),
    reason="this platform has no AF_UNIX, so no ws+unix: URL can be dialled here",
)


@pytest.fixture
async def unix_acceptor() -> AsyncIterator[Acceptor]:
    """The `acceptor` fixture listening on a socket file instead of an ephemeral port.

    Uses `tempfile.TemporaryDirectory`, not pytest's `tmp_path`: `sun_path` holds about 108 bytes, and
    pytest's per-test directory takes about 74 of them on Linux and about 120 under a macOS `TMPDIR`,
    where binding fails with `AF_UNIX path too long`.
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
    """`_upgrade` over a socket file: the same request, sent through `open_unix_connection`.

    The URL is resolved with the library's own parser, not taken from the fixture, so the test also
    checks that the parsed URL names the file the acceptor listens on. A wrong parse has nothing to
    connect to and fails the test.
    """
    target = parse_unix_url(url)
    assert target is not None, f"{url!r} must parse as a ws+unix: URL"
    logical = urlsplit(target.uri)
    reader, writer = await asyncio.open_unix_connection(target.path)
    return await _send_upgrade_request(
        reader,
        writer,
        host=logical.netloc,
        request_target=logical.path,
        offered=offered,
    )


@requires_af_unix
async def test_a_mismatched_codec_offer_over_a_unix_socket_is_answered_with_http_400(unix_acceptor: Acceptor):
    """WSM-CDC-022 over AF_UNIX, checked at the HTTP level.

    The refusal is HTTP, which runs over a socket file as over a TCP port. An implementation that
    left the mismatch to the post-handshake check for local sockets would complete the handshake and
    close afterwards. `handshakes == 0` asserts that it did not.
    """
    status, headers = await _upgrade_over_unix(unix_acceptor.url, "muxws.v1.msgpack")

    assert status == 400
    assert "sec-websocket-protocol" not in headers
    assert unix_acceptor.handshakes == 0, "a refused upgrade must never reach the connection handler"


@requires_af_unix
async def test_a_matching_codec_offer_over_a_unix_socket_is_answered_with_http_101(unix_acceptor: Acceptor):
    """Matching case over AF_UNIX: the control for the 400 test above.

    It also checks that the request target and `Host` this transport derives from the URL are
    accepted by a real acceptor: `websockets` answers 400 to a malformed request line as well as to
    an unsupported codec.
    """
    status, headers = await _upgrade_over_unix(unix_acceptor.url, "muxws.v1.json")

    assert status == 101
    assert headers["sec-websocket-protocol"] == "muxws.v1.json"


@requires_af_unix
async def test_a_unix_socket_carries_every_call_shape(unix_server: str):
    """Unary, streaming response, notify, a remote failure and cancel, over a socket file.

    `connect()` with a `ws+unix:` URL returns a peer that behaves like a TCP one for every
    application call. `ping()` is included because the heartbeat detects dead sockets
    (WSM-RCN-010/011) and runs over the frame layer, which uses the `WebsocketsSocket` it was given.
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
    """WSM-CDC-022/024 through the public entry point: a 400 over AF_UNIX raises `CodecMismatch`.

    The dialer translates the 400 into `CodecMismatch` in one `except` around the dial. A Unix dial
    in a separate `try` would raise `InvalidStatus`, a `websockets` type, instead. No other test in
    the suite checks this.

    The empty frame log is asserted as in the TCP variant: with `hello=`, a dialer that got a socket
    sends a frame immediately.
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
    """A missing socket file raises `FileNotFoundError` naming the path, not `CodecMismatch`.

    A missing socket file is the most common failure of this transport: the service has not started,
    or writes its socket to another path.
    """
    with tempfile.TemporaryDirectory(prefix="muxws-") as directory:
        url = f"ws+unix://{Path(directory) / 'absent.sock'}:/ws"

        # `CodecMismatch` is not an `OSError`, so the expected class is the assertion.
        with pytest.raises(FileNotFoundError):
            await muxws.connect(url)


@requires_af_unix
async def test_a_peer_reconnects_across_a_re_created_socket_file():
    """WSM-RCN-020/030 over AF_UNIX: the dial closure re-opens the path and connects to the new inode.

    `unix_serve` leaves the socket file behind when it closes, and the next server unlinks it and
    binds a new inode at the same path. A dialer that kept a file descriptor or inode from the first
    dial would reconnect to a socket with no listener and hang. The URL is parsed outside the dial
    closure and only the path is passed into it, so every attempt re-opens the path.

    The hello is asserted twice: it is replayed verbatim on the new connection (WSM-RCN-020), and
    `on_reconnect` fires after it is acknowledged, not before (WSM-RCN-030).
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
# The tests below open no socket. They cover two failures: an address `websockets` cannot parse,
# and `websockets` not being installed. Each raises a `MuxwsError` under a shared transport base, so
# one `except MuxwsError` around `connect()` catches a malformed address for either scheme.


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

    Both schemes are tested, so an implementation that handles only `ws:` fails. The last two rows:
    `parse_uri` raises a plain `ValueError` from `urllib.parse` for a bad port, and `InvalidURI` is not
    a `ValueError`, so `except InvalidURI` alone does not translate it; a string that is not a URL is
    what a configuration file contains when a variable was not substituted.

    Each of the three `isinstance` assertions covers one kind of caller: `MuxwsError` for an
    application-wide handler, `ValueError` for a caller that catches URL errors without knowing muxws,
    and `TransportUrlError` for a caller that handles a wrong address for any transport without
    importing this module (which does not import on a machine without `websockets`).
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
    """The original exception is chained with `from exc`, so the traceback shows the specific cause.

    `WebsocketUrlError` reports that a URL was refused; the `websockets` exception says whether the
    hostname was missing, the scheme was wrong or the port was not a number. The message quotes the
    `websockets` message, and `__cause__` holds the original exception.
    """
    with pytest.raises(WebsocketUrlError) as info:
        await muxws.connect(url)

    cause = info.value.__cause__
    assert isinstance(cause, underlying), f"__cause__ is {cause!r}"
    assert str(cause) in str(info.value), "the message does not contain the websockets message"


async def test_a_url_containing_the_refusal_status_is_not_a_codec_mismatch():
    """The URL check runs before the dial, not in the failed-dial handler (WSM-ERR-016, WSM-CDC-024).

    `_looks_like_a_refused_handshake` falls back to matching `HTTP 400` in the exception text, and an
    `InvalidURI` quotes the URL, so a URL check inside the dial closure would report
    `connect("ws:/HTTP 400")` as a `CodecMismatch` for a connection that was never made.

    Only this test fails if the check is moved into `dial()`'s `except`. `not isinstance` is asserted
    as well as the expected class, because `CodecMismatch` and `WebsocketUrlError` are unrelated
    branches of the hierarchy.
    """
    with pytest.raises(WebsocketUrlError) as info:
        await muxws.connect("ws:/HTTP 400")

    assert not isinstance(info.value, CodecMismatch)


@requires_af_unix
async def test_a_ws_unix_url_with_an_unparseable_authority_is_a_transport_url_error():
    """The `ws+unix:` arm goes through the same URL check, on the URI it synthesises.

    A `ws+unix:` URL carries an optional authority, which becomes the `Host` header of the HTTP
    handshake. `parse_unix_url` copies it unchecked, so a non-numeric port passes the grammar and fails
    in `websockets` when the logical URI is parsed; without this check the caller gets a bare
    `ValueError` that names neither muxws nor the URL. Checking `unix.uri` puts the error under the
    same base class as the TCP cases, so `except TransportUrlError` covers a wrong address over either
    transport. The message names both the `ws+unix:` URL and the synthesised URI.
    """
    with pytest.raises(WebsocketUrlError) as info:
        await muxws.connect("ws+unix://host:notaport/tmp/p.sock:/r")

    assert isinstance(info.value, TransportUrlError)
    message = str(info.value)
    assert "ws+unix://host:notaport/tmp/p.sock:/r" in message, "the URL the caller typed"
    assert "ws://host:notaport/r" in message, "and the logical URI the handshake would have asked for"


class _RefuseWebsockets:
    """A `sys.meta_path` finder that makes `import websockets` fail without uninstalling anything.

    Raising from `find_spec` produces the same `ModuleNotFoundError` as an absent package. The finder
    also blocks the submodules: `websockets.uri` and `websockets.asyncio.client` are imported by name
    in this transport, and blocking only the top-level package would leave them importable.
    """

    def find_spec(self, name: str, path: Any = None, target: Any = None) -> None:
        _ = path, target
        if name == "websockets" or name.startswith("websockets."):
            raise ModuleNotFoundError(f"No module named {name!r}", name=name)
        return None


#: The name of the transitive dependency a partially installed `websockets` is missing. The value is
#: arbitrary but must differ from `websockets`: `require_websockets` distinguishes the two cases by
#: the module name (WSM-ERR-016).
LOST_TRANSITIVE_DEPENDENCY = "websockets_internal_helper_that_does_not_exist"


class _BreakWebsockets:
    """A finder that makes `websockets` present but unimportable: a broken install, not an absent one.

    `find_spec` raises a `ModuleNotFoundError` naming a different module, as an interrupted install
    or a version skew inside the package does: the package is found and its `__init__` imports a
    missing module. `_RefuseWebsockets` raises for the same import with `name="websockets"`; the
    module name is the only difference between the two cases.
    """

    def find_spec(self, name: str, path: Any = None, target: Any = None) -> None:
        _ = path, target
        if name == "websockets" or name.startswith("websockets."):
            missing = LOST_TRANSITIVE_DEPENDENCY
            raise ModuleNotFoundError(f"No module named {missing!r}", name=missing)
        return None


@contextmanager
def _websockets_import_broken_by(blocker: Any) -> Iterator[None]:
    """Install `blocker` on `sys.meta_path` for the body, and restore the interpreter state afterwards.

    Every `websockets` module is also removed from `sys.modules`, because importing an
    already-imported package does not consult the finders. Both are restored in `finally`: the module
    objects are restored by identity, so `websockets.serve`, `unix_serve` and `InvalidURI` held by
    this file are the same objects afterwards, and later tests still dial real sockets. A blocker left
    installed would break every remaining test in the session.
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
        # Guarded; the TCP row is not. `_websocket_dialer` in `api.py` parses the `ws+unix:` URL
        # before it checks the dependency, so on a platform without `AF_UNIX` this URL raises
        # `UnixSocketsUnsupportedError` and does not reach `require_websockets`. The TCP row runs on
        # every platform, including those that cannot dial a socket file.
        pytest.param("ws+unix:///tmp/muxws-absent.sock:/ws", id="over-a-socket-file", marks=requires_af_unix),
    ],
)
async def test_a_missing_websockets_package_names_the_extra_that_installs_it(
    websockets_uninstalled: None,
    url: str,
):
    """WSM-ERR-016: a missing optional dependency is a `TransportUnsupportedError` with the remedy.

    A bare `ModuleNotFoundError: No module named 'websockets'` from `connect()` names neither the
    library that needs it nor the install command, and it is not a `MuxwsError`, so an application
    that handles all muxws failures in one place does not catch it.

    Both URL schemes are tested and raise the same class, because both fail on the same
    `import websockets`. The class is a `RuntimeError`, not a `ValueError`: the URL is valid and a
    retry does not help.
    """
    _ = websockets_uninstalled

    with pytest.raises(WebsocketsNotInstalledError) as info:
        await muxws.connect(url)

    assert isinstance(info.value, TransportUnsupportedError)
    assert isinstance(info.value, MuxwsError)
    assert isinstance(info.value, RuntimeError)
    assert "pip install muxws[websockets]" in str(info.value)
    assert not isinstance(info.value, TransportUrlError), "a missing package raised TransportUrlError"


async def test_a_websockets_that_is_installed_but_broken_keeps_its_own_import_error(
    websockets_installed_but_broken: None,
):
    """WSM-ERR-016: an import failure that is not the dependency being absent is re-raised untouched.

    The control for the test above: a handler that caught every `ImportError` would report a broken
    install as `WebsocketsNotInstalledError` with the remedy `pip install muxws[websockets]`, and hide
    the `ModuleNotFoundError` naming the missing module.

    Corresponds to `ts/transport-errors.spec.ts` *"rethrows a broken install untouched, so it is never
    reported as an absent one"*, which checks `ERR_MODULE_NOT_FOUND`.
    """
    _ = websockets_installed_but_broken

    with pytest.raises(ModuleNotFoundError) as info:
        await muxws.connect("ws://127.0.0.1:9/x")

    assert info.value.name == LOST_TRANSITIVE_DEPENDENCY, "the error names the missing module"
    assert not isinstance(info.value, MuxwsError), "a broken install raised WebsocketsNotInstalledError"
    assert INSTALL_HINT not in str(info.value), "the package is installed; suggesting the install is wrong"


def test_this_transport_module_still_imports_with_its_dependency_blocked(websockets_uninstalled: None):
    """The module defining `WebsocketsNotInstalledError` imports without `websockets`.

    A caller names the class by importing this module on a machine without `websockets`, so the
    module does not import `websockets` at import time (also required of every adapter by
    WSM-PKG-002). The module is executed from its file under a temporary name, not reloaded in
    place: reloading would rebind the classes, and `api.py` would hold a `WebsocketsNotInstalledError`
    different from the one this file imported.
    """
    _ = websockets_uninstalled
    source = Path(str(sys.modules[WebsocketsSocket.__module__].__file__))

    spec = importlib.util.spec_from_file_location("muxws_websockets_probe", source)
    assert spec is not None
    assert spec.loader is not None
    module: ModuleType = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert issubclass(module.WebsocketsNotInstalledError, TransportUnsupportedError)


async def _origin_acceptor(allowed_origins: Any, *, wrap: bool) -> tuple[Any, list[BaseException | None]]:
    """A `websockets` acceptor whose handler calls `accept(allowed_origins=...)`; records each outcome."""
    outcomes: list[BaseException | None] = []

    async def handle(connection: Any) -> None:
        socket = WebsocketsSocket(connection) if wrap else connection
        try:
            peer = await muxws.accept(socket, codec=muxws.get_codec("json"), allowed_origins=allowed_origins)
        except MuxwsError as exc:
            outcomes.append(exc)
            return
        outcomes.append(None)
        peer.on_stream(_echo)
        try:
            await peer.serve()
        except muxws.ConnectionClosed:
            pass

    service = await websockets.serve(handle, "127.0.0.1", 0, select_subprotocol=muxws.select_subprotocol)
    return service, outcomes


@pytest.mark.parametrize("wrap", [False, True], ids=["connection", "websockets-socket"])
@pytest.mark.parametrize(
    ("allowed", "origin"),
    [
        (None, None),
        (None, "https://evil.test"),
        (["https://app.example.com"], "https://app.example.com"),
        (["https://app.example.com"], "https://APP.example.com:443"),
        (lambda origin: origin is None, None),
    ],
    ids=["default-no-origin", "default-any-origin", "listed", "listed-normalised", "callable-no-origin"],
)
async def test_an_allowed_origin_gets_a_serving_peer(allowed: Any, origin: str | None, wrap: bool):
    service, outcomes = await _origin_acceptor(allowed, wrap=wrap)
    async with service:
        url = f"ws://127.0.0.1:{service.sockets[0].getsockname()[1]}"
        async with websockets.connect(url, subprotocols=["muxws.v1.json"], origin=origin) as client:
            await client.send('{"type": "open", "stream": 1, "payload": {"n": 1}, "end": true}')
            reply = await asyncio.wait_for(client.recv(), timeout=5)
    assert '"echo"' in reply
    assert outcomes == [None]


@pytest.mark.parametrize("wrap", [False, True], ids=["connection", "websockets-socket"])
@pytest.mark.parametrize(
    ("allowed", "origin"),
    [
        (["https://app.example.com"], "https://evil.test"),
        (["https://app.example.com"], "http://app.example.com"),
        (["https://app.example.com"], None),
        (["https://app.example.com"], "null"),
        (lambda origin: origin == "https://app.example.com", "https://evil.test"),
    ],
    ids=["other-host", "other-scheme", "missing", "null", "callable-false"],
)
async def test_a_refused_origin_is_closed_with_1008(allowed: Any, origin: str | None, wrap: bool):
    service, outcomes = await _origin_acceptor(allowed, wrap=wrap)
    async with service:
        url = f"ws://127.0.0.1:{service.sockets[0].getsockname()[1]}"
        async with websockets.connect(url, subprotocols=["muxws.v1.json"], origin=origin) as client:
            with pytest.raises(websockets.exceptions.ConnectionClosed) as info:
                await asyncio.wait_for(client.recv(), timeout=5)
    assert info.value.rcvd is not None
    assert info.value.rcvd.code == 1008
    assert len(outcomes) == 1
    assert isinstance(outcomes[0], muxws.OriginRefused)
    assert outcomes[0].origin == origin


@pytest.mark.parametrize("wrap", [False, True], ids=["connection", "websockets-socket"])
async def test_same_origin_compares_origin_with_the_host_header(wrap: bool):
    service, outcomes = await _origin_acceptor(muxws.SameOrigin(), wrap=wrap)
    async with service:
        address = f"127.0.0.1:{service.sockets[0].getsockname()[1]}"
        async with websockets.connect(
            f"ws://{address}", subprotocols=["muxws.v1.json"], origin=f"http://{address}"
        ) as client:
            await client.send('{"type": "open", "stream": 1, "payload": {"n": 1}, "end": true}')
            assert '"echo"' in await asyncio.wait_for(client.recv(), timeout=5)
        async with websockets.connect(
            f"ws://{address}", subprotocols=["muxws.v1.json"], origin="http://evil.test"
        ) as client:
            with pytest.raises(websockets.exceptions.ConnectionClosed) as info:
                await asyncio.wait_for(client.recv(), timeout=5)
    assert info.value.rcvd is not None
    assert info.value.rcvd.code == 1008
    assert outcomes[0] is None
    assert isinstance(outcomes[1], muxws.OriginRefused)


async def test_serve_returns_without_raising_on_a_refused_origin():
    returned: list[bool] = []

    async def handle(connection: Any) -> None:
        await muxws.serve(connection, handler=_echo, codec=muxws.get_codec("json"), allowed_origins=[])
        returned.append(True)

    async with websockets.serve(handle, "127.0.0.1", 0, select_subprotocol=muxws.select_subprotocol) as service:
        url = f"ws://127.0.0.1:{service.sockets[0].getsockname()[1]}"
        async with websockets.connect(url, subprotocols=["muxws.v1.json"], origin="https://a.test") as client:
            with pytest.raises(websockets.exceptions.ConnectionClosed):
                await asyncio.wait_for(client.recv(), timeout=5)
            await asyncio.sleep(0)
    assert returned == [True]


async def test_allowed_origins_on_an_adapter_without_request_headers_is_a_protocol_error():
    from muxws.transports.memory import memory_pair

    _, acceptor_socket = memory_pair()
    with pytest.raises(muxws.ProtocolError, match="exposes none"):
        await muxws.accept(acceptor_socket, codec=muxws.get_codec("json"), allowed_origins=["https://a.test"])
    with pytest.raises(muxws.ConnectionClosed):
        await acceptor_socket.receive()
