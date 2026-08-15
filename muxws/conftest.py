"""Shared fixtures: a pair of peers wired to each other in memory, and a way to read the wire."""

from __future__ import annotations

import asyncio

from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest

from muxws.codecs.json_ import JsonCodec
from muxws.errors import ResetCode
from muxws.frames import Frame
from muxws.peer import Peer, StreamHandler
from muxws.reconnect import ConnectionLoop, Hello, Reconnect
from muxws.stream import Stream
from muxws.transports.memory import memory_pair, MemorySocket


class Pair:
    """A dialer and an acceptor, each serving, plus the frames each put on the wire."""

    def __init__(self, dialer: Peer, acceptor: Peer, dialer_socket: MemorySocket, acceptor_socket: MemorySocket):
        self.dialer = dialer
        self.acceptor = acceptor
        self.dialer_socket = dialer_socket
        self.acceptor_socket = acceptor_socket
        self._tasks: list[asyncio.Task[None]] = []

    def start(self) -> None:
        self._tasks = [asyncio.create_task(self.dialer.serve()), asyncio.create_task(self.acceptor.serve())]

    async def settle(self, rounds: int = 12) -> None:
        """Let both read loops and both writers reach quiescence."""
        for _ in range(rounds):
            await asyncio.sleep(0)

    def sent_by(self, who: str) -> list[Frame]:
        socket = self.dialer_socket if who == "dialer" else self.acceptor_socket
        codec = JsonCodec()
        return [codec.decode(message) for message in socket.sent]

    def frames_of_type(self, who: str, frame_type: str) -> list[Frame]:
        return [frame for frame in self.sent_by(who) if frame.type == frame_type]

    async def stop(self) -> None:
        await self.dialer_socket.drop()
        await self.settle()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            # A cancelled serve() is the expected end of a test; anything it raises on the way out
            # is teardown noise, not a result.
            await asyncio.gather(task, return_exceptions=True)


@pytest.fixture
def make_pair() -> Callable[..., Pair]:
    """Build a peer pair without starting it, so a test can register handlers first."""

    def build(**peer_options: Any) -> Pair:
        left, right = memory_pair()
        codec = JsonCodec()
        dialer = Peer(left, codec=codec, is_dialer=True, **peer_options)
        acceptor = Peer(right, codec=codec, is_dialer=False, **peer_options)
        return Pair(dialer, acceptor, left, right)

    return build


@pytest.fixture
async def pair(make_pair: Callable[..., Pair]) -> AsyncIterator[Pair]:
    """A started pair, torn down at the end of the test."""
    built = make_pair()
    built.start()
    yield built
    await built.stop()


class Lone:
    """One peer, fed frames from the wire by hand.

    A live pair cannot test a receiver's reaction to a frame no correct sender would produce: the
    counterpart peer sees the answer, cannot account for it, and kills the connection - correctly,
    and entirely beside the point. Injecting into a peer with no counterpart is how the conformance
    runner does it.
    """

    def __init__(self, peer: Peer, socket: MemorySocket, codec: JsonCodec) -> None:
        self.peer = peer
        self.socket = socket
        self.codec = codec
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self.peer.serve())

    def inject(self, frame: Frame | str | bytes) -> None:
        self.socket.inject(frame if isinstance(frame, (str, bytes)) else self.codec.encode(frame))

    async def settle(self, rounds: int = 12) -> None:
        for _ in range(rounds):
            await asyncio.sleep(0)

    def sent(self) -> list[Frame]:
        return [self.codec.decode(message) for message in self.socket.sent]

    def frames_of_type(self, frame_type: str) -> list[Frame]:
        return [frame for frame in self.sent() if frame.type == frame_type]

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)


@pytest.fixture
def make_lone() -> Callable[..., Lone]:
    """An acceptor with no counterpart, so injected frames cannot confuse a real peer."""

    def build(**peer_options: Any) -> Lone:
        _, socket = memory_pair()
        codec = JsonCodec()
        peer = Peer(socket, codec=codec, is_dialer=False, **peer_options)
        return Lone(peer, socket, codec)

    return build


class DialableServer:
    """A fake server the reconnect driver can actually dial (§7).

    Every `dial()` builds a fresh `memory_pair()`, stands an acceptor `Peer` up over one end, starts
    its `serve()` task and returns the other end - so `server.dial` is a `Dial` callable a
    `ConnectionLoop` can be constructed with directly. Each connection is a *new* acceptor peer,
    which is the acceptor-side truth of a reconnect (WSM-RCN-033): nothing carries forward.

    It can be told to refuse the next dial, and to answer the hello in each of the four ways the
    driver has to survive - acknowledge it, reset it, never answer it, or drop the socket while it is
    outstanding - because those are the branches of WSM-RCN-026 and of WSM-RCN-004's named test.

    Everything the dialer put on the wire is kept per connection, so "three drops replay
    byte-identical hellos" (WSM-RCN-027) can be asserted on the raw messages rather than on decoded
    objects that would compare equal after a mutation the encoder happened to smooth over.
    """

    def __init__(self) -> None:
        self.codec = JsonCodec()
        #: A handler a test supplies. When set it replaces `on_hello` entirely.
        self.handler: StreamHandler | None = None
        #: How the built-in handler answers a stream: `"ack"` acknowledges it by returning
        #: (WSM-RCN-022), `"reset"` resets it, `"hang"` never answers, `"drop"` kills the socket with
        #: the hello outstanding.
        self.on_hello = "ack"
        self.acceptors: list[Peer] = []
        #: The dialer-side socket of each connection, in dial order. `.sent` is the raw wire.
        self.sockets: list[MemorySocket] = []
        #: Every payload every acceptor received, in arrival order.
        self.received: list[Any] = []
        self.dials = 0
        self.refusals = 0
        self._refuse = 0
        self._tasks: list[asyncio.Task[None]] = []

    # ------------------------------------------------------------------ the dial

    def refuse_next(self, count: int = 1) -> None:
        """The next `count` dials fail before any socket exists - not a socket loss (WSM-RCN-040)."""
        self._refuse += count

    async def dial(self) -> MemorySocket:
        self.dials += 1
        if self._refuse > 0:
            self._refuse -= 1
            self.refusals += 1
            raise ConnectionRefusedError(f"nothing is listening (dial {self.dials})")

        dialer_side, acceptor_side = memory_pair()
        acceptor = Peer(acceptor_side, codec=self.codec, is_dialer=False)
        acceptor.on_stream(self._handle)
        self.acceptors.append(acceptor)
        self.sockets.append(dialer_side)
        self._tasks.append(asyncio.create_task(acceptor.serve()))
        return dialer_side

    async def driver(
        self,
        *,
        options: Reconnect | None = None,
        hello: Hello | None = None,
        **loop_options: Any,
    ) -> tuple[Peer, ConnectionLoop]:
        """The first connection and its driver, unestablished: `connect()`'s first three steps.

        `establish()` is deliberately left to the caller. It is the step that raises (WSM-RCN-006),
        and a fixture that called it would hide the one thing several of these tests are about.

        The defaults are what a driver test wants: a hundredth of a second of backoff with no jitter,
        so waiting three attempts out costs milliseconds rather than seconds, and no heartbeat unless
        the test asks for one - a heartbeat nobody asked for would put `ping` frames on a wire other
        tests read.
        """
        socket = await self.dial()
        peer = Peer(socket, codec=self.codec, is_dialer=True)
        loop_options.setdefault("ping_interval", 0.0)
        loop = ConnectionLoop(
            peer,
            self.dial,
            options=options or Reconnect(initial_delay=0.01, jitter=0.0),
            hello=hello or Hello(),
            **loop_options,
        )
        return peer, loop

    async def _handle(self, payload: Any, stream: Stream) -> None:
        self.received.append(payload)
        if self.handler is not None:
            result = self.handler(payload, stream)
            if asyncio.iscoroutine(result):
                await result
            return
        if self.on_hello == "ack":
            # Returning is the acknowledgement: WSM-STM-035 ends the stream implicitly, and
            # WSM-RCN-022 says that is all an acceptor has to do.
            return
        if self.on_hello == "reset":
            await stream.reset(ResetCode.REFUSED, "the hello was refused")
            return
        if self.on_hello == "drop":
            await self.sockets[-1].drop()
            return
        await stream.closed.wait()

    # ------------------------------------------------------------------ reading the wire

    async def drop(self) -> None:
        """Kill the current socket the way a server going down kills one: no close frame."""
        await self.sockets[-1].drop()

    async def go_silent(self) -> None:
        """Stop reading on the current connection without closing it.

        The socket stays up and every frame the dialer sends is swallowed, `ping` included - which is
        the only thing a heartbeat can be tested against (WSM-RCN-011). A closed socket would be
        detected by the read loop instead, and would prove nothing about the heartbeat.
        """
        task = self._tasks[-1]
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    def raw(self, index: int) -> list[str | bytes]:
        """Every message the dialer put on connection `index`, exactly as it went out."""
        return list(self.sockets[index].sent)

    def frames(self, index: int) -> list[Frame]:
        return [self.codec.decode(message) for message in self.sockets[index].sent]

    async def aclose(self) -> None:
        for socket in self.sockets:
            await socket.drop()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            # A cancelled serve() is the expected end of a test; anything it raises on the way out is
            # teardown noise, not a result.
            await asyncio.gather(task, return_exceptions=True)


@pytest.fixture
async def dialable_server() -> AsyncIterator[DialableServer]:
    """A fake server every reconnect-driver test dials. Torn down at the end of the test."""
    server = DialableServer()
    yield server
    await server.aclose()
