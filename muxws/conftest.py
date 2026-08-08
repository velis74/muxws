"""Shared fixtures: a pair of peers wired to each other in memory, and a way to read the wire."""

from __future__ import annotations

import asyncio

from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest

from muxws.codecs.json_ import JsonCodec
from muxws.frames import Frame
from muxws.peer import Peer
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
    runner does it, and it is what these tests need too.
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
