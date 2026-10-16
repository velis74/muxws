"""The demo's backend against the in-memory transport.

The main test is `test_ticks_keep_flowing_during_an_export`. It asserts on the frames recorded by
`on_frame`, and fails if the round-robin writer is replaced by a FIFO writer.
"""

from __future__ import annotations

import asyncio
import itertools

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest

from demo.backend_python.handlers import MarketService
from demo.backend_python.market import EXPORT_ROWS, HISTORY_POINTS, Market, SYMBOLS
from muxws import Frame, MAX_FRAME_BYTES, Peer, PeerRegistry, RemoteError, StreamReset
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair, MemorySocket

#: Frames each tick stream has queued when the export's sends are released: more than the export
#: has fragments, so every tick stream has a frame to send between any two export fragments.
BACKLOG = 32


class GatedSocket:
    """A `MemorySocket` whose sends wait while `gate` is clear. Implements `SocketAdapter`.

    With the gate set, a send is the wrapped socket's send with no added suspension: `Event.wait()` on
    a set event returns without yielding. With the gate clear, the write loop blocks at its next send
    and every frame enqueued meanwhile stays in the writer, so a test can fill the writer's queues
    before the writer chooses between them.
    """

    def __init__(self, socket: MemorySocket) -> None:
        self._socket = socket
        self.gate = asyncio.Event()
        self.gate.set()

    async def send_text(self, text: str) -> None:
        await self.gate.wait()
        await self._socket.send_text(text)

    async def send_bytes(self, data: bytes) -> None:
        await self.gate.wait()
        await self._socket.send_bytes(data)

    async def receive(self) -> str | bytes:
        return await self._socket.receive()

    async def close(self, code: int = 1000, reason: str = "") -> None:
        await self._socket.close(code, reason)


class Wire:
    """A browser peer and a backend peer connected in memory, with the demo's handler on the acceptor.

    The browser side uses a plain `MemorySocket`. `Peer._write_loop` takes one event-loop turn per
    frame, so the writer interleaves streams without the transport adding suspensions; a socket double
    that slept in proportion to frame size would cause interleaving of its own. The backend's sends
    pass through a `GatedSocket`, which is open unless a test clears its gate.
    """

    def __init__(
        self,
        dialer: Peer,
        acceptor: Peer,
        service: MarketService,
        sockets: list[MemorySocket],
        backend: GatedSocket,
    ) -> None:
        self.dialer = dialer
        self.acceptor = acceptor
        self.service = service
        self.sockets = sockets
        self.backend = backend
        #: Every frame the browser saw, in order, recorded through the WSM-OBS-003 hook.
        self.seen: list[tuple[str, Frame, int]] = []
        #: The browser's market data, filled only from the pushed streams (D1).
        self.board: dict[str, Any] = {}
        #: The first row each symbol pushed, kept separately because `board` holds the latest one.
        #: The first row is the snapshot, which lets the grid be drawn before the first tick.
        self.first: dict[str, Any] = {}
        #: Every `CloseReason` passed to the browser's `on_close`.
        self.closes: list[Any] = []
        self._tasks: list[asyncio.Task[None]] = []

    async def _on_push(self, payload: Any, stream: Any) -> None:
        """The browser's own `on_stream` handler: the acceptor's push arrives here (WSM-INV-002)."""
        symbol = payload["symbol"]
        async for row in stream:
            self.first.setdefault(symbol, row)
            self.board[symbol] = row

    def start(self) -> None:
        self.dialer.on_frame(lambda direction, frame, length: self.seen.append((direction, frame, length)))
        # Registered before anything is sent: the backend pushes the board as soon as it receives the
        # hello, and a push that arrives before a handler is registered is answered with
        # `reset(REFUSED, "no on_stream handler")` (WSM-STM-033). `connect()` takes `on_stream=` as a
        # parameter for this reason.
        self.dialer.on_stream(self._on_push)
        self.dialer.on_close(self.closes.append)
        self._tasks = [asyncio.create_task(self.dialer.serve()), asyncio.create_task(self.acceptor.serve())]

    async def hello(self, **tags: Any) -> Any:
        """Send the hello `connect(hello=...)` would send, as a request so the reply can be read (D3)."""
        return await self.dialer.request({"action": "hello", "tags": tags or {"topic": "board"}})

    def received(self, direction: str = "rx") -> list[Frame]:
        return [frame for way, frame, _length in self.seen if way == direction]

    async def until_the_board_is_live(self, timeout: float = 5.0) -> None:
        """Wait until every symbol has pushed at least one price, so the export starts into a live board."""
        deadline = asyncio.get_running_loop().time() + timeout
        while len(self.board) < len(SYMBOLS):
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(f"only {len(self.board)} of {len(SYMBOLS)} tick streams started")
            await asyncio.sleep(0.005)

    def tick_streams(self) -> set[int]:
        """The ids of the streams the backend pushed, one per symbol."""
        return {stream.id for stream in self.acceptor._streams.values() if stream.local}

    async def until_backlogged(self, stream_id: int, depth: int, timeout: float = 5.0) -> None:
        """Wait until `stream_id` has a frame queued and every tick stream has `depth` frames queued.

        Used only while `backend.gate` is clear, when the writer holds every enqueued frame.
        """
        writer = self.acceptor._writer
        deadline = asyncio.get_running_loop().time() + timeout
        while writer.depth_of(stream_id) == 0 or min(writer.depth_of(lane) for lane in self.tick_streams()) < depth:
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(f"the tick streams did not queue {depth} frames each")
            await asyncio.sleep(0.005)

    async def stop(self) -> None:
        self.backend.gate.set()
        self.service.forget(self.acceptor)
        for socket in self.sockets:
            await socket.drop()
        for _ in range(20):
            await asyncio.sleep(0)
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            # A cancelled serve() is the normal end of a test; its exceptions are ignored.
            await asyncio.gather(task, return_exceptions=True)


@asynccontextmanager
async def market_wire(**service_options: Any) -> AsyncIterator[Wire]:
    """One browser peer and one backend peer on one in-memory socket pair, using the demo's own objects."""
    left, right = memory_pair()
    browser_side, backend_side = left, right
    codec = JsonCodec()
    dialer = Peer(browser_side, codec=codec, is_dialer=True)
    backend = GatedSocket(backend_side)
    acceptor = Peer(backend, codec=codec, is_dialer=False)
    service = MarketService(Market(), PeerRegistry(), **service_options)
    acceptor.on_stream(service.handler_for(acceptor))
    wire = Wire(dialer, acceptor, service, [browser_side, backend_side], backend)
    wire.start()
    try:
        yield wire
    finally:
        await wire.stop()


# ---------------------------------------------------------------------------- export and ticks


async def test_ticks_keep_flowing_during_an_export() -> None:
    """WSM-INV-004: a megabyte on one stream does not stall the twenty other streams.

    Asserted on the frames the browser's `on_frame` recorded. The backend's sends are held while the
    export is enqueued, until every tick stream has more frames queued than the export has fragments;
    then they are released. From that state the writer's output does not depend on scheduling. A FIFO
    writer orders frames at enqueue time, so the ticks queued before the export would be sent first
    and all export fragments after them, consecutively. The round-robin writer sends one export
    fragment per turn and one frame of every tick stream in between (WSM-FRG-017/018/019).
    """
    async with market_wire(tick_interval=0.005) as wire:
        await wire.hello()
        await wire.until_the_board_is_live()
        ticks = wire.tick_streams()
        assert len(ticks) == len(SYMBOLS)

        wire.backend.gate.clear()
        export = wire.dialer.open({"action": "export", "symbol": "ACME"}, end=True)
        await wire.until_backlogged(export.id, depth=BACKLOG)
        wire.backend.gate.set()
        payload = await asyncio.wait_for(export, 10.0)

        received = wire.received()
        fragments = [
            index
            for index, frame in enumerate(received)
            if frame.stream == export.id and frame.type == "data" and frame.fragment is not None
        ]
        print(f"export fragments={len(fragments)} span={fragments[0]}..{fragments[-1]} of {len(received)} frames")
        # The export is fragmented; with a single frame there is nothing to interleave.
        assert len(fragments) >= 8, f"the export arrived in {len(fragments)} fragments; it cannot stall anything"
        assert len(fragments) < BACKLOG, "the tick streams ran dry before the export finished"
        assert payload["count"] == EXPORT_ROWS

        # Between any two export fragments, every tick stream sent exactly one frame and nothing else
        # was sent: the rotation visits every stream once before visiting any stream twice.
        for before, after in itertools.pairwise(fragments):
            between = sorted(frame.stream for frame in received[before + 1 : after])
            assert between == sorted(ticks), f"frames {before}..{after} carried streams {between}"


# ---------------------------------------------------------------------------- the other actions


async def test_every_action_answers() -> None:
    """Each action the frontend can send is handled, and answers in the shape the frontend expects."""
    async with market_wire(tick_interval=0.5, history_delay=0.0) as wire:
        acknowledgement = await wire.hello(topic="board", session="demo")
        assert acknowledgement["ok"] is True
        assert acknowledgement["symbols"] == len(SYMBOLS)
        # D3: the peer is indexed under the tags the frontend sent.
        assert wire.service.registry.peers_for(topic="board") == [wire.acceptor]
        assert wire.service.registry.peers_for(session="demo") == [wire.acceptor]

        # Server push and D1: the browser requested no symbol list and holds no seed data; it learns
        # all twenty symbols from the streams the acceptor opened, received through the browser's own
        # `on_stream` handler (WSM-INV-002).
        await wire.until_the_board_is_live()
        assert sorted(wire.board) == sorted(symbol for symbol, _name, _price in SYMBOLS)
        # The first frame of every pushed stream is the snapshot, not a tick, so the grid can be drawn
        # on subscription instead of after the first tick interval.
        assert [row["ticks"] for row in wire.first.values()] == [0] * len(SYMBOLS)

        quote = await wire.dialer.request({"action": "quote", "symbol": "ACME"})
        assert quote["symbol"] == "ACME"
        assert quote["bid"] < quote["last"] < quote["ask"]

        history = wire.dialer.open({"action": "history", "symbol": "ACME"}, end=True)
        points = [point async for point in history]
        assert len(points) == HISTORY_POINTS
        assert [point["t"] for point in points] == list(range(HISTORY_POINTS))
        # Trailers are carried on the `end` frame and are available once the iteration has finished.
        assert history.trailers == {"symbol": "ACME", "points": HISTORY_POINTS}

        before = len(wire.seen)
        depth = await wire.dialer.request({"action": "depth", "symbol": "ACME"})
        assert len(depth["bids"]) == len(depth["asks"]) == depth["levels"]
        assert depth["bids"][0]["px"] < depth["asks"][0]["px"]
        # WSM-FRG-010, not covered by the assertions above, which a five-level book would also pass:
        # the payload exceeds MAX_FRAME_BYTES and is fragmented and reassembled without the handler or
        # the test requesting it.
        pieces = [length for way, frame, length in wire.seen[before:] if way == "rx" and frame.fragment is not None]
        print(f"depth: {len(pieces)} fragments, {sum(pieces)} bytes on the wire")
        assert len(pieces) > 1, "the order book arrived in one frame; there was nothing to reassemble"
        assert sum(pieces) > MAX_FRAME_BYTES, f"the whole book was {sum(pieces)} bytes; it fits in one frame"

        export = await wire.dialer.request({"action": "export", "symbol": "ACME"})
        assert len(export["trades"]) == EXPORT_ROWS

        stats = await wire.dialer.request({"action": "stats"})
        assert stats["symbols"] == [symbol for symbol, _name, _price in SYMBOLS]
        assert stats["quotes"] == 1
        assert stats["exports"] == 1
        assert stats["peers"] == 1

        # An unknown action is an application error, not a refusal: REFUSED would state that the
        # request was not processed and may be retried (WSM-STM-034).
        try:
            await wire.dialer.request({"action": "buy", "symbol": "ACME"})
        except RemoteError as refused:
            print(f"unknown action answered with: {refused}")
        else:
            raise AssertionError("an unknown action was not reported as an application error")


async def test_the_rate_action_changes_how_often_the_board_pushes() -> None:
    """The `rate` action sets the board's tick interval, within bounds.

    `TICK_INTERVAL` is a `sleep` in the generator and does not reflect the transport's capacity
    (`muxws/throughput_test.py` measures that); the board's default sends eighty frames per second.
    This asserts that the interval can be changed and that it is bounded: zero would turn the
    generator into a busy loop that starves the event loop it sends on.
    """
    async with market_wire() as wire:
        answer = await wire.dialer.request({"action": "rate", "interval_ms": 10})
        assert answer["ok"] is True
        assert answer["interval_ms"] == 10
        assert wire.service._tick_interval == 0.01

        stats = await wire.dialer.request({"action": "stats"})
        assert stats["tick_interval"] == 0.01, "the change is not visible to the panel that displays it"

        for refused in (0, -5, 10_000):
            with pytest.raises(RemoteError):
                await wire.dialer.request({"action": "rate", "interval_ms": refused})
        assert wire.service._tick_interval == 0.01, "a refused rate must not have been applied"


async def test_cancelling_history_stops_the_generator() -> None:
    """WSM-ERR-012/013: `cancel()` stops the backend generator, as its counters show.

    Asserted on the backend's counters, not on the absence of frames: a frontend that stopped reading
    would also receive no further frames.
    """
    async with market_wire(tick_interval=0.5, history_delay=0.005) as wire:
        await wire.hello()
        history = wire.dialer.open({"action": "history", "symbol": "ACME"}, end=True)

        received = 0
        async for _point in history:
            received += 1
            if received == 3:
                break
        # A `break` out of an `async for` does not cancel the stream in Python, so the consumer cancels
        # it explicitly, as the guide describes and as the frontend does when another row is selected.
        await history.cancel("switched symbols")
        await asyncio.sleep(0.05)

        counters = wire.service.counters
        assert counters.history_started == 1
        assert counters.history_cancelled == 1, "the backend generator was never cancelled"

        stopped_at = counters.history_points_sent
        assert stopped_at < HISTORY_POINTS, "the generator ran to completion despite the cancellation"
        # Long enough for several more points if the generator were still running.
        await asyncio.sleep(0.1)
        print(f"points sent before the cancellation: {stopped_at}; after another 100ms: {counters.history_points_sent}")
        assert counters.history_points_sent == stopped_at, "the backend kept generating after it was cancelled"


async def test_the_export_payload_fragments() -> None:
    """WSM-FRG-010: the export exceeds MAX_FRAME_BYTES, is fragmented on the wire and arrives whole."""
    async with market_wire(tick_interval=0.5) as wire:
        await wire.hello()
        export = wire.dialer.open({"action": "export", "symbol": "ACME"}, end=True)
        payload = await export

        arrived = [(frame, length) for way, frame, length in wire.seen if way == "rx" and frame.stream == export.id]
        fragments = [(frame, length) for frame, length in arrived if frame.fragment is not None]
        total = sum(length for _frame, length in fragments)

        print(f"export: {len(fragments)} fragments, {total} bytes on the wire")
        assert len(fragments) > 1, "a megabyte arrived in one frame; nothing was fragmented"
        assert total > 1_000_000, f"the export was only {total} bytes; it is meant to be about a megabyte"
        # No fragment exceeds the limit. The demo passes no `max_frame_bytes`, so the default applies.
        for frame, length in fragments:
            assert length <= MAX_FRAME_BYTES, f"fragment {frame.fragment is not None} of {length} bytes exceeds the cap"
        # `more: true` on every fragment except the last.
        assert [frame.more for frame, _length in fragments] == [True] * (len(fragments) - 1) + [False]

        assert payload["count"] == EXPORT_ROWS
        assert len(payload["trades"]) == EXPORT_ROWS
        assert payload["trades"][-1]["seq"] == EXPORT_ROWS - 1


def test_the_market_is_deterministic_whatever_the_reader_clicked() -> None:
    """D2: values do not depend on which other requests were made.

    With one shared seeded generator, ACME's tenth price would depend on whether an export was
    requested first. With one generator per (seed, purpose, symbol) it does not, so two markets
    driven with different requests produce the same numbers.
    """
    quiet, busy = Market(), Market()
    ten = [quiet.tick("ACME") for _ in range(10)]

    busy.export("ACME")
    busy.depth("BOLT")
    busy.history("ACME")
    busy.tick("BOLT")
    assert [busy.tick("ACME") for _ in range(10)] == ten, "ACME's prices depended on what else was asked for"

    # Also across processes: `random.Random` hashes a string seed itself, not through `hash()`, so the
    # result does not depend on PYTHONHASHSEED.
    assert Market().history("ACME") == quiet.history("ACME")
    # The book's prices derive from the symbol's current price, which differs in `busy`, so only the
    # randomly drawn quantities are compared.
    drawn = lambda book: [(level["qty"], level["orders"]) for level in book["bids"][:5]]
    assert drawn(Market().depth("BOLT")) == drawn(busy.depth("BOLT"))


async def test_the_kill_switch_closes_every_peer() -> None:
    """The kill switch closes every peer: live streams fail and the browser's `on_close` is called."""
    async with market_wire(tick_interval=0.02, kill_delay=0.01) as wire:
        await wire.hello()
        await wire.until_the_board_is_live()
        watching = next(iter(wire.dialer.streams.values()))

        answer = await wire.dialer.request({"action": "kill"})
        assert answer["closing"] == 1

        deadline = asyncio.get_running_loop().time() + 2.0
        while not wire.closes and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.005)
        # The browser's `on_close` is called.
        assert wire.closes, "the kill switch closed nothing the browser could observe"
        print(f"the browser was told: code={wire.closes[0].code} clean={wire.closes[0].was_clean}")
        assert wire.dialer.is_open is False
        assert wire.dialer.streams == {}
        # WSM-REG-016: the registry removes a closed peer through its own `on_close` hook, independently
        # of `MarketService.forget`. The kill switch is the only place in the demo where a registered
        # peer closes.
        assert wire.service.registry.peers_for() == [], "a closed peer is still in the registry"

        # Every open stream is closed, and a send on one raises (WSM-ERR-002: a lost connection has
        # its own error, distinct from a stream's).
        assert watching.closed.is_set()
        try:
            await watching.send({"anything": True})
        except StreamReset as lost:
            print(f"a pushed stream ended with: {type(lost).__name__}: {lost}")
        else:
            raise AssertionError("a stream on a killed connection still accepted a send")
