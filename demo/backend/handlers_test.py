"""The demo's backend against the in-memory transport.

A demo with no tests rots into a screenshot. The one that carries the milestone is
`test_ticks_keep_flowing_during_an_export`: the panel is the demonstration, this is the proof, and it
is written against `on_frame`'s record rather than against anything on a screen so that it fails if
the round-robin writer is ever replaced by a FIFO.
"""

from __future__ import annotations

import asyncio

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from demo.backend.handlers import MarketService
from demo.backend.market import EXPORT_ROWS, HISTORY_POINTS, Market, SYMBOLS
from muxws import Frame, MAX_FRAME_BYTES, Peer, PeerRegistry, RemoteError, StreamReset
from muxws.codecs.json_ import JsonCodec
from muxws.transports.memory import memory_pair, MemorySocket


class Wire:
    """A browser and a backend, wired to each other, with the demo's real handler on the acceptor.

    Over a plain `MemorySocket`, deliberately. Until M8 this fixture wrapped both ends in a
    `PacedSocket` that slept in proportion to frame size, because `Peer._write_loop` had no
    suspension of its own: a send that completed without yielding let the writer drain a whole
    megabyte in one run of the task, nothing else was scheduled to enqueue, and the rotation had a
    single lane to rotate over. The double supplied the yield the transport did not, so this test was
    green for a reason the running demo could not reproduce - measured against uvicorn on loopback,
    the export's fragments arrived back to back with no tick between them.

    That was reported rather than worked around, and then fixed in `muxws/peer.py`: the write loop now
    takes one event-loop turn per frame. The double is gone with the defect, and the property is
    observed over the same transport every other test uses (WSM-INV-004).
    """

    def __init__(self, dialer: Peer, acceptor: Peer, service: MarketService, sockets: list[MemorySocket]) -> None:
        self.dialer = dialer
        self.acceptor = acceptor
        self.service = service
        self.sockets = sockets
        #: Every frame the *browser* saw, in arrival order. WSM-OBS-003's hook is the only witness
        #: this milestone's headline is allowed to use.
        self.seen: list[tuple[str, Frame, int]] = []
        #: What the browser knows about the market, and it knows nothing the socket did not tell it
        #: (D1). Filled by the pushed streams alone.
        self.board: dict[str, Any] = {}
        #: The **first** row each symbol pushed, kept separately because the latest one overwrites it.
        #: A board that could only draw a row once a price had moved would leave the grid empty for a
        #: whole tick interval, which is what the snapshot exists to prevent.
        self.first: dict[str, Any] = {}
        #: Every `CloseReason` the browser was handed. The kill-switch panel's claim is that the UI
        #: is *told*, rather than left with a frozen screen.
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
        # Registered **before** anything goes out, because the backend pushes the board the instant it
        # sees the hello and a handler registered one await later meets that push with
        # `reset(REFUSED, "no on_stream handler")` (WSM-STM-033). `connect()` takes `on_stream=` as a
        # parameter for exactly this reason.
        self.dialer.on_stream(self._on_push)
        self.dialer.on_close(self.closes.append)
        self._tasks = [asyncio.create_task(self.dialer.serve()), asyncio.create_task(self.acceptor.serve())]

    async def hello(self, **tags: Any) -> Any:
        """What `connect(hello=...)` puts on the wire, sent by hand so the ack can be read (D3)."""
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

    async def stop(self) -> None:
        self.service.forget(self.acceptor)
        for socket in self.sockets:
            await socket.drop()
        for _ in range(20):
            await asyncio.sleep(0)
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            # A cancelled serve() is the expected end of a test; whatever it raises on the way out is
            # teardown noise rather than a result.
            await asyncio.gather(task, return_exceptions=True)


@asynccontextmanager
async def market_wire(**service_options: Any) -> AsyncIterator[Wire]:
    """One browser, one backend, one socket - the demo's own objects and nobody's test doubles."""
    left, right = memory_pair()
    browser_side, backend_side = left, right
    codec = JsonCodec()
    dialer = Peer(browser_side, codec=codec, is_dialer=True)
    acceptor = Peer(backend_side, codec=codec, is_dialer=False)
    service = MarketService(Market(), PeerRegistry(), **service_options)
    acceptor.on_stream(service.handler_for(acceptor))
    wire = Wire(dialer, acceptor, service, [browser_side, backend_side])
    wire.start()
    try:
        yield wire
    finally:
        await wire.stop()


def _runs_of(flags: list[bool]) -> int:
    """The longest run of consecutive True values."""
    longest = run = 0
    for flag in flags:
        run = run + 1 if flag else 0
        longest = max(longest, run)
    return longest


# ---------------------------------------------------------------------------- the headline


async def test_ticks_keep_flowing_during_an_export() -> None:
    """WSM-INV-004, as a test: a megabyte on one stream does not stall the twenty others.

    Asserted from what the *browser's* `on_frame` recorded, not from anything visual and not from the
    handler's own bookkeeping. A FIFO anywhere on the send path decides the order at enqueue time, so
    every one of the export's sixteen fragments would go out back to back and the ticks produced
    while they did would arrive afterwards in a heap. Under the round-robin writer the export gets
    one fragment per turn and every tick stream gets its own turn in between (WSM-FRG-017/018/019).
    """
    async with market_wire(tick_interval=0.005) as wire:
        await wire.hello()
        await wire.until_the_board_is_live()

        export = wire.dialer.open({"action": "export", "symbol": "ACME"}, end=True)
        payload = await export

        received = wire.received()
        fragments = [
            index
            for index, frame in enumerate(received)
            if frame.stream == export.id and frame.type == "data" and frame.fragment is not None
        ]
        ticks = {
            index
            for index, frame in enumerate(received)
            if frame.type == "data" and frame.stream is not None and frame.stream % 2 == 0
        }

        print(f"export fragments={len(fragments)} span={fragments[0]}..{fragments[-1]} of {len(received)} frames")
        # A megabyte really did have to be fragmented; otherwise there was nothing to stall behind.
        assert len(fragments) >= 8, f"the export arrived in {len(fragments)} fragments; it cannot stall anything"
        assert payload["count"] == EXPORT_ROWS

        interleaved = sum(1 for index in ticks if fragments[0] < index < fragments[-1])
        print(f"tick frames between the first and last export fragment: {interleaved}")
        assert interleaved >= 40, f"only {interleaved} ticks arrived while a megabyte was on the wire"

        # The sharper form of the same claim, and the one a FIFO cannot survive: no tick ever waits
        # on more than a fragment or two of the export.
        window = received[fragments[0] : fragments[-1] + 1]
        back_to_back = _runs_of([index + fragments[0] in set(fragments) for index in range(len(window))])
        print(f"longest run of consecutive export fragments: {back_to_back}")
        assert back_to_back <= 2, f"{back_to_back} export fragments went out back to back; the writer is a FIFO"

        # And it was the whole board that kept moving, not one lucky stream: the rotation asks every
        # lane once before it asks any lane twice.
        streams = {
            frame.stream
            for frame in window
            if frame.type == "data" and frame.stream is not None and frame.stream % 2 == 0
        }
        print(f"distinct tick streams delivering during the export: {len(streams)} of {len(SYMBOLS)}")
        assert len(streams) == len(SYMBOLS), f"only {len(streams)} of {len(SYMBOLS)} symbols kept ticking"


# ---------------------------------------------------------------------------- section 6's rest


async def test_every_action_answers() -> None:
    """Each action the frontend can send is handled, and answers in the shape the frontend expects."""
    async with market_wire(tick_interval=0.5, history_delay=0.0) as wire:
        acknowledgement = await wire.hello(topic="board", session="demo")
        assert acknowledgement["ok"] is True
        assert acknowledgement["symbols"] == len(SYMBOLS)
        # D3: the peer is findable under the tags the *frontend* named, and under nothing muxws chose.
        assert wire.service.registry.peers_for(topic="board") == [wire.acceptor]
        assert wire.service.registry.peers_for(session="demo") == [wire.acceptor]

        # Server push, and D1 with it: the browser asked for no symbol list and holds no seed data,
        # and yet it knows all twenty - because the acceptor opened twenty streams of its own accord
        # and the browser received them through its own `on_stream` handler (WSM-INV-002).
        await wire.until_the_board_is_live()
        assert sorted(wire.board) == sorted(symbol for symbol, _name, _price in SYMBOLS)
        # The first frame of every pushed stream is the snapshot, not a tick: the grid is drawable
        # the moment it is subscribed rather than a tick interval later. A backend that pushed only
        # movements would fill the board just as surely and leave it blank until it did.
        assert [row["ticks"] for row in wire.first.values()] == [0] * len(SYMBOLS)

        quote = await wire.dialer.request({"action": "quote", "symbol": "ACME"})
        assert quote["symbol"] == "ACME"
        assert quote["bid"] < quote["last"] < quote["ask"]

        history = wire.dialer.open({"action": "history", "symbol": "ACME"}, end=True)
        points = [point async for point in history]
        assert len(points) == HISTORY_POINTS
        assert [point["t"] for point in points] == list(range(HISTORY_POINTS))
        # Trailers ride the `end` frame and are readable only once the iteration is over.
        assert history.trailers == {"symbol": "ACME", "points": HISTORY_POINTS}

        before = len(wire.seen)
        depth = await wire.dialer.request({"action": "depth", "symbol": "ACME"})
        assert len(depth["bids"]) == len(depth["asks"]) == depth["levels"]
        assert depth["bids"][0]["px"] < depth["asks"][0]["px"]
        # WSM-FRG-010, which is the entire claim of the "full depth" panel and is not implied by the
        # three assertions above: a book of five levels is internally consistent, arrives whole, and
        # witnesses nothing. The payload has to cross MAX_FRAME_BYTES for there to be a reassembly to
        # watch, and nothing in the handler or here asked for one.
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

        # An action nobody defined is an application error, never a refusal: REFUSED would promise
        # the request had not been processed and invite the browser to retry it (WSM-STM-034).
        try:
            await wire.dialer.request({"action": "buy", "symbol": "ACME"})
        except RemoteError as refused:
            print(f"unknown action answered with: {refused}")
        else:
            raise AssertionError("an unknown action was not reported as an application error")


async def test_cancelling_history_stops_the_generator() -> None:
    """WSM-ERR-012/013: `cancel()` stops the *backend*, and a counter it increments says so.

    Asserted from that counter and never from the absence of frames. A frontend that stopped reading
    would produce exactly the same silence on the wire, and the panel's whole claim is that the
    backend stopped generating.
    """
    async with market_wire(tick_interval=0.5, history_delay=0.005) as wire:
        await wire.hello()
        history = wire.dialer.open({"action": "history", "symbol": "ACME"}, end=True)

        received = 0
        async for _point in history:
            received += 1
            if received == 3:
                break
        # Python does not observe a `break` out of an `async for`, so the consumer cancels the stream
        # itself - which is what the guide tells a consumer to do and what the frontend does when the
        # reader clicks another row.
        await history.cancel("switched symbols")
        await asyncio.sleep(0.05)

        counters = wire.service.counters
        assert counters.history_started == 1
        assert counters.history_cancelled == 1, "the backend generator was never cancelled"

        stopped_at = counters.history_points_sent
        assert stopped_at < HISTORY_POINTS, "the generator ran to completion despite the cancellation"
        # Long enough for a dozen more points had anything still been generating them.
        await asyncio.sleep(0.1)
        print(f"points sent before the cancellation: {stopped_at}; after another 100ms: {counters.history_points_sent}")
        assert counters.history_points_sent == stopped_at, "the backend kept generating after it was cancelled"


async def test_the_export_payload_fragments() -> None:
    """WSM-FRG-010: over MAX_FRAME_BYTES on the wire, whole on arrival, and nothing asked for it."""
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
        # A receiver must accept anything up to the cap and a sender must never exceed it. The cap is
        # a protocol constant, not a setting - nothing in the demo passed `max_frame_bytes` at all.
        for frame, length in fragments:
            assert length <= MAX_FRAME_BYTES, f"fragment {frame.fragment is not None} of {length} bytes exceeds the cap"
        # `more: true` on every fragment but the last, which is how the receiver knows it is done.
        assert [frame.more for frame, _length in fragments] == [True] * (len(fragments) - 1) + [False]

        assert payload["count"] == EXPORT_ROWS
        assert len(payload["trades"]) == EXPORT_ROWS
        assert payload["trades"][-1]["seq"] == EXPORT_ROWS - 1


def test_the_market_is_deterministic_whatever_the_reader_clicked() -> None:
    """D2, and the half of it that a single seeded generator would not give you.

    Seeding one shared stream of randomness makes a run reproducible only if it is *driven*
    identically: ACME's tenth price would then depend on whether anybody asked for an export first,
    and the screenshot in the documentation would stop being true the moment a reader clicked in a
    different order. One generator per (seed, purpose, symbol) is what makes the claim hold, so this
    drives two markets differently on purpose and expects the same numbers out of both.
    """
    quiet, busy = Market(), Market()
    ten = [quiet.tick("ACME") for _ in range(10)]

    busy.export("ACME")
    busy.depth("BOLT")
    busy.history("ACME")
    busy.tick("BOLT")
    assert [busy.tick("ACME") for _ in range(10)] == ten, "ACME's prices depended on what else was asked for"

    # And across processes, which is what a screenshot actually needs: `random.Random` hashes the
    # string seed itself rather than through `hash()`, so this owes nothing to PYTHONHASHSEED.
    assert Market().history("ACME") == quiet.history("ACME")
    # The book's prices hang off the symbol's *current* price, which `busy` has moved on purpose, so
    # what is compared is the drawn part: the quantities are the same draws in the same order.
    drawn = lambda book: [(level["qty"], level["orders"]) for level in book["bids"][:5]]
    assert drawn(Market().depth("BOLT")) == drawn(busy.depth("BOLT"))


async def test_the_kill_switch_closes_every_peer() -> None:
    """The "kill the backend" panel: every live stream fails and the browser is told, not frozen."""
    async with market_wire(tick_interval=0.02, kill_delay=0.01) as wire:
        await wire.hello()
        await wire.until_the_board_is_live()
        watching = next(iter(wire.dialer.streams.values()))

        answer = await wire.dialer.request({"action": "kill"})
        assert answer["closing"] == 1

        deadline = asyncio.get_running_loop().time() + 2.0
        while not wire.closes and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.005)
        # The browser is *told*, which is the whole of the panel: a UI that only froze would be
        # indistinguishable from a slow backend.
        assert wire.closes, "the kill switch closed nothing the browser could observe"
        print(f"the browser was told: code={wire.closes[0].code} clean={wire.closes[0].was_clean}")
        assert wire.dialer.is_open is False
        assert wire.dialer.streams == {}
        # WSM-REG-016 end to end, and it is the *library* this witnesses rather than the demo: the
        # registry hooks `on_close` itself, so this stays empty even if `MarketService.forget` never
        # called `deregister`. That is worth one line here because the kill switch is the only place
        # in the demo where a registered peer dies, and a registry that answered with sockets that
        # have been shut since would make the next `peers_for()` count a fiction.
        assert wire.service.registry.peers_for() == [], "a closed peer is still in the registry"

        # Every open stream is closed rather than left hanging, and a send on one raises
        # (WSM-ERR-002 keeps socket death out of a stream's own vocabulary and gives it its own).
        assert watching.closed.is_set()
        try:
            await watching.send({"anything": True})
        except StreamReset as lost:
            print(f"a pushed stream ended with: {type(lost).__name__}: {lost}")
        else:
            raise AssertionError("a stream on a killed connection still accepted a send")
