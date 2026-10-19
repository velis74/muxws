"""The `on_stream` handler and every action the frontend can request.

There is one handler per peer, and every stream the browser opens arrives at it (WSM-STM-030). The
four call shapes use the same API and differ in what the handler does with the stream:

  * `quote`   - `reply()`: one payload, which also ends the stream (WSM-API-006).
  * `history` - `send()` in a loop, then `end()`: a response that arrives progressively and stops
                when the opener cancels it (WSM-ERR-012/013).
  * `depth`   - `reply()` with a payload over MAX_FRAME_BYTES, fragmented and reassembled without
                application code (WSM-FRG-010).
  * `export`  - the same with a megabyte. While its sixteen fragments are sent, the twenty pushed
                tick streams continue to send frames between them (WSM-INV-004).

The board is not an action. The backend calls `peer.open()` and the browser receives the stream in
its own `on_stream` handler; both directions use the same `Peer` type and correlation (WSM-INV-002).
"""

from __future__ import annotations

import asyncio
import logging

from dataclasses import asdict, dataclass
from typing import Any

from demo.backend_python.market import HISTORY_POINTS, Market
from muxws import (
    ConnectionGoingAway,
    ConnectionLost,
    Peer,
    PeerRegistry,
    ResetCode,
    Stream,
    StreamClosed,
    StreamHandler,
    StreamReset,
)

logger = logging.getLogger("muxws.demo")

#: Seconds between prices on each symbol's pushed stream: four per second per symbol.
TICK_INTERVAL = 0.25

#: Seconds between two points of a history response, so the chart fills in progressively and the
#: response can be cancelled while in progress.
HISTORY_POINT_DELAY = 0.02

#: Seconds the kill switch waits before closing the sockets, so that its own reply is sent before
#: the connection closes.
KILL_DELAY = 0.1


@dataclass
class Counters:
    """Counters shown in the diagnostics strip and asserted on by `handlers_test.py`.

    Under WSM-ERR-012/013 the opener's `cancel()` stops the backend generator. `history_cancelled` is
    incremented by the generator itself, so it shows that the backend stopped, which the rendered
    chart alone cannot show.
    """

    history_started: int = 0
    history_points_sent: int = 0
    history_cancelled: int = 0
    exports: int = 0
    depths: int = 0
    quotes: int = 0


class MarketService:
    """The demo's backend: the market, the registry, and the pushed tick streams."""

    def __init__(
        self,
        market: Market,
        registry: PeerRegistry,
        *,
        tick_interval: float = TICK_INTERVAL,
        history_delay: float = HISTORY_POINT_DELAY,
        kill_delay: float = KILL_DELAY,
    ) -> None:
        self.market = market
        self.registry = registry
        self.counters = Counters()
        self._tick_interval = tick_interval
        self._history_delay = history_delay
        self._kill_delay = kill_delay
        #: One list of pushing tasks per peer, referenced so the tasks are not garbage-collected.
        self._boards: dict[Peer, list[asyncio.Task[None]]] = {}
        #: The kill switch's task, referenced for the same reason.
        self._killer: asyncio.Task[None] | None = None
        self._actions = {
            "hello": self._hello,
            "quote": self._quote,
            "history": self._history,
            "depth": self._depth,
            "export": self._export,
            "stats": self._stats,
            "rate": self._rate,
            "kill": self._kill,
        }

    @property
    def actions(self) -> tuple[str, ...]:
        """Every action the frontend may name."""
        return tuple(self._actions)

    # ------------------------------------------------------------------ registration

    def handler_for(self, peer: Peer) -> StreamHandler:
        """The `on_stream` handler for one connection.

        Bound per peer because a handler receives `(payload, stream)` and the actions need the
        `Peer`: the hello tags and registers this connection, and the board is pushed onto it. The
        route has the peer in scope, as in the documented examples.
        """

        async def on_stream(payload: Any, stream: Stream) -> None:
            await self.dispatch(peer, payload, stream)

        return on_stream

    async def dispatch(self, peer: Peer, payload: Any, stream: Stream) -> None:
        """Route one stream to one action.

        An unknown action raises `ValueError`. WSM-STM-034 turns a handler exception into
        `reset(APPLICATION_ERROR)`, not `REFUSED`, which would state that the request was not
        processed and may be retried.
        """
        action = payload.get("action") if isinstance(payload, dict) else None
        handler = self._actions.get(action) if isinstance(action, str) else None
        if handler is None:
            raise ValueError(f"unknown action {action!r}; this backend answers {', '.join(self._actions)}")
        await handler(peer, payload, stream)

    def forget(self, peer: Peer) -> None:
        """Stop pushing to a peer and drop it from the index. Idempotent."""
        for task in self._boards.pop(peer, []):
            task.cancel()
        self.registry.deregister(peer)

    # ------------------------------------------------------------------ the hello

    async def _hello(self, peer: Peer, payload: Any, stream: Stream) -> None:
        """The subscription: an ordinary stream with no marker on the wire (WSM-RCN-021).

        The dialer sends it through `connect(hello=...)`, so it is replayed unchanged on every socket
        that peer gets (WSM-RCN-020), and this runs again on a new `Peer` after every reconnect. An
        acceptor's `tags` end with their connection (WSM-RCN-033), so the peer is indexed here, on
        every connection.

        Nothing depends on the reply. `connect()`'s reconnect helper owns this stream and discards
        the reply; the acknowledgement WSM-RCN-022 requires is this handler returning. The frontend
        receives its data on the pushed streams below; the backend is the only source of symbol data.
        """
        tags = payload.get("tags") if isinstance(payload, dict) else None
        if isinstance(tags, dict):
            # muxws does not read `tags` and defines no keys in it (WSM-REG-001/002); these are the
            # frontend's labels, and the index uses them unchanged.
            peer.tags.update(tags)
        # After the tag writes: the registry indexes tags at registration and does not observe later
        # changes (WSM-REG-010).
        self.registry.register(peer)
        self._start_board(peer)
        await stream.reply({"ok": True, "symbols": len(self.market.symbols), "tick_interval": self._tick_interval})

    async def _rate(self, peer: Peer, payload: dict[str, Any], stream: Stream) -> None:
        """Change how often each pushed stream carries a price.

        The tick interval is a `sleep` in the generator below and does not reflect the transport's
        capacity; `muxws/throughput_test.py` measures throughput. The board's default sends eighty
        frames per second.

        The minimum is 1 ms. At zero the generator becomes a busy loop that starves the event loop it
        sends on.
        """
        _ = peer
        requested = payload.get("interval_ms") if isinstance(payload, dict) else None
        if not isinstance(requested, (int, float)) or not 1 <= requested <= 5_000:
            raise ValueError(f"interval_ms must be between 1 and 5000; got {requested!r}")
        self._tick_interval = float(requested) / 1000.0
        await stream.reply({"ok": True, "interval_ms": requested, "symbols": len(self.market.symbols)})

    # ------------------------------------------------------------------ server push

    def _start_board(self, peer: Peer) -> None:
        """Open twenty streams from the acceptor side (WSM-INV-002).

        One stream per symbol, not one stream for all twenty: with twenty independent streams plus
        the export, a round-robin writer and a FIFO writer produce different frame orders, which
        shows that a stream sending a megabyte does not block the others.
        """
        if peer in self._boards:
            # A second hello on one socket does not start a second board. The reconnect helper sends
            # one hello per connection, so this applies only to a client that sends another.
            return
        self._boards[peer] = [asyncio.create_task(self._push_ticks(peer, symbol)) for symbol in self.market.symbols]
        # Synchronous, as `on_close` handlers are; cancelling a task is synchronous. Without it a
        # socket that closes outside the route (for example in a test) leaves twenty tasks pushing
        # into a closed peer whose writer was discarded (WSM-RCN-042).
        peer.on_close(lambda _reason, target=peer: self.forget(target))

    async def _push_ticks(self, peer: Peer, symbol: str) -> None:
        """One symbol's price stream, for as long as the socket lasts.

        `peer.open()` here is the same call the browser makes for a quote, on the same socket, with
        the same correlation and cancellation. There is no separate push API (WSM-INV-002).
        """
        try:
            stream = peer.open({"topic": "ticks", "symbol": symbol})
        except (ConnectionLost, ConnectionGoingAway):
            # The socket closed between the hello and this task's first turn. Nothing is buffered for
            # the next connection (WSM-RCN-042).
            return
        try:
            # The snapshot first, so the row is drawn before the first price change; the frontend
            # holds no seed data.
            await stream.send(self.market.snapshot(symbol))
            while True:
                await asyncio.sleep(self._tick_interval)
                await stream.send(self.market.tick(symbol))
        except (StreamReset, StreamClosed):
            # `ConnectionLost` is a `StreamReset`, so this covers the normal end: the socket closed
            # and every live stream failed before `on_close` ran.
            return
        finally:
            # A board stream does not end on its own, and `close()` waits for live streams before
            # closing the socket (WSM-CON-024/025); without the cancel, twenty board streams would keep
            # the drain waiting for its full window. A no-op when the stream is already closed.
            await stream.cancel("the board stopped")

    # ------------------------------------------------------------------ the four call shapes

    async def _quote(self, _peer: Peer, payload: Any, stream: Stream) -> None:
        """Unary: one payload out, exactly one payload back (WSM-API-006)."""
        self.counters.quotes += 1
        await stream.reply(self.market.quote(_symbol_of(payload)))

    async def _history(self, _peer: Peer, payload: Any, stream: Stream) -> None:
        """Streaming and cancellable (WSM-ERR-012/013).

        When the user selects another row during loading, the browser calls `cancel()` on this
        stream. The reset reaches this peer, which cancels this handler's task, and `CancelledError`
        is raised at the current `await`. The `history_cancelled` counter shows that the backend
        stopped generating.
        """
        symbol = _symbol_of(payload)
        points = self.market.history(symbol, HISTORY_POINTS)
        self.counters.history_started += 1
        try:
            for point in points:
                await stream.send(point)
                self.counters.history_points_sent += 1
                await asyncio.sleep(self._history_delay)
        except asyncio.CancelledError:
            self.counters.history_cancelled += 1
            # Re-raised (WSM-ERR-014). Returning normally here would end the stream as completed.
            raise
        # Trailers are carried on the `end` frame and hold values known only after the body is sent.
        await stream.end(trailers={"symbol": symbol, "points": len(points)})

    async def _depth(self, _peer: Peer, payload: Any, stream: Stream) -> None:
        """A payload larger than MAX_FRAME_BYTES, fragmented by the writer (WSM-FRG-010)."""
        self.counters.depths += 1
        await stream.reply(self.market.depth(_symbol_of(payload)))

    async def _export(self, _peer: Peer, payload: Any, stream: Stream) -> None:
        """The stall test: one megabyte as one payload on one stream, while twenty other streams continue.

        The handler only replies. The writer sends the reply in 64 KiB fragments and sends frames of
        the twenty tick streams between them (WSM-FRG-018/019, WSM-INV-004).
        """
        self.counters.exports += 1
        await stream.reply(self.market.export(_symbol_of(payload)))

    # ------------------------------------------------------------------ the diagnostics strip

    async def _stats(self, peer: Peer, _payload: Any, stream: Stream) -> None:
        """Backend counters and state, requested over the same socket as everything else."""
        await stream.reply(
            {
                **asdict(self.counters),
                "peers": len(self.registry.peers_for()),
                "streams": len(peer.streams),
                "symbols": self.market.symbols,
                # The board's tick interval, the baseline for the tick-latency readout. The hello reply
                # also carries it, but `connect(hello=...)` discards that reply (WSM-RCN-022).
                "tick_interval": self._tick_interval,
            }
        )

    async def _kill(self, _peer: Peer, _payload: Any, stream: Stream) -> None:
        """Close every peer, so the browser reconnects.

        Scheduled, not awaited: `close()` sends `goaway`, drains and closes the socket (WSM-CON-025),
        and doing that inline would close the socket before this handler's reply is sent.
        """
        peers = self.registry.peers_for()
        await stream.reply({"closing": len(peers)})
        self._killer = asyncio.create_task(self._close_every_peer(peers))

    async def _close_every_peer(self, peers: list[Peer]) -> None:
        await asyncio.sleep(self._kill_delay)
        for peer in peers:
            # The board first. `close()` drains every live stream before it closes the socket
            # (WSM-CON-024), and the twenty pushed streams do not end on their own, so the drain would
            # wait for its full window.
            self.forget(peer)
            try:
                await peer.close(ResetCode.NO_ERROR, "the demo's kill switch")
            except Exception:  # noqa: BLE001 - a peer that is already closed needs no closing
                logger.debug("muxws demo: peer %s was already gone when the kill switch reached it", peer.id)


def _symbol_of(payload: Any) -> str:
    """The symbol of a request; raises `ValueError` when the payload names none."""
    symbol = payload.get("symbol") if isinstance(payload, dict) else None
    if not isinstance(symbol, str):
        raise ValueError(f"this action needs a symbol; got {symbol!r}")
    return symbol
