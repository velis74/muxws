"""The one `on_stream` handler, and every action the frontend can ask for.

There is one handler per peer and every stream the browser opens arrives at it - the first one and
every one after it (WSM-STM-030). What distinguishes the four call shapes is not four APIs but what
this code does with the stream it was handed:

  * `quote`   - `reply()`: one payload, and the stream ends with it (WSM-API-006).
  * `history` - `send()` in a loop, then `end()`: a response that arrives progressively, and one that
                stops when the opener cancels it (WSM-ERR-012/013).
  * `depth`   - `reply()` with a payload over MAX_FRAME_BYTES: fragmented and reassembled with the
                application doing nothing at all (WSM-FRG-010).
  * `export`  - the same, with a megabyte. The headline: while its sixteen fragments are on the wire,
                the twenty pushed tick streams keep their turn (WSM-INV-004).

The board itself is not an action. The backend calls `peer.open()` on its own initiative and the
browser receives it through *its* `on_stream` handler - one `Peer` type, one mechanism, no second
correlation story (WSM-INV-002).
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

#: How often each symbol's pushed stream carries a new price. Four a second per symbol is fast enough
#: that the board is visibly live and slow enough that a reader can follow one row.
TICK_INTERVAL = 0.25

#: The pause between two points of a history response. It exists so the response is visibly
#: *progressive*: a chart that filled in one frame would demonstrate nothing that a unary reply does
#: not, and there would be nothing to cancel halfway through.
HISTORY_POINT_DELAY = 0.02

#: How long the kill switch waits before closing the sockets. Long enough for its own reply to reach
#: the wire, because a handler whose connection is closed underneath it is a handler whose answer
#: never arrives - and the panel would then look like a bug rather than like a demonstration.
KILL_DELAY = 0.1


@dataclass
class Counters:
    """What the diagnostics strip reads, and what `handlers_test.py` asserts on.

    `history_cancelled` is the one that matters. WSM-ERR-012/013 say the opener's `cancel()` stops
    the *backend* generator, and nothing on a screen can distinguish a backend that stopped from a
    frontend that stopped looking. A number the generator increments itself can.
    """

    history_started: int = 0
    history_points_sent: int = 0
    history_cancelled: int = 0
    exports: int = 0
    depths: int = 0
    quotes: int = 0


class MarketService:
    """The demo's backend, as one object: the market, the registry, and the pushed tick streams."""

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
        #: One list of pushing tasks per peer, held so the garbage collector cannot take the board
        #: away from a connection that is still watching it.
        self._boards: dict[Peer, list[asyncio.Task[None]]] = {}
        #: The kill switch's own task, held for the same reason.
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
        """Every action the frontend may name. The test asserts each one answers."""
        return tuple(self._actions)

    # ------------------------------------------------------------------ registration

    def handler_for(self, peer: Peer) -> StreamHandler:
        """The `on_stream` handler for one connection.

        Bound per peer because a handler is handed `(payload, stream)` and the actions need the
        `Peer`: the hello has to tag and register *this* connection, and the board has to be pushed
        onto it. The route has the peer in scope, which is where the documented examples close over
        it too.
        """

        async def on_stream(payload: Any, stream: Stream) -> None:
            await self.dispatch(peer, payload, stream)

        return on_stream

    async def dispatch(self, peer: Peer, payload: Any, stream: Stream) -> None:
        """Route one stream to one action.

        An unknown action raises, and that raise is the answer: WSM-STM-034 turns a handler exception
        into `reset(APPLICATION_ERROR)` - never `REFUSED`, which would promise the request had not
        been processed and would invite the browser to retry something that may already have run.
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

    # ------------------------------------------------------------------ the hello (D3)

    async def _hello(self, peer: Peer, payload: Any, stream: Stream) -> None:
        """The subscription. It is an ordinary stream and nothing marks it on the wire (WSM-RCN-021).

        The dialer sends it through `connect(hello=...)`, so it is replayed verbatim on every socket
        that peer ever gets (WSM-RCN-020) and this runs again, on a brand-new `Peer`, after every
        reconnect. That is the whole of why the acceptor re-indexes here rather than anywhere else:
        an acceptor's `tags` die with their connection (WSM-RCN-033), so whatever it indexes it must
        index again on every connection.

        The reply is a courtesy and nothing depends on it. `connect()`'s reconnect helper owns this
        stream and discards what comes back on it - the acknowledgement WSM-RCN-022 asks for is this
        handler *returning*. Everything the frontend actually needs arrives on the pushed streams
        below, which is what makes the backend the only source of truth about symbols (D1).
        """
        tags = payload.get("tags") if isinstance(payload, dict) else None
        if isinstance(tags, dict):
            # muxws never reads `tags` and defines no key of its own in it (WSM-REG-001/002); these
            # are the frontend's own labels, and the index is built from exactly what it named.
            peer.tags.update(tags)
        # After the writes and never before: the registry indexes, it does not watch (WSM-REG-010).
        self.registry.register(peer)
        self._start_board(peer)
        await stream.reply({"ok": True, "symbols": len(self.market.symbols), "tick_interval": self._tick_interval})

    async def _rate(self, peer: Peer, payload: dict[str, Any], stream: Stream) -> None:
        """Change how often each pushed stream carries a price.

        This exists because the demo's pacing was read as the library's speed - reasonably, since
        nothing on the screen said otherwise. `TICK_INTERVAL` is a `sleep` in the generator below and
        has nothing to do with what the transport can carry: measured over the in-memory transport,
        one peer pair moves ~25,000 frames a second across twenty concurrent streams
        (`muxws/throughput_test.py`), and the board's default asks for eighty.

        The floor is 1 ms rather than zero. At zero the generator becomes a busy loop that starves
        the very event loop it needs to send on, which would demonstrate the opposite of the point.
        """
        _ = peer
        requested = payload.get("interval_ms") if isinstance(payload, dict) else None
        if not isinstance(requested, (int, float)) or not 1 <= requested <= 5_000:
            raise ValueError(f"interval_ms must be between 1 and 5000; got {requested!r}")
        self._tick_interval = float(requested) / 1000.0
        await stream.reply({"ok": True, "interval_ms": requested, "symbols": len(self.market.symbols)})

    # ------------------------------------------------------------------ server push

    def _start_board(self, peer: Peer) -> None:
        """Twenty streams, opened by the acceptor on its own initiative (WSM-INV-002).

        One per symbol rather than one carrying all twenty, because the claim being witnessed is that
        a stream mid-way through a megabyte does not hold the wire: twenty independent lanes plus the
        export is the arrangement in which a FIFO and a round-robin writer look different.
        """
        if peer in self._boards:
            # A second hello on one socket must not double the board. It should not happen - the
            # helper replays a hello per *connection* - but a duplicated tick stream would look like
            # a protocol bug rather than like this.
            return
        self._boards[peer] = [asyncio.create_task(self._push_ticks(peer, symbol)) for symbol in self.market.symbols]
        # Sync, because `on_close` handlers are, and cancelling a task is a synchronous call. Without
        # it a socket that dies with no route around it - a test, say - leaves twenty tasks pushing
        # into a peer that has been dead since WSM-RCN-042 discarded its writer.
        peer.on_close(lambda _reason, target=peer: self.forget(target))

    async def _push_ticks(self, peer: Peer, symbol: str) -> None:
        """One symbol's price stream, for as long as the socket lasts.

        `peer.open()` here is the same call the browser makes for a quote, on the same socket, with
        the same correlation and the same cancellation. There is no push API because there does not
        need to be one (WSM-INV-002).
        """
        try:
            stream = peer.open({"topic": "ticks", "symbol": symbol})
        except (ConnectionLost, ConnectionGoingAway):
            # The socket died between the hello and this task's first turn. Nothing is buffered for a
            # next one (WSM-RCN-042) and there is nothing to report.
            return
        try:
            # The snapshot first, so a row can be drawn before any price has moved: the frontend
            # holds no seed data and this is where a row comes from (D1).
            await stream.send(self.market.snapshot(symbol))
            while True:
                await asyncio.sleep(self._tick_interval)
                await stream.send(self.market.tick(symbol))
        except (StreamReset, StreamClosed):
            # `ConnectionLost` is a `StreamReset`, so this covers the ordinary end of the demo: the
            # socket went away and every live stream failed with it before `on_close` fired.
            return
        finally:
            # A board stream has no natural end, and `close()` lets every live stream finish before
            # it closes the socket (WSM-CON-024/025). Twenty streams that never finish would hold
            # that drain open for its whole window, so the producer that is going away says so. A
            # no-op when the stream is already closed, which is the ordinary path.
            await stream.cancel("the board stopped")

    # ------------------------------------------------------------------ the four call shapes

    async def _quote(self, _peer: Peer, payload: Any, stream: Stream) -> None:
        """Unary: one payload out, exactly one payload back (WSM-API-006)."""
        self.counters.quotes += 1
        await stream.reply(self.market.quote(_symbol_of(payload)))

    async def _history(self, _peer: Peer, payload: Any, stream: Stream) -> None:
        """Streaming, and cancellable. The cancellation is the point (WSM-ERR-012/013).

        When the browser clicks another row mid-load it calls `cancel()` on this stream. The reset
        reaches this peer, which cancels the task this handler is running on, and `CancelledError` is
        raised at whatever it is awaiting. The counter incremented below is how a reader sees that
        the *backend* stopped generating rather than that the frontend stopped looking.
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
            # Re-raised, never swallowed (WSM-ERR-014). A handler that returned normally here would
            # tell the peer this stream finished, and the browser would be shown a completed chart
            # for a request it had already abandoned.
            raise
        # Trailers ride the `end` frame: they are the things only known once the body is finished,
        # and a separate trailing message would be a second frame the browser has to correlate.
        await stream.end(trailers={"symbol": symbol, "points": len(points)})

    async def _depth(self, _peer: Peer, payload: Any, stream: Stream) -> None:
        """A payload comfortably over MAX_FRAME_BYTES. Nothing here calls a splitter (WSM-FRG-010)."""
        self.counters.depths += 1
        await stream.reply(self.market.depth(_symbol_of(payload)))

    async def _export(self, _peer: Peer, payload: Any, stream: Stream) -> None:
        """The stall test. One megabyte, one payload, one stream, and twenty others unaffected.

        This handler is deliberately unremarkable: the whole demonstration is what the *writer* does
        with what it enqueues here, which is to hand the socket back after every 64 KiB fragment so
        each of the twenty tick streams gets its turn (WSM-FRG-018/019, WSM-INV-004).
        """
        self.counters.exports += 1
        await stream.reply(self.market.export(_symbol_of(payload)))

    # ------------------------------------------------------------------ the diagnostics strip

    async def _stats(self, peer: Peer, _payload: Any, stream: Stream) -> None:
        """What the backend knows about itself, asked for over the same socket as everything else."""
        await stream.reply(
            {
                **asdict(self.counters),
                "peers": len(self.registry.peers_for()),
                "streams": len(peer.streams),
                "symbols": self.market.symbols,
                # The board's own period, so a tick-latency readout has a baseline it was told rather
                # than one it guessed. The hello ack carries it too, but `connect(hello=...)` owns
                # that stream and discards what comes back on it (WSM-RCN-022).
                "tick_interval": self._tick_interval,
            }
        )

    async def _kill(self, _peer: Peer, _payload: Any, stream: Stream) -> None:
        """Close every peer, so the browser can watch a reconnect it did not cause.

        Scheduled rather than awaited: `close()` sends `goaway`, drains and closes the socket
        (WSM-CON-025), and doing that inline would take down the socket this handler's own reply is
        still queued on.
        """
        peers = self.registry.peers_for()
        await stream.reply({"closing": len(peers)})
        self._killer = asyncio.create_task(self._close_every_peer(peers))

    async def _close_every_peer(self, peers: list[Peer]) -> None:
        await asyncio.sleep(self._kill_delay)
        for peer in peers:
            # The board first. `close()` drains every live stream before it closes the socket
            # (WSM-CON-024), and the twenty pushed streams are exactly the ones that would never
            # reach an end of their own - the drain would spend its full window waiting for them.
            self.forget(peer)
            try:
                await peer.close(ResetCode.NO_ERROR, "the demo's kill switch")
            except Exception:  # noqa: BLE001 - a socket already gone is what the kill switch wanted
                logger.debug("muxws demo: peer %s was already gone when the kill switch reached it", peer.id)


def _symbol_of(payload: Any) -> str:
    """Every action but `hello`, `stats` and `kill` names a symbol; a request that does not is an error."""
    symbol = payload.get("symbol") if isinstance(payload, dict) else None
    if not isinstance(symbol, str):
        raise ValueError(f"this action needs a symbol; got {symbol!r}")
    return symbol
