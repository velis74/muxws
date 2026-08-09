"""The fake market: deterministic prices, a deep order book, and a megabyte to export.

Nothing here touches a network. Real market data is out of scope for the demo, and a demo that needed
an upstream feed could not be run by a reader at all.

Every number is drawn from a generator seeded from the symbol and the purpose (D2), never from one
shared stream of randomness. That is the difference between "deterministic" and "deterministic if
nobody clicks anything": a shared generator would make a symbol's tenth price depend on whether the
reader happened to ask for an export first, and a screenshot in the documentation would stop being
reproducible the moment the demo was driven differently.
"""

from __future__ import annotations

import random

#: Twenty symbols, because the headline is a 1 MB export fragmenting on one stream while ticks keep
#: arriving on twenty *others*. With one symbol a round-robin writer and a FIFO produce the same wire
#: and WSM-INV-004 has nothing to be witnessed against.
SYMBOLS: tuple[tuple[str, str, float], ...] = (
    ("ACME", "Acme Industrial", 128.40),
    ("BOLT", "Boltworks", 34.15),
    ("CIRQ", "Cirquit Systems", 212.80),
    ("DYNA", "Dynaflow Energy", 57.05),
    ("EMBR", "Ember Materials", 19.90),
    ("FLUX", "Fluxion Labs", 341.25),
    ("GRID", "Gridline Power", 88.60),
    ("HALO", "Halo Optics", 145.75),
    ("IRON", "Ironvale Mining", 42.30),
    ("JUNO", "Juno Logistics", 76.45),
    ("KITE", "Kite Aerospace", 263.10),
    ("LOOM", "Loomcraft Textiles", 11.85),
    ("MESA", "Mesa Foods", 63.70),
    ("NOVA", "Nova Biotics", 198.35),
    ("ORBX", "Orbex Telemetry", 29.55),
    ("PIER", "Pierpoint Shipping", 51.20),
    ("QUAR", "Quarry Cement", 24.65),
    ("RUNE", "Rune Software", 407.90),
    ("SAGE", "Sagewater Utilities", 95.05),
    ("TIDE", "Tidewater Marine", 38.75),
)

#: Fixed rather than drawn from the clock, so the demo starts from the same market every time (D2).
DEFAULT_SEED = 1_234_567

#: One side of the book. 1500 levels a side puts the depth reply around 190 KB - comfortably over
#: MAX_FRAME_BYTES (65_536), so it is fragmented on the way out and reassembled on the way in without
#: the application doing anything at all (WSM-FRG-010).
DEPTH_LEVELS = 1_500

#: The stall test's payload: a little over 1 MB, which the 64 KiB frame cap turns into sixteen
#: fragments. Sixteen chances to starve a 200-byte tick on another stream, and sixteen times it is
#: not starved (WSM-INV-004).
EXPORT_ROWS = 8_000

#: Hex characters of filler per exported row. The filler is drawn rather than repeated so the payload
#: is a megabyte of genuinely different bytes: a megabyte of "aaaa..." would fragment identically but
#: would look like a trick to a reader watching the network tab.
NOTE_WIDTH = 64

#: How many points a history request answers with. Enough that the chart fills in visibly, which is
#: what makes cancelling it mid-flight something a reader can actually catch.
HISTORY_POINTS = 120


def _generator(seed: int, purpose: str, symbol: str) -> random.Random:
    """One generator per (seed, purpose, symbol).

    `random`, not `secrets`: a fake price is not security-sensitive, and `secrets` cannot be seeded -
    which is the whole of D2. The string seed is hashed by `random` itself, so it is stable across
    runs and platforms and owes nothing to `PYTHONHASHSEED`.
    """
    return random.Random(f"{seed}:{purpose}:{symbol}")  # noqa: S311 - a fake price is not a secret


class Instrument:
    """One symbol's price walk. Its own generator, so its Nth tick never depends on the others."""

    def __init__(self, symbol: str, name: str, base_price: float, seed: int) -> None:
        self.symbol = symbol
        self.name = name
        self.open_price = base_price
        self.price = base_price
        self.volume = 0
        self.ticks = 0
        self._random = _generator(seed, "tick", symbol)

    def advance(self) -> dict[str, object]:
        """Move the price one step and report the new row."""
        drift = self._random.uniform(-0.004, 0.004)
        self.price = round(max(1.0, self.price * (1.0 + drift)), 2)
        self.volume += self._random.randint(50, 5_000)
        self.ticks += 1
        return self.row()

    def row(self) -> dict[str, object]:
        """What a board row needs, and nothing else - this goes out twenty times a tick."""
        change = round(self.price - self.open_price, 2)
        return {
            "symbol": self.symbol,
            "name": self.name,
            "price": self.price,
            "open": self.open_price,
            "change": change,
            "change_pct": round(change / self.open_price * 100.0, 2),
            "volume": self.volume,
            "ticks": self.ticks,
        }


class Market:
    """The demo's only source of truth about symbols (D1).

    The frontend holds no seed data of any kind: every symbol it renders arrived over the socket. A
    board that could draw itself without a connection would prove nothing about the transport.
    """

    def __init__(self, seed: int = DEFAULT_SEED) -> None:
        self.seed = seed
        self._instruments = {symbol: Instrument(symbol, name, base, seed) for symbol, name, base in SYMBOLS}

    @property
    def symbols(self) -> list[str]:
        return list(self._instruments)

    def _instrument(self, symbol: object) -> Instrument:
        """A symbol nobody trades is an application error, and reaches the caller as one.

        Raising here is deliberate: the handler lets it out, and WSM-STM-034 turns it into
        `reset(APPLICATION_ERROR)` - never `REFUSED`, which would promise the request had not been
        processed and invite a retry.
        """
        instrument = self._instruments.get(symbol) if isinstance(symbol, str) else None
        if instrument is None:
            raise ValueError(f"no such symbol: {symbol!r}")
        return instrument

    def snapshot(self, symbol: str) -> dict[str, object]:
        """The row as it stands, without advancing it - the first frame of a pushed tick stream."""
        return self._instrument(symbol).row()

    def tick(self, symbol: str) -> dict[str, object]:
        return self._instrument(symbol).advance()

    def quote(self, symbol: str) -> dict[str, object]:
        """The unary shape's answer: one payload, and the stream ends with it (WSM-API-006)."""
        instrument = self._instrument(symbol)
        spread = round(max(0.01, instrument.price * 0.0004), 2)
        return {
            "symbol": instrument.symbol,
            "name": instrument.name,
            "last": instrument.price,
            "bid": round(instrument.price - spread, 2),
            "ask": round(instrument.price + spread, 2),
            "spread": spread,
            "open": instrument.open_price,
            "volume": instrument.volume,
        }

    def history(self, symbol: str, points: int = HISTORY_POINTS) -> list[dict[str, object]]:
        """A price series ending at the current price, oldest first.

        Built whole and streamed one point at a time by the handler. The handler, not this: what the
        streaming shape demonstrates is a response arriving progressively, and a generator that also
        computed the numbers would leave a reader wondering which of the two the pauses came from.
        """
        instrument = self._instrument(symbol)
        generator = _generator(self.seed, "history", symbol)
        price = instrument.open_price
        series: list[dict[str, object]] = []
        for step in range(points):
            price = round(max(1.0, price * (1.0 + generator.uniform(-0.012, 0.012))), 2)
            series.append({"t": step, "px": price, "volume": generator.randint(1_000, 90_000)})
        return series

    def depth(self, symbol: str, levels: int = DEPTH_LEVELS) -> dict[str, object]:
        """The full order book: one payload the writer has to fragment (WSM-FRG-010)."""
        instrument = self._instrument(symbol)
        generator = _generator(self.seed, "depth", symbol)
        bids: list[dict[str, object]] = []
        asks: list[dict[str, object]] = []
        for level in range(levels):
            step = round(0.01 * (level + 1), 2)
            bids.append(
                {
                    "px": round(instrument.price - step, 2),
                    "qty": generator.randint(100, 9_999),
                    "orders": generator.randint(1, 40),
                    "id": f"{symbol}-B-{level:05d}",
                }
            )
            asks.append(
                {
                    "px": round(instrument.price + step, 2),
                    "qty": generator.randint(100, 9_999),
                    "orders": generator.randint(1, 40),
                    "id": f"{symbol}-A-{level:05d}",
                }
            )
        return {"symbol": symbol, "levels": levels, "bids": bids, "asks": asks}

    def export(self, symbol: str, rows: int = EXPORT_ROWS) -> dict[str, object]:
        """The stall test's payload: one megabyte, as one payload, on one stream.

        The application never calls a splitter. The writer fragments it at MAX_FRAME_BYTES on the way
        out and the receiver reassembles it, and in between every other stream keeps its turn
        (WSM-FRG-017/018/019, WSM-INV-004).
        """
        instrument = self._instrument(symbol)
        generator = _generator(self.seed, "export", symbol)
        price = instrument.open_price
        trades: list[dict[str, object]] = []
        for seq in range(rows):
            price = round(max(1.0, price * (1.0 + generator.uniform(-0.003, 0.003))), 2)
            trades.append(
                {
                    "seq": seq,
                    "symbol": symbol,
                    "px": price,
                    "qty": generator.randrange(100, 10_000, 100),
                    "note": f"{generator.getrandbits(4 * NOTE_WIDTH):0{NOTE_WIDTH}x}",
                }
            )
        return {"symbol": symbol, "count": rows, "trades": trades}
