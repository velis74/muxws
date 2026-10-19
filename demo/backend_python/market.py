"""The fake market: deterministic prices, a deep order book, and a megabyte to export.

Nothing here uses a network; the demo runs without an upstream data feed.

Every number is drawn from a generator seeded from the symbol and the purpose, not from one
shared generator. With a shared generator a symbol's tenth price would depend on which other
requests (for example an export) ran first; with separate generators the values are the same on
every run regardless of user actions.
"""

from __future__ import annotations

import random

#: Twenty symbols, so that ticks arrive on twenty other streams while a 1 MB export is fragmented on
#: one. With one symbol a round-robin writer and a FIFO writer produce the same frame order, and
#: WSM-INV-004 is not observable.
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

#: Fixed, not taken from the clock, so the demo starts from the same market every time.
DEFAULT_SEED = 1_234_567

#: Levels per side of the book. 1500 levels a side make the depth reply about 190 KB, larger than
#: MAX_FRAME_BYTES (65_536), so it is fragmented when sent and reassembled when received without
#: application code (WSM-FRG-010).
DEPTH_LEVELS = 1_500

#: The stall test's payload: a little over 1 MB, sent as sixteen fragments under the 64 KiB frame
#: limit. Ticks of about 200 bytes on other streams are sent between the fragments (WSM-INV-004).
EXPORT_ROWS = 8_000

#: Hex characters of filler per exported row. The filler is random, not repeated, so the payload in
#: the browser's network tab is a megabyte of varied data; repeated filler fragments identically.
NOTE_WIDTH = 64

#: Number of points a history request returns. The chart fills in gradually, so the stream can be
#: cancelled while it is in progress.
HISTORY_POINTS = 120


def _generator(seed: int, purpose: str, symbol: str) -> random.Random:
    """One generator per (seed, purpose, symbol).

    `random`, not `secrets`: a fake price is not security-sensitive, and `secrets` cannot be seeded.
    `random` hashes the string seed itself, so it is stable across runs and platforms and
    independent of `PYTHONHASHSEED`.
    """
    return random.Random(f"{seed}:{purpose}:{symbol}")  # noqa: S311 - a fake price is not a secret


class Instrument:
    """One symbol's price walk, with its own generator so its Nth tick does not depend on other symbols."""

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
        """The fields of one board row; sent twenty times per tick."""
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
    """The demo's only source of truth about symbols.

    The frontend holds no seed data: every symbol it renders arrives over the socket, so the board is
    empty without a connection.
    """

    def __init__(self, seed: int = DEFAULT_SEED) -> None:
        self.seed = seed
        self._instruments = {symbol: Instrument(symbol, name, base, seed) for symbol, name, base in SYMBOLS}

    @property
    def symbols(self) -> list[str]:
        return list(self._instruments)

    def _instrument(self, symbol: object) -> Instrument:
        """Look up a symbol; an unknown symbol raises `ValueError`.

        The handler does not catch it, and WSM-STM-034 turns it into `reset(APPLICATION_ERROR)`, not
        `REFUSED`, which would state that the request was not processed and may be retried.
        """
        instrument = self._instruments.get(symbol) if isinstance(symbol, str) else None
        if instrument is None:
            raise ValueError(f"no such symbol: {symbol!r}")
        return instrument

    def snapshot(self, symbol: str) -> dict[str, object]:
        """The current row, without advancing; the first frame of a pushed tick stream."""
        return self._instrument(symbol).row()

    def tick(self, symbol: str) -> dict[str, object]:
        return self._instrument(symbol).advance()

    def quote(self, symbol: str) -> dict[str, object]:
        """The unary answer: one payload, which also ends the stream (WSM-API-006)."""
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

        Built in full here; the handler streams it one point at a time with pauses between points.
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
        """The full order book: one payload that the writer fragments (WSM-FRG-010)."""
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

        The application does not split it. The writer fragments it at MAX_FRAME_BYTES, the receiver
        reassembles it, and other streams' frames are sent between the fragments
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
