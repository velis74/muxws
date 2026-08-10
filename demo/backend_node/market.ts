/**
 * The fake market: deterministic prices, a deep order book, and a megabyte to export.
 *
 * A port of `demo/backend_python/market.py`, shape for shape. Nothing here touches a network - real
 * market data is out of scope for a demo, and a demo that needed an upstream feed could not be run by
 * a reader at all.
 *
 * The **numbers deliberately differ from the Python backend** and no effort is made to make them
 * agree. Node has no Mersenne Twister and `Math.random()` cannot be seeded, so the generator below is
 * this file's own; matching Python's draws would mean porting `random.Random` itself, which would buy
 * a price series nobody reads. What has to agree is the *format*, because the format is what the
 * frontend destructures (`demo/frontend/src/muxws.ts`) and the wire is the contract this second
 * backend exists to demonstrate.
 *
 * What does survive the port is D2: every number is drawn from a generator seeded from the symbol and
 * the purpose, never from one shared stream of randomness. That is the difference between
 * "deterministic" and "deterministic if nobody clicks anything" - a shared generator would make a
 * symbol's tenth price depend on whether the reader happened to ask for an export first, and a
 * screenshot in the documentation would stop being reproducible the moment the demo was driven
 * differently.
 */

/**
 * Twenty symbols, because the headline is a 1 MB export fragmenting on one stream while ticks keep
 * arriving on twenty *others*. With one symbol a round-robin writer and a FIFO produce the same wire
 * and WSM-INV-004 has nothing to be witnessed against.
 *
 * The same twenty the Python backend trades, so a reader switching backends sees the same board and
 * only the prices move differently.
 */
export const SYMBOLS: readonly [string, string, number][] = [
  ['ACME', 'Acme Industrial', 128.4],
  ['BOLT', 'Boltworks', 34.15],
  ['CIRQ', 'Cirquit Systems', 212.8],
  ['DYNA', 'Dynaflow Energy', 57.05],
  ['EMBR', 'Ember Materials', 19.9],
  ['FLUX', 'Fluxion Labs', 341.25],
  ['GRID', 'Gridline Power', 88.6],
  ['HALO', 'Halo Optics', 145.75],
  ['IRON', 'Ironvale Mining', 42.3],
  ['JUNO', 'Juno Logistics', 76.45],
  ['KITE', 'Kite Aerospace', 263.1],
  ['LOOM', 'Loomcraft Textiles', 11.85],
  ['MESA', 'Mesa Foods', 63.7],
  ['NOVA', 'Nova Biotics', 198.35],
  ['ORBX', 'Orbex Telemetry', 29.55],
  ['PIER', 'Pierpoint Shipping', 51.2],
  ['QUAR', 'Quarry Cement', 24.65],
  ['RUNE', 'Rune Software', 407.9],
  ['SAGE', 'Sagewater Utilities', 95.05],
  ['TIDE', 'Tidewater Marine', 38.75],
];

/** Fixed rather than drawn from the clock, so the demo starts from the same market every time (D2). */
export const DEFAULT_SEED = 1_234_567;

/**
 * One side of the book. 1500 levels a side puts the depth reply around 190 KB - comfortably over
 * MAX_FRAME_BYTES (65_536), so it is fragmented on the way out and reassembled on the way in without
 * the application doing anything at all (WSM-FRG-010).
 */
export const DEPTH_LEVELS = 1_500;

/**
 * The stall test's payload: a little over 1 MB, which the 64 KiB frame cap turns into sixteen
 * fragments. Sixteen chances to starve a 200-byte tick on another stream, and sixteen times it is not
 * starved (WSM-INV-004).
 */
export const EXPORT_ROWS = 8_000;

/**
 * Hex characters of filler per exported row. The filler is drawn rather than repeated so the payload
 * is a megabyte of genuinely different bytes: a megabyte of "aaaa..." would fragment identically but
 * would look like a trick to a reader watching the network tab.
 */
export const NOTE_WIDTH = 64;

/**
 * How many points a history request answers with. Enough that the chart fills in visibly, which is
 * what makes cancelling it mid-flight something a reader can actually catch.
 */
export const HISTORY_POINTS = 120;

// --------------------------------------------------------------------------- what the wire carries

/** One board row. The same eight fields `BoardRow` in `demo/frontend/src/muxws.ts` destructures. */
export interface BoardRow {
  symbol: string;
  name: string;
  price: number;
  open: number;
  change: number;
  change_pct: number;
  volume: number;
  ticks: number;
}

export interface Quote {
  symbol: string;
  name: string;
  last: number;
  bid: number;
  ask: number;
  spread: number;
  open: number;
  volume: number;
}

export interface HistoryPoint {
  t: number;
  px: number;
  volume: number;
}

export interface DepthLevel {
  px: number;
  qty: number;
  orders: number;
  id: string;
}

export interface DepthBook {
  symbol: string;
  levels: number;
  bids: DepthLevel[];
  asks: DepthLevel[];
}

export interface ExportTrade {
  seq: number;
  symbol: string;
  px: number;
  qty: number;
  note: string;
}

export interface ExportPayload {
  symbol: string;
  count: number;
  trades: ExportTrade[];
}

// --------------------------------------------------------------------------- the seeded generator

/**
 * FNV-1a over the seed string, because `mulberry32` wants 32 bits and D2 wants a *string* seed.
 *
 * Not `hashCode`-by-multiplication and not `Math.random()`: the seed has to be a pure function of
 * `(seed, purpose, symbol)` and of nothing else, or the same symbol's tenth price stops being the
 * same number across runs and the screenshot in the documentation stops being true.
 */
function seedOf(text: string): number {
  let hash = 0x811c9dc5;
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

/**
 * mulberry32: thirty-two bits of state, four operations, and a period long enough for a demo.
 *
 * A fake price is not security-sensitive, which is the whole reason a seedable generator is allowed
 * here at all - `crypto.getRandomValues` cannot be seeded, and D2 needs it to be.
 */
function mulberry32(state: number): () => number {
  let value = state >>> 0;
  return () => {
    value = (value + 0x6d2b79f5) >>> 0;
    let mixed = Math.imul(value ^ (value >>> 15), 1 | value);
    mixed = (mixed + Math.imul(mixed ^ (mixed >>> 7), 61 | mixed)) ^ mixed;
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 4_294_967_296;
  };
}

/**
 * The four draws this market needs, spelled the way `random.Random` spells them.
 *
 * Named after their Python counterparts on purpose: the two `market` modules are meant to be read
 * side by side, and a reader comparing them should be comparing the market rather than translating
 * `randint` into `Math.floor` twice a line.
 */
class Rng {
  private readonly next: () => number;

  constructor(seed: string) {
    this.next = mulberry32(seedOf(seed));
  }

  /** `random.uniform`: a float in [low, high). */
  uniform(low: number, high: number): number {
    return low + (high - low) * this.next();
  }

  /** `random.randint`: an integer in [low, high], **inclusive** at both ends as Python's is. */
  randint(low: number, high: number): number {
    return low + Math.floor(this.next() * (high - low + 1));
  }

  /** `random.randrange(start, stop, step)`: `start`, `start + step`, ... below `stop`. */
  randrange(start: number, stop: number, step: number): number {
    return start + step * Math.floor(this.next() * Math.ceil((stop - start) / step));
  }

  /** `f"{getrandbits(4 * width):0{width}x}"`: exactly `width` hex characters, eight bits per draw. */
  hex(width: number): string {
    let out = '';
    while (out.length < width) {
      out += Math.floor(this.next() * 4_294_967_296)
        .toString(16)
        .padStart(8, '0');
    }
    return out.slice(0, width);
  }
}

/** One generator per (seed, purpose, symbol), which is the whole of D2. */
function generator(seed: number, purpose: string, symbol: string): Rng {
  return new Rng(`${seed}:${purpose}:${symbol}`);
}

/**
 * Two decimals, the way a price is quoted.
 *
 * `Math.round` is half-up where Python's `round` is half-to-even, so a value landing exactly on a
 * half rounds the other way here. That is one more reason the two backends' numbers differ and no
 * reason to reach for a decimal library: the frontend renders whatever arrives.
 */
function round2(value: number): number {
  return Math.round(value * 100) / 100;
}

// --------------------------------------------------------------------------- the market

/** One symbol's price walk. Its own generator, so its Nth tick never depends on the others. */
class Instrument {
  readonly symbol: string;

  readonly name: string;

  readonly openPrice: number;

  price: number;

  volume = 0;

  ticks = 0;

  private readonly random: Rng;

  constructor(symbol: string, name: string, basePrice: number, seed: number) {
    this.symbol = symbol;
    this.name = name;
    this.openPrice = basePrice;
    this.price = basePrice;
    this.random = generator(seed, 'tick', symbol);
  }

  /** Move the price one step and report the new row. */
  advance(): BoardRow {
    const drift = this.random.uniform(-0.004, 0.004);
    this.price = round2(Math.max(1.0, this.price * (1.0 + drift)));
    this.volume += this.random.randint(50, 5_000);
    this.ticks += 1;
    return this.row();
  }

  /** What a board row needs, and nothing else - this goes out twenty times a tick. */
  row(): BoardRow {
    const change = round2(this.price - this.openPrice);
    return {
      symbol: this.symbol,
      name: this.name,
      price: this.price,
      open: this.openPrice,
      change,
      change_pct: round2((change / this.openPrice) * 100.0),
      volume: this.volume,
      ticks: this.ticks,
    };
  }
}

/**
 * The demo's only source of truth about symbols (D1).
 *
 * The frontend holds no seed data of any kind: every symbol it renders arrived over the socket. A
 * board that could draw itself without a connection would prove nothing about the transport - and it
 * would prove even less here, where the point is that the frontend cannot tell which language filled
 * it in.
 */
export class Market {
  readonly seed: number;

  private readonly instruments: Map<string, Instrument>;

  constructor(seed: number = DEFAULT_SEED) {
    this.seed = seed;
    this.instruments = new Map(
      SYMBOLS.map(([symbol, name, base]) => [symbol, new Instrument(symbol, name, base, seed)]),
    );
  }

  get symbols(): string[] {
    return [...this.instruments.keys()];
  }

  /**
   * A symbol nobody trades is an application error, and reaches the caller as one.
   *
   * Throwing here is deliberate: the handler lets it out, and WSM-STM-034 turns it into
   * `reset(APPLICATION_ERROR)` - never `REFUSED`, which would promise the request had not been
   * processed and invite a retry.
   */
  private instrument(symbol: unknown): Instrument {
    const found = typeof symbol === 'string' ? this.instruments.get(symbol) : undefined;
    if (found === undefined) throw new Error(`no such symbol: ${JSON.stringify(symbol)}`);
    return found;
  }

  /** The row as it stands, without advancing it - the first frame of a pushed tick stream. */
  snapshot(symbol: string): BoardRow {
    return this.instrument(symbol).row();
  }

  tick(symbol: string): BoardRow {
    return this.instrument(symbol).advance();
  }

  /** The unary shape's answer: one payload, and the stream ends with it (WSM-API-006). */
  quote(symbol: string): Quote {
    const instrument = this.instrument(symbol);
    const spread = round2(Math.max(0.01, instrument.price * 0.0004));
    return {
      symbol: instrument.symbol,
      name: instrument.name,
      last: instrument.price,
      bid: round2(instrument.price - spread),
      ask: round2(instrument.price + spread),
      spread,
      open: instrument.openPrice,
      volume: instrument.volume,
    };
  }

  /**
   * A price series ending at the current price, oldest first.
   *
   * Built whole and streamed one point at a time by the handler. The handler, not this: what the
   * streaming shape demonstrates is a response arriving progressively, and a generator that also
   * computed the numbers would leave a reader wondering which of the two the pauses came from.
   */
  history(symbol: string, points: number = HISTORY_POINTS): HistoryPoint[] {
    const instrument = this.instrument(symbol);
    const random = generator(this.seed, 'history', symbol);
    let price = instrument.openPrice;
    const series: HistoryPoint[] = [];
    for (let step = 0; step < points; step += 1) {
      price = round2(Math.max(1.0, price * (1.0 + random.uniform(-0.012, 0.012))));
      series.push({ t: step, px: price, volume: random.randint(1_000, 90_000) });
    }
    return series;
  }

  /** The full order book: one payload the writer has to fragment (WSM-FRG-010). */
  depth(symbol: string, levels: number = DEPTH_LEVELS): DepthBook {
    const instrument = this.instrument(symbol);
    const random = generator(this.seed, 'depth', symbol);
    const bids: DepthLevel[] = [];
    const asks: DepthLevel[] = [];
    for (let level = 0; level < levels; level += 1) {
      const step = round2(0.01 * (level + 1));
      const padded = String(level).padStart(5, '0');
      bids.push({
        px: round2(instrument.price - step),
        qty: random.randint(100, 9_999),
        orders: random.randint(1, 40),
        id: `${symbol}-B-${padded}`,
      });
      asks.push({
        px: round2(instrument.price + step),
        qty: random.randint(100, 9_999),
        orders: random.randint(1, 40),
        id: `${symbol}-A-${padded}`,
      });
    }
    return { symbol, levels, bids, asks };
  }

  /**
   * The stall test's payload: one megabyte, as one payload, on one stream.
   *
   * The application never calls a splitter. The writer fragments it at MAX_FRAME_BYTES on the way out
   * and the receiver reassembles it, and in between every other stream keeps its turn
   * (WSM-FRG-017/018/019, WSM-INV-004).
   */
  export(symbol: string, rows: number = EXPORT_ROWS): ExportPayload {
    const instrument = this.instrument(symbol);
    const random = generator(this.seed, 'export', symbol);
    let price = instrument.openPrice;
    const trades: ExportTrade[] = [];
    for (let seq = 0; seq < rows; seq += 1) {
      price = round2(Math.max(1.0, price * (1.0 + random.uniform(-0.003, 0.003))));
      trades.push({
        seq,
        symbol,
        px: price,
        qty: random.randrange(100, 10_000, 100),
        note: random.hex(NOTE_WIDTH),
      });
    }
    return { symbol, count: rows, trades };
  }
}
