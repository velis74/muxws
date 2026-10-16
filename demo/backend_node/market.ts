/**
 * The simulated market: deterministic prices, a 1500-level order book and a 1 MB export.
 *
 * A port of `demo/backend_python/market.py` with the same data shapes. It makes no network calls; all
 * data is generated locally.
 *
 * The numbers differ from the Python backend's. Node has no Mersenne Twister and `Math.random()`
 * cannot be seeded, so this file uses its own generator. The payload format is identical to the Python
 * backend's, because the frontend destructures it (`demo/frontend/src/muxws.ts`).
 *
 * Every number is drawn from a generator seeded from the market seed, a purpose and the symbol, not
 * from one shared generator. A symbol's Nth price is therefore the same in every run regardless of
 * which other requests were made before it.
 */

/**
 * The twenty traded symbols: `[symbol, name, base price]`. The demo sends a 1 MB export on one stream
 * while ticks arrive on twenty other streams; with a single stream, a round-robin writer and a FIFO
 * writer would produce the same frame order and WSM-INV-004 would not be observable.
 *
 * The list is identical to the Python backend's.
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

/** A fixed seed, so every run starts from the same market. */
export const DEFAULT_SEED = 1_234_567;

/**
 * Levels per side of the order book. With 1500 levels a side the depth reply is about 190 KB, above
 * MAX_FRAME_BYTES (65_536), so the writer fragments it and the receiver reassembles it without
 * application code (WSM-FRG-010).
 */
export const DEPTH_LEVELS = 1_500;

/**
 * Rows in the export payload: a little over 1 MB, which the 64 KiB frame limit splits into sixteen
 * fragments. Ticks on other streams are interleaved between those fragments (WSM-INV-004).
 */
export const EXPORT_ROWS = 8_000;

/**
 * Hex characters of filler per exported row. The filler is random, so the payload contains no
 * repeated content.
 */
export const NOTE_WIDTH = 64;

/**
 * Points in a history reply. The handler sends them one at a time with a delay between points, so the
 * chart fills in progressively and the request can be cancelled while it streams.
 */
export const HISTORY_POINTS = 120;

// --------------------------------------------------------------------------- what the wire carries

/** One board row; the same eight fields as `BoardRow` in `demo/frontend/src/muxws.ts`. */
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
 * 32-bit FNV-1a hash of the seed string; `mulberry32` takes a 32-bit state and the seed is a string.
 * The result depends only on `text`.
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
 * mulberry32: a seedable PRNG with 32 bits of state, returning floats in [0, 1). Not cryptographically
 * secure; `crypto.getRandomValues` cannot be seeded. The values are simulated prices only.
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
 * The four draws this market uses, named after their `random.Random` counterparts in
 * `demo/backend_python/market.py`.
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

  /** `f"{getrandbits(4 * width):0{width}x}"`: exactly `width` hex characters, 32 bits per draw. */
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

/** A generator seeded from `(seed, purpose, symbol)`. */
function generator(seed: number, purpose: string, symbol: string): Rng {
  return new Rng(`${seed}:${purpose}:${symbol}`);
}

/**
 * Rounds to two decimals.
 *
 * `Math.round` rounds halves up; Python's `round` rounds halves to even. A value exactly on a half can
 * therefore round differently than in the Python backend.
 */
function round2(value: number): number {
  return Math.round(value * 100) / 100;
}

// --------------------------------------------------------------------------- the market

/** One symbol's price walk. It has its own generator, so its Nth tick is independent of other symbols. */
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

  /** The board row for this symbol; contains only the fields the board renders. */
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
 * The only source of symbol data in the demo.
 *
 * The frontend holds no symbol data; every symbol it renders is received over the socket.
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
   * Returns the instrument for `symbol`, or throws `Error` for an unknown symbol.
   *
   * The handler does not catch the error, so the stream is reset with `APPLICATION_ERROR`
   * (WSM-STM-034). `REFUSED` would state that the request was not processed and may be retried.
   */
  private instrument(symbol: unknown): Instrument {
    const found = typeof symbol === 'string' ? this.instruments.get(symbol) : undefined;
    if (found === undefined) throw new Error(`no such symbol: ${JSON.stringify(symbol)}`);
    return found;
  }

  /** The current row, without advancing the price; the first frame of a pushed tick stream. */
  snapshot(symbol: string): BoardRow {
    return this.instrument(symbol).row();
  }

  tick(symbol: string): BoardRow {
    return this.instrument(symbol).advance();
  }

  /** The reply to a unary request: one payload, and the stream ends with it (WSM-API-006). */
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
   * A price series of `points` points, oldest first.
   *
   * The series is computed in full here; the handler streams it one point at a time.
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

  /** The full order book as one payload, larger than one frame, so the writer fragments it (WSM-FRG-010). */
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
   * The export payload: about 1 MB as one payload on one stream.
   *
   * The application does not split it. The writer fragments it at MAX_FRAME_BYTES and the receiver
   * reassembles it; frames of other streams are interleaved between the fragments
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
