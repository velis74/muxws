/**
 * Tests of the Node demo backend over the in-memory transport; the counterpart of
 * `demo/backend_python/handlers_test.py`.
 *
 * The test `interleaves tick frames between the fragments of a 1 MB export` asserts on the frames
 * recorded by `onFrame`, so it fails if the round-robin writer is replaced by a FIFO writer.
 *
 * Where the two ports differ (TypeScript observes a `break` out of a `for await`, `closed` is a
 * promise, and an abort signal replaces task cancellation), the tests assert the TypeScript behaviour.
 */

import {
  type CloseReason,
  type Frame,
  JsonCodec,
  MAX_FRAME_BYTES,
  type MemorySocket,
  memoryPair,
  Peer,
  PeerRegistry,
  RemoteError,
  type SocketAdapter,
  type Stream,
  StreamReset,
  StreamState,
} from '../../ts/index';
import type { Writer } from '../../ts/writer';

import { type Counters, MarketService, type MarketServiceOptions } from './handlers';
import {
  type BoardRow,
  type DepthBook,
  EXPORT_ROWS,
  type ExportPayload,
  HISTORY_POINTS,
  type HistoryPoint,
  Market,
  type Quote,
  SYMBOLS,
} from './market';

// --------------------------------------------------------------------------- the harness

/** The `stats` reply; the same fields as `Stats` in the frontend. */
interface Stats extends Counters {
  peers: number;
  streams: number;
  symbols: string[];
  tick_interval: number;
}

interface HelloAck {
  ok: boolean;
  symbols: number;
  tick_interval: number;
}

interface RateAck {
  ok: boolean;
  interval_ms: number;
  symbols: number;
}

interface Seen {
  readonly direction: 'tx' | 'rx';
  readonly frame: Frame;
  readonly byteLength: number;
}

/**
 * Logs a diagnostic line when `MUXWS_VERBOSE` is set; otherwise does nothing.
 *
 * Every call is next to an `expect` on the same value, and a failing assertion prints its actual value
 * regardless of this setting.
 */
function note(line: string): void {
  const verbose = process.env.MUXWS_VERBOSE;
  if (verbose !== undefined && verbose !== '') console.log(line);
}

function pause(ms: number): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

/** Frames each tick stream has queued when the export's sends are released, more than the export has fragments. */
const BACKLOG = 32;

/**
 * A `MemorySocket` whose sends wait while it is held.
 *
 * When not held, a send is the wrapped socket's synchronous send. When held, the write loop waits at
 * its next send and every frame enqueued meanwhile stays in the writer, so a test can fix the set of
 * queued frames before the writer chooses among them.
 */
class GatedSocket implements SocketAdapter {
  private held: Promise<void> | null = null;

  private release: (() => void) | null = null;

  constructor(private readonly socket: MemorySocket) {}

  hold(): void {
    this.held = new Promise<void>((resolve) => {
      this.release = resolve;
    });
  }

  open(): void {
    this.release?.();
    this.held = null;
    this.release = null;
  }

  sendText(text: string): Promise<void> | void {
    if (this.held === null) return this.socket.sendText(text);
    return this.held.then(() => this.socket.sendText(text));
  }

  sendBytes(bytes: ArrayBuffer): Promise<void> | void {
    if (this.held === null) return this.socket.sendBytes(bytes);
    return this.held.then(() => this.socket.sendBytes(bytes));
  }

  receive(): Promise<string | ArrayBuffer> {
    return this.socket.receive();
  }

  close(code?: number, reason?: string): void {
    this.socket.close(code, reason);
  }
}

/**
 * A browser peer and a backend peer connected to each other, with the demo's handler on the acceptor.
 *
 * The transport is a `MemorySocket`, as in the other tests in this repository. The write loop takes one
 * event-loop turn per frame, so several streams have frames queued at each choice and WSM-INV-004 is
 * observable without a paced socket. The backend's sends pass a `GatedSocket`, which is open unless a
 * test holds it.
 */
class Wire {
  readonly dialer: Peer;

  readonly acceptor: Peer;

  readonly service: MarketService;

  /**
   * Every frame the browser received, in arrival order, recorded by the WSM-OBS-003 `onFrame` hook.
   */
  readonly seen: Seen[] = [];

  /**
   * The latest board row per symbol, filled only from the pushed tick streams.
   */
  readonly board = new Map<string, BoardRow>();

  /**
   * The first row each symbol pushed (the snapshot), kept separately because later rows overwrite
   * `board`.
   */
  readonly first = new Map<string, BoardRow>();

  /**
   * Every `CloseReason` passed to the browser's close handler.
   */
  readonly closes: CloseReason[] = [];

  readonly backend: GatedSocket;

  private readonly sockets: MemorySocket[];

  private tasks: Promise<void>[] = [];

  constructor(options: MarketServiceOptions = {}) {
    const codec = new JsonCodec();
    const [browserSide, backendSide] = memoryPair();
    this.dialer = new Peer(browserSide, { codec, isDialer: true });
    this.backend = new GatedSocket(backendSide);
    this.acceptor = new Peer(this.backend, { codec, isDialer: false });
    this.service = new MarketService(new Market(), new PeerRegistry(), options);
    this.acceptor.onStream(this.service.handlerFor(this.acceptor));
    this.sockets = [browserSide, backendSide];
  }

  start(): void {
    this.dialer.onFrame((direction, frame, byteLength) => {
      this.seen.push({ direction, frame, byteLength });
    });
    // Registered before anything is sent: the backend pushes the board as soon as it receives the
    // hello, and a handler registered one `await` later would see those streams reset with
    // `REFUSED, "no on_stream handler"` (WSM-STM-033). `connect()` takes `onStream` as an option for
    // the same reason.
    this.dialer.onStream(this.onPush.bind(this));
    this.dialer.onClose((reason) => {
      this.closes.push(reason);
    });
    this.tasks = [this.dialer.serve(), this.acceptor.serve()];
    // Dropping the socket in teardown ends both read loops. Their errors are ignored, because an
    // unhandled rejection would fail an unrelated test.
    this.tasks.forEach((task) => {
      void task.catch(() => undefined);
    });
  }

  /** The browser's own `onStream` handler: the acceptor's push arrives here (WSM-INV-002). */
  private async onPush(payload: unknown, stream: Stream): Promise<void> {
    const symbol = (payload as { symbol: string }).symbol;
    for await (const row of stream as Stream<BoardRow>) {
      if (!this.first.has(symbol)) this.first.set(symbol, row);
      this.board.set(symbol, row);
    }
  }

  /** Sends the hello that `connect({hello})` would send, directly, so the reply can be read. */
  async hello(tags?: Record<string, unknown>): Promise<HelloAck> {
    return this.dialer.request<HelloAck>({ action: 'hello', tags: tags ?? { topic: 'board' } });
  }

  received(direction: 'tx' | 'rx' = 'rx'): Frame[] {
    return this.seen.filter((entry) => entry.direction === direction).map((entry) => entry.frame);
  }

  /** Waits until every symbol has a row on the board. */
  async untilTheBoardIsLive(timeoutMs = 5_000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (this.board.size < SYMBOLS.length) {
      if (Date.now() > deadline) {
        throw new Error(`only ${this.board.size} of ${SYMBOLS.length} tick streams started`);
      }
      await pause(5);
    }
  }

  /** The ids of the streams the backend pushed, one per symbol. */
  tickStreams(): number[] {
    return [...this.acceptor.streams.values()].filter((stream) => stream.local).map((stream) => stream.id);
  }

  /**
   * Waits until `streamId` has a frame queued and every tick stream has `depth` frames queued.
   *
   * Used only while `backend` is held, so the writer keeps every enqueued frame.
   */
  async untilBacklogged(streamId: number, depth: number, timeoutMs = 5_000): Promise<void> {
    const { writer } = this.acceptor as unknown as { writer: Writer };
    const deadline = Date.now() + timeoutMs;
    while (
      writer.depthOf(streamId) === 0 ||
      Math.min(...this.tickStreams().map((lane) => writer.depthOf(lane))) < depth
    ) {
      if (Date.now() > deadline) throw new Error(`the tick streams did not queue ${depth} frames each`);
      await pause(5);
    }
  }

  async stop(): Promise<void> {
    this.backend.open();
    this.service.forget(this.acceptor);
    this.sockets.forEach((socket) => {
      socket.drop();
    });
    await Promise.allSettled(this.tasks);
    // Waits one turn after the drop, so sleeping generators observe the aborted board and finish
    // before the next test starts.
    await pause(5);
  }
}

/** Runs `body` with a connected browser peer and backend peer using the demo's `MarketService`. */
async function marketWire(options: MarketServiceOptions, body: (wire: Wire) => Promise<void>): Promise<void> {
  const wire = new Wire(options);
  wire.start();
  try {
    await body(wire);
  } finally {
    await wire.stop();
  }
}

function isFragment(frame: Frame): boolean {
  return frame.fragment !== null && frame.fragment !== undefined;
}

// --------------------------------------------------------------------------- stream interleaving

it('interleaves tick frames between the fragments of a 1 MB export', async () => {
  // WSM-INV-004: a 1 MB payload on one stream does not block the twenty tick streams.
  //
  // Asserted on the frames recorded by the browser's `onFrame`. The backend's sends are held while the
  // export is enqueued, until every tick stream has more frames queued than the export has fragments;
  // then they are released, so the send order does not depend on scheduling. A FIFO writer would send
  // the queued ticks first and then every export fragment consecutively. The round-robin writer sends
  // one export fragment per turn and one frame of every tick stream between them (WSM-FRG-017/018/019).
  await marketWire({ tickIntervalMs: 1 }, async (wire) => {
    await wire.hello();
    await wire.untilTheBoardIsLive();
    const ticks = wire.tickStreams().sort((a, b) => a - b);
    expect(ticks).toHaveLength(SYMBOLS.length);

    wire.backend.hold();
    const exported = wire.dialer.open<ExportPayload>({ action: 'export', symbol: 'ACME' }, { end: true });
    await wire.untilBacklogged(exported.id, BACKLOG);
    wire.backend.open();
    const payload = await exported;

    const received = wire.received();
    const fragments = received
      .map((frame, index) => ({ frame, index }))
      .filter(({ frame }) => frame.stream === exported.id && frame.type === 'data' && isFragment(frame))
      .map(({ index }) => index);

    note(
      `export fragments=${fragments.length} span=${fragments[0]}..${fragments[fragments.length - 1]} ` +
        `of ${received.length} frames`,
    );
    // The export was fragmented.
    expect(fragments.length).toBeGreaterThanOrEqual(8);
    expect(fragments.length, 'the tick streams had no frames queued before the export finished').toBeLessThan(BACKLOG);
    expect(payload.count).toBe(EXPORT_ROWS);

    // Between any two fragments of the export, every tick stream delivered exactly one frame and no
    // other frame was sent: the rotation serves every stream once before serving any stream twice.
    fragments.slice(1).forEach((after, position) => {
      const before = fragments[position];
      const between = received
        .slice(before + 1, after)
        .map((frame) => frame.stream as number)
        .sort((a, b) => a - b);
      expect(between, `frames ${before}..${after}`).toEqual(ticks);
    });
  });
}, 30_000);

// --------------------------------------------------------------------------- actions

it('answers every action the frontend can name', async () => {
  await marketWire({ tickIntervalMs: 500, historyDelayMs: 0 }, async (wire) => {
    // The eight actions the frontend may send, which `dispatch` routes; the Python backend has the
    // same eight.
    expect(wire.service.actions).toEqual(['hello', 'quote', 'history', 'depth', 'export', 'stats', 'rate', 'kill']);

    const acknowledgement = await wire.hello({ topic: 'board', session: 'demo' });
    expect(acknowledgement.ok).toBe(true);
    expect(acknowledgement.symbols).toBe(SYMBOLS.length);
    // The peer is registered under the tags the frontend sent, and under no tags added by muxws.
    expect(wire.service.registry.peersFor({ topic: 'board' })).toEqual([wire.acceptor]);
    expect(wire.service.registry.peersFor({ session: 'demo' })).toEqual([wire.acceptor]);

    // Server push: the browser requested no symbol list and receives all twenty symbols on streams the
    // acceptor opened, through the browser's own `onStream` handler (WSM-INV-002).
    await wire.untilTheBoardIsLive();
    expect([...wire.board.keys()].sort()).toEqual(SYMBOLS.map(([symbol]) => symbol).sort());
    // The first frame of every pushed stream is the snapshot, not a tick, so the grid can be drawn
    // immediately after the hello instead of one tick interval later.
    expect([...wire.first.values()].map((row) => row.ticks)).toEqual(SYMBOLS.map(() => 0));

    const quote = await wire.dialer.request<Quote>({ action: 'quote', symbol: 'ACME' });
    expect(quote.symbol).toBe('ACME');
    expect(quote.bid).toBeLessThan(quote.last);
    expect(quote.last).toBeLessThan(quote.ask);

    const history = wire.dialer.open<HistoryPoint>({ action: 'history', symbol: 'ACME' }, { end: true });
    const points: HistoryPoint[] = [];
    for await (const point of history) points.push(point);
    expect(points).toHaveLength(HISTORY_POINTS);
    expect(points.map((point) => point.t)).toEqual([...Array(HISTORY_POINTS).keys()]);
    // Trailers are carried on the `end` frame and are readable after the iteration completes.
    expect(history.trailers).toEqual({ symbol: 'ACME', points: HISTORY_POINTS });

    const before = wire.seen.length;
    const depth = await wire.dialer.request<DepthBook>({ action: 'depth', symbol: 'ACME' });
    expect(depth.bids).toHaveLength(depth.levels);
    expect(depth.asks).toHaveLength(depth.levels);
    expect(depth.bids[0].px).toBeLessThan(depth.asks[0].px);
    // WSM-FRG-010: the depth payload exceeds MAX_FRAME_BYTES, so it was fragmented and reassembled.
    // The three assertions above would also pass for a five-level book that fits in one frame. Neither
    // the handler nor this test requests fragmentation.
    const pieces = wire.seen
      .slice(before)
      .filter((entry) => entry.direction === 'rx' && isFragment(entry.frame))
      .map((entry) => entry.byteLength);
    const bytes = pieces.reduce((total, length) => total + length, 0);
    note(`depth: ${pieces.length} fragments, ${bytes} bytes on the wire`);
    expect(pieces.length).toBeGreaterThan(1);
    expect(bytes).toBeGreaterThan(MAX_FRAME_BYTES);

    const exported = await wire.dialer.request<ExportPayload>({ action: 'export', symbol: 'ACME' });
    expect(exported.trades).toHaveLength(EXPORT_ROWS);

    const stats = await wire.dialer.request<Stats>({ action: 'stats' });
    expect(stats.symbols).toEqual(SYMBOLS.map(([symbol]) => symbol));
    expect(stats.quotes).toBe(1);
    expect(stats.exports).toBe(1);
    expect(stats.peers).toBe(1);

    // An unknown action is an application error, not a refusal: REFUSED would state that the request
    // was not processed and may be retried (WSM-STM-034).
    await expect(wire.dialer.request({ action: 'buy', symbol: 'ACME' })).rejects.toBeInstanceOf(RemoteError);
  });
}, 30_000);

it('changes the tick interval on a rate request', async () => {
  // The tick interval is a `sleep` in the demo's generator, not a library limit. This asserts that
  // `rate` changes it and rejects values outside 1..5000 ms.
  await marketWire({}, async (wire) => {
    const answer = await wire.dialer.request<RateAck>({ action: 'rate', interval_ms: 10 });
    expect(answer.ok).toBe(true);
    expect(answer.interval_ms).toBe(10);
    expect(wire.service.tickIntervalMs).toBe(10);

    const stats = await wire.dialer.request<Stats>({ action: 'stats' });
    // `tick_interval` is in seconds on the wire, as the Python backend sends it; this port stores
    // milliseconds.
    expect(stats.tick_interval).toBe(0.01);

    for (const refused of [0, -5, 10_000]) {
      await expect(wire.dialer.request({ action: 'rate', interval_ms: refused })).rejects.toBeInstanceOf(RemoteError);
    }
    expect(wire.service.tickIntervalMs).toBe(10);
  });
});

it('stops the generator when the browser cancels a history', async () => {
  // WSM-ERR-012/013: `cancel()` stops the backend generator, which increments `history_cancelled`.
  //
  // Asserted on that counter, not on the absence of frames: a frontend that stopped reading would also
  // produce no further frames.
  //
  // The history delay is 250 ms (the Python test uses 5 ms). A handler that ignored `stream.signal`
  // would still stop one delay later, when its next `send` fails on the reset stream. The test requires
  // the cancellation to be observed within half the delay, which only a handler that watches the signal
  // satisfies.
  const historyDelayMs = 250;
  await marketWire({ tickIntervalMs: 500, historyDelayMs }, async (wire) => {
    await wire.hello();
    const history = wire.dialer.open<HistoryPoint>({ action: 'history', symbol: 'ACME' }, { end: true });

    let received = 0;
    for await (const _point of history) {
      received += 1;
      if (received === 3) break;
    }
    // Python cannot observe a `break` out of an `async for`, so the Python test cancels the stream
    // explicitly. TypeScript finalises the iterator, and `Stream.iterate`'s `finally` sends the same
    // `reset(CANCELLED)` (WSM-ERR-014); the backend receives the same frame.
    const cancelledAt = Date.now();
    const counters = wire.service.counters;
    const window = historyDelayMs / 2;
    while (counters.history_cancelled === 0 && Date.now() - cancelledAt < window) await pause(1);
    const noticedMs = Date.now() - cancelledAt;

    expect(counters.history_started).toBe(1);
    // WSM-API-023: the generator is notified through the signal. Within half a delay it has made no
    // further write that could have failed.
    note(`the generator noticed the cancellation after ${noticedMs}ms of a ${historyDelayMs}ms delay`);
    expect(counters.history_cancelled).toBe(1);
    expect(noticedMs).toBeLessThan(window);

    const stoppedAt = counters.history_points_sent;
    expect(stoppedAt).toBeLessThan(HISTORY_POINTS);
    // Twice the delay, so a generator still looping would have sent at least one more point.
    await pause(historyDelayMs * 2);
    note(
      `points sent before the cancellation: ${stoppedAt}; ` +
        `after another ${historyDelayMs * 2}ms: ${counters.history_points_sent}`,
    );
    expect(counters.history_points_sent).toBe(stoppedAt);
  });
});

it('fragments the export payload without application code', async () => {
  // WSM-FRG-010: the payload exceeds MAX_FRAME_BYTES, is fragmented on the wire and arrives whole,
  // without the application requesting fragmentation.
  await marketWire({ tickIntervalMs: 500 }, async (wire) => {
    await wire.hello();
    const exported = wire.dialer.open<ExportPayload>({ action: 'export', symbol: 'ACME' }, { end: true });
    const payload = await exported;

    const arrived = wire.seen.filter((entry) => entry.direction === 'rx' && entry.frame.stream === exported.id);
    const fragments = arrived.filter((entry) => isFragment(entry.frame));
    const total = fragments.reduce((sum, entry) => sum + entry.byteLength, 0);

    note(`export: ${fragments.length} fragments, ${total} bytes on the wire`);
    expect(fragments.length).toBeGreaterThan(1);
    expect(total).toBeGreaterThan(1_000_000);
    // A receiver accepts frames up to the limit and a sender never exceeds it. The demo passes no
    // `maxFrameBytes`, so the limit is the protocol default.
    fragments.forEach((entry) => {
      expect(entry.byteLength).toBeLessThanOrEqual(MAX_FRAME_BYTES);
    });
    // `more: true` on every fragment except the last, which marks the end of the payload.
    //
    // A field at its default is omitted from the envelope (WSM-CDC-005), so the last fragment has no
    // `more` key and TypeScript leaves the property `undefined`; `?? false` applies the default.
    // `FIELD_DEFAULTS` in `ts/frames.ts` defines the defaults, and `framesEqual` compares frames with
    // the same normalisation.
    expect(fragments.map((entry) => entry.frame.more ?? false)).toEqual([
      ...fragments.slice(0, -1).map(() => true),
      false,
    ]);

    expect(payload.count).toBe(EXPORT_ROWS);
    expect(payload.trades).toHaveLength(EXPORT_ROWS);
    expect(payload.trades[EXPORT_ROWS - 1].seq).toBe(EXPORT_ROWS - 1);
  });
}, 30_000);

it('generates the same market data regardless of request order', () => {
  // Each (seed, purpose, symbol) has its own generator, so ACME's tenth price does not depend on which
  // other requests were made first. This test sends different requests to two markets and expects the
  // same ACME ticks from both.
  //
  // The numbers are reproducible across runs of this backend; they differ from the Python backend's.
  const quiet = new Market();
  const busy = new Market();
  const ten = Array.from({ length: 10 }, () => quiet.tick('ACME'));

  busy.export('ACME');
  busy.depth('BOLT');
  busy.history('ACME');
  busy.tick('BOLT');
  expect(Array.from({ length: 10 }, () => busy.tick('ACME'))).toEqual(ten);

  // Reproducible across processes: the generator is seeded from the characters of
  // `<seed>:<purpose>:<symbol>` and does not depend on insertion order or a per-process hash seed.
  expect(new Market().history('ACME')).toEqual(quiet.history('ACME'));
  // The book's prices are offsets from the symbol's current price, which `busy` has changed, so only
  // the drawn values are compared: quantities and order counts.
  const drawn = (book: DepthBook): [number, number][] =>
    book.bids.slice(0, 5).map((level) => [level.qty, level.orders]);
  expect(drawn(new Market().depth('BOLT'))).toEqual(drawn(busy.depth('BOLT')));
});

it('closes every peer on a kill request', async () => {
  // The kill switch: every live stream fails and the browser's close handler receives a reason.
  await marketWire({ tickIntervalMs: 20, killDelayMs: 10 }, async (wire) => {
    await wire.hello();
    await wire.untilTheBoardIsLive();
    const watching = [...wire.dialer.streams.values()][0];

    const answer = await wire.dialer.request<{ closing: number }>({ action: 'kill' });
    expect(answer.closing).toBe(1);

    const deadline = Date.now() + 2_000;
    while (wire.closes.length === 0 && Date.now() < deadline) await pause(5);
    // The browser receives a `CloseReason`, so the UI can report the closed connection.
    expect(wire.closes.length).toBeGreaterThan(0);
    note(`the browser was told: code=${wire.closes[0].code} clean=${wire.closes[0].wasClean}`);
    expect(wire.dialer.isOpen).toBe(false);
    expect(wire.dialer.streams.size).toBe(0);
    // WSM-REG-016: the registry installs its own `onClose` hook, so it is empty even without
    // `MarketService.forget` calling `deregister`. The kill switch is the only place in the demo where
    // a registered peer closes.
    expect(wire.service.registry.peersFor()).toEqual([]);

    // Every open stream is closed, and a send on one throws `StreamReset` (WSM-ERR-002). `closed` is a
    // promise in TypeScript, so the test asserts on `state` (WSM-API-023).
    expect(watching.state).toBe(StreamState.CLOSED);
    await expect(watching.send({ anything: true })).rejects.toBeInstanceOf(StreamReset);
  });
});
