/**
 * The node backend against the in-memory transport - the twin of `demo/backend_python/handlers_test.py`.
 *
 * A demo with no tests rots into a screenshot. The headline is
 * `keeps the ticks flowing while a megabyte goes out`: the panel is the demonstration, this is the
 * proof, and it is written against `onFrame`'s record rather than against anything on a screen so
 * that it fails if the round-robin writer is ever replaced by a FIFO.
 *
 * It is a twin and not a translation. Where the two ports differ - a `break` out of a `for await` that
 * TypeScript *does* observe, `closed` being a promise, a signal standing in for task cancellation -
 * the difference is asserted here rather than glossed over.
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
  type Stream,
  StreamReset,
  StreamState,
} from '../../ts/index';

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

/** What `MarketService._stats` answers with - the same fields `Stats` in the frontend destructures. */
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
 * A measurement worth reading while you are investigating, and noise while you are not.
 *
 * Every call below sits beside an `expect` on the same number, so these lines are evidence for a
 * human and never the test's strength - and a suite whose normal state is silence is one where an
 * unexpected line means something. Set `MUXWS_VERBOSE` to get them back; a failing assertion prints
 * its own actual value either way.
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

/**
 * A browser and a backend, wired to each other, with the demo's real handler on the acceptor.
 *
 * Over a plain `MemorySocket` - the same transport every other test in this repository uses, and that
 * is what makes WSM-INV-004 observable here: the write loop takes one event-loop turn per frame, so
 * the rotation has something to rotate over without a paced socket double supplying a yield the
 * transport does not.
 */
class Wire {
  readonly dialer: Peer;

  readonly acceptor: Peer;

  readonly service: MarketService;

  /**
   * Every frame the *browser* saw, in arrival order. WSM-OBS-003's hook is the only witness the
   * headline test below uses.
   */
  readonly seen: Seen[] = [];

  /**
   * What the browser knows about the market, and it knows nothing the socket did not tell it (D1).
   * Filled by the pushed streams alone.
   */
  readonly board = new Map<string, BoardRow>();

  /**
   * The **first** row each symbol pushed, kept separately because the latest one overwrites it. A board
   * that could only draw a row once a price had moved would leave the grid empty for a whole tick
   * interval, which is what the snapshot exists to prevent.
   */
  readonly first = new Map<string, BoardRow>();

  /**
   * Every `CloseReason` the browser was handed. The kill-switch panel's claim is that the UI is *told*,
   * rather than left with a frozen screen.
   */
  readonly closes: CloseReason[] = [];

  private readonly sockets: MemorySocket[];

  private tasks: Promise<void>[] = [];

  constructor(options: MarketServiceOptions = {}) {
    const codec = new JsonCodec();
    const [browserSide, backendSide] = memoryPair();
    this.dialer = new Peer(browserSide, { codec, isDialer: true });
    this.acceptor = new Peer(backendSide, { codec, isDialer: false });
    this.service = new MarketService(new Market(), new PeerRegistry(), options);
    this.acceptor.onStream(this.service.handlerFor(this.acceptor));
    this.sockets = [browserSide, backendSide];
  }

  start(): void {
    this.dialer.onFrame((direction, frame, byteLength) => {
      this.seen.push({ direction, frame, byteLength });
    });
    // Registered **before** anything goes out, because the backend pushes the board the instant it sees
    // the hello and a handler registered one await later meets that push with
    // `reset(REFUSED, "no on_stream handler")` (WSM-STM-033). `connect()` takes `onStream` as an option
    // for exactly this reason.
    this.dialer.onStream(this.onPush.bind(this));
    this.dialer.onClose((reason) => {
      this.closes.push(reason);
    });
    this.tasks = [this.dialer.serve(), this.acceptor.serve()];
    // A socket dropped in teardown ends both read loops; whatever they report on the way out is
    // teardown noise rather than a result, and an unhandled rejection would fail an unrelated test.
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

  /** What `connect({hello})` puts on the wire, sent by hand so the ack can be read (D3). */
  async hello(tags?: Record<string, unknown>): Promise<HelloAck> {
    return this.dialer.request<HelloAck>({ action: 'hello', tags: tags ?? { topic: 'board' } });
  }

  received(direction: 'tx' | 'rx' = 'rx'): Frame[] {
    return this.seen.filter((entry) => entry.direction === direction).map((entry) => entry.frame);
  }

  /** Wait until every symbol has pushed at least one price, so the export starts into a live board. */
  async untilTheBoardIsLive(timeoutMs = 5_000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (this.board.size < SYMBOLS.length) {
      if (Date.now() > deadline) {
        throw new Error(`only ${this.board.size} of ${SYMBOLS.length} tick streams started`);
      }
      await pause(5);
    }
  }

  async stop(): Promise<void> {
    this.service.forget(this.acceptor);
    this.sockets.forEach((socket) => {
      socket.drop();
    });
    await Promise.allSettled(this.tasks);
    // One turn past the drop, so the generators that were sleeping observe their aborted board and
    // finish before the next test starts counting frames.
    await pause(5);
  }
}

/** One browser, one backend, one socket - the demo's own objects and nobody's test doubles. */
async function marketWire(options: MarketServiceOptions, body: (wire: Wire) => Promise<void>): Promise<void> {
  const wire = new Wire(options);
  wire.start();
  try {
    await body(wire);
  } finally {
    await wire.stop();
  }
}

/** The longest run of consecutive `true` values. */
function runsOf(flags: boolean[]): number {
  let longest = 0;
  let run = 0;
  flags.forEach((flag) => {
    run = flag ? run + 1 : 0;
    longest = Math.max(longest, run);
  });
  return longest;
}

function isFragment(frame: Frame): boolean {
  return frame.fragment !== null && frame.fragment !== undefined;
}

/** A tick stream is one the **acceptor** opened, and the acceptor's ids are the even ones. */
function isTickFrame(frame: Frame): boolean {
  return frame.type === 'data' && frame.stream !== null && frame.stream !== undefined && frame.stream % 2 === 0;
}

// --------------------------------------------------------------------------- the headline

it('keeps the ticks flowing while a megabyte goes out', async () => {
  // WSM-INV-004, as a test: a megabyte on one stream does not stall the twenty others.
  //
  // Asserted from what the *browser's* `onFrame` recorded, not from anything visual and not from the
  // handler's own bookkeeping. A FIFO anywhere on the send path decides the order at enqueue time, so
  // every one of the export's sixteen fragments would go out back to back and the ticks produced
  // while they did would arrive afterwards in a heap. Under the round-robin writer the export gets
  // one fragment per turn and every tick stream gets its own turn in between (WSM-FRG-017/018/019).
  //
  // One millisecond per symbol rather than the Python twin's five, and the reason is the *producers*
  // rather than the writer. A tick lane can only take its turn if it has something in it, and a lane
  // fed by a `setTimeout` is empty for as long as the timer runs late - on a loaded machine running
  // the whole suite in parallel workers, long enough for several export fragments to go out with
  // nothing between them, which reads as a FIFO writer when it is a starved producer. At one
  // millisecond every lane always has a frame waiting, so what the assertion below measures is the
  // rotation and only the rotation.
  await marketWire({ tickIntervalMs: 1 }, async (wire) => {
    await wire.hello();
    await wire.untilTheBoardIsLive();

    const exported = wire.dialer.open<ExportPayload>({ action: 'export', symbol: 'ACME' }, { end: true });
    const payload = await exported;

    const received = wire.received();
    const fragments = received
      .map((frame, index) => ({ frame, index }))
      .filter(({ frame }) => frame.stream === exported.id && frame.type === 'data' && isFragment(frame))
      .map(({ index }) => index);
    const ticks = new Set(
      received
        .map((frame, index) => ({ frame, index }))
        .filter(({ frame }) => isTickFrame(frame))
        .map(({ index }) => index),
    );

    note(
      `export fragments=${fragments.length} span=${fragments[0]}..${fragments[fragments.length - 1]} ` +
        `of ${received.length} frames`,
    );
    // A megabyte really did have to be fragmented; otherwise there was nothing to stall behind.
    expect(fragments.length).toBeGreaterThanOrEqual(8);
    expect(payload.count).toBe(EXPORT_ROWS);

    const firstFragment = fragments[0];
    const lastFragment = fragments[fragments.length - 1];
    const interleaved = [...ticks].filter((index) => index > firstFragment && index < lastFragment).length;
    note(`tick frames between the first and last export fragment: ${interleaved}`);
    expect(interleaved).toBeGreaterThanOrEqual(40);

    // The sharper form of the same claim, and the one a FIFO cannot survive: no tick ever waits on
    // more than a fragment or two of the export.
    const window = received.slice(firstFragment, lastFragment + 1);
    const inWindow = new Set(fragments);
    const backToBack = runsOf(window.map((_frame, index) => inWindow.has(index + firstFragment)));
    note(`longest run of consecutive export fragments: ${backToBack}`);
    expect(backToBack).toBeLessThanOrEqual(2);

    // And it was the whole board that kept moving, not one lucky stream: the rotation asks every lane
    // once before it asks any lane twice.
    const streams = new Set(window.filter((frame) => isTickFrame(frame)).map((frame) => frame.stream));
    note(`distinct tick streams delivering during the export: ${streams.size} of ${SYMBOLS.length}`);
    expect(streams.size).toBe(SYMBOLS.length);
  });
}, 30_000);

// --------------------------------------------------------------------------- the rest of section 6

it('answers every action the frontend can name', async () => {
  await marketWire({ tickIntervalMs: 500, historyDelayMs: 0 }, async (wire) => {
    // The eight the frontend may send, and the eight `dispatch` will route. An action added to one
    // backend and not the other is a demo whose two halves are not the same demo.
    expect(wire.service.actions).toEqual(['hello', 'quote', 'history', 'depth', 'export', 'stats', 'rate', 'kill']);

    const acknowledgement = await wire.hello({ topic: 'board', session: 'demo' });
    expect(acknowledgement.ok).toBe(true);
    expect(acknowledgement.symbols).toBe(SYMBOLS.length);
    // D3: the peer is findable under the tags the *frontend* named, and under nothing muxws chose.
    expect(wire.service.registry.peersFor({ topic: 'board' })).toEqual([wire.acceptor]);
    expect(wire.service.registry.peersFor({ session: 'demo' })).toEqual([wire.acceptor]);

    // Server push, and D1 with it: the browser asked for no symbol list and holds no seed data, and
    // yet it knows all twenty - because the acceptor opened twenty streams of its own accord and the
    // browser received them through its own `onStream` handler (WSM-INV-002).
    await wire.untilTheBoardIsLive();
    expect([...wire.board.keys()].sort()).toEqual(SYMBOLS.map(([symbol]) => symbol).sort());
    // The first frame of every pushed stream is the snapshot, not a tick: the grid is drawable the
    // moment it is subscribed rather than a tick interval later. A backend that pushed only movements
    // would fill the board just as surely and leave it blank until it did.
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
    // Trailers ride the `end` frame and are readable only once the iteration is over.
    expect(history.trailers).toEqual({ symbol: 'ACME', points: HISTORY_POINTS });

    const before = wire.seen.length;
    const depth = await wire.dialer.request<DepthBook>({ action: 'depth', symbol: 'ACME' });
    expect(depth.bids).toHaveLength(depth.levels);
    expect(depth.asks).toHaveLength(depth.levels);
    expect(depth.bids[0].px).toBeLessThan(depth.asks[0].px);
    // WSM-FRG-010, which is the entire claim of the "full depth" panel and is not implied by the three
    // assertions above: a book of five levels is internally consistent, arrives whole, and witnesses
    // nothing. The payload has to cross MAX_FRAME_BYTES for there to be a reassembly to watch, and
    // nothing in the handler or here asked for one.
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

    // An action nobody defined is an application error, never a refusal: REFUSED would promise the
    // request had not been processed and invite the browser to retry it (WSM-STM-034).
    await expect(wire.dialer.request({ action: 'buy', symbol: 'ACME' })).rejects.toBeInstanceOf(RemoteError);
  });
}, 30_000);

it('changes how often the board pushes when asked', async () => {
  // The pacing is the demo's and not the library's, and a reader has to be able to prove that. The
  // interval is a `sleep` in the generator; this asserts the choice is reachable, and that it is
  // bounded, because zero would turn the generator into a busy loop that starves the loop it sends on.
  await marketWire({}, async (wire) => {
    const answer = await wire.dialer.request<RateAck>({ action: 'rate', interval_ms: 10 });
    expect(answer.ok).toBe(true);
    expect(answer.interval_ms).toBe(10);
    expect(wire.service.tickIntervalMs).toBe(10);

    const stats = await wire.dialer.request<Stats>({ action: 'stats' });
    // Seconds on the wire, milliseconds in this port: the frontend reads `tick_interval` the way the
    // Python backend spells it, and a second backend that changed the units would be a second backend
    // the frontend could tell apart.
    expect(stats.tick_interval).toBe(0.01);

    for (const refused of [0, -5, 10_000]) {
      await expect(wire.dialer.request({ action: 'rate', interval_ms: refused })).rejects.toBeInstanceOf(RemoteError);
    }
    expect(wire.service.tickIntervalMs).toBe(10);
  });
});

it('stops the generator when the browser cancels a history', async () => {
  // WSM-ERR-012/013: `cancel()` stops the *backend*, and a counter it increments says so.
  //
  // Asserted from that counter and never from the absence of frames. A frontend that stopped reading
  // would produce exactly the same silence on the wire, and the panel's whole claim is that the backend
  // stopped generating.
  //
  // The delay is a quarter of a second and not the Python twin's five milliseconds, and that is the
  // whole of what makes this test bite *this* port. The signal is the only thing standing in for task
  // cancellation here, and a handler that never looked at it would still stop - one delay later, when
  // its next `send` met a stream the browser had already reset. At five milliseconds those two are
  // indistinguishable and a handler that ignored `stream.signal` outright would pass. Against the
  // window below - far shorter than the delay - only the handler that watches the signal can answer
  // in time, which is exactly the claim `history`'s doc comment makes.
  const historyDelayMs = 250;
  await marketWire({ tickIntervalMs: 500, historyDelayMs }, async (wire) => {
    await wire.hello();
    const history = wire.dialer.open<HistoryPoint>({ action: 'history', symbol: 'ACME' }, { end: true });

    let received = 0;
    for await (const _point of history) {
      received += 1;
      if (received === 3) break;
    }
    // The one place the twin tests differ, and it is a difference in the *consumer*: Python cannot
    // observe a `break` out of an `async for`, so its version cancels the stream by hand. TypeScript
    // finalises the generator, and `Stream.iterate`'s `finally` sends the same `reset(CANCELLED)`
    // (WSM-ERR-014). The frame on the wire is identical, and the frame is all the backend reacts to.
    const cancelledAt = Date.now();
    const counters = wire.service.counters;
    const window = historyDelayMs / 2;
    while (counters.history_cancelled === 0 && Date.now() - cancelledAt < window) await pause(1);
    const noticedMs = Date.now() - cancelledAt;

    expect(counters.history_started).toBe(1);
    // WSM-API-023: the generator was told, rather than finding out on its next write. Inside half a
    // delay there has been no next write to find out on.
    note(`the generator noticed the cancellation after ${noticedMs}ms of a ${historyDelayMs}ms delay`);
    expect(counters.history_cancelled).toBe(1);
    expect(noticedMs).toBeLessThan(window);

    const stoppedAt = counters.history_points_sent;
    expect(stoppedAt).toBeLessThan(HISTORY_POINTS);
    // Longer than the delay itself, so a generator still looping would have sent at least one more.
    await pause(historyDelayMs * 2);
    note(
      `points sent before the cancellation: ${stoppedAt}; ` +
        `after another ${historyDelayMs * 2}ms: ${counters.history_points_sent}`,
    );
    expect(counters.history_points_sent).toBe(stoppedAt);
  });
});

it('fragments the export payload without anybody asking it to', async () => {
  // WSM-FRG-010: over MAX_FRAME_BYTES on the wire, whole on arrival, and nothing asked for it.
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
    // A receiver must accept anything up to the cap and a sender must never exceed it. The cap is a
    // protocol constant, not a setting - nothing in the demo passed `maxFrameBytes` at all.
    fragments.forEach((entry) => {
      expect(entry.byteLength).toBeLessThanOrEqual(MAX_FRAME_BYTES);
    });
    // `more: true` on every fragment but the last, which is how the receiver knows it is done.
    //
    // `?? false`, and that is not a weakening: a field at its default is omitted from the envelope
    // (WSM-CDC-005), so the last fragment arrives with no `more` key at all. Python rebuilds a
    // dataclass whose default is `False` and TypeScript leaves the property `undefined`; `FIELD_DEFAULTS`
    // in `ts/frames.ts` is the statement that the two are the same frame, and `framesEqual` compares
    // them through exactly this normalisation.
    expect(fragments.map((entry) => entry.frame.more ?? false)).toEqual([
      ...fragments.slice(0, -1).map(() => true),
      false,
    ]);

    expect(payload.count).toBe(EXPORT_ROWS);
    expect(payload.trades).toHaveLength(EXPORT_ROWS);
    expect(payload.trades[EXPORT_ROWS - 1].seq).toBe(EXPORT_ROWS - 1);
  });
}, 30_000);

it('draws the same market whatever the reader clicked', () => {
  // D2, and the half of it that a single seeded generator would not give you.
  //
  // Seeding one shared stream of randomness makes a run reproducible only if it is *driven*
  // identically: ACME's tenth price would then depend on whether anybody asked for an export first, and
  // the screenshot in the documentation would stop being true the moment a reader clicked in a
  // different order. One generator per (seed, purpose, symbol) is what makes the claim hold, so this
  // drives two markets differently on purpose and expects the same numbers out of both.
  //
  // Within this backend, and only within it: the numbers deliberately differ from the Python one, so
  // the reproducibility a screenshot needs is reproducibility across runs of *this* process.
  const quiet = new Market();
  const busy = new Market();
  const ten = Array.from({ length: 10 }, () => quiet.tick('ACME'));

  busy.export('ACME');
  busy.depth('BOLT');
  busy.history('ACME');
  busy.tick('BOLT');
  expect(Array.from({ length: 10 }, () => busy.tick('ACME'))).toEqual(ten);

  // And across processes, which is what a screenshot actually needs: the generator is seeded from the
  // characters of `<seed>:<purpose>:<symbol>` and owes nothing to insertion order or to a hash seed.
  expect(new Market().history('ACME')).toEqual(quiet.history('ACME'));
  // The book's prices hang off the symbol's *current* price, which `busy` has moved on purpose, so what
  // is compared is the drawn part: the quantities are the same draws in the same order.
  const drawn = (book: DepthBook): [number, number][] =>
    book.bids.slice(0, 5).map((level) => [level.qty, level.orders]);
  expect(drawn(new Market().depth('BOLT'))).toEqual(drawn(busy.depth('BOLT')));
});

it('closes every peer when the kill switch is thrown', async () => {
  // The "kill the backend" panel: every live stream fails and the browser is told, not frozen.
  await marketWire({ tickIntervalMs: 20, killDelayMs: 10 }, async (wire) => {
    await wire.hello();
    await wire.untilTheBoardIsLive();
    const watching = [...wire.dialer.streams.values()][0];

    const answer = await wire.dialer.request<{ closing: number }>({ action: 'kill' });
    expect(answer.closing).toBe(1);

    const deadline = Date.now() + 2_000;
    while (wire.closes.length === 0 && Date.now() < deadline) await pause(5);
    // The browser is *told*, which is the whole of the panel: a UI that only froze would be
    // indistinguishable from a slow backend.
    expect(wire.closes.length).toBeGreaterThan(0);
    note(`the browser was told: code=${wire.closes[0].code} clean=${wire.closes[0].wasClean}`);
    expect(wire.dialer.isOpen).toBe(false);
    expect(wire.dialer.streams.size).toBe(0);
    // WSM-REG-016 end to end, and it is the *library* this witnesses rather than the demo: the registry
    // hooks `onClose` itself, so this stays empty even if `MarketService.forget` never called
    // `deregister`. That is worth one line here because the kill switch is the only place in the demo
    // where a registered peer dies, and a registry that answered with sockets that have been shut since
    // would make the next `peersFor()` count a fiction.
    expect(wire.service.registry.peersFor()).toEqual([]);

    // Every open stream is closed rather than left hanging, and a send on one throws (WSM-ERR-002 keeps
    // socket death out of a stream's own vocabulary and gives it its own). `closed` is a promise here
    // where Python has an `Event`, so the state is what this asserts on (WSM-API-023).
    expect(watching.state).toBe(StreamState.CLOSED);
    await expect(watching.send({ anything: true })).rejects.toBeInstanceOf(StreamReset);
  });
});
