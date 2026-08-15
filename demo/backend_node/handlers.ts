/**
 * The one `onStream` handler, and every action the frontend can ask for.
 *
 * A port of `demo/backend_python/handlers.py`. There is one handler per peer and every stream the
 * browser opens arrives at it - the first one and every one after it (WSM-STM-030). What distinguishes
 * the four call shapes is not four APIs but what this code does with the stream it was handed:
 *
 *   * `quote`   - `reply()`: one payload, and the stream ends with it (WSM-API-006).
 *   * `history` - `send()` in a loop, then `end()`: a response that arrives progressively, and one that
 *                 stops when the opener cancels it (WSM-ERR-012/013).
 *   * `depth`   - `reply()` with a payload over MAX_FRAME_BYTES: fragmented and reassembled with the
 *                 application doing nothing at all (WSM-FRG-010).
 *   * `export`  - the same, with a megabyte. The headline: while its sixteen fragments are on the wire,
 *                 the twenty pushed tick streams keep their turn (WSM-INV-004).
 *
 * The board itself is not an action. The backend calls `peer.open()` on its own initiative and the
 * browser receives it through *its* `onStream` handler - one `Peer` type, one mechanism, no second
 * correlation story (WSM-INV-002).
 *
 * The frontend does not know this file exists, and that is the point of the second backend: it asks
 * for the same actions, destructures the same fields and displays the same counters whichever language
 * answered. Where this port differs from the Python one it is because TypeScript forced it, and each
 * such place says so where it happens - `stream.signal` in `history`, and `forget` below.
 */

import {
  ConnectionGoingAway,
  ConnectionLost,
  logger,
  type Peer,
  type PeerRegistry,
  ResetCode,
  type Stream,
  StreamClosed,
  StreamReset,
  type StreamHandler,
} from '../../ts/index';

import { HISTORY_POINTS, type Market } from './market';

/**
 * How often each symbol's pushed stream carries a new price. Four a second per symbol is fast enough
 * that the board is visibly live and slow enough that a reader can follow one row.
 *
 * Milliseconds, because every duration in the TypeScript port is milliseconds as an integer where
 * Python's is seconds as a float - the library spells `drainMs` and `timeoutMs` the same way. What
 * goes *on the wire* stays seconds: `tick_interval` is a field the frontend already reads from the
 * Python backend, and the wire is the contract.
 */
export const TICK_INTERVAL_MS = 250;

/**
 * The pause between two points of a history response. It exists so the response is visibly
 * *progressive*: a chart that filled in one frame would demonstrate nothing that a unary reply does
 * not, and there would be nothing to cancel halfway through.
 */
export const HISTORY_POINT_DELAY_MS = 20;

/**
 * How long the kill switch waits before closing the sockets. Long enough for its own reply to reach
 * the wire, because a handler whose connection is closed underneath it is a handler whose answer never
 * arrives - and the panel would then look like a bug rather than like a demonstration.
 */
export const KILL_DELAY_MS = 100;

/**
 * What the diagnostics strip reads, and what `handlers.spec.ts` asserts on.
 *
 * snake_case, and not because this file is a translation: these keys are the wire, `Stats` in
 * `demo/frontend/src/muxws.ts` destructures exactly them, and a backend that camelCased them would be
 * a backend the frontend could tell apart from the Python one - which is the one thing this port must
 * not be.
 *
 * `history_cancelled` is the one that matters. WSM-ERR-012/013 say the opener's `cancel()` stops the
 * *backend* generator, and nothing on a screen can distinguish a backend that stopped from a frontend
 * that stopped looking. A number the generator increments itself can.
 */
export interface Counters {
  history_started: number;
  history_points_sent: number;
  history_cancelled: number;
  exports: number;
  depths: number;
  quotes: number;
}

/** The three timings the tests dial down, mirroring `MarketService.__init__`'s keyword arguments. */
export interface MarketServiceOptions {
  tickIntervalMs?: number;
  historyDelayMs?: number;
  killDelayMs?: number;
}

type Action = (peer: Peer, payload: unknown, stream: Stream) => Promise<void>;

/**
 * One peer's pushed board: the streams it is pushing on, and the way to tell them to stop.
 *
 * Python holds a list of `asyncio.Task`s and cancels them. TypeScript has no task to cancel, so the
 * handle is an `AbortController` the generators watch and the streams they own, held so `forget` can
 * close them without waiting for the generators to notice.
 */
interface Board {
  readonly stop: AbortController;
  readonly streams: Stream[];
}

/**
 * `asyncio.sleep`, with the one thing a JavaScript `await` cannot otherwise do: end early.
 *
 * It **resolves** on abort rather than rejecting, so a caller checks `signal.aborted` afterwards and
 * decides. A rejecting sleep would put a synthetic error into paths whose real question is only
 * "should I still be running", and would hide the genuine failures - `StreamReset`, `StreamClosed` -
 * that the same `try` has to tell apart.
 */
function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted === true) return Promise.resolve();
  return new Promise<void>((resolve) => {
    const done = (): void => {
      clearTimeout(timer);
      signal?.removeEventListener('abort', done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal?.addEventListener('abort', done, { once: true });
  });
}

/** One field of an opening payload, or `undefined` if the payload is not an object at all. */
function fieldOf(payload: unknown, key: string): unknown {
  if (typeof payload !== 'object' || payload === null) return undefined;
  return (payload as Record<string, unknown>)[key];
}

/** Every action but `hello`, `stats` and `kill` names a symbol; a request that does not is an error. */
function symbolOf(payload: unknown): string {
  const symbol = fieldOf(payload, 'symbol');
  if (typeof symbol !== 'string') throw new Error(`this action needs a symbol; got ${JSON.stringify(symbol)}`);
  return symbol;
}

/** The demo's backend, as one object: the market, the registry, and the pushed tick streams. */
export class MarketService {
  readonly market: Market;

  readonly registry: PeerRegistry;

  readonly counters: Counters = {
    history_started: 0,
    history_points_sent: 0,
    history_cancelled: 0,
    exports: 0,
    depths: 0,
    quotes: 0,
  };

  /** One board per peer, held so nothing takes the board away from a connection still watching it. */
  private readonly boards = new Map<Peer, Board>();

  /** A `Map` and not an object literal: the repository's eslint config bans `for...in` outright. */
  private readonly actionTable: Map<string, Action>;

  private tickInterval: number;

  private readonly historyDelay: number;

  private readonly killDelay: number;

  /**
   * The kill switch's outstanding work.
   *
   * Python holds its task because the garbage collector may take an un-referenced one away mid-flight.
   * A pending promise chain keeps itself alive, so this field buys no safety here - it is kept because
   * an object that starts work in the background should be able to say what it started, and because
   * the two ports are meant to be read side by side.
   */
  private killer: Promise<void> | null = null;

  constructor(market: Market, registry: PeerRegistry, options: MarketServiceOptions = {}) {
    this.market = market;
    this.registry = registry;
    this.tickInterval = options.tickIntervalMs ?? TICK_INTERVAL_MS;
    this.historyDelay = options.historyDelayMs ?? HISTORY_POINT_DELAY_MS;
    this.killDelay = options.killDelayMs ?? KILL_DELAY_MS;
    this.actionTable = new Map<string, Action>([
      ['hello', this.hello.bind(this)],
      ['quote', this.quote.bind(this)],
      ['history', this.history.bind(this)],
      ['depth', this.depth.bind(this)],
      ['export', this.export.bind(this)],
      ['stats', this.stats.bind(this)],
      ['rate', this.rate.bind(this)],
      ['kill', this.kill.bind(this)],
    ]);
  }

  /** Every action the frontend may name. The test asserts each one answers. */
  get actions(): string[] {
    return [...this.actionTable.keys()];
  }

  /** How often each pushed stream carries a price, in milliseconds. `rate` changes it. */
  get tickIntervalMs(): number {
    return this.tickInterval;
  }

  // ------------------------------------------------------------------ registration

  /**
   * The `onStream` handler for one connection.
   *
   * Bound per peer because a handler is handed `(payload, stream)` and the actions need the `Peer`: the
   * hello has to tag and register *this* connection, and the board has to be pushed onto it. The route
   * has the peer in scope, which is where the documented examples close over it too.
   */
  handlerFor(peer: Peer): StreamHandler {
    return async (payload: unknown, stream: Stream): Promise<void> => {
      await this.dispatch(peer, payload, stream);
    };
  }

  /**
   * Route one stream to one action.
   *
   * An unknown action throws, and that throw is the answer: WSM-STM-034 turns a handler failure into
   * `reset(APPLICATION_ERROR)` - never `REFUSED`, which would promise the request had not been
   * processed and would invite the browser to retry something that may already have run.
   */
  async dispatch(peer: Peer, payload: unknown, stream: Stream): Promise<void> {
    const action = fieldOf(payload, 'action');
    const handler = typeof action === 'string' ? this.actionTable.get(action) : undefined;
    if (handler === undefined) {
      throw new Error(`unknown action ${JSON.stringify(action)}; this backend answers ${this.actions.join(', ')}`);
    }
    await handler(peer, payload, stream);
  }

  /**
   * Stop pushing to a peer and drop it from the index. Idempotent.
   *
   * Two steps where Python has one, and the second is the one TypeScript forces. `task.cancel()` raises
   * inside a pending `await`; aborting a signal only *tells* a generator, and it learns on some later
   * turn. `close()` will not wait that long - it drains every live stream before closing the socket
   * (WSM-CON-024) - so the streams are closed here and now, and the abort is what stops the generators
   * looping afterwards. `cancel()` is a no-op on an already-closed stream, so the two cannot collide.
   */
  forget(peer: Peer): void {
    const board = this.boards.get(peer);
    if (board !== undefined) {
      this.boards.delete(peer);
      board.stop.abort();
      board.streams.forEach((stream) => {
        void stream.cancel('the board stopped');
      });
    }
    this.registry.deregister(peer);
  }

  // ------------------------------------------------------------------ the hello (D3)

  /**
   * The subscription. It is an ordinary stream and nothing marks it on the wire (WSM-RCN-021).
   *
   * The dialer sends it through `connect({hello})`, so it is replayed verbatim on every socket that
   * peer ever gets (WSM-RCN-020) and this runs again, on a brand-new `Peer`, after every reconnect.
   * That is the whole of why the acceptor re-indexes here rather than anywhere else: an acceptor's
   * `tags` die with their connection (WSM-RCN-033), so whatever it indexes it must index again on
   * every connection.
   *
   * The reply is a courtesy and nothing depends on it. `connect()`'s reconnect helper owns this stream
   * and discards what comes back on it - the acknowledgement WSM-RCN-022 asks for is this handler
   * *returning*. Everything the frontend actually needs arrives on the pushed streams below, which is
   * what makes the backend the only source of truth about symbols (D1).
   */
  private async hello(peer: Peer, payload: unknown, stream: Stream): Promise<void> {
    const tags = fieldOf(payload, 'tags');
    if (typeof tags === 'object' && tags !== null) {
      // muxws never reads `tags` and defines no key of its own in it (WSM-REG-001/002); these are the
      // frontend's own labels, and the index is built from exactly what it named.
      Object.assign(peer.tags, tags);
    }
    // After the writes and never before: the registry indexes, it does not watch (WSM-REG-010).
    this.registry.register(peer);
    this.startBoard(peer);
    await stream.reply({
      ok: true,
      symbols: this.market.symbols.length,
      tick_interval: this.tickInterval / 1000,
    });
  }

  /**
   * Change how often each pushed stream carries a price.
   *
   * The demo's pacing is not the library's speed, and this control is what lets a reader see the
   * difference. The interval is a `sleep` in the generator below and has nothing to do with what the
   * transport can carry.
   *
   * The floor is 1 ms rather than zero. At zero the generator becomes a busy loop that starves the very
   * event loop it needs to send on, which would demonstrate the opposite of the point.
   */
  private async rate(_peer: Peer, payload: unknown, stream: Stream): Promise<void> {
    const requested = fieldOf(payload, 'interval_ms');
    if (typeof requested !== 'number' || !Number.isFinite(requested) || requested < 1 || requested > 5_000) {
      throw new Error(`interval_ms must be between 1 and 5000; got ${JSON.stringify(requested)}`);
    }
    this.tickInterval = requested;
    await stream.reply({ ok: true, interval_ms: requested, symbols: this.market.symbols.length });
  }

  // ------------------------------------------------------------------ server push

  /**
   * Twenty streams, opened by the acceptor on its own initiative (WSM-INV-002).
   *
   * One per symbol rather than one carrying all twenty, because the claim being witnessed is that a
   * stream mid-way through a megabyte does not hold the wire: twenty independent lanes plus the export
   * is the arrangement in which a FIFO and a round-robin writer look different.
   */
  private startBoard(peer: Peer): void {
    if (this.boards.has(peer)) {
      // A second hello on one socket must not double the board. It should not happen - the helper
      // replays a hello per *connection* - but a duplicated tick stream would look like a protocol bug
      // rather than like this.
      return;
    }
    const board: Board = { stop: new AbortController(), streams: [] };
    this.boards.set(peer, board);
    this.market.symbols.forEach((symbol) => {
      // Deliberately not awaited - these run for as long as the socket does - so the rejection has to
      // be caught here: an unhandled rejection in Node takes the whole demo down, and a board stream
      // that failed is one row going stale.
      void this.pushTicks(peer, symbol, board).catch((error: unknown) => {
        logger.error(`muxws demo: the ${symbol} tick stream stopped`, error);
      });
    });
    // Synchronous, because `onClose` handlers are. Without it a socket that dies with no route around
    // it - a test, say - leaves twenty generators pushing into a peer that has been dead since
    // WSM-RCN-042 discarded its writer.
    peer.onClose(() => {
      this.forget(peer);
    });
  }

  /**
   * One symbol's price stream, for as long as the socket lasts.
   *
   * `peer.open()` here is the same call the browser makes for a quote, on the same socket, with the
   * same correlation and the same cancellation. There is no push API because there does not need to be
   * one (WSM-INV-002).
   */
  private async pushTicks(peer: Peer, symbol: string, board: Board): Promise<void> {
    let stream: Stream;
    try {
      stream = peer.open({ topic: 'ticks', symbol });
    } catch (error) {
      // The socket died between the hello and this generator's first turn. Nothing is buffered for a
      // next one (WSM-RCN-042) and there is nothing to report.
      if (error instanceof ConnectionLost || error instanceof ConnectionGoingAway) return;
      throw error;
    }
    board.streams.push(stream);
    try {
      // The snapshot first, so a row can be drawn before any price has moved: the frontend holds no
      // seed data and this is where a row comes from (D1).
      await stream.send(this.market.snapshot(symbol));
      for (;;) {
        await sleep(this.tickInterval, board.stop.signal);
        if (board.stop.signal.aborted) return;
        await stream.send(this.market.tick(symbol));
      }
    } catch (error) {
      // `ConnectionLost` is a `StreamReset`, so this covers the ordinary end of the demo: the socket
      // went away and every live stream failed with it before `onClose` fired.
      if (!(error instanceof StreamReset) && !(error instanceof StreamClosed)) throw error;
    } finally {
      // A board stream has no natural end, and `close()` lets every live stream finish before it closes
      // the socket (WSM-CON-024/025). Twenty streams that never finish would hold that drain open for
      // its whole window, so the producer that is going away says so. A no-op when the stream is
      // already closed, which is the ordinary path.
      await stream.cancel('the board stopped');
    }
  }

  // ------------------------------------------------------------------ the four call shapes

  /** Unary: one payload out, exactly one payload back (WSM-API-006). */
  private async quote(_peer: Peer, payload: unknown, stream: Stream): Promise<void> {
    this.counters.quotes += 1;
    await stream.reply(this.market.quote(symbolOf(payload)));
  }

  /**
   * Streaming, and cancellable. The cancellation is the point (WSM-ERR-012/013).
   *
   * **This is the one place the two backends genuinely differ.** In Python the browser's `cancel()`
   * cancels the task this handler runs on and `CancelledError` is raised at whatever it is awaiting.
   * TypeScript cannot interrupt a pending `await` at all, so the peer aborts `stream.signal` at the
   * same instant instead (WSM-API-023) and the handler cooperates: the sleep below ends early and the
   * loop asks whether it should still be running. A generator that ignored the signal would keep
   * producing points for a reader who has already left, which is precisely what WSM-ERR-012 forbids.
   *
   * The counter incremented below is how a reader sees that the *backend* stopped generating rather
   * than that the frontend stopped looking.
   */
  private async history(_peer: Peer, payload: unknown, stream: Stream): Promise<void> {
    const symbol = symbolOf(payload);
    const points = this.market.history(symbol, HISTORY_POINTS);
    this.counters.history_started += 1;
    try {
      for (const point of points) {
        await stream.send(point);
        this.counters.history_points_sent += 1;
        await sleep(this.historyDelay, stream.signal);
        if (stream.signal.aborted) throw stream.signal.reason ?? new Error(`stream ${stream.id} was cancelled`);
      }
    } catch (error) {
      // A cancel that lands mid-`send` arrives as the stream's own failure rather than through the
      // signal, so both paths end here and exactly one of them is a cancellation.
      if (!stream.signal.aborted) throw error;
      this.counters.history_cancelled += 1;
      // Re-thrown, never swallowed (WSM-ERR-014). The peer sees an aborted handler and stays quiet;
      // a handler that returned normally here would be claiming to have finished a response the
      // browser had already abandoned. The state check in the library's `runHandler` would keep it off
      // the wire, but the claim would still be wrong, and this is a demo about what the wire says.
      throw error;
    }
    // Trailers ride the `end` frame: they are the things only known once the body is finished, and a
    // separate trailing message would be a second frame the browser has to correlate.
    await stream.end({ trailers: { symbol, points: points.length } });
  }

  /** A payload comfortably over MAX_FRAME_BYTES. Nothing here calls a splitter (WSM-FRG-010). */
  private async depth(_peer: Peer, payload: unknown, stream: Stream): Promise<void> {
    this.counters.depths += 1;
    await stream.reply(this.market.depth(symbolOf(payload)));
  }

  /**
   * The stall test. One megabyte, one payload, one stream, and twenty others unaffected.
   *
   * This handler is deliberately unremarkable: the whole demonstration is what the *writer* does with
   * what it enqueues here, which is to hand the socket back after every 64 KiB fragment so each of the
   * twenty tick streams gets its turn (WSM-FRG-018/019, WSM-INV-004).
   */
  private async export(_peer: Peer, payload: unknown, stream: Stream): Promise<void> {
    this.counters.exports += 1;
    await stream.reply(this.market.export(symbolOf(payload)));
  }

  // ------------------------------------------------------------------ the diagnostics strip

  /** What the backend knows about itself, asked for over the same socket as everything else. */
  private async stats(peer: Peer, _payload: unknown, stream: Stream): Promise<void> {
    await stream.reply({
      ...this.counters,
      peers: this.registry.peersFor().length,
      streams: peer.streams.size,
      symbols: this.market.symbols,
      // The board's own period, in seconds because that is what the frontend already reads from the
      // other backend. The hello ack carries it too, but `connect({hello})` owns that stream and
      // discards what comes back on it (WSM-RCN-022).
      tick_interval: this.tickInterval / 1000,
    });
  }

  /**
   * Close every peer, so the browser can watch a reconnect it did not cause.
   *
   * Scheduled rather than awaited: `close()` sends `goaway`, drains and closes the socket
   * (WSM-CON-025), and doing that inline would take down the socket this handler's own reply is still
   * queued on.
   */
  private async kill(_peer: Peer, _payload: unknown, stream: Stream): Promise<void> {
    const peers = this.registry.peersFor();
    await stream.reply({ closing: peers.length });
    this.killer = this.closeEveryPeer(peers);
    void this.killer.catch((error: unknown) => {
      logger.error('muxws demo: the kill switch failed', error);
    });
  }

  private async closeEveryPeer(peers: Peer[]): Promise<void> {
    await sleep(this.killDelay);
    for (const peer of peers) {
      // The board first. `close()` drains every live stream before it closes the socket (WSM-CON-024),
      // and the twenty pushed streams are exactly the ones that would never reach an end of their own -
      // the drain would spend its full window waiting for them.
      this.forget(peer);
      try {
        await peer.close({ code: ResetCode.NO_ERROR, reason: "the demo's kill switch" });
      } catch (error) {
        logger.debug(`muxws demo: peer ${peer.id} was already gone when the kill switch reached it`, error);
      }
    }
  }
}
