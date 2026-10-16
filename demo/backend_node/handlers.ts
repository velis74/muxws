/**
 * The `onStream` handler and the actions the frontend can request.
 *
 * A port of `demo/backend_python/handlers.py`. There is one handler per peer, and every stream the
 * browser opens is delivered to it (WSM-STM-030). The four call shapes use the same API and differ in
 * how the handler uses the stream:
 *
 *   * `quote`   - `reply()`: one payload, and the stream ends with it (WSM-API-006).
 *   * `history` - `send()` in a loop, then `end()`: a progressive response that stops when the opener
 *                 cancels it (WSM-ERR-012/013).
 *   * `depth`   - `reply()` with a payload over MAX_FRAME_BYTES; the library fragments and reassembles
 *                 it without application code (WSM-FRG-010).
 *   * `export`  - `reply()` with about 1 MB. While its sixteen fragments are sent, the twenty pushed tick
 *                 streams continue to receive frames (WSM-INV-004).
 *
 * The board is not an action. The backend opens the tick streams with `peer.open()` and the browser
 * receives them through its own `onStream` handler, using the same `Peer` API (WSM-INV-002).
 *
 * The frontend receives the same actions, fields and counters from this backend as from the Python
 * backend. The port differs from the Python one in two places, each commented where it occurs:
 * `stream.signal` in `history`, and `forget`.
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
 * Default interval between price updates on each symbol's pushed stream, in milliseconds (four per
 * second per symbol).
 *
 * Durations in the TypeScript port are integer milliseconds (as in the library's `drainMs` and
 * `timeoutMs`); the Python port uses float seconds. The `tick_interval` field sent to the frontend is
 * in seconds, as the Python backend sends it.
 */
export const TICK_INTERVAL_MS = 250;

/**
 * Delay between two points of a history response, in milliseconds. It makes the response arrive
 * progressively, so it can be cancelled before it completes.
 */
export const HISTORY_POINT_DELAY_MS = 20;

/**
 * Delay before the kill switch closes the sockets, in milliseconds, so the kill reply is sent before
 * its connection closes.
 */
export const KILL_DELAY_MS = 100;

/**
 * Counters the diagnostics strip displays and `handlers.spec.ts` asserts on.
 *
 * The keys are snake_case because they are sent on the wire; `Stats` in `demo/frontend/src/muxws.ts`
 * destructures them, and the Python backend sends the same keys.
 *
 * `history_cancelled` is incremented by the backend history generator when the opener's `cancel()`
 * stops it (WSM-ERR-012/013), so it shows that the backend stopped generating, not only that the
 * frontend stopped displaying.
 */
export interface Counters {
  history_started: number;
  history_points_sent: number;
  history_cancelled: number;
  exports: number;
  depths: number;
  quotes: number;
}

/** Timing overrides in milliseconds, used by the tests; mirrors `MarketService.__init__`'s keyword arguments. */
export interface MarketServiceOptions {
  tickIntervalMs?: number;
  historyDelayMs?: number;
  killDelayMs?: number;
}

type Action = (peer: Peer, payload: unknown, stream: Stream) => Promise<void>;

/**
 * One peer's pushed board: the tick streams and the controller that stops their generators.
 *
 * The Python port cancels a list of `asyncio.Task`s. Here the generators watch `stop.signal`, and
 * `streams` lets `forget` cancel the streams immediately instead of waiting for each generator to
 * observe the abort.
 */
interface Board {
  readonly stop: AbortController;
  readonly streams: Stream[];
}

/**
 * Waits `ms` milliseconds, or until `signal` aborts.
 *
 * It resolves on abort instead of rejecting; the caller checks `signal.aborted` afterwards. The
 * callers' `try` blocks therefore see only real failures such as `StreamReset` and `StreamClosed`.
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

/** Returns the payload's `symbol` field; throws `Error` if it is not a string. */
function symbolOf(payload: unknown): string {
  const symbol = fieldOf(payload, 'symbol');
  if (typeof symbol !== 'string') throw new Error(`this action needs a symbol; got ${JSON.stringify(symbol)}`);
  return symbol;
}

/** The demo backend: the market, the registry, the counters and the pushed tick streams. */
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

  /** The board of each peer that sent a hello. */
  private readonly boards = new Map<Peer, Board>();

  /** A `Map` rather than an object literal, because the repository's eslint config forbids `for...in`. */
  private readonly actionTable: Map<string, Action>;

  private tickInterval: number;

  private readonly historyDelay: number;

  private readonly killDelay: number;

  /**
   * The kill switch's pending close operation, or `null`. It mirrors the task reference the Python port
   * holds; a pending promise does not need a reference to stay alive.
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

  /** Names of the actions the frontend may request. */
  get actions(): string[] {
    return [...this.actionTable.keys()];
  }

  /** How often each pushed stream carries a price, in milliseconds. `rate` changes it. */
  get tickIntervalMs(): number {
    return this.tickInterval;
  }

  // ------------------------------------------------------------------ registration

  /**
   * Returns the `onStream` handler for one connection.
   *
   * A handler receives `(payload, stream)`, and the actions also need the `Peer`: the hello tags and
   * registers this connection, and the board is pushed onto it. The returned closure captures the peer.
   */
  handlerFor(peer: Peer): StreamHandler {
    return async (payload: unknown, stream: Stream): Promise<void> => {
      await this.dispatch(peer, payload, stream);
    };
  }

  /**
   * Routes one stream to the action named by the payload's `action` field.
   *
   * An unknown action throws `Error`; the peer resets the stream with `APPLICATION_ERROR`
   * (WSM-STM-034). `REFUSED` would state that the request was not processed and may be retried.
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
   * Stops pushing to a peer and removes it from the registry. Idempotent.
   *
   * The Python port cancels the tasks, which raises inside their pending `await`. Aborting a signal
   * only takes effect when the generator next checks it, while `close()` drains every live stream
   * before closing the socket (WSM-CON-024). This method therefore cancels the streams immediately and
   * aborts the signal to end the generator loops. `cancel()` is a no-op on a closed stream.
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

  // ------------------------------------------------------------------ the hello

  /**
   * Handles the hello (the subscription). It is an ordinary stream with no wire marking (WSM-RCN-021).
   *
   * The dialer sends it through `connect({hello})`, so it is replayed on every new socket
   * (WSM-RCN-020) and this method runs on a new `Peer` after every reconnect. An acceptor's `tags` end
   * with their connection (WSM-RCN-033), so the peer is tagged and registered here, on each connection.
   *
   * The frontend does not use the reply: `connect()`'s reconnect helper owns this stream and discards
   * its replies, and the acknowledgement WSM-RCN-022 requires is this handler returning. The symbol
   * data arrives on the pushed tick streams.
   */
  private async hello(peer: Peer, payload: unknown, stream: Stream): Promise<void> {
    const tags = fieldOf(payload, 'tags');
    if (typeof tags === 'object' && tags !== null) {
      // muxws does not read `tags` and defines no keys in it (WSM-REG-001/002); the keys are the
      // frontend's labels, and the registry indexes them as sent.
      Object.assign(peer.tags, tags);
    }
    // Registered after the tags are written: the registry indexes tags at registration and does not
    // observe later changes (WSM-REG-010).
    this.registry.register(peer);
    this.startBoard(peer);
    await stream.reply({
      ok: true,
      symbols: this.market.symbols.length,
      tick_interval: this.tickInterval / 1000,
    });
  }

  /**
   * Sets the interval between price updates on each pushed stream, from the payload's `interval_ms`.
   *
   * The interval is a `sleep` in the tick generator and is independent of transport throughput.
   * Accepted values are 1 to 5000 ms; anything else throws `Error`. The minimum is 1 ms because at
   * zero the twenty generators would occupy the event loop they send on.
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
   * Opens one tick stream per symbol from the acceptor side (WSM-INV-002).
   *
   * One stream per symbol, not one stream for all symbols: with twenty streams plus the export, a FIFO
   * writer and a round-robin writer produce different frame orders, which shows that a 1 MB payload
   * does not block other streams.
   */
  private startBoard(peer: Peer): void {
    if (this.boards.has(peer)) {
      // A second hello on the same socket does not start a second board. The reconnect helper sends
      // one hello per connection; a duplicate would produce duplicate tick streams.
      return;
    }
    const board: Board = { stop: new AbortController(), streams: [] };
    this.boards.set(peer, board);
    this.market.symbols.forEach((symbol) => {
      // Not awaited: these run for the lifetime of the socket. The rejection is caught here, because an
      // unhandled rejection terminates the Node process; a failed tick stream stops updating one row.
      void this.pushTicks(peer, symbol, board).catch((error: unknown) => {
        logger.error(`muxws demo: the ${symbol} tick stream stopped`, error);
      });
    });
    // Synchronous, as `onClose` handlers are. Without it, a socket closed outside `serveConnection`
    // (for example in a test) leaves twenty generators sending to a closed peer whose writer has been
    // discarded (WSM-RCN-042).
    peer.onClose(() => {
      this.forget(peer);
    });
  }

  /**
   * One symbol's price stream, for the lifetime of the socket.
   *
   * `peer.open()` is the same call the browser makes for a quote, with the same correlation and
   * cancellation; muxws has no separate push API (WSM-INV-002).
   */
  private async pushTicks(peer: Peer, symbol: string, board: Board): Promise<void> {
    let stream: Stream;
    try {
      stream = peer.open({ topic: 'ticks', symbol });
    } catch (error) {
      // The socket closed between the hello and this generator's first step. Nothing is buffered for a
      // later connection (WSM-RCN-042), and the error is not reported.
      if (error instanceof ConnectionLost || error instanceof ConnectionGoingAway) return;
      throw error;
    }
    board.streams.push(stream);
    try {
      // The snapshot is sent first, so the frontend can draw the row before the first price change;
      // the frontend holds no symbol data of its own.
      await stream.send(this.market.snapshot(symbol));
      for (;;) {
        await sleep(this.tickInterval, board.stop.signal);
        if (board.stop.signal.aborted) return;
        await stream.send(this.market.tick(symbol));
      }
    } catch (error) {
      // `ConnectionLost` is a `StreamReset`, so this covers a closed socket: every live stream fails
      // with it before `onClose` runs.
      if (!(error instanceof StreamReset) && !(error instanceof StreamClosed)) throw error;
    } finally {
      // A board stream does not end by itself, and `close()` waits for every live stream to finish
      // before closing the socket (WSM-CON-024/025). Without this cancel, twenty open tick streams would
      // keep the drain waiting for its full window. A no-op when the stream is already closed.
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
   * Streaming and cancellable (WSM-ERR-012/013).
   *
   * In the Python port the browser's `cancel()` cancels the handler's task and `CancelledError` is
   * raised at its pending `await`. TypeScript cannot interrupt a pending `await`, so the peer aborts
   * `stream.signal` instead (WSM-API-023): the sleep below ends early and the loop checks
   * `signal.aborted`. A handler that ignored the signal would keep producing points after the
   * cancellation, which WSM-ERR-012 forbids.
   *
   * `history_cancelled` counts cancellations observed by this handler.
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
      // A cancel during `send` is raised by `send` itself; a cancel during the sleep is detected through
      // the signal. Both paths reach this block, and `signal.aborted` identifies a cancellation.
      if (!stream.signal.aborted) throw error;
      this.counters.history_cancelled += 1;
      // Re-thrown (WSM-ERR-014). The peer sends nothing for a handler that fails after its stream was
      // cancelled. Returning normally would report a completed response for a cancelled stream; the
      // state check in the library's `runHandler` would still keep the `end` off the wire.
      throw error;
    }
    // Trailers are carried on the `end` frame; they hold values known only after the body is complete.
    await stream.end({ trailers: { symbol, points: points.length } });
  }

  /** Replies with a payload larger than MAX_FRAME_BYTES; the library fragments it (WSM-FRG-010). */
  private async depth(_peer: Peer, payload: unknown, stream: Stream): Promise<void> {
    this.counters.depths += 1;
    await stream.reply(this.market.depth(symbolOf(payload)));
  }

  /**
   * Replies with about 1 MB as one payload on one stream.
   *
   * The handler only calls `reply()`. The writer sends the payload in 64 KiB fragments and schedules
   * frames of the twenty tick streams between them (WSM-FRG-018/019, WSM-INV-004).
   */
  private async export(_peer: Peer, payload: unknown, stream: Stream): Promise<void> {
    this.counters.exports += 1;
    await stream.reply(this.market.export(symbolOf(payload)));
  }

  // ------------------------------------------------------------------ the diagnostics strip

  /** Replies with the counters and the backend's current peer, stream and symbol state. */
  private async stats(peer: Peer, _payload: unknown, stream: Stream): Promise<void> {
    await stream.reply({
      ...this.counters,
      peers: this.registry.peersFor().length,
      streams: peer.streams.size,
      symbols: this.market.symbols,
      // The tick interval in seconds, as the Python backend sends it. The hello reply also carries it,
      // but `connect({hello})` discards that reply (WSM-RCN-022).
      tick_interval: this.tickInterval / 1000,
    });
  }

  /**
   * Closes every registered peer after `killDelayMs`, so the browser reconnects.
   *
   * The close is scheduled, not awaited: `close()` sends `goaway`, drains and closes the socket
   * (WSM-CON-025), and running it inline would close the socket this handler's reply is queued on.
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
      // The board is stopped first. `close()` drains every live stream before closing the socket
      // (WSM-CON-024), and the twenty tick streams do not end by themselves, so the drain would wait
      // for its full window.
      this.forget(peer);
      try {
        await peer.close({ code: ResetCode.NO_ERROR, reason: "the demo's kill switch" });
      } catch (error) {
        logger.debug(`muxws demo: peer ${peer.id} was already gone when the kill switch reached it`, error);
      }
    }
  }
}
