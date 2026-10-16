/**
 * `Peer`: one end of one WebSocket; dialer and acceptor have the same API (§5, §9, WSM-INV-002).
 *
 * Mirrors `muxws/peer.py`. Differences follow from JavaScript having no task cancellation, no
 * `asyncio.Queue` and no `logging` module; each is commented where it occurs.
 */

import type { Codec } from './codec';
import {
  ConnectionClosed,
  ConnectionGoingAway,
  ConnectionLost,
  exceptionForReset,
  ProtocolError,
  RemoteError,
  ResetCode,
  StreamReset,
  StreamTimeout,
} from './errors';
import { encodedLength, MAX_FRAME_BYTES, splitFrame } from './fragment';
import { ABSENT, type Absent, type Frame } from './frames';
import { GoawayState, MAX_STREAM_ID, newNonce, PingRegistry } from './lifecycle';
import { type CloseReason, type FrameDirection, logFrame, logger } from './observability';
import { Stream, StreamState } from './stream';
import type { SocketAdapter } from './transports';
import { CONNECTION_LANE, LaneEncodingError, Writer } from './writer';

// `logger` is defined in `ts/observability.ts` and re-exported here for modules that import it from
// this one.
export { logger };
export type { CloseReason };

// --------------------------------------------------------------------------- connection identity

/**
 * Three lowercase hex characters, chosen once per module load. A log correlation id is not
 * security-sensitive, so `Math.random` is used.
 */
const PROCESS_PREFIX = Math.floor(Math.random() * 0x1000)
  .toString(16)
  .padStart(3, '0');

/**
 * Only incremented, so no two connections in a process share an id (WSM-API-009).
 */
let connectionCounter = 0;

// --------------------------------------------------------------------------- public types

/** `(payload, stream)`; the single incoming-stream handler of a peer (WSM-STM-030/031). */
export type StreamHandler = (payload: any, stream: Stream) => void | Promise<void>;

/** Converts a handler's error into the payload of the `reset(APPLICATION_ERROR)` frame (WSM-ERR-006). */
export type ErrorSerializer = (error: unknown) => unknown;

/**
 * `(headers, payload)`, called synchronously inside `open()` before an id is allocated. It adds,
 * replaces or deletes keys by mutating `headers`; its return value is ignored (WSM-API-027).
 */
export type OpenInterceptor = (headers: Record<string, unknown>, payload: unknown) => void;

/**
 * `(headers, stream)`, called synchronously by the call that sends this peer's first frame on a stream
 * the remote opened. It adds, replaces or deletes keys by mutating `headers`; its return value is
 * ignored (WSM-API-028).
 */
export type ReplyHeadersInterceptor = (headers: Record<string, unknown>, stream: Stream) => void;

/**
 * `(headers, stream)`, called once per stream when the remote's first frame on it arrives. `headers`
 * is frozen; the return value is ignored (WSM-API-029).
 */
export type IncomingHeadersInterceptor = (headers: Readonly<Record<string, unknown>>, stream: Stream) => void;

/** `open()`'s options. There is no `timeoutMs` among them: `open()` does not wait (WSM-API-018). */
export interface OpenOptions {
  payload?: unknown;
  headers?: Record<string, unknown>;
  end?: boolean;
}

/** `request()`'s options: `OpenOptions` plus the deadline, because this call waits (WSM-API-018). */
export interface RequestOptions extends OpenOptions {
  timeoutMs?: number;
}

/** Constructor options. Mirrors `Peer.__init__`'s keyword arguments. */
export interface PeerOptions {
  codec: Codec;
  isDialer: boolean;
  errorSerializer?: ErrorSerializer;
  /**
   * Test-only (WSM-FRG-005). The cap is a protocol constant; the conformance runner lowers it to
   * exercise fragmentation with small fixtures. A value too small to hold an envelope plus one
   * indivisible unit raises `ProtocolError` at construction (WSM-FRG-034).
   */
  maxFrameBytes?: number;
  /**
   * The largest reassembled payload this peer accepts, in bytes. Local and not announced
   * (WSM-FRG-035); a sender learns of it only from the resulting reset.
   */
  maxPayloadBytes?: number;
  /**
   * The number of streams the remote may have open on this peer at once. Local, not announced, and
   * not checked by the sender (WSM-STM-036/037).
   */
  maxConcurrentStreams?: number;
}

/** WSM-FRG-035's default: 64 MiB of reassembled payload. */
export const DEFAULT_MAX_PAYLOAD_BYTES = 67_108_864;

/** WSM-STM-036's default: 100 concurrent remote-opened streams. */
export const DEFAULT_MAX_CONCURRENT_STREAMS = 100;

/**
 * `close()`'s options (WSM-CON-025).
 *
 * Durations in TypeScript are integer milliseconds; Python uses float seconds. `drainMs`
 * corresponds to Python's `drain=10.0` (WSM-CON-024).
 */
export interface CloseOptions {
  code?: ResetCode;
  reason?: string;
  drainMs?: number;
}

/** `peer.ping()`'s default deadline in milliseconds; Python: `timeout=5.0` (WSM-CON-012). */
export const DEFAULT_PING_TIMEOUT_MS = 5000;

/** `peer.close()`'s default drain window in milliseconds; Python: `drain=10.0` (WSM-CON-024). */
export const DEFAULT_DRAIN_MS = 10_000;

/**
 * Milliseconds a connection-level protocol error waits for queued frames, `goaway` among them,
 * before it closes the socket. Equal to `close()`'s default drain; Python: `_FAILURE_DRAIN`.
 */
const FAILURE_DRAIN_MS = DEFAULT_DRAIN_MS;

/** WSM-ERR-006's default. A public-facing deployment should replace it with a redacting one. */
export function defaultErrorSerializer(error: unknown): unknown {
  if (error instanceof Error) return { type: error.name, message: error.message };
  return { type: typeof error, message: String(error) };
}

// --------------------------------------------------------------------------- small helpers

/**
 * Wait one macrotask turn, so timer callbacks and socket events can run.
 *
 * WSM-INV-004 depends on this. `nextFrame()` resolves immediately when there is work, and a socket
 * with buffer space resolves its send immediately, so awaiting them only drains the microtask queue.
 * Producers driven by a timer or an inbound socket message run as macrotasks; without this yield the
 * write loop would send every fragment of a large payload before any of them could enqueue a frame,
 * and the round-robin would have only one lane.
 *
 * Uses `setImmediate` where available (Node), otherwise `MessageChannel`, and `setTimeout(0)` only as
 * a last resort: browsers clamp nested timers to 4 ms, which would limit a per-frame yield to a few
 * hundred frames per second.
 */
const yieldToOtherTasks: () => Promise<void> = (() => {
  const immediate = (globalThis as { setImmediate?: (callback: () => void) => unknown }).setImmediate;
  if (typeof immediate === 'function') {
    return () => new Promise<void>((resolve) => immediate(resolve));
  }
  if (typeof MessageChannel === 'function') {
    const channel = new MessageChannel();
    const waiting: (() => void)[] = [];
    channel.port1.onmessage = () => waiting.shift()?.();
    return () =>
      new Promise<void>((resolve) => {
        waiting.push(resolve);
        channel.port2.postMessage(undefined);
      });
  }
  return () => new Promise<void>((resolve) => setTimeout(resolve, 0));
})();

/** The longest close reason a WebSocket close frame carries (RFC 6455 5.5: 125 bytes less the code). */
const MAX_CLOSE_REASON_BYTES = 123;

/**
 * @internal `reason` cut to the 123 UTF-8 bytes a WebSocket close frame carries, at a character boundary.
 *
 * `ws` throws on a longer reason, and a browser `WebSocket` throws a `SyntaxError`; either leaves the
 * socket open. The `goaway` frame and `onClose` carry the full text.
 */
export function closeFrameReason(reason: string): string {
  const bytes = new TextEncoder().encode(reason);
  let end = Math.min(bytes.length, MAX_CLOSE_REASON_BYTES);
  // Back off over UTF-8 continuation bytes (10xxxxxx) so no character is split.
  if (end < bytes.length) {
    while (end > 0 && (bytes[end] & 0xc0) === 0x80) end -= 1;
  }
  return new TextDecoder().decode(bytes.subarray(0, end));
}

/** Python's `str(exc)`. */
function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function isAbsent(value: unknown): value is Absent {
  return value === ABSENT;
}

/** The frame's payload, with `undefined` read as `ABSENT` (see `frames.fieldValue`). */
function payloadOf(frame: Frame): unknown {
  return frame.payload === undefined ? ABSENT : frame.payload;
}

function hasFragment(frame: Frame): boolean {
  return frame.fragment !== null && frame.fragment !== undefined;
}

const OPEN_OPTION_KEYS: ReadonlySet<string> = new Set(['payload', 'headers', 'end']);
const REQUEST_OPTION_KEYS: ReadonlySet<string> = new Set(['payload', 'headers', 'end', 'timeoutMs']);

/**
 * Whether a single argument is the options object rather than a payload (WSM-API-020).
 *
 * True only when the object has at least one own key and every own key is an option of this call.
 * `open({})` therefore sends the payload `{}`; `open()` sends no payload.
 */
function isOptionsObject(value: unknown, keys: ReadonlySet<string>): boolean {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const own = Object.keys(value as object);
  return own.length > 0 && own.every((key) => keys.has(key));
}

interface ResolvedCall {
  payload: unknown;
  headers: Record<string, unknown> | null;
  end: boolean;
  timeoutMs: number | undefined;
}

/** Resolve the `(payload?)` / `(payload?, options?)` / `(options)` overloads. */
function resolveCall(first: unknown, second: RequestOptions | undefined, keys: ReadonlySet<string>): ResolvedCall {
  const options: RequestOptions =
    second === undefined && isOptionsObject(first, keys) ? (first as RequestOptions) : (second ?? {});
  const positional = options === first ? undefined : first;
  return {
    // `open()` with no payload sends `"payload": null`, as Python's default does.
    payload: positional ?? options.payload ?? null,
    headers: options.headers ?? null,
    end: options.end ?? false,
    timeoutMs: options.timeoutMs,
  };
}

/**
 * WSM-FRG-034: a cap too small to hold an envelope plus one unit raises `ProtocolError` at
 * construction.
 *
 * The cap is a protocol constant (WSM-FRG-004); a smaller value is passed only through the test-only
 * constructor option of WSM-FRG-005, which the conformance runner uses.
 */
function checkedFrameCap(cap: number, codec: Codec): number {
  if (cap === MAX_FRAME_BYTES) return cap;
  const probe: Frame = { type: 'data', stream: 1, payload: { a: 'aaaaaaaa' } };
  try {
    splitFrame(probe, cap, codec);
  } catch (error) {
    if (!(error instanceof ProtocolError)) throw error;
    throw new ProtocolError(
      `maxFrameBytes=${cap} cannot hold an envelope plus one indivisible unit of payload: ` +
        `${error.message} (WSM-FRG-034)`,
    );
  }
  return cap;
}

/**
 * A monotonic clock in milliseconds; the equivalent of Python's `asyncio.get_running_loop().time()`.
 *
 * `performance.now()`, not `Date.now()`: it is monotonic, so a system clock adjustment cannot produce
 * a negative round-trip time, and it has sub-millisecond resolution, so an in-memory ping does not
 * measure zero.
 */
function monotonicNowMs(): number {
  return globalThis.performance.now();
}

/** Whether `value` is a thenable, as returned by an `async` interceptor. */
function isThenable(value: unknown): value is PromiseLike<unknown> {
  return (
    value !== null &&
    (typeof value === 'object' || typeof value === 'function') &&
    typeof (value as { then?: unknown }).then === 'function'
  );
}

/**
 * Run outgoing interceptors over a copy of the caller's headers (WSM-API-027/028).
 *
 * Runs before anything is enqueued, so an interceptor that throws causes nothing to be sent. The
 * result is copied again, so an interceptor that keeps a reference to `headers` cannot change the
 * frame after returning. Returns `null` when the caller passed no headers and the interceptors added
 * none, so the frame has no `headers` field.
 */
function interceptedHeaders<A>(
  interceptors: Map<symbol, (headers: Record<string, unknown>, argument: A) => void>,
  headers: Record<string, unknown> | null,
  argument: A,
  kind: string,
): Record<string, unknown> | null {
  const working: Record<string, unknown> = { ...(headers ?? {}) };
  [...interceptors.values()].forEach((interceptor) => {
    const result: unknown = interceptor(working, argument);
    if (isThenable(result)) {
      result.then(undefined, () => undefined);
      throw new TypeError(
        `a ${kind} interceptor returned a thenable; interceptors are synchronous and nothing awaits them ` +
          '(WSM-API-027)',
      );
    }
  });
  if (Object.getOwnPropertySymbols(working).length > 0) {
    throw new TypeError(`${kind} interceptors left symbol header keys, which no codec carries (WSM-API-027)`);
  }
  if (Object.keys(working).length === 0 && headers === null) return null;
  return { ...working };
}

/**
 * @internal Register the three interceptor arrays that `connect()` and `accept()` accept on `peer`, in
 * array order, as the corresponding `peer.intercept*()` calls do (WSM-API-026).
 */
export function registerInterceptors(
  peer: Peer,
  options: {
    openInterceptors?: readonly OpenInterceptor[];
    replyHeadersInterceptors?: readonly ReplyHeadersInterceptor[];
    incomingHeadersInterceptors?: readonly IncomingHeadersInterceptor[];
  },
): void {
  (options.openInterceptors ?? []).forEach((interceptor) => peer.interceptOpen(interceptor));
  (options.replyHeadersInterceptors ?? []).forEach((interceptor) => peer.interceptReplyHeaders(interceptor));
  (options.incomingHeadersInterceptors ?? []).forEach((interceptor) => peer.interceptIncomingHeaders(interceptor));
}

/** Add `entry` under a new key and return a function that removes that entry. */
function register<T>(registry: Map<symbol, T>, entry: T): () => void {
  const key = Symbol('registration');
  registry.set(key, entry);
  return () => {
    registry.delete(key);
  };
}

/** The value a deadline rejects with; a symbol, so it is distinct from any `Error`. */
const DEADLINE_EXPIRED: unique symbol = Symbol('DEADLINE_EXPIRED');

// --------------------------------------------------------------------------- the peer

/**
 * The part of the reconnect helper that `Peer` uses.
 *
 * Structural, not an import of `ConnectionLoop`, because `ts/reconnect.ts` imports `Peer` and an
 * import here would create a cycle.
 */
export interface ConnectionSupervisor {
  stop(): Promise<void>;
}

/** The single peer type for both roles; a server push is an `open()` from the acceptor. */
export class Peer {
  /**
   * Not `readonly`: a `Peer` is reused across reconnects, and `adoptSocket` assigns the next counter
   * value (WSM-API-009). A reconnect therefore appears in the log as a new `conn=`, and no id is
   * reused.
   */
  id: string;

  /** A plain object for application data. muxws does not read it (WSM-REG-001/002). */
  readonly tags: Record<string, unknown> = {};

  /** Not `readonly`: `adoptSocket` replaces it when the helper re-establishes the connection. */
  private socket: SocketAdapter;

  private readonly codec: Codec;

  private readonly dialer: boolean;

  private readonly errorSerializer: ErrorSerializer;

  /** The frame cap: the send path fragments at it, the receive path checks whole messages against it. */
  private readonly maxFrameBytes: number;

  /** WSM-FRG-035. Local, not announced, and enforced as fragments accumulate (WSM-FRG-032). */
  private readonly maxPayloadBytes: number;

  /** WSM-STM-036. Local, not announced, and counting only remote-opened streams (WSM-STM-037). */
  private readonly maxConcurrentStreams: number;

  /**
   * @internal The next id this peer will allocate: the dialer allocates odd ids and the acceptor even
   * ones (WSM-SID-002).
   *
   * Public so a test can move the allocator to the end of the id space; the library writes it only
   * when allocating and in `adoptSocket`.
   */
  nextId: number;

  private readonly liveStreams = new Map<number, Stream>();

  /** Two integers plus the live-stream map; no state per closed stream (WSM-STM-001). */
  private highestLocalOpen = 0;

  private highestRemoteOpen = 0;

  private ignoredLateFrames = 0;

  private handler: StreamHandler | null = null;

  private readonly closeHandlers: ((reason: CloseReason) => void)[] = [];

  private readonly reconnectHandlers: ((attempt: number, peer: Peer) => void)[] = [];

  private readonly frameHandlers: ((direction: 'tx' | 'rx', frame: Frame, byteLength: number) => void)[] = [];

  private readonly errorHandlers: ((error: unknown, stream: Stream | null) => void)[] = [];

  /**
   * Keyed by one symbol per registration, so a function registered twice is two registrations and
   * each unregister function removes only its own. `adoptSocket` keeps all three: they belong to the
   * `Peer`, not to its connection (WSM-API-026).
   */
  private readonly openInterceptors = new Map<symbol, OpenInterceptor>();

  private readonly replyInterceptors = new Map<symbol, ReplyHeadersInterceptor>();

  private readonly incomingInterceptors = new Map<symbol, IncomingHeadersInterceptor>();

  /** Outstanding pings, keyed by nonce (see `ts/lifecycle.ts`). */
  private readonly pings = new PingRegistry();

  /** The send time of each outstanding ping, for computing the round-trip time on `pong`. */
  private readonly pingStarted = new Map<string, number>();

  /** The `goaway` state, per direction. */
  private goaway = new GoawayState();

  /**
   * The send path: round-robin across streams, not a FIFO of frames (WSM-FRG-019), so a large
   * payload on one stream does not delay small frames on others.
   */
  private writer: Writer;

  private writerTask: Promise<void> | null = null;

  private writerStopped = false;

  private open_ = true;

  /**
   * @internal False from when the helper adopts a socket until the hello is acknowledged.
   *
   * Defaults to `true`, so a peer created by `accept()`, which has no hello and no helper, is
   * established when its socket is open (WSM-CON-030). Only the reconnect driver writes it, for the
   * window in which WSM-RCN-043 requires `isOpen` to be false.
   */
  established = true;

  /**
   * Whether a `willRetry: false` close has already been reported (WSM-RCN-044).
   *
   * At most one is reported per connection. When the attempt cap is reached through hello failures,
   * the last loss already carries `willRetry: false`, and the helper's own give-up call to
   * `notifyClose` is then suppressed.
   */
  private finalCloseReported = false;

  private death: ConnectionClosed | null = null;

  /**
   * @internal The monotonic clock used for activity stamps and by `Heartbeat` (WSM-RCN-010).
   *
   * `Heartbeat` reads this same function, so a clock injected by a test applies to both the activity
   * stamp and the idle measurement.
   */
  clock: () => number = monotonicNowMs;

  private lastActivityMs = 0;

  /**
   * @internal True while the reconnect helper intends to dial again.
   *
   * `CloseReason.willRetry` is read from it when the socket is lost, so it is set when a connection
   * is established, not when it is lost (WSM-RCN-040).
   */
  willRetry = false;

  /** @internal Lets `close()` stop the helper from dialling again (WSM-RCN-040/044). */
  connectionLoop: ConnectionSupervisor | null = null;

  constructor(socket: SocketAdapter, options: PeerOptions) {
    this.id = `${PROCESS_PREFIX}-${connectionCounter}`;
    connectionCounter += 1;

    this.socket = socket;
    this.codec = options.codec;
    this.dialer = options.isDialer;
    this.errorSerializer = options.errorSerializer ?? defaultErrorSerializer;
    this.maxFrameBytes = checkedFrameCap(options.maxFrameBytes ?? MAX_FRAME_BYTES, options.codec);
    this.maxPayloadBytes = options.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES;
    this.maxConcurrentStreams = options.maxConcurrentStreams ?? DEFAULT_MAX_CONCURRENT_STREAMS;
    this.nextId = options.isDialer ? 1 : 2;
    this.writer = new Writer(options.codec, { maxFrameBytes: this.maxFrameBytes });
  }

  // ------------------------------------------------------------------ properties

  /**
   * True when the socket is open and the connection is established (WSM-RCN-043).
   *
   * With a hello these are two different moments (WSM-RCN-004). Until the hello is acknowledged the
   * application may not send: an `open()` in that window would send an application frame before the
   * hello (WSM-RCN-023).
   */
  get isOpen(): boolean {
    return this.open_ && this.established;
  }

  /**
   * @internal Whether a frame enqueued now can still be sent.
   *
   * Differs from `isOpen`: `isOpen` is false during the hello window (WSM-RCN-043), while this is true
   * whenever a socket exists, including during that window, in which the hello is sent.
   * `Stream.reset()` uses this, so a reset of a stream the acceptor opened during the hello window is
   * sent and the remote does not keep a stream this side has closed (WSM-STM-021).
   */
  get hasASocket(): boolean {
    return this.open_;
  }

  /**
   * The time a frame was last sent or received on this socket, on `clock`'s scale.
   *
   * The heartbeat pings only after an idle interval measured from this stamp, not on a fixed
   * schedule, so a busy connection is not pinged (WSM-RCN-010). Frames in both directions count.
   */
  get lastActivity(): number {
    return this.lastActivityMs;
  }

  /**
   * @internal Record the current `clock` time as the last activity.
   *
   * Called by `reportFrame` for every frame, and once by `Heartbeat` when it starts, so a newly
   * established connection without traffic is not treated as idle since time zero and pinged
   * immediately.
   */
  stampActivity(): void {
    this.lastActivityMs = this.clock();
  }

  /** Live streams, read-only. */
  get streams(): ReadonlyMap<number, Stream> {
    return new Map(this.liveStreams);
  }

  get isDialer(): boolean {
    return this.dialer;
  }

  // ------------------------------------------------------------------ registration

  /** Register the incoming-stream handler. A second call replaces the first and logs a warning (WSM-STM-030). */
  onStream(handler: StreamHandler): void {
    if (this.handler !== null) {
      logger.warn(`muxws conn=${this.id} replacing the onStream handler (WSM-STM-030)`);
    }
    this.handler = handler;
  }

  onClose(handler: (reason: CloseReason) => void): void {
    this.closeHandlers.push(handler);
  }

  /**
   * Called once per re-established connection (WSM-RCN-030).
   *
   * When it is called, the socket is open and the acceptor has acknowledged the hello. No stream is
   * carried over, nothing is replayed, and the new connection's id space is empty (WSM-RCN-031/032).
   */
  onReconnect(handler: (attempt: number, peer: Peer) => void): void {
    this.reconnectHandlers.push(handler);
  }

  /** @internal Called by `ConnectionLoop` after the hello is acknowledged. */
  fireReconnect(attempt: number): void {
    this.fanOut('onReconnect', this.reconnectHandlers, attempt, this);
  }

  /**
   * Call every handler, catching and logging what each one throws.
   *
   * `fireReconnect` and `notifyClose` are called by the reconnect supervisor; an exception from an
   * application callback would otherwise propagate into it and stop it. A handler that throws does
   * not prevent the remaining handlers from running.
   */
  private fanOut<T extends unknown[]>(what: string, handlers: ((...args: T) => void)[], ...args: T): void {
    handlers.forEach((handler) => {
      try {
        handler(...args);
      } catch (error) {
        logger.error(`muxws conn=${this.id} a ${what} handler threw; the rest still run`, error);
      }
    });
  }

  /** `(direction, frame, byteLength)`, before encode and after decode (WSM-OBS-003). */
  onFrame(handler: (direction: 'tx' | 'rx', frame: Frame, byteLength: number) => void): void {
    this.frameHandlers.push(handler);
  }

  /**
   * Register an interceptor for the headers of every stream this peer opens (WSM-API-026/027).
   *
   * `open()`, `notify()` and `request()` call it synchronously as `(headers, payload)` before an id is
   * allocated; it adds, replaces or deletes keys by mutating `headers`. One that throws fails the open
   * with that error. Returns a function that removes this registration.
   */
  interceptOpen(interceptor: OpenInterceptor): () => void {
    return register(this.openInterceptors, interceptor);
  }

  /**
   * Register an interceptor for the leading headers this peer answers with (WSM-API-026/028).
   *
   * Called once per stream the remote opened, as `(headers, stream)`, by the call that sends this
   * peer's first frame on it: `sendHeaders()`, `send()`, `end()`, `reply()`, or the implicit end of a
   * handler that returns. It adds, replaces or deletes keys by mutating `headers`. One that throws
   * fails that call with that error. Returns a function that removes this registration.
   */
  interceptReplyHeaders(interceptor: ReplyHeadersInterceptor): () => void {
    return register(this.replyInterceptors, interceptor);
  }

  /**
   * @internal The leading headers of this peer's answer on `stream` after the reply interceptors have
   * run. Called by the stream before it records its first frame as sent, so after an interceptor
   * throws the stream can still send its first frame (WSM-API-028).
   */
  replyHeadersFor(headers: Record<string, unknown> | null, stream: Stream): Record<string, unknown> | null {
    if (this.replyInterceptors.size === 0) return headers;
    return interceptedHeaders(this.replyInterceptors, headers, stream, 'reply headers');
  }

  /**
   * Register an interceptor for the leading headers the remote sends (WSM-API-026/029).
   *
   * Called once per stream as `(headers, stream)` when the remote's first frame on it arrives: the
   * `open` of a stream the remote opened, the first `data` of a stream this peer opened. One that
   * throws is logged and changes nothing. Returns a function that removes this registration.
   */
  interceptIncomingHeaders(interceptor: IncomingHeadersInterceptor): () => void {
    return register(this.incomingInterceptors, interceptor);
  }

  /**
   * Register a handler for failures of streams without a consumer (WSM-API-016).
   *
   * A `Stream`'s internal promise has a no-op rejection handler from its constructor, so a reset on a
   * stream that is not awaited does not become an unhandled rejection; the failure is reported to
   * these handlers instead.
   */
  onError(handler: (error: unknown, stream: Stream | null) => void): void {
    this.errorHandlers.push(handler);
  }

  /**
   * @internal Called by `Stream`. Report the failure of a stream without a consumer (WSM-API-016).
   */
  reportStreamError(error: unknown, stream: Stream | null = null): void {
    if (this.errorHandlers.length === 0) {
      logger.debug(
        `muxws conn=${this.id} stream=${stream?.id ?? '-'} failure with no consumer: ${errorMessage(error)}`,
      );
      return;
    }
    this.errorHandlers.forEach((handler) => {
      handler(error, stream);
    });
  }

  // ------------------------------------------------------------------ opening

  /**
   * Open a stream. Synchronous; it never waits for capacity (WSM-API-001/004).
   *
   * The guard and the body run in one synchronous turn, so allocation and enqueue are one indivisible
   * step (WSM-SID-006); see `allocateAndEnqueue`.
   */
  open<T = unknown>(payload?: unknown, options?: OpenOptions): Stream<T>;
  open<T = unknown>(options: OpenOptions): Stream<T>;
  open<T = unknown>(first?: unknown, second?: OpenOptions): Stream<T> {
    this.throwIfUnopenable();
    if (this.openInterceptors.size === 0) return this.allocateAndEnqueue<T>(first, second);
    const { payload, headers, end } = resolveCall(first, second, OPEN_OPTION_KEYS);
    // Before the allocation, so an interceptor that throws consumes no id (WSM-API-027).
    const intercepted = interceptedHeaders(this.openInterceptors, headers, payload, 'open');
    return this.allocateAndEnqueue<T>(payload, { headers: intercepted ?? undefined, end });
  }

  /**
   * @internal `open()`'s body without the `isOpen` and `goaway` guard. The reconnect driver uses it to
   * send the hello.
   *
   * `isOpen` is false between adopting a socket and the hello acknowledgement (WSM-RCN-043), so
   * `open()` throws in that window and no application frame precedes the hello (WSM-RCN-023). The
   * hello is sent in that window, so the driver calls this method. It is not a parameter of the
   * public `open()`, so applications cannot bypass the guard.
   *
   * Allocation and enqueue are one indivisible step (WSM-SID-006): there is no `await` in this
   * method. An `await` between the allocation and the `enqueue` would let two concurrent calls
   * interleave and send stream ids out of order (WSM-INV-005).
   */
  allocateAndEnqueue<T = unknown>(first?: unknown, second?: OpenOptions): Stream<T> {
    if (this.exhausted()) {
      throw new ConnectionGoingAway(
        `stream ids are exhausted at ${MAX_STREAM_ID}; this connection can open no more (WSM-SID-007)`,
      );
    }
    const { payload, headers, end } = resolveCall(first, second, OPEN_OPTION_KEYS);

    const streamId = this.nextId;
    this.nextId += 2;
    this.highestLocalOpen = streamId;
    const stream = new Stream<T>(this, streamId, { headers, payload, local: true });
    stream.state = end ? StreamState.HALF_CLOSED_LOCAL : StreamState.OPEN;
    this.liveStreams.set(streamId, stream);
    this.enqueue({ type: 'open', stream: streamId, payload, headers, end });
    if (this.exhausted()) this.beginExhaustionShutdown();
    return stream;
  }

  /**
   * WSM-SID-007: exhausting the stream ids starts an orderly shutdown, not an error.
   *
   * WSM-SID-007 requires sending `goaway`, refusing further opens, draining in-flight streams, then
   * closing. `close()` is started, not awaited, because `open()` returns without suspending
   * (WSM-API-001). The stream just allocated is in flight and gets the drain window like any other.
   */
  private beginExhaustionShutdown(): void {
    if (this.goaway.sent || this.exhaustionShutdown !== null) return;
    this.exhaustionShutdown = this.close({
      code: ResetCode.NO_ERROR,
      reason: `stream ids exhausted at ${MAX_STREAM_ID} (WSM-SID-007)`,
    });
    void this.exhaustionShutdown.catch(() => undefined);
  }

  /** WSM-API-004: throw `ConnectionLost` or `ConnectionGoingAway` synchronously; never for concurrency. */
  private throwIfUnopenable(): void {
    // `isOpen`, not `open_`: during the hello window the socket is open but the connection is not
    // established, and no application frame may precede the hello (WSM-RCN-023/043).
    if (!this.isOpen) {
      throw new ConnectionLost('the peer is between sockets; nothing is buffered for the next one');
    }
    if (this.goaway.received) {
      throw new ConnectionGoingAway(
        `the remote sent goaway (code ${this.goaway.receivedCode}); no new stream can be opened on ` +
          'this connection. Dial again to get one that can.',
      );
    }
    if (this.goaway.sent) {
      throw new ConnectionGoingAway('this peer sent goaway and opens no further streams (WSM-CON-021)');
    }
  }

  /** The WSM-SID-007 shutdown in progress, if any; tests await it. */
  exhaustionShutdown: Promise<void> | null = null;

  private exhausted(): boolean {
    return this.nextId > MAX_STREAM_ID;
  }

  /** One-shot push. Returns nothing and produces no awaitable handle (WSM-API-005). */
  async notify(payload?: unknown, options?: { headers?: Record<string, unknown> }): Promise<void> {
    const stream = this.open(payload, { headers: options?.headers, end: true });
    stream.claim = 'notify';
  }

  /**
   * `open(payload, { end: true })` awaited to the stream's end (WSM-API-006).
   *
   * Unlike `await stream`, a second payload is an error: the stream is reset with `PROTOCOL_ERROR`
   * and the call rejects with `ProtocolError` (WSM-API-007).
   */
  request<T = unknown>(payload?: unknown, options?: RequestOptions): Promise<T>;
  request<T = unknown>(options: RequestOptions): Promise<T>;
  async request<T = unknown>(first?: unknown, second?: RequestOptions): Promise<T> {
    const { payload, headers, timeoutMs } = resolveCall(first, second, REQUEST_OPTION_KEYS);
    const stream = this.open(payload, { headers: headers ?? undefined, end: true });
    return this.collectUnary<T>(stream, timeoutMs);
  }

  private async collectUnary<T>(stream: Stream, timeoutMs: number | undefined): Promise<T> {
    const collector = this.collect<T>(stream);
    if (timeoutMs === undefined) return collector;

    let timer: ReturnType<typeof setTimeout> | undefined;
    const deadline = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => {
        reject(DEADLINE_EXPIRED);
      }, timeoutMs);
    });

    try {
      return await Promise.race([collector, deadline]);
    } catch (error) {
      if (error !== DEADLINE_EXPIRED) throw error;
      // JavaScript cannot cancel the collector; it keeps running and its later rejection is absorbed
      // here. The reset is sent with TIMEOUT before the collector ends, so the remote receives
      // TIMEOUT, not the CANCELLED that an abandoned iterator sends (WSM-ERR-011).
      collector.catch(() => undefined);
      await stream.reset(ResetCode.TIMEOUT, `deadline of ${timeoutMs}ms expired`);
      throw new StreamTimeout(`stream ${stream.id} did not answer within ${timeoutMs}ms`, { streamId: stream.id });
    } finally {
      clearTimeout(timer);
    }
  }

  private async collect<T>(stream: Stream): Promise<T> {
    const payloads: unknown[] = [];
    for await (const item of stream) {
      payloads.push(item);
      if (payloads.length > 1) {
        await stream.reset(ResetCode.PROTOCOL_ERROR, 'request() received more than one payload');
        throw new ProtocolError(
          `request() on stream ${stream.id} received more than one payload; use peer.open() and ` +
            'iterate if the remote streams (WSM-API-006)',
        );
      }
    }
    if (payloads.length === 0) throw new ProtocolError(`request() on stream ${stream.id} ended without a payload`);
    return payloads[0] as T;
  }

  // ------------------------------------------------------------------ the writer

  /** @internal Called by `Stream` and `Peer`. Synchronous, so `open()` does not suspend mid-allocation. */
  enqueue(frame: Frame): void {
    this.writer.enqueue(frame);
  }

  private async runWriter(): Promise<void> {
    for (;;) {
      let frame: Frame | null;
      try {
        frame = await this.writer.nextFrame();
      } catch (error) {
        if (error instanceof LaneEncodingError) {
          // A codec error inside the writer fails only that lane's stream; the write loop and the
          // connection continue.
          await this.failLane(error);
          continue;
        }
        throw error;
      }
      if (frame === null || this.writerStopped) return;

      try {
        const encoded = this.codec.encode(frame);
        this.reportFrame('tx', frame, encoded);
        if (this.codec.binary) {
          await this.socket.sendBytes(encoded as ArrayBuffer);
        } else {
          await this.socket.sendText(encoded as string);
        }
        // Fragment n+1 is sliced only after fragment n has been passed to the socket (WSM-FRG-018).
        this.writer.advance(frame.stream ?? CONNECTION_LANE);
        // One macrotask turn, so other producers can enqueue; see `yieldToOtherTasks` (WSM-INV-004).
        await yieldToOtherTasks();
      } catch (error) {
        // The socket is closed; the read loop in `serve()` marks the peer as dead.
        if (error instanceof ConnectionClosed) return;
        logger.error(`muxws conn=${this.id} could not send a ${frame.type} frame`, error);
        await this.failUnsendable(frame, error);
      }
    }
  }

  /**
   * A frame on one lane could not be encoded. Fail its stream; the connection continues.
   *
   * With no live stream on the lane, the peer is marked dead and the socket is closed with 1011.
   */
  private async failLane(failure: LaneEncodingError): Promise<void> {
    const detail = errorMessage(failure.cause);
    logger.error(`muxws conn=${this.id} could not encode a frame on stream ${failure.lane}`, failure.cause);
    const stream = this.liveStreams.get(failure.lane);
    if (stream === undefined) {
      await this.dieAndClose(new ConnectionClosed(String(failure), { code: 1011, reason: String(failure) }));
      return;
    }
    this.enqueue({ type: 'reset', stream: stream.id, code: ResetCode.INTERNAL_ERROR, reason: detail });
    stream.fail(exceptionForReset(ResetCode.INTERNAL_ERROR, detail, { streamId: stream.id }));
  }

  /**
   * A frame could not be encoded or sent. Fail its stream; the connection continues.
   *
   * With no live stream for the frame, the peer is marked dead and the socket is closed with 1011.
   */
  private async failUnsendable(frame: Frame, error: unknown): Promise<void> {
    const detail = errorMessage(error);
    const stream = frame.stream === null || frame.stream === undefined ? undefined : this.liveStreams.get(frame.stream);
    if (stream === undefined) {
      const reason = `could not encode a ${frame.type} frame: ${detail}`;
      await this.dieAndClose(new ConnectionClosed(reason, { code: 1011, reason }));
      return;
    }
    this.enqueue({ type: 'reset', stream: stream.id, code: ResetCode.INTERNAL_ERROR, reason: detail });
    stream.fail(exceptionForReset(ResetCode.INTERNAL_ERROR, detail, { streamId: stream.id }));
  }

  /**
   * WSM-OBS-003: the hook receives the logical frame and the encoded length. On tx the frame is
   * encoded first and the hook is called with the encoded length before the send; on rx the hook is
   * called after decode.
   */
  private reportFrame(direction: FrameDirection, frame: Frame, encoded: string | ArrayBuffer): void {
    // Stamped for every frame in both directions, before the hooks run, so a throwing hook cannot
    // prevent the activity stamp (WSM-RCN-010).
    this.stampActivity();
    const length = encodedLength(encoded);
    // `onFrame` handlers run on the read and write loops; `fanOut` catches what they throw, so a
    // throwing handler does not stop either loop.
    this.fanOut('onFrame', this.frameHandlers, direction, frame, length);
    // Payload contents are not logged; application data may contain secrets (WSM-OBS-002).
    logFrame(this.id, direction, frame, length);
  }

  // ------------------------------------------------------------------ the read loop

  /** Run the read loop until the socket closes. */
  async serve(): Promise<void> {
    if (this.writerTask === null) this.writerTask = this.runWriter();
    try {
      await this.readLoop();
    } catch (error) {
      // `readLoop` is not expected to throw. If it does, the peer is marked dead and the socket closed
      // before `serve()` rejects with the error, so `isOpen` does not stay true after the read loop has
      // stopped, and `isOpen` is not false while the socket stays open.
      logger.error(`muxws conn=${this.id} read loop failed`, error);
      const detail = errorMessage(error);
      await this.failInternal(`read loop failed: ${detail}`, detail);
      throw error;
    } finally {
      await this.stopWriter();
    }
  }

  private async readLoop(): Promise<void> {
    for (;;) {
      let message: string | ArrayBuffer;
      try {
        message = await this.socket.receive();
      } catch (error) {
        if (!(error instanceof ConnectionClosed)) throw error;
        this.die(error);
        return;
      }

      let frame: Frame;
      try {
        frame = this.codec.decode(message);
      } catch (error) {
        if (!(error instanceof ProtocolError)) throw error;
        await this.failConnection(`undecodable message: ${errorMessage(error)}`);
        return;
      }

      this.reportFrame('rx', frame, message);
      // Checked before dispatch, so an over-cap message is not assembled, decoded further, or passed
      // to a handler (WSM-FRG-031).
      if (!(await this.withinFrameCap(frame, message))) continue;

      let keepGoing: boolean;
      try {
        keepGoing = await this.dispatch(frame);
      } catch (error) {
        // An exception from `dispatch` closes the connection and the socket. Otherwise pending awaits
        // would never settle, `onClose` would not fire, `open()` would keep succeeding on a stopped
        // read loop, and an open socket would leave the remote waiting on a peer that reads nothing.
        logger.error(`muxws conn=${this.id} read loop failed on a ${frame.type} frame`, error);
        const detail = errorMessage(error);
        await this.failInternal(`read loop failed: ${detail}`, detail);
        return;
      }
      if (!keepGoing) return;
    }
  }

  /**
   * WSM-FRG-031: check the size of the whole encoded message, not of the `fragment` field.
   *
   * A receiver accepts messages up to `MAX_FRAME_BYTES` (WSM-FRG-004), so only larger messages fail
   * (or, in a test, messages above the lowered constructor cap). For a stream frame the violation is
   * stream-level and the connection continues (WSM-STM-020); for a connection-level frame it fails
   * the connection.
   */
  private async withinFrameCap(frame: Frame, message: string | ArrayBuffer): Promise<boolean> {
    const size = encodedLength(message);
    if (size <= this.maxFrameBytes) return true;
    if (frame.stream === null || frame.stream === undefined) {
      await this.failConnection(`a connection-level frame of ${size} bytes exceeds the cap`);
      return false;
    }

    const stream = this.liveStreams.get(frame.stream);
    const continuesOpen = stream !== undefined && stream.opening && hasFragment(frame);
    if (frame.type === 'open' && !continuesOpen) {
      // The id of an over-cap `open` is validated and counts toward the high-water mark, so a later
      // `data` or `reset` on it is ignored (WSM-STM-002) and is not above the mark (WSM-STM-003).
      if (!(await this.admitRemoteOpen(frame.stream))) return false;
    } else if (stream === undefined && (frame.type === 'data' || frame.type === 'reset')) {
      // No live stream to reset: the id is classified by the high-water mark (WSM-STM-002/003).
      await this.onStreamFrame(frame);
      return false;
    }
    if (stream !== undefined) {
      this.resetStream(
        stream,
        ResetCode.PAYLOAD_TOO_LARGE,
        `an encoded message of ${size} bytes exceeds this receiver's cap (WSM-FRG-031)`,
      );
    } else {
      // No live stream, typically an over-cap `open`. The reset is still sent, because it is the only
      // way the sender learns of the unannounced limit.
      this.enqueue({
        type: 'reset',
        stream: frame.stream,
        code: ResetCode.PAYLOAD_TOO_LARGE,
        reason: `an encoded message of ${size} bytes exceeds this receiver's cap`,
      });
    }
    return false;
  }

  /** Route one decoded frame. Returns false when the connection must end. */
  private async dispatch(frame: Frame): Promise<boolean> {
    if (frame.type === 'open') return this.onOpenFrame(frame);
    if (frame.type === 'data' || frame.type === 'reset') return this.onStreamFrame(frame);
    if (frame.type === 'ping') {
      // Answered immediately with the same nonce, without application involvement (WSM-CON-010). The
      // `pong` is a muxws frame sent through the writer, not a WebSocket control frame, which
      // browsers do not expose to JavaScript (WSM-CON-011).
      this.enqueue({ type: 'pong', nonce: frame.nonce ?? null });
      return true;
    }
    if (frame.type === 'pong') {
      this.onPong(frame);
      return true;
    }
    if (frame.type === 'goaway') return this.onGoaway(frame);
    // WSM-FRM-002: an unknown type is ignored and logged at info level; it is not an error.
    logger.info(`muxws conn=${this.id} ignoring unknown frame type '${frame.type}' (WSM-FRM-002)`);
    return true;
  }

  // ------------------------------------------------------------------ liveness

  /**
   * Round-trip time in **milliseconds** (WSM-CON-012; Python returns seconds).
   *
   * Sends a muxws `ping` frame, not a WebSocket control frame, because browsers do not expose control
   * frames to JavaScript (WSM-CON-011). Rejects with `ConnectionClosed` when no `pong` arrives within
   * `timeoutMs`; the connection itself is not closed.
   */
  async ping(timeoutMs: number = DEFAULT_PING_TIMEOUT_MS): Promise<number> {
    // `hasASocket`, not `isOpen`: during the hello window `isOpen` is false while the socket is open
    // (WSM-RCN-043). This guard rejects a ping between sockets.
    if (!this.hasASocket) throw new ConnectionLost('cannot ping a peer that is between sockets');

    const nonce = newNonce();
    const waiting = this.pings.open(nonce);
    this.pingStarted.set(nonce, monotonicNowMs());
    this.enqueue({ type: 'ping', nonce });

    let timer: ReturnType<typeof setTimeout> | undefined;
    const deadline = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => {
        reject(DEADLINE_EXPIRED);
      }, timeoutMs);
    });
    try {
      return await Promise.race([waiting, deadline]);
    } catch (error) {
      if (error !== DEADLINE_EXPIRED) throw error;
      // A missing pong fails this call only; the connection is left open. The heartbeat decides
      // whether the socket is dead (WSM-RCN-011).
      this.pings.giveUp(nonce);
      this.pingStarted.delete(nonce);
      throw new ConnectionClosed(`no pong within ${timeoutMs}ms`, { code: 1006 });
    } finally {
      clearTimeout(timer);
    }
  }

  /** Resolve the ping with this nonce; a `pong` with an unknown nonce is ignored. */
  private onPong(frame: Frame): void {
    const nonce = frame.nonce;
    if (nonce === null || nonce === undefined) return;
    const elapsed = monotonicNowMs() - (this.pingStarted.get(nonce) ?? 0);
    if (!this.pings.settle(nonce, elapsed)) {
      logger.debug(`muxws conn=${this.id} pong for an unknown nonce, ignored`);
    }
    this.pingStarted.delete(nonce);
  }

  // ------------------------------------------------------------------ shutdown

  /**
   * Send `goaway`, drain for up to `drainMs`, then close the socket (WSM-CON-025).
   *
   * The drain waits for live streams to end and for the write loop to send every queued frame,
   * including every remaining fragment of a fragmented payload; one deadline bounds both. An error
   * from the socket adapter's `close()` is logged, and the peer is marked closed.
   */
  async close(options: CloseOptions = {}): Promise<void> {
    const { code = ResetCode.NO_ERROR, reason = null, drainMs = DEFAULT_DRAIN_MS } = options;
    // Before the `open_` check: after `close()` the helper never dials again (WSM-RCN-040/044),
    // including when the peer is between sockets and the helper is waiting for the next dial.
    // `willRetry` must be false before `die()` builds the `CloseReason` below.
    this.willRetry = false;
    await this.connectionLoop?.stop();
    if (!this.open_) return;
    this.sendGoaway(code, reason);
    await this.drain(drainMs);
    try {
      await this.socket.close(1000, reason ?? '');
    } catch (error) {
      // A socket whose close fails is already closed; the peer is still marked dead below.
      logger.warn(`muxws conn=${this.id} closing the socket failed`, error);
    }
    this.die(new ConnectionClosed(reason ?? 'closed', { code: 1000, reason: reason ?? '', wasClean: true }));
  }

  /**
   * Send `goaway` once. `last_stream` is the highest stream id the remote opened and this peer
   * dispatched, so it has the remote's parity.
   */
  private sendGoaway(code: ResetCode, reason: string | null): void {
    if (this.goaway.sent) return;
    this.goaway.sent = true;
    this.goaway.sentCode = code;
    this.enqueue({ type: 'goaway', code, reason, last_stream: this.highestRemoteOpen });
  }

  /** Handle a received `goaway`: fail local streams above `last_stream`; the rest continue. */
  private onGoaway(frame: Frame): boolean {
    this.goaway.received = true;
    this.goaway.receivedCode = frame.code ?? null;
    this.goaway.receivedReason = frame.reason ?? null;
    this.goaway.remoteLastStream = frame.last_stream ?? null;

    // Streams above `last_stream` were not processed and may be retried elsewhere (WSM-CON-023).
    // They fail locally with `StreamRefused` through `fail()`; no `reset` is sent for them.
    [...this.liveStreams.values()].forEach((stream) => {
      if (!stream.local || this.goaway.survivesDrain(stream.id)) return;
      stream.fail(
        exceptionForReset(ResetCode.REFUSED, `the remote went away before processing stream ${stream.id}`, {
          streamId: stream.id,
        }),
      );
    });
    return true;
  }

  /**
   * Wait until every live stream has closed and every queued frame is sent, or `timeoutMs` has
   * elapsed, whichever is first (WSM-CON-024).
   *
   * Without the deadline a remote that keeps a stream open, or a socket that does not accept the
   * queued frames, would prevent the close. Streams still live at the deadline fail with
   * `ConnectionLost` when `die()` runs; nothing is sent for them.
   */
  private async drain(timeoutMs: number): Promise<void> {
    const deadline = monotonicNowMs() + timeoutMs;
    if (this.liveStreams.size > 0) {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const expired = new Promise<void>((resolve) => {
        timer = setTimeout(resolve, timeoutMs);
      });
      try {
        await Promise.race([this.allStreamsClosed(), expired]);
      } finally {
        clearTimeout(timer);
      }
    }
    await this.drainOutbound(deadline);
  }

  /**
   * Resolves when no stream is live. The live set is re-read after each round, because streams can
   * be added while waiting; `Stream.closed` resolves on every close path and never rejects
   * (WSM-API-023).
   */
  private async allStreamsClosed(): Promise<void> {
    while (this.liveStreams.size > 0) {
      await Promise.all([...this.liveStreams.values()].map((stream) => stream.closed));
    }
  }

  // ------------------------------------------------------------------ inbound opens

  private async onOpenFrame(frame: Frame): Promise<boolean> {
    const streamId = frame.stream;
    if (streamId === null || streamId === undefined) {
      await this.failConnection('open frame with no stream id');
      return false;
    }

    // Uses the explicit `opening` flag, not whether an assembler is active on that id; that inference
    // would accept a wrong-parity `open` as the continuation of a running reassembly and run a
    // handler for a stream this peer opened (WSM-STM-031).
    const existing = this.liveStreams.get(streamId);
    if (existing !== undefined && existing.opening && hasFragment(frame)) {
      return this.continueOpen(existing, frame);
    }

    if (!(await this.admitRemoteOpen(streamId))) return false;

    if (this.remoteStreamCount() >= this.maxConcurrentStreams) {
      // Refused without invoking the handler, so the opener may retry elsewhere (WSM-STM-036). There
      // is no STREAM_LIMIT code and no announced quota; the sender learns of the limit only from this
      // reset.
      this.enqueue({
        type: 'reset',
        stream: streamId,
        code: ResetCode.REFUSED,
        reason: `this receiver already holds ${this.maxConcurrentStreams} of your streams`,
      });
      return true;
    }

    const stream = new Stream(this, streamId, { headers: frame.headers ?? null, local: false });
    stream.state = frame.end === true ? StreamState.HALF_CLOSED_REMOTE : StreamState.OPEN;
    this.liveStreams.set(streamId, stream);
    this.interceptIncoming(stream, stream.headers);

    if (hasFragment(frame)) {
      stream.opening = true;
      // The payload cap applies to a fragmented `open` as to fragmented `data` (WSM-FRG-032).
      if (!this.withinPayloadCap(stream, frame)) return true;
      const payload = this.reassemble(stream, frame);
      if (isAbsent(payload)) return true;
      stream.opening = false;
      stream.payload = payload;
    } else {
      const payload = payloadOf(frame);
      stream.payload = isAbsent(payload) ? null : payload;
    }

    this.startHandler(stream);
    return true;
  }

  /**
   * Validate the id of an `open` that does not continue a live opening (WSM-SID-005), and record it as
   * the remote's highest open. Returns false after failing the connection.
   */
  private async admitRemoteOpen(streamId: number): Promise<boolean> {
    const remoteParity = this.dialer ? 0 : 1;
    if (streamId % 2 !== remoteParity) {
      await this.failConnection(`open on stream ${streamId}: wrong parity for the remote (WSM-SID-005)`);
      return false;
    }
    if (streamId <= this.highestRemoteOpen) {
      await this.failConnection(
        `open on stream ${streamId} is not greater than the remote's highest previous open ` +
          `${this.highestRemoteOpen} (WSM-SID-005)`,
      );
      return false;
    }
    this.highestRemoteOpen = streamId;
    return true;
  }

  /** Handle a later fragment of an opening payload; it is not a second open (WSM-STM-031). */
  private continueOpen(stream: Stream, frame: Frame): boolean {
    if (!this.withinPayloadCap(stream, frame)) return true;
    const payload = this.reassemble(stream, frame);
    if (isAbsent(payload)) return true;
    stream.opening = false;
    stream.payload = payload;
    if (frame.end === true) stream.state = StreamState.HALF_CLOSED_REMOTE;
    this.startHandler(stream);
    return true;
  }

  /** Run the handler for a new remote stream, or refuse the stream when there is none (WSM-STM-033). */
  private startHandler(stream: Stream): void {
    if (this.goaway.sent) {
      // After sending `goaway` no new handler runs. REFUSED means the stream was not processed, so the
      // opener may retry elsewhere (WSM-CON-021).
      this.enqueue({
        type: 'reset',
        stream: stream.id,
        code: ResetCode.REFUSED,
        reason: 'this peer is going away',
      });
      stream.fail(exceptionForReset(ResetCode.REFUSED, 'peer is going away', { streamId: stream.id }));
      return;
    }
    if (this.handler === null) {
      // The wire `reason` uses the Python wording, so both implementations send the same bytes for
      // this event; the local error message uses the TypeScript name.
      this.enqueue({ type: 'reset', stream: stream.id, code: ResetCode.REFUSED, reason: 'no on_stream handler' });
      stream.fail(exceptionForReset(ResetCode.REFUSED, 'no handler', { streamId: stream.id }));
      return;
    }
    // Stored on the stream so an incoming reset(CANCELLED) aborts it (WSM-ERR-013). The controller
    // replaces Python's task handle: the stream aborts it, and the handler observes `stream.signal`
    // where Python's handler receives CancelledError. It is assigned before the handler is
    // scheduled, so a reset that arrives in between still aborts it.
    const controller = new AbortController();
    stream.handlerTask = controller;
    const handler = this.handler;
    // Started in a microtask, as `asyncio.create_task` starts the coroutine after `_on_open` returns.
    // Calling `runHandler` directly would run the handler's synchronous part inside the read loop.
    void Promise.resolve().then(async () => {
      try {
        await this.runHandler(handler, stream, controller);
      } catch (error) {
        logger.error(`muxws conn=${this.id} stream=${stream.id} handler teardown failed`, error);
        this.reportStreamError(error, stream);
      }
    });
  }

  private async runHandler(handler: StreamHandler, stream: Stream, controller: AbortController): Promise<void> {
    if (controller.signal.aborted) return;
    try {
      await handler(stream.payload, stream);
    } catch (error) {
      // A handler that throws after its stream was aborted is not reported as an application error
      // and no reset is sent for it (WSM-ERR-014).
      if (controller.signal.aborted) return;
      this.resetForHandlerError(stream, error);
      return;
    }
    // WSM-STM-035: a handler that returns without ending its stream ends it implicitly.
    if (stream.state === StreamState.OPEN || stream.state === StreamState.HALF_CLOSED_REMOTE) {
      try {
        await stream.end();
      } catch (error) {
        if (error instanceof StreamReset) return;
        if (error instanceof ConnectionLost) throw error;
        // An error from a reply interceptor during the implicit end is treated as a handler error
        // (WSM-API-028).
        this.resetForHandlerError(stream, error);
      }
    }
  }

  /**
   * WSM-STM-034/WSM-INV-008: always APPLICATION_ERROR, never REFUSED.
   *
   * REFUSED means the operation was not performed and may be retried. A handler that throws may have
   * performed side effects, so a retry is not safe.
   */
  private resetForHandlerError(stream: Stream, error: unknown): void {
    let payload: unknown = null;
    try {
      payload = this.errorSerializer(error);
    } catch (serializerError) {
      // WSM-STM-034 is unconditional: if the serializer throws, the reset is sent without a payload,
      // so the opener still receives the failure.
      logger.error(`muxws conn=${this.id} errorSerializer threw; sending the reset without a payload`, serializerError);
      payload = null;
    }
    const reason = errorMessage(error);
    if (stream.state !== StreamState.CLOSED) {
      this.enqueue({
        type: 'reset',
        stream: stream.id,
        code: ResetCode.APPLICATION_ERROR,
        reason,
        payload: payload === null ? ABSENT : payload,
      });
    }
    stream.fail(new RemoteError(reason, { streamId: stream.id, payload }));
  }

  // ------------------------------------------------------------------ inbound stream frames

  private async onStreamFrame(frame: Frame): Promise<boolean> {
    const streamId = frame.stream;
    if (streamId === null || streamId === undefined) {
      await this.failConnection(`${frame.type} frame with no stream id`);
      return false;
    }

    const stream = this.liveStreams.get(streamId);
    if (stream === undefined) {
      const highWater = this.isOurParity(streamId) ? this.highestLocalOpen : this.highestRemoteOpen;
      if (streamId > highWater) {
        // Above the high-water mark: the stream was never opened, so this is a connection error (WSM-STM-003).
        await this.failConnection(
          `${frame.type} on stream ${streamId}, above the high-water mark ${highWater} (WSM-STM-003)`,
        );
        return false;
      }
      // At or below the mark: a late frame for a closed stream, ignored (WSM-STM-002).
      this.ignoredLateFrames += 1;
      return true;
    }

    if (frame.type === 'reset') {
      const payload = payloadOf(frame);
      stream.fail(
        this.resetException(frame.code ?? 0, frame.reason ?? null, streamId, isAbsent(payload) ? null : payload),
      );
      return true;
    }
    return this.onData(stream, frame);
  }

  /**
   * Build the exception for an inbound reset.
   *
   * A code this generation does not define fails only its stream, with a `StreamReset` carrying the
   * numeric code; the connection continues. Only APPLICATION_ERROR carries a payload, so only
   * `RemoteError` is constructed here directly.
   */
  private resetException(code: number, reason: string | null, streamId: number, payload: unknown): StreamReset {
    if (code === ResetCode.APPLICATION_ERROR) return new RemoteError(reason, { streamId, payload });
    return exceptionForReset(code as ResetCode, reason, { streamId });
  }

  /**
   * The number of live streams the remote opened (WSM-STM-037).
   *
   * The limit bounds the work the remote can impose. Streams this peer opened are not counted
   * (WSM-INV-007).
   */
  private remoteStreamCount(): number {
    let count = 0;
    this.liveStreams.forEach((stream) => {
      if (!stream.local) count += 1;
    });
    return count;
  }

  private isOurParity(streamId: number): boolean {
    return streamId % 2 === (this.dialer ? 1 : 0);
  }

  private onData(stream: Stream, frame: Frame): boolean {
    if (stream.state === StreamState.HALF_CLOSED_REMOTE || stream.state === StreamState.CLOSED) {
      // `data` after the remote's end is a stream error; the connection continues.
      this.resetStream(stream, ResetCode.PROTOCOL_ERROR, 'data after end (WSM-STM-020)');
      return true;
    }

    // WSM-FRM-016, checked before delivery: headers are allowed only on the remote's first frame on
    // the stream. A violation resets the stream; the connection continues.
    const answer = stream.local && !stream.receivedAFrame;
    if (!stream.noteRemoteFrame(frame.headers)) {
      this.resetStream(stream, ResetCode.PROTOCOL_ERROR, 'headers after the first frame (WSM-FRM-016)');
      return true;
    }
    if (answer) this.interceptIncoming(stream, stream.replyHeaders);

    if (stream.assembler.inProgress && !hasFragment(frame)) {
      this.resetStream(stream, ResetCode.PROTOCOL_ERROR, 'non-fragment frame mid-reassembly (WSM-FRG-033)');
      return true;
    }

    if (hasFragment(frame)) {
      if (!this.withinPayloadCap(stream, frame)) return true;
      const payload = this.reassemble(stream, frame);
      if (isAbsent(payload)) return true;
      stream.acceptPayload(payload);
    } else {
      const payload = payloadOf(frame);
      if (!isAbsent(payload)) stream.acceptPayload(payload);
    }

    if (frame.end === true) stream.remoteEnd(frame.trailers ?? null);
    return true;
  }

  /**
   * Pass the remote's leading headers to every incoming interceptor (WSM-API-029).
   *
   * Runs in the read loop. An interceptor that throws is logged; the frame, the stream and the
   * connection are processed as if it were not registered.
   */
  private interceptIncoming(stream: Stream, headers: Record<string, unknown>): void {
    if (this.incomingInterceptors.size === 0) return;
    const view = Object.freeze({ ...headers });
    [...this.incomingInterceptors.values()].forEach((interceptor) => {
      let result: unknown;
      try {
        result = interceptor(view, stream);
      } catch (error) {
        logger.error(`muxws conn=${this.id} stream=${stream.id} an incoming headers interceptor threw`, error);
        return;
      }
      if (isThenable(result)) {
        result.then(undefined, () => undefined);
        logger.error(
          `muxws conn=${this.id} stream=${stream.id} an incoming headers interceptor returned a thenable; ` +
            'it is not awaited (WSM-API-029)',
        );
      }
    });
  }

  /**
   * WSM-FRG-032/WSM-INV-017: reject on the fragment that crosses the limit, not after reassembly.
   *
   * The limit bounds memory, so the payload is not assembled to be measured. The partial buffer is
   * released here, before `resetStream` (`Stream.fail` also resets the assembler).
   */
  private withinPayloadCap(stream: Stream, frame: Frame): boolean {
    const incoming = hasFragment(frame) ? encodedLength(frame.fragment as string | ArrayBuffer) : 0;
    if (stream.assembler.byteLength + incoming <= this.maxPayloadBytes) return true;

    stream.assembler.reset();
    this.resetStream(
      stream,
      ResetCode.PAYLOAD_TOO_LARGE,
      `a payload crossed this receiver's ${this.maxPayloadBytes}-byte limit (WSM-FRG-032)`,
    );
    return false;
  }

  /**
   * Add one fragment to the stream's payload. Returns the payload, or `ABSENT` while more are expected.
   *
   * Fragments that mix text and binary values, or a joined payload the codec cannot decode, reset this stream with
   * PROTOCOL_ERROR and return `ABSENT`. Each message decoded on its own, so the peers still agree about every other
   * stream and the connection stays open (WSM-STM-020/024).
   */
  private reassemble(stream: Stream, frame: Frame): unknown {
    try {
      return stream.assembler.feed(frame, this.codec);
    } catch (error) {
      if (!(error instanceof ProtocolError)) throw error;
      this.resetStream(stream, ResetCode.PROTOCOL_ERROR, `undecodable reassembled payload: ${errorMessage(error)}`);
      return ABSENT;
    }
  }

  private resetStream(stream: Stream, code: ResetCode, reason: string): void {
    this.enqueue({ type: 'reset', stream: stream.id, code, reason });
    stream.fail(exceptionForReset(code, reason, { streamId: stream.id }));
  }

  // ------------------------------------------------------------------ ending

  /** Connection-level protocol error: send goaway(PROTOCOL_ERROR), close the socket, then fail every live stream. */
  private async failConnection(reason: string): Promise<void> {
    logger.warn(`muxws conn=${this.id} connection-level protocol error: ${reason}`);
    // Marked as sent before the drain, so no stream is opened while the queued frames are sent
    // (WSM-CON-021).
    this.goaway.sent = true;
    this.goaway.sentCode = ResetCode.PROTOCOL_ERROR;
    this.enqueue({
      type: 'goaway',
      code: ResetCode.PROTOCOL_ERROR,
      reason,
      last_stream: this.highestRemoteOpen,
    });
    await this.drainOutbound(monotonicNowMs() + FAILURE_DRAIN_MS);
    try {
      await this.socket.close(1002, closeFrameReason(reason));
    } catch (error) {
      // A socket whose close fails is already closed; the peer is still marked dead below.
      logger.warn(`muxws conn=${this.id} closing the socket failed`, error);
    }
    this.die(new ConnectionClosed(reason, { code: 1002, reason, wasClean: false }));
  }

  /** A failure inside this peer: goaway(INTERNAL_ERROR), then mark the peer dead and close the socket (1011). */
  private async failInternal(message: string, reason: string): Promise<void> {
    if (this.open_) {
      // Marked as sent before the drain, so no stream is opened while the queued frames are sent
      // (WSM-CON-021).
      this.goaway.sent = true;
      this.goaway.sentCode = ResetCode.INTERNAL_ERROR;
      this.enqueue({ type: 'goaway', code: ResetCode.INTERNAL_ERROR, reason, last_stream: this.highestRemoteOpen });
      await this.drainOutbound(monotonicNowMs() + FAILURE_DRAIN_MS);
    }
    await this.dieAndClose(new ConnectionClosed(message, { code: 1011, reason }));
  }

  /**
   * Mark the peer dead with `cause`, then close the socket with 1011.
   *
   * The peer is marked dead first, so `onClose` reports `cause` and not the closure the read loop
   * observes. The close reason is fixed and short: a WebSocket close reason is limited to 123 bytes.
   */
  private async dieAndClose(cause: ConnectionClosed): Promise<void> {
    this.die(cause);
    await this.closeSocketLocally(1011, 'internal error');
  }

  /**
   * Wait until the write loop has sent every queued frame, the write loop has ended, or `deadline`
   * (a `monotonicNowMs()` value) passes.
   *
   * Every remaining fragment of a fragmented payload counts as queued, including while the write
   * loop awaits the socket's send.
   */
  private async drainOutbound(deadline: number): Promise<void> {
    if (this.writerStopped) return;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const expired = new Promise<void>((resolve) => {
      timer = setTimeout(resolve, Math.max(0, deadline - monotonicNowMs()));
    });
    const waited = [this.writer.waitIdle(), expired];
    if (this.writerTask !== null) {
      waited.push(
        this.writerTask.then(
          () => undefined,
          () => undefined,
        ),
      );
    }
    try {
      await Promise.race(waited);
    } finally {
      clearTimeout(timer);
    }
  }

  private async stopWriter(): Promise<void> {
    const task = this.writerTask;
    if (task === null) return;
    this.writer.stop();

    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<void>((resolve) => {
      timer = setTimeout(resolve, 1000);
    });
    try {
      await Promise.race([task.catch(() => undefined), timeout]);
    } finally {
      clearTimeout(timer);
    }
    // JavaScript cannot cancel a task waiting in `await socket.sendText()`. The flag replaces
    // Python's `task.cancel()`: the loop returns instead of sending further frames.
    this.writerStopped = true;
  }

  /**
   * @internal Socket loss: every live stream fails with `ConnectionLost` before `onClose` handlers run.
   *
   * Also called by the reconnect helper (`ts/reconnect.ts`) when a missing pong or an incomplete
   * hello shows the socket is dead without a socket error, so every loss is reported through this one
   * path (WSM-RCN-011).
   *
   * `notify: false` suppresses `onClose`; it is used for a first connection that was never
   * established, where `connect()` rejects instead (WSM-RCN-006) and the caller has no peer to attach
   * handlers to. The peer is still marked dead, so `isOpen` is false immediately rather than after the
   * read loop observes the close.
   */
  die(cause: ConnectionClosed, options: { notify?: boolean } = {}): void {
    if (!this.open_) return;
    this.open_ = false;
    this.death = cause;

    // Reject outstanding pings; no pong can arrive.
    this.pings.failAll(cause);
    this.writer.discardAll();
    this.pingStarted.clear();

    const detail = cause.reason || errorMessage(cause);
    [...this.liveStreams.values()].forEach((stream) => {
      stream.fail(new ConnectionLost(`connection closed: ${detail}`, { streamId: stream.id }));
    });
    this.liveStreams.clear();

    if (options.notify ?? true) {
      this.notifyClose({
        code: cause.code,
        reason: cause.reason,
        wasClean: cause.wasClean,
        willRetry: this.willRetry,
      });
    }
  }

  /**
   * @internal Call the `onClose` handlers; nothing else.
   *
   * Separate from `die` so the reconnect helper can report a close without a socket loss:
   * `maxAttempts` exhausted, reported once with `willRetry: false`, after which the helper does not
   * dial again (WSM-RCN-044).
   */
  notifyClose(reason: CloseReason): void {
    // At most one `willRetry: false` close per connection (`adoptSocket` resets the flag). When the
    // attempt cap is reached through failed hellos, the last loss already reported `willRetry: false`,
    // and the helper's give-up is suppressed (WSM-RCN-040/044). The first report is kept.
    if (!reason.willRetry) {
      if (this.finalCloseReported) return;
      this.finalCloseReported = true;
    }
    this.fanOut('onClose', this.closeHandlers, reason);
  }

  /**
   * @internal Use a newly established socket; called for a `Peer` that is reused across a reconnect.
   *
   * The `Peer` is kept; `Stream` objects are not (WSM-RCN-032). The id space and the high-water marks
   * are reset, and no queued frame is carried over from the previous socket (WSM-RCN-031), because
   * the server has no state for those streams (WSM-INV-010).
   *
   * The connection id advances, so a log shows the reconnect as a new `conn=` (WSM-API-009).
   */
  adoptSocket(socket: SocketAdapter): void {
    this.id = `${PROCESS_PREFIX}-${connectionCounter}`;
    connectionCounter += 1;

    this.socket = socket;
    this.liveStreams.clear();
    this.nextId = this.dialer ? 1 : 2;
    this.highestLocalOpen = 0;
    this.highestRemoteOpen = 0;
    // WSM-RCN-044's "at most one" applies per connection, not per `Peer` object. Without this reset,
    // a `Peer` given a new socket after a `willRetry: false` close would not report its next loss.
    this.finalCloseReported = false;
    this.goaway = new GoawayState();
    this.writer = new Writer(this.codec, { maxFrameBytes: this.maxFrameBytes });
    // `writerTask` corresponds to Python's `_writer_task = None`; `writerStopped` has no Python
    // counterpart, because Python cancels the task. A `writerTask` still pointing at the previous
    // socket's loop, or `writerStopped` still true, would make `serve()` on the new socket send
    // nothing.
    this.writerTask = null;
    this.writerStopped = false;
    this.exhaustionShutdown = null;
    this.open_ = true;
    this.death = null;
  }

  /**
   * @internal Close the underlying socket; nothing else.
   *
   * When the heartbeat declares a socket dead, WSM-RCN-011 requires marking it dead and closing it
   * locally; `die()` does only the first. Without the close, the read loop stays in
   * `socket.receive()`, `serve()` does not settle, and the reconnect supervisor never redials.
   *
   * `code` defaults to 1000, not 1006: 1006 means "closed without a close frame" and MUST NOT be sent.
   * `ws` throws for it, which would leave the socket open. The peer reports 1006 as the close code
   * of the loss, but sends 1000.
   */
  async closeSocketLocally(code = 1000, reason = ''): Promise<void> {
    try {
      await this.socket.close(code, reason);
    } catch (error) {
      // A socket whose close fails is already closed.
      logger.debug(`muxws conn=${this.id} closing a locally-declared-dead socket failed`, error);
    }
  }

  /** @internal Called by `Stream` on close. No state is retained per closed stream (WSM-STM-001). */
  forget(stream: Stream): void {
    this.liveStreams.delete(stream.id);
  }

  /** Debug representation; mirrors Python's `__repr__`. */
  toString(): string {
    return `<Peer ${this.id} ${this.dialer ? 'dialer' : 'acceptor'} streams=${this.liveStreams.size}>`;
  }
}
