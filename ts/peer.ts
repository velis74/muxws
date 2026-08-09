/**
 * `Peer`: one end of one WebSocket, symmetric by construction (§5, §9, WSM-INV-002).
 *
 * A port of `muxws/peer.py`, which is the specification of behaviour. Everything that differs does so
 * because JavaScript differs - there is no task cancellation, no `asyncio.Queue` and no `logging`
 * module - and each of those places says so where it happens.
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

// The log seam moved to `ts/observability.ts` at M5a, where the frame line lives; it is re-exported
// here because every call site that had one imported it from this module.
export { logger };
export type { CloseReason };

// --------------------------------------------------------------------------- connection identity

/**
 * Three lowercase hex characters, drawn once per module load. A log correlation id is not
 * security-sensitive, so `Math.random` is right here and `crypto.getRandomValues` would be cargo cult.
 */
const PROCESS_PREFIX = Math.floor(Math.random() * 0x1000)
  .toString(16)
  .padStart(3, '0');

/**
 * Never rewound, and never consulted for reuse: two connections under one name read as one connection
 * in a log, which is the failure WSM-API-009 exists to prevent.
 */
let connectionCounter = 0;

// --------------------------------------------------------------------------- public types

/** `(payload, stream)`; the one incoming-stream handler per peer (WSM-STM-030/031). */
export type StreamHandler = (payload: any, stream: Stream) => void | Promise<void>;

/** Turns a handler's failure into a payload for the `reset(APPLICATION_ERROR)` frame (WSM-ERR-006). */
export type ErrorSerializer = (error: unknown) => unknown;

/** `open()`'s options. It carries no `timeoutMs` in any milestone (WSM-API-018). */
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
   * exercise fragmentation without megabyte fixtures, and a value too small to hold an envelope plus
   * one indivisible unit is rejected here rather than looping in the splitter (WSM-FRG-034).
   */
  maxFrameBytes?: number;
  /**
   * The largest reassembled payload this peer accepts, in bytes. **Local** (WSM-FRG-035): never
   * announced, and a sender learns of it only from the reset it provokes.
   */
  maxPayloadBytes?: number;
  /**
   * How many streams the **remote** may have open here at once. Also local, also unannounced, and
   * never checked by the sender (WSM-STM-036/037).
   */
  maxConcurrentStreams?: number;
}

/** WSM-FRG-035's default: 64 MiB of reassembled payload. */
export const DEFAULT_MAX_PAYLOAD_BYTES = 67_108_864;

/** WSM-STM-036's default: 100 streams the remote may hold open here at once. */
export const DEFAULT_MAX_CONCURRENT_STREAMS = 100;

/**
 * `close()`'s options (WSM-CON-025).
 *
 * Every duration in the TypeScript port is **milliseconds as an integer**, where Python's is seconds
 * as a float; `drainMs` is the mirror of Python's `drain=10.0` (WSM-CON-024).
 */
export interface CloseOptions {
  code?: ResetCode;
  reason?: string;
  drainMs?: number;
}

/** `peer.ping()`'s default deadline, in milliseconds - Python's `timeout=5.0` (WSM-CON-012). */
export const DEFAULT_PING_TIMEOUT_MS = 5000;

/** `peer.close()`'s default drain window, in milliseconds - Python's `drain=10.0` (WSM-CON-024). */
export const DEFAULT_DRAIN_MS = 10_000;

/** WSM-ERR-006's default. A public-facing deployment should replace it with a redacting one. */
export function defaultErrorSerializer(error: unknown): unknown {
  if (error instanceof Error) return { type: error.name, message: error.message };
  return { type: typeof error, message: String(error) };
}

// --------------------------------------------------------------------------- small helpers

/** Python's `str(exc)`. */
function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function isAbsent(value: unknown): value is Absent {
  return value === ABSENT;
}

/** `undefined` and `ABSENT` are the same thing on the read side (see `frames.fieldValue`). */
function payloadOf(frame: Frame): unknown {
  return frame.payload === undefined ? ABSENT : frame.payload;
}

function hasFragment(frame: Frame): boolean {
  return frame.fragment !== null && frame.fragment !== undefined;
}

const OPEN_OPTION_KEYS: ReadonlySet<string> = new Set(['payload', 'headers', 'end']);
const REQUEST_OPTION_KEYS: ReadonlySet<string> = new Set(['payload', 'headers', 'end', 'timeoutMs']);

/**
 * Is this lone argument the options object rather than a payload (WSM-API-020)?
 *
 * Only when every own key is one this call defines **and there is at least one**. `open({})` is
 * therefore a payload of `{}` rather than an empty options object, which is the conservative reading:
 * a caller who meant "no arguments" writes `open()`.
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

/** The `(payload?)` / `(payload?, options?)` / `(options)` overload triple, resolved once. */
function resolveCall(first: unknown, second: RequestOptions | undefined, keys: ReadonlySet<string>): ResolvedCall {
  const options: RequestOptions =
    second === undefined && isOptionsObject(first, keys) ? (first as RequestOptions) : (second ?? {});
  const positional = options === first ? undefined : first;
  return {
    // `open()` with no payload puts `"payload": null` on the wire, exactly as Python's default does.
    payload: positional ?? options.payload ?? null,
    headers: options.headers ?? null,
    end: options.end ?? false,
    timeoutMs: options.timeoutMs,
  };
}

/**
 * WSM-FRG-034: a cap too small to hold an envelope plus one unit is a configuration error.
 *
 * Discovered here, at construction, rather than later as an infinite split loop. The cap is a
 * protocol constant (WSM-FRG-004); the only way a smaller number reaches a peer is the test-only
 * construction argument of WSM-FRG-005, which the conformance runner uses.
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

/** One turn of the event loop, the closest equivalent of `await asyncio.sleep(0)`. */
function yieldToEventLoop(): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, 0);
  });
}

/**
 * A monotonic clock in milliseconds - the mirror of Python's `asyncio.get_running_loop().time()`.
 *
 * `performance.now()` rather than `Date.now()`: it is monotonic (a clock adjustment mid-flight cannot
 * produce a negative round-trip time) and sub-millisecond, so an in-memory ping does not measure zero.
 */
function monotonicNowMs(): number {
  return globalThis.performance.now();
}

/** The sentinel a deadline rejects with; never an `Error`, so it cannot be confused with one. */
const DEADLINE_EXPIRED: unique symbol = Symbol('DEADLINE_EXPIRED');

/**
 * A minimal unbounded async queue: `put` never blocks, `get` waits.
 *
 * TypeScript has no `asyncio.Queue`, and the outbound queue is the one place the peer needs one.
 */

// --------------------------------------------------------------------------- the peer

/**
 * What `Peer` needs from the reconnect helper, and nothing more.
 *
 * Structural rather than an import of `ConnectionLoop`: `ts/reconnect.ts` imports `Peer`, so naming
 * the class here would close a cycle for one method call.
 */
export interface ConnectionSupervisor {
  stop(): Promise<void>;
}

/** One symmetric peer type per language: server push is a client request with the roles swapped. */
export class Peer {
  /**
   * Not `readonly`: `Peer` survives a reconnect while its connection does not, so `adoptSocket` takes
   * the **next** counter value (WSM-API-009). A log then shows the reconnect as a new `conn=` rather
   * than as one continuous connection, and no id from a dropped socket is handed out again.
   */
  id: string;

  /** An ordinary object with ordinary object semantics. muxws never reads it (WSM-REG-001/002). */
  readonly tags: Record<string, unknown> = {};

  /** Not `readonly`: `adoptSocket` replaces it when the helper re-establishes the connection. */
  private socket: SocketAdapter;

  private readonly codec: Codec;

  private readonly dialer: boolean;

  private readonly errorSerializer: ErrorSerializer;

  /** The cap the send path fragments at, and the receive path measures whole messages against. */
  private readonly maxFrameBytes: number;

  /** WSM-FRG-035. Local, unannounced, and enforced as fragments accumulate (WSM-FRG-032). */
  private readonly maxPayloadBytes: number;

  /** WSM-STM-036. Local, unannounced, and counted over the remote's opens alone (WSM-STM-037). */
  private readonly maxConcurrentStreams: number;

  /** The dialer allocates odd ids, the acceptor even ones (WSM-SID-002). */
  /**
   * @internal The next id this peer will allocate. Public so a test can drive the allocator to the
   * end of the id space without waiting for two billion opens; nothing in the library writes it.
   */
  nextId: number;

  private readonly liveStreams = new Map<number, Stream>();

  /** Exactly two integers plus the live-stream map; nothing per closed stream (WSM-STM-001). */
  private highestLocalOpen = 0;

  private highestRemoteOpen = 0;

  private ignoredLateFrames = 0;

  private handler: StreamHandler | null = null;

  private readonly closeHandlers: ((reason: CloseReason) => void)[] = [];

  private readonly reconnectHandlers: ((attempt: number, peer: Peer) => void)[] = [];

  private readonly frameHandlers: ((direction: 'tx' | 'rx', frame: Frame, byteLength: number) => void)[] = [];

  private readonly errorHandlers: ((error: unknown, stream: Stream | null) => void)[] = [];

  /** Outstanding pings, keyed by nonce and never by order (see `ts/lifecycle.ts`). */
  private readonly pings = new PingRegistry();

  /** When each outstanding ping went out, so a `pong` can be turned into an elapsed time. */
  private readonly pingStarted = new Map<string, number>();

  /** What each side has said about stopping; the two directions mean different things. */
  private goaway = new GoawayState();

  /**
   * The send path. Round-robin across streams, never a FIFO of frames (WSM-FRG-019) - without this
   * a 1 MB payload adds a full second of latency to a 200-byte update on another stream.
   */
  private writer: Writer;

  private writerTask: Promise<void> | null = null;

  /**
   * Frames handed to the writer and not yet dealt with.
   *
   * `outbound.size` alone cannot answer "is anything still in flight": a `put` that lands on a
   * waiting `get` hands the frame straight over, so the queue reads empty for the microtask before
   * the writer resumes. `drainOutbound` would then return before `goaway` reached the wire.
   */
  private pendingFrames = 0;

  private writerStopped = false;

  private open_ = true;

  /**
   * @internal False from the moment the helper adopts a socket until the hello is acknowledged.
   *
   * Default **true**, so an acceptor built by `accept()` is unaffected: it has no hello and no
   * helper, and for it "established" is exactly what socket-open already means (WSM-CON-030). Only
   * the reconnect driver ever writes it, and only across the window WSM-RCN-043 puts `isOpen` false
   * for.
   */
  established = true;

  /**
   * Whether a `willRetry: false` close has already been reported (WSM-RCN-044).
   *
   * There is at most one per peer, ever: the attempt cap can be spent by hello failures rather than
   * by refused dials, and then the last loss already carried `willRetry: false` before the helper's
   * own give-up reached `notifyClose` - two endings reported for one peer, the second of which
   * claims a socket loss that never happened.
   */
  private finalCloseReported = false;

  private death: ConnectionClosed | null = null;

  /**
   * @internal The monotonic clock both halves of the heartbeat read (WSM-RCN-010).
   *
   * One clock, not two: `Heartbeat` reads this same function, so an injected one moves the stamp and
   * the idle measurement together. A test that advanced only one of them would be measuring itself.
   */
  clock: () => number = monotonicNowMs;

  private lastActivityMs = 0;

  /**
   * @internal True while the reconnect helper intends to dial again.
   *
   * `CloseReason.willRetry` reads it at socket death, so it is set when a connection is
   * **established** and not when one is lost - by then it is too late to be right (WSM-RCN-040).
   */
  willRetry = false;

  /** @internal Held so `close()` can tell the helper to stop dialling (WSM-RCN-040/044). */
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
   * Socket-open **and** established (WSM-RCN-043).
   *
   * Both halves, because a hello makes them two different moments (WSM-RCN-004): a socket that is
   * open but whose hello has not been acknowledged is not yet a connection an application may send
   * on, and an `open()` accepted in that window would put an application frame ahead of the hello
   * (WSM-RCN-023). Reporting `true` there also hands the application a peer that looks alive one
   * failed hello before the helper backs off and dials again.
   */
  get isOpen(): boolean {
    return this.open_ && this.established;
  }

  /**
   * @internal Whether a frame put on the writer now can still reach a wire.
   *
   * Not the same question as `isOpen`, and the difference is why this exists. `isOpen` answers "may
   * the application start something here", which the hello window makes false (WSM-RCN-043). This
   * answers "is there a socket underneath", which that window does not make false - the hello itself
   * travels on it. `Stream.reset()` needs the second question: a stream the acceptor pushed during
   * the hello window that is reset by its handler must put the `reset` on the wire, or the remote is
   * left holding a stream this side has already closed (WSM-STM-021).
   */
  get hasASocket(): boolean {
    return this.open_;
  }

  /**
   * When a frame last crossed this socket **in either direction**, on `clock`'s scale.
   *
   * Idle means idle (WSM-RCN-010): a busy connection must not spend a ping every interval, so the
   * heartbeat's timer is this stamp rather than a fixed schedule. Both directions count - a socket
   * carrying inbound frames is demonstrably alive, and pinging it proves nothing new.
   */
  get lastActivity(): number {
    return this.lastActivityMs;
  }

  /**
   * @internal Mark the socket as having been busy just now.
   *
   * Every frame does this through `reportFrame`; `Heartbeat` also does it once when it starts,
   * because a socket that has just been established is the most recent thing that happened on it and
   * an unused peer would otherwise read as infinitely idle and be pinged immediately.
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

  /** Register the one incoming-stream handler. A second replaces the first and logs (WSM-STM-030). */
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
   * Fires once per **re-established** connection (WSM-RCN-030).
   *
   * It guarantees exactly two things and nothing more: a live socket, and an identity the acceptor
   * has already accepted on it. No stream survives a reconnect, nothing is replayed, and the new
   * socket's id space starts empty (WSM-RCN-031/032).
   */
  onReconnect(handler: (attempt: number, peer: Peer) => void): void {
    this.reconnectHandlers.push(handler);
  }

  /** @internal ConnectionLoop -> Peer. Fired after the hello acknowledgement, never before. */
  fireReconnect(attempt: number): void {
    this.fanOut('onReconnect', this.reconnectHandlers, attempt, this);
  }

  /**
   * Run every handler, and let none of them out.
   *
   * The caller of `fireReconnect` and `notifyClose` is the reconnect supervisor, so an application
   * callback that throws would otherwise unwind into it and stop it for good: a library whose
   * reconnect loop can be killed by an application's logging call is not a reconnect loop. The same
   * applies within one fan-out - the second handler is not the first one's business.
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
   * Where a failure nobody is waiting for goes (WSM-API-016).
   *
   * A `Stream`'s internal promise carries a no-op rejection handler from its constructor, so a reset
   * on a stream nobody awaited cannot become an unhandled rejection; this is where that failure is
   * reported instead, so it is silenced without being lost.
   */
  onError(handler: (error: unknown, stream: Stream | null) => void): void {
    this.errorHandlers.push(handler);
  }

  /**
   * @internal Stream -> Peer. Report a failure that has no consumer to hand it to (WSM-API-016).
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
   * Open a stream. **Synchronous**, and it never queues (WSM-API-001/004).
   *
   * The guard, then the body. Both halves run in one synchronous turn, so allocation and enqueue
   * remain the indivisible step WSM-SID-006 asks for - see `allocateAndEnqueue`, which is where that
   * is stated rather than left to luck.
   */
  open<T = unknown>(payload?: unknown, options?: OpenOptions): Stream<T>;
  open<T = unknown>(options: OpenOptions): Stream<T>;
  open<T = unknown>(first?: unknown, second?: OpenOptions): Stream<T> {
    this.throwIfUnopenable();
    return this.allocateAndEnqueue<T>(first, second);
  }

  /**
   * @internal `open()`'s body, without the between-sockets guard. The hello's one route in.
   *
   * `isOpen` is false for the whole window between adopting a socket and the hello acknowledgement
   * (WSM-RCN-043), so `open()` itself refuses there - which is the point, since an application frame
   * accepted in that window would precede the hello (WSM-RCN-023). The hello is the one frame that
   * has to go out inside it, and it is the driver's rather than the application's, so it calls this
   * instead. A parameter on the public `open()` would have offered the same bypass to everyone.
   *
   * **Allocation and enqueue stay one indivisible step** (WSM-SID-006): this is the same body
   * `open()` runs and there is no `await` anywhere in it. Inserting one between the allocation and
   * the `enqueue` below would let two concurrent calls interleave and put a non-monotonic id
   * sequence on the wire (WSM-INV-005).
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
   * WSM-SID-007: running out of ids is an orderly shutdown, not an error.
   *
   * The rule asks for four things - send `goaway`, stop opening, let in-flight streams drain, then
   * close - and only the second is something `open()` can do by returning. The other three are
   * started rather than awaited, because `open()` returns a `Stream` without suspending
   * (WSM-API-001): a call that blocked here to drain would be a different method. The stream just
   * allocated is in flight and gets its drain window like any other.
   */
  private beginExhaustionShutdown(): void {
    if (this.goaway.sent || this.exhaustionShutdown !== null) return;
    this.exhaustionShutdown = this.close({
      code: ResetCode.NO_ERROR,
      reason: `stream ids exhausted at ${MAX_STREAM_ID} (WSM-SID-007)`,
    });
    void this.exhaustionShutdown.catch(() => undefined);
  }

  /** WSM-API-004: exactly two synchronous throws, and never one for concurrency. */
  private throwIfUnopenable(): void {
    // `isOpen` and not `open_`: the hello window is a socket that is open and not yet a connection,
    // and nothing an application sends may precede the hello on it (WSM-RCN-023/043).
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

  /** Held so the shutdown WSM-SID-007 requires can be awaited by a test rather than raced. */
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
   * Unlike `await stream`, this polices a second payload: a unary call that quietly discarded extra
   * values would hide a handler bug rather than report it (WSM-API-007).
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
      // JavaScript cannot cancel the collector, so it is left running and its later rejection is
      // absorbed here. Python shields it for the same reason Python cancels it late: the reset must
      // go out carrying TIMEOUT, telling the remote a deadline expired rather than that the caller
      // changed its mind, which is what a CANCELLED from a torn-down consumer would say
      // (WSM-ERR-011).
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

  /** @internal Stream -> Peer. Synchronous by contract: `open()` must not suspend mid-allocation. */
  enqueue(frame: Frame): void {
    this.pendingFrames += 1;
    this.writer.enqueue(frame);
  }

  private async runWriter(): Promise<void> {
    for (;;) {
      let frame: Frame | null;
      try {
        frame = await this.writer.nextFrame();
      } catch (error) {
        if (error instanceof LaneEncodingError) {
          // The encode now happens inside the writer, so a codec that refuses a payload would
          // otherwise take the write loop down with it - and a peer whose writer is dead while it
          // still reports itself open is the worst possible state. The lane is carried so exactly
          // one stream fails and the connection keeps working.
          this.pendingFrames = Math.max(0, this.pendingFrames - 1);
          this.failLane(error);
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
        // Only now: fragment n+1 is sliced once fragment n has reached the socket, never before
        // (WSM-FRG-018).
        this.writer.advance(frame.stream ?? CONNECTION_LANE);
      } catch (error) {
        // The socket is gone; `serve()`'s read loop is the one that declares the peer dead.
        if (error instanceof ConnectionClosed) return;
        logger.error(`muxws conn=${this.id} could not send a ${frame.type} frame`, error);
        this.failUnsendable(frame, error);
      } finally {
        this.pendingFrames -= 1;
      }
    }
  }

  /** One lane's frame could not be encoded. Fail its stream; keep the connection working. */
  private failLane(failure: LaneEncodingError): void {
    const detail = errorMessage(failure.cause);
    const stream = this.liveStreams.get(failure.lane);
    if (stream === undefined) {
      this.die(new ConnectionClosed(String(failure), { code: 1011 }));
      return;
    }
    logger.error(`muxws conn=${this.id} could not encode a frame on stream ${failure.lane}`, failure.cause);
    this.enqueue({ type: 'reset', stream: stream.id, code: ResetCode.INTERNAL_ERROR, reason: detail });
    stream.fail(exceptionForReset(ResetCode.INTERNAL_ERROR, detail, { streamId: stream.id }));
  }

  /** One frame could not be encoded. Fail its stream and keep the connection working. */
  private failUnsendable(frame: Frame, error: unknown): void {
    const detail = errorMessage(error);
    const stream = frame.stream === null || frame.stream === undefined ? undefined : this.liveStreams.get(frame.stream);
    if (stream === undefined) {
      this.die(new ConnectionClosed(`could not encode a ${frame.type} frame: ${detail}`, { code: 1011 }));
      return;
    }
    this.enqueue({ type: 'reset', stream: stream.id, code: ResetCode.INTERNAL_ERROR, reason: detail });
    stream.fail(exceptionForReset(ResetCode.INTERNAL_ERROR, detail, { streamId: stream.id }));
  }

  /**
   * WSM-OBS-003: the hook sees the logical frame and the encoded length, before encode on the way
   * out and after decode on the way in - so on tx the encoding is computed first and the hook called
   * with the result, never the other way round.
   */
  private reportFrame(direction: FrameDirection, frame: Frame, encoded: string | ArrayBuffer): void {
    // Every frame, both directions, before the hooks: a handler that throws must not be able to
    // leave the heartbeat believing an active socket has gone quiet (WSM-RCN-010).
    this.stampActivity();
    const length = encodedLength(encoded);
    // Isolated like every other application callback: an `onFrame` handler runs on the read loop and
    // on the write loop, so one that throws would take the whole connection down for a log line.
    this.fanOut('onFrame', this.frameHandlers, direction, frame, length);
    // Never the payload's contents: application data routinely holds secrets (WSM-OBS-002).
    logFrame(this.id, direction, frame, length);
  }

  // ------------------------------------------------------------------ the read loop

  /** Run the read loop until the socket closes. */
  async serve(): Promise<void> {
    if (this.writerTask === null) this.writerTask = this.runWriter();
    try {
      await this.readLoop();
    } catch (error) {
      // Nothing below is expected to escape, but a peer whose read loop died while still reporting
      // `isOpen` is the worst possible state, so the guard is unconditional: `serve()` still rejects
      // with whatever went wrong, and the peer is properly dead by the time it does.
      const detail = errorMessage(error);
      this.die(new ConnectionClosed(`read loop failed: ${detail}`, { code: 1011, reason: detail }));
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
      // Measured before dispatch, so an over-cap message is never assembled, decoded further, or
      // handed to a handler (WSM-FRG-031).
      if (!(await this.withinFrameCap(frame, message))) continue;

      let keepGoing: boolean;
      try {
        keepGoing = await this.dispatch(frame);
      } catch (error) {
        // Every pending await would hang, `onClose` would never fire, and `open()` would keep
        // succeeding into a queue nobody drains. An unknown reset code off the wire used to land
        // here; it does not any more, and neither may anything else.
        logger.error(`muxws conn=${this.id} read loop failed on a ${frame.type} frame`, error);
        const detail = errorMessage(error);
        this.die(new ConnectionClosed(`read loop failed: ${detail}`, { code: 1011, reason: detail }));
        return;
      }
      if (!keepGoing) return;
    }
  }

  /**
   * WSM-FRG-031: measure the **whole encoded message**, never the `fragment` field alone.
   *
   * A receiver must accept anything up to `MAX_FRAME_BYTES` (WSM-FRG-004), so the check only bites
   * above the constant - or, in a test, above the lowered construction cap. A size violation is
   * stream-level (WSM-STM-020): the connection survives it.
   */
  private async withinFrameCap(frame: Frame, message: string | ArrayBuffer): Promise<boolean> {
    const size = encodedLength(message);
    if (size <= this.maxFrameBytes) return true;
    if (frame.stream === null || frame.stream === undefined) {
      await this.failConnection(`a connection-level frame of ${size} bytes exceeds the cap`);
      return false;
    }

    const stream = this.liveStreams.get(frame.stream);
    if (stream !== undefined) {
      this.resetStream(
        stream,
        ResetCode.PAYLOAD_TOO_LARGE,
        `an encoded message of ${size} bytes exceeds this receiver's cap (WSM-FRG-031)`,
      );
    } else {
      // No stream to fail - an over-cap `open`, most often. The reset still goes out: the sender is
      // owed an answer, and it is the only way it can learn of a limit nothing announces.
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
      // Echoed verbatim and at once, with no application involvement whatever (WSM-CON-010). It goes
      // through the ordinary outbound queue, so it is a `ping` frame on the wire and never a native
      // WebSocket control frame - browsers do not expose those to JavaScript (WSM-CON-011).
      this.enqueue({ type: 'pong', nonce: frame.nonce ?? null });
      return true;
    }
    if (frame.type === 'pong') {
      this.onPong(frame);
      return true;
    }
    if (frame.type === 'goaway') return this.onGoaway(frame);
    // WSM-FRM-002: unknown types are ignored, logged once, and are never an error.
    logger.info(`muxws conn=${this.id} ignoring unknown frame type '${frame.type}' (WSM-FRM-002)`);
    return true;
  }

  // ------------------------------------------------------------------ liveness

  /**
   * Round-trip time in **milliseconds** (WSM-CON-012; Python returns seconds).
   *
   * A `ping` frame, not a WebSocket control frame: browsers do not expose those to JavaScript, so a
   * liveness mechanism built on them cannot work on half the peers that exist (WSM-CON-011).
   */
  async ping(timeoutMs: number = DEFAULT_PING_TIMEOUT_MS): Promise<number> {
    // `hasASocket`, not `isOpen`: a ping asks whether there is a wire to put a frame on, and the
    // hello window makes `isOpen` false while the socket is perfectly alive (WSM-RCN-043). The
    // heartbeat is the caller that matters and it only runs on an established connection, so the
    // behaviour is unchanged - what goes away is a public call succeeding on a peer that reports
    // `isOpen === false`.
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
      // A lost pong is not a lost connection: the call fails and the connection is untouched. M5b is
      // where a run of them becomes liveness detection.
      this.pings.giveUp(nonce);
      this.pingStarted.delete(nonce);
      throw new ConnectionClosed(`no pong within ${timeoutMs}ms`, { code: 1006 });
    } finally {
      clearTimeout(timer);
    }
  }

  /** Settle the ping this nonce belongs to; a nonce nobody is waiting for is dropped. */
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

  /** `goaway`, drain, then close - in that order (WSM-CON-025). */
  async close(options: CloseOptions = {}): Promise<void> {
    const { code = ResetCode.NO_ERROR, reason = null, drainMs = DEFAULT_DRAIN_MS } = options;
    // Above the `isOpen` guard, and not below it: a deliberate close never dials again
    // (WSM-RCN-040/044), and the case that most needs saying so is a peer *between* sockets, where
    // there is no socket to close but a helper is still counting down to the next dial. `willRetry`
    // has to be false before `die()` composes the `CloseReason` at the bottom of this method.
    this.willRetry = false;
    void this.connectionLoop?.stop();
    if (!this.open_) return;
    this.sendGoaway(code, reason);
    await this.drain(drainMs);
    try {
      await this.socket.close(1000, reason ?? '');
    } catch (error) {
      // A socket that cannot be closed is already gone; the peer must still die.
      logger.warn(`muxws conn=${this.id} closing the socket failed`, error);
    }
    this.die(new ConnectionClosed(reason ?? 'closed', { code: 1000, reason: reason ?? '', wasClean: true }));
  }

  /**
   * `last_stream` is the highest id **the remote** opened that we have dispatched.
   *
   * Their parity, not ours. Getting it backwards makes every drain reset everything, which looks like
   * a race rather than like an arithmetic mistake.
   */
  private sendGoaway(code: ResetCode, reason: string | null): void {
    if (this.goaway.sent) return;
    this.goaway.sent = true;
    this.goaway.sentCode = code;
    this.enqueue({ type: 'goaway', code, reason, last_stream: this.highestRemoteOpen });
  }

  /** The remote is stopping. Refuse what it never processed; let the rest finish. */
  private onGoaway(frame: Frame): boolean {
    this.goaway.received = true;
    this.goaway.receivedCode = frame.code ?? null;
    this.goaway.receivedReason = frame.reason ?? null;
    this.goaway.remoteLastStream = frame.last_stream ?? null;

    // Streams above the cut-off were never processed, so they are safe to retry elsewhere - and
    // nothing goes out for them: the remote has already stopped reading (WSM-CON-023). `fail()` is
    // the local-only path; `reset()` would put a frame on a wire nobody is reading.
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
   * Let streams at or below the cut-off finish, then close regardless (WSM-CON-024).
   *
   * A deadline, not a poll loop: a peer that waited for quiet would never close against a remote that
   * keeps one stream open. Whatever is still live when the deadline expires takes the socket-death
   * path - it fails locally and nothing is sent for it, because the socket is about to be gone.
   */
  private async drain(timeoutMs: number): Promise<void> {
    if (this.liveStreams.size > 0) {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const deadline = new Promise<void>((resolve) => {
        timer = setTimeout(resolve, timeoutMs);
      });
      try {
        await Promise.race([this.allStreamsClosed(), deadline]);
      } finally {
        clearTimeout(timer);
      }
    }
    await this.drainOutbound();
  }

  /**
   * Resolves once nothing is live. Re-read each round because a stream can close while we wait, and
   * `Stream.closed` resolves on every close path and never rejects (WSM-API-023).
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

    // Explicit state, never inferred from "is an assembler running on that id": inferring it let a
    // wrong-parity `open` pose as the continuation of whatever reassembly happened to be running
    // there, and ran a handler for a stream this peer had opened itself (WSM-STM-031).
    const existing = this.liveStreams.get(streamId);
    if (existing !== undefined && existing.opening && hasFragment(frame)) {
      return this.continueOpen(existing, frame);
    }

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

    if (this.remoteStreamCount() >= this.maxConcurrentStreams) {
      // Refused **without invoking the handler**: nothing ran, so the opener may safely take it
      // elsewhere (WSM-STM-036). There is no STREAM_LIMIT code and no announced quota - the sender is
      // told nothing in advance and learns only from this reset.
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

    if (hasFragment(frame)) {
      stream.opening = true;
      // A fragmented *open* is the same memory exposure as a fragmented *data*, and was the one path
      // into this peer that no cap watched.
      if (!this.withinPayloadCap(stream, frame)) return true;
      const payload = stream.assembler.feed(frame, this.codec);
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

  /** A later fragment of an opening payload: not a second open (WSM-STM-031). */
  private continueOpen(stream: Stream, frame: Frame): boolean {
    if (!this.withinPayloadCap(stream, frame)) return true;
    const payload = stream.assembler.feed(frame, this.codec);
    if (isAbsent(payload)) return true;
    stream.opening = false;
    stream.payload = payload;
    if (frame.end === true) stream.state = StreamState.HALF_CLOSED_REMOTE;
    this.startHandler(stream);
    return true;
  }

  /** Dispatch to the one handler, or refuse when there is none (WSM-STM-033). */
  private startHandler(stream: Stream): void {
    if (this.goaway.sent) {
      // We have said we are stopping; nothing new runs here. REFUSED promises it did not run, so the
      // opener may safely take it elsewhere (WSM-CON-021).
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
      // The wire `reason` is spelled the way Python spells it, so the two ports put the same bytes
      // on the wire for the same event; the local diagnostics below use the TypeScript names.
      this.enqueue({ type: 'reset', stream: stream.id, code: ResetCode.REFUSED, reason: 'no on_stream handler' });
      stream.fail(exceptionForReset(ResetCode.REFUSED, 'no handler', { streamId: stream.id }));
      return;
    }
    // Held on the stream so an incoming reset(CANCELLED) can abort it (WSM-ERR-013). TypeScript has
    // no task cancellation, so the controller *is* the task handle: the stream aborts it, and the
    // handler observes `stream.signal` where Python's handler observes CancelledError. It is assigned
    // synchronously, before the handler is scheduled, so a reset landing in between still aborts it.
    const controller = new AbortController();
    stream.handlerTask = controller;
    const handler = this.handler;
    // `asyncio.create_task` starts the coroutine on a later turn of the loop, so `_on_open` has
    // already returned by the time a handler body runs. A microtask is the JavaScript equivalent;
    // calling `runHandler` directly would run the handler's synchronous prefix inside the read loop.
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
      // Not an application error. WSM-ERR-014 keeps a cancellation propagating rather than dressing
      // it up as a handler failure the remote gets told about.
      if (controller.signal.aborted) return;
      this.resetForHandlerError(stream, error);
      return;
    }
    // WSM-STM-035: a handler that returns without ending its stream ends it implicitly.
    if (stream.state === StreamState.OPEN || stream.state === StreamState.HALF_CLOSED_REMOTE) {
      try {
        await stream.end();
      } catch (error) {
        if (!(error instanceof StreamReset)) throw error;
      }
    }
  }

  /**
   * WSM-STM-034/WSM-INV-008: **always** APPLICATION_ERROR, never REFUSED.
   *
   * REFUSED promises the operation definitively did not happen. A handler that debits an account and
   * then throws would, under REFUSED, be inviting the client to retry the debit.
   */
  private resetForHandlerError(stream: Stream, error: unknown): void {
    let payload: unknown = null;
    try {
      payload = this.errorSerializer(error);
    } catch (serializerError) {
      // WSM-STM-034 is unconditional. A serializer that throws must not swallow the reset with it, or
      // the handler's failure reaches the opener as silence and the caller waits forever.
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
        // A genuine disagreement about the id space, not a late frame (WSM-STM-003).
        await this.failConnection(
          `${frame.type} on stream ${streamId}, above the high-water mark ${highWater} (WSM-STM-003)`,
        );
        return false;
      }
      // Below the mark: expected during a normal race, silently ignored (WSM-STM-002).
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
   * A code this generation does not define is **not** a read-loop failure: it resets its own stream
   * carrying the raw number, which is what a receiver is required to do with vocabulary it does not
   * know. Only APPLICATION_ERROR carries a payload, so only it needs `RemoteError` built by hand.
   */
  private resetException(code: number, reason: string | null, streamId: number, payload: unknown): StreamReset {
    if (code === ResetCode.APPLICATION_ERROR) return new RemoteError(reason, { streamId, payload });
    return exceptionForReset(code as ResetCode, reason, { streamId });
  }

  /**
   * Only the streams the **remote** opened (WSM-STM-037).
   *
   * The limit bounds work the other side can impose. Counting our own would be the announced quota
   * rebuilt by hand, against a number this peer cannot know (WSM-INV-007).
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
      // data after the remote ended: illegal for this stream, harmless for the connection.
      this.resetStream(stream, ResetCode.PROTOCOL_ERROR, 'data after end (WSM-STM-020)');
      return true;
    }

    if (stream.assembler.inProgress && !hasFragment(frame)) {
      this.resetStream(stream, ResetCode.PROTOCOL_ERROR, 'non-fragment frame mid-reassembly (WSM-FRG-033)');
      return true;
    }

    if (hasFragment(frame)) {
      if (!this.withinPayloadCap(stream, frame)) return true;
      const payload = stream.assembler.feed(frame, this.codec);
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
   * WSM-FRG-032/WSM-INV-017: reject **on the crossing fragment**, not after reassembly.
   *
   * Bounding memory is the limit's whole purpose. A receiver that assembles the payload in order to
   * measure it has already spent everything the limit existed to protect, and the partial buffer is
   * dropped in the same step for the same reason (M5a decision 1) - `resetStream` fails the stream,
   * and `Stream.fail` resets its assembler, but the buffer is released here first so the order does
   * not depend on that.
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

  private resetStream(stream: Stream, code: ResetCode, reason: string): void {
    this.enqueue({ type: 'reset', stream: stream.id, code, reason });
    stream.fail(exceptionForReset(code, reason, { streamId: stream.id }));
  }

  // ------------------------------------------------------------------ ending

  /** ILL-C: goaway(PROTOCOL_ERROR), close the socket, then fail every live stream. */
  private async failConnection(reason: string): Promise<void> {
    logger.warn(`muxws conn=${this.id} connection-level protocol error: ${reason}`);
    this.enqueue({
      type: 'goaway',
      code: ResetCode.PROTOCOL_ERROR,
      reason,
      last_stream: this.highestRemoteOpen,
    });
    await this.drainOutbound();
    try {
      await this.socket.close(1002, reason);
    } catch (error) {
      // A socket that cannot be closed is already gone; the peer must still die (see `serve`).
      logger.warn(`muxws conn=${this.id} closing the socket failed`, error);
    }
    this.die(new ConnectionClosed(reason, { code: 1002, reason, wasClean: false }));
  }

  /** Let the writer flush what is already queued, so `goaway` actually reaches the wire. */
  private async drainOutbound(): Promise<void> {
    for (let turn = 0; turn < 100; turn += 1) {
      if (this.pendingFrames === 0) return;
      if (this.writerTask === null || this.writerStopped) return;
      await yieldToEventLoop();
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
    // JavaScript cannot cancel a task stuck inside `await socket.sendText()`. The flag is what
    // Python's `task.cancel()` buys: the loop returns rather than sending anything further.
    this.writerStopped = true;
  }

  /**
   * @internal Socket death: every live stream fails with ConnectionLost **before** onClose fires.
   *
   * Public to the reconnect helper (`ts/reconnect.ts`), which calls it when a swallowed pong or a
   * hello that never completed means the socket is dead but nothing on it has said so. Both go
   * through here so the loss is reported down exactly one path (WSM-RCN-011).
   *
   * `notify: false` is the one death that fires no `onClose`: the **first** connection, which never
   * established. `connect()` throwing is the report (WSM-RCN-006), and there is nobody who could
   * have received the callback - the caller never got the peer back, so it never attached a handler.
   * Marking it dead is still not optional: leaving `isOpen` to a race between the read loop and the
   * socket close is how a peer whose hello failed reads as alive.
   */
  die(cause: ConnectionClosed, options: { notify?: boolean } = {}): void {
    if (!this.open_) return;
    this.open_ = false;
    this.death = cause;

    // Nobody is going to answer a ping now, so nobody should keep waiting for one.
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
   * @internal Fire `onClose`, and nothing else.
   *
   * Extracted from `die` so the reconnect helper can report the one close that has no socket death
   * behind it: `maxAttempts` exhausted, which must fire once with `willRetry` false and then never
   * dial again (WSM-RCN-044). Without this seam that close would either be silent or would have to
   * fake a socket loss.
   */
  notifyClose(reason: CloseReason): void {
    // At most one `willRetry: false` close per peer, ever. When the cap is spent by failed *hellos*
    // rather than by refused dials, the last loss already reported the ending and the helper's
    // give-up would report it a second time - the same peer ending twice, once as a socket loss and
    // once as a decision (WSM-RCN-040/044). Whichever fires first wins.
    if (!reason.willRetry) {
      if (this.finalCloseReported) return;
      this.finalCloseReported = true;
    }
    this.fanOut('onClose', this.closeHandlers, reason);
  }

  /**
   * @internal Take a freshly established socket, for a `Peer` that survived a reconnect.
   *
   * `Peer` survives; `Stream` objects do not (WSM-RCN-032). The id space starts empty, the high-water
   * marks reset, and nothing whatever is carried forward from the dead socket (WSM-RCN-031): a queue
   * that flushed into the new one would deliver work the server has forgotten the sender of
   * (WSM-INV-010).
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
    // A new socket is a new connection, so the "at most one" of WSM-RCN-044 is per connection and
    // not per `Peer` object. Left unreset, a bare `Peer` handed a fresh socket after a
    // `willRetry: false` close reports its next loss to nobody.
    this.finalCloseReported = false;
    this.goaway = new GoawayState();
    this.writer = new Writer(this.codec, { maxFrameBytes: this.maxFrameBytes });
    // `writerTask` has a Python twin (`_writer_task = None`); the two below do not, because Python
    // cancels the task where JavaScript can only flag it. A `writerTask` left pointing at the dead
    // socket's loop - or a `writerStopped` still true from its teardown - would make `serve()` on
    // the new socket send nothing at all.
    this.writerTask = null;
    this.writerStopped = false;
    this.pendingFrames = 0;
    this.exhaustionShutdown = null;
    this.open_ = true;
    this.death = null;
  }

  /**
   * @internal Close the underlying socket, and nothing else.
   *
   * WSM-RCN-011 asks for two things when the heartbeat declares a socket dead - declared dead **and
   * closed locally** - and `die()` only does the first. Without the second the read loop stays parked
   * inside `socket.receive()` on a socket the remote will never write to again, `serve()` never
   * settles, and the reconnect supervisor waits for it forever: a peer that reports itself closed and
   * never reconnects.
   *
   * `code` defaults to 1000 and never to 1006: 1006 is reserved for "the connection dropped without a
   * close frame" and MUST NOT be put on the wire - `ws` rejects it outright, which would leave the
   * socket open and this method silently useless. What the peer *reports* for the death is 1006; what
   * it *sends* to end the socket is not.
   */
  async closeSocketLocally(code = 1000, reason = ''): Promise<void> {
    try {
      await this.socket.close(code, reason);
    } catch (error) {
      // A socket that cannot be closed is already gone, which is the outcome this method wanted.
      logger.debug(`muxws conn=${this.id} closing a locally-declared-dead socket failed`, error);
    }
  }

  /** @internal Stream -> Peer. Nothing is retained per closed stream (WSM-STM-001). */
  forget(stream: Stream): void {
    this.liveStreams.delete(stream.id);
  }

  /** Debug aid; mirrors Python's `__repr__`. */
  toString(): string {
    return `<Peer ${this.id} ${this.dialer ? 'dialer' : 'acceptor'} streams=${this.liveStreams.size}>`;
  }
}
