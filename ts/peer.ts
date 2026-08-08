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
  ConnectionLost,
  exceptionForReset,
  ProtocolError,
  RemoteError,
  ResetCode,
  StreamReset,
  StreamTimeout,
} from './errors';
import { encodedLength, MAX_FRAME_BYTES } from './fragment';
import { ABSENT, type Absent, type Frame } from './frames';
import { Stream, StreamState } from './stream';
import type { SocketAdapter } from './transports';

// --------------------------------------------------------------------------- the logger shim

/**
 * The browser entry point has zero runtime dependencies (WSM-PKG-003), so there is no logging library
 * to reach for. This is the whole of it: `muxws.frames` in Python, four methods over `console` here.
 *
 * `level` starts at `'warn'` because that is what an unconfigured Python logger does - `logger.info`
 * and `logger.debug` on the Python side print nothing until an application configures logging, and a
 * port that spammed every frame to the console by default would not be mirroring it. M5a's
 * observability module takes ownership of configuring this.
 */
type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'silent';

const LEVEL_ORDER: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40, silent: 100 };

/**
 * @internal Exported so a test can raise the level the way `caplog.at_level` does in Python, and so
 * M5a's observability module has something to configure. Not re-exported from `ts/index.ts`: it is a
 * seam, not public API.
 */
export const logger = {
  level: 'warn' as LogLevel,
  isEnabledFor(level: LogLevel): boolean {
    return LEVEL_ORDER[level] >= LEVEL_ORDER[this.level];
  },
  debug(message: string, ...rest: unknown[]): void {
    if (this.isEnabledFor('debug')) console.debug(message, ...rest);
  },
  info(message: string, ...rest: unknown[]): void {
    if (this.isEnabledFor('info')) console.info(message, ...rest);
  },
  warn(message: string, ...rest: unknown[]): void {
    if (this.isEnabledFor('warn')) console.warn(message, ...rest);
  },
  error(message: string, ...rest: unknown[]): void {
    if (this.isEnabledFor('error')) console.error(message, ...rest);
  },
};

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

/** Why a socket ended. The same four fields in both languages (WSM-RCN-045). */
export interface CloseReason {
  readonly code: number;
  readonly reason: string;
  readonly wasClean: boolean;
  /**
   * False only when `maxAttempts` is exhausted or `close()` was called deliberately. Until the
   * reconnect helper lands in M5b there is nothing that retries, so it is always false here.
   */
  readonly willRetry: boolean;
}

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
  maxFrameBytes?: number;
}

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

/** One turn of the event loop, the closest equivalent of `await asyncio.sleep(0)`. */
function yieldToEventLoop(): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, 0);
  });
}

/** The sentinel a deadline rejects with; never an `Error`, so it cannot be confused with one. */
const DEADLINE_EXPIRED: unique symbol = Symbol('DEADLINE_EXPIRED');

/**
 * A minimal unbounded async queue: `put` never blocks, `get` waits.
 *
 * TypeScript has no `asyncio.Queue`, and the outbound queue is the one place the peer needs one.
 */
class AsyncQueue<T> {
  private items: T[] = [];

  private waiters: ((item: T) => void)[] = [];

  get size(): number {
    return this.items.length;
  }

  put(item: T): void {
    const waiter = this.waiters.shift();
    if (waiter !== undefined) {
      waiter(item);
      return;
    }
    this.items.push(item);
  }

  get(): Promise<T> {
    if (this.items.length > 0) return Promise.resolve(this.items.shift() as T);
    return new Promise<T>((resolve) => {
      this.waiters.push(resolve);
    });
  }
}

// --------------------------------------------------------------------------- the peer

/** One symmetric peer type per language: server push is a client request with the roles swapped. */
export class Peer {
  readonly id: string;

  /** An ordinary object with ordinary object semantics. muxws never reads it (WSM-REG-001/002). */
  readonly tags: Record<string, unknown> = {};

  private readonly socket: SocketAdapter;

  private readonly codec: Codec;

  private readonly dialer: boolean;

  private readonly errorSerializer: ErrorSerializer;

  /** M5a wires the splitter into the send path; it is stored here so the signature stays stable. */
  private readonly maxFrameBytes: number;

  /** The dialer allocates odd ids, the acceptor even ones (WSM-SID-002). */
  private nextId: number;

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

  private readonly outbound = new AsyncQueue<Frame | null>();

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

  private death: ConnectionClosed | null = null;

  constructor(socket: SocketAdapter, options: PeerOptions) {
    this.id = `${PROCESS_PREFIX}-${connectionCounter}`;
    connectionCounter += 1;

    this.socket = socket;
    this.codec = options.codec;
    this.dialer = options.isDialer;
    this.errorSerializer = options.errorSerializer ?? defaultErrorSerializer;
    this.maxFrameBytes = options.maxFrameBytes ?? MAX_FRAME_BYTES;
    this.nextId = options.isDialer ? 1 : 2;
  }

  // ------------------------------------------------------------------ properties

  get isOpen(): boolean {
    return this.open_;
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
   * Accepted, stored, and not acted on until M5b.
   *
   * Registering it now is what keeps every call site stable across the milestone that adds the
   * reconnect helper; nothing in M3 ever calls these back.
   */
  onReconnect(handler: (attempt: number, peer: Peer) => void): void {
    this.reconnectHandlers.push(handler);
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
   * Allocation and enqueue are one indivisible step (WSM-SID-006). JavaScript gives this for free -
   * a synchronous function body cannot be preempted, and there is no `await` between the two - but it
   * is stated rather than left to luck: inserting an `await` anywhere between the allocation and the
   * `enqueue` below would let two concurrent `open()` calls interleave and put a non-monotonic id
   * sequence on the wire, a protocol error this peer would be committing against itself
   * (WSM-INV-005).
   */
  open<T = unknown>(payload?: unknown, options?: OpenOptions): Stream<T>;
  open<T = unknown>(options: OpenOptions): Stream<T>;
  open<T = unknown>(first?: unknown, second?: OpenOptions): Stream<T> {
    this.throwIfUnopenable();
    const { payload, headers, end } = resolveCall(first, second, OPEN_OPTION_KEYS);

    const streamId = this.nextId;
    this.nextId += 2;
    this.highestLocalOpen = streamId;
    const stream = new Stream<T>(this, streamId, { headers, payload, local: true });
    stream.state = end ? StreamState.HALF_CLOSED_LOCAL : StreamState.OPEN;
    this.liveStreams.set(streamId, stream);
    this.enqueue({ type: 'open', stream: streamId, payload, headers, end });
    return stream;
  }

  /** WSM-API-004: exactly one synchronous throw, and never one for concurrency. */
  private throwIfUnopenable(): void {
    if (!this.open_) {
      throw new ConnectionLost('the peer is between sockets; nothing is buffered for the next one');
    }
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
    this.outbound.put(frame);
  }

  private async runWriter(): Promise<void> {
    for (;;) {
      const frame = await this.outbound.get();
      if (frame === null || this.writerStopped) return;
      try {
        const encoded = this.codec.encode(frame);
        this.reportFrame('tx', frame, encoded);
        if (this.codec.binary) {
          await this.socket.sendBytes(encoded as ArrayBuffer);
        } else {
          await this.socket.sendText(encoded as string);
        }
      } catch (error) {
        // The socket is gone; `serve()`'s read loop is the one that declares the peer dead.
        if (error instanceof ConnectionClosed) return;
        // A codec that cannot encode this frame - an ArrayBuffer payload under JSON, say - used to
        // take the writer down with it. Nothing then drained the queue, every later send sat in it
        // forever, and the peer went on reporting itself open.
        logger.error(`muxws conn=${this.id} could not send a ${frame.type} frame`, error);
        this.failUnsendable(frame, error);
      } finally {
        this.pendingFrames -= 1;
      }
    }
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

  private reportFrame(direction: 'tx' | 'rx', frame: Frame, encoded: string | ArrayBuffer): void {
    const length = encodedLength(encoded);
    this.frameHandlers.forEach((handler) => {
      handler(direction, frame, length);
    });
    if (logger.isEnabledFor('debug')) {
      // Never the payload's contents: application data routinely holds secrets (WSM-OBS-002).
      logger.debug(
        `muxws conn=${this.id} dir=${direction} type=${frame.type.padEnd(6)} ` +
          `stream=${frame.stream ?? '-'} end=${frame.end === true ? 1 : 0} bytes=${length}`,
      );
    }
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

  /** Route one decoded frame. Returns false when the connection must end. */
  private async dispatch(frame: Frame): Promise<boolean> {
    if (frame.type === 'open') return this.onOpenFrame(frame);
    if (frame.type === 'data' || frame.type === 'reset') return this.onStreamFrame(frame);
    if (frame.type === 'ping' || frame.type === 'pong' || frame.type === 'goaway') {
      // Connection-level frames arrive in M4; tolerating them now costs nothing.
      return true;
    }
    // WSM-FRM-002: unknown types are ignored, logged once, and are never an error.
    logger.info(`muxws conn=${this.id} ignoring unknown frame type '${frame.type}' (WSM-FRM-002)`);
    return true;
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
    const stream = new Stream(this, streamId, { headers: frame.headers ?? null, local: false });
    stream.state = frame.end === true ? StreamState.HALF_CLOSED_REMOTE : StreamState.OPEN;
    this.liveStreams.set(streamId, stream);

    if (hasFragment(frame)) {
      stream.opening = true;
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
    this.outbound.put(null);

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

  /** Socket death: every live stream fails with ConnectionLost **before** onClose fires. */
  private die(cause: ConnectionClosed): void {
    if (!this.open_) return;
    this.open_ = false;
    this.death = cause;

    const detail = cause.reason || errorMessage(cause);
    [...this.liveStreams.values()].forEach((stream) => {
      stream.fail(new ConnectionLost(`connection closed: ${detail}`, { streamId: stream.id }));
    });
    this.liveStreams.clear();

    const reason: CloseReason = {
      code: cause.code,
      reason: cause.reason,
      wasClean: cause.wasClean,
      willRetry: false,
    };
    this.closeHandlers.forEach((handler) => {
      handler(reason);
    });
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
