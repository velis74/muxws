/**
 * `Stream`: one independently addressed, independently cancellable, bidirectional exchange (§5, §9).
 *
 * Mirrors `muxws/stream.py`: the same five states, the same claim on first use, and the same three
 * error classes for a send on a stream that no longer accepts one. TypeScript differs in four points:
 *
 * 1. `Stream<T>` **implements** `PromiseLike<T>`; it does not extend `Promise` (WSM-API-015).
 * 2. The internal answer promise gets its no-op rejection handler in the constructor, and the
 *    failure is passed to the peer's error hook (WSM-API-016).
 * 3. `closed` is a `Promise<void>` that resolves and never rejects, and `signal` is an `AbortSignal`
 *    aborted at the same time (WSM-API-023).
 * 4. Durations are milliseconds and integers.
 */

import {
  exceptionForReset,
  ProtocolError,
  RemoteError,
  ResetCode,
  StreamAlreadyConsumed,
  StreamClosed,
  StreamReset,
  StreamTimeout,
} from './errors';
import { Assembler } from './fragment';
import { ABSENT, type Frame } from './frames';

/** The five states of §5.3, tracked per stream per peer. */
export enum StreamState {
  IDLE = 'idle',
  OPEN = 'open',
  HALF_CLOSED_LOCAL = 'half_closed_local',
  HALF_CLOSED_REMOTE = 'half_closed_remote',
  CLOSED = 'closed',
}

/** How the stream is consumed, once claimed: awaited, iterated, or `notify` (no consumer). */
export type StreamClaim = 'await' | 'iterate' | 'notify';

/** `send()`'s options object. */
export interface SendOptions {
  end?: boolean;
  /** This side's leading headers, if this is the first frame it sends on the stream (WSM-API-024). */
  headers?: Record<string, unknown>;
}

/** `end()`'s options object. The last payload is an option here, not a positional argument. */
export interface EndOptions {
  payload?: unknown;
  trailers?: Record<string, unknown>;
  /** This side's leading headers, if this is the first frame it sends on the stream (WSM-API-024). */
  headers?: Record<string, unknown>;
}

/** `reply()`'s options object. */
export interface ReplyOptions {
  trailers?: Record<string, unknown>;
  /** This side's leading headers, if this is the first frame it sends on the stream (WSM-API-024). */
  headers?: Record<string, unknown>;
}

/**
 * `result()`'s options object.
 *
 * `signal` replaces cancelling the awaiting task in Python. A promise cannot detect that nobody awaits
 * it any more, so a consumer that wants WSM-ERR-014's `reset(CANCELLED)` passes an `AbortSignal`.
 */
export interface ResultOptions {
  timeoutMs?: number;
  signal?: AbortSignal;
}

/**
 * The part of `Peer` that a `Stream` uses.
 *
 * Declared structurally, not imported, to avoid an import cycle; `Peer` satisfies it structurally.
 */
export interface StreamPeer {
  /** @internal Whether a socket exists, independent of whether the connection is established. See `Peer`. */
  readonly hasASocket: boolean;
  /** @internal Synchronous, so `open()` does not suspend between allocating a stream id and enqueuing. */
  enqueue(frame: Frame): void;
  /** @internal Enqueue a stream's `reset` in place of its queued frames (WSM-FRM-014). */
  enqueueReset(streamId: number, code: ResetCode, reason: string | null): void;
  /** @internal Drop a closed stream from the live map. */
  forget(stream: Stream<any>): void;
  /**
   * @internal Receives the failure of a stream that has no consumer (WSM-API-016).
   *
   * Optional; without it the failure is only absorbed by the no-op rejection handler attached in the
   * constructor.
   */
  reportStreamError?(error: unknown, stream: Stream<any> | null): void;
  /**
   * @internal The leading headers of this peer's answer after its reply interceptors have run
   * (WSM-API-028). Optional; without it the headers are sent unchanged.
   */
  replyHeadersFor?(headers: Record<string, unknown> | null, stream: Stream<any>): Record<string, unknown> | null;
}

/** Sentinel pushed into the iteration queue to end a `for await` cleanly. */
const END: unique symbol = Symbol('END');

/** Rejection markers for `waitGuarded`, distinct from any value or error the stream produces. */
const EXPIRED: unique symbol = Symbol('EXPIRED');
const ABANDONED: unique symbol = Symbol('ABANDONED');

/**
 * Method syntax, not function-typed properties: a property-typed `resolve` would make `Stream<T>`
 * invariant in `T`, and the peer's `Map<number, Stream>` would not accept a `Stream<Something>`.
 */
interface Deferred<V> {
  promise: Promise<V>;
  resolve(value: V): void;
  reject(error: unknown): void;
}

function deferred<V>(): Deferred<V> {
  let resolve!: (value: V) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<V>((settle, fail) => {
    resolve = settle;
    reject = fail;
  });
  return { promise, resolve, reject };
}

/**
 * Return `code` if it may be sent; throw `ProtocolError` otherwise.
 *
 * `CONNECTION_CLOSED` is synthesised locally when the socket is lost and MUST NEVER appear on the
 * wire (§8.1). Values outside `ResetCode` are not defined by this protocol generation.
 */
function sendableResetCode(code: ResetCode): ResetCode {
  if (code === ResetCode.CONNECTION_CLOSED) {
    throw new ProtocolError(
      'CONNECTION_CLOSED is synthesised locally when the socket dies and must never be sent; ' +
        'use cancel() or another reset code (§8.1)',
    );
  }
  const name: string | undefined = typeof code === 'number' ? ResetCode[code] : undefined;
  if (!Number.isInteger(code as number) || name === undefined) {
    throw new ProtocolError(`${String(code)} is not a reset code this generation defines (§8.1)`);
  }
  return code;
}

/**
 * A new instance of the same failure.
 *
 * `send()` on a reset stream throws the stream's stored failure. A new instance per throw gives each
 * caller its own object and a `stack` for its own call site. `exceptionForReset` builds the same
 * class, so `instanceof` checks are unaffected.
 */
function freshReset(error: StreamReset): StreamReset {
  if (error instanceof RemoteError) {
    return new RemoteError(error.reason, { streamId: error.streamId, payload: error.payload });
  }
  return exceptionForReset(error.code, error.reason, { streamId: error.streamId });
}

/** The iteration buffer: a FIFO whose `get()` waits when it is empty. */
class PayloadQueue {
  private readonly items: unknown[] = [];
  private readonly waiters: ((item: unknown) => void)[] = [];

  get isEmpty(): boolean {
    return this.items.length === 0;
  }

  /** The head, without consuming it. */
  peek(): unknown {
    return this.items[0];
  }

  put(item: unknown): void {
    const waiter = this.waiters.shift();
    if (waiter !== undefined) {
      waiter(item);
      return;
    }
    this.items.push(item);
  }

  get(): Promise<unknown> {
    if (this.items.length > 0) return Promise.resolve(this.items.shift());
    return new Promise<unknown>((resolve) => {
      this.waiters.push(resolve);
    });
  }
}

/** Simultaneously awaitable and async-iterable; the first use claims it (WSM-API-002/014). */
export class Stream<T = unknown> implements PromiseLike<T>, AsyncIterable<T> {
  readonly id: number;

  /** The `open` frame's headers, as sent by the opener; the same on both peers. */
  readonly headers: Record<string, unknown>;

  /**
   * The answering side's leading headers; an empty object until received (WSM-API-025).
   *
   * The counterpart of `headers`, with the same value on both peers: the opener sees the headers the
   * answer carried, and the answering peer sees the headers it sent. Empty, not `null`, when none
   * were sent.
   *
   * Not `readonly`: on the opener's side it is set when the answer's first frame arrives.
   * `replyHeadersArrived` resolves when the value is final.
   */
  replyHeaders: Record<string, unknown> = {};

  /**
   * Resolves when `replyHeaders` can no longer change, and never rejects (WSM-API-025).
   *
   * It resolves on the answering side's first frame on the stream, with or without headers (a first
   * frame without headers means none will be sent), or when the stream closes without an answer
   * (a reset before any answer, or a lost socket).
   */
  readonly replyHeadersArrived: Promise<void>;

  /**
   * The opening payload, already reassembled.
   *
   * Not `readonly`: the peer sets it when the last fragment of a fragmented `open` arrives.
   */
  payload: unknown;

  /** Populated when the stream ends, if the remote sent any. */
  trailers: Record<string, unknown> | null = null;

  /**
   * Resolves on every close path, including a reset and a lost socket, and never rejects
   * (WSM-API-023). A reset is reported through the awaited result and the iterator.
   */
  readonly closed: Promise<void>;

  /** @internal The state of §5.3. Read and written by the peer. */
  state: StreamState = StreamState.IDLE;

  /** @internal True when this peer opened the stream (WSM-STM-037 counts only the remote's). */
  local: boolean;

  /**
   * @internal True from the first fragment of a remote opening payload until the frame that
   * completes it.
   *
   * Explicit state, not inferred from whether an assembler is active on this id; that inference
   * would treat a wrong-parity `open` as the continuation of a running reassembly.
   */
  opening = false;

  /** @internal Reassembles a fragmented payload for this stream. */
  assembler = new Assembler();

  /**
   * @internal The controller of the handler the peer runs for this stream, so an incoming
   * `reset(CANCELLED)` aborts it (WSM-ERR-013). Aborted by `fail()`, where Python cancels the task.
   */
  handlerTask: AbortController | null = null;

  /** @internal Which shape claimed the stream; the peer sets `'notify'` directly. */
  claim: StreamClaim | null = null;

  /**
   * @internal True once the remote has sent a frame on this stream (the receiving side of
   * WSM-FRM-016). The peer reads it to recognise the answer to a stream it opened (WSM-API-029).
   */
  receivedAFrame = false;

  private readonly peer: StreamPeer;
  private readonly queue = new PayloadQueue();
  private readonly answer = deferred<T>();
  private readonly closeGate = deferred<void>();
  private readonly headersGate = deferred<void>();
  private readonly closeController = new AbortController();

  /** @internal True once this side has sent a frame on this stream (WSM-FRM-016). */
  private sentAFrame = false;

  /** True once `replyHeadersArrived` has been resolved. */
  private headersSettled = false;

  /** True once the memoized promise has been resolved or rejected. */
  private answerSettled = false;

  /** True once the memoized promise has been requested; corresponds to Python's lazily created future. */
  private answerRequested = false;

  /** How the stream ended: `null` while live, `'normal'` when both ends ended, else the failure. */
  private closeCause: 'normal' | StreamReset | null = null;

  constructor(
    peer: StreamPeer,
    id: number,
    options: { headers?: Record<string, unknown> | null; payload?: unknown; local: boolean },
  ) {
    this.peer = peer;
    this.id = id;
    this.headers = options.headers ?? {};
    this.payload = options.payload === undefined ? ABSENT : options.payload;
    this.local = options.local;
    this.closed = this.closeGate.promise;
    this.replyHeadersArrived = this.headersGate.promise;

    // Leading headers are allowed only on each side's first frame (WSM-FRM-016). The opener's first
    // frame is the `open`: for a local stream this peer has sent it, for a remote stream it has
    // received it.
    if (options.local) this.sentAFrame = true;
    else this.receivedAFrame = true;

    // WSM-API-016: attached in the constructor, not on the first `then`, because a rejection before
    // the first `then` would already be reported as unhandled. The failure of an unconsumed stream
    // is passed to `reportUnconsumed` from `fail()`.
    void this.answer.promise.catch(() => undefined);
  }

  /**
   * Aborted when `closed` resolves, on every close path.
   *
   * Replaces Python's `CancelledError` (WSM-ERR-013): JavaScript cannot interrupt a running function,
   * so a handler observes the signal and stops.
   */
  get signal(): AbortSignal {
    return this.closeController.signal;
  }

  // ------------------------------------------------------------------ sending

  /** Send one payload. Rejects per WSM-ERR-009 on a stream that is no longer open. */
  async send(payload: unknown, options: SendOptions = {}): Promise<void> {
    const end = options.end ?? false;
    this.raiseIfNotSendable();
    if (this.state === StreamState.HALF_CLOSED_LOCAL) {
      throw new StreamClosed(`stream ${this.id} already sent end; it cannot send again`);
    }
    const headers = this.leadingHeaders(options.headers);
    this.peer.enqueue({ type: 'data', stream: this.id, payload, headers, end });
    if (end) this.localEnd();
  }

  /**
   * Send this side's leading headers without a payload (WSM-API-024).
   *
   * The headers are carried by a `data` frame without a payload, so an answering peer can send them
   * before it has a payload. This is this side's first frame, so a later `send({ headers })` on the
   * same stream throws.
   */
  async sendHeaders(headers: Record<string, unknown>): Promise<void> {
    this.raiseIfNotSendable();
    if (this.state === StreamState.HALF_CLOSED_LOCAL) {
      throw new StreamClosed(`stream ${this.id} already sent end; it cannot send headers after it`);
    }
    this.peer.enqueue({ type: 'data', stream: this.id, headers: this.leadingHeaders(headers) });
  }

  /** End this side of the stream, optionally with a last payload and trailers. */
  async end(options: EndOptions = {}): Promise<void> {
    this.raiseIfNotSendable();
    if (this.state === StreamState.HALF_CLOSED_LOCAL) {
      throw new StreamClosed(`stream ${this.id} already sent end; it cannot end twice`);
    }
    const payload = options.payload === undefined ? ABSENT : options.payload;
    const headers = this.leadingHeaders(options.headers);
    this.peer.enqueue({
      type: 'data',
      stream: this.id,
      payload,
      headers,
      end: true,
      trailers: options.trailers ?? null,
    });
    this.localEnd();
  }

  /** Send `payload` with `end`; for unary handlers. */
  async reply(payload: unknown, options: ReplyOptions = {}): Promise<void> {
    await this.end({ payload, trailers: options.trailers, headers: options.headers });
  }

  /**
   * The sending side of WSM-FRM-016: headers are allowed only on this side's first frame.
   *
   * Called for every frame this side sends, with or without headers, so the first frame is
   * recorded. Headers passed after the first frame raise `ProtocolError` instead of being dropped
   * (WSM-API-024).
   */
  private leadingHeaders(headers: Record<string, unknown> | undefined): Record<string, unknown> | null {
    if (headers !== undefined && this.sentAFrame) {
      throw new ProtocolError(
        this.local
          ? `stream ${this.id} was opened by this peer, so its first frame was the open; ` +
              'pass headers to open() instead (WSM-FRM-016)'
          : `stream ${this.id} has already sent its first frame; headers ride that one or none (WSM-FRM-016)`,
      );
    }
    let leading = headers ?? null;
    if (!this.sentAFrame && !this.local && this.peer.replyHeadersFor !== undefined) {
      // Runs before `sentAFrame` is set, so if an interceptor throws, the first frame is still unsent
      // (WSM-API-028).
      leading = this.peer.replyHeadersFor(leading, this);
    }
    this.sentAFrame = true;
    // On the answering side `replyHeaders` holds the headers this peer sent, matching what the opener
    // receives.
    if (!this.local) this.acceptReplyHeaders(leading);
    return leading;
  }

  /** Throw per WSM-ERR-009: a fresh `StreamReset` subclass after a reset, `StreamClosed` after a normal close. */
  private raiseIfNotSendable(): void {
    if (this.closeCause === null) return;
    if (this.closeCause !== 'normal') throw freshReset(this.closeCause);
    throw new StreamClosed(`stream ${this.id} closed normally; nothing more can be sent on it`);
  }

  // ------------------------------------------------------------------ resetting

  /** `reset(CANCELLED)`. Closes locally immediately, without waiting for acknowledgement. */
  async cancel(reason?: string): Promise<void> {
    await this.reset(ResetCode.CANCELLED, reason);
  }

  /**
   * Terminate the stream in both directions.
   *
   * A no-op once the stream is closed, including after the socket is lost (WSM-RCN-041).
   */
  async reset(code: ResetCode, reason?: string): Promise<void> {
    const sendable = sendableResetCode(code);
    if (this.state === StreamState.CLOSED) return;
    // `hasASocket`, not `isOpen`: during the hello window `isOpen` is false while the socket is open
    // (WSM-RCN-043). Checking `isOpen` would drop the `reset` for a stream the acceptor opened in that
    // window.
    if (this.peer.hasASocket) {
      this.peer.enqueueReset(this.id, sendable, reason ?? null);
    }
    this.fail(exceptionForReset(sendable, reason ?? null, { streamId: this.id }));
  }

  // ------------------------------------------------------------------ receiving

  /**
   * @internal The receiving side of WSM-FRM-016. Returns false when the frame violates it.
   *
   * Called for every stream-level frame from the remote, with or without headers, so the remote's
   * first frame is recorded even without headers; `replyHeadersArrived` then resolves on that frame.
   */
  noteRemoteFrame(headers: Record<string, unknown> | null | undefined): boolean {
    const first = !this.receivedAFrame;
    this.receivedAFrame = true;
    if (!first) return headers === null || headers === undefined;
    // On a stream this peer opened, the remote's first frame is the answer and carries the reply
    // headers. On a stream the remote opened, its first frame was the `open`, whose headers were set
    // as `headers` at construction.
    if (this.local) this.acceptReplyHeaders(headers);
    return true;
  }

  /** Set `replyHeaders` (if `headers` is present) and resolve `replyHeadersArrived`. */
  private acceptReplyHeaders(headers: Record<string, unknown> | null | undefined): void {
    if (headers !== null && headers !== undefined) this.replyHeaders = headers;
    this.settleHeaders();
  }

  /** Resolve `replyHeadersArrived`, once. */
  private settleHeaders(): void {
    if (this.headersSettled) return;
    this.headersSettled = true;
    this.headersGate.resolve();
  }

  /** @internal Deliver one reassembled payload to the stream's consumer. */
  acceptPayload(payload: unknown): void {
    if (this.answerRequested) this.settleAnswer(payload as T);
    this.queue.put(payload);
  }

  /** @internal The remote sent `end: true`. */
  remoteEnd(trailers: Record<string, unknown> | null): void {
    if (trailers !== null && trailers !== undefined) this.trailers = trailers;
    if (this.state === StreamState.HALF_CLOSED_LOCAL) {
      this.closeNormally();
      return;
    }
    if (this.state !== StreamState.CLOSED) {
      this.state = StreamState.HALF_CLOSED_REMOTE;
      this.queue.put(END);
      this.failAnswerIfEmpty();
    }
  }

  /** @internal Close because of a local or remote reset, or a lost socket. */
  fail(error: StreamReset): void {
    if (this.state === StreamState.CLOSED) return;
    this.state = StreamState.CLOSED;
    this.closeCause = error;
    this.assembler.reset();
    this.failAnswer(error);
    this.queue.put(error);
    this.markClosed(error);
    if (this.handlerTask !== null && !this.handlerTask.signal.aborted) this.handlerTask.abort(error);
    this.peer.forget(this);
    this.reportUnconsumed(error);
  }

  /**
   * Pass the failure of a stream without a consumer to the peer's hook (WSM-API-016).
   *
   * An awaited or iterated stream reports the failure through the promise or the iterator. An
   * unclaimed stream, or the stream behind `notify()`, which has no handle, reports it through the
   * hook.
   *
   * An exception thrown by the hook is caught, so it does not interrupt the close.
   */
  private reportUnconsumed(error: StreamReset): void {
    if (this.claim === 'await' || this.claim === 'iterate') return;
    try {
      this.peer.reportStreamError?.(error, this);
    } catch {
      // Ignored: re-throwing here would abort the read loop.
    }
  }

  private localEnd(): void {
    if (this.state === StreamState.HALF_CLOSED_REMOTE) this.closeNormally();
    else this.state = StreamState.HALF_CLOSED_LOCAL;
  }

  private closeNormally(): void {
    this.state = StreamState.CLOSED;
    this.closeCause = 'normal';
    this.queue.put(END);
    this.failAnswerIfEmpty();
    this.markClosed();
    this.peer.forget(this);
  }

  /** Resolve `closed` and abort `signal`, on every close path (WSM-API-023). */
  private markClosed(reason?: StreamReset): void {
    // A stream that closes without an answer receives no reply headers, so `replyHeadersArrived`
    // resolves here (WSM-API-025).
    this.settleHeaders();
    this.closeGate.resolve();
    if (!this.closeController.signal.aborted) this.closeController.abort(reason);
  }

  /** Reject a pending await with `ProtocolError` when the stream ends without a payload. */
  private failAnswerIfEmpty(): void {
    this.failAnswer(new ProtocolError(`stream ${this.id} ended without producing a payload`));
  }

  /**
   * Reject the memoized promise, if there is one to reject.
   *
   * Only if the promise has been requested, as Python checks whether the future exists. A rejection
   * that nobody retrieves would produce a warning. A later await still receives the failure:
   * `resolveFromCurrentState` reads it from `closeCause`.
   */
  private failAnswer(error: unknown): void {
    if (!this.answerRequested) return;
    this.rejectAnswer(error);
  }

  private settleAnswer(value: T): void {
    if (this.answerSettled) return;
    this.answerSettled = true;
    this.answer.resolve(value);
  }

  private rejectAnswer(error: unknown): void {
    if (this.answerSettled) return;
    this.answerSettled = true;
    this.answer.reject(error);
  }

  // ------------------------------------------------------------------ consuming

  private claimFor(use: StreamClaim): void {
    if (this.claim !== null && this.claim !== use) {
      throw new StreamAlreadyConsumed(
        `stream ${this.id} was already consumed by '${this.claim}' and cannot also be consumed by ` +
          `'${use}'; a stream has one consumer (WSM-API-014)`,
      );
    }
    if (this.claim === 'iterate' && use === 'iterate') {
      throw new StreamAlreadyConsumed(
        `stream ${this.id} is already being iterated; two 'iterate' consumers would split its ` +
          'payloads between them (WSM-API-014)',
      );
    }
    this.claim = use;
  }

  /** The stream's single answer promise, created on first use and settled once (WSM-API-010/011). */
  private memoizedAnswer(): Promise<T> {
    if (!this.answerRequested) {
      this.answerRequested = true;
      this.resolveFromCurrentState();
    }
    return this.answer.promise;
  }

  /**
   * Settle the memoized promise from the stream's current state.
   *
   * The first await may come after a payload, a reset, or an end without payload has arrived; in each
   * case the promise settles immediately instead of staying pending (WSM-INV-011).
   */
  private resolveFromCurrentState(): void {
    if (this.answerSettled) return;
    if (this.closeCause !== null && this.closeCause !== 'normal') {
      this.rejectAnswer(this.closeCause);
      return;
    }
    if (this.queue.isEmpty) return;
    const pending = this.queue.peek();
    if (pending instanceof StreamReset) this.rejectAnswer(pending);
    else if (pending === END)
      this.rejectAnswer(new ProtocolError(`stream ${this.id} ended without producing a payload`));
    else this.settleAnswer(pending as T);
  }

  /**
   * `PromiseLike`, not a `Promise` subclass (WSM-API-015).
   *
   * With a `Promise` subclass, species semantics would construct a `Stream` for every derived promise
   * in a `stream.then(...).then(...)` chain. `then` returns an ordinary `Promise`.
   */
  then<R1 = T, R2 = never>(
    onOk?: ((value: T) => R1 | PromiseLike<R1>) | null,
    onErr?: ((error: unknown) => R2 | PromiseLike<R2>) | null,
  ): Promise<R1 | R2> {
    this.claimFor('await');
    return this.wait().then(onOk, onErr);
  }

  catch<R2 = never>(onErr?: ((error: unknown) => R2 | PromiseLike<R2>) | null): Promise<T | R2> {
    return this.then<T, R2>(undefined, onErr);
  }

  finally(onSettled?: (() => void) | null): Promise<T> {
    return this.then().finally(onSettled);
  }

  /**
   * The same promise with a deadline, and optionally an `AbortSignal`, wrapped around the wait
   * (WSM-API-012).
   *
   * `await stream` and `await stream.result()` share one promise, so a second read returns the first
   * read's value, not the next payload.
   */
  async result(options: ResultOptions = {}): Promise<T> {
    this.claimFor('await');
    return this.wait(options);
  }

  /** The wait shared by `await stream` and `result()`. */
  private wait(options: ResultOptions = {}): Promise<T> {
    const answer = this.memoizedAnswer();
    if (options.timeoutMs === undefined && options.signal === undefined) return answer;
    return this.waitGuarded(answer, options);
  }

  /**
   * Race the memoized promise against the caller's deadline and signal.
   *
   * Losing the `Promise.race` does not settle the memoized promise, so a second await still receives
   * the same answer (WSM-API-011).
   */
  private async waitGuarded(answer: Promise<T>, options: ResultOptions): Promise<T> {
    const { timeoutMs, signal } = options;
    if (signal?.aborted === true) return this.abandon(signal);

    let timer: ReturnType<typeof setTimeout> | undefined;
    let onAbort: (() => void) | undefined;
    const guard = new Promise<never>((_resolve, reject) => {
      if (timeoutMs !== undefined) timer = setTimeout(() => reject(EXPIRED), timeoutMs);
      if (signal !== undefined) {
        onAbort = () => reject(ABANDONED);
        signal.addEventListener('abort', onAbort, { once: true });
      }
    });

    try {
      return await Promise.race([answer, guard]);
    } catch (error) {
      if (error === EXPIRED) {
        // WSM-ERR-011: the remote is told to stop working before the caller is told it timed out.
        await this.reset(ResetCode.TIMEOUT, `deadline of ${timeoutMs}ms expired`);
        throw new StreamTimeout(`stream ${this.id} did not answer within ${timeoutMs}ms`, { streamId: this.id });
      }
      if (error === ABANDONED && signal !== undefined) return this.abandon(signal);
      throw error;
    } finally {
      if (timer !== undefined) clearTimeout(timer);
      if (onAbort !== undefined && signal !== undefined) signal.removeEventListener('abort', onAbort);
    }
  }

  /**
   * WSM-ERR-014: a consumer that aborts resets the stream with `CANCELLED`, so the remote stops
   * producing, and the abort reason is re-thrown.
   */
  private async abandon(signal: AbortSignal): Promise<never> {
    await this.reset(ResetCode.CANCELLED, 'consumer cancelled');
    throw signal.reason ?? new StreamReset('consumer cancelled', { code: ResetCode.CANCELLED, streamId: this.id });
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    this.claimFor('iterate');
    return this.iterate();
  }

  private async *iterate(): AsyncGenerator<T> {
    // A `break`, `return` or throw in the loop body finalises the generator here. This is the only
    // way TypeScript observes that the consumer stopped iterating, so WSM-ERR-014's reset is sent
    // here. `reset()` is a no-op on a closed stream, so a loop that ended because of a reset does not
    // send a second one.
    let walkedAway = true;
    try {
      for (;;) {
        const item = await this.queue.get();
        if (item === END) {
          walkedAway = false;
          return;
        }
        if (item instanceof StreamReset) {
          walkedAway = false;
          throw item;
        }
        yield item as T;
      }
    } finally {
      if (walkedAway) await this.reset(ResetCode.CANCELLED, 'consumer cancelled');
    }
  }

  toString(): string {
    return `<Stream ${this.id} ${this.state}>`;
  }
}
