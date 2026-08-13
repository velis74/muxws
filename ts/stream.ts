/**
 * `Stream`: one independently addressed, independently cancellable, bidirectional exchange (§5, §9).
 *
 * A port of `muxws/stream.py` - same five states, same claim-on-first-use handle, same three-way
 * distinction on a send that is no longer legal - with the four differences TypeScript forces:
 *
 * 1. `Stream<T>` **implements** `PromiseLike<T>`; it does not extend `Promise` (WSM-API-015).
 * 2. The internal answer promise gets its no-op rejection handler in the **constructor**, and the
 *    failure is routed to the peer's error hook (WSM-API-016).
 * 3. `closed` is a `Promise<void>` that resolves and never rejects, and `signal` is an `AbortSignal`
 *    aborted at the same instant (WSM-API-023).
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

/** Which of the two consuming shapes owns this stream, once one of them has taken it. */
export type StreamClaim = 'await' | 'iterate' | 'notify';

/** `send()`'s options object. */
export interface SendOptions {
  end?: boolean;
  /** This side's leading headers, if this is the first frame it sends on the stream (WSM-API-024). */
  headers?: Record<string, unknown>;
}

/** `end()`'s options object. TypeScript takes the last payload here rather than positionally. */
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
 * `signal` is the TypeScript stand-in for cancelling the awaiting task in Python: abandoning a
 * promise is invisible to the promise, so a consumer that wants WSM-ERR-014's `reset(CANCELLED)`
 * says so with an `AbortSignal`.
 */
export interface ResultOptions {
  timeoutMs?: number;
  signal?: AbortSignal;
}

/**
 * The half of `Peer` a `Stream` is allowed to reach into.
 *
 * Declared structurally rather than imported so the two modules do not form an import cycle; the
 * real `Peer` satisfies it by shape.
 */
export interface StreamPeer {
  /** @internal Whether a wire still exists - not whether the connection is established. See `Peer`. */
  readonly hasASocket: boolean;
  /** @internal Synchronous by contract: `open()` must not suspend between allocating and enqueuing. */
  enqueue(frame: Frame): void;
  /** @internal Drop a closed stream from the live map. */
  forget(stream: Stream<any>): void;
  /**
   * @internal WSM-API-016's destination: the failure of a stream nobody consumed.
   *
   * Optional so a peer that has not wired it up still type-checks; without it the rejection is
   * simply swallowed by the constructor's handler, which is the half of the rule that must never
   * be missing.
   */
  reportStreamError?(error: unknown, stream: Stream<any> | null): void;
}

/** Sentinel pushed into the iteration queue to end a `for await` cleanly. */
const END: unique symbol = Symbol('END');

/** Race markers, distinguishable from any value or error the stream itself could produce. */
const EXPIRED: unique symbol = Symbol('EXPIRED');
const ABANDONED: unique symbol = Symbol('ABANDONED');

/**
 * Method syntax rather than function-typed properties on purpose: a property-typed `resolve` would
 * make `Stream<T>` invariant in `T`, and the peer's `Map<number, Stream>` would then refuse every
 * `Stream<Something>` the application opened.
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
 * Guard the one code that is synthesised locally and the numbers this generation does not use.
 *
 * `CONNECTION_CLOSED` means "the socket under this stream died"; putting it on the wire would tell a
 * remote that *its* connection had died, which is both false and unfalsifiable (§8.1: it MUST NEVER
 * appear on the wire). Reset code 5 is retired and MUST NOT be reused. Anything outside the enum
 * would be this peer inventing wire vocabulary.
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
    throw new ProtocolError(
      `${String(code)} is not a reset code this generation defines; 5 is retired and must not be reused (§8.1)`,
    );
  }
  return code;
}

/**
 * A fresh instance of the same failure.
 *
 * `send()` on a reset stream reports the stream's stored failure. Throwing the stored *instance*
 * hands every caller the same mutable object, with one shared `stack` that no longer describes where
 * it was thrown; a loop that keeps sending would keep handing out the same one. `exceptionForReset`
 * rebuilds exactly the class the failure was built as, so identity checks still hold.
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

  /** The `open` frame's headers. The same mapping on both peers: the opener's, as it sent them. */
  readonly headers: Record<string, unknown>;

  /**
   * The **answering** side's leading headers, empty until there are any (WSM-API-025).
   *
   * The other half of `headers`, and read the same way from either end: the peer that opened the
   * stream sees what the answer announced, the peer answering sees what it announced itself. Empty
   * rather than null when nothing was announced, so a caller never has two absences to handle.
   *
   * Not `readonly`, because on the opener's side these land when the answer's first frame does -
   * `replyHeadersArrived` is how a caller knows the value it is reading is the final one.
   */
  replyHeaders: Record<string, unknown> = {};

  /**
   * Resolves when `replyHeaders` can no longer change, and **never rejects** (WSM-API-025).
   *
   * That instant is the answering side's first frame on the stream - carrying headers or not, since
   * a first frame without them means none are coming - or the close of a stream that was never
   * answered. Both, because the second is the one the remote controls: a stream reset before any
   * answer, or a socket that dies, would otherwise leave this pending for the rest of the process.
   */
  readonly replyHeadersArrived: Promise<void>;

  /**
   * The opening payload, already reassembled.
   *
   * Not `readonly`: the peer fills it in once the last fragment of a fragmented `open` lands.
   */
  payload: unknown;

  /** Populated when the stream ends, if the remote sent any. */
  trailers: Record<string, unknown> | null = null;

  /**
   * Resolves on every close path, including a reset and socket death, and **never rejects**
   * (WSM-API-023) - a stream that closed by being reset still closed, and the reset reaches the
   * awaits and the iterator instead.
   */
  readonly closed: Promise<void>;

  /** @internal The state of §5.3. Read and written by the peer. */
  state: StreamState = StreamState.IDLE;

  /** @internal True when this peer opened the stream (WSM-STM-037 counts only the remote's). */
  local: boolean;

  /**
   * @internal True between the first fragment of a *remote* opening payload and the frame that
   * completes it.
   *
   * Explicit state, never inferred from "is an assembler running on this id": inferring it let a
   * wrong-parity `open` be taken for a continuation of whatever reassembly happened to be running.
   */
  opening = false;

  /** @internal Reassembles a fragmented payload for this stream. */
  assembler = new Assembler();

  /**
   * @internal The controller the peer runs its handler under, so an incoming `reset(CANCELLED)` can
   * abort it (WSM-ERR-013). It is aborted by `fail()`, exactly where Python cancels the task.
   */
  handlerTask: AbortController | null = null;

  /** @internal Which shape claimed the stream; the peer sets `'notify'` directly. */
  claim: StreamClaim | null = null;

  private readonly peer: StreamPeer;
  private readonly queue = new PayloadQueue();
  private readonly answer = deferred<T>();
  private readonly closeGate = deferred<void>();
  private readonly headersGate = deferred<void>();
  private readonly closeController = new AbortController();

  /** @internal True once this side has put any frame on this stream - the one chance of WSM-FRM-016. */
  private sentAFrame = false;

  /** True once the remote has sent one - the other half of the same rule. */
  private receivedAFrame = false;

  /** True once `headersArrived` has been resolved; it resolves exactly once, from three places. */
  private headersSettled = false;

  /** True once the memoized promise has been settled, either way. It settles exactly once. */
  private answerSettled = false;

  /** True once something has asked for the memoized promise - the analogue of Python's lazy future. */
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

    // Each side's one chance at leading headers (WSM-FRM-016) is spent by its first frame, and one
    // of the two is already gone: a locally opened stream was created by the `open` this peer had
    // just enqueued, a remotely opened one by the `open` it had just read.
    if (options.local) this.sentAFrame = true;
    else this.receivedAFrame = true;

    // WSM-API-016: attached **here**, not on the first `then`. By the time a caller reaches for the
    // stream the rejection may already have been reported to the runtime as unhandled, and there is
    // no way to un-report it. Where the failure of a stream nobody consumed actually goes is
    // `reportUnconsumed`, called from `fail()`; this handler is the guarantee that it can never
    // reach the console on the way.
    void this.answer.promise.catch(() => undefined);
  }

  /**
   * Aborted at the same instant `closed` resolves, on every close path.
   *
   * This is what a TypeScript handler observes in place of Python's `CancelledError` (WSM-ERR-013):
   * there is no way to interrupt a running function, so the handler is told and cooperates.
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
   * Announce this side's leading headers with no payload at all (WSM-API-024).
   *
   * The frame that carries them is a `data` with nothing in it, which is what lets an answering peer
   * say what is coming before it has computed any of it. It spends this side's one chance either
   * way, so a later `send({ headers })` on the same stream raises.
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

  /** `send` plus `end`, which is what a unary handler wants. */
  async reply(payload: unknown, options: ReplyOptions = {}): Promise<void> {
    await this.end({ payload, trailers: options.trailers, headers: options.headers });
  }

  /**
   * WSM-FRM-016's sending half: headers ride this side's first frame on the stream, or nothing.
   *
   * Called on the way to **every** frame this side sends, headers or not, because the rule is about
   * which frame is first and not about which call carried a `headers` argument. Refusing rather than
   * dropping is the point of WSM-API-024: a sender whose second set vanished quietly would believe
   * it had announced something the remote never saw.
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
    this.sentAFrame = true;
    // This peer is the one answering, so what it announces is what `replyHeaders` means on both
    // ends: a handler reads back what it sent, exactly as the opener reads back what it received.
    if (!this.local) this.acceptReplyHeaders(headers);
    return headers ?? null;
  }

  /** WSM-ERR-009: three different outcomes, three different classes. */
  private raiseIfNotSendable(): void {
    if (this.closeCause === null) return;
    if (this.closeCause !== 'normal') throw freshReset(this.closeCause);
    throw new StreamClosed(`stream ${this.id} closed normally; nothing more can be sent on it`);
  }

  // ------------------------------------------------------------------ resetting

  /** `reset(CANCELLED)`, closing locally at once without waiting for acknowledgement. */
  async cancel(reason?: string): Promise<void> {
    await this.reset(ResetCode.CANCELLED, reason);
  }

  /**
   * Terminate the stream in both directions.
   *
   * A no-op once the stream is closed - including after socket death, where there is nothing to send
   * it on (WSM-RCN-041).
   */
  async reset(code: ResetCode, reason?: string): Promise<void> {
    const sendable = sendableResetCode(code);
    if (this.state === StreamState.CLOSED) return;
    // `hasASocket`, not `isOpen`: the question here is whether a wire exists, and the hello window
    // makes `isOpen` false while the socket is perfectly alive (WSM-RCN-043). Asking the wrong one
    // loses the `reset` for any stream the acceptor pushed inside that window.
    if (this.peer.hasASocket) {
      this.peer.enqueue({ type: 'reset', stream: this.id, code: sendable, reason: reason ?? null });
    }
    this.fail(exceptionForReset(sendable, reason ?? null, { streamId: this.id }));
  }

  // ------------------------------------------------------------------ receiving

  /**
   * @internal WSM-FRM-016's receiving half. Returns false when the frame breaks it.
   *
   * Called for every stream-level frame the remote sends, headers or not: what makes a set of
   * headers legal is being on the remote's **first** frame, so the first frame has to be recognised
   * even when it carries none - and recognising it is also what lets `headersArrived` settle then
   * rather than waiting for a second set that the rule says can never come.
   */
  noteRemoteFrame(headers: Record<string, unknown> | null | undefined): boolean {
    const first = !this.receivedAFrame;
    this.receivedAFrame = true;
    if (!first) return headers === null || headers === undefined;
    // The remote's first frame on a stream **this** peer opened is its answer, so these are the
    // reply headers. On a stream the remote opened, its first frame was the `open`, whose headers
    // are `headers` and were read at construction - an opener has no reply to announce.
    if (this.local) this.acceptReplyHeaders(headers);
    return true;
  }

  /** `replyHeaders` and its gate move together, whichever side of the stream produced them. */
  private acceptReplyHeaders(headers: Record<string, unknown> | null | undefined): void {
    if (headers !== null && headers !== undefined) this.replyHeaders = headers;
    this.settleHeaders();
  }

  /** `replyHeadersArrived` resolves exactly once, from the answering side's first frame or the close. */
  private settleHeaders(): void {
    if (this.headersSettled) return;
    this.headersSettled = true;
    this.headersGate.resolve();
  }

  /** @internal Deliver one reassembled payload to whichever shape is consuming this stream. */
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

  /** @internal Close because of a reset - local or remote - or because the socket died. */
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
   * WSM-API-016's other half: a failure with nobody to hand it to goes to the peer's hook.
   *
   * A stream that was awaited or iterated has a consumer, and that consumer gets the failure through
   * the promise or the iterator; anything else - a stream nobody took, or the fire-and-forget stream
   * behind `notify()`, which has no handle at all - would otherwise lose it silently.
   *
   * The hook is a diagnostic on a close path, so it is not allowed to take the close path down with
   * it if an application's handler throws.
   */
  private reportUnconsumed(error: StreamReset): void {
    if (this.claim === 'await' || this.claim === 'iterate') return;
    try {
      this.peer.reportStreamError?.(error, this);
    } catch {
      // Nothing sensible to do with it, and re-throwing here would abort a read loop.
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

  /** `closed` and `signal` move together, on every close path (WSM-API-023). */
  private markClosed(reason?: StreamReset): void {
    // A stream that closes before it was ever answered is the path WSM-API-025 cares about: no reply
    // headers are coming, so the wait for them ends here rather than never.
    this.settleHeaders();
    this.closeGate.resolve();
    if (!this.closeController.signal.aborted) this.closeController.abort(reason);
  }

  /** A stream that ends without ever producing a payload must not leave an await hanging. */
  private failAnswerIfEmpty(): void {
    this.failAnswer(new ProtocolError(`stream ${this.id} ended without producing a payload`));
  }

  /**
   * Reject the memoized promise, if there is one to reject.
   *
   * Gated on the promise having been asked for, exactly as Python gates on the future having been
   * created: a promise nobody requested must not be rejected, because a rejection nobody retrieves
   * is a warning the application did not cause and cannot act on. A late await still gets the answer
   * - `resolveFromCurrentState` reads it back off `closeCause`.
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

  /** One promise per stream, handed its answer on first use and settled exactly once (WSM-API-010/011). */
  private memoizedAnswer(): Promise<T> {
    if (!this.answerRequested) {
      this.answerRequested = true;
      this.resolveFromCurrentState();
    }
    return this.answer.promise;
  }

  /**
   * Hand the memoized promise whatever answer the stream already has.
   *
   * The first await may arrive long after the answer did - a payload, a reset, or an end with no
   * payload at all. Every one of those must settle it at once; leaving it pending is the spinner that
   * never stops which WSM-INV-011 names.
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
   * Subclassing would make species semantics construct a `Stream` for every derived promise - a
   * `stream.then(...).then(...)` chain of objects each claiming to be an addressable exchange with an
   * id, none of which is one. This returns an ordinary `Promise` instead.
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
   * Never a second source of the value: `await stream` and `await stream.result()` resolve from one
   * place, so a second read returns the first read's value rather than the next payload.
   */
  async result(options: ResultOptions = {}): Promise<T> {
    this.claimFor('await');
    return this.wait(options);
  }

  /** The one wait both `await stream` and `result()` go through. */
  private wait(options: ResultOptions = {}): Promise<T> {
    const answer = this.memoizedAnswer();
    if (options.timeoutMs === undefined && options.signal === undefined) return answer;
    return this.waitGuarded(answer, options);
  }

  /**
   * Race the memoized promise against the caller's deadline and signal.
   *
   * `Promise.race` is the natural shield: losing the race does not settle the memoized promise, so a
   * second await still gets the same answer (WSM-API-011).
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
   * WSM-ERR-014: a consumer that walked away resets the stream, rather than leaving the remote
   * producing for nobody, and the abort keeps propagating rather than being swallowed.
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
    // A `break`, a `return`, or a throw out of the loop body finalises the generator here. That is
    // the one form of "the consumer walked away" TypeScript can actually observe, so it is the one
    // WSM-ERR-014 is honoured on; `reset()` is a no-op if the stream is already closed, so a loop
    // that exited because of a reset does not send a second one.
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
