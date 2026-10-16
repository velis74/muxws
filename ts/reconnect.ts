/**
 * The reconnect helper: backoff, heartbeat and hello replay (§7).
 *
 * Mirrors `muxws/reconnect.py` structure for structure, including the order of the supervisor's
 * steps. Differences: JavaScript has no task cancellation, so every long wait is raced against a gate
 * that `stop()` opens; durations are milliseconds where Python uses seconds (§9.3).
 *
 * Dialer only. An acceptor cannot dial and MUST NOT have a reconnect helper.
 *
 * The helper's only persistent state is an attempt counter (WSM-RCN-001). The delay, the jitter and
 * whether to give up are computed from it, so the schedule is a pure function testable without a
 * clock (WSM-RCN-003).
 */

import type { Codec } from './codec';
import { ConnectionClosed, StreamTimeout } from './errors';
import { MAX_FRAME_BYTES } from './fragment';
import type { CloseReason } from './observability';
import { logger } from './observability';
import {
  checkedFrameCap,
  type ErrorSerializer,
  type IncomingHeadersInterceptor,
  type OpenInterceptor,
  Peer,
  registerInterceptors,
  type ReplyHeadersInterceptor,
  type StreamHandler,
} from './peer';
import type { Stream } from './stream';
import type { SocketAdapter } from './transports';

// --------------------------------------------------------------------------- backoff options

/** Options for `new Reconnect()`. Every field is optional and has a default. */
export interface ReconnectOptions {
  initialDelayMs?: number;
  factor?: number;
  maxDelayMs?: number;
  jitter?: number;
  maxAttempts?: number;
}

/**
 * Backoff options. Durations are milliseconds (§9.3).
 *
 * A class, not an interface, so the defaults are defined in one place and invalid values are rejected
 * in the constructor: `initialDelayMs` not greater than 0, `factor` below 1 (a shrinking schedule),
 * and `jitter` outside [0, 1].
 */
export class Reconnect {
  readonly initialDelayMs: number;

  readonly factor: number;

  readonly maxDelayMs: number;

  readonly jitter: number;

  /**
   * `Infinity` means unlimited (Python: `None`). When it is exhausted, `onClose` fires once with
   * `willRetry` false and no further dial is made (WSM-RCN-044).
   */
  readonly maxAttempts: number;

  constructor(options: ReconnectOptions = {}) {
    this.initialDelayMs = options.initialDelayMs ?? 250;
    this.factor = options.factor ?? 2;
    this.maxDelayMs = options.maxDelayMs ?? 30_000;
    this.jitter = options.jitter ?? 0.3;
    this.maxAttempts = options.maxAttempts ?? Infinity;

    // Python raises `ValueError`; here a plain `Error`, because this is an argument error, not a
    // protocol or connection failure that a muxws error class would indicate.
    if (this.initialDelayMs <= 0) throw new Error('initialDelayMs must be greater than zero');
    if (this.factor < 1) throw new Error('factor must be at least 1');
    if (!(this.jitter >= 0 && this.jitter <= 1)) throw new Error('jitter is a fraction between 0 and 1');
  }
}

/**
 * A random number in [-1, 1) for jitter. Injectable, so tests can evaluate the schedule as a pure
 * function (WSM-RCN-003).
 */
export type RandomDraw = () => number;

/**
 * Uses `Math.random`; WSM-RCN-005 excludes `crypto.getRandomValues`, because jitter is used to spread
 * out simultaneous reconnects, not to resist an adversary. The ping nonce in `lifecycle.ts` uses
 * `crypto.getRandomValues`.
 */
function uniform(): number {
  return Math.random() * 2 - 1;
}

/**
 * The delay before retry number `attempts`, jittered.
 *
 *     delay = min(initialDelayMs * factor ** attempts, maxDelayMs)
 *     delay = delay * (1 + uniform(-jitter, +jitter))
 *
 * Jitter is applied to every computed delay, including capped ones (WSM-RCN-002), so peers that lost
 * their sockets at the same moment do not all retry at the same moment. The result is never negative.
 */
export function backoffDelay(attempts: number, options: Reconnect, draw: RandomDraw = uniform): number {
  if (attempts < 0) throw new Error('attempts cannot be negative');
  const base = unjitteredDelay(attempts, options);
  return Math.max(0, base * (1 + options.jitter * draw()));
}

/** The delay after the cap and before jitter: `min(initialDelayMs * factor ** attempts, maxDelayMs)`. */
export function unjitteredDelay(attempts: number, options: Reconnect): number {
  return Math.min(options.initialDelayMs * options.factor ** attempts, options.maxDelayMs);
}

/** False once `maxAttempts` is exhausted (WSM-RCN-044). */
export function shouldRetry(attempts: number, options: Reconnect): boolean {
  return attempts < options.maxAttempts;
}

/**
 * The helper's only persistent state (WSM-RCN-001).
 *
 * It resets only when the connection is established: the socket is open with the subprotocol
 * accepted (WSM-CON-030) and the hello has been acknowledged (WSM-RCN-004). Resetting when the socket
 * opens would turn exponential backoff into fixed-interval retries against a server that accepts
 * sockets while its backend is down (WSM-INV-012).
 */
export class AttemptCounter {
  private attempts = 0;

  get value(): number {
    return this.attempts;
  }

  failed(): number {
    this.attempts += 1;
    return this.attempts;
  }

  /** Called at one point only: after the subprotocol is accepted and the hello is acknowledged. */
  established(): void {
    this.attempts = 0;
  }
}

// --------------------------------------------------------------------------- the hello

/**
 * How long the hello exchange may take before the attempt fails (WSM-RCN-026), in milliseconds.
 * Python: `timeout=10.0`.
 */
export const DEFAULT_HELLO_TIMEOUT_MS = 10_000;

export interface HelloOptions {
  payload?: unknown;
  headers?: Record<string, unknown> | null;
  timeoutMs?: number;
}

/**
 * The opening payload replayed on every connection this peer ever makes (WSM-RCN-020).
 *
 * Captured once, at `connect()`; it is not re-read, recomputed or supplied as a callback. It is
 * deep-copied on capture and on every read, so every replay is byte-identical (WSM-RCN-027) even if
 * the application mutates its original object, and no two connections send the same object.
 *
 * It is sent as an ordinary `open(payload, { headers, end: true })`, with no marker on the wire
 * (WSM-RCN-021). The acknowledgement is the acceptor's handler returning, which ends the stream
 * implicitly (WSM-RCN-022/WSM-STM-035); no application code sends it.
 *
 * A credential MUST NOT be carried here: authentication is a handshake concern (WSM-RCN-025,
 * WSM-AUT-001).
 */
export class Hello {
  readonly timeoutMs: number;

  private readonly capturedPayload: unknown;

  private readonly capturedHeaders: Record<string, unknown> | null;

  constructor(options: HelloOptions = {}) {
    // `structuredClone` is the platform deep copy. It throws for values such as functions, which no
    // codec can encode, so such a hello fails at `connect()` instead of on a later replay.
    this.capturedPayload = deepCopy(options.payload ?? null);
    this.capturedHeaders = options.headers == null ? null : deepCopy(options.headers);
    this.timeoutMs = options.timeoutMs ?? DEFAULT_HELLO_TIMEOUT_MS;
  }

  /**
   * False when neither payload nor headers were given: no hello is sent, and the connection is
   * established when the socket opens (WSM-RCN-024).
   */
  get configured(): boolean {
    return this.capturedPayload !== null || this.capturedHeaders !== null;
  }

  /** A new deep copy on each call. */
  payloadForWire(): unknown {
    return deepCopy(this.capturedPayload);
  }

  headersForWire(): Record<string, unknown> | undefined {
    return this.capturedHeaders === null ? undefined : deepCopy(this.capturedHeaders);
  }
}

/** Python's `copy.deepcopy`. Accessed through `globalThis` because it is a platform global. */
function deepCopy<T>(value: T): T {
  return globalThis.structuredClone(value);
}

/** Python's `str(exc)`. */
function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

// --------------------------------------------------------------------------- waiting

/** Python's `asyncio.sleep`, in milliseconds. Injectable, so tests do not depend on the wall clock. */
export type Sleep = (ms: number) => Promise<void>;

function realSleep(ms: number): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

/**
 * The heartbeat's sleep: a `setTimeout` that is unref'd in Node.
 *
 * A pending `await` cannot be cancelled, so when `peer.close()` opens the stop gate the heartbeat
 * loop exits while its timer is still pending, and in Node a pending timer keeps the process alive.
 * Without `unref`, a program that closes its peer would wait for the rest of the ping interval (20 s
 * by default) before exiting; the Python implementation exits immediately.
 *
 * The backoff sleep is not unref'd. While a socket is open, its handle keeps the process alive; while
 * the peer is between sockets, the backoff timer is the only handle, and the process must stay alive
 * to reconnect (WSM-RCN-006). Unref'ing it would let a client exit on its first outage.
 *
 * `unref` exists only in Node; a browser's `setTimeout` returns a number, hence the optional call.
 */
function heartbeatSleep(ms: number): Promise<void> {
  return new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, ms) as unknown as { unref?: () => void };
    timer.unref?.();
  });
}

interface Gate {
  readonly promise: Promise<void>;
  open(): void;
}

/**
 * A promise resolved by calling `open()`. It replaces Python's `task.cancel()`: a pending `await`
 * cannot be interrupted in JavaScript, so every long wait is raced against a gate, and the waiter
 * re-checks its stop flag when the race settles.
 */
function gate(): Gate {
  let open: () => void = () => undefined;
  const promise = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { promise, open };
}

// --------------------------------------------------------------------------- the heartbeat

/** WSM-RCN-010's default: a `ping` every 20 s on an otherwise idle socket. */
export const DEFAULT_PING_INTERVAL_MS = 20_000;

/**
 * WSM-RCN-011's default: 10 s for the `pong`.
 *
 * Distinct from `peer.ts`'s `DEFAULT_PING_TIMEOUT_MS`, the 5 s default of an explicit `peer.ping()`
 * (WSM-CON-012); the spec defines the two values separately.
 */
export const DEFAULT_HEARTBEAT_TIMEOUT_MS = 10_000;

export interface HeartbeatOptions {
  intervalMs: number;
  timeoutMs: number;
  sleep?: Sleep;
}

/**
 * Sends a `ping` on an idle socket and declares the socket dead when no `pong` arrives in time.
 *
 * Idleness is measured from `peer.lastActivity`, which every frame in either direction updates
 * (WSM-RCN-010); a connection with traffic sends no pings.
 *
 * Detection takes at most `intervalMs + timeoutMs` (WSM-RCN-011): the ping is sent at most one
 * interval after the last frame, and `ping()` fails after the deadline.
 */
export class Heartbeat {
  private readonly peer: Peer;

  private readonly intervalMs: number;

  private readonly timeoutMs: number;

  private readonly sleep: Sleep;

  private readonly stopGate = gate();

  private stopped = false;

  constructor(peer: Peer, options: HeartbeatOptions) {
    this.peer = peer;
    this.intervalMs = options.intervalMs;
    this.timeoutMs = options.timeoutMs;
    this.sleep = options.sleep ?? heartbeatSleep;
  }

  async run(): Promise<void> {
    // An interval of 0 or less disables the heartbeat, for deployments with their own liveness check.
    if (this.intervalMs <= 0) return;

    // Counts establishment as activity. Otherwise a peer with no traffic yet would be considered idle
    // and pinged as soon as the heartbeat starts.
    this.peer.stampActivity();

    while (!this.stopped && this.peer.isOpen) {
      const idle = this.peer.clock() - this.peer.lastActivity;
      if (idle < this.intervalMs) {
        await Promise.race([this.sleep(this.intervalMs - idle), this.stopGate.promise]);
        continue;
      }
      try {
        await this.peer.ping(this.timeoutMs);
      } catch (error) {
        if (!(error instanceof ConnectionClosed)) return;
        // Python cancels this task; here the `await` above cannot be interrupted, so the flag is
        // re-read after it settles. Without this check, a `peer.close()` whose drain outlasts one ping
        // deadline would be reported as a missed pong instead of a deliberate close, and a heartbeat
        // stopped after its socket died could close the peer's replacement socket (WSM-RCN-011/040).
        if (this.stopped) return;
        // No pong within the deadline: the socket is declared dead here, and the supervisor then
        // follows the same backoff path as for any other socket loss, so `onClose` fires through a
        // single code path (WSM-RCN-011).
        //
        // `die()` runs first, so `onClose` carries this reason and not the socket's close reason; the
        // local close follows, as WSM-RCN-011 requires, so the read loop ends and `serve()` settles.
        // `reason` is set explicitly because `ConnectionClosed` does not derive it from the message,
        // and `CloseReason.reason` is the text applications log (WSM-RCN-045).
        this.peer.die(
          new ConnectionClosed(`no pong within ${this.timeoutMs}ms`, {
            code: 1006,
            reason: `no pong within ${this.timeoutMs}ms`,
          }),
        );
        await this.peer.closeSocketLocally(1000, 'no pong');
        return;
      }
    }
  }

  /** Set the stop flag and wake the pending sleep (Python cancels the task instead). */
  stop(): void {
    this.stopped = true;
    this.stopGate.open();
  }
}

// --------------------------------------------------------------------------- the driver

/** Dial once and return an open socket, or throw. The transport-specific part of `connect()`. */
export type Dial = () => Promise<SocketAdapter>;

/**
 * How many entries `delays` keeps. `delays` is test instrumentation on an object that can live as
 * long as the process; without a bound it would grow by one entry per attempt during an outage. It is
 * a log, not state (WSM-RCN-001).
 */
const RECORDED_DELAYS = 100;

export interface ConnectionLoopOptions {
  options: Reconnect;
  hello: Hello;
  pingIntervalMs?: number;
  pingTimeoutMs?: number;
  sleep?: Sleep;
  draw?: RandomDraw;
}

/**
 * The reconnect driver. Dialer only; the constructor throws for an acceptor.
 *
 * `establish()` performs the hello on the first connection and throws on failure; `start()` then
 * starts the supervisor, and `stop()` shuts the helper down.
 */
export class ConnectionLoop {
  private readonly peer: Peer;

  private readonly dial: Dial;

  private readonly options: Reconnect;

  private readonly hello: Hello;

  private readonly pingIntervalMs: number;

  private readonly pingTimeoutMs: number;

  private readonly sleep: Sleep;

  /**
   * The injected sleep, or `undefined` when none was given.
   *
   * Kept separately from `sleep` so `startHeartbeat` passes the `Heartbeat` the caller's sleep, not
   * this loop's default. An injected fake sleep is used by both; without one, the heartbeat uses its
   * own default, which unrefs its timer (this loop's timer is not unref'd).
   */
  private readonly injectedSleep: Sleep | undefined;

  private readonly draw: RandomDraw;

  private readonly counter = new AttemptCounter();

  private readonly delaysTaken: number[] = [];

  private readonly stopGate = gate();

  private reestablished = 0;

  private stopped = false;

  private serving: Promise<void> | null = null;

  private heartbeat: Heartbeat | null = null;

  private supervisor: Promise<void> | null = null;

  constructor(peer: Peer, dial: Dial, options: ConnectionLoopOptions) {
    if (!peer.isDialer) {
      // An acceptor has no url to dial back to its remote.
      throw new Error('the reconnect helper is dialer-only; an acceptor cannot dial (§7)');
    }
    this.peer = peer;
    this.dial = dial;
    this.options = options.options;
    this.hello = options.hello;
    this.pingIntervalMs = options.pingIntervalMs ?? DEFAULT_PING_INTERVAL_MS;
    this.pingTimeoutMs = options.pingTimeoutMs ?? DEFAULT_HEARTBEAT_TIMEOUT_MS;
    this.sleep = options.sleep ?? realSleep;
    this.injectedSleep = options.sleep;
    this.draw = options.draw ?? uniform;
  }

  /** The counter's live value (WSM-RCN-001). */
  get attempts(): number {
    return this.counter.value;
  }

  /**
   * A copy of the most recent {@link RECORDED_DELAYS} backoff delays, in order.
   *
   * WSM-RCN-004's test reads this sequence. It is bounded so it does not grow without limit during a
   * long outage with no attempt cap; it is returned as a copy so callers cannot modify the record.
   */
  get delays(): readonly number[] {
    return [...this.delaysTaken];
  }

  /** How many times the connection has been re-established, excluding the first connection. */
  get reconnections(): number {
    return this.reestablished;
  }

  /**
   * Perform the hello on the first connection. The socket is already attached to the peer.
   *
   * Throws the failure (WSM-RCN-006/WSM-INV-018): `connect()` propagates it and does not return a
   * peer that retries in the background, so a wrong url, an unreachable host or a codec mismatch is
   * reported to the caller.
   */
  async establish(): Promise<void> {
    // The connection is not established until the hello is acknowledged (WSM-RCN-004). Until then
    // `isOpen` is false and `open()` throws, so no application frame precedes the hello, including
    // one from an `onStream` handler answering a stream the acceptor opened (WSM-RCN-023/043).
    this.peer.established = !this.hello.configured;
    this.startServing();
    try {
      await this.performHello();
    } catch (error) {
      // No connection was established, so `onClose` is not notified; `connect()` throws instead
      // (WSM-RCN-006), and the caller never receives the peer. The peer is marked dead here so that
      // `isOpen` does not depend on the order of the read loop and the socket close.
      //
      // The socket is closed before rethrowing. `peer.close()` is not used: there is no established
      // connection to shut down in order, and its ten-second drain window would delay the error from
      // `connect()` by ten seconds.
      this.peer.die(
        new ConnectionClosed('the first connection was never established', {
          code: 1006,
          reason: 'the first connection was never established',
        }),
        { notify: false },
      );
      await this.abandonSocket();
      throw error;
    }
    // The hello is acknowledged: `isOpen` is true and `open()` is accepted from here on
    // (WSM-RCN-004/043).
    this.peer.established = true;
    // The counter is not reset here: it is still 0, because `establish()` runs once, before any
    // failure. `redial()` is the only reset point (WSM-RCN-004, WSM-INV-012).
    //
    // `willRetry` is set when the connection is established, not when it dies, because `die()` reads
    // it to compose the `CloseReason` (WSM-RCN-040).
    this.peer.willRetry = shouldRetry(0, this.options);
  }

  /** Start the supervisor. Called only after `establish()` returned. */
  start(): void {
    // A stopped helper does not start a supervisor; it would wait on the live socket and continue
    // after the shutdown (WSM-RCN-040/044).
    if (this.supervisor !== null || this.stopped) return;
    this.supervisor = this.supervise();
    void this.supervisor.catch((error: unknown) => {
      logger.error(`muxws conn=${this.peer.id} the reconnect supervisor failed`, error);
    });
  }

  /**
   * Shut the helper down: no further dial, and the pending backoff sleep is woken.
   *
   * It does not wait for the supervisor. The supervisor normally waits in `peer.serve()` on a live
   * socket, which this method does not close; `peer.close()` calls this method before closing the
   * socket, so waiting here would deadlock. The stop state is set synchronously, and the supervisor
   * reads it when the current socket ends.
   */
  async stop(): Promise<void> {
    this.stopped = true;
    // A socket that dies after this call reports `willRetry` false; `die()` reads it from the peer
    // when it composes the `CloseReason` (WSM-RCN-040). `peer.close()` also sets it, but `stop()`
    // can be called on its own.
    this.peer.willRetry = false;
    this.stopGate.open();
    this.heartbeat?.stop();
    await Promise.resolve();
  }

  // ------------------------------------------------------------------ the supervisor

  /**
   * Serve the current socket, then back off and re-dial until told to stop.
   *
   * The numbered steps run in order: no dial starts before the previous socket's read loop has
   * finished, and `stop()` is checked before giving up.
   */
  private async supervise(): Promise<void> {
    // The first socket's heartbeat starts here, not in `establish()`, so a heartbeat runs only when
    // a supervisor exists to redial after it declares the socket dead.
    this.startHeartbeat();
    for (;;) {
      // 1. Serve the current socket until it ends. `serve()` rejects with the cause; by then the
      //    peer is dead and `onClose` has fired with the `willRetry` set at establishment.
      await this.serving?.catch(() => undefined);
      this.stopHeartbeat();

      // 2. No further dial after `stop()` (also called by `peer.close()`) or when the attempt cap is
      //    exhausted.
      if (this.stopped) return;
      if (!shouldRetry(this.counter.value, this.options)) {
        // Reached only with `maxAttempts: 0`, where `establish()` set `willRetry` false and the
        // socket loss already produced WSM-RCN-044's single report; the latch suppresses this one.
        // The branch matches `muxws/reconnect.py`.
        this.giveUp();
        return;
      }
      if (await this.redial()) continue;

      // 3. `redial` returned false: either `stop()` was called during backoff (no report needed), or
      //    the attempt cap is exhausted, which `giveUp()` reports.
      if (!this.stopped) this.giveUp();
      return;
    }
  }

  /** Back off and redial after a socket loss; true once a connection is established, false on stop or give-up. */
  private async redial(): Promise<boolean> {
    while (!this.stopped && shouldRetry(this.counter.value, this.options)) {
      const delay = backoffDelay(this.counter.value, this.options, this.draw);
      this.recordDelay(delay);
      await Promise.race([this.sleep(delay), this.stopGate.promise]);
      if (this.stopped) return false;

      let socket: SocketAdapter;
      try {
        socket = await this.dial();
      } catch (error) {
        // No `onClose`: a dial that produced no socket is not a socket loss.
        logger.debug(`muxws conn=${this.peer.id} dial attempt ${this.counter.value + 1} failed`, error);
        this.counter.failed();
        continue;
      }
      // `stop()` (and `peer.close()`) cannot cancel a dial in progress, so a socket returned after
      // `stop()` is closed here, not adopted (WSM-RCN-040).
      if (this.stopped) {
        await socket.close(1000, 'the connection was closed while dialling');
        return false;
      }

      // Set before adopting the socket, because the socket may die during the hello and `die()` reads
      // `willRetry`. For an attempt not yet established, it is whether another attempt remains after
      // this one (WSM-RCN-040/045).
      this.peer.willRetry = shouldRetry(this.counter.value + 1, this.options);
      // Not yet established: `isOpen` stays false during the hello, so no application frame precedes
      // the hello on this socket (WSM-RCN-023/043). `isOpen` requires both a socket (set by
      // `adoptSocket`) and `established`, which only the driver writes.
      this.peer.established = !this.hello.configured;
      this.peer.adoptSocket(socket);
      this.startServing();

      try {
        await this.performHello();
      } catch (error) {
        // A reset or timed-out hello is a failed attempt (WSM-RCN-026). The socket is closed, the
        // loss is reported through `die()`, the counter is incremented, and `onReconnect` does not
        // fire.
        //
        // `die()` runs before the close (the rule lists them in the other order): closing first
        // would race the read loop, which could report the socket's generic reason instead of this
        // one.
        logger.debug(`muxws conn=${this.peer.id} hello failed on attempt ${this.counter.value + 1}`, error);
        // `reason` includes the cause (WSM-RCN-045), distinguishing an expired deadline from a hello
        // the acceptor reset.
        const detail = `the hello did not complete: ${errorMessage(error)}`;
        this.peer.die(new ConnectionClosed(detail, { code: 1006, reason: detail }));
        await this.abandonSocket();
        this.counter.failed();
        continue;
      }
      // A hello acknowledged after `stop()` does not establish the connection: the socket is
      // abandoned, `isOpen` stays false and `onReconnect` does not fire (WSM-RCN-040).
      if (this.stopped) {
        await this.abandonSocket();
        return false;
      }

      // The only point where the counter resets (WSM-RCN-004/WSM-INV-012). Resetting after `dial()`
      // returns would turn exponential backoff into fixed-interval retries against a server that
      // accepts sockets while its backend is down.
      this.counter.established();
      // At the same point the connection becomes established: `isOpen` is true and `open()` is
      // accepted (WSM-RCN-004/043).
      this.peer.established = true;
      this.reestablished += 1;
      this.peer.willRetry = shouldRetry(0, this.options);
      this.peer.fireReconnect(this.reestablished);
      this.startHeartbeat();
      return true;
    }
    return false;
  }

  /**
   * `maxAttempts` is exhausted: fire `onClose` once with `willRetry` false; no further dial is made
   * (WSM-RCN-044).
   *
   * Uses `notifyClose`, not `die()`: there is no socket, and the last socket loss was already
   * reported. This report tells the application that the helper has stopped retrying.
   */
  private giveUp(): void {
    this.peer.willRetry = false;
    this.peer.notifyClose({
      code: 1006,
      reason: `gave up reconnecting after ${this.options.maxAttempts} attempts`,
      wasClean: false,
      willRetry: false,
    });
  }

  // ------------------------------------------------------------------ the pieces

  /** Keep the newest `RECORDED_DELAYS` entries (Python: `deque(maxlen=...)`). */
  private recordDelay(delay: number): void {
    this.delaysTaken.push(delay);
    if (this.delaysTaken.length > RECORDED_DELAYS) {
      this.delaysTaken.splice(0, this.delaysTaken.length - RECORDED_DELAYS);
    }
  }

  /** The read loop for the current socket. Never awaited here; the supervisor awaits it. */
  private startServing(): void {
    this.serving = this.peer.serve();
    // Attached immediately, so a socket that dies before the supervisor awaits `serving` does not
    // cause an unhandled rejection.
    void this.serving.catch(() => undefined);
  }

  /**
   * Send the hello and wait for the acceptor's handler to end it (WSM-RCN-022), under
   * `hello.timeoutMs`. Does nothing when no hello is configured (WSM-RCN-024).
   *
   * The hello is an ordinary `open()` (WSM-RCN-021) and is sent before any application frame and
   * before `onReconnect` (WSM-RCN-023). The peer is not established during this window, so `open()`
   * throws for application code; this method calls `allocateAndEnqueue` directly. `fireReconnect`
   * runs only after this method returns.
   */
  private async performHello(): Promise<void> {
    if (!this.hello.configured) return;

    const stream = this.peer.allocateAndEnqueue(this.hello.payloadForWire(), {
      headers: this.hello.headersForWire(),
      end: true,
    });
    const acknowledged = drain(stream);
    // A no-op handler in addition to the race below: when the deadline wins, `acknowledged` still
    // rejects when the socket closes, and would otherwise be an unhandled rejection.
    void acknowledged.catch(() => undefined);

    let timer: ReturnType<typeof setTimeout> | undefined;
    const deadline = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => {
        reject(new StreamTimeout(`the hello was not acknowledged within ${this.hello.timeoutMs}ms`));
      }, this.hello.timeoutMs);
    });
    try {
      await Promise.race([acknowledged, deadline]);
    } finally {
      clearTimeout(timer);
    }
  }

  private startHeartbeat(): void {
    this.heartbeat = new Heartbeat(this.peer, {
      intervalMs: this.pingIntervalMs,
      timeoutMs: this.pingTimeoutMs,
      sleep: this.injectedSleep,
    });
    void this.heartbeat.run().catch((error: unknown) => {
      logger.error(`muxws conn=${this.peer.id} the heartbeat failed`, error);
    });
  }

  private stopHeartbeat(): void {
    this.heartbeat?.stop();
    this.heartbeat = null;
  }

  /**
   * Close this socket and wait for its read loop to finish before the next attempt.
   *
   * `serve()` tears down the peer's write side in its `finally`. If the read loop is still unwinding
   * after the next attempt has called `adoptSocket()`, it tears down the new socket's writer; the
   * replayed hello is then never sent and every remaining attempt times out. `_abandon_socket` in
   * `muxws/reconnect.py` waits for the same reason.
   *
   * The close code sent is 1000, not 1006 (see `peer.closeSocketLocally`); the peer reports 1006 for
   * this loss, but 1006 is never sent.
   */
  private async abandonSocket(): Promise<void> {
    // Closed through the peer, which holds the current socket; a separately stored handle could be
    // stale after an `adoptSocket`. `muxws/reconnect.py` closes `peer._socket` likewise.
    await this.peer.closeSocketLocally(1000, 'hello failed');
    await this.serving?.catch(() => undefined);
    this.serving = null;
  }
}

/**
 * Wait for the remote to end the stream, and throw if it resets it instead.
 *
 * Iterates instead of `await stream`: `await stream` rejects with `ProtocolError` on a stream that
 * ends without a payload, and a hello acknowledgement is such a stream (the acceptor's handler
 * returns and the stream ends implicitly, WSM-RCN-022/WSM-STM-035). Iteration ends normally in that
 * case and throws the `StreamReset` when the hello was refused.
 */
async function drain(stream: Stream): Promise<void> {
  for await (const payload of stream) {
    // An acceptor may send payloads on the hello stream; muxws does not interpret them
    // (WSM-RCN-021) and discards them.
    void payload;
  }
}

// --------------------------------------------------------------------------- connect()

/**
 * `connect()`'s options.
 *
 * `headers` is node-only and defined on `NodeConnectOptions` in `ts/node.ts`, because a browser
 * cannot set request headers on a WebSocket handshake.
 */
export interface ConnectOptions {
  /** Replayed verbatim on every connection this peer ever makes (WSM-RCN-020). */
  hello?: unknown;
  helloHeaders?: Record<string, unknown>;
  reconnect?: Reconnect;
  pingIntervalMs?: number;
  pingTimeoutMs?: number;
  helloTimeoutMs?: number;
  codec?: Codec;
  onStream?: StreamHandler;
  onClose?: (reason: CloseReason) => void;
  onReconnect?: (attempt: number, peer: Peer) => void;
  /** Registered before the hello, in order, as `peer.interceptOpen()` would (WSM-API-026). */
  openInterceptors?: readonly OpenInterceptor[];
  /** Registered before the hello, in order, as `peer.interceptReplyHeaders()` would (WSM-API-026). */
  replyHeadersInterceptors?: readonly ReplyHeadersInterceptor[];
  /** Registered before the hello, in order, as `peer.interceptIncomingHeaders()` would (WSM-API-026). */
  incomingHeadersInterceptors?: readonly IncomingHeadersInterceptor[];
  /** Appended **after** the muxws entry in the subprotocol offer (WSM-CDC-020/021). */
  subprotocols?: readonly string[];
  errorSerializer?: ErrorSerializer;
  maxPayloadBytes?: number;
  maxConcurrentStreams?: number;
  maxFrameBytes?: number;
}

/**
 * @internal The transport-agnostic half of `connect()`.
 *
 * Both entry points call this: `ts/index.ts` with `BrowserSocket`, `ts/node.ts` with `WsSocket`. Only
 * the dial closure differs (WSM-API-021), so the reconnect driver redials without transport-specific
 * code.
 *
 * The first `dial()` happens here and is not retried (WSM-RCN-006): a hello that fails on it throws
 * instead of starting a background retry.
 *
 * The frame cap (WSM-FRG-034) and the hello copy (`structuredClone`) are checked before the dial, so
 * an unusable `maxFrameBytes` or an uncloneable hello rejects without opening a socket. Any other
 * failure after the dial closes the socket before it propagates.
 */
export async function dialAndEstablish(dial: Dial, options: ConnectOptions, codec: Codec): Promise<Peer> {
  checkedFrameCap(options.maxFrameBytes ?? MAX_FRAME_BYTES, codec);
  const hello = new Hello({
    payload: options.hello,
    headers: options.helloHeaders ?? null,
    timeoutMs: options.helloTimeoutMs,
  });

  const socket = await dial();
  let peer: Peer;
  let loop: ConnectionLoop;
  try {
    peer = new Peer(socket, {
      codec,
      isDialer: true,
      errorSerializer: options.errorSerializer,
      maxFrameBytes: options.maxFrameBytes,
      maxPayloadBytes: options.maxPayloadBytes,
      maxConcurrentStreams: options.maxConcurrentStreams,
    });
    // Registered before the loop runs, so a stream the acceptor opens during the first hello has a
    // handler. `onClose` and `onReconnect` are registered after the first connection is established.
    if (options.onStream !== undefined) peer.onStream(options.onStream);
    registerInterceptors(peer, options);

    loop = new ConnectionLoop(peer, dial, {
      options: options.reconnect ?? new Reconnect(),
      hello,
      pingIntervalMs: options.pingIntervalMs,
      pingTimeoutMs: options.pingTimeoutMs,
    });
  } catch (error) {
    // No peer owns the socket yet; `establish()` closes it on its own failures.
    await socket.close();
    throw error;
  }

  // WSM-RCN-006: a first-connection failure reaches the caller unchanged, and no peer is returned.
  // `establish()` closes the socket before throwing.
  await loop.establish();

  // Registered after the first connection is established, so a failed `connect()` is reported only
  // through the exception, never through `onClose`
  // (`muxws/reconnect_test.py::test_first_attempt_failure_raises_with_unlimited_retries_configured`
  // asserts the same in Python). The two registrations and `start()` run with no await between
  // them, before the supervisor exists, so no event is missed.
  if (options.onClose !== undefined) peer.onClose(options.onClose);
  if (options.onReconnect !== undefined) peer.onReconnect(options.onReconnect);

  loop.start();
  // Stored on the peer so `peer.close()` can stop the loop.
  peer.connectionLoop = loop;
  return peer;
}
