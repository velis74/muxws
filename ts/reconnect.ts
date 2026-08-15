/**
 * The reconnect helper: backoff, heartbeat and hello replay (§7).
 *
 * A port of `muxws/reconnect.py`, structure for structure, including the ordering of the supervisor.
 * Everything that differs does so because JavaScript differs: there is no task cancellation, so every
 * long wait is raced against a gate the stopper opens, and durations are **milliseconds** where
 * Python's are seconds (§9.3).
 *
 * **Dialer only.** An acceptor cannot dial and MUST NOT have one.
 *
 * The helper's entire persistent state is an attempt counter (WSM-RCN-001). Everything else - the
 * delay, the jitter, whether to give up - is computed from it, which is what makes the schedule a
 * pure function that can be tested without a clock (WSM-RCN-003).
 */

import type { Codec } from './codec';
import { ConnectionClosed, StreamTimeout } from './errors';
import type { CloseReason } from './observability';
import { logger } from './observability';
import { type ErrorSerializer, Peer, type StreamHandler } from './peer';
import type { Stream } from './stream';
import type { SocketAdapter } from './transports';

// --------------------------------------------------------------------------- backoff options

/** The shape a caller may pass. Every field has a default, so `new Reconnect()` is the whole API. */
export interface ReconnectOptions {
  initialDelayMs?: number;
  factor?: number;
  maxDelayMs?: number;
  jitter?: number;
  maxAttempts?: number;
}

/**
 * Backoff options. Milliseconds as numbers, because this is the TypeScript port (§9.3).
 *
 * A class rather than a bare interface so the defaults live in one place and the three impossible
 * configurations are rejected where they are written rather than where they first misbehave - a
 * `factor` below 1 is a schedule that shrinks, and it would read as a flaky server for a week.
 */
export class Reconnect {
  readonly initialDelayMs: number;

  readonly factor: number;

  readonly maxDelayMs: number;

  readonly jitter: number;

  /**
   * `Infinity` means unlimited - Python spells the same thing `None`. Exhausting it fires `onClose`
   * once with `willRetry` false and never dials again (WSM-RCN-044).
   */
  readonly maxAttempts: number;

  constructor(options: ReconnectOptions = {}) {
    this.initialDelayMs = options.initialDelayMs ?? 250;
    this.factor = options.factor ?? 2;
    this.maxDelayMs = options.maxDelayMs ?? 30_000;
    this.jitter = options.jitter ?? 0.3;
    this.maxAttempts = options.maxAttempts ?? Infinity;

    // Python raises `ValueError`; `Error` is the closest thing TypeScript has to one, and a muxws
    // error class would claim this is a protocol or connection failure, which it is not.
    if (this.initialDelayMs <= 0) throw new Error('initialDelayMs must be greater than zero');
    if (this.factor < 1) throw new Error('factor must be at least 1');
    if (!(this.jitter >= 0 && this.jitter <= 1)) throw new Error('jitter is a fraction between 0 and 1');
  }
}

/**
 * The draw a schedule needs, in [-1, 1). Injected so the schedule can be tested as the pure function
 * WSM-RCN-003 says it is, rather than sampled and hoped about.
 */
export type RandomDraw = () => number;

/**
 * `Math.random`, deliberately, with `crypto.getRandomValues` ruled out by WSM-RCN-005: reconnect
 * jitter exists to disperse a thundering herd, not to resist an adversary. The ping nonce in
 * `lifecycle.ts` is the opposite case and does use one.
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
 * Jitter is applied to **every** computed delay, including the capped ones (WSM-RCN-002). Without it,
 * N peers whose sockets died at the same instant retry at the same instant, and a server coming back
 * up is knocked over by the reconnection rather than by the load.
 */
export function backoffDelay(attempts: number, options: Reconnect, draw: RandomDraw = uniform): number {
  if (attempts < 0) throw new Error('attempts cannot be negative');
  const base = unjitteredDelay(attempts, options);
  return Math.max(0, base * (1 + options.jitter * draw()));
}

/** The schedule before jitter and after the cap - the half that is exactly predictable. */
export function unjitteredDelay(attempts: number, options: Reconnect): number {
  return Math.min(options.initialDelayMs * options.factor ** attempts, options.maxDelayMs);
}

/** False once `maxAttempts` is exhausted (WSM-RCN-044). */
export function shouldRetry(attempts: number, options: Reconnect): boolean {
  return attempts < options.maxAttempts;
}

/**
 * The helper's entire persistent state (WSM-RCN-001).
 *
 * It resets **only when the connection is established**, where established means both of: the socket
 * is open with the subprotocol accepted (WSM-CON-030), and the hello has been acknowledged
 * (WSM-RCN-004). Resetting on socket-open instead silently converts exponential backoff into a
 * fixed-interval hammer against a server that accepts sockets while its backend is down
 * (WSM-INV-012).
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

  /** Called at exactly one point: after the subprotocol AND after the hello acknowledgement. */
  established(): void {
    this.attempts = 0;
  }
}

// --------------------------------------------------------------------------- the hello

/**
 * How long the hello exchange may take before the attempt is failed (WSM-RCN-026), in milliseconds -
 * Python's `timeout=10.0`.
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
 * Captured **once**, at `connect()`, and never re-read, recomputed or supplied as a callback. It is
 * deep-copied in and deep-copied out again on every read, so "byte-identical on every replay"
 * (WSM-RCN-027) is true by construction rather than by hoping the application did not mutate its own
 * object after handing it over - and so that the copy one connection handed to the codec cannot be
 * the object the next connection sends.
 *
 * It goes out as an ordinary `open(payload, { headers, end: true })` and nothing marks it on the wire
 * (WSM-RCN-021). The acknowledgement is the acceptor's handler returning, which ends the stream
 * implicitly (WSM-RCN-022/WSM-STM-035) - no application code is required to send one.
 *
 * A credential MUST NOT be carried here: authentication is a handshake concern (WSM-RCN-025,
 * WSM-AUT-001).
 */
export class Hello {
  readonly timeoutMs: number;

  private readonly capturedPayload: unknown;

  private readonly capturedHeaders: Record<string, unknown> | null;

  constructor(options: HelloOptions = {}) {
    // `structuredClone` rather than a hand-rolled walk: it is the platform's own deep copy, and it
    // throws on a value no codec could have encoded anyway - which surfaces at `connect()`, where the
    // application can see it, instead of as an unreplayable hello three hours into a reconnect storm.
    this.capturedPayload = deepCopy(options.payload ?? null);
    this.capturedHeaders = options.headers == null ? null : deepCopy(options.headers);
    this.timeoutMs = options.timeoutMs ?? DEFAULT_HELLO_TIMEOUT_MS;
  }

  /** A peer given no hello sends none, and is established as soon as the socket is (WSM-RCN-024). */
  get configured(): boolean {
    return this.capturedPayload !== null || this.capturedHeaders !== null;
  }

  /** A **fresh** copy each call. The one handed to a previous send is never handed out twice. */
  payloadForWire(): unknown {
    return deepCopy(this.capturedPayload);
  }

  headersForWire(): Record<string, unknown> | undefined {
    return this.capturedHeaders === null ? undefined : deepCopy(this.capturedHeaders);
  }
}

/** Python's `copy.deepcopy`. Reached through `globalThis` because it is a platform global. */
function deepCopy<T>(value: T): T {
  return globalThis.structuredClone(value);
}

/** Python's `str(exc)`, which is what an f-string interpolating a cause produces. */
function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

// --------------------------------------------------------------------------- waiting

/** Python's `asyncio.sleep`, in milliseconds. Injected everywhere so tests need no wall clock. */
export type Sleep = (ms: number) => Promise<void>;

function realSleep(ms: number): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

/**
 * The heartbeat's sleep: the same timer, released from Node's event loop.
 *
 * JavaScript cannot cancel a pending `await`, so when `peer.close()` opens the stop gate the
 * heartbeat's loop exits while its `setTimeout` keeps running - and in Node a pending timer holds the
 * whole process open. Without the unref a program that closes its peer and has nothing else to do
 * sits out the rest of the ping interval - twenty seconds on the default - before it exits, where the
 * Python twin returns at once.
 *
 * Only the heartbeat unrefs, never the backoff sleep. While a socket is open the socket's own handle
 * keeps the loop alive, so releasing this timer costs nothing; while the peer is *between* sockets
 * there is no socket handle and the backoff timer is the only thing keeping the program running -
 * which is correct, because the program is waiting to reconnect (WSM-RCN-006). Unref'ing that one
 * would make a client exit silently on its first outage.
 *
 * `unref` is Node's, and a browser's `setTimeout` returns a number that has no such method - hence
 * the optional call rather than a cast.
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
 * A promise somebody else resolves. This is what `task.cancel()` buys Python: there is no way to
 * interrupt a pending `await` in JavaScript, so every long wait is raced against one of these and the
 * waiter re-checks its flag when the race settles.
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
 * Not `peer.ts`'s `DEFAULT_PING_TIMEOUT_MS`, which is the 5 s an explicit `peer.ping()` waits
 * (WSM-CON-012). The two numbers answer different questions and the spec gives them different values.
 */
export const DEFAULT_HEARTBEAT_TIMEOUT_MS = 10_000;

export interface HeartbeatOptions {
  intervalMs: number;
  timeoutMs: number;
  sleep?: Sleep;
}

/**
 * A `ping` on an **idle** socket, and a bounded verdict when no `pong` comes back.
 *
 * Idle means idle (WSM-RCN-010): the timer is `peer.lastActivity`, stamped by every frame in either
 * direction, not a fixed schedule - otherwise a busy connection spends a ping every interval to learn
 * what its own traffic already proved.
 *
 * Detection is therefore bounded by `intervalMs + timeoutMs` (WSM-RCN-011): the ping goes out at most
 * one interval after the last frame, and `ping()` gives up after the deadline.
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
    // Disabled, and disabled at once: a heartbeat is a bounded detection time, not a requirement,
    // and a deployment with its own liveness signal is allowed to turn it off.
    if (this.intervalMs <= 0) return;

    // The socket has just been established, and that is the most recent thing that happened on it.
    // Without this stamp an unused peer reads as infinitely idle and gets a ping the instant the
    // heartbeat starts, which is the opposite of what "idle for an interval" means.
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
        // Python cancels this task, so a stopped heartbeat cannot reach the line below; JavaScript
        // cannot interrupt the `await` above, so the flag has to be re-read after it settles. Without
        // this check a `peer.close()` whose drain window outlives one ping deadline is reported to the
        // application as a swallowed pong rather than as the deliberate close it was - and a heartbeat
        // stopped because its socket already died would close whatever socket the peer holds *now*,
        // which by then can be the replacement (WSM-RCN-011/040).
        if (this.stopped) return;
        // No pong within the deadline. The socket is declared dead **here**, and the supervisor then
        // takes the same backoff path as a clean close - there must be no second code path for a dead
        // socket, or the two drift and only one of them fires `onClose` (WSM-RCN-011).
        //
        // `die()` first, so `onClose` carries this reason rather than whatever the socket says on the
        // way out; the local close second, because WSM-RCN-011 asks for both and because without it
        // the read loop never learns and `serve()` never settles.
        // `reason` as well as the message: `CloseReason.reason` is the text an application logs
        // (WSM-RCN-045), and `ConnectionClosed` does not derive one from its message - a close that
        // arrived with an empty `reason` would tell an operator only that something ended.
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

  /** Python cancels the task; JavaScript cannot, so the loop is told and the pending sleep woken. */
  stop(): void {
    this.stopped = true;
    this.stopGate.open();
  }
}

// --------------------------------------------------------------------------- the driver

/** Dial once and return an open socket, or throw. The transport half of `connect()`. */
export type Dial = () => Promise<SocketAdapter>;

/**
 * How many entries `delays` keeps. Test instrumentation, and bounded because of it: the object it
 * hangs off lives as long as the process, and an unbounded record would grow one entry per attempt
 * for as long as a server stays down (WSM-RCN-001 - the counter is the state; this is a log).
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
 * The reconnect driver. **Dialer only** - constructing one on an acceptor throws.
 *
 * `establish()` does the hello half of the first connection and raises whatever went wrong;
 * `start()` then supervises, and `stop()` is the deliberate shutdown.
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
   * Kept separately from `sleep` so `startHeartbeat` can hand the `Heartbeat` what the caller
   * actually passed rather than this loop's resolved default. A test that injects a fake clock
   * still gets it in both places; a caller that injected nothing lets the heartbeat pick its own
   * default, which unrefs its timer where this loop's must not.
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
      // An acceptor has no url to dial and no idea who its remote was. A helper here would be a
      // server trying to call its clients back, which is not what this protocol does.
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
   * The most recent {@link RECORDED_DELAYS} delays actually waited, in order, as a **copy**.
   *
   * This is the sequence WSM-RCN-004's test reads, and it is instrumentation on an object that lives
   * as long as the process: against a server that is down for a week with no attempt cap it would
   * otherwise grow without bound, which is a memory leak in the one component whose job is to
   * survive a long outage. The copy is so a caller reading it cannot edit the record.
   */
  get delays(): readonly number[] {
    return [...this.delaysTaken];
  }

  /** How many times the connection has been **re**-established. The first one does not count. */
  get reconnections(): number {
    return this.reestablished;
  }

  /**
   * The hello half of the FIRST connection. The socket is already on the peer.
   *
   * Throws whatever went wrong (WSM-RCN-006/WSM-INV-018): `connect()` does not swallow it and does
   * not return a peer that is retrying in the background. A typo in the URL, an unreachable host or a
   * codec mismatch that surfaced nowhere would leave the application holding a peer that looks alive
   * and retries forever against something that will never answer.
   */
  async establish(): Promise<void> {
    // The socket is open; the connection is not, until the hello comes back (WSM-RCN-004). `isOpen`
    // is false for that window and `open()` refuses in it, so the hello cannot be preceded by an
    // application frame - not even by one sent from an `onStream` handler answering a stream the
    // acceptor pushed at the hello itself (WSM-RCN-023/043).
    this.peer.established = !this.hello.configured;
    this.startServing();
    try {
      await this.performHello();
    } catch (error) {
      // Nothing was established, so there is no loss to report - `connect()` throwing is the report
      // (WSM-RCN-006), and the caller never got the peer, so there is nobody who could have attached
      // an `onClose` to hear a second one. The peer is still marked dead deterministically: leaving
      // `isOpen` to a race between the read loop and the socket close is how a peer whose hello
      // failed reads as alive.
      //
      // The socket is ours and must not be left open behind the exception either. `peer.close()` is
      // the wrong tool for it: a connection that never established has no orderly shutdown to
      // perform, and its ten-second drain window would turn a hello nobody answered into a
      // `connect()` that takes ten seconds to say so.
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
    // Acknowledged, so the socket is now a connection: `isOpen` is true and `open()` is accepted
    // from here on (WSM-RCN-004/043).
    this.peer.established = true;
    // The counter is **not** reset here. It is at its initial value already - `establish()` runs
    // once, before anything can have failed - and a second reset call site is exactly what
    // WSM-INV-012 warns about: the two would mask each other, and backoff would flatten into a
    // fixed-interval hammer. `redial()` is the one place a connection becomes established
    // (WSM-RCN-004).
    //
    // Set as soon as the connection is established and never at death: by then `die()` has already
    // composed the `CloseReason` the application will read (WSM-RCN-040).
    this.peer.willRetry = shouldRetry(0, this.options);
  }

  /** Start the supervisor. Called only after `establish()` returned. */
  start(): void {
    // A helper already told to stop must not acquire a supervisor afterwards: it would park on the
    // live socket and outlive the shutdown that was supposed to have ended it (WSM-RCN-040/044).
    if (this.supervisor !== null || this.stopped) return;
    this.supervisor = this.supervise();
    void this.supervisor.catch((error: unknown) => {
      logger.error(`muxws conn=${this.peer.id} the reconnect supervisor failed`, error);
    });
  }

  /**
   * Deliberate shutdown: no further dial, and the pending sleep is cancelled.
   *
   * It does **not** wait for the supervisor. Python cancels the task; JavaScript cannot, and the
   * supervisor is normally parked inside `peer.serve()` on a socket that is still alive - a socket
   * this method deliberately does not close, because a deliberate shutdown goes through
   * `peer.close()`, which calls this *before* closing it. Waiting here would be waiting for the very
   * close that is waiting for this call. What matters is set synchronously below, so the supervisor
   * sees it the moment the current socket ends, whenever that is.
   */
  async stop(): Promise<void> {
    this.stopped = true;
    // Not only `stopped`: a socket that dies after this call must report `willRetry` false, and the
    // peer is what `die()` reads when it composes the `CloseReason` (WSM-RCN-040). `peer.close()`
    // sets it before calling here, but `stop()` is public and is the deliberate-shutdown seam in its
    // own right - an application told a retry was coming that nobody intends to make waits forever.
    this.peer.willRetry = false;
    this.stopGate.open();
    this.heartbeat?.stop();
    await Promise.resolve();
  }

  // ------------------------------------------------------------------ the supervisor

  /**
   * Serve the current socket, then back off and re-dial until told to stop.
   *
   * The numbered steps below are ordered, and the order is the rule: nothing dials before the
   * previous socket's read loop has finished, and nothing gives up before a deliberate `stop()` has
   * been checked for.
   */
  private async supervise(): Promise<void> {
    // The first socket's heartbeat starts here rather than in `establish()`: a caller that
    // establishes and never supervises would otherwise be left with a heartbeat that can declare the
    // socket dead with nobody to back off and re-dial - a peer that closes itself and stays closed.
    this.startHeartbeat();
    for (;;) {
      // 1. Serve the current socket until it dies. `serve()` rejects with whatever killed it; the
      //    peer is already dead by then and `onClose` has already fired with the `willRetry` set
      //    when this socket was established.
      await this.serving?.catch(() => undefined);
      this.stopHeartbeat();

      // 2. A deliberate `stop()` - which `peer.close()` also calls - never dials again, and neither
      //    does a helper whose attempt cap is already spent.
      if (this.stopped) return;
      if (!shouldRetry(this.counter.value, this.options)) {
        // Reached only at `maxAttempts: 0`, where `establish()` has already set `willRetry` false
        // and the loss itself latched WSM-RCN-044's single report. No test fails if this call is
        // deleted - the latch suppresses its report - so it is held in place only by
        // `muxws/reconnect.py` carrying the same branch.
        this.giveUp();
        return;
      }
      if (await this.redial()) continue;

      // 3. `redial` gave up. Either it was stopped mid-backoff - in which case the application asked
      //    for this and has already been told - or the attempt cap ran out, which has not been
      //    reported anywhere yet.
      if (!this.stopped) this.giveUp();
      return;
    }
  }

  /** One socket loss, backed off until a connection is established or the helper gives up. */
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
        // **No `onClose` here.** A dial that never produced a socket is not a socket loss, and
        // reporting one would tell the application a connection ended that never began.
        logger.debug(`muxws conn=${this.peer.id} dial attempt ${this.counter.value + 1} failed`, error);
        this.counter.failed();
        continue;
      }

      // Before the socket, because this socket may die during the hello and `die()` reads it: what
      // `CloseReason.willRetry` has to say is whether the helper intends to dial *again*, which for
      // an attempt that has not established yet is whether one more attempt is left after this one
      // (WSM-RCN-040/045).
      this.peer.willRetry = shouldRetry(this.counter.value + 1, this.options);
      // A socket, not yet a connection: `isOpen` stays false across the hello window, so nothing the
      // application sends can precede the hello on this socket (WSM-RCN-023/043). `adoptSocket` sets
      // the socket half true; this is the other half, and only the driver ever writes it.
      this.peer.established = !this.hello.configured;
      this.peer.adoptSocket(socket);
      this.startServing();

      try {
        await this.performHello();
      } catch (error) {
        // A reset hello or a timed-out one is a **failed attempt** (WSM-RCN-026), not a connection.
        // The socket is closed, the loss is reported through the one death path, the counter is
        // incremented and `onReconnect` does **not** fire.
        //
        // `die()` before the close, though the rule lists them the other way round: closing first
        // races the read loop, which would then report the socket's own generic reason instead of
        // this one, and which of the two an application sees would depend on the scheduler.
        logger.debug(`muxws conn=${this.peer.id} hello failed on attempt ${this.counter.value + 1}`, error);
        // The cause, not only the shape: `reason` is part of the type an application reads
        // (WSM-RCN-045), and an operator looking at it wants to know *which* hello failure this was -
        // a deadline that expired and a hello the acceptor refused are two different outages.
        const detail = `the hello did not complete: ${errorMessage(error)}`;
        this.peer.die(new ConnectionClosed(detail, { code: 1006, reason: detail }));
        await this.abandonSocket();
        this.counter.failed();
        continue;
      }

      // Only now. This is the single point where the counter resets (WSM-RCN-004/WSM-INV-012):
      // resetting it after `dial()` returned is the bug the rule exists to forbid, and it turns
      // exponential backoff into a fixed-interval hammer against a server that accepts sockets while
      // its backend is down.
      this.counter.established();
      // The same point, and deliberately the same statement block: the hello is acknowledged, so
      // this socket is a connection - `isOpen` true, `open()` accepted - and the counter resets.
      // Two rules, one instant (WSM-RCN-004/043).
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
   * `maxAttempts` exhausted: fire `onClose` **once** with `willRetry` false, and never dial again
   * (WSM-RCN-044).
   *
   * `notifyClose` rather than `die()`: there is no socket to kill - the last one died attempts ago
   * and reported itself then. This close says the *helper* has stopped, which is the one thing the
   * application has not been told and the only thing that distinguishes "still trying" from "gone".
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

  /** Keep the newest `RECORDED_DELAYS` and drop the rest - Python's `deque(maxlen=...)`. */
  private recordDelay(delay: number): void {
    this.delaysTaken.push(delay);
    if (this.delaysTaken.length > RECORDED_DELAYS) {
      this.delaysTaken.splice(0, this.delaysTaken.length - RECORDED_DELAYS);
    }
  }

  /** The read loop for the current socket. Never awaited here; the supervisor awaits it. */
  private startServing(): void {
    this.serving = this.peer.serve();
    // Attached at once, so a socket that dies before the supervisor gets to it is not reported to
    // the runtime as an unhandled rejection.
    void this.serving.catch(() => undefined);
  }

  /**
   * Send the hello and wait for the acceptor's handler to end it (WSM-RCN-022), under
   * `hello.timeoutMs`. Nothing at all when no hello is configured (WSM-RCN-024).
   *
   * It is an ordinary `open()` (WSM-RCN-021) and it goes out **before any application frame and
   * before `onReconnect`** (WSM-RCN-023) - enforced rather than merely arranged: the peer is not
   * established across this window, so `open()` refuses for everyone else, and this call goes to
   * `open()`'s own body instead. `fireReconnect` is two statements below the call.
   */
  private async performHello(): Promise<void> {
    if (!this.hello.configured) return;

    const stream = this.peer.allocateAndEnqueue(this.hello.payloadForWire(), {
      headers: this.hello.headersForWire(),
      end: true,
    });
    const acknowledged = drain(stream);
    // The rejection is absorbed here as well as raced below: when the deadline wins, nothing is
    // consuming `acknowledged` any more, and the stream still fails when the socket is closed.
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
   * Give up this socket: close it, **and let its read loop finish** before the next attempt.
   *
   * The second half is not tidiness. `serve()` tears the peer's write side down in its `finally`, and
   * a read loop still unwinding when the next attempt has already called `adoptSocket()` tears down
   * the writer of the *new* socket instead - which sends nothing, so the replayed hello never leaves,
   * every remaining attempt times out, and the peer reconnects to a server it can no longer talk to.
   * `_abandon_socket` in `muxws/reconnect.py` waits for the same reason.
   *
   * The close code is 1000 and never 1006, for the reason `peer.closeSocketLocally` gives: what the
   * peer *reports* for this death is 1006; what it *sends* to end the socket is not.
   */
  private async abandonSocket(): Promise<void> {
    // Through the peer rather than through a socket handle of its own: the peer is what holds the
    // current socket, and a helper that remembered one separately could close the wrong one after an
    // `adoptSocket` it did not witness. `muxws/reconnect.py` closes `peer._socket` for the same reason.
    await this.peer.closeSocketLocally(1000, 'hello failed');
    await this.serving?.catch(() => undefined);
    this.serving = null;
  }
}

/**
 * Wait for the remote to end the stream, and throw if it resets it instead.
 *
 * Iterating rather than awaiting: `await stream` on a stream that ends without a payload rejects with
 * `ProtocolError`, which is exactly what a hello acknowledgement is - the acceptor's handler
 * returning ends the stream implicitly and sends nothing (WSM-RCN-022/WSM-STM-035). Iteration
 * terminates normally there and throws the `StreamReset` when the hello was refused.
 */
async function drain(stream: Stream): Promise<void> {
  for await (const payload of stream) {
    // A hello acknowledgement carries nothing, but an acceptor is free to answer one; muxws does not
    // interpret what comes back (WSM-RCN-021), so it is read and dropped.
    void payload;
  }
}

// --------------------------------------------------------------------------- connect()

/**
 * `connect()`'s options.
 *
 * `headers` is node-only and lives on `NodeConnectOptions` in `ts/node.ts`: a browser cannot set
 * request headers on a WebSocket handshake, and offering the field there would be a lie.
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
 * Both entry points call this: `ts/index.ts` over `BrowserSocket`, `ts/node.ts` over `WsSocket`. The
 * dial closure is the only difference between them, which is the whole point of `SocketAdapter`
 * (WSM-API-021) - and the reason the reconnect driver can dial again without knowing what a
 * WebSocket is.
 *
 * The first attempt is **awaited by the caller and never retried here** (WSM-RCN-006): `dial()` has
 * already produced `socket` before this is entered, and a hello that fails on it throws rather than
 * starting a background retry.
 */
export async function dialAndEstablish(
  socket: SocketAdapter,
  dial: Dial,
  options: ConnectOptions,
  codec: Codec,
): Promise<Peer> {
  const peer = new Peer(socket, {
    codec,
    isDialer: true,
    errorSerializer: options.errorSerializer,
    maxFrameBytes: options.maxFrameBytes,
    maxPayloadBytes: options.maxPayloadBytes,
    maxConcurrentStreams: options.maxConcurrentStreams,
  });
  // Registered **before** the loop runs, so the acceptor cannot push a stream at the first hello and
  // find nobody listening. `onClose` and `onReconnect` go on further down, once the connection stands.
  if (options.onStream !== undefined) peer.onStream(options.onStream);

  const loop = new ConnectionLoop(peer, dial, {
    options: options.reconnect ?? new Reconnect(),
    hello: new Hello({
      payload: options.hello,
      headers: options.helloHeaders ?? null,
      timeoutMs: options.helloTimeoutMs,
    }),
    pingIntervalMs: options.pingIntervalMs,
    pingTimeoutMs: options.pingTimeoutMs,
  });

  // WSM-RCN-006: whatever went wrong on the first connection reaches the caller unaltered, and no
  // peer that retries in the background is handed back. `establish()` has already closed the socket
  // it gave up on, so nothing here has to unwind it.
  await loop.establish();

  // On **after** the first connection stands, which is what makes a failed `connect()` report itself
  // exactly once, through the exception. Nothing was established, so nothing was lost, and an
  // `onClose` fired here would hand a `CloseReason` to an application for a peer it never received -
  // `muxws/reconnect_test.py::test_first_attempt_failure_raises_with_unlimited_retries_configured`
  // asserts the same silence on the Python side. Nothing can slip through the gap: the two lines
  // below run before the supervisor exists, with no await between them and `start()`.
  if (options.onClose !== undefined) peer.onClose(options.onClose);
  if (options.onReconnect !== undefined) peer.onReconnect(options.onReconnect);

  loop.start();
  // Held so nothing can collect the supervisor, and so `peer.close()` can tell it to stop.
  peer.connectionLoop = loop;
  return peer;
}
