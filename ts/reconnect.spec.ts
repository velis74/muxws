/**
 * The reconnect helper - and, more importantly, that anything calls it.
 *
 * A mirror of `muxws/reconnect_test.py`. M5a shipped a writer nothing used, so the first block below
 * goes through the **real** `connect()` against a **real** `ws` server: kill the server, watch the
 * peer re-dial, replay its hello and fire `onReconnect`. A `ConnectionLoop` with perfect unit tests
 * that `connect()` never constructs is a failed milestone, and only that block can tell the
 * difference.
 *
 * The blocks after it exercise the driver over `ts/transports/memory.ts`, which is where the ordering
 * rules of §7 are actually asserted. Durations are **milliseconds** here and seconds in Python (§9.3),
 * so the schedule the two suites assert is the same numbers scaled by a thousand.
 */

import { readFileSync } from 'node:fs';
import type { AddressInfo } from 'node:net';
import { join } from 'node:path';

import { vi } from 'vitest';
import { WebSocketServer } from 'ws';

import { JsonCodec } from './codec';
import { ConnectionClosed, ConnectionLost, ResetCode, StreamTimeout } from './errors';
import type { Frame } from './frames';
// The browser entry point registers the JSON codec (WSM-CDC-004); `ts/node.ts` deliberately does not,
// so a spec that dials over `ws` still has to bring the registration in.
import './index';
import { connect, handleProtocols, serve } from './node';
import type { CloseReason } from './observability';
import { Peer, type StreamHandler } from './peer';
import {
  AttemptCounter,
  backoffDelay,
  ConnectionLoop,
  DEFAULT_HEARTBEAT_TIMEOUT_MS,
  DEFAULT_PING_INTERVAL_MS,
  Heartbeat,
  Hello,
  Reconnect,
  type Sleep,
  dialAndEstablish,
  shouldRetry,
  unjitteredDelay,
} from './reconnect';
import { PeerRegistry } from './registry';
import type { Stream } from './stream';
import { memoryPair, type MemorySocket } from './transports/memory';

// --------------------------------------------------------------------------- the fake dialable server

/** The decoder the test itself reads the wire with, independent of any peer's codec. */
const WIRE = new JsonCodec();

function framesOn(socket: MemorySocket): Frame[] {
  return socket.sent.map((message) => WIRE.decode(message));
}

/**
 * A dialable acceptor with no socket anywhere - Python's `conftest.DialableServer`.
 *
 * Every `dial()` builds a fresh `memoryPair()`, puts an acceptor `Peer` on one end with the handler
 * the test supplied, and returns the other. It can be told to refuse the next dials, to drop the
 * current socket, or to leave the acceptor unserved so nothing ever answers a ping.
 */
class FakeServer {
  readonly acceptors: Peer[] = [];

  readonly acceptorSockets: MemorySocket[] = [];

  /** The dialer's end of each connection, which is where "byte-identical" is read from. */
  readonly dialerSockets: MemorySocket[] = [];

  handler: StreamHandler = () => undefined;

  refusals = 0;

  /** When false the acceptor is built but its read loop is never started: no pongs, no hello ack. */
  answering = true;

  dials = 0;

  dial = async (): Promise<MemorySocket> => {
    this.dials += 1;
    if (this.refusals > 0) {
      this.refusals -= 1;
      throw new Error('connection refused');
    }
    const [dialerSide, acceptorSide] = memoryPair();
    const acceptor = new Peer(acceptorSide, { codec: new JsonCodec(), isDialer: false });
    acceptor.onStream((payload, stream) => this.handler(payload, stream));
    this.acceptors.push(acceptor);
    this.acceptorSockets.push(acceptorSide);
    this.dialerSockets.push(dialerSide);
    if (this.answering) void acceptor.serve().catch(() => undefined);
    return dialerSide;
  };

  /** Socket death, from the server's side. */
  drop(): void {
    this.acceptorSockets[this.acceptorSockets.length - 1]?.drop();
  }

  /** The first message the dialer put on connection `index` - the hello, when one is configured. */
  firstMessageOn(index: number): unknown {
    return this.dialerSockets[index]?.sent[0];
  }
}

/** A sleep that takes no time and records what it was asked to wait. */
function instantSleep(taken: number[]): Sleep {
  return async (ms: number) => {
    taken.push(ms);
    await Promise.resolve();
  };
}

/** Let the microtask queue and the timer queue both settle. */
function settle(ms = 20): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

/**
 * A promise's outcome as a value: what it resolved to, or what it rejected with.
 *
 * Named for the outcome rather than `settled`, which one letter away from `settle` above would read
 * as the same helper at a glance.
 */
function outcomeOf(work: PromiseLike<unknown>): Promise<unknown> {
  return Promise.resolve(work).then(
    (value) => value,
    (error: unknown) => error,
  );
}

async function until(predicate: () => boolean, timeoutMs = 2000, what = 'the condition'): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`${what} never became true within ${timeoutMs}ms`);
    await settle(5);
  }
}

/**
 * Teardown for anything with a timer in it.
 *
 * Registered the moment it starts rather than at the end of the test body: a failing assertion skips
 * the rest of the body, and a supervisor or a heartbeat left running keeps the runner's event loop
 * alive forever - which turns one honest assertion failure into a suite that never finishes and says
 * nothing about why.
 */
const cleanups: (() => void)[] = [];

afterEach(() => {
  cleanups.splice(0, cleanups.length).forEach((stop) => stop());
});

/** A dialer over a `FakeServer`, with the loop the test is about to drive. */
function dialerOver(socket: MemorySocket): Peer {
  return new Peer(socket, { codec: new JsonCodec(), isDialer: true });
}

// --------------------------------------------------------------------------- the seam

describe('connect() is wired to the reconnect driver', () => {
  let server: WebSocketServer;
  let url: string;
  const hellos: unknown[] = [];

  beforeEach(async () => {
    hellos.length = 0;
    server = new WebSocketServer({ port: 0, handleProtocols });
    server.on('connection', (socket) => {
      void serve(socket, {
        handler: (payload) => {
          // The hello is an ordinary stream reaching an ordinary handler (WSM-RCN-021), and the
          // handler returning is the whole of the acknowledgement (WSM-RCN-022).
          hellos.push(payload);
        },
      });
    });
    await new Promise<void>((resolve) => server.once('listening', resolve));
    url = `ws://127.0.0.1:${(server.address() as AddressInfo).port}`;
  });

  afterEach(async () => {
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
  });

  it('re-dials, replays the hello and fires onReconnect when the server dies', async () => {
    const reconnects: number[] = [];
    const peer = await connect(url, {
      hello: { session: 'abc' },
      reconnect: new Reconnect({ initialDelayMs: 10, jitter: 0 }),
      onReconnect: (attempt) => reconnects.push(attempt),
    });

    await until(() => hellos.length === 1, 2000, 'the first hello');
    expect(reconnects).toEqual([]);

    // Kill every connection the server holds. Nothing else in the test touches the peer.
    server.clients.forEach((client) => client.terminate());

    await until(() => reconnects.length === 1, 2000, 'onReconnect');
    expect(hellos).toEqual([{ session: 'abc' }, { session: 'abc' }]);
    expect(peer.isOpen).toBe(true);

    await peer.close();
  });

  it('raises on the first attempt with unlimited retries configured - WSM-RCN-006/WSM-INV-018', async () => {
    // Nothing answers on this port. `reconnect` says retry forever; the FIRST attempt still throws,
    // with the underlying error, and there is no `retryInitial`-style option that would change it -
    // a caller who wants the first dial retried writes that loop where it can decide what a permanent
    // failure looks like.
    const started = Date.now();
    const caught = await connect('ws://127.0.0.1:1', { reconnect: new Reconnect({ maxAttempts: Infinity }) }).then(
      () => null,
      (error: unknown) => error,
    );

    expect(caught, 'connect() must not return a peer that is retrying in the background').toBeInstanceOf(Error);
    expect(Date.now() - started, 'and it must not back off before saying so').toBeLessThan(2000);
  });

  it('throws promptly when the first hello is never acknowledged - WSM-RCN-006/026', async () => {
    const stalled = new WebSocketServer({ port: 0, handleProtocols });
    stalled.on('connection', (socket) => {
      // A handler that never returns never ends its stream, so the hello is never acknowledged.
      void serve(socket, { handler: () => new Promise<void>(() => undefined) });
    });
    await new Promise<void>((resolve) => stalled.once('listening', resolve));
    const stalledUrl = `ws://127.0.0.1:${(stalled.address() as AddressInfo).port}`;

    const started = Date.now();
    const closes: CloseReason[] = [];
    // Not a peer that retries in the background, and not ten seconds of `close()`'s drain window
    // either: the connection never established, so there is nothing to drain.
    //
    // The **class** is pinned, not merely that something was thrown: WSM-RCN-006 asks for the
    // underlying error, and a hello deadline wrapped in a `ConnectionClosed` is not the underlying
    // error - it is a report that the connection ended, which says nothing about what went wrong.
    // `rejects.toThrow()` alone would pass against either, which is exactly what a rule about
    // *which* error reaches the caller cannot afford. `muxws/reconnect_test.py` pins the same class.
    await expect(
      connect(stalledUrl, { hello: { session: 'abc' }, helloTimeoutMs: 50, onClose: (r) => closes.push(r) }),
    ).rejects.toThrow(StreamTimeout);
    expect(Date.now() - started).toBeLessThan(1000);
    // The failure reports itself exactly once, through the exception. Nothing was established, so
    // nothing was lost - and a `CloseReason` handed to an application for a peer it never received
    // would be the second report of one failure, in a shape reserved for losing a live connection
    // (WSM-RCN-040/045). Python asserts the same silence.
    expect(closes, 'nothing was established, so nothing was lost and nothing is reported').toEqual([]);
    // And the socket it gave up on is closed rather than left open behind the exception: a `connect()`
    // that threw while leaving a live socket and its read loop behind leaks one per failed attempt,
    // and the server goes on holding a client nobody will ever speak for.
    await until(() => stalled.clients.size === 0, 2000, 'the abandoned socket closing');

    await new Promise<void>((resolve) => {
      stalled.close(() => resolve());
    });
  });

  it('re-dials on a real socket whose pong is swallowed - WSM-RCN-011', async () => {
    // The whole heartbeat path over a real socket, through the real `connect()`: a swallowed pong is
    // noticed, the socket is closed locally so `serve()` settles, the supervisor sees the loss and
    // dials again. Every other heartbeat test here drives `Heartbeat` or `ConnectionLoop` directly,
    // so deleting `startHeartbeat()` from the supervisor left them all green - a heartbeat nothing
    // starts is the M5a failure mode, one milestone later.
    //
    // No muxws peer on the server side, deliberately: `serve()` echoes a `ping` verbatim
    // (WSM-CON-010), and a server that answers is the one case the heartbeat cannot detect.
    //
    // Unlike `muxws/reconnect_test.py::test_a_swallowed_pong_on_a_real_socket_re_dials`, this cannot
    // also police the close *code*: `ws.close(1006)` throws, but it has already moved the socket to
    // CLOSING by then, so the loss still surfaces and the re-dial still happens. Python's adapter
    // leaves the socket open instead, which is why the rule is asserted there and only stated here.
    const deaf = new WebSocketServer({ port: 0, handleProtocols });
    const seen: unknown[] = [];
    deaf.on('connection', (socket) => seen.push(socket));
    await new Promise<void>((resolve) => deaf.once('listening', resolve));
    const deafUrl = `ws://127.0.0.1:${(deaf.address() as AddressInfo).port}`;

    const closes: CloseReason[] = [];
    const peer = await connect(deafUrl, {
      reconnect: new Reconnect({ initialDelayMs: 10, jitter: 0 }),
      pingIntervalMs: 50,
      pingTimeoutMs: 50,
      onClose: (reason) => closes.push(reason),
    });
    // Nothing here waits on anything TCP-shaped: the whole detection budget is interval + timeout.
    await until(() => seen.length >= 2, 3000, 'the re-dial after the swallowed pong');

    expect(closes.length, 'a swallowed pong is a socket loss and is reported as one (WSM-RCN-040)').toBeGreaterThan(0);
    expect(closes[0].reason).toBe('no pong within 50ms');
    expect(closes[0].willRetry).toBe(true);

    await peer.close();
    await new Promise<void>((resolve) => {
      deaf.close(() => resolve());
    });
  });

  it('never dials again after a deliberate close - WSM-RCN-040/044', async () => {
    const peer = await connect(url, { reconnect: new Reconnect({ initialDelayMs: 5, jitter: 0 }) });
    await peer.close();
    await settle(60);
    expect(peer.isOpen).toBe(false);
    expect(server.clients.size).toBe(0);
  });

  it('does not change the wire when the hello object is mutated after connect - WSM-RCN-020', async () => {
    // The application's own object, handed over once and then written to - which is exactly what an
    // application that reuses a config object does. The helper captured it by value at `connect()`,
    // so the replay three drops later is still the payload the server first saw.
    const payload: Record<string, unknown> = { session: 'abc', rooms: ['lobby'] };
    const peer = await connect(url, {
      hello: payload,
      reconnect: new Reconnect({ initialDelayMs: 10, jitter: 0 }),
    });
    await until(() => hellos.length === 1, 2000, 'the first hello');

    payload.session = 'changed';
    (payload.rooms as string[]).push('secret');

    server.clients.forEach((client) => client.terminate());
    await until(() => hellos.length === 2, 2000, 'the replayed hello');

    expect(hellos[1], 'the replay must be what was captured, not what the object holds now').toEqual({
      session: 'abc',
      rooms: ['lobby'],
    });
    await peer.close();
  });
});

// --------------------------------------------------------------------------- the schedule

describe('the backoff schedule', () => {
  it('doubles from 250 ms and stops at 30 s, before jitter and after the cap - WSM-RCN-003', () => {
    // A pure function of (attempts, options): no clock, no sleeping, no sampling - the whole point of
    // computing the delay separately from waiting it out.
    const options = new Reconnect();
    const schedule = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11].map((attempt) => unjitteredDelay(attempt, options));
    expect(schedule).toEqual([250, 500, 1000, 2000, 4000, 8000, 16_000, 30_000, 30_000, 30_000, 30_000, 30_000]);
  });

  it('applies jitter to every delay, including the capped ones - WSM-RCN-002', () => {
    const options = new Reconnect();
    [0, 3, 9].forEach((attempt) => {
      const base = unjitteredDelay(attempt, options);
      expect(backoffDelay(attempt, options, () => 1)).toBeCloseTo(base * 1.3);
      expect(backoffDelay(attempt, options, () => -1)).toBeCloseTo(base * 0.7);
      expect(backoffDelay(attempt, options, () => 0)).toBeCloseTo(base);
    });
  });

  it('disperses N simultaneous reconnects across the jitter window - WSM-RCN-002', () => {
    // Without this, a server coming back up is knocked over by the reconnection rather than by the
    // load: N peers whose sockets died at the same instant would retry at the same instant.
    const options = new Reconnect();
    const draws = [...Array.from({ length: 200 }).keys()].map((index) => -1 + (2 * index) / 199);
    let next = 0;
    const delays = draws.map(() => backoffDelay(0, options, () => draws[next++]));

    expect(new Set(delays).size, 'the delays must actually differ').toBeGreaterThan(190);
    expect(Math.min(...delays)).toBeGreaterThanOrEqual(250 * 0.7);
    expect(Math.max(...delays)).toBeLessThanOrEqual(250 * 1.3);
    const spread = Math.max(...delays) - Math.min(...delays);
    expect(spread, `the window is barely used: ${spread}`).toBeGreaterThan(250 * 0.5);
  });

  it('never computes a negative delay', () => {
    // A jitter of 1.0 with the worst draw lands exactly on zero, never below it.
    expect(backoffDelay(0, new Reconnect({ jitter: 1 }), () => -1)).toBe(0);
  });

  it('takes a configurable growth factor and cap', () => {
    const options = new Reconnect({ initialDelayMs: 1000, factor: 3, maxDelayMs: 10_000 });
    const schedule = [0, 1, 2, 3, 4].map((attempt) => unjitteredDelay(attempt, options));
    expect(schedule).toEqual([1000, 3000, 9000, 10_000, 10_000]);
  });

  it('is a fixed interval at a factor of one', () => {
    const options = new Reconnect({ factor: 1 });
    expect(unjitteredDelay(0, options)).toBe(250);
    expect(unjitteredDelay(9, options)).toBe(250);
  });

  it('refuses nonsense options at construction', () => {
    // A `factor` below 1 is a schedule that shrinks, and it would read as a flaky server for a week.
    expect(() => new Reconnect({ initialDelayMs: 0 })).toThrow(/initialDelayMs/);
    expect(() => new Reconnect({ factor: 0.5 })).toThrow(/factor/);
    expect(() => new Reconnect({ jitter: 1.5 })).toThrow(/jitter/);
  });

  it('uses Math.random and never a cryptographic source - WSM-RCN-005', () => {
    // Reconnect jitter exists to disperse a thundering herd, not to resist an adversary. The ping
    // nonce in M4 is the opposite case and does use one; conflating the two is the mistake this
    // guards against, in the direction the rule names.
    const source = readFileSync(join(process.cwd(), 'ts', 'reconnect.ts'), 'utf8');
    expect(source).toContain('Math.random');
    // A call site, not the word: the comment above `uniform()` names `crypto.getRandomValues` in
    // order to rule it out, and a test that banned the name would forbid saying why.
    expect(source).not.toMatch(/getRandomValues\s*\(/);
    expect(source).toContain('not to resist an adversary');
  });
});

// --------------------------------------------------------------------------- the attempt counter

describe('the attempt counter', () => {
  it('is the helper entire persistent state - WSM-RCN-001', () => {
    const counter = new AttemptCounter();
    expect(counter.value).toBe(0);
    expect([counter.failed(), counter.failed(), counter.failed()]).toEqual([1, 2, 3]);
    counter.established();
    expect(counter.value).toBe(0);
    // Everything else - the delay, the jitter, whether to give up - is computed from it, which is
    // what makes the schedule a pure function.
    expect(Object.getOwnPropertyNames(counter)).toEqual(['attempts']);
  });

  it('resets only on established, so socket-open alone leaves it climbing - WSM-RCN-004/WSM-INV-012', () => {
    // A server that accepts sockets while its backend is down turns exponential backoff into a
    // fixed-interval hammer if this is got wrong, and nothing about the peer looks broken.
    const options = new Reconnect();
    const counter = new AttemptCounter();

    const delays: number[] = [];
    for (let attempt = 0; attempt < 5; attempt += 1) {
      delays.push(unjitteredDelay(counter.value, options));
      counter.failed(); // socket opened, hello never acknowledged - still a failed attempt
    }

    expect(delays).toEqual([250, 500, 1000, 2000, 4000]);
    expect(delays, 'a growing delay sequence is the whole assertion').toEqual([...delays].sort((a, b) => a - b));

    counter.established();
    expect(unjitteredDelay(counter.value, options)).toBe(250);
  });

  it('stops the helper once maxAttempts is exhausted - WSM-RCN-044', () => {
    expect(shouldRetry(10_000, new Reconnect())).toBe(true);
    const bounded = new Reconnect({ maxAttempts: 3 });
    expect([0, 1, 2, 3, 4].map((attempt) => shouldRetry(attempt, bounded))).toEqual([true, true, true, false, false]);
  });
});

// --------------------------------------------------------------------------- the hello, as a value

describe('the hello', () => {
  it('sends none when none was configured - WSM-RCN-024', () => {
    expect(new Hello().configured).toBe(false);
    expect(new Hello({ payload: { session: 'abc' } }).configured).toBe(true);
    expect(new Hello({ headers: { trace: 'x' } }).configured).toBe(true);
  });

  it('is captured once, by value, and is never re-read - WSM-RCN-020', () => {
    const payload = { session: 'abc', tags: ['a'] };
    const hello = new Hello({ payload });
    payload.session = 'changed';
    payload.tags.push('b');
    expect(hello.payloadForWire()).toEqual({ session: 'abc', tags: ['a'] });
  });

  it('hands out a fresh copy each read, so one send cannot poison the next replay - WSM-RCN-027', () => {
    const hello = new Hello({ payload: { session: 'abc' } });
    const first = hello.payloadForWire() as Record<string, unknown>;
    first.session = 'mutated on its way to the codec';
    expect(hello.payloadForWire()).toEqual({ session: 'abc' });
  });

  it('defaults its deadline to ten seconds', () => {
    expect(new Hello().timeoutMs).toBe(10_000);
  });
});

// --------------------------------------------------------------------------- the heartbeat

describe('the heartbeat', () => {
  it('detects a swallowed pong within intervalMs plus timeoutMs - WSM-RCN-011', async () => {
    // Nothing serves the other end, so nobody will ever answer a ping. The clock is injected, so this
    // waits on nothing resembling a TCP timeout - the real elapsed time is asserted below to be a
    // rounding error next to the 30 s bound the rule states.
    const [dialerSide] = memoryPair();
    const peer = dialerOver(dialerSide);
    const closes: CloseReason[] = [];
    peer.onClose((reason) => closes.push(reason));

    let now = 0;
    peer.clock = () => now;
    const sleep: Sleep = async (ms: number) => {
      now += ms;
    };

    const intervalMs = DEFAULT_PING_INTERVAL_MS;
    const timeoutMs = DEFAULT_HEARTBEAT_TIMEOUT_MS;
    // `peer.ping()`'s own deadline is a real timer, and a test that waited it out for ten seconds
    // would be the very thing WSM-RCN-011 rules out. What is under test is the heartbeat's
    // arithmetic, so the ping is replaced by exactly what a swallowed pong does to it: the deadline
    // passes on the injected clock, and it throws.
    const ping = vi.spyOn(peer, 'ping').mockImplementation(async (deadlineMs?: number) => {
      now += deadlineMs ?? 0;
      throw new ConnectionClosed(`no pong within ${deadlineMs ?? 0}ms`, { code: 1006 });
    });

    const startedAt = now;
    const wallClockStart = Date.now();
    await new Heartbeat(peer, { intervalMs, timeoutMs, sleep }).run();

    expect(ping).toHaveBeenCalledTimes(1);
    expect(now - startedAt, 'the ping goes out one idle interval after the last frame, and no later').toBe(
      intervalMs + timeoutMs,
    );
    expect(Date.now() - wallClockStart, 'nothing here may wait on a real timeout').toBeLessThan(2000);

    // Declared dead **and** closed locally: without the second the read loop stays parked inside
    // `receive()` on a socket nobody will write to again, and the supervisor waits for it forever.
    expect(closes).toHaveLength(1);
    expect(closes[0].reason).toBe(`no pong within ${timeoutMs}ms`);
    expect(peer.isOpen).toBe(false);
    expect(dialerSide.isClosed).toBe(true);
  });

  it('resets its timer on any traffic, so a busy socket is never pinged - WSM-RCN-010', async () => {
    const [left, right] = memoryPair();
    const codec = new JsonCodec();
    const dialer = new Peer(left, { codec, isDialer: true });
    const acceptor = new Peer(right, { codec, isDialer: false });
    acceptor.onStream(() => undefined);
    void dialer.serve().catch(() => undefined);
    void acceptor.serve().catch(() => undefined);

    const heartbeat = new Heartbeat(dialer, { intervalMs: 30, timeoutMs: 500 });
    cleanups.push(() => {
      heartbeat.stop();
      left.drop();
    });
    void heartbeat.run().catch(() => undefined);

    // Idle means idle: the timer is `lastActivity`, stamped by every frame in either direction, and
    // not a fixed schedule - otherwise a busy connection spends a ping every interval to learn what
    // its own traffic has already proved.
    for (let round = 0; round < 12; round += 1) {
      await dialer.notify({ round });
      await settle(10);
    }
    expect(framesOn(left).filter((frame) => frame.type === 'ping')).toEqual([]);

    // The control: stop the traffic and the very same heartbeat pings. Without it this test would
    // pass just as well against a heartbeat that had already died.
    await until(() => framesOn(left).some((frame) => frame.type === 'ping'), 2000, 'a ping on the idle socket');
  });

  it('touches nothing when it is stopped while a ping is in flight - WSM-RCN-011/040', async () => {
    // Python cancels the heartbeat task, so a stopped heartbeat cannot reach its verdict; JavaScript
    // cannot interrupt the `await` inside `ping()`, so the flag has to be re-read after it settles.
    // The two cases this rules out are both silent: a `peer.close()` whose drain window outlives one
    // ping deadline gets reported to the application as a swallowed pong rather than as the close it
    // asked for, and a heartbeat stopped because its own socket died closes whatever socket the peer
    // holds by then - which, one backoff later, is the replacement.
    const [dialerSide] = memoryPair();
    const peer = dialerOver(dialerSide);
    const closes: CloseReason[] = [];
    peer.onClose((reason) => closes.push(reason));

    let now = 0;
    peer.clock = () => now;
    const sleep: Sleep = async (ms: number) => {
      now += ms;
    };

    let failPing: (error: unknown) => void = () => undefined;
    vi.spyOn(peer, 'ping').mockImplementation(
      async () =>
        new Promise<number>((_resolve, reject) => {
          failPing = reject;
        }),
    );

    const heartbeat = new Heartbeat(peer, { intervalMs: 10, timeoutMs: 10, sleep });
    const running = heartbeat.run();
    await settle(10);

    heartbeat.stop();
    failPing(new ConnectionClosed('no pong within 10ms', { code: 1006 }));
    await running;

    expect(closes, 'a stopped heartbeat must not report a death nobody is going to act on').toEqual([]);
    expect(peer.isOpen).toBe(true);
    expect(dialerSide.isClosed, 'nor close the socket the peer holds now').toBe(false);
  });

  it('counts a frame in either direction as activity - WSM-RCN-010', async () => {
    // Both directions, and this is where that is actually pinned: a socket carrying inbound frames is
    // demonstrably alive, and pinging it proves nothing new. The busy-socket test above cannot tell
    // the two halves apart, because its traffic stamps both.
    const [left, right] = memoryPair();
    const codec = new JsonCodec();
    // The acceptor end is never served, so nothing answers and nothing comes back: every frame below
    // travels in exactly one direction.
    const dialer = new Peer(left, { codec, isDialer: true });
    void dialer.serve().catch(() => undefined);
    cleanups.push(() => left.drop());
    void right;

    const atStart = dialer.lastActivity;
    await dialer.notify({ outbound: true });
    await settle(10);
    const afterSending = dialer.lastActivity;
    expect(afterSending, 'a frame this peer sent must count').toBeGreaterThan(atStart);

    // A `pong` for a nonce nobody is waiting for is dropped without a reply, so it is inbound and
    // nothing else.
    left.inject(codec.encode({ type: 'pong', nonce: 'nobody-asked-for-this' } as unknown as Frame));
    await settle(10);
    expect(dialer.lastActivity, 'a frame this peer received must count too').toBeGreaterThan(afterSending);
  });
});

// --------------------------------------------------------------------------- the driver

describe('the reconnect driver', () => {
  it('is dialer-only: an acceptor cannot have a reconnect helper', () => {
    // An acceptor has no url to dial and no idea who its remote was. A helper here would be a server
    // trying to call its clients back, which is not what this protocol does.
    const [socket] = memoryPair();
    const acceptor = new Peer(socket, { codec: new JsonCodec(), isDialer: false });
    expect(
      () => new ConnectionLoop(acceptor, async () => socket, { options: new Reconnect(), hello: new Hello() }),
    ).toThrow(/dialer-only/);
  });

  it('does not reset the counter when the hello never completes - WSM-RCN-004', async () => {
    const server = new FakeServer();
    server.handler = () => undefined;

    const peer = dialerOver(await server.dial());
    const reconnects: number[] = [];
    peer.onReconnect((attempt) => reconnects.push(attempt));

    const loop = new ConnectionLoop(peer, server.dial, {
      options: new Reconnect({ initialDelayMs: 250, jitter: 0, maxAttempts: 4 }),
      hello: new Hello({ payload: { session: 'abc' }, timeoutMs: 10 }),
      sleep: instantSleep([]),
      draw: () => 0,
      pingIntervalMs: 0,
    });
    cleanups.push(() => void loop.stop());
    await loop.establish();
    loop.start();

    // From here the server accepts every socket and then never answers the hello - a backend that is
    // down behind a load balancer that is not. A helper that reset its counter when `dial()` returned
    // would hammer this server every 250 ms forever (WSM-INV-012).
    server.handler = () => new Promise<void>(() => undefined);
    server.drop();

    await until(() => loop.delays.length === 4, 4000, 'four backed-off attempts');
    await settle(50);

    const delays = loop.delays;
    expect(delays).toEqual([250, 500, 1000, 2000]);
    // A fresh array each read, so a caller cannot edit the driver's own record by editing what it
    // was handed - Python returns `list(self._delays)` for the same reason.
    expect(loop.delays).not.toBe(loop.delays);
    expect(delays, 'a growing delay sequence is the whole assertion').toEqual([...delays].sort((a, b) => a - b));
    expect(reconnects, 'an attempt whose hello never completed is not a reconnection').toEqual([]);
    await loop.stop();
  });

  it('backs off on a reset hello and on a timed-out hello alike - WSM-RCN-026', async () => {
    const server = new FakeServer();
    // The acceptor decides from its own count rather than the test swapping handlers between
    // attempts: with an injected sleep the two failing attempts happen within a millisecond of each
    // other, and a test that polled between them would lose that race about as often as it won it.
    // Hello 1 is the first connection and is acknowledged; hello 2 is refused outright; hello 3 is
    // accepted and then never answered.
    let hellos = 0;
    server.handler = (_payload, stream: Stream) => {
      hellos += 1;
      if (hellos === 2) return stream.reset(ResetCode.REFUSED, 'not today');
      if (hellos === 3) return new Promise<void>(() => undefined);
      return undefined;
    };

    const peer = dialerOver(await server.dial());
    const reconnects: number[] = [];
    const closes: CloseReason[] = [];
    peer.onReconnect((attempt) => reconnects.push(attempt));
    peer.onClose((reason) => closes.push(reason));

    const loop = new ConnectionLoop(peer, server.dial, {
      options: new Reconnect({ initialDelayMs: 250, jitter: 0, maxAttempts: 2 }),
      hello: new Hello({ payload: { session: 'abc' }, timeoutMs: 30 }),
      sleep: instantSleep([]),
      draw: () => 0,
      pingIntervalMs: 0,
    });
    cleanups.push(() => void loop.stop());
    await loop.establish();
    loop.start();

    // Both are failed *connection attempts*, not connections: no `onReconnect`, the counter climbs,
    // and the socket each was on is closed.
    server.drop();

    await until(() => closes.length === 3, 4000, 'the loss and the two failed attempts');
    await settle(80);
    expect(hellos, 'both failing attempts must have reached the acceptor as an ordinary open').toBe(3);

    expect(loop.delays).toEqual([250, 500]);
    expect(loop.attempts).toBe(2);
    expect(reconnects).toEqual([]);
    expect(loop.reconnections).toBe(0);
    // A failed hello is reported as the loss it is, with its own text rather than the socket's - and
    // carrying **which** hello failure it was, because that is what an operator reads `reason` for
    // (WSM-RCN-045): a refusal and a deadline are two different outages.
    expect(closes.map((reason) => reason.reason)).toEqual([
      '',
      'the hello did not complete: not today',
      'the hello did not complete: the hello was not acknowledged within 30ms',
    ]);
    // `willRetry` is the helper's intention for *this* attempt, so it is set before the socket is
    // adopted: the last attempt before the cap already knows it is the last (WSM-RCN-040).
    //
    // And there are three closes, not four. The cap here is spent by failed *hellos*, so the last
    // loss already reported `willRetry: false`; the helper's own give-up would be the same peer
    // ending a second time, and it is suppressed (WSM-RCN-044).
    expect(closes.map((reason) => reason.willRetry)).toEqual([true, true, false]);
    // The socket each failed hello was on is closed rather than left dangling, and both ends know.
    expect(server.dialerSockets[1].isClosed, 'the reset hello left its socket open').toBe(true);
    expect(server.dialerSockets[2].isClosed, 'the timed-out hello left its socket open').toBe(true);
    await loop.stop();
  });

  it('replays byte-identical hellos across three drops - WSM-RCN-027', async () => {
    const server = new FakeServer();
    const seen: unknown[] = [];
    server.handler = (payload) => {
      seen.push(payload);
    };

    const peer = dialerOver(await server.dial());
    const reconnects: number[] = [];
    /** How many hellos the server had *acknowledged* at the instant `onReconnect` ran. */
    const acknowledgedAtReconnect: number[] = [];
    peer.onReconnect((attempt) => {
      reconnects.push(attempt);
      acknowledgedAtReconnect.push(seen.length);
    });

    const loop = new ConnectionLoop(peer, server.dial, {
      options: new Reconnect({ initialDelayMs: 10, jitter: 0 }),
      hello: new Hello({ payload: { session: 'abc', rooms: ['lobby'] } }),
      sleep: instantSleep([]),
      draw: () => 0,
      pingIntervalMs: 0,
    });
    cleanups.push(() => void loop.stop());
    await loop.establish();
    loop.start();

    for (let drop = 0; drop < 3; drop += 1) {
      const wanted = drop + 1;
      server.drop();
      await until(() => reconnects.length === wanted, 3000, `reconnect ${wanted}`);
    }

    // **Encoded bytes**, not decoded objects: two payloads can compare equal after decoding and still
    // have gone out as different bytes - a different key order, a re-encoded number - and "verbatim"
    // (WSM-RCN-020) is a claim about what crossed the wire.
    const onTheWire = [0, 1, 2, 3].map((index) => server.firstMessageOn(index));
    expect(typeof onTheWire[0]).toBe('string');
    expect(new Set(onTheWire).size, 'the four hellos must be byte-identical').toBe(1);
    expect(WIRE.decode(onTheWire[0] as string).payload).toEqual({ session: 'abc', rooms: ['lobby'] });

    expect(reconnects).toEqual([1, 2, 3]);
    // Fired *after* each acknowledgement and never before (WSM-RCN-030): at reconnect n the server
    // had acknowledged n + 1 hellos, the first connection's included.
    expect(acknowledgedAtReconnect).toEqual([2, 3, 4]);
    // Established every time, so the counter reset every time and the delay never grew.
    expect(loop.delays).toEqual([10, 10, 10]);
    await loop.stop();
  });

  it('leaves a peer findable in the registry even with no onReconnect handler - WSM-RCN-027', async () => {
    // An application that registers no `onReconnect` still ends up with a peer the server can find:
    // the hello is replayed by the *helper*, not by the application (WSM-INV-013), so the acceptor
    // learns who this is without any application code running on either side of the reconnect.
    const server = new FakeServer();
    const registry = new PeerRegistry();
    const seen: unknown[] = [];
    server.handler = (payload) => {
      const acceptor = server.acceptors[server.acceptors.length - 1];
      acceptor.tags.session = (payload as { session: string }).session;
      registry.register(acceptor);
      seen.push(payload);
    };

    const peer = dialerOver(await server.dial());
    const loop = new ConnectionLoop(peer, server.dial, {
      options: new Reconnect({ initialDelayMs: 10, jitter: 0 }),
      hello: new Hello({ payload: { session: 'abc' } }),
      sleep: instantSleep([]),
      draw: () => 0,
      pingIntervalMs: 0,
    });
    cleanups.push(() => void loop.stop());
    await loop.establish();
    loop.start();
    expect(registry.peersFor({ session: 'abc' })).toEqual([server.acceptors[0]]);

    server.drop();
    await until(() => seen.length === 2, 3000, 'the replayed hello');
    await settle(30);

    const found = registry.peersFor({ session: 'abc' });
    expect(found, 'the successor is findable, and the dead one is not').toEqual([server.acceptors[1]]);
    await loop.stop();
  });

  it('sends the hello as an ordinary open that reaches onStream - WSM-RCN-021', async () => {
    const server = new FakeServer();
    const seen: { payload: unknown; stream: Stream }[] = [];
    server.handler = (payload, stream) => {
      seen.push({ payload, stream });
    };

    const peer = dialerOver(await server.dial());
    const loop = new ConnectionLoop(peer, server.dial, {
      options: new Reconnect({ initialDelayMs: 10, jitter: 0 }),
      hello: new Hello({ payload: { session: 'abc' } }),
      sleep: instantSleep([]),
      draw: () => 0,
      pingIntervalMs: 0,
    });
    cleanups.push(() => void loop.stop());
    await loop.establish();

    // Delivered to the acceptor's own handler like any other stream.
    expect(seen).toHaveLength(1);
    expect(seen[0].payload).toEqual({ session: 'abc' });
    expect(seen[0].stream.id, 'an ordinary first stream, on the ordinary dialer parity').toBe(1);

    // And nothing marks it on the wire: byte for byte the same message an application `open()` with
    // the same payload produces. muxws must not flag it, interpret it, or mark it in any way.
    const [ordinaryLeft, ordinaryRight] = memoryPair();
    const ordinaryDialer = new Peer(ordinaryLeft, { codec: new JsonCodec(), isDialer: true });
    const ordinaryAcceptor = new Peer(ordinaryRight, { codec: new JsonCodec(), isDialer: false });
    ordinaryAcceptor.onStream(() => undefined);
    void ordinaryDialer.serve().catch(() => undefined);
    void ordinaryAcceptor.serve().catch(() => undefined);
    ordinaryDialer.open({ session: 'abc' }, { end: true });
    await until(() => ordinaryLeft.sent.length === 1, 2000, 'the control open');

    expect(server.firstMessageOn(0)).toBe(ordinaryLeft.sent[0]);
    ordinaryLeft.drop();
    await loop.stop();
  });

  it('does not interpret what an acceptor answers the hello with - WSM-RCN-021', async () => {
    // muxws must not flag, interpret or mark the hello, and that cuts both ways: an acceptor is free
    // to answer one, and whatever comes back is read and dropped rather than given a meaning.
    const server = new FakeServer();
    server.handler = (_payload, stream: Stream) => stream.reply({ welcome: true });

    const peer = dialerOver(await server.dial());
    const loop = new ConnectionLoop(peer, server.dial, {
      options: new Reconnect({ initialDelayMs: 10, jitter: 0 }),
      hello: new Hello({ payload: { session: 'abc' } }),
      sleep: instantSleep([]),
      draw: () => 0,
      pingIntervalMs: 0,
    });
    cleanups.push(() => void loop.stop());
    await loop.establish();

    expect(loop.attempts, 'an answered hello is acknowledged like any other').toBe(0);
    expect(peer.isOpen).toBe(true);
    await loop.stop();
  });

  it('puts the hello before every application frame on the socket - WSM-RCN-023', async () => {
    const server = new FakeServer();
    const seenByServer: unknown[] = [];
    server.handler = (payload) => {
      seenByServer.push(payload);
    };

    const peer = dialerOver(await server.dial());
    // `onReconnect` is the earliest moment an application learns it has a socket again, so an
    // application that sends the instant it is told still cannot get ahead of the hello.
    peer.onReconnect(() => {
      void peer.notify({ app: 'the first thing the application sends' });
    });

    const loop = new ConnectionLoop(peer, server.dial, {
      options: new Reconnect({ initialDelayMs: 10, jitter: 0 }),
      hello: new Hello({ payload: { session: 'abc' } }),
      sleep: instantSleep([]),
      draw: () => 0,
      pingIntervalMs: 0,
    });
    cleanups.push(() => void loop.stop());
    await loop.establish();
    loop.start();

    server.drop();
    await until(() => seenByServer.length === 3, 3000, 'the replayed hello and the application frame');

    const replayed = framesOn(server.dialerSockets[1]);
    expect(replayed[0].type).toBe('open');
    expect(replayed[0].payload, 'the first frame on the new socket must be the hello').toEqual({ session: 'abc' });
    expect(replayed[1].payload).toEqual({ app: 'the first thing the application sends' });
    // And from the server's side, in the order it saw them.
    expect(seenByServer).toEqual([
      { session: 'abc' },
      { session: 'abc' },
      { app: 'the first thing the application sends' },
    ]);
    await loop.stop();
  });

  it('is not open for the length of the hello window, so nothing can precede the hello - WSM-RCN-043', async () => {
    // The window WSM-RCN-043 is about: the socket is open and the connection is not, because the
    // hello has not come back (WSM-RCN-004). A peer that reported itself open here would accept
    // `open()`, `notify()` and `request()`, and every one of them would put an application frame
    // ahead of the hello on the wire - which is WSM-RCN-023, broken by the same line.
    const server = new FakeServer();
    let hellos = 0;
    let acknowledge: () => void = () => undefined;
    server.handler = () => {
      hellos += 1;
      // The first hello is answered at once; the second is held open, which is the only way to stand
      // still inside a window that is otherwise a few microtasks wide.
      if (hellos === 1) return undefined;
      return new Promise<void>((resolve) => {
        acknowledge = resolve;
      });
    };

    const peer = dialerOver(await server.dial());
    const loop = new ConnectionLoop(peer, server.dial, {
      options: new Reconnect({ initialDelayMs: 10, jitter: 0 }),
      hello: new Hello({ payload: { session: 'abc' }, timeoutMs: 5000 }),
      sleep: instantSleep([]),
      draw: () => 0,
      pingIntervalMs: 0,
    });
    cleanups.push(() => void loop.stop());
    await loop.establish();
    loop.start();
    expect(peer.isOpen, 'an acknowledged hello is a connection').toBe(true);

    server.drop();
    await until(() => hellos === 2, 3000, 'the replayed hello reaching the acceptor');

    expect(peer.isOpen, 'a socket whose hello is unanswered is not yet a connection').toBe(false);
    expect(() => peer.open({ app: 'too early' })).toThrow(ConnectionLost);
    expect(await outcomeOf(peer.notify({ app: 'too early' }))).toBeInstanceOf(ConnectionLost);
    expect(await outcomeOf(peer.request({ app: 'too early' }))).toBeInstanceOf(ConnectionLost);

    acknowledge();
    await until(() => loop.reconnections === 1, 3000, 'the acknowledgement');
    expect(peer.isOpen).toBe(true);

    // The hello is the first frame on the new socket, and the only one: everything attempted in the
    // window was refused rather than buffered for it (WSM-RCN-023/042).
    const replayed = framesOn(server.dialerSockets[1]);
    expect(replayed[0].type).toBe('open');
    expect(replayed[0].payload).toEqual({ session: 'abc' });
    expect(replayed.map((frame) => frame.payload)).toEqual([{ session: 'abc' }]);
    await loop.stop();
  });

  it('still puts a reset on the wire for a stream pushed inside the hello window - WSM-STM-021', async () => {
    // Two different questions about one socket, and WSM-RCN-043 answers only the first. `isOpen`
    // says "may the application start something here", which the hello window makes false. A reset
    // already owed to the remote asks "is there a wire", which that window does not make false - the
    // hello itself is travelling on it. Reading the wrong one drops the reset, and the acceptor is
    // left holding a stream this side has already closed.
    const server = new FakeServer();
    let hellos = 0;
    let acknowledge: () => void = () => undefined;
    server.handler = () => {
      hellos += 1;
      if (hellos === 1) return undefined;
      // Pushed while our own hello is still outstanding, so the dialer answers it from inside the
      // window.
      server.acceptors[server.acceptors.length - 1].open({ push: 'during the hello' });
      return new Promise<void>((resolve) => {
        acknowledge = resolve;
      });
    };

    const peer = dialerOver(await server.dial());
    peer.onStream(async (_payload, stream) => {
      await stream.reset(ResetCode.CANCELLED, 'not now');
    });
    const loop = new ConnectionLoop(peer, server.dial, {
      options: new Reconnect({ initialDelayMs: 10, jitter: 0 }),
      hello: new Hello({ payload: { session: 'abc' }, timeoutMs: 5000 }),
      sleep: instantSleep([]),
      draw: () => 0,
      pingIntervalMs: 0,
    });
    cleanups.push(() => void loop.stop());
    await loop.establish();
    loop.start();

    server.drop();
    await until(() => hellos === 2, 3000, 'the replayed hello reaching the acceptor');
    // Bounded, and the rejection is swallowed so the expectation below reports what was actually on
    // the wire: a test that hangs on the defect it exists to catch reads in CI as a bare timeout.
    await until(
      () => framesOn(server.dialerSockets[1]).some((frame) => frame.type === 'reset'),
      2000,
      'the reset reaching the wire',
    ).catch(() => undefined);

    const resets = framesOn(server.dialerSockets[1]).filter((frame) => frame.type === 'reset');
    expect(
      resets.map((frame) => frame.code),
      'the reset never reached the wire; the remote still believes that stream is live',
    ).toEqual([ResetCode.CANCELLED]);

    acknowledge();
    await loop.stop();
  });

  it('is established at the subprotocol accept when no hello is configured - WSM-RCN-024/WSM-CON-030', async () => {
    const server = new FakeServer();
    const peer = dialerOver(await server.dial());
    /** How many frames the dialer had put on the new socket at the instant `onReconnect` ran. */
    const framesAtReconnect: number[] = [];
    peer.onReconnect(() => {
      framesAtReconnect.push(server.dialerSockets[server.dialerSockets.length - 1].sent.length);
    });

    const loop = new ConnectionLoop(peer, server.dial, {
      options: new Reconnect({ initialDelayMs: 10, jitter: 0 }),
      hello: new Hello(),
      sleep: instantSleep([]),
      draw: () => 0,
      pingIntervalMs: 0,
    });
    cleanups.push(() => void loop.stop());
    await loop.establish();
    loop.start();

    // With the `settings` exchange gone, the socket-open moment and the subprotocol moment coincide,
    // so for a peer with no hello "established" really is socket-open: the counter is reset and
    // `isOpen` is true with no frame exchanged first.
    expect(loop.attempts).toBe(0);
    expect(peer.isOpen).toBe(true);
    expect(server.dialerSockets[0].sent, 'a peer given no hello sends none').toEqual([]);

    server.drop();
    await until(() => framesAtReconnect.length === 1, 3000, 'onReconnect on the new socket');
    expect(framesAtReconnect, 'established, and announced, with nothing on the wire').toEqual([0]);
    await loop.stop();
  });

  it('takes the same backoff path on a dead socket as on a clean close - WSM-RCN-011', async () => {
    /** One socket loss, one recovery, reported as the driver saw it. */
    interface Shape {
      delays: readonly number[];
      reconnections: number;
      closes: boolean[];
    }

    const runClean = async (): Promise<Shape> => {
      const server = new FakeServer();
      const peer = dialerOver(await server.dial());
      const closes: boolean[] = [];
      peer.onClose((reason) => closes.push(reason.willRetry));
      // The real sleep, not an injected one: the loop hands its `sleep` to the heartbeat too, and an
      // instant sleep there would spin a heartbeat that never gets any older. 5 ms of real backoff is
      // the price of running the two paths through the same code.
      const loop = new ConnectionLoop(peer, server.dial, {
        options: new Reconnect({ initialDelayMs: 5, jitter: 0 }),
        hello: new Hello(),
        draw: () => 0,
        pingIntervalMs: 0,
      });
      cleanups.push(() => void loop.stop());
      await loop.establish();
      loop.start();
      server.drop();
      await until(() => loop.reconnections === 1, 3000, 'the recovery after a clean close');
      await settle(40);
      await loop.stop();
      return { delays: loop.delays, reconnections: loop.reconnections, closes };
    };

    const runDead = async (): Promise<Shape> => {
      const server = new FakeServer();
      // The first acceptor is built but never served, so a `ping` on it is received by nobody. Every
      // socket after it answers normally, so exactly one loss happens - by heartbeat rather than by
      // a close frame, which is the only difference between the two runs.
      server.answering = false;
      const peer = dialerOver(await server.dial());
      server.answering = true;
      const closes: boolean[] = [];
      peer.onClose((reason) => closes.push(reason.willRetry));
      const loop = new ConnectionLoop(peer, server.dial, {
        options: new Reconnect({ initialDelayMs: 5, jitter: 0 }),
        hello: new Hello(),
        draw: () => 0,
        pingIntervalMs: 10,
        pingTimeoutMs: 30,
      });
      cleanups.push(() => void loop.stop());
      await loop.establish();
      loop.start();
      await until(() => loop.reconnections === 1, 3000, 'the recovery after a swallowed pong');
      await settle(40);
      await loop.stop();
      return { delays: loop.delays, reconnections: loop.reconnections, closes };
    };

    // There must be no second code path for a dead socket: the supervisor backs off and
    // re-establishes exactly as after a drop, `onClose` fires for the loss with the same
    // `willRetry`, and one delay is waited.
    expect(await runDead()).toEqual(await runClean());
  });

  it('fires onClose once with willRetry false when maxAttempts is exhausted and never dials again', async () => {
    // WSM-RCN-044.
    const server = new FakeServer();
    const peer = dialerOver(await server.dial());
    const closes: CloseReason[] = [];
    peer.onClose((reason) => closes.push(reason));

    const loop = new ConnectionLoop(peer, server.dial, {
      options: new Reconnect({ initialDelayMs: 5, jitter: 0, maxAttempts: 2 }),
      hello: new Hello(),
      sleep: instantSleep([]),
      draw: () => 0,
      pingIntervalMs: 0,
    });
    cleanups.push(() => void loop.stop());
    await loop.establish();
    loop.start();

    server.refusals = 99;
    const dialsBefore = server.dials;
    server.drop();
    await until(() => closes.length === 2, 3000, 'the loss and the give-up');
    await settle(80);

    // One close for the socket that died while the helper still meant to retry, one for the helper
    // giving up. Never a third, and never another dial - a refused dial lost no socket, so it fires
    // no close of its own (WSM-RCN-040).
    expect(closes.map((reason) => reason.willRetry)).toEqual([true, false]);
    expect(server.dials - dialsBefore, 'exactly maxAttempts dials, then silence').toBe(2);
    expect(peer.willRetry).toBe(false);
    await loop.stop();
  });

  it('reports the end once when the cap is spent by failed hellos - WSM-RCN-044', async () => {
    // The path where two reports of one ending are easy to write: the cap runs out on a *hello* that
    // never completed rather than on a refused dial, so the last loss already carries
    // `willRetry: false` and the helper's own give-up would say the same thing again a moment later.
    // An application counting endings would see this peer end twice.
    const server = new FakeServer();
    let hellos = 0;
    server.handler = () => {
      hellos += 1;
      return hellos === 1 ? undefined : new Promise<void>(() => undefined);
    };

    const peer = dialerOver(await server.dial());
    const closes: CloseReason[] = [];
    peer.onClose((reason) => closes.push(reason));

    const loop = new ConnectionLoop(peer, server.dial, {
      options: new Reconnect({ initialDelayMs: 5, jitter: 0, maxAttempts: 1 }),
      hello: new Hello({ payload: { session: 'abc' }, timeoutMs: 20 }),
      sleep: instantSleep([]),
      draw: () => 0,
      pingIntervalMs: 0,
    });
    cleanups.push(() => void loop.stop());
    await loop.establish();
    loop.start();

    server.drop();
    await until(() => closes.length === 2, 3000, 'the loss and the failed hello');
    // Longer than the give-up needs, so "there was no third" is a statement about suppression rather
    // than about the test having been quick.
    await settle(120);

    expect(
      closes.map((reason) => reason.willRetry),
      'exactly one ending per peer',
    ).toEqual([true, false]);
    await loop.stop();
  });

  it('marks the peer dead but reports nothing when the first hello fails - WSM-RCN-006', async () => {
    // The `connect()`-level test above asserts the same silence from outside; this one is inside,
    // where a handler is already attached and could hear a report if one were made. Both halves
    // matter and they pull in opposite directions.
    const server = new FakeServer();
    server.handler = () => new Promise<void>(() => undefined);

    const peer = dialerOver(await server.dial());
    const closes: CloseReason[] = [];
    peer.onClose((reason) => closes.push(reason));

    const loop = new ConnectionLoop(peer, server.dial, {
      options: new Reconnect({ initialDelayMs: 5, jitter: 0 }),
      hello: new Hello({ payload: { session: 'abc' }, timeoutMs: 20 }),
      sleep: instantSleep([]),
      draw: () => 0,
      pingIntervalMs: 0,
    });
    cleanups.push(() => void loop.stop());

    await expect(loop.establish()).rejects.toThrow(StreamTimeout);

    // Dead, and deterministically so: leaving `isOpen` to a race between the read loop and the
    // socket close is how a peer whose hello never completed reads as alive.
    expect(peer.isOpen, 'a peer that never established is not open').toBe(false);
    // And silent. Nothing was established, so nothing was lost, and `establish()` throwing is the
    // report (WSM-RCN-006). A `CloseReason` here would be the second report of one failure.
    expect(closes, 'a callback nobody could be listening to is not a report').toEqual([]);
  });

  it('keeps its record of delays bounded - WSM-RCN-001', async () => {
    // `delays` is instrumentation hanging off an object that lives as long as the process. Against a
    // server that is down for a week with no attempt cap, an unbounded record grows one entry per
    // attempt forever - a memory leak in the one component whose whole job is to survive a long
    // outage. The helper's persistent state is the counter; this is a log, and a log is trimmed.
    const server = new FakeServer();
    const peer = dialerOver(await server.dial());
    const loop = new ConnectionLoop(peer, server.dial, {
      options: new Reconnect({ initialDelayMs: 1, factor: 1, jitter: 0, maxAttempts: 150 }),
      hello: new Hello(),
      sleep: instantSleep([]),
      draw: () => 0,
      pingIntervalMs: 0,
    });
    cleanups.push(() => void loop.stop());
    await loop.establish();
    loop.start();

    server.refusals = 999;
    server.drop();
    await until(() => loop.attempts === 150, 5000, 'the attempt cap running out');

    expect(loop.delays.length, 'the most recent hundred, and not one more').toBe(100);
    await loop.stop();
  });

  it('never dials at all when maxAttempts is zero - WSM-RCN-044', async () => {
    // The degenerate cap, and the one an operator writes to mean "one connection, no reconnection".
    // It must still *report* the loss, once, with `willRetry` false - a helper that returned silently
    // here would leave the application waiting for a retry nobody intends to make.
    const server = new FakeServer();
    const peer = dialerOver(await server.dial());
    const closes: CloseReason[] = [];
    peer.onClose((reason) => closes.push(reason));

    const loop = new ConnectionLoop(peer, server.dial, {
      options: new Reconnect({ initialDelayMs: 5, jitter: 0, maxAttempts: 0 }),
      hello: new Hello(),
      sleep: instantSleep([]),
      draw: () => 0,
      pingIntervalMs: 0,
    });
    cleanups.push(() => void loop.stop());
    await loop.establish();
    loop.start();

    const dialsBefore = server.dials;
    server.drop();
    await until(() => closes.length === 1, 3000, 'the one and only close');
    await settle(120);

    expect(closes.map((reason) => reason.willRetry)).toEqual([false]);
    expect(server.dials - dialsBefore, 'a cap of zero dials zero times').toBe(0);
    expect(loop.delays).toEqual([]);
    await loop.stop();
  });

  it('decides by its own arithmetic and not by peer.willRetry - WSM-RCN-044/WSM-RCN-001', async () => {
    // `willRetry` is peer state that anything holding the peer can write - `ts/peer.spec.ts` writes
    // it, and so could an application. A supervisor that read it back as its own stop condition would
    // let one stray assignment retire the reconnect loop, with nothing anywhere reporting that the
    // peer had stopped trying: it would look exactly like a server that never came back. The attempt
    // counter is the helper's entire persistent state (WSM-RCN-001) and the cap is read off it, so
    // the field is only ever *read* by `CloseReason`. The `maxAttempts: 0` test above cannot see
    // this: there the loss itself already carries `willRetry` false, so both readings agree.
    const server = new FakeServer();
    const peer = dialerOver(await server.dial());

    const loop = new ConnectionLoop(peer, server.dial, {
      options: new Reconnect({ initialDelayMs: 5, jitter: 0 }),
      hello: new Hello(),
      sleep: instantSleep([]),
      draw: () => 0,
      pingIntervalMs: 0,
    });
    cleanups.push(() => void loop.stop());
    await loop.establish();
    loop.start();

    peer.willRetry = false;
    server.drop();
    await until(() => loop.reconnections === 1, 3000, "the helper's own arithmetic still saying yes");

    expect(peer.isOpen).toBe(true);
    await loop.stop();
  });

  it('keeps going when an application callback throws - WSM-RCN-011/030', async () => {
    // A library whose reconnect loop can be killed by an application's logging call is not a
    // reconnect loop. `onClose` and `onReconnect` are called from the supervisor, so an exception out
    // of one of them unwinds into it and stops it for good: no further dial, no further report, and
    // nothing anywhere saying why.
    const errors = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const server = new FakeServer();
    const peer = dialerOver(await server.dial());
    const order: string[] = [];
    peer.onClose(() => {
      order.push('close-1');
      throw new Error('the application logging call failed');
    });
    peer.onClose(() => order.push('close-2'));
    peer.onReconnect(() => {
      order.push('reconnect-1');
      throw new Error('and so did the one that tells the UI');
    });
    peer.onReconnect(() => order.push('reconnect-2'));

    const loop = new ConnectionLoop(peer, server.dial, {
      options: new Reconnect({ initialDelayMs: 5, jitter: 0 }),
      hello: new Hello(),
      sleep: instantSleep([]),
      draw: () => 0,
      pingIntervalMs: 0,
    });
    cleanups.push(() => void loop.stop());
    await loop.establish();
    loop.start();

    server.drop();
    await until(() => loop.reconnections === 1, 3000, 'the first recovery');
    // The next handler in the fan-out still ran: the second handler is not the first one's business.
    expect(order).toEqual(['close-1', 'close-2', 'reconnect-1', 'reconnect-2']);

    // And the driver is still a driver: it re-dials a second time, through the same throwing
    // handlers. This is the half a fan-out test alone cannot see.
    server.drop();
    await until(() => loop.reconnections === 2, 3000, 'the second recovery');
    expect(peer.isOpen).toBe(true);
    // Swallowed, not silenced: an application whose handler throws must be able to find out.
    expect(errors.mock.calls.length, 'each failure is logged where an operator can see it').toBe(4);
    await loop.stop();
    errors.mockRestore();
  });

  it('stops dialling on a deliberate close in the gap between sockets - WSM-RCN-040/044', async () => {
    // The gap is the one moment `close()` has nothing to close: `isOpen` is already false, and the
    // helper is asleep in a backoff nobody but the helper can end. A `close()` that took the peer's
    // own state as the whole story and returned early here would stop nothing - the driver would dial
    // on, re-establish, and hand the application back a live connection it had already given up. On
    // the same `Peer` object, so it would never think to close it twice.
    const server = new FakeServer();
    const peer = dialerOver(await server.dial());
    const loop = new ConnectionLoop(peer, server.dial, {
      options: new Reconnect({ initialDelayMs: 20, jitter: 0 }),
      hello: new Hello(),
      draw: () => 0,
      pingIntervalMs: 0,
    });
    cleanups.push(() => void loop.stop());
    // What `connect()` does, and the only route `close()` has to the driver.
    peer.connectionLoop = loop;
    await loop.establish();
    loop.start();

    server.refusals = 200;
    server.drop();
    await until(() => server.dials >= 3, 3000, 'a few refused dials');
    expect(peer.isOpen, 'the gap is where this test has to happen').toBe(false);

    await peer.close();
    const dials = server.dials;
    // Longer than the backoff still to come, so "it never dialled again" is a statement about the
    // driver having stopped rather than about the test having been quick.
    await settle(250);
    expect(server.dials, 'a close in the gap never dials again either').toBe(dials);
  });

  it("has connect()'s onStream listening before the first hello goes out - WSM-STM-033", async () => {
    // The acceptor pushes a stream the instant the hello arrives, which is what a server that hands
    // out a session, or that has something queued for this client, does. `connect()` registers the
    // caller's handler **before** `establish()` runs, so the push cannot arrive to nobody: a handler
    // attached after the hello would answer the first stream of every connection with
    // `reset(REFUSED, 'no onStream handler')`, and the same protocol would behave differently in the
    // two languages.
    const server = new FakeServer();
    server.handler = async () => {
      server.acceptors[server.acceptors.length - 1].open({ pushed: 'at the hello' }, { end: true });
      // The push reaches the dialer **before** the acknowledgement does, which is what makes this a
      // test about *when* the handler is registered rather than about whether it ever is: a handler
      // attached after `establish()` returned would be one frame too late.
      await settle(20);
    };

    const pushed: unknown[] = [];
    const socket = await server.dial();
    const peer = await dialAndEstablish(
      socket,
      server.dial,
      {
        hello: { session: 'abc' },
        reconnect: new Reconnect({ initialDelayMs: 10, jitter: 0 }),
        pingIntervalMs: 0,
        onStream: (payload) => {
          pushed.push(payload);
        },
      },
      new JsonCodec(),
    );
    cleanups.push(() => void peer.close());

    await until(() => pushed.length === 1, 3000, 'the stream pushed at the hello');
    expect(pushed).toEqual([{ pushed: 'at the hello' }]);
    // Nothing was refused: the push was answered by the application, not by the library.
    expect(framesOn(server.dialerSockets[0]).filter((frame) => frame.type === 'reset')).toEqual([]);
  });

  it('does not report a refused dial as a socket loss - WSM-RCN-040', async () => {
    const server = new FakeServer();
    const peer = dialerOver(await server.dial());
    const closes: CloseReason[] = [];
    peer.onClose((reason) => closes.push(reason));
    const reconnects: number[] = [];
    peer.onReconnect((attempt) => reconnects.push(attempt));

    const loop = new ConnectionLoop(peer, server.dial, {
      options: new Reconnect({ initialDelayMs: 5, jitter: 0 }),
      hello: new Hello(),
      sleep: instantSleep([]),
      draw: () => 0,
      pingIntervalMs: 0,
    });
    cleanups.push(() => void loop.stop());
    await loop.establish();
    loop.start();

    server.refusals = 3;
    server.drop();
    await until(() => reconnects.length === 1, 3000, 'the recovery after three refusals');

    expect(server.refusals).toBe(0);
    expect(closes, 'one socket died, so onClose fired once - not once per refused dial').toHaveLength(1);
    expect(closes[0].willRetry).toBe(true);
    expect(loop.attempts).toBe(0);
    await loop.stop();
  });
});

// --------------------------------------------------------------------------- CloseReason

describe('CloseReason', () => {
  it('has the same four fields in both languages, for every socket loss - WSM-RCN-045', async () => {
    // Read out of `muxws/observability.py` rather than restated here: a hand-copied list would agree
    // with itself forever while the two languages drifted apart, which is the one thing this test
    // exists to catch.
    const python = readFileSync(join(process.cwd(), 'muxws', 'observability.py'), 'utf8');
    const declaration = python.slice(python.indexOf('class CloseReason'), python.indexOf('\nlogger ='));
    const pythonFields = [...declaration.matchAll(/^ {4}(\w+): \w+/gm)]
      .map(([, name]) => name.replace(/_(.)/g, (_all, letter: string) => letter.toUpperCase()))
      .sort();
    expect(pythonFields).toEqual(['code', 'reason', 'wasClean', 'willRetry']);

    // One type per language, used for every socket loss - so all three paths below are asserted
    // against the same list: a drop, a deliberate close, and the helper giving up.
    const server = new FakeServer();
    const peer = dialerOver(await server.dial());
    const closes: CloseReason[] = [];
    peer.onClose((reason) => closes.push(reason));
    const loop = new ConnectionLoop(peer, server.dial, {
      options: new Reconnect({ initialDelayMs: 5, jitter: 0, maxAttempts: 1 }),
      hello: new Hello(),
      sleep: instantSleep([]),
      draw: () => 0,
      pingIntervalMs: 0,
    });
    cleanups.push(() => void loop.stop());
    await loop.establish();
    loop.start();
    server.refusals = 99;
    server.drop();
    await until(() => closes.length === 2, 3000, 'the loss and the give-up');
    await loop.stop();

    const [dropped] = makeDeliberate();
    const deliberate = await dropped;
    [...closes, deliberate].forEach((reason) => {
      expect(Object.keys(reason).sort()).toEqual(pythonFields);
      expect(typeof reason.code).toBe('number');
      expect(typeof reason.reason).toBe('string');
      expect(typeof reason.wasClean).toBe('boolean');
      expect(typeof reason.willRetry).toBe('boolean');
    });
  });
});

/** A deliberate `close()`, whose `CloseReason` must be the same type as a socket death's. */
function makeDeliberate(): [Promise<CloseReason>] {
  const [left, right] = memoryPair();
  const codec = new JsonCodec();
  const peer = new Peer(left, { codec, isDialer: true });
  const acceptor = new Peer(right, { codec, isDialer: false });
  void peer.serve().catch(() => undefined);
  void acceptor.serve().catch(() => undefined);
  const reported = new Promise<CloseReason>((resolve) => {
    peer.onClose(resolve);
  });
  void peer.close({ drainMs: 50 });
  return [reported];
}
