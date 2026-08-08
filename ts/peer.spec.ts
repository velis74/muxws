/**
 * `Peer`: id allocation, retention, dispatch, and what socket death does to every shape.
 *
 * A mirror of `muxws/peer_test.py`, test for test, against `ts/transports/memory.ts`. Where a rule is
 * asserted differently here it is because JavaScript offers a different witness of the same fact -
 * `stream.signal.aborted` where Python reads `stream.closed.is_set()`, a `console` spy where Python
 * reads `caplog` - and each of those places says so.
 */

// `describe`/`it`/`expect` are configured as globals; `vi` is imported because the shared eslint
// config does not know it as one, and a `no-undef` error per spy is not worth the three characters.
import { vi } from 'vitest';

import { type Codec, JsonCodec } from './codec';
import { ConnectionLost, ProtocolError, RemoteError, ResetCode, StreamReset, StreamTimeout } from './errors';
import { ABSENT, type Frame, framesEqual } from './frames';
import {
  type CloseReason,
  defaultErrorSerializer,
  type ErrorSerializer,
  type OpenOptions,
  Peer,
  type RequestOptions,
  logger,
} from './peer';
import { type Stream, StreamState } from './stream';
import { MemorySocket, memoryPair } from './transports/memory';

// --------------------------------------------------------------------------- the harness

type Who = 'dialer' | 'acceptor';

/** The decoder the test itself reads the wire with, independent of the pair's own codec. */
const WIRE = new JsonCodec();

/** A dialer and an acceptor, each serving, plus the frames each put on the wire. */
class Pair {
  private served: Promise<void>[] = [];

  private stopped = false;

  constructor(
    readonly dialer: Peer,
    readonly acceptor: Peer,
    readonly dialerSocket: MemorySocket,
    readonly acceptorSocket: MemorySocket,
    readonly codec: Codec,
  ) {}

  start(): void {
    // There is no `asyncio.Task` here: `serve()` is simply a promise nobody awaits until `stop()`.
    // The rejection handler is attached at once so a read loop that dies mid-test is not reported as
    // an unhandled rejection before the assertions get to look at it.
    this.served = [this.dialer.serve(), this.acceptor.serve()];
    this.served.forEach((task) => {
      void task.catch(() => undefined);
    });
  }

  /**
   * Let both read loops and both writers reach quiescence.
   *
   * Python spins the event loop with `asyncio.sleep(0)`; a macrotask turn is the JavaScript
   * equivalent that also drains the microtask queue, which is where every peer-internal step runs.
   */
  async settle(rounds = 12): Promise<void> {
    for (let turn = 0; turn < rounds; turn += 1) {
      await new Promise<void>((resolve) => {
        setTimeout(resolve, 0);
      });
    }
  }

  socketOf(who: Who): MemorySocket {
    return who === 'dialer' ? this.dialerSocket : this.acceptorSocket;
  }

  sentBy(who: Who): Frame[] {
    return this.socketOf(who).sent.map((message) => WIRE.decode(message));
  }

  framesOfType(who: Who, frameType: string): Frame[] {
    return this.sentBy(who).filter((frame) => frame.type === frameType);
  }

  lastFrameOfType(who: Who, frameType: string): Frame {
    const frames = this.framesOfType(who, frameType);
    expect(frames.length).toBeGreaterThan(0);
    return frames[frames.length - 1];
  }

  /** Deliver a frame no correct implementation would send, straight into `who`'s inbox. */
  inject(who: Who, frame: Frame): void {
    this.socketOf(who).inject(this.codec.encode(frame));
  }

  injectRaw(who: Who, message: string): void {
    this.socketOf(who).inject(message);
  }

  async stop(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    this.dialerSocket.drop();
    this.acceptorSocket.drop();
    if (this.served.length === 0) return;
    await this.settle();
    await Promise.all(this.served.map((task) => task.catch(() => undefined)));
  }
}

const built: Pair[] = [];

/** Build a peer pair without starting it, so a test can register handlers first. */
function makePair(options: { errorSerializer?: ErrorSerializer; codec?: Codec } = {}): Pair {
  const [left, right] = memoryPair();
  const codec = options.codec ?? new JsonCodec();
  const pair = new Pair(
    new Peer(left, { codec, isDialer: true, errorSerializer: options.errorSerializer }),
    new Peer(right, { codec, isDialer: false, errorSerializer: options.errorSerializer }),
    left,
    right,
    codec,
  );
  built.push(pair);
  return pair;
}

/**
 * The peer's private bookkeeping, which the Python suite reads as `_ignored_late_frames` and friends.
 *
 * Reaching for it is deliberate: WSM-STM-001 and WSM-STM-002 are statements about what the peer
 * *retains*, and a test that could only see the wire could not tell "ignored" from "not received".
 */
interface PeerInternals {
  ignoredLateFrames: number;
  highestLocalOpen: number;
  highestRemoteOpen: number;
  dispatch(frame: Frame): Promise<boolean>;
}

function internals(peer: Peer): PeerInternals {
  return peer as unknown as PeerInternals;
}

/** What the `console` shim in `ts/peer.ts` emitted, per level. Python reads the same through caplog. */
interface ConsoleLog {
  debug: string[];
  info: string[];
  warn: string[];
  error: string[];
}

const LEVELS = ['debug', 'info', 'warn', 'error'] as const;

let logged: ConsoleLog;

beforeEach(() => {
  logged = { debug: [], info: [], warn: [], error: [] };
  LEVELS.forEach((level) => {
    vi.spyOn(console, level).mockImplementation((...args: unknown[]) => {
      logged[level].push(args.map((arg) => String(arg)).join(' '));
    });
  });
});

afterEach(async () => {
  const pending = built.splice(0, built.length);
  await Promise.all(pending.map((pair) => pair.stop()));
  vi.restoreAllMocks();
});

// --------------------------------------------------------------------------- helpers

/** Python's `_hold`: keep the stream open until somebody else closes it. */
async function hold(payload: unknown, stream: Stream): Promise<void> {
  await stream.closed;
}

/** Python's `_reply_now`. */
async function replyNow(payload: unknown, stream: Stream): Promise<void> {
  await stream.reply({ ok: true });
}

/**
 * The handler failure the tests raise.
 *
 * Python raises `ValueError`, whose class name reaches the wire through the default serializer. The
 * JavaScript equivalent of "a named application exception" is a subclass that sets `name`.
 */
class HandlerFailure extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'HandlerFailure';
  }
}

/** `pytest.raises`, as a value: the rejection reason, or a failure if there was none. */
async function rejection(work: PromiseLike<unknown>): Promise<unknown> {
  try {
    await work;
  } catch (error) {
    return error;
  }
  throw new Error('expected a rejection, but the promise resolved');
}

/** Turn a promise into one that always resolves - to its value, or to its rejection reason. */
function settled(work: PromiseLike<unknown>): Promise<unknown> {
  return Promise.resolve(work).then(
    (value) => value,
    (error: unknown) => error,
  );
}

/** Everything a peer holds that could be keyed by a stream, in a container of any kind. */
function containersOf(peer: Peer): { name: string; size: number }[] {
  const target = peer as unknown as Record<string, unknown>;
  return (
    Object.getOwnPropertyNames(peer)
      // `tags` is the application's own dictionary; muxws never reads it (WSM-REG-001/002).
      .filter((name) => name !== 'tags')
      .map((name) => ({ name, value: target[name] }))
      .filter(({ value }) => sizeOf(value) !== null)
      .map(({ name, value }) => ({ name, size: sizeOf(value) as number }))
  );
}

function sizeOf(value: unknown): number | null {
  if (Array.isArray(value)) return value.length;
  if (value instanceof Map || value instanceof Set) return value.size;
  if (typeof value !== 'object' || value === null) return null;
  if (Object.getPrototypeOf(value) === Object.prototype) return Object.keys(value).length;
  // Anything else that reports its own occupancy counts too. The peer's outbound `AsyncQueue` is
  // exactly such a container: a frame retained per closed stream would sit in it, invisible to a
  // check that only understood `Map`, `Set` and `Array`.
  const reported = value as { size?: unknown; length?: unknown };
  if (typeof reported.size === 'number') return reported.size;
  if (typeof reported.length === 'number') return reported.length;
  return null;
}

// --------------------------------------------------------------------------- ids

describe('stream ids', () => {
  it('gives the dialer odd ids and the acceptor even ones, never reused - WSM-SID-002/004', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.dialer.onStream(hold);
    pair.start();

    expect([pair.dialer.open({}).id, pair.dialer.open({}).id, pair.dialer.open({}).id]).toEqual([1, 3, 5]);
    expect([pair.acceptor.open({}).id, pair.acceptor.open({}).id, pair.acceptor.open({}).id]).toEqual([2, 4, 6]);
    await pair.settle();

    await Promise.all([...pair.dialer.streams.values()].map((stream) => stream.reset(ResetCode.NO_ERROR)));
    await pair.settle();

    expect(pair.dialer.open({}).id, 'an id is never reused, even after the stream closes').toBe(7);
  });

  it('allocates and enqueues as one indivisible step - WSM-SID-006/WSM-INV-005', async () => {
    // Fifty opens from fifty separate tasks. If anything suspended between taking the id and queueing
    // the frame, the wire would carry a non-monotonic sequence - a protocol error this peer would be
    // committing against itself.
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    const ids = await Promise.all(
      Array.from({ length: 50 }, async () => {
        await Promise.resolve();
        return pair.dialer.open({ n: 1 }).id;
      }),
    );
    await pair.settle(40);

    const onWire = pair.framesOfType('dialer', 'open').map((frame) => frame.stream as number);
    const ascending = [...onWire].sort((left, right) => left - right);
    expect(onWire, 'wire order must be allocation order').toEqual(ascending);
    expect(onWire).toEqual([...ids].sort((left, right) => left - right).slice(0, onWire.length));
    expect(new Set(ids).size).toBe(50);
  });

  it('names a peer with a per-process prefix and a counter that never rewinds - WSM-API-009', () => {
    const seen: string[] = [];
    Array.from({ length: 100 }, () => makePair()).forEach((pair) => {
      seen.push(pair.dialer.id, pair.acceptor.id);
    });

    seen.forEach((peerId) => {
      expect(peerId).toMatch(/^[0-9a-f]{3}-\d+$/);
    });
    expect(new Set(seen).size, 'ids must not repeat within a process').toBe(seen.length);

    const prefixes = new Set(seen.map((peerId) => peerId.split('-')[0]));
    expect(prefixes.size, 'the prefix is drawn once per process').toBe(1);
    const counters = seen.map((peerId) => Number(peerId.split('-')[1]));
    expect(counters, 'the counter never rewinds').toEqual([...counters].sort((left, right) => left - right));
  });

  it("does not free a closed peer's id for reuse - WSM-API-009", async () => {
    // Reuse is the failure being prevented, so this needs a peer that was actually closed: counting
    // distinct ids among peers that were never closed cannot fail against a scheme that recycles the
    // ids of closed connections - which is precisely the scheme the rule forbids.
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();
    const closedIds = [pair.dialer.id, pair.acceptor.id];
    await pair.stop();

    expect(pair.dialer.isOpen).toBe(false);
    expect(pair.acceptor.isOpen).toBe(false);

    const fresh = makePair();
    const freshIds = [fresh.dialer.id, fresh.acceptor.id];
    freshIds.forEach((peerId) => {
      expect(closedIds, "a closed peer's id must not be reissued").not.toContain(peerId);
    });
    const counterOf = (peerId: string): number => Number(peerId.split('-')[1]);
    expect(Math.min(...freshIds.map(counterOf))).toBeGreaterThan(Math.max(...closedIds.map(counterOf)));
  });
});

// --------------------------------------------------------------------------- retention

describe('retention', () => {
  it('ignores a frame for a closed id below the high-water mark - WSM-STM-002', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    const stream = pair.dialer.open({ q: 1 });
    await pair.settle();
    await stream.reset(ResetCode.NO_ERROR);
    await pair.settle();

    const before = pair.sentBy('dialer').length;
    pair.inject('dialer', { type: 'data', stream: stream.id, payload: { late: true } });
    await pair.settle();

    expect(pair.dialer.isOpen).toBe(true);
    expect(pair.sentBy('dialer'), 'nothing goes out for a late frame').toHaveLength(before);
    expect(internals(pair.dialer).ignoredLateFrames).toBe(1);
  });

  it('kills the connection on a frame above the high-water mark - WSM-STM-003/WSM-INV-006', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    pair.dialer.open({ q: 1 });
    await pair.settle();
    pair.inject('acceptor', { type: 'data', stream: 99, payload: { a: 1 } });
    await pair.settle();

    expect(pair.lastFrameOfType('acceptor', 'goaway').code).toBe(ResetCode.PROTOCOL_ERROR);
    expect(pair.acceptor.isOpen).toBe(false);
  });

  it('kills the connection on a wrong-parity open - WSM-SID-005', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    pair.inject('acceptor', { type: 'open', stream: 2, payload: {} });
    await pair.settle();

    expect(pair.framesOfType('acceptor', 'goaway').length).toBeGreaterThan(0);
    expect(pair.acceptor.isOpen).toBe(false);
  });

  it('kills the connection on a non-monotonic open - WSM-SID-005', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    pair.inject('acceptor', { type: 'open', stream: 5, payload: {} });
    await pair.settle();
    pair.inject('acceptor', { type: 'open', stream: 3, payload: {} });
    await pair.settle();

    expect(pair.framesOfType('acceptor', 'goaway').length).toBeGreaterThan(0);
    expect(pair.acceptor.isOpen).toBe(false);
  });

  it('resets only the offending stream when data arrives after end - WSM-STM-020/021', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    const first = pair.dialer.open({ q: 1 });
    const second = pair.dialer.open({ q: 2 });
    await pair.settle();

    pair.inject('dialer', { type: 'data', stream: first.id, payload: {}, end: true });
    await pair.settle();
    pair.inject('dialer', { type: 'data', stream: first.id, payload: { more: true } });
    await pair.settle();

    const resets = pair.framesOfType('dialer', 'reset').filter((frame) => frame.stream === first.id);
    expect(resets.length).toBeGreaterThan(0);
    expect(resets[resets.length - 1].code).toBe(ResetCode.PROTOCOL_ERROR);
    expect(pair.dialer.isOpen, 'the connection survives a stream-level error').toBe(true);
    expect(second.state, 'other streams are unaffected').not.toBe(StreamState.CLOSED);
  });

  it('retains nothing per closed stream - WSM-STM-001', async () => {
    const pair = makePair();
    pair.acceptor.onStream(replyNow);
    pair.start();

    for (let call = 0; call < 200; call += 1) {
      expect(await pair.dialer.request({ q: 1 })).toEqual({ ok: true });
    }
    await pair.settle();

    expect(pair.dialer.streams.size).toBe(0);
    expect(pair.acceptor.streams.size).toBe(0);
    expect(typeof internals(pair.dialer).highestLocalOpen).toBe('number');
    expect(typeof internals(pair.acceptor).highestRemoteOpen).toBe('number');
    expect(internals(pair.dialer).highestLocalOpen).toBe(399);

    // Nothing keyed by a closed stream may linger anywhere on the peer, in a container of any kind.
    // Checking only maps would pass a `private seenIds = new Set<number>()` growing without bound.
    const lingering = containersOf(pair.dialer).filter((entry) => entry.size > 0);
    expect(lingering, 'the peer still holds entries after 200 closed streams').toEqual([]);
    expect(containersOf(pair.acceptor).filter((entry) => entry.size > 0)).toEqual([]);

    // And the scan has teeth. A request still in flight is reported, so the emptiness above is a
    // fact about the peer rather than about a scan that found nothing to look at - which is exactly
    // how a retention check passes for the wrong reason.
    const inFlight = pair.dialer.request({ q: 1 });
    expect(
      containersOf(pair.dialer).some((entry) => entry.size > 0),
      'the scan sees a live stream',
    ).toBe(true);
    expect(await inFlight).toEqual({ ok: true });
    await pair.settle();
    expect(containersOf(pair.dialer).filter((entry) => entry.size > 0)).toEqual([]);
  });
});

// --------------------------------------------------------------------------- dispatch

describe('dispatch', () => {
  it('ignores an unknown frame type, staying quiet and alive - WSM-FRM-002', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    // Non-vacuity. Without a witness that the frame *arrived*, every assertion below would hold just
    // as well against a peer that never received it, which is not what "ignored" means.
    const arrived: Frame[] = [];
    pair.acceptor.onFrame((direction, frame) => {
      if (direction === 'rx') arrived.push(frame);
    });
    pair.start();

    const before = pair.sentBy('acceptor').length;
    pair.injectRaw('acceptor', '{"type":"widget","stream":1}');
    await pair.settle();

    expect(arrived.filter((frame) => frame.type === 'widget')).toHaveLength(1);
    expect(pair.acceptor.isOpen).toBe(true);
    expect(pair.sentBy('acceptor'), 'an unknown type is never answered').toHaveLength(before);
    // Python asserts the one log line by raising `muxws.frames` to INFO with `caplog.at_level`. The
    // TypeScript shim has no such seam: its `level` is module-private and starts at 'warn', so the
    // single `logger.info` call in `dispatch` cannot reach `console.info` from any test or any
    // application. What is assertable here is the other half - an unknown frame is not an error and
    // must not spill onto the console by default. The "logged once" half waits on M5a's seam.
    LEVELS.forEach((level) => {
      expect(logged[level], `an ignored frame wrote to console.${level}`).toEqual([]);
    });
  });

  it('refuses an incoming open when there is no handler - WSM-STM-033', async () => {
    const pair = makePair();
    pair.start();

    const stream = pair.dialer.open({ q: 1 });
    const error = await rejection(stream);
    expect(error).toBeInstanceOf(StreamReset);
    // Nothing ran, so the opener may safely retry elsewhere.
    expect((error as StreamReset).code).toBe(ResetCode.REFUSED);
    expect(pair.lastFrameOfType('acceptor', 'reset').code).toBe(ResetCode.REFUSED);
  });

  it('answers a throwing handler with APPLICATION_ERROR always - WSM-STM-034/WSM-INV-008', async () => {
    // REFUSED promises the operation definitively did not happen. A handler that debits an account
    // and then throws would, under REFUSED, be inviting the client to retry the debit.
    const pair = makePair();
    pair.acceptor.onStream(async (payload: unknown, stream: Stream) => {
      await stream.send({ partial: true });
      throw new HandlerFailure('halfway through');
    });
    pair.start();

    const stream = pair.dialer.open({ q: 1 });
    const collected: unknown[] = [];
    const error = await rejection(
      (async () => {
        for await (const item of stream) collected.push(item);
      })(),
    );

    expect(error).toBeInstanceOf(RemoteError);
    expect((error as RemoteError).code).toBe(ResetCode.APPLICATION_ERROR);
    expect(collected, 'the payload the handler already sent still arrived').toEqual([{ partial: true }]);

    const reset = pair.lastFrameOfType('acceptor', 'reset');
    expect(reset.code).toBe(ResetCode.APPLICATION_ERROR);
    expect(reset.code).not.toBe(ResetCode.REFUSED);
    expect(reset.reason).toBe('halfway through');
    expect(reset.payload).toEqual({ type: 'HandlerFailure', message: 'halfway through' });
  });

  it('lets the errorSerializer hook replace the default payload - WSM-ERR-006', async () => {
    const pair = makePair({ errorSerializer: () => ({ redacted: true }) });
    pair.acceptor.onStream(() => {
      throw new HandlerFailure('secret detail');
    });
    pair.start();

    const error = await rejection(pair.dialer.open({ q: 1 }));
    expect(error).toBeInstanceOf(RemoteError);
    expect((error as RemoteError).payload).toEqual({ redacted: true });
    expect(JSON.stringify(pair.lastFrameOfType('acceptor', 'reset').payload)).not.toContain('secret detail');
  });

  // Python has one way to return nothing and JavaScript has two. A serializer written as
  // `(error) => { if (isPublic(error)) return describe(error); }` returns `undefined` on the
  // redacting path, so both spellings have to mean the same thing or the redaction leaks.
  (
    [
      ['null', () => null],
      ['undefined', () => undefined],
    ] as [string, ErrorSerializer][]
  ).forEach(([spelling, errorSerializer]) => {
    it(`sends no payload when the errorSerializer returns ${spelling} - WSM-ERR-006`, async () => {
      const pair = makePair({ errorSerializer });
      pair.acceptor.onStream(() => {
        throw new HandlerFailure('nope');
      });
      pair.start();

      expect(await rejection(pair.dialer.open({ q: 1 }))).toBeInstanceOf(RemoteError);
      const reset = pair.lastFrameOfType('acceptor', 'reset');
      expect(reset.payload).toBe(ABSENT);
      expect(reset.reason).toBe('nope');
    });
  });

  it('keeps the errorSerializer per peer - WSM-ERR-008', async () => {
    // One process, two peers, two answers to the same exception.
    const redacting = makePair({ errorSerializer: () => ({ redacted: true }) });
    const verbose = makePair({ errorSerializer: defaultErrorSerializer });

    [redacting, verbose].forEach((pair) => {
      pair.acceptor.onStream(() => {
        throw new HandlerFailure('detail');
      });
      pair.start();
    });

    const redacted = await rejection(redacting.dialer.open({}));
    const verbatim = await rejection(verbose.dialer.open({}));

    expect((redacted as RemoteError).payload).toEqual({ redacted: true });
    expect((verbatim as RemoteError).payload).toEqual({ type: 'HandlerFailure', message: 'detail' });
  });

  it("ends a handler's stream implicitly when it returns - WSM-STM-035", async () => {
    const pair = makePair();
    pair.acceptor.onStream(() => undefined);
    pair.start();

    const stream = pair.dialer.open({ q: 1 });
    const collected: unknown[] = [];
    for await (const item of stream) collected.push(item);
    await pair.settle();

    expect(collected).toEqual([]);
    const ends = pair.framesOfType('acceptor', 'data').filter((frame) => frame.end === true);
    expect(ends.length).toBeGreaterThan(0);
    expect(ends[ends.length - 1].stream).toBe(stream.id);
  });

  it('replaces a second onStream handler and says so - WSM-STM-030', async () => {
    const pair = makePair();
    const calls: string[] = [];
    pair.acceptor.onStream(() => {
      calls.push('first');
    });
    pair.acceptor.onStream(() => {
      calls.push('second');
    });
    pair.start();

    await pair.dialer.notify({ q: 1 });
    await pair.settle();

    expect(calls).toEqual(['second']);
    expect(logged.warn.filter((line) => line.includes('onStream'))).toHaveLength(1);
  });

  it('hands a fragmented open to the handler whole - WSM-STM-031/032', async () => {
    const pair = makePair();
    const received: unknown[][] = [];
    pair.acceptor.onStream((payload: unknown, stream: Stream) => {
      received.push([payload, stream.payload]);
    });
    pair.start();

    const whole = { body: 'x'.repeat(40) };
    const encoded = pair.codec.encodePayload(whole) as string;
    const half = Math.floor(encoded.length / 2);

    pair.inject('acceptor', { type: 'open', stream: 1, fragment: encoded.slice(0, half), more: true });
    await pair.settle();
    expect(received, 'a fragmented open must not reach the application in pieces').toEqual([]);

    pair.inject('acceptor', { type: 'open', stream: 1, fragment: encoded.slice(half), end: true });
    await pair.settle();

    expect(received).toHaveLength(1);
    expect(received[0]).toEqual([whole, whole]);
  });

  it('returns nothing from notify and leaves no handle - WSM-API-005', async () => {
    const pair = makePair();
    pair.acceptor.onStream(replyNow);
    pair.start();

    const result = await pair.dialer.notify({ event: 'tick' });
    expect(result).toBeUndefined();
    await pair.settle();
    expect(pair.lastFrameOfType('dialer', 'open').end).toBe(true);
  });
});

// --------------------------------------------------------------------------- socket death

/** Per-shape hang deadline, so a shape that never settles is *named* rather than timed out anonymously. */
const SHAPE_DEADLINE_MS = 2_000;

/** The deadline around the whole assertion block, exactly as Python's `asyncio.wait_for` is. */
const BLOCK_DEADLINE_MS = 8_000;

async function within(label: string, work: Promise<unknown>, deadlineMs = SHAPE_DEADLINE_MS): Promise<unknown> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const hang = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`the '${label}' shape hung instead of failing with ConnectionLost (WSM-INV-011)`));
    }, deadlineMs);
  });
  try {
    return await Promise.race([work, hang]);
  } finally {
    clearTimeout(timer);
  }
}

describe('socket death', () => {
  it('names the shape that hung rather than timing out anonymously - WSM-INV-011', async () => {
    // The safety net below is the point of the next test, so it is itself tested. A `within` that
    // silently awaited forever would turn "a shape hung" into a vitest timeout naming only the file,
    // which is the anonymous failure the brief rules out.
    const never = new Promise<never>(() => undefined);
    const caught = await rejection(within('iterate', never, 10));

    expect(caught).toBeInstanceOf(Error);
    expect((caught as Error).message).toContain("the 'iterate' shape hung");
    expect((caught as Error).message).toContain('WSM-INV-011');
    // And it resolves normally when the work does, so it can never invent a hang that did not happen.
    expect(await within('settles', Promise.resolve('done'), 10)).toBe('done');
  });

  it('fails every shape and none of them hangs - WSM-RCN-041/WSM-STM-014', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    const closes: CloseReason[] = [];
    const watched: Stream[] = [];
    /**
     * What the world looked like at the instant `onClose` ran. WSM-STM-014 puts the synthesised
     * failures *before* it, so a handler that fired first would observe streams still live - and
     * counting the calls alone cannot tell the two orderings apart.
     */
    const worldAtClose: { liveStreams: number; allClosed: boolean[]; allFailed: StreamState[] }[] = [];

    pair.dialer.onClose((reason) => {
      closes.push(reason);
      worldAtClose.push({
        liveStreams: pair.dialer.streams.size,
        // `closed` is a promise, so `signal.aborted` is the synchronous witness of "this stream has
        // closed"; WSM-API-023 requires the two to move at the same instant.
        allClosed: watched.map((stream) => stream.signal.aborted),
        allFailed: watched.map((stream) => stream.state),
      });
    });

    const body = async (): Promise<void> => {
      const awaited = pair.dialer.open({ shape: 'await' });
      const iterated = pair.dialer.open({ shape: 'iterate' });
      const sender = pair.dialer.open({ shape: 'send' });
      watched.push(awaited, iterated, sender);
      await pair.settle();

      const requestOutcome = settled(pair.dialer.request({ shape: 'request' }));
      const awaitOutcome = settled((async () => awaited)());
      const iterateOutcome = settled(
        (async () => {
          const collected: unknown[] = [];
          for await (const item of iterated) collected.push(item);
          // A clean end would read as "the export finished", which is exactly the lie WSM-RCN-041
          // forbids.
          throw new Error('an async for must raise on socket death, not terminate normally');
        })(),
      );
      await pair.settle();

      pair.dialerSocket.drop();
      await pair.settle();

      expect(await within('await', awaitOutcome)).toBeInstanceOf(ConnectionLost);
      expect(await within('iterate', iterateOutcome)).toBeInstanceOf(ConnectionLost);
      expect(await within('request', requestOutcome)).toBeInstanceOf(ConnectionLost);
      expect(await within('send', settled(sender.send({ late: true })))).toBeInstanceOf(ConnectionLost);
      expect(await within('end', settled(sender.end()))).toBeInstanceOf(ConnectionLost);

      // cancel() and reset() are no-ops, and every stream reports itself closed.
      expect(await within('cancel', settled(sender.cancel()))).toBeUndefined();
      expect(await within('reset', settled(sender.reset(ResetCode.NO_ERROR)))).toBeUndefined();
      await within('closed', settled(Promise.all(watched.map((stream) => stream.closed))));
      watched.forEach((stream) => {
        expect(stream.signal.aborted).toBe(true);
      });

      // A second await gets the same error rather than hanging (WSM-API-010).
      expect(await within('second await', settled((async () => awaited)()))).toBeInstanceOf(ConnectionLost);
    };

    let blockTimer: ReturnType<typeof setTimeout> | undefined;
    const blockDeadline = new Promise<never>((_resolve, reject) => {
      blockTimer = setTimeout(() => {
        reject(new Error('the socket-death assertion block hung as a whole (WSM-INV-011)'));
      }, BLOCK_DEADLINE_MS);
    });
    let outcome: unknown;
    try {
      outcome = await Promise.race([settled(body()), blockDeadline]);
    } finally {
      clearTimeout(blockTimer);
    }
    if (outcome !== undefined) throw outcome;

    expect(closes, 'on_close fires once per loss, after every stream has failed').toHaveLength(1);
    expect(closes[0].willRetry).toBe(false);
    const snapshot = worldAtClose[0];
    expect(snapshot.liveStreams, 'onClose ran while the peer still held live streams').toBe(0);
    expect(snapshot.allClosed, 'onClose ran before every stream closed').toEqual([true, true, true]);
    expect(snapshot.allFailed).toEqual([StreamState.CLOSED, StreamState.CLOSED, StreamState.CLOSED]);
  }, 20_000);

  it('refuses to open while disconnected - WSM-RCN-042/WSM-INV-010', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    pair.dialerSocket.drop();
    await pair.settle();

    // Nothing is buffered for a next socket.
    expect(() => pair.dialer.open({ q: 1 })).toThrow(ConnectionLost);
    expect(await rejection(pair.dialer.notify({ q: 1 }))).toBeInstanceOf(ConnectionLost);
    expect(await rejection(pair.dialer.request({ q: 1 }))).toBeInstanceOf(ConnectionLost);
    expect(pair.dialer.isOpen).toBe(false);
  });

  it('never puts reset code 9 on the wire', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    pair.dialer.open({ q: 1 });
    await pair.settle();
    pair.dialerSocket.drop();
    await pair.settle();

    (['dialer', 'acceptor'] as const).forEach((who) => {
      pair.sentBy(who).forEach((frame) => {
        expect(frame.code).not.toBe(ResetCode.CONNECTION_CLOSED);
      });
    });
  });
});

// --------------------------------------------------------------------------- observability

describe('observability', () => {
  it('shows onFrame both directions with byte lengths - WSM-OBS-003', async () => {
    const pair = makePair();
    pair.acceptor.onStream(replyNow);
    const seen: [string, string, number][] = [];
    pair.dialer.onFrame((direction, frame, length) => {
      seen.push([direction, frame.type, length]);
    });
    pair.start();

    await pair.dialer.request({ q: 1 });
    await pair.settle();

    expect(seen[0][0]).toBe('tx');
    expect(seen[0][1]).toBe('open');
    expect(seen.some(([direction]) => direction === 'rx')).toBe(true);
    seen.forEach(([, , length]) => {
      expect(length).toBeGreaterThan(0);
    });
  });

  it('never puts payload contents in a log record - WSM-OBS-002', async () => {
    const pair = makePair();
    pair.acceptor.onStream(replyNow);
    const seen: Frame[] = [];
    pair.dialer.onFrame((direction, frame) => {
      seen.push(frame);
    });
    pair.start();

    expect(await pair.dialer.request({ password: 'hunter2-sentinel' })).toEqual({ ok: true });
    await pair.settle();

    // Non-vacuity: the secret really did travel through the frame path, so there was something to
    // leak. What this cannot show is the *debug* line Python inspects: the shim's level defaults to
    // 'warn' and is module-private, so `logger.debug` never reaches `console.debug` here at all.
    expect(seen.some((frame) => JSON.stringify(frame).includes('hunter2-sentinel'))).toBe(true);
    LEVELS.forEach((level) => {
      logged[level].forEach((line) => {
        expect(line).not.toContain('hunter2-sentinel');
      });
    });
  });
});

// --------------------------------------------------------------------------- edges

describe('edges', () => {
  it('reports its role readably', () => {
    const pair = makePair();
    expect(pair.dialer.isDialer).toBe(true);
    expect(pair.acceptor.isDialer).toBe(false);
    expect(String(pair.dialer)).toContain('dialer');
    expect(String(pair.acceptor)).toContain('acceptor');
  });

  it('raises when a request ends without a payload', async () => {
    // A unary call that returned nothing would be indistinguishable from one that returned null.
    const pair = makePair();
    pair.acceptor.onStream(async (payload: unknown, stream: Stream) => {
      await stream.end();
    });
    pair.start();

    const error = await rejection(pair.dialer.request({ q: 1 }));
    expect(error).toBeInstanceOf(ProtocolError);
    expect((error as ProtocolError).message).toContain('without a payload');
  });

  it('resets and raises when a request deadline expires - WSM-ERR-011', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    const error = await rejection(pair.dialer.request({ q: 1 }, { timeoutMs: 20 }));
    expect(error).toBeInstanceOf(StreamTimeout);
    await pair.settle();
    expect(pair.sentBy('dialer').some((frame) => frame.type === 'reset' && frame.code === ResetCode.TIMEOUT)).toBe(
      true,
    );
  });

  it('completes a request whose deadline does not expire', async () => {
    const pair = makePair();
    pair.acceptor.onStream(replyNow);
    pair.start();
    expect(await pair.dialer.request({ q: 1 }, { timeoutMs: 5_000 })).toEqual({ ok: true });
  });

  it('takes the binary branch from codec.binary, never from sniffing - WSM-CDC-002/WSM-API-021', async () => {
    const pair = makePair({ codec: new BinaryJsonCodec() });
    const sendText = vi.spyOn(pair.dialerSocket, 'sendText');
    pair.acceptor.onStream(replyNow);
    pair.start();

    expect(await pair.dialer.request({ q: 1 })).toEqual({ ok: true });
    expect(pair.dialerSocket.sent.length).toBeGreaterThan(0);
    expect(pair.dialerSocket.sent.every((message) => message instanceof ArrayBuffer)).toBe(true);
    expect(sendText).not.toHaveBeenCalled();
  });

  (['open', 'data', 'reset'] as const).forEach((frameType) => {
    it(`kills the connection on a ${frameType} frame naming no stream`, async () => {
      const pair = makePair();
      pair.acceptor.onStream(hold);
      pair.start();

      pair.injectRaw('acceptor', `{"type":"${frameType}","code":0}`);
      await pair.settle();

      expect(pair.acceptor.isOpen).toBe(false);
      expect(pair.framesOfType('acceptor', 'goaway').length).toBeGreaterThan(0);
    });
  });

  it('reassembles a fragmented data payload for the consumer', async () => {
    const rows = Array.from({ length: 20 }, (_value, index) => index);
    const pair = makePair();
    pair.acceptor.onStream((payload: unknown, stream: Stream) => {
      const encoded = pair.codec.encodePayload({ rows }) as string;
      const half = Math.floor(encoded.length / 2);
      pair.acceptor.enqueue({ type: 'data', stream: stream.id, fragment: encoded.slice(0, half), more: true });
      pair.acceptor.enqueue({ type: 'data', stream: stream.id, fragment: encoded.slice(half), end: true });
    });
    pair.start();

    expect(await pair.dialer.open({ q: 1 })).toEqual({ rows });
  });

  it('half-closes a fragmented open that ends on its last fragment', async () => {
    const pair = makePair();
    const states: StreamState[] = [];
    pair.acceptor.onStream((payload: unknown, stream: Stream) => {
      states.push(stream.state);
    });
    pair.start();

    const encoded = pair.codec.encodePayload({ body: 'y'.repeat(30) }) as string;
    const half = Math.floor(encoded.length / 2);
    pair.inject('acceptor', { type: 'open', stream: 1, fragment: encoded.slice(0, half), more: true });
    await pair.settle();
    pair.inject('acceptor', { type: 'open', stream: 1, fragment: encoded.slice(half), end: true });
    await pair.settle();

    expect(states).toEqual([StreamState.HALF_CLOSED_REMOTE]);
  });

  it("ends quietly when a handler's stream died mid-flight - WSM-STM-035", async () => {
    const pair = makePair();
    let finished = false;
    pair.acceptor.onStream(async (payload: unknown, stream: Stream) => {
      await Promise.resolve();
      await stream.reset(ResetCode.NO_ERROR);
      finished = true;
    });
    pair.start();

    pair.dialer.open({ q: 1 });
    await pair.settle();

    expect(finished).toBe(true);
    expect(pair.acceptor.isOpen).toBe(true);
  });

  it('resets the stream but not the peer on an unknown reset code', async () => {
    // A peer of another generation - or one still using the retired 5 - must be heard, not crashed on.
    // Regression: converting the wire value straight to a `ResetCode` threw out of the read loop,
    // which left `isOpen` true, `onClose` unfired, and every pending await hanging.
    for (const wireCode of [5, 42]) {
      const pair = makePair();
      pair.acceptor.onStream(hold);
      pair.start();
      const closes: CloseReason[] = [];
      pair.dialer.onClose((reason) => closes.push(reason));

      const stream = pair.dialer.open({ q: 1 });
      await pair.settle();
      pair.injectRaw('dialer', `{"type":"reset","stream":${stream.id},"code":${wireCode},"reason":"old peer"}`);
      await pair.settle();

      expect(pair.dialer.isOpen, `code ${wireCode} must not kill the connection`).toBe(true);
      expect(closes).toEqual([]);
      const error = await rejection(stream);
      expect(error).toBeInstanceOf(StreamReset);
      expect((error as StreamReset).code).toBe(wireCode);
      expect(stream.signal.aborted).toBe(true);
    }
  });

  it('closes the peer when the read loop fails rather than zombifying it', async () => {
    // A peer that reports itself open while its read loop is dead is worse than one that closed.
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();
    const closes: CloseReason[] = [];
    pair.dialer.onClose((reason) => closes.push(reason));

    internals(pair.dialer).dispatch = async () => {
      throw new Error('simulated bug in dispatch');
    };
    const stream = pair.dialer.open({ q: 1 });
    await pair.settle();
    pair.injectRaw('dialer', '{"type":"data","stream":1,"payload":{}}');
    await pair.settle();

    expect(pair.dialer.isOpen).toBe(false);
    expect(closes).toHaveLength(1);
    expect(stream.signal.aborted).toBe(true);
  });
});

// --------------------------------------------------------------------------- audit regressions

describe('audit regressions', () => {
  it('does not let a wrong-parity open pose as a fragment continuation - WSM-SID-005', async () => {
    // Regression: "is this a continuation?" was inferred from whether *some* assembler was running on
    // that id. A stream this peer opened, receiving fragmented `data`, therefore accepted an `open`
    // carrying our own parity as a continuation - dispatching a handler for a stream we opened, and
    // skipping the connection-level error the rule requires.
    const pair = makePair();
    const handled: unknown[] = [];
    pair.acceptor.onStream((payload: unknown) => {
      handled.push(payload);
    });
    pair.start();

    pair.acceptor.open({ mine: true }); // the acceptor's own stream 2
    await pair.settle();

    const encoded = pair.codec.encodePayload({ x: 1 }) as string;
    pair.inject('acceptor', { type: 'data', stream: 2, fragment: encoded.slice(0, 3), more: true });
    await pair.settle();
    pair.inject('acceptor', { type: 'open', stream: 2, fragment: encoded.slice(3) });
    await pair.settle();

    expect(handled, 'a handler ran for a stream the local peer opened').toEqual([]);
    expect(pair.acceptor.isOpen).toBe(false);
    expect(pair.framesOfType('acceptor', 'goaway').length).toBeGreaterThan(0);
  });

  it('keeps a duplicate open a connection error mid-reassembly', async () => {
    // The same hole from the other side: a re-used id must not be laundered by a fragment.
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    const encoded = pair.codec.encodePayload({ x: 1 }) as string;
    pair.inject('acceptor', { type: 'open', stream: 3, fragment: encoded.slice(0, 3), more: true });
    await pair.settle();
    pair.inject('acceptor', { type: 'open', stream: 1, fragment: encoded.slice(3) });
    await pair.settle();

    expect(pair.acceptor.isOpen, 'an open below the high-water mark must be ILL-C').toBe(false);
  });

  it("fails an unencodable frame's stream without wedging the connection", async () => {
    // Regression: a codec that could not encode a frame took the writer down in silence. Nothing
    // drained the queue afterwards, every later send sat in it forever, and the peer went on
    // reporting itself open - the same shape as the read-loop zombie, from the other end.
    const pair = makePair();
    pair.acceptor.onStream(replyNow);
    pair.start();

    const stream = pair.dialer.open({ q: 1 });
    await pair.settle();
    // Bytes are not a payload type under JSON, and the codec refuses rather than base64-ing them.
    await stream.send({ blob: new Uint8Array([0, 255]).buffer });
    await pair.settle();

    expect(pair.dialer.isOpen, 'one unencodable frame must not end the connection').toBe(true);
    const error = await rejection(stream);
    expect(error).toBeInstanceOf(StreamReset);
    expect((error as StreamReset).code).toBe(ResetCode.INTERNAL_ERROR);

    // The writer is still alive: a later request still completes.
    expect(await pair.dialer.request({ q: 2 })).toEqual({ ok: true });
  });

  it('still produces the reset when the errorSerializer itself throws - WSM-STM-034', async () => {
    const pair = makePair({
      errorSerializer: () => {
        throw new Error('the serializer itself is broken');
      },
    });
    pair.acceptor.onStream(() => {
      throw new HandlerFailure('handler said no');
    });
    pair.start();

    const error = await rejection(pair.dialer.request({ q: 1 }));
    expect(error).toBeInstanceOf(RemoteError);
    expect((error as RemoteError).code).toBe(ResetCode.APPLICATION_ERROR);
    const reset = pair.lastFrameOfType('acceptor', 'reset');
    expect(reset.code).toBe(ResetCode.APPLICATION_ERROR);
    expect(reset.reason).toBe('handler said no');
    expect(reset.payload).toBe(ABSENT);
  });

  it('hands each send on a reset stream its own failure object', async () => {
    // Python's failure mode is a traceback that grows a frame on every re-raise of one stored
    // instance; JavaScript's is one shared mutable object, with one `stack` that no longer describes
    // where it was thrown, handed to every caller. They are the same bug.
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    const stream = pair.dialer.open({ q: 1 });
    await pair.settle();
    await stream.cancel();

    const failures: StreamReset[] = [];
    for (let attempt = 0; attempt < 5; attempt += 1) {
      failures.push((await rejection(stream.send({ late: true }))) as StreamReset);
    }

    expect(failures).toHaveLength(5);
    failures.forEach((failure) => {
      expect(failure).toBeInstanceOf(StreamReset);
      expect(failure.code).toBe(ResetCode.CANCELLED);
    });
    expect(new Set(failures).size, 'every caller was handed one shared error object').toBe(5);
    const depths = failures.map((failure) => String(failure.stack ?? '').split('\n').length);
    expect(new Set(depths).size, `the stack grows on every raise: ${depths.join(', ')}`).toBe(1);
  });
});

// --------------------------------------------------------------------------- TypeScript-only

describe("open()'s overloads", () => {
  it('accepts payload, payload + options, or options alone - WSM-API-020', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    pair.dialer.open({ q: 1 });
    pair.dialer.open({ q: 1 }, {});
    pair.dialer.open({ payload: { q: 1 } });
    await pair.settle();

    const opens = pair.framesOfType('dialer', 'open');
    expect(opens).toHaveLength(3);
    expect(opens.map((frame) => frame.stream)).toEqual([1, 3, 5]);

    // The three forms differ only in the id they were allocated.
    const normalised = opens.map((frame) => ({ ...frame, stream: 0 }));
    expect(framesEqual(normalised[0], normalised[1])).toBe(true);
    expect(framesEqual(normalised[0], normalised[2])).toBe(true);
    expect(normalised[0].payload).toEqual({ q: 1 });
  });

  it('carries headers and end through the options-only form', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    pair.dialer.open({ payload: { q: 2 }, headers: { h: 'v' }, end: true });
    await pair.settle();

    const open = pair.lastFrameOfType('dialer', 'open');
    expect(open.payload).toEqual({ q: 2 });
    expect(open.headers).toEqual({ h: 'v' });
    expect(open.end).toBe(true);
  });

  it('gives RequestOptions a deadline and OpenOptions none - WSM-API-018', async () => {
    // Compile-time, not runtime: `tsc` rejects the file if either answer changes.
    type Has<T, K extends string> = K extends keyof T ? true : false;
    const openHasTimeout: Has<OpenOptions, 'timeoutMs'> = false;
    const requestHasTimeout: Has<RequestOptions, 'timeoutMs'> = true;
    expect(openHasTimeout).toBe(false);
    expect(requestHasTimeout).toBe(true);

    // And it cannot sneak in at runtime either: `timeoutMs` is not one of `open()`'s option keys, so
    // an object carrying it is a payload, and the deadline is visibly not honoured.
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();
    pair.dialer.open({ timeoutMs: 5 } as unknown as OpenOptions);
    await pair.settle();
    expect(pair.lastFrameOfType('dialer', 'open').payload).toEqual({ timeoutMs: 5 });
  });
});

// --------------------------------------------------------------------------- codec double

/**
 * A binary codec, declared binary rather than inferred from what it emits (WSM-CDC-002).
 *
 * Composition rather than `extends JsonCodec`: `JsonCodec.binary` is `readonly false`, so a subclass
 * cannot narrow it to `true` without a type error.
 */
class BinaryJsonCodec implements Codec {
  readonly name = 'json';

  readonly binary = true;

  private readonly inner = new JsonCodec();

  encode(frame: Frame): ArrayBuffer {
    const bytes = new TextEncoder().encode(this.inner.encode(frame));
    // The copy is not ceremony. Under jsdom `TextEncoder` is Node's global and `ArrayBuffer` is
    // jsdom's intrinsic, so `bytes.buffer instanceof ArrayBuffer` is false purely because the two
    // come from different realms. Allocating through the ambient constructor is what a real browser
    // codec hands to `socket.send`, and keeps `instanceof` a statement about the send branch rather
    // than about which realm built the buffer.
    const buffer = new ArrayBuffer(bytes.byteLength);
    new Uint8Array(buffer).set(bytes);
    return buffer;
  }

  decode(message: string | ArrayBuffer): Frame {
    return this.inner.decode(message);
  }

  encodePayload(payload: unknown): string {
    return this.inner.encodePayload(payload);
  }

  decodePayload(data: string | ArrayBuffer): unknown {
    return this.inner.decodePayload(data);
  }
}

describe('the frame logger', () => {
  /**
   * The mirror of Python's `caplog.at_level` half of `test_unknown_frame_type_is_ignored` and
   * `test_payload_contents_never_appear_in_a_log_record`. Both were previously asserted only by
   * their negative half - nothing went out, nothing reached the console - because the level was a
   * module-private constant starting at 'warn' and no seam existed to raise it.
   */
  it('logs an unknown frame type exactly once when the level allows it - WSM-FRM-002', async () => {
    const original = logger.level;
    const lines: string[] = [];
    const spy = vi.spyOn(console, 'info').mockImplementation((message: unknown) => {
      lines.push(String(message));
    });
    logger.level = 'info';

    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();
    try {
      pair.acceptorSocket.inject('{"type":"widget","stream":1}');
      await pair.settle();

      expect(lines.filter((line) => line.includes('widget'))).toHaveLength(1);
      expect(pair.acceptor.isOpen).toBe(true);
    } finally {
      logger.level = original;
      spy.mockRestore();
      await pair.stop();
    }
  });

  it('never puts payload contents in a log line, at any level - WSM-OBS-002', async () => {
    const original = logger.level;
    const written: string[] = [];
    const spies = (['debug', 'info', 'warn', 'error'] as const).map((level) =>
      vi.spyOn(console, level).mockImplementation((...args: unknown[]) => {
        written.push(args.map(String).join(' '));
      }),
    );
    logger.level = 'debug';

    const pair = makePair();
    pair.acceptor.onStream(async (_p, stream) => stream.reply({ ok: true }));
    pair.start();
    try {
      await pair.dialer.request({ password: 'hunter2-sentinel' });
      await pair.settle();
      // Application data routinely contains secrets, and a frame line is emitted for every frame.
      expect(written.join('\n')).not.toContain('hunter2-sentinel');
      expect(written.length, 'the level was raised, so something must have been logged').toBeGreaterThan(0);
    } finally {
      logger.level = original;
      spies.forEach((spy) => spy.mockRestore());
      await pair.stop();
    }
  });
});
