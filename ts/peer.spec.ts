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
import {
  ConnectionClosed,
  ConnectionLost,
  ProtocolError,
  RemoteError,
  ResetCode,
  StreamReset,
  StreamTimeout,
} from './errors';
import { ABSENT, type Frame, V1_FRAME_TYPES, framesEqual, toMapping } from './frames';
import { MAX_STREAM_ID } from './lifecycle';
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
import type { SocketAdapter } from './transports';
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
  /** The next id `open()` will allocate, so a test can walk the peer up to WSM-SID-007's ceiling. */
  nextId: number;
  highestLocalOpen: number;
  highestRemoteOpen: number;
  /** M5a's writer, so a test can see what the peer is still holding for a socket that is gone. */
  writer: { depth: number; lanes: number };
  dispatch(frame: Frame): Promise<boolean>;
  /** M5b's socket-death fan-out and the socket a reconnected `Peer` takes next. */
  adoptSocket(socket: SocketAdapter): void;
  die(cause: ConnectionClosed): void;
  willRetry: boolean;
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
 * Python's `Lone` fixture: one peer, fed a raw message by hand, with no counterpart at all.
 *
 * A live pair cannot be asked what a receiver does with a frame no correct sender would produce -
 * the counterpart sees the answer to a stream it never opened and kills the connection, correctly
 * and entirely beside the point.
 *
 * Reports the three things a "changes nothing" assertion needs: everything the peer put on the wire,
 * what its handler was handed, and whether it survived.
 */
async function replayIntoLonePeer(
  message: string,
): Promise<{ wire: string[]; received: Frame[]; seen: unknown[]; alive: boolean }> {
  const [, acceptorSide] = memoryPair();
  const peer = new Peer(acceptorSide, { codec: new JsonCodec(), isDialer: false });
  const seen: unknown[] = [];
  const received: Frame[] = [];
  peer.onStream(async (payload: unknown, stream: Stream) => {
    seen.push(payload);
    await stream.reply({ ok: true });
  });
  peer.onFrame((direction, frame) => {
    if (direction === 'rx') received.push(frame);
  });
  const served = peer.serve();
  void served.catch(() => undefined);
  try {
    acceptorSide.inject(message);
    for (let turn = 0; turn < 12; turn += 1) {
      await new Promise<void>((resolve) => {
        setTimeout(resolve, 0);
      });
    }
    return { wire: acceptorSide.sent.map((sent) => String(sent)), received, seen, alive: peer.isOpen };
  } finally {
    acceptorSide.drop();
    await served.catch(() => undefined);
  }
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
    // All three halves of the rule in one test, because each alone passes for the wrong reason: a
    // peer that never received the frame is also alive and also quiet, and one that answered it with
    // a `reset` also logged something.
    const pair = makePair();
    pair.acceptor.onStream(hold);
    // Non-vacuity. Without a witness that the frame *arrived*, every assertion below would hold just
    // as well against a peer that never received it, which is not what "ignored" means.
    const arrived: Frame[] = [];
    pair.acceptor.onFrame((direction, frame) => {
      if (direction === 'rx') arrived.push(frame);
    });
    pair.start();

    // Python raises `muxws.frames` to INFO with `caplog.at_level`; M5a gave the TypeScript shim the
    // same seam, so the "logged once" half is assertable here rather than only in Python. Below
    // 'info' the single `logger.info` call in `dispatch` reaches no console at all and the count
    // would be zero for a reason that has nothing to do with the peer.
    const level = logger.level;
    logger.level = 'info';
    const before = pair.sentBy('acceptor').length;
    try {
      // `stream=1` is above this acceptor's high-water mark, so an implementation that let an
      // unrecognised type fall through to the stream-frame path kills the connection (WSM-STM-003)
      // rather than failing some subtler assertion.
      pair.injectRaw('acceptor', '{"type":"widget","stream":1}');
      await pair.settle();
    } finally {
      logger.level = level;
    }

    expect(arrived.filter((frame) => frame.type === 'widget')).toHaveLength(1);
    expect(pair.acceptor.isOpen).toBe(true);
    expect(pair.sentBy('acceptor'), 'an unknown type is never answered').toHaveLength(before);
    expect(
      logged.info.filter((line) => line.includes('widget')),
      'logged exactly once',
    ).toHaveLength(1);
    // An unknown frame is not an error, so nothing may reach the levels an operator watches.
    (['debug', 'warn', 'error'] as const).forEach((quiet) => {
      expect(logged[quiet], `an ignored frame wrote to console.${quiet}`).toEqual([]);
    });
  });

  it('ignores an unknown envelope field - WSM-FRM-001', async () => {
    // `frames.spec.ts` proves the decoder drops the key. That is not the rule: a receiver could drop
    // it and still behave differently - refuse the frame, warn, take a slower path. So the same
    // exchange is replayed twice, once with `"colour": "red"` on the `open` and once without, and
    // the two peers' *whole wires* are compared. Equality across the two runs is the only
    // formulation that cannot pass while the extra field is quietly acted on downstream, and it is
    // what WSM-CON-009 rests on: additive revisions are safe to receive precisely because of this.
    const tainted = await replayIntoLonePeer('{"type":"open","stream":1,"payload":{"q":1},"end":true,"colour":"red"}');
    const clean = await replayIntoLonePeer('{"type":"open","stream":1,"payload":{"q":1},"end":true}');

    // Non-vacuity: two silent peers also have equal wires. The handler must have run and answered.
    expect(clean.seen).toEqual([{ q: 1 }]);
    expect(clean.wire.length, 'the control exchange produced no frames at all').toBeGreaterThan(0);
    expect(tainted.alive).toBe(true);
    expect(clean.alive).toBe(true);
    expect(tainted.seen, 'the payload the handler saw').toEqual(clean.seen);
    expect(tainted.wire, 'an unknown envelope field changed what went out').toEqual(clean.wire);
    // The wire is the loudest witness but not the finest: a decoder that let the extra key disturb a
    // *quiet* field - `end`, `more`, `code` - can answer this one exchange identically and the next
    // one differently. `onFrame` fires after decode and before dispatch (WSM-OBS-003), so this
    // compares the frames the peer actually acted on, field for field.
    expect(tainted.received).toHaveLength(clean.received.length);
    tainted.received.forEach((frame, index) => {
      expect(framesEqual(frame, clean.received[index]), 'the frame the peer dispatched').toBe(true);
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

/** What one shape needs: a peer to open on, and the two moments the runner synchronises it against. */
class Rig {
  stream: Stream | null = null;

  constructor(
    readonly name: string,
    readonly peer: Peer,
    private readonly watched: Stream[],
    /** Say this shape is in position. The socket is not dropped until every shape has said so. */
    readonly arm: () => void,
    /** Resolves once the socket has been dropped, for the shapes that act only afterwards. */
    readonly dropped: Promise<void>,
  ) {}

  open(): Stream {
    this.stream = this.peer.open({ shape: this.name });
    this.watched.push(this.stream);
    return this.stream;
  }
}

async function awaitShape(rig: Rig): Promise<void> {
  const stream = rig.open();
  rig.arm();
  await stream;
}

async function resultShape(rig: Rig): Promise<void> {
  const stream = rig.open();
  rig.arm();
  await stream.result();
}

async function iterateShape(rig: Rig): Promise<void> {
  const stream = rig.open();
  rig.arm();
  for await (const item of stream) void item;
  // A clean end would read as "the export finished", which is exactly the lie WSM-RCN-041 forbids.
  throw new Error('an async for must raise on socket death, not terminate normally');
}

async function requestShape(rig: Rig): Promise<void> {
  rig.arm();
  await rig.peer.request({ shape: rig.name });
}

async function sendShape(rig: Rig): Promise<void> {
  const stream = rig.open();
  rig.arm();
  for (;;) {
    // Mid-send when the socket dies, rather than sending once and waiting: a sender that had already
    // stopped would be testing `send()` on a closed stream, which is a different rule.
    await stream.send({ shape: rig.name });
    // A macrotask and not `Promise.resolve()`: a loop of resolved microtasks never yields to the
    // timer queue, so the drop this shape is waiting for could never happen and it would starve the
    // whole test instead of being killed by it. Python's `asyncio.sleep(0)` buys the same turn.
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 0);
    });
  }
}

async function cancelShape(rig: Rig): Promise<void> {
  const stream = rig.open();
  rig.arm();
  await rig.dropped;
  await stream.cancel();
  await stream.reset(ResetCode.NO_ERROR);
}

/**
 * Every shape WSM-RCN-041 names, with what the socket dying under it must do to that shape. **This
 * map is the only place they are listed**: the runner below opens one stream per entry, waits for all
 * of them to be armed, kills the socket once, and checks each outcome against the expectation
 * recorded here - so adding a seventh shape is one line in one place (§6).
 */
const DEATH_SHAPES: Record<string, { run: (rig: Rig) => Promise<void>; expected: typeof ConnectionLost | null }> = {
  'await stream': { run: awaitShape, expected: ConnectionLost },
  'await stream.result()': { run: resultShape, expected: ConnectionLost },
  'async for': { run: iterateShape, expected: ConnectionLost },
  'in-flight request()': { run: requestShape, expected: ConnectionLost },
  'sender mid-send()': { run: sendShape, expected: ConnectionLost },
  // `cancel()` and `reset()` are the shapes WSM-RCN-041 makes **no-ops** rather than failures, so
  // `null` is the expectation. They belong here anyway: "does not raise" is worth nothing if it
  // hangs, and hanging is what this test detects.
  'a stream awaiting cancel()': { run: cancelShape, expected: null },
};

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
    // One stream per shape, all of them live at the same instant, and one socket death underneath all
    // of them at once. What each shape must do is read from `DEATH_SHAPES` rather than written out
    // here, so the list of shapes has exactly one home.
    //
    // **This test fails by hang detection.** A shape that never finishes is named in the failure,
    // because the failure WSM-INV-011 describes is not an exception: it is a caller who sees no
    // error, no log and no timeout, just a spinner that never stops.
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
      let dropped: () => void = () => undefined;
      const hasDropped = new Promise<void>((resolve) => {
        dropped = resolve;
      });
      const armed: Promise<void>[] = [];
      const rigs = new Map<string, Rig>();
      const outcomes = new Map<string, Promise<unknown>>();

      Object.entries(DEATH_SHAPES).forEach(([name, shape]) => {
        let arm: () => void = () => undefined;
        armed.push(
          new Promise<void>((resolve) => {
            arm = resolve;
          }),
        );
        const rig = new Rig(name, pair.dialer, watched, arm, hasDropped);
        rigs.set(name, rig);
        outcomes.set(name, settled(shape.run(rig)));
      });

      // Nothing dies until every shape says it is in position, or the test would be asserting about
      // whichever shapes happened to be ready.
      await within('arming', Promise.all(armed), 5_000);
      await pair.settle();

      pair.dialerSocket.drop();
      dropped();
      await pair.settle();

      for (const [name, shape] of Object.entries(DEATH_SHAPES)) {
        const outcome = await within(name, outcomes.get(name) as Promise<unknown>);
        if (shape.expected === null) {
          expect(outcome, `${name} must be a no-op after death, and it produced ${String(outcome)}`).toBeUndefined();
        } else {
          expect(outcome, `${name} ended with ${String(outcome)}`).toBeInstanceOf(shape.expected);
        }
      }

      await within('closed', settled(Promise.all(watched.map((stream) => stream.closed))));
      watched.forEach((stream) => {
        expect(stream.signal.aborted).toBe(true);
      });

      // The memoized future was resolved once, so a second await gets the same error (WSM-API-010).
      const awaited = rigs.get('await stream')?.stream as Stream;
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
    expect(snapshot.allClosed, 'onClose ran before every stream closed').toEqual(watched.map(() => true));
    expect([...new Set(snapshot.allFailed)]).toEqual([StreamState.CLOSED]);
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

  it('gives a second await after death the same error - WSM-RCN-041/WSM-API-010', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    const stream = pair.dialer.open({ q: 1 });
    await pair.settle();
    pair.dialerSocket.drop();
    await pair.settle();

    // The memoized future is resolved **once**. A second await that hung, or that raised something
    // different, would mean the failure had been delivered rather than recorded.
    const first = await within('await', rejection((async () => stream)()));
    const second = await within('second await', rejection((async () => stream)()));
    expect(first).toBeInstanceOf(ConnectionLost);
    expect(second).toBeInstanceOf(ConnectionLost);
    expect((first as ConnectionLost).code).toBe(ResetCode.CONNECTION_CLOSED);
    expect((second as ConnectionLost).code).toBe((first as ConnectionLost).code);
  });

  it('raises out of an async for rather than terminating it normally - WSM-RCN-041', async () => {
    const pair = makePair();
    pair.acceptor.onStream(async (payload, stream) => {
      await stream.send({ row: 0 });
      await stream.closed;
    });
    pair.start();

    const stream = pair.dialer.open({ q: 1 });
    const collected: unknown[] = [];
    const consuming = settled(
      (async () => {
        for await (const item of stream) collected.push(item);
        // A clean end would read as "the export finished", which is exactly the lie the rule forbids.
        throw new Error('the loop terminated normally instead of raising ConnectionLost');
      })(),
    );
    await pair.settle();
    pair.dialerSocket.drop();
    await pair.settle();

    expect(await within('iterate', consuming)).toBeInstanceOf(ConnectionLost);
    expect(collected, 'what did arrive is still delivered').toEqual([{ row: 0 }]);
  });

  it('makes cancel and reset no-ops after death - WSM-RCN-041', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    const stream = pair.dialer.open({ q: 1 });
    await pair.settle();
    const framesBefore = pair.sentBy('dialer').length;
    pair.dialerSocket.drop();
    await pair.settle();

    // No-ops, not errors: the caller asked for something the socket already did, and neither may put
    // a frame on a wire that is gone.
    expect(await within('cancel', settled(stream.cancel()))).toBeUndefined();
    expect(await within('reset', settled(stream.reset(ResetCode.NO_ERROR)))).toBeUndefined();
    expect(pair.sentBy('dialer')).toHaveLength(framesBefore);
    expect(stream.signal.aborted).toBe(true);
    expect(stream.state).toBe(StreamState.CLOSED);
  });

  it('puts nothing attempted while disconnected on the new socket - WSM-RCN-042/WSM-INV-010', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    pair.dialerSocket.drop();
    await pair.settle();

    for (let index = 0; index < 5; index += 1) {
      expect(() => pair.dialer.open({ attempt: index })).toThrow(ConnectionLost);
      expect(await rejection(pair.dialer.notify({ attempt: index }))).toBeInstanceOf(ConnectionLost);
      expect(await rejection(pair.dialer.request({ attempt: index }))).toBeInstanceOf(ConnectionLost);
    }

    // A queue that flushed into the next socket would deliver work the server has forgotten the
    // sender of, turning a failure that would have reached a call site into silent misdelivery.
    const [fresh] = memoryPair();
    pair.dialer.adoptSocket(fresh);
    const serving = pair.dialer.serve();
    void serving.catch(() => undefined);
    await pair.settle();

    expect(fresh.sent, 'the new socket must carry none of what was attempted in the gap').toEqual([]);
    fresh.drop();
    await serving.catch(() => undefined);
  });

  it("discards the writer's queues when the socket dies - WSM-RCN-042/WSM-INV-010", async () => {
    // M5a shipped `discardAll()` and M5b is what calls it. `ts/writer.spec.ts` proves the method
    // empties the queues when it is called, which is a different claim from proving that socket death
    // reaches it - and until this test, deleting the call from `die()` would have failed nothing
    // anywhere. That is exactly the failure M5a shipped once already.
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    for (let index = 0; index < 5; index += 1) {
      pair.dialer.open({ body: 'x'.repeat(5000), n: index });
    }
    // Not settled first, deliberately: the queues have to still be full at the instant the socket
    // dies, or a writer that had already drained them would make this pass against a peer that holds
    // everything for the next socket.
    expect(internals(pair.dialer).writer.depth, 'the queues must be full at the moment of death').toBeGreaterThan(0);

    pair.dialerSocket.drop();
    await pair.settle();

    // Nothing is held for a next socket: a queue that flushed into it would deliver work the server
    // has forgotten the sender of, turning a failure that would have reached a call site into silent
    // misdelivery (WSM-INV-010).
    expect(internals(pair.dialer).writer.depth, 'frames were kept for a socket that will never carry them').toBe(0);
    expect(internals(pair.dialer).writer.lanes).toBe(0);
  });

  it('reports isOpen false for the whole gap - WSM-RCN-043', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    expect(pair.dialer.isOpen).toBe(true);
    pair.dialerSocket.drop();
    await pair.settle();
    expect(pair.dialer.isOpen, 'false from the loss until the next connection, not until the next call').toBe(false);

    const [fresh] = memoryPair();
    pair.dialer.adoptSocket(fresh);
    expect(pair.dialer.isOpen).toBe(true);
    fresh.drop();
  });

  it("clears the dead socket's exhaustion shutdown on adopt - WSM-SID-007/WSM-RCN-031", async () => {
    // Running out of stream ids schedules an orderly shutdown and remembers it, so a second `open()`
    // cannot schedule a second one. That memory belongs to the socket that ran out and not to the
    // `Peer`, which outlives it: carried into the next connection it makes `beginExhaustionShutdown`
    // a no-op there, and a socket that exhausts its ids never says goodbye and never closes - it
    // refuses every `open()` from then on while reporting itself perfectly healthy.
    // `muxws/socket_death_test.py` asserts the same on the field Python resets on the line above.
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    internals(pair.dialer).nextId = MAX_STREAM_ID;
    pair.dialer.open({ q: 1 });
    const first = pair.dialer.exhaustionShutdown;
    expect(first).not.toBeNull();

    pair.dialerSocket.drop();
    await pair.settle();
    await first?.catch(() => undefined);

    const [fresh] = memoryPair();
    pair.dialer.adoptSocket(fresh);
    expect(pair.dialer.exhaustionShutdown, "the dead socket's shutdown must not carry forward").toBeNull();

    internals(pair.dialer).nextId = MAX_STREAM_ID;
    pair.dialer.open({ q: 2 });
    expect(pair.dialer.exhaustionShutdown, 'and the new socket schedules its own').not.toBeNull();
    // Nothing reads `fresh`, so dropping it tells nobody and the second shutdown would sit out its
    // whole drain window waiting for a stream no read loop will ever end (WSM-CON-024). Killing the
    // peer is what ends that wait here; that the shutdown *started* is the whole claim.
    pair.dialer.die(new ConnectionClosed('the test is over', { code: 1006 }));
    await pair.dialer.exhaustionShutdown?.catch(() => undefined);
  });

  it('fires onClose once per loss with the right willRetry - WSM-RCN-040/045', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    const seen: CloseReason[] = [];
    pair.dialer.onClose((reason) => seen.push(reason));
    pair.start();

    pair.dialerSocket.drop();
    await pair.settle();
    pair.dialerSocket.drop();
    // The heartbeat and the read loop are two code paths that can both notice one dead socket, and
    // the reconnect helper calls `die()` from the first of them. Whichever arrives second must add
    // nothing: `onClose` fires exactly once per loss, not once per witness.
    pair.dialer.die(new ConnectionClosed('the heartbeat noticed the same death', { code: 1006 }));
    await pair.settle();

    expect(seen, 'one loss, one call - a second witness of a dead socket is not a second loss').toHaveLength(1);
    expect(Object.keys(seen[0]).sort()).toEqual(['code', 'reason', 'wasClean', 'willRetry']);
    // Nothing retries until the helper says so: `willRetry` reads the intention set when the socket
    // was established, and this peer has no helper at all.
    expect(seen[0].willRetry).toBe(false);
  });

  it('says willRetry true only while the helper intends to dial again - WSM-RCN-040', async () => {
    const retrying = makePair();
    retrying.acceptor.onStream(hold);
    const seen: CloseReason[] = [];
    retrying.dialer.onClose((reason) => seen.push(reason));
    retrying.dialer.willRetry = true;
    retrying.start();
    retrying.dialerSocket.drop();
    await retrying.settle();
    expect(seen[0].willRetry).toBe(true);

    // A deliberate close never says it will retry, whatever the helper meant a moment earlier.
    const deliberate = makePair();
    deliberate.acceptor.onStream(hold);
    const closes: CloseReason[] = [];
    deliberate.dialer.onClose((reason) => closes.push(reason));
    deliberate.dialer.willRetry = true;
    deliberate.start();
    await deliberate.dialer.close();
    await deliberate.settle();
    expect(closes[0].willRetry).toBe(false);
    expect(closes[0].wasClean).toBe(true);
  });

  it('does not let a stream survive a reconnect - WSM-RCN-031/032', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    const held = pair.dialer.open({ q: 1 });
    await pair.settle();
    pair.dialerSocket.drop();
    await pair.settle();

    const [fresh] = memoryPair();
    pair.dialer.adoptSocket(fresh);

    // `Peer` survives a reconnect; `Stream` objects do not, and nothing is replayed on the new one.
    expect(held.signal.aborted).toBe(true);
    expect(pair.dialer.streams.size, "the new socket's id space starts empty").toBe(0);
    expect(pair.dialer.open({ q: 2 }).id, 'and the allocator starts over').toBe(1);
    fresh.drop();
  });

  it('advances peer.id on every reconnect - WSM-API-009', async () => {
    const pair = makePair();
    pair.start();
    const seen = [pair.dialer.id];
    const sockets: MemorySocket[] = [];

    for (let round = 0; round < 3; round += 1) {
      pair.dialerSocket.drop();
      await pair.settle();
      const [fresh] = memoryPair();
      sockets.push(fresh);
      pair.dialer.adoptSocket(fresh);
      seen.push(pair.dialer.id);
    }

    // A reconnect reads as a new `conn=` in a log rather than as one continuous connection, and no
    // id from a dropped socket is handed out again.
    expect(new Set(seen).size, 'no id may repeat').toBe(seen.length);
    const counters = seen.map((id) => Number(id.split('-')[1]));
    expect(counters, 'the counter never rewinds').toEqual([...counters].sort((a, b) => a - b));
    sockets.forEach((socket) => socket.drop());
  });

  it('never puts reset code 9 on the wire', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    pair.dialer.open({ q: 1 });
    pair.dialer.open({ q: 2 });
    await pair.settle();
    const sentBefore = pair.sentBy('dialer').length;

    // Declared dead on a socket that is still perfectly **writable** - which is exactly what the
    // reconnect helper does when a pong is swallowed. Dropping the socket first and then looking for
    // code 9 proves nothing at all: nothing can reach a wire that is already gone, so that version
    // of this test passes against an implementation that resets every stream on the wire.
    pair.dialer.die(new ConnectionClosed('declared dead with the socket still up', { code: 1006 }));
    await pair.settle();

    expect(
      pair.sentBy('dialer'),
      'the synthesised reset is local: nothing whatever is sent for it (WSM-STM-014)',
    ).toHaveLength(sentBefore);
    (['dialer', 'acceptor'] as const).forEach((who) => {
      pair.sentBy(who).forEach((frame) => {
        expect(frame.code).not.toBe(ResetCode.CONNECTION_CLOSED);
      });
    });

    pair.dialerSocket.drop();
    await pair.settle();
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

  it('takes zero mandatory arguments and defaults payload to null - WSM-API-003', async () => {
    // A promise made to every caller and, until this test, checked by nothing. `open()` on its own is
    // the one-line push shape from section 5.1's call-shapes note; a required argument would break it
    // at the call site, and a payload defaulting to *absent* rather than `null` would put a
    // different frame on the wire from Python's `payload=None` - the two ports must agree.
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    const stream = pair.dialer.open();
    expect(stream.id).toBe(1);
    await pair.settle();

    const open = pair.lastFrameOfType('dialer', 'open');
    // `"payload": null` is on the wire, not an absent key: an absent `payload` is a different frame
    // (section 2.2), and Python's `open()` sends the null.
    expect('payload' in toMapping(open), 'open() sent no payload key at all').toBe(true);
    expect(open.payload).toBeNull();
    expect(open.headers ?? null).toBeNull();
    expect(open.end ?? false).toBe(false);
    expect(stream.payload).toBeNull();

    // The remote sees an open, so nothing about the empty call is a local-only shortcut.
    expect(pair.acceptor.streams.size).toBe(1);
  });

  it('accepts no stream id at any public entry point - WSM-SID-001', async () => {
    // The parity test covers *which* id the library picks. This covers the other half: a caller
    // cannot pick one. Every spelling an application might reach for is checked, because the rule is
    // about the whole API surface rather than about one keyword - and in TypeScript an options object
    // is structural, so an unexpected key is silently ignored rather than rejected by the compiler.
    const idKeys = ['stream', 'streamId', 'stream_id', 'id'];

    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    idKeys.forEach((key) => {
      // As the lone argument the object is not one of `open()`'s option keys, so it is a *payload*
      // (WSM-API-020) - the id lands in application data, where it means nothing.
      pair.dialer.open({ [key]: 9999 });
      // As the options argument it is an unknown key and is dropped by `resolveCall`.
      pair.dialer.open({ q: 1 }, { [key]: 9999 } as unknown as OpenOptions);
    });
    await pair.settle();

    const opens = pair.framesOfType('dialer', 'open');
    expect(opens).toHaveLength(idKeys.length * 2);
    // Ids stayed the library's own: odd, monotonic, allocated in call order, and never 9999.
    expect(opens.map((frame) => frame.stream)).toEqual([1, 3, 5, 7, 9, 11, 13, 15]);

    // And the caller's number is visible only where it belongs, as opaque payload on the odd ones.
    idKeys.forEach((key, index) => {
      expect(opens[index * 2].payload).toEqual({ [key]: 9999 });
      expect(opens[index * 2 + 1].payload).toEqual({ q: 1 });
    });
  });

  it('accepts no stream id on request, notify or the stream sends - WSM-SID-001', async () => {
    const pair = makePair();
    pair.acceptor.onStream(replyNow);
    pair.start();

    await pair.dialer.request({ q: 1 }, { stream: 9999 } as unknown as RequestOptions);
    await pair.dialer.notify({ q: 2 }, { stream: 9999 } as unknown as { headers?: Record<string, unknown> });
    const stream = pair.dialer.open({ q: 3 });
    await stream.send({ q: 4 }, { stream: 9999 } as unknown as { end?: boolean });
    await stream.end({ stream: 9999 } as unknown as { trailers?: Record<string, unknown> });
    await pair.settle();

    // Every stream-level frame this peer sent names an id it allocated itself: odd, and one of the
    // three it opened. Nothing anywhere carried the caller's 9999.
    const sent = pair.sentBy('dialer').filter((frame) => frame.stream !== null && frame.stream !== undefined);
    expect(sent.length).toBeGreaterThan(0);
    sent.forEach((frame) => {
      expect([1, 3, 5]).toContain(frame.stream);
    });
  });

  it('has no default request timeout - WSM-ERR-010', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    // A default deadline can only be a timer, and `request()` schedules its timer synchronously
    // before it returns (see `collectUnary`). Counting scheduled timers is therefore an exact
    // witness that does not need a test to wait out whatever the default would have been.
    const timers = vi.spyOn(globalThis, 'setTimeout');
    const forever = pair.dialer.request({ q: 1 });
    void forever.catch(() => undefined);
    expect(timers, 'request() with no timeout scheduled a deadline').not.toHaveBeenCalled();

    const explicit = pair.dialer.request({ q: 2 }, {});
    void explicit.catch(() => undefined);
    expect(timers, 'an empty options object supplied a deadline from somewhere').not.toHaveBeenCalled();

    // The counter can see a deadline when there is one, so its silence above means something.
    const deadlined = pair.dialer.request({ q: 3 }, { timeoutMs: 20 });
    expect(timers).toHaveBeenCalledTimes(1);
    timers.mockRestore();

    await expect(deadlined).rejects.toThrow(StreamTimeout);
    await pair.settle();

    // Only the deadlined stream was reset, and only that one; the other two are still live and
    // waiting, which is what "a stream lives until it ends, is reset, or the connection dies" means.
    expect(pair.framesOfType('dialer', 'reset').map((frame) => frame.stream)).toEqual([5]);
    expect(pair.framesOfType('dialer', 'reset')[0].code).toBe(ResetCode.TIMEOUT);
    expect(pair.dialer.streams.size).toBe(2);
    let settled = false;
    void Promise.race([forever, explicit]).then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    await pair.settle();
    expect(settled, 'a request nobody answered resolved on its own').toBe(false);
  });

  it('never interprets per-stream headers - WSM-AUT-002', async () => {
    // Headers exist for the application. The rule is a *negative*: whatever an opener puts there must
    // arrive unchanged and must change nothing the library does - including keys that look like
    // authentication, which is exactly where a library is tempted to grow a re-auth hook (WSM-AUT-001
    // puts authentication at the upgrade, before `accept()` is ever called).
    const headers = {
      authorization: 'Bearer expired.and.invalid',
      cookie: 'session=deleted',
      'x-api-key': '',
      'sec-websocket-protocol': 'muxws.v1.msgpack',
      // Envelope field names as header keys: still just keys.
      stream: 9999,
      end: true,
      code: 3,
      type: 'reset',
      nested: { deep: [1, null, 'two'] },
    };

    // Snapshotted before the send for the same reason the payload test snapshots: a library that
    // consumed a header by deleting it from the caller's object would pass a comparison against the
    // object it mutilated.
    const expected = JSON.parse(JSON.stringify(headers)) as typeof headers;

    const seen: Record<string, unknown>[] = [];
    const pair = makePair();
    pair.acceptor.onStream(async (_payload: unknown, stream: Stream) => {
      seen.push(stream.headers);
      await stream.reply({ ok: true });
    });
    pair.start();

    const withHeaders = await pair.dialer.request({ q: 1 }, { headers });
    const without = await pair.dialer.request({ q: 1 });
    await pair.settle();

    // Unchanged on the wire, and unchanged again by the time the opener's counterpart reads them.
    expect(pair.framesOfType('dialer', 'open')[0].headers).toEqual(expected);
    expect(seen[0]).toEqual(expected);
    expect(headers, "the caller's own headers object was modified in flight").toEqual(expected);

    // And they changed nothing: the same answer, the same frames, the same peer state as the open
    // that carried none. Comparing the two exchanges is the assertion - a library that had reacted
    // to `authorization` would differ here even if it left the object itself alone.
    expect(withHeaders).toEqual(without);
    const opens = pair.framesOfType('dialer', 'open');
    expect(framesEqual({ ...opens[0], stream: 0, headers: null }, { ...opens[1], stream: 0 })).toBe(true);

    // The whole exchange, both directions, with the ids normalised away: the two requests produced
    // the same frames in the same order, so nothing branched on a header. `authorization` was the
    // most tempting branch and there is no branch at all.
    const trace = (who: Who, id: number): unknown[] =>
      pair
        .sentBy(who)
        .filter((frame) => frame.stream === id)
        .map((frame) => ({ ...toMapping(frame), stream: 0, headers: undefined }));
    expect(trace('dialer', 1)).toEqual(trace('dialer', 3));
    expect(trace('acceptor', 1)).toEqual(trace('acceptor', 3));

    expect(pair.framesOfType('dialer', 'reset')).toHaveLength(0);
    expect(pair.framesOfType('acceptor', 'reset')).toHaveLength(0);
    expect(pair.framesOfType('acceptor', 'goaway')).toHaveLength(0);
    expect(pair.acceptor.isOpen).toBe(true);
  });

  it('defines no vocabulary inside payload - WSM-FRM-006/WSM-INV-016', async () => {
    // Every envelope field name, used as a payload key, plus the discriminators a library of this
    // shape is usually tempted to reserve. If any of these meant anything to muxws, two consumers
    // sharing one socket would have to nest their own vocabulary inside an imposed one.
    const payload = {
      type: 'reset',
      stream: 9999,
      end: true,
      more: true,
      code: ResetCode.PROTOCOL_ERROR,
      reason: 'not a reason',
      nonce: 'not a nonce',
      last_stream: 4242,
      fragment: 'not a fragment',
      headers: { authorization: 'nope' },
      trailers: { checksum: 'nope' },
      payload: { kind: 'nested', $type: 'Envelope', _muxws: true },
      kind: 'command',
    };

    // The comparison is against a snapshot taken before the send, never against `payload` itself: a
    // library that consumed a reserved key by deleting it from the caller's own object would leave
    // both sides of `toEqual(payload)` equally mutilated and the assertion would pass.
    const expected = JSON.parse(JSON.stringify(payload)) as typeof payload;

    const seen: unknown[] = [];
    const pair = makePair();
    pair.acceptor.onStream(async (received: unknown, stream: Stream) => {
      seen.push(received);
      // Echoed back, so the round trip is asserted in both directions rather than only outbound.
      await stream.reply(received);
    });
    pair.start();

    const echoed = await pair.dialer.request(payload);
    await pair.settle();

    expect(seen[0]).toEqual(expected);
    expect(echoed).toEqual(expected);
    expect(payload, "the caller's own object was modified in flight").toEqual(expected);

    // The envelope is untouched by what the payload says. `end` is true because `request()` set it,
    // `stream` is the library's own 1 rather than the payload's 9999, and no `code` field appeared.
    const open = pair.lastFrameOfType('dialer', 'open');
    expect(open.type).toBe('open');
    expect(open.stream).toBe(1);
    expect(open.payload).toEqual(expected);
    expect('code' in toMapping(open)).toBe(false);
    expect('trailers' in toMapping(open)).toBe(false);
    expect('nonce' in toMapping(open)).toBe(false);

    // Nothing acted on it either: no stream 9999, no reset carrying the payload's PROTOCOL_ERROR, no
    // ping answered for its "nonce", and the connection is still up on both sides.
    const streams = new Set(pair.sentBy('dialer').map((frame) => frame.stream));
    expect(streams.has(9999)).toBe(false);
    expect(pair.framesOfType('dialer', 'reset')).toHaveLength(0);
    expect(pair.framesOfType('acceptor', 'reset')).toHaveLength(0);
    expect(pair.framesOfType('acceptor', 'pong')).toHaveLength(0);
    expect(pair.framesOfType('acceptor', 'goaway')).toHaveLength(0);
    expect(pair.dialer.isOpen && pair.acceptor.isOpen).toBe(true);
  });

  it('puts only the six v1 frame types on the wire - WSM-FRM-012/WSM-FRM-013', async () => {
    // The constant is asserted by set equality in `frames.spec.ts`; this is the behavioural half.
    // Ending a stream and attaching trailers must produce a `data` frame with flags on it, never a
    // seventh frame type - which is the only way the absence of an `end` type is visible on a wire.
    const pair = makePair();
    pair.acceptor.onStream(async (_payload: unknown, stream: Stream) => {
      await stream.send({ chunk: 1 });
      await stream.end({ trailers: { checksum: 'deadbeef' } });
    });
    pair.start();

    // `end: true` on the open: the dialer has nothing to say, so `close()` below has nothing to
    // drain and the test is not waiting out a drain window.
    const stream = pair.dialer.open({ q: 1 }, { end: true });
    const received: unknown[] = [];
    for await (const item of stream) received.push(item);
    await pair.dialer.ping();
    await pair.dialer.close({ reason: 'done' });
    await pair.settle();

    const emitted = [...pair.sentBy('dialer'), ...pair.sentBy('acceptor')];
    expect(emitted.length).toBeGreaterThan(5);
    emitted.forEach((frame) => {
      expect(V1_FRAME_TYPES.has(frame.type), `${frame.type} is not one of the six v1 frame types`).toBe(true);
    });

    // The stream ended, and it ended as a flagged `data` frame carrying its trailers.
    const closing = pair.lastFrameOfType('acceptor', 'data');
    expect(closing.end).toBe(true);
    expect(closing.trailers).toEqual({ checksum: 'deadbeef' });
    expect(received).toEqual([{ chunk: 1 }]);
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

describe('WSM-RCN-044 is per connection, not per Peer object', () => {
  it('lets a new socket report its own final close', async () => {
    // The latch that stops one loss being reported twice must not outlive the connection it was set
    // on. A `Peer` handed a fresh socket is a new connection, and its next loss is a loss nobody has
    // heard about - left unreset, it is reported to nobody at all.
    const [dialerSide] = memoryPair();
    const peer = new Peer(dialerSide, { codec: new JsonCodec(), isDialer: true });
    const closes: CloseReason[] = [];
    peer.onClose((reason) => closes.push(reason));

    internals(peer).willRetry = false;
    internals(peer).die(new ConnectionClosed('the first socket died', { code: 1006 }));
    expect(
      closes.map((reason) => reason.willRetry),
      'the first loss reports, once',
    ).toEqual([false]);

    const [replacement] = memoryPair();
    internals(peer).adoptSocket(replacement);
    internals(peer).willRetry = false;
    internals(peer).die(new ConnectionClosed('the second socket died too', { code: 1006 }));

    expect(
      closes.map((reason) => reason.willRetry),
      "the second connection's loss was swallowed by a latch left over from the first",
    ).toEqual([false, false]);
  });
});
