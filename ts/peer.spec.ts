/**
 * `Peer`: id allocation, retention, dispatch, and the effect of socket death on every stream shape.
 *
 * Mirrors `muxws/peer_test.py` test for test, using `ts/transports/memory.ts`. Where an assertion differs from
 * Python it uses the JavaScript observable for the same fact - `stream.signal.aborted` for Python's
 * `stream.closed.is_set()`, a `console` spy for `caplog` - and a comment at that place names it.
 */

// `describe`/`it`/`expect` are configured as globals; `vi` is imported because the shared eslint
// config does not know it as one.
import { vi } from 'vitest';

import { type Codec, JsonCodec } from './codec';
import {
  ConnectionClosed,
  ConnectionGoingAway,
  ConnectionLost,
  ProtocolError,
  RemoteError,
  ResetCode,
  StreamClosed,
  StreamReset,
  StreamTimeout,
} from './errors';
import { ABSENT, type Frame, V1_FRAME_TYPES, framesEqual, toMapping } from './frames';
import { MAX_STREAM_ID } from './lifecycle';
import { MsgpackCodec } from './msgpack';
import {
  type CloseReason,
  closeFrameReason,
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
    // `serve()` returns a promise that `stop()` awaits; there is no `asyncio.Task`. The rejection handler is
    // attached immediately so a read loop that fails mid-test is not reported as an unhandled rejection before the
    // assertions inspect it.
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
function makePair(
  options: {
    errorSerializer?: ErrorSerializer;
    codec?: Codec;
    maxFrameBytes?: number;
    /** The adapter the acceptor uses in place of its `MemorySocket`; `sentBy` still reads the `MemorySocket`. */
    wrapAcceptor?: (socket: MemorySocket) => SocketAdapter;
  } = {},
): Pair {
  const [left, right] = memoryPair();
  const codec = options.codec ?? new JsonCodec();
  const { errorSerializer, maxFrameBytes } = options;
  const pair = new Pair(
    new Peer(left, { codec, isDialer: true, errorSerializer, maxFrameBytes }),
    new Peer(options.wrapAcceptor?.(right) ?? right, { codec, isDialer: false, errorSerializer, maxFrameBytes }),
    left,
    right,
    codec,
  );
  built.push(pair);
  return pair;
}

/**
 * The peer's private bookkeeping (Python: `_ignored_late_frames` and related fields).
 *
 * WSM-STM-001 and WSM-STM-002 are statements about what the peer retains; the wire alone cannot distinguish
 * "ignored" from "not received".
 */
interface PeerInternals {
  ignoredLateFrames: number;
  /** The next id `open()` will allocate, so a test can walk the peer up to WSM-SID-007's ceiling. */
  nextId: number;
  highestLocalOpen: number;
  highestRemoteOpen: number;
  /** The writer, so a test can inspect what the peer holds for a socket that is closed. */
  writer: { depth: number; lanes: number };
  dispatch(frame: Frame): Promise<boolean>;
  /** The socket-death fan-out, and the socket a reconnected `Peer` takes next. */
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

/** Python's `_hold`: keep the stream open until the other side closes it. */
async function hold(payload: unknown, stream: Stream): Promise<void> {
  await stream.closed;
}

/** Python's `_reply_now`. */
async function replyNow(payload: unknown, stream: Stream): Promise<void> {
  await stream.reply({ ok: true });
}

/**
 * Python's `Lone` fixture: one peer with no counterpart, fed a raw message directly.
 *
 * A live pair cannot test how a receiver handles a frame no correct sender produces: the counterpart receives the
 * answer to a stream it never opened and kills the connection, which is unrelated to the assertion.
 *
 * Returns what a "changes nothing" assertion needs: every message the peer wrote, the frames it received, what its
 * handler was given, and whether the peer is still open.
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

/** Equivalent of `pytest.raises`: the rejection reason, or a test failure if the promise resolves. */
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
  // Any other object that reports its own size is counted too. The peer's outbound `AsyncQueue` is such a
  // container: a frame retained per closed stream would be stored there, where a check limited to `Map`, `Set` and
  // `Array` does not see it.
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
    // Fifty opens from fifty separate tasks. If anything suspended between allocating the id and queueing the
    // frame, the wire would carry a non-monotonic id sequence, which is a protocol error by this peer.
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
    // The test closes a peer before creating the next ones. Counting distinct ids among peers that were never
    // closed also passes for a scheme that reuses the ids of closed connections, which is the scheme the rule
    // forbids.
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

    // The scan reports a request that is still in flight, so the empty result above reflects the peer's state and
    // not a scan that inspects nothing.
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
    // All three parts of the rule are asserted in one test. Each part alone also holds for a non-conforming peer: a
    // peer that never received the frame is alive and quiet, and a peer that answered it with a `reset` also logs.
    const pair = makePair();
    pair.acceptor.onStream(hold);
    // Assert first that the frame arrived; otherwise every assertion below also holds for a peer that never
    // received it.
    const arrived: Frame[] = [];
    pair.acceptor.onFrame((direction, frame) => {
      if (direction === 'rx') arrived.push(frame);
    });
    pair.start();

    // Python raises `muxws.frames` to INFO with `caplog.at_level`; `logger.level` is the equivalent setting. Below
    // 'info' the single `logger.info` call in `dispatch` reaches no console and the count is zero regardless of the
    // peer's behaviour.
    const level = logger.level;
    logger.level = 'info';
    const before = pair.sentBy('acceptor').length;
    try {
      // `stream=1` is above this acceptor's high-water mark, so an implementation that passes an unrecognised type
      // to the stream-frame path kills the connection (WSM-STM-003) and the test fails directly.
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
    // `frames.spec.ts` checks that the decoder drops the key. The rule is broader: a receiver that drops the key
    // can still behave differently (refuse the frame, warn, take a slower path). The same exchange therefore runs
    // twice, once with `"colour": "red"` on the `open` and once without, and the complete wires of both peers are
    // compared. Equal wires show that the extra field has no effect downstream. WSM-CON-009 depends on this:
    // additive revisions are safe to receive because receivers ignore unknown fields.
    const tainted = await replayIntoLonePeer('{"type":"open","stream":1,"payload":{"q":1},"end":true,"colour":"red"}');
    const clean = await replayIntoLonePeer('{"type":"open","stream":1,"payload":{"q":1},"end":true}');

    // Two silent peers also have equal wires, so the handler must have run and answered.
    expect(clean.seen).toEqual([{ q: 1 }]);
    expect(clean.wire.length, 'the control exchange produced no frames at all').toBeGreaterThan(0);
    expect(tainted.alive).toBe(true);
    expect(clean.alive).toBe(true);
    expect(tainted.seen, 'the payload the handler saw').toEqual(clean.seen);
    expect(tainted.wire, 'an unknown envelope field changed what went out').toEqual(clean.wire);
    // Wire equality does not cover every field: a decoder that let the extra key change a field such as `end`,
    // `more` or `code` could produce an identical answer for this exchange and a different one for the next.
    // `onFrame` fires after decode and before dispatch (WSM-OBS-003), so this compares the decoded frames the peer
    // acted on, field by field.
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
    // REFUSED states that the operation did not happen. A handler that debits an account and then throws would,
    // under REFUSED, invite the client to retry the debit.
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

  // JavaScript has two values for "nothing" where Python has one. A serializer written as `(error) => { if
  // (isPublic(error)) return describe(error); }` returns `undefined` on the redacting path, so `undefined` and
  // `null` must both mean "no payload", otherwise the redacted data is sent.
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

  it('replaces the handler on a second onStream() call and logs one warning - WSM-STM-030', async () => {
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
    /** Signal that this shape is ready. The socket is dropped only after every shape has signalled. */
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
  // A clean end would report a completed export; WSM-RCN-041 requires an error.
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
    // The shape is mid-send when the socket dies. A sender that had already stopped would test `send()` on a closed
    // stream, which is a different rule.
    await stream.send({ shape: rig.name });
    // The loop yields with a macrotask, not `Promise.resolve()`: a loop of resolved microtasks never yields to the
    // timer queue, so the socket drop it waits for would never run and the loop would block the whole test.
    // Python's `asyncio.sleep(0)` yields the same way.
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
 * Every shape WSM-RCN-041 names, with the required outcome when the socket dies under it. **This map is the only
 * list of shapes**: the runner below opens one stream per entry, waits until all are armed, kills the socket once,
 * and checks each outcome against the expectation here. Adding a seventh shape is one entry in this map (§6).
 */
const DEATH_SHAPES: Record<string, { run: (rig: Rig) => Promise<void>; expected: typeof ConnectionLost | null }> = {
  'await stream': { run: awaitShape, expected: ConnectionLost },
  'await stream.result()': { run: resultShape, expected: ConnectionLost },
  'async for': { run: iterateShape, expected: ConnectionLost },
  'in-flight request()': { run: requestShape, expected: ConnectionLost },
  'sender mid-send()': { run: sendShape, expected: ConnectionLost },
  // WSM-RCN-041 makes `cancel()` and `reset()` no-ops rather than failures, so their expectation is `null`. They
  // are included because the test also detects a no-op that hangs.
  'a stream awaiting cancel()': { run: cancelShape, expected: null },
};

describe('socket death', () => {
  it('names the shape that hung rather than timing out anonymously - WSM-INV-011', async () => {
    // The next test detects failure with a hang deadline, so this test checks the deadline helper. A `within` that
    // awaited forever would turn a hung shape into a vitest timeout naming only the file, which is the anonymous
    // failure WSM-INV-011 describes.
    const never = new Promise<never>(() => undefined);
    const caught = await rejection(within('iterate', never, 10));

    expect(caught).toBeInstanceOf(Error);
    expect((caught as Error).message).toContain("the 'iterate' shape hung");
    expect((caught as Error).message).toContain('WSM-INV-011');
    // `within` resolves normally when the work completes, so it reports no hang that did not happen.
    expect(await within('settles', Promise.resolve('done'), 10)).toBe('done');
  });

  it('fails every shape and none of them hangs - WSM-RCN-041/WSM-STM-014', async () => {
    // One stream per shape, all live at the same time, and one socket death under all of them. The expected outcome
    // of each shape is read from `DEATH_SHAPES`, the single list of shapes.
    //
    // **This test fails by hang detection.** A shape that never finishes is named in the failure. The failure
    // WSM-INV-011 describes raises no exception: the caller sees no error, no log and no timeout, and the operation
    // never completes.
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    const closes: CloseReason[] = [];
    const watched: Stream[] = [];
    /**
     * The state at the instant `onClose` ran. WSM-STM-014 requires the synthesised failures to happen before
     * `onClose`; a handler that fired first would observe streams still open, and counting calls alone does not
     * distinguish the two orderings.
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

      // No socket dies until every shape has signalled that it is ready; otherwise the test would cover only the
      // shapes that happened to be ready.
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

    // The memoized future is resolved **once**. A second await that hung, or that raised a different error, would
    // mean the failure was delivered to one awaiter instead of being stored.
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
        // A clean end would report a completed export; the rule requires an error.
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

    // No-ops, not errors: the socket has already ended the stream, and neither call may write a frame to a socket
    // that is closed.
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

    // A queue flushed into the next socket would deliver work whose sender the server no longer knows, so a failure
    // that would have reached the call site would become a silent misdelivery.
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
    // `ts/writer.spec.ts` checks that `discardAll()` empties the queues when called; this test checks that socket
    // death calls it, and fails if the call is removed from `die()`.
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    for (let index = 0; index < 5; index += 1) {
      pair.dialer.open({ body: 'x'.repeat(5000), n: index });
    }
    // Not settled first: the queues must still be full when the socket dies, otherwise a writer that had already
    // drained them makes this pass against a peer that keeps everything for the next socket.
    expect(internals(pair.dialer).writer.depth, 'the queues must be full at the moment of death').toBeGreaterThan(0);

    pair.dialerSocket.drop();
    await pair.settle();

    // Nothing is held for a next socket: a queue flushed into it would deliver work whose sender the server no
    // longer knows, so a failure that would have reached the call site would become a silent misdelivery
    // (WSM-INV-010).
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
    // Running out of stream ids schedules an orderly shutdown and records it, so a second `open()` does not
    // schedule another. The record belongs to the socket that ran out, not to the `Peer`, which outlives it.
    // Carried into the next connection, it makes `beginExhaustionShutdown` a no-op there: a socket that exhausts
    // its ids then never sends `goaway` and never closes, and refuses every `open()` while `isOpen` stays true.
    // `muxws/socket_death_test.py` asserts the same for the corresponding Python field.
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
    // Nothing reads `fresh`, so dropping it is not detected and the second shutdown would wait its whole drain
    // window for a stream that no read loop ends (WSM-CON-024). Killing the peer ends that wait; the assertion is
    // only that the shutdown started.
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
    // The heartbeat and the read loop can both detect one dead socket, and the reconnect helper calls `die()` from
    // the heartbeat. The second detection has no effect: `onClose` fires exactly once per loss.
    pair.dialer.die(new ConnectionClosed('the heartbeat noticed the same death', { code: 1006 }));
    await pair.settle();

    expect(seen, 'a second detection of the same dead socket fires no second onClose').toHaveLength(1);
    expect(Object.keys(seen[0]).sort()).toEqual(['code', 'reason', 'wasClean', 'willRetry']);
    // `willRetry` reads the retry intention set when the socket was established; this peer has no reconnect helper,
    // so nothing retries.
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

    // An explicit close reports `willRetry` false, whatever the helper intended before.
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

    // A reconnect appears as a new `conn=` in logs, and no id from a dropped socket is reused.
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

    // The peer is declared dead on a socket that is still **writable**, as the reconnect helper does when a pong is
    // lost. Dropping the socket first and then looking for code 9 would prove nothing: no frame reaches a closed
    // socket, so that variant passes against an implementation that resets every stream on the wire.
    pair.dialer.die(new ConnectionClosed('declared dead with the socket still up', { code: 1006 }));
    await pair.settle();

    expect(pair.sentBy('dialer'), 'the synthesised reset is local: nothing is sent for it (WSM-STM-014)').toHaveLength(
      sentBefore,
    );
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

    // The secret did pass through the frame path, so there was data to leak. This test does not cover the debug
    // line Python inspects: the shim's level defaults to 'warn' and is module-private, so `logger.debug` never
    // reaches `console.debug` here.
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

  it("ends without error when the handler's stream closed before the handler returned - WSM-STM-035", async () => {
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
    // A reset code from another protocol version, or the unassigned code 5, resets the stream and leaves the peer
    // running. Converting the wire value directly to a `ResetCode` throws out of the read loop, which leaves
    // `isOpen` true, `onClose` unfired, and every pending await hanging.
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

  it('closes the peer when the read loop fails', async () => {
    // A peer whose read loop has failed must close; a peer that reports itself open in that state leaves every
    // caller waiting.
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
    expect(closes[0].code).toBe(1011);
    expect(stream.signal.aborted).toBe(true);
    expect(pair.dialerSocket.isClosed, 'a peer that reports itself closed has closed its socket').toBe(true);
    expect(pair.framesOfType('dialer', 'goaway').map((frame) => frame.code)).toEqual([ResetCode.INTERNAL_ERROR]);
  });

  it('closes the socket when the read loop itself throws', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();
    const closes: CloseReason[] = [];
    pair.dialer.onClose((reason) => closes.push(reason));

    (internals(pair.dialer) as unknown as { reportFrame: () => void }).reportFrame = () => {
      throw new Error('simulated bug outside dispatch');
    };
    pair.injectRaw('dialer', '{"type":"ping","nonce":"n"}');
    await pair.settle();

    expect(pair.dialer.isOpen).toBe(false);
    expect(closes.map((reason) => reason.code)).toEqual([1011]);
    expect(pair.dialerSocket.isClosed).toBe(true);
  });
});

// --------------------------------------------------------------------------- shutdown

/** Delegates to a `MemorySocket`. With `suspend`, every send waits 1 ms before it reaches the wire. */
class WrappedSocket implements SocketAdapter {
  constructor(
    private readonly inner: MemorySocket,
    private readonly options: { suspend?: boolean; failingClose?: boolean } = {},
  ) {}

  async sendText(text: string): Promise<void> {
    if (this.options.suspend === true) await sleepMs(1);
    this.inner.sendText(text);
  }

  async sendBytes(bytes: ArrayBuffer): Promise<void> {
    if (this.options.suspend === true) await sleepMs(1);
    this.inner.sendBytes(bytes);
  }

  receive(): Promise<string | ArrayBuffer> {
    return this.inner.receive();
  }

  close(code?: number, reason?: string): void {
    this.inner.close(code, reason);
    if (this.options.failingClose === true) throw new Error('the adapter failed to close');
  }
}

function sleepMs(ms: number): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

/** [label, send suspends, reply size]: a socket whose send suspends, and a reply of more than 1000 fragments. */
const DRAIN_CASES: [string, boolean, number][] = [
  ['a send that suspends', true, 4_000],
  ['more than 1000 fragments', false, 200_000],
];

function expectReplyAndGoawaySent(frames: Frame[]): void {
  const pieces = frames.filter((frame) => frame.type === 'data');
  expect(pieces.length, 'the reply must have been fragmented').toBeGreaterThan(1);
  const last = pieces[pieces.length - 1];
  expect(last.more === true, 'the last fragment of the reply must be on the wire').toBe(false);
  expect(last.end).toBe(true);
  expect(frames.some((frame) => frame.type === 'goaway')).toBe(true);
}

/** Start `pair` with an acceptor handler that replies with `payload`; resolves once the reply is enqueued. */
function replyOnce(pair: Pair, payload: unknown): Promise<void> {
  return new Promise<void>((resolve) => {
    pair.acceptor.onStream(async (_payload: unknown, stream: Stream) => {
      await stream.reply(payload);
      resolve();
    });
    pair.start();
  });
}

describe('shutdown', () => {
  it.each(DRAIN_CASES)(
    'close() sends every queued fragment before closing the socket, with %s - WSM-CON-025/WSM-FRG-017',
    async (_label, suspend, size) => {
      const pair = makePair({ maxFrameBytes: 200, wrapAcceptor: (socket) => new WrappedSocket(socket, { suspend }) });
      const blob = { blob: 'x'.repeat(size) };
      const replied = replyOnce(pair, blob);
      const stream = pair.dialer.open({ q: 1 }, { end: true });
      await replied;
      await pair.acceptor.close({ drainMs: 5000 });

      expectReplyAndGoawaySent(pair.sentBy('acceptor'));
      expect(await stream.result()).toEqual(blob);
    },
    15_000,
  );

  it.each(DRAIN_CASES)(
    'a connection failure closes the socket only after goaway and every queued fragment are sent, with %s - ILL-C',
    async (_label, suspend, size) => {
      const pair = makePair({ maxFrameBytes: 200, wrapAcceptor: (socket) => new WrappedSocket(socket, { suspend }) });
      const replied = replyOnce(pair, { blob: 'x'.repeat(size) });
      pair.dialer.open({ q: 1 }, { end: true });
      await replied;
      pair.injectRaw('acceptor', '{"type":"open","code":0}');
      for (let turn = 0; turn < 2000 && pair.acceptor.isOpen; turn += 1) await sleepMs(1);

      expect(pair.acceptor.isOpen).toBe(false);
      const frames = pair.sentBy('acceptor');
      expectReplyAndGoawaySent(frames);
      expect(frames.filter((frame) => frame.type === 'goaway').map((frame) => frame.code)).toEqual([
        ResetCode.PROTOCOL_ERROR,
      ]);
    },
    15_000,
  );

  it('a connection failure refuses local opens while it drains - WSM-CON-021/ILL-C', async () => {
    const pair = makePair({
      maxFrameBytes: 200,
      wrapAcceptor: (socket) => new WrappedSocket(socket, { suspend: true }),
    });
    const replied = replyOnce(pair, { blob: 'x'.repeat(40_000) });
    pair.dialer.open({ q: 1 }, { end: true });
    await replied;
    pair.injectRaw('acceptor', '{"type":"open","code":0}');
    const goawaySent = (): boolean => pair.sentBy('acceptor').some((frame) => frame.type === 'goaway');
    for (let turn = 0; turn < 2000 && !goawaySent(); turn += 1) await sleepMs(1);

    expect(pair.acceptor.isOpen, 'the drain must still be sending the reply').toBe(true);
    expect(() => pair.acceptor.open({ q: 2 })).toThrow(ConnectionGoingAway);
  }, 15_000);

  it('a read-loop failure refuses local opens while it drains - WSM-CON-021', async () => {
    const pair = makePair({
      maxFrameBytes: 200,
      wrapAcceptor: (socket) => new WrappedSocket(socket, { suspend: true }),
    });
    const replied = replyOnce(pair, { blob: 'x'.repeat(40_000) });
    pair.dialer.open({ q: 1 }, { end: true });
    await replied;
    internals(pair.acceptor).dispatch = async () => {
      throw new Error('simulated bug in dispatch');
    };
    pair.injectRaw('acceptor', '{"type":"ping","nonce":"n"}');
    const goawaySent = (): boolean => pair.sentBy('acceptor').some((frame) => frame.type === 'goaway');
    for (let turn = 0; turn < 2000 && !goawaySent(); turn += 1) await sleepMs(1);

    expect(pair.acceptor.isOpen, 'the drain must still be sending the reply').toBe(true);
    expect(() => pair.acceptor.open({ q: 2 })).toThrow(ConnectionGoingAway);
  }, 15_000);

  it('close() marks the peer dead when the adapter close throws - WSM-CON-025/WSM-STM-014', async () => {
    const pair = makePair({ wrapAcceptor: (socket) => new WrappedSocket(socket, { failingClose: true }) });
    pair.acceptor.onStream(hold);
    const closes: CloseReason[] = [];
    pair.acceptor.onClose((reason) => closes.push(reason));
    pair.start();
    const stream = pair.acceptor.open({ q: 1 });
    await pair.settle();
    await pair.acceptor.close({ drainMs: 10 });

    expect(pair.acceptor.isOpen).toBe(false);
    expect(stream.signal.aborted).toBe(true);
    expect(closes.map((reason) => reason.code)).toEqual([1000]);
    expect(logged.warn.some((line) => line.includes('closing the socket failed'))).toBe(true);
  });
});

// --------------------------------------------------------------------------- hostile frames and failing hooks

describe('hostile frames and failing hooks', () => {
  it('does not let a wrong-parity open pose as a fragment continuation - WSM-SID-005', async () => {
    // Whether a frame is a continuation must not be inferred from whether some assembler is active on that id.
    // Otherwise a stream this peer opened, and receives fragmented `data` on, would accept an `open` with this
    // peer's own parity as a continuation: it would dispatch a handler for a stream this peer opened and skip the
    // connection-level error the rule requires.
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
    // The same check for a duplicate open: a reused id is a connection error even while a fragment is being
    // reassembled on it.
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

  it.each([
    '{"type":"open","stream":"x"}',
    '{"type":"open","stream":"3"}',
    '{"type":"open","stream":-1}',
    '{"type":"open","stream":1,"headers":"x"}',
    '{"type":"open","stream":1,"fragment":5}',
    '{"type":"open","stream":1,"end":"yes"}',
    '{"type":"data","stream":true}',
    '{"type":"ping","nonce":5}',
    '{"type":5}',
  ])('fails the connection on an envelope field of the wrong type %s - WSM-FRM-005/WSM-STM-023', async (message) => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    pair.injectRaw('acceptor', message);
    await pair.settle();

    expect(pair.framesOfType('acceptor', 'goaway').map((frame) => frame.code)).toEqual([ResetCode.PROTOCOL_ERROR]);
    expect(pair.acceptor.isOpen).toBe(false);
    expect(pair.acceptorSocket.isClosed).toBe(true);
  });

  it('closes the socket with a reason that fits a close frame on a connection-level error - WSM-STM-023', async () => {
    // The socket throws on a reason over 123 UTF-8 bytes, as `ws` does. A text message on a msgpack connection
    // produces a longer reason; the `goaway` and `onClose` carry it whole.
    const [, inner] = memoryPair();
    const closedWith: [number, string][] = [];
    const socket: SocketAdapter = {
      sendText: (text: string) => inner.sendText(text),
      sendBytes: (bytes: ArrayBuffer) => inner.sendBytes(bytes),
      receive: async () => inner.receive(),
      close: (code = 1000, reason = '') => {
        if (new TextEncoder().encode(reason).length > 123)
          throw new Error('The message must not be greater than 123 bytes');
        closedWith.push([code, reason]);
        inner.close(code, reason);
      },
    };
    const codec = new MsgpackCodec();
    const peer = new Peer(socket, { codec, isDialer: false });
    const closes: CloseReason[] = [];
    peer.onClose((reason) => closes.push(reason));
    const serving = peer.serve();

    inner.inject('{"type":"ping","nonce":"n"}');
    await serving;

    const goaways = inner.sent.map((message) => codec.decode(message)).filter((frame) => frame.type === 'goaway');
    expect(goaways.map((frame) => frame.code)).toEqual([ResetCode.PROTOCOL_ERROR]);
    const reason = goaways[0].reason ?? '';
    expect(new TextEncoder().encode(reason).length).toBeGreaterThan(123);
    expect(closedWith.map(([code]) => code)).toEqual([1002]);
    expect(reason.startsWith(closedWith[0][1])).toBe(true);
    expect(closes.map((close) => [close.code, close.reason])).toEqual([[1002, reason]]);
    expect(inner.isClosed).toBe(true);
  });

  it('cuts the close frame reason that close() sends to 123 UTF-8 bytes', async () => {
    // The socket throws on a longer reason, as `ws` does; the `goaway` and `onClose` carry it whole.
    const [, inner] = memoryPair();
    const closedWith: [number, string][] = [];
    const socket: SocketAdapter = {
      sendText: (text: string) => inner.sendText(text),
      sendBytes: (bytes: ArrayBuffer) => inner.sendBytes(bytes),
      receive: async () => inner.receive(),
      close: (code = 1000, reason = '') => {
        if (new TextEncoder().encode(reason).length > 123)
          throw new Error('The message must not be greater than 123 bytes');
        closedWith.push([code, reason]);
        inner.close(code, reason);
      },
    };
    const peer = new Peer(socket, { codec: new JsonCodec(), isDialer: true });
    const closes: CloseReason[] = [];
    peer.onClose((reason) => closes.push(reason));
    const serving = peer.serve();
    void serving.catch(() => undefined);
    const reason = 'é'.repeat(100);

    await peer.close({ reason, drainMs: 1000 });

    const goaways = inner.sent.map((message) => WIRE.decode(message)).filter((frame) => frame.type === 'goaway');
    expect(goaways.map((frame) => frame.reason)).toEqual([reason]);
    expect(closedWith).toEqual([[1000, 'é'.repeat(61)]]);
    expect(closes.map((close) => [close.code, close.reason])).toEqual([[1000, reason]]);
    expect(inner.isClosed).toBe(true);
    await serving.catch(() => undefined);
  });

  it('cuts a close frame reason to 123 UTF-8 bytes at a character boundary', () => {
    expect(closeFrameReason('short')).toBe('short');
    expect(closeFrameReason('a'.repeat(200))).toBe('a'.repeat(123));
    expect(closeFrameReason(`a${'é'.repeat(100)}`)).toBe(`a${'é'.repeat(61)}`);
    expect(closeFrameReason(`aa${'é'.repeat(100)}`)).toBe(`aa${'é'.repeat(60)}`);
  });

  it('resets only the stream whose reassembled payload cannot be decoded - WSM-STM-020/024', async () => {
    // No handler runs for the stream, and the connection stays open.
    const [, socket] = memoryPair();
    const peer = new Peer(socket, { codec: new JsonCodec(), isDialer: false });
    const handled: unknown[] = [];
    peer.onStream(async (payload: unknown, stream: Stream) => {
      handled.push(payload);
      await stream.closed;
    });
    const serving = peer.serve();
    void serving.catch(() => undefined);

    socket.inject(WIRE.encode({ type: 'open', stream: 1, fragment: '{"a":', more: true }));
    socket.inject(WIRE.encode({ type: 'open', stream: 1, fragment: ']' }));
    for (let turn = 0; turn < 12; turn += 1) {
      await new Promise<void>((resolve) => {
        setTimeout(resolve, 0);
      });
    }

    const sent = socket.sent.map((message) => WIRE.decode(message));
    expect(sent.map((frame) => [frame.type, frame.stream, frame.code])).toEqual([
      ['reset', 1, ResetCode.PROTOCOL_ERROR],
    ]);
    expect(handled).toEqual([]);
    expect(peer.isOpen).toBe(true);
    expect(peer.streams.size).toBe(0);
    socket.drop();
    await serving.catch(() => undefined);
  });

  it('resets only the stream whose fragments mix text and binary values - WSM-STM-020/024', async () => {
    const codec = new MsgpackCodec();
    const pair = makePair({ codec });
    pair.acceptor.onStream(hold);
    pair.start();

    const stream = pair.dialer.open({ q: 1 });
    const other = pair.dialer.open({ q: 2 });
    await pair.settle();
    pair.inject('dialer', { type: 'data', stream: stream.id, fragment: '\u0081', more: true });
    pair.inject('dialer', { type: 'data', stream: stream.id, fragment: new Uint8Array([0xa1, 0x61, 0x01]).buffer });
    await pair.settle();

    const resets = pair.dialerSocket.sent
      .map((message) => codec.decode(message))
      .filter((frame) => frame.type === 'reset' && frame.stream === stream.id);
    expect(resets.map((frame) => frame.code)).toEqual([ResetCode.PROTOCOL_ERROR]);
    expect(await rejection(stream)).toBeInstanceOf(StreamReset);
    expect(pair.dialer.isOpen).toBe(true);
    expect(other.state).not.toBe(StreamState.CLOSED);
  });

  it('closes the socket when an unencodable frame has no live stream', async () => {
    // The error serializer returns a payload the codec cannot encode; the stream is already removed when the writer
    // encodes its reset.
    const pair = makePair({ errorSerializer: () => ({ blob: new Uint8Array([0]).buffer }) });
    const closes: CloseReason[] = [];
    pair.acceptor.onClose((reason) => closes.push(reason));
    pair.acceptor.onStream(() => {
      throw new HandlerFailure('handler said no');
    });
    pair.start();

    pair.dialer.open({ q: 1 });
    await pair.settle();

    expect(pair.acceptor.isOpen).toBe(false);
    expect(closes.map((reason) => reason.code)).toEqual([1011]);
    expect(pair.acceptorSocket.isClosed).toBe(true);
  });

  it('closes the socket when a connection-level frame cannot be sent', async () => {
    const [left] = memoryPair();
    const closedWith: number[] = [];
    const socket: SocketAdapter = {
      sendText: (text: string) => {
        if (text.includes('"ping"')) throw new Error('the transport refused this message');
        left.sendText(text);
      },
      sendBytes: (bytes: ArrayBuffer) => left.sendBytes(bytes),
      receive: async () => left.receive(),
      close: (code?: number, reason?: string) => {
        closedWith.push(code ?? 1000);
        left.close(code, reason);
      },
    };
    const peer = new Peer(socket, { codec: new JsonCodec(), isDialer: true });
    const serving = peer.serve();
    void serving.catch(() => undefined);

    expect(await rejection(peer.ping(1000))).toBeInstanceOf(ConnectionClosed);
    await serving.catch(() => undefined);

    expect(peer.isOpen).toBe(false);
    expect(closedWith).toEqual([1011]);
    expect(left.isClosed).toBe(true);
  });

  it('fails the stream of an unencodable frame and keeps the writer sending', async () => {
    // A codec that cannot encode a frame must not stop the writer: otherwise nothing drains the queue, every later
    // send stays in it, and the peer keeps reporting itself open, the writer-side equivalent of a failed read loop.
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

    // The writer is still running: a later request still completes.
    expect(await pair.dialer.request({ q: 2 })).toEqual({ ok: true });
  });

  it('sends neither an open that fails to encode nor a reset for its id', async () => {
    // The remote has not seen the id, and a `reset` above its high-water mark would end the connection
    // (WSM-STM-003).
    const pair = makePair();
    pair.acceptor.onStream(replyNow);
    pair.start();
    const level = logger.level;
    logger.level = 'silent';
    try {
      const stream = pair.dialer.open({ blob: new Uint8Array([0, 255]).buffer });
      await pair.settle();

      expect(pair.sentBy('dialer').filter((frame) => frame.stream === stream.id)).toEqual([]);
      const error = await rejection(stream);
      expect((error as StreamReset).code).toBe(ResetCode.INTERNAL_ERROR);
      expect(pair.dialer.isOpen).toBe(true);
      expect(pair.acceptor.isOpen).toBe(true);
      expect(await pair.dialer.request({ q: 2 })).toEqual({ ok: true });
    } finally {
      logger.level = level;
      await pair.stop();
    }
  });

  it('sends nothing more on a stream after a later queued frame fails to encode', async () => {
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();
    const level = logger.level;
    logger.level = 'silent';
    try {
      const stream = pair.dialer.open({ q: 1 });
      // Queued behind the `open`, so the writer slices it when it advances after sending the `open`.
      void stream.send({ blob: new Uint8Array([0, 255]).buffer });
      void stream.send({ n: 3 });
      await pair.settle();

      const sent = pair.sentBy('dialer').filter((frame) => frame.stream === stream.id);
      expect(sent.map((frame) => frame.type)).toEqual(['open', 'reset']);
      // The reset carries the codec's own error, as when the rotation fails to encode a frame.
      let refused: unknown = null;
      try {
        new JsonCodec().encode({ type: 'data', stream: stream.id, payload: { blob: new Uint8Array([0, 255]).buffer } });
      } catch (error) {
        refused = error;
      }
      expect(sent[1]?.reason).toBe((refused as Error).message);
      const error = await rejection(stream);
      expect((error as StreamReset).code).toBe(ResetCode.INTERNAL_ERROR);
      expect(pair.dialer.isOpen).toBe(true);
    } finally {
      logger.level = level;
      await pair.stop();
    }
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
    // In Python the failure mode is a traceback that grows by one frame on every re-raise of one stored instance;
    // in JavaScript it is one shared mutable error object, whose `stack` does not describe where each caller's
    // failure happened, handed to every caller. Both are the same defect.
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
    // Checked at compile time: `tsc` rejects the file if either type changes.
    type Has<T, K extends string> = K extends keyof T ? true : false;
    const openHasTimeout: Has<OpenOptions, 'timeoutMs'> = false;
    const requestHasTimeout: Has<RequestOptions, 'timeoutMs'> = true;
    expect(openHasTimeout).toBe(false);
    expect(requestHasTimeout).toBe(true);

    // At runtime `timeoutMs` is not one of `open()`'s option keys, so an object carrying it is a payload and no
    // deadline is applied.
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();
    pair.dialer.open({ timeoutMs: 5 } as unknown as OpenOptions);
    await pair.settle();
    expect(pair.lastFrameOfType('dialer', 'open').payload).toEqual({ timeoutMs: 5 });
  });

  it('takes zero mandatory arguments and defaults payload to null - WSM-API-003', async () => {
    // `open()` with no arguments is the one-line push shape from section 5.1's call-shapes note. A required
    // argument would break that call, and a payload defaulting to absent rather than `null` would put a different
    // frame on the wire from Python's `payload=None`; both ports must send the same frame.
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    const stream = pair.dialer.open();
    expect(stream.id).toBe(1);
    await pair.settle();

    const open = pair.lastFrameOfType('dialer', 'open');
    // `"payload": null` is on the wire, not an absent key: an absent `payload` is a different frame
    // (section 2.2), and Python's `open()` sends the null.
    expect('payload' in toMapping(open), 'open() sent no payload key').toBe(true);
    expect(open.payload).toBeNull();
    expect(open.headers ?? null).toBeNull();
    expect(open.end ?? false).toBe(false);
    expect(stream.payload).toBeNull();

    // The remote receives an open, so the empty call is not handled locally only.
    expect(pair.acceptor.streams.size).toBe(1);
  });

  it('accepts no stream id at any public entry point - WSM-SID-001', async () => {
    // The parity test covers which id the library picks; this test checks that a caller cannot pick one. Every form
    // an application might use is checked, because the rule covers the whole API surface. In TypeScript an options
    // object is structural, so the compiler accepts an unexpected key and the runtime ignores it.
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
    // Ids are allocated by the library: odd, monotonic, in call order, and never 9999.
    expect(opens.map((frame) => frame.stream)).toEqual([1, 3, 5, 7, 9, 11, 13, 15]);

    // The caller's number appears only as opaque payload on the odd ones.
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

    // A default deadline would be a timer, and `request()` schedules its timer synchronously before it returns (see
    // `collectUnary`). Counting scheduled timers therefore detects a default deadline without waiting for it to
    // expire.
    const timers = vi.spyOn(globalThis, 'setTimeout');
    const forever = pair.dialer.request({ q: 1 });
    void forever.catch(() => undefined);
    expect(timers, 'request() with no timeout scheduled a deadline').not.toHaveBeenCalled();

    const explicit = pair.dialer.request({ q: 2 }, {});
    void explicit.catch(() => undefined);
    expect(timers, 'an empty options object applied a deadline').not.toHaveBeenCalled();

    // The counter detects a deadline when one is set, so the zero count above is meaningful.
    const deadlined = pair.dialer.request({ q: 3 }, { timeoutMs: 20 });
    expect(timers).toHaveBeenCalledTimes(1);
    timers.mockRestore();

    await expect(deadlined).rejects.toThrow(StreamTimeout);
    await pair.settle();

    // Only the deadlined stream was reset; the other two are still open and waiting. A stream lives until it ends,
    // is reset, or the connection dies.
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
    // Headers belong to the application. Whatever an opener sends must arrive unchanged and must not change the
    // library's behaviour, including keys that look like authentication, where a re-auth hook would be attached
    // (WSM-AUT-001 places authentication at the upgrade, before `accept()` is called).
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

    // Snapshotted before the send, as in the payload test: a library that removed a header from the caller's object
    // would pass a comparison against the modified object.
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

    // Unchanged on the wire, and unchanged when the opener's counterpart reads them.
    expect(pair.framesOfType('dialer', 'open')[0].headers).toEqual(expected);
    expect(seen[0]).toEqual(expected);
    expect(headers, "the caller's own headers object was modified in flight").toEqual(expected);

    // The headers change nothing: the same answer, the same frames, and the same peer state as the open without
    // headers. A library that reacted to `authorization` would differ here even if it did not modify the object.
    expect(withHeaders).toEqual(without);
    const opens = pair.framesOfType('dialer', 'open');
    expect(framesEqual({ ...opens[0], stream: 0, headers: null }, { ...opens[1], stream: 0 })).toBe(true);

    // The whole exchange in both directions, with ids normalised: the two requests produce the same frames in the
    // same order, so no code path depends on a header, including `authorization`.
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

  it('never interprets the answering side’s headers either - WSM-AUT-002/WSM-FRM-016', async () => {
    // The same check as the opener's test above, for the answering side. The keys are ones a library might act on:
    // a `content-type` to decode by, a `status` to turn into an exception.
    const announced = {
      'content-type': 'application/octet-stream',
      status: 500,
      error: true,
      code: 3,
      type: 'reset',
      expires_at: 0,
    };
    const expected = JSON.parse(JSON.stringify(announced)) as typeof announced;

    const pair = makePair();
    pair.acceptor.onStream(async (payload: unknown, stream: Stream) => {
      // Stream 1 is answered with them and stream 3 without, so the two exchanges can be compared
      // frame for frame below.
      if ((payload as { n: number }).n === 1) await stream.reply({ ok: true }, { headers: announced });
      else await stream.reply({ ok: true });
    });
    pair.start();

    const first = pair.dialer.open({ n: 1 }, { end: true });
    const withHeaders = await first;
    const second = pair.dialer.open({ n: 2 }, { end: true });
    const without = await second;
    await pair.settle();

    await first.replyHeadersArrived;
    await second.replyHeadersArrived;
    expect(first.replyHeaders).toEqual(expected);
    expect(announced, "the handler's own headers object was modified in flight").toEqual(expected);
    // An answer that announced none leaves an empty mapping, not the other stream's.
    expect(second.replyHeaders).toEqual({});
    expect(pair.framesOfType('acceptor', 'data')[0].headers).toEqual(expected);

    // The headers change nothing: `status: 500` did not become an exception, and both exchanges produce the same
    // frames in the same order once ids and headers are normalised.
    expect(withHeaders).toEqual(without);
    const trace = (who: Who, id: number): unknown[] =>
      pair
        .sentBy(who)
        .filter((frame) => frame.stream === id)
        .map((frame) => ({ ...toMapping(frame), stream: 0, headers: undefined, payload: undefined }));
    expect(trace('dialer', first.id)).toEqual(trace('dialer', second.id));
    expect(trace('acceptor', first.id)).toEqual(trace('acceptor', second.id));
    expect(pair.framesOfType('acceptor', 'reset')).toHaveLength(0);
    expect(pair.acceptor.isOpen).toBe(true);
  });

  it('defines no vocabulary inside payload - WSM-FRM-006/WSM-INV-016', async () => {
    // Every envelope field name used as a payload key, plus discriminators that libraries commonly reserve. If
    // muxws interpreted any of them, two consumers sharing one socket would have to nest their own vocabulary
    // inside the library's.
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

    // The comparison is against a snapshot taken before the send, not against `payload`: a library that removed a
    // reserved key from the caller's object would modify both sides of `toEqual(payload)` and the assertion would
    // pass.
    const expected = JSON.parse(JSON.stringify(payload)) as typeof payload;

    const seen: unknown[] = [];
    const pair = makePair();
    pair.acceptor.onStream(async (received: unknown, stream: Stream) => {
      seen.push(received);
      // Echoed back, so the round trip is asserted in both directions.
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

    // Nothing acted on it: no stream 9999, no reset carrying the payload's PROTOCOL_ERROR, no pong for its "nonce",
    // and both sides are still connected.
    const streams = new Set(pair.sentBy('dialer').map((frame) => frame.stream));
    expect(streams.has(9999)).toBe(false);
    expect(pair.framesOfType('dialer', 'reset')).toHaveLength(0);
    expect(pair.framesOfType('acceptor', 'reset')).toHaveLength(0);
    expect(pair.framesOfType('acceptor', 'pong')).toHaveLength(0);
    expect(pair.framesOfType('acceptor', 'goaway')).toHaveLength(0);
    expect(pair.dialer.isOpen && pair.acceptor.isOpen).toBe(true);
  });

  it('puts only the six v1 frame types on the wire - WSM-FRM-012/WSM-FRM-013', async () => {
    // `frames.spec.ts` asserts the constant by set equality; this test checks the behaviour. Ending a stream and
    // attaching trailers must produce a `data` frame with flags, never a seventh frame type; this is how the
    // absence of an `end` type shows on a wire.
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
    // The copy is required. Under jsdom `TextEncoder` is Node's global and `ArrayBuffer` is jsdom's, so
    // `bytes.buffer instanceof ArrayBuffer` is false because the two come from different realms. Allocating through
    // the ambient constructor matches what a browser codec passes to `socket.send`, so `instanceof` tests the send
    // branch and not the realm that built the buffer.
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
   * Counterpart of the `caplog.at_level` part of Python's `test_unknown_frame_type_is_ignored` and
   * `test_payload_contents_never_appear_in_a_log_record`. Setting `logger.level` makes the positive assertions
   * possible: the level starts at 'warn', below which neither line reaches a console, and the negative assertions
   * alone (nothing sent, nothing logged) hold either way.
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

  it('logs each unknown frame type once per peer when it arrives repeatedly - WSM-FRM-002', async () => {
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
      for (const type of ['widget', 'widget', 'gadget', 'widget', 'gadget']) {
        pair.acceptorSocket.inject(JSON.stringify({ type, stream: 1 }));
      }
      await pair.settle();

      expect(lines.filter((line) => line.includes("'widget'"))).toHaveLength(1);
      expect(lines.filter((line) => line.includes("'gadget'"))).toHaveLength(1);
      expect(pair.acceptor.isOpen).toBe(true);
    } finally {
      logger.level = original;
      spy.mockRestore();
      await pair.stop();
    }
  });

  it.each(['5', '[1]', '{"a":1}', '{"toString":1}', 'null'])(
    'fails the connection on the non-string frame type %s with frame logging on - WSM-FRM-005',
    async (frameType) => {
      const original = logger.level;
      const spy = vi.spyOn(console, 'debug').mockImplementation(() => undefined);
      const infoSpy = vi.spyOn(console, 'info').mockImplementation(() => undefined);
      const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
      logger.level = 'debug';

      const pair = makePair();
      pair.acceptor.onStream(hold);
      pair.start();
      try {
        pair.injectRaw('acceptor', `{"type":${frameType},"stream":1}`);
        await pair.settle();

        expect(pair.framesOfType('acceptor', 'goaway').map((frame) => frame.code)).toEqual([ResetCode.PROTOCOL_ERROR]);
        expect(pair.acceptor.isOpen).toBe(false);
        expect(pair.acceptorSocket.isClosed).toBe(true);
      } finally {
        warnSpy.mockRestore();
        logger.level = original;
        spy.mockRestore();
        infoSpy.mockRestore();
        await pair.stop();
      }
    },
  );

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

describe('the final close - WSM-RCN-046/047', () => {
  it('reports isClosed once the willRetry false close is reported', async () => {
    const pair = makePair();
    const closes: CloseReason[] = [];
    pair.acceptor.onClose((reason) => closes.push(reason));
    pair.start();
    expect(pair.acceptor.isClosed).toBe(false);

    internals(pair.acceptor).die(new ConnectionClosed('the socket died', { code: 1006 }));

    expect(closes.map((reason) => reason.willRetry)).toEqual([false]);
    expect(pair.acceptor.isClosed).toBe(true);
    expect(pair.acceptor.isOpen).toBe(false);
    await pair.stop();
  });

  it('calls an onClose handler registered after the final close at once', async () => {
    const [dialerSide] = memoryPair();
    const peer = new Peer(dialerSide, { codec: new JsonCodec(), isDialer: true });
    internals(peer).die(new ConnectionClosed('the socket died', { code: 1006 }));
    expect(peer.isClosed).toBe(true);

    const late: CloseReason[] = [];
    peer.onClose((reason) => late.push(reason));
    expect(late).toHaveLength(1);
    expect(late[0].willRetry).toBe(false);

    const errors = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    try {
      expect(() =>
        peer.onClose(() => {
          throw new Error('handler failure');
        }),
      ).not.toThrow();
      expect(errors).toHaveBeenCalled();
    } finally {
      errors.mockRestore();
    }

    peer.notifyClose({ code: 1000, reason: '', wasClean: true, willRetry: false });
    expect(late, 'a second final close is suppressed').toHaveLength(1);
  });
});

describe('header interceptors', () => {
  it('shapes the headers of every open in registration order, on a copy - WSM-API-026/027', async () => {
    const pair = makePair();
    const received: Record<string, unknown>[] = [];
    pair.acceptor.onStream(async (_payload: unknown, stream: Stream) => {
      received.push({ ...stream.headers });
      await stream.reply({ ok: true });
    });
    const calls: [string, unknown][] = [];
    const kept: Record<string, unknown>[] = [];
    const unregisterFirst = pair.dialer.interceptOpen((headers, payload) => {
      calls.push(['first', payload]);
      headers.trace = 't-1';
      headers.attempt = 2;
      kept.push(headers);
    });
    const unregisterSecond = pair.dialer.interceptOpen((headers) => {
      calls.push(['second', { ...headers }]);
      delete headers['drop-me'];
    });
    pair.start();

    const callers = { attempt: 1, 'drop-me': true, own: 'x' };
    const stream = pair.dialer.open({ n: 1 }, { headers: callers, end: true });
    kept[0].late = 'after return';
    await stream;

    expect(callers, "the caller's object is untouched").toEqual({ attempt: 1, 'drop-me': true, own: 'x' });
    expect(calls).toEqual([
      ['first', { n: 1 }],
      ['second', { attempt: 2, 'drop-me': true, own: 'x', trace: 't-1' }],
    ]);
    expect(stream.headers).toEqual({ attempt: 2, own: 'x', trace: 't-1' });
    expect(received, 'a change after return never reaches the frame').toEqual([{ attempt: 2, own: 'x', trace: 't-1' }]);

    await pair.dialer.request('r');
    await pair.dialer.notify('n');
    await pair.settle();
    expect(
      received.map((headers) => headers.trace),
      'request() and notify() headers pass the interceptors too',
    ).toEqual(['t-1', 't-1', 't-1']);

    unregisterFirst();
    unregisterFirst();
    unregisterSecond();
    let before = pair.dialerSocket.sent.length;
    await pair.dialer.request('bare');
    expect(String(pair.dialerSocket.sent[before]), 'no interceptor left, no headers field').not.toContain('"headers"');

    pair.dialer.interceptOpen(() => undefined);
    before = pair.dialerSocket.sent.length;
    await pair.dialer.request('bare');
    expect(String(pair.dialerSocket.sent[before]), 'an interceptor that adds nothing changes no byte').not.toContain(
      '"headers"',
    );
  });

  it('fails the open with the interceptor error and spends nothing - WSM-API-027', async () => {
    const pair = makePair();
    pair.acceptor.onStream(replyNow);
    const later: string[] = [];
    let unregister = pair.dialer.interceptOpen(() => {
      throw new RangeError('no ambient token');
    });
    pair.dialer.interceptOpen(() => {
      later.push('ran');
    });
    pair.start();

    expect(() => pair.dialer.open('x')).toThrow(RangeError);
    await expect(pair.dialer.request('x')).rejects.toThrow('no ambient token');
    expect(later, 'the interceptors after the one that threw do not run').toEqual([]);
    await pair.settle();
    expect(pair.dialerSocket.sent, 'nothing reached the wire').toEqual([]);
    unregister();

    unregister = pair.dialer.interceptOpen(async (headers) => {
      headers.late = true;
    });
    expect(() => pair.dialer.open('x')).toThrow(/thenable/);
    unregister();

    unregister = pair.dialer.interceptOpen((headers) => {
      (headers as Record<string | symbol, unknown>)[Symbol('key')] = 'symbol';
    });
    expect(() => pair.dialer.open('x')).toThrow(TypeError);
    unregister();

    expect(pair.dialer.open('x').id, 'no failed open spent an id').toBe(1);
    expect(pair.dialer.isOpen).toBe(true);
  });

  it('shapes the first frame of every answer on a copy, for each sending call - WSM-API-028', async () => {
    const pair = makePair();
    const seen: [Record<string, unknown>, unknown][] = [];
    const kept: Record<string, unknown>[] = [];
    const announced = { 'content-type': 'text/csv' };
    const onOwnStreams: number[] = [];
    pair.acceptor.onStream(async (payload: unknown, stream: Stream) => {
      if (payload === 'announce') {
        await stream.sendHeaders(announced);
        kept[kept.length - 1].late = 'after return';
        await stream.send('row');
        await stream.end({ payload: 'last' });
      } else if (payload === 'reply') {
        await stream.reply('done', { headers: { 'drop-me': 1 } });
      }
    });
    const unregister = pair.acceptor.interceptReplyHeaders((headers, stream) => {
      seen.push([{ ...headers }, stream.payload]);
      headers['served-by'] = 'acceptor';
      delete headers['drop-me'];
      kept.push(headers);
    });
    pair.dialer.interceptReplyHeaders((_headers, stream) => {
      onOwnStreams.push(stream.id);
    });
    pair.start();

    let stream = pair.dialer.open('announce');
    await stream.end();
    await stream.closed;
    expect(stream.replyHeaders).toEqual({ 'content-type': 'text/csv', 'served-by': 'acceptor' });
    expect(announced, "the caller's object is untouched").toEqual({ 'content-type': 'text/csv' });

    stream = pair.dialer.open('reply', { end: true });
    expect(await stream).toBe('done');
    expect(stream.replyHeaders).toEqual({ 'served-by': 'acceptor' });

    stream = pair.dialer.open('implicit', { end: true });
    await stream.closed;
    expect(stream.replyHeaders, 'replyHeaders are set on the implicit end of a returning handler').toEqual({
      'served-by': 'acceptor',
    });

    expect(seen).toEqual([
      [{ 'content-type': 'text/csv' }, 'announce'],
      [{ 'drop-me': 1 }, 'reply'],
      [{}, 'implicit'],
    ]);
    expect(onOwnStreams, 'never on a stream this peer opened').toEqual([]);

    unregister();
    const before = pair.acceptorSocket.sent.length;
    stream = pair.dialer.open('implicit', { end: true });
    await stream.closed;
    expect(String(pair.acceptorSocket.sent[before]), 'no interceptor left, no headers field').not.toContain(
      '"headers"',
    );
  });

  it('fails the call that sends the answer and leaves its first frame unspent - WSM-API-028', async () => {
    const pair = makePair();
    const sentBeforeAnswer: number[] = [];
    const refuse = (): void => {
      throw new RangeError('no tenant');
    };
    pair.acceptor.onStream(async (payload: unknown, stream: Stream) => {
      if (payload === 'implicit') return;
      let unregister = pair.acceptor.interceptReplyHeaders(refuse);
      await expect(stream.sendHeaders({ a: 1 })).rejects.toThrow('no tenant');
      unregister();
      unregister = pair.acceptor.interceptReplyHeaders(async (headers) => {
        headers.late = true;
      });
      await expect(stream.send('x')).rejects.toThrow(/thenable/);
      unregister();
      sentBeforeAnswer.push(pair.acceptorSocket.sent.length);
      await stream.reply('ok', { headers: { a: 1 } });
    });
    pair.start();

    const stream = pair.dialer.open('explicit', { end: true });
    expect(await stream).toBe('ok');
    expect(sentBeforeAnswer, 'a failed call put nothing on the wire').toEqual([0]);
    expect(stream.replyHeaders, 'and left the first frame for the next call').toEqual({ a: 1 });

    pair.acceptor.interceptReplyHeaders(refuse);
    await expect(pair.dialer.request('implicit', { timeoutMs: 5000 })).rejects.toBeInstanceOf(RemoteError);
    expect(pair.dialer.isOpen && pair.acceptor.isOpen).toBe(true);
  });

  it("hands each stream's leading headers to incoming interceptors once, frozen - WSM-API-029", async () => {
    const pair = makePair();
    const order: string[] = [];
    const seenByAcceptor: [Record<string, unknown>, boolean][] = [];
    const seenByDialer: [Record<string, unknown>, number, boolean][] = [];
    pair.acceptor.onStream(async (payload: unknown, stream: Stream) => {
      order.push('handler');
      if (payload === 'reset') {
        await stream.reset(ResetCode.APPLICATION_ERROR, 'no answer');
        return;
      }
      await stream.sendHeaders({ 'content-type': 'text/csv' });
      await stream.send('row');
      await stream.end({ payload: 'last' });
    });
    pair.acceptor.interceptIncomingHeaders((headers, stream) => {
      order.push('interceptor');
      seenByAcceptor.push([{ ...headers }, stream.local]);
      expect(() => {
        (headers as Record<string, unknown>).x = 1;
      }).toThrow(TypeError);
    });
    pair.dialer.interceptIncomingHeaders((headers, stream) => {
      seenByDialer.push([{ ...headers }, stream.id, stream.local]);
    });
    pair.start();

    const stream = pair.dialer.open('go', { headers: { trace: 'abc' } });
    const items: unknown[] = [];
    for await (const item of stream) items.push(item);
    expect(items).toEqual(['row', 'last']);
    expect(order, 'before the handler runs').toEqual(['interceptor', 'handler']);
    expect(seenByAcceptor).toEqual([[{ trace: 'abc' }, false]]);
    expect(seenByDialer, 'once, on the answer').toEqual([[{ 'content-type': 'text/csv' }, stream.id, true]]);

    await expect(pair.dialer.request('reset')).rejects.toBeInstanceOf(StreamReset);
    await pair.settle();
    expect(seenByDialer, 'a reset is not an answer').toHaveLength(1);
    expect(seenByAcceptor.at(-1), 'an open without headers is seen with an empty object').toEqual([{}, false]);
  });

  it('logs an incoming interceptor that throws and changes nothing else - WSM-API-029', async () => {
    const pair = makePair();
    pair.acceptor.onStream(replyNow);
    const after: unknown[] = [];
    const broken = (): void => {
      throw new Error('interceptor bug');
    };
    pair.acceptor.interceptIncomingHeaders(broken);
    pair.acceptor.interceptIncomingHeaders(async () => {
      after.push('never awaited');
      throw new Error('rejected');
    });
    pair.acceptor.interceptIncomingHeaders((headers) => {
      after.push({ ...headers });
    });
    pair.dialer.interceptIncomingHeaders(broken);
    pair.start();

    expect(await pair.dialer.request('q', { headers: { k: 'v' } })).toEqual({ ok: true });
    await pair.settle();
    expect(after).toEqual(['never awaited', { k: 'v' }]);
    expect(logged.error.filter((line) => line.includes('incoming headers interceptor threw'))).toHaveLength(2);
    expect(logged.error.filter((line) => line.includes('returned a thenable'))).toHaveLength(1);
    expect(pair.dialer.isOpen && pair.acceptor.isOpen).toBe(true);
  });
});

describe('oneway streams', () => {
  it('leaves no live stream at the opener of a notify - WSM-STM-041', async () => {
    const pair = makePair();
    const seen = new Promise<unknown>((resolve) => {
      pair.acceptor.onStream((payload) => {
        resolve(payload);
      });
    });
    pair.start();

    await pair.dialer.notify({ event: 'cache-invalidated' });
    expect(pair.dialer.streams.size).toBe(0);
    expect(await seen).toEqual({ event: 'cache-invalidated' });
    const [opened] = pair.framesOfType('dialer', 'open');
    expect([opened.end, opened.oneway]).toEqual([true, true]);
  });

  it('refuses every send on a oneway stream and closes it without a frame - WSM-STM-040', async () => {
    const pair = makePair();
    const outcome: { state?: StreamState; raised?: unknown; stream?: Stream } = {};
    pair.acceptor.onStream(async (payload, stream) => {
      outcome.state = stream.state;
      try {
        await stream.send({ answer: 1 });
      } catch (error) {
        outcome.raised = error;
      }
      outcome.stream = stream;
    });
    pair.start();

    await pair.dialer.notify({ q: 1 });
    await pair.settle(24);

    expect(outcome.state).toBe(StreamState.HALF_CLOSED_REMOTE);
    expect(outcome.raised).toBeInstanceOf(StreamClosed);
    expect(outcome.stream?.state).toBe(StreamState.CLOSED);
    expect(pair.acceptor.streams.size).toBe(0);
    expect(pair.framesOfType('acceptor', 'data')).toEqual([]);
    expect(pair.framesOfType('acceptor', 'reset')).toEqual([]);
  });

  it('closes a oneway stream locally on reset() without a frame - WSM-STM-040', async () => {
    const pair = makePair();
    const closed = new Promise<void>((resolve) => {
      pair.acceptor.onStream(async (payload, stream) => {
        await stream.reset(ResetCode.CANCELLED, 'not interested');
        resolve();
      });
    });
    pair.start();

    await pair.dialer.notify({ q: 1 });
    await closed;
    await pair.settle();
    expect(pair.framesOfType('acceptor', 'reset')).toEqual([]);
  });

  it('logs a oneway handler that throws and sends no reset - WSM-STM-040', async () => {
    const pair = makePair();
    pair.acceptor.onStream(() => {
      throw new HandlerFailure('the handler blew up');
    });
    pair.start();

    await pair.dialer.notify({ q: 1 });
    await pair.settle(24);

    expect(pair.framesOfType('acceptor', 'reset')).toEqual([]);
    expect(logged.error.filter((line) => line.includes('handler of oneway stream'))).toHaveLength(1);
  });

  it('drops a oneway open without a handler, with no frame - WSM-STM-040', async () => {
    const pair = makePair();
    pair.start();

    await pair.dialer.notify({ q: 1 });
    await pair.settle(24);

    expect(pair.framesOfType('acceptor', 'reset')).toEqual([]);
    expect(pair.acceptor.streams.size).toBe(0);
    expect(logged.warn.filter((line) => line.includes('no on_stream handler'))).toHaveLength(1);
  });

  it('drops a oneway open after this peer sent goaway - WSM-STM-040', async () => {
    const pair = makePair();
    const called: unknown[] = [];
    pair.acceptor.onStream((payload) => {
      called.push(payload);
    });
    pair.start();
    const closing = pair.acceptor.close({ drainMs: 200 });
    await pair.settle();
    expect(pair.framesOfType('acceptor', 'goaway').length).toBe(1);

    pair.inject('acceptor', { type: 'open', stream: 101, payload: { q: 1 }, end: true, oneway: true });
    await pair.settle();
    expect(called).toEqual([]);
    expect(pair.framesOfType('acceptor', 'reset')).toEqual([]);
    await closing.catch(() => undefined);
  });

  it('takes a oneway open without end as ending - WSM-FRM-020', async () => {
    const pair = makePair();
    const states: StreamState[] = [];
    pair.acceptor.onStream((payload, stream) => {
      states.push(stream.state);
    });
    pair.start();

    pair.inject('acceptor', { type: 'open', stream: 101, payload: { q: 1 }, oneway: true });
    await pair.settle();
    expect(states).toEqual([StreamState.HALF_CLOSED_REMOTE]);
    expect(pair.framesOfType('acceptor', 'data')).toEqual([]);
    expect(pair.framesOfType('acceptor', 'reset')).toEqual([]);
  });

  it('carries oneway on the first fragment of a fragmented notify - WSM-FRM-020', async () => {
    const pair = makePair({ maxFrameBytes: 96 });
    const seen = new Promise<unknown>((resolve) => {
      pair.acceptor.onStream((payload) => {
        resolve(payload);
      });
    });
    pair.start();

    const payload = { body: 'x'.repeat(400) };
    await pair.dialer.notify(payload);
    expect(await seen).toEqual(payload);
    const fragments = pair.framesOfType('dialer', 'open');
    expect(fragments.length).toBeGreaterThan(1);
    expect(fragments.map((fragment) => fragment.oneway === true)).toEqual([
      true,
      ...fragments.slice(1).map(() => false),
    ]);
    expect(fragments[fragments.length - 1].end).toBe(true);
    await pair.settle();
    expect(pair.framesOfType('acceptor', 'data')).toEqual([]);
  });
});
