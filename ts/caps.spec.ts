/**
 * The three receive-side caps, and the observability hooks (§4.3, §12).
 *
 * A mirror of `muxws/caps_test.py`, test for test. All three caps are **local defences**. None is
 * announced, none has a remote counterpart to consult, and a sender learns of one only from the reset
 * it provokes (WSM-CON-031, WSM-FRG-035, WSM-STM-036).
 *
 * Where an assertion is spelled differently here it is because JavaScript offers a different witness
 * of the same fact - a `console` spy where Python reads `caplog`, `Function.prototype.toString` where
 * Python reads `inspect.getsource` - and each of those places says so.
 */

// `describe`/`it`/`expect` are configured as globals; `vi` is imported because the shared eslint
// config does not know it as one.
import { vi } from 'vitest';

import { type Codec, JsonCodec } from './codec';
import { ProtocolError, ResetCode, StreamRefused, StreamReset } from './errors';
import { encodedLength, MAX_FRAME_BYTES, splitFrame } from './fragment';
import type { Frame } from './frames';
import { logger } from './observability';
import { Peer } from './peer';
import type { Stream } from './stream';
import { MemorySocket, memoryPair } from './transports/memory';

// --------------------------------------------------------------------------- the harness

type Who = 'dialer' | 'acceptor';

/** The decoder the test itself reads the wire with, independent of the peer's own codec. */
const WIRE = new JsonCodec();

/** Python spins the loop with `asyncio.sleep(0)`; a macrotask turn also drains the microtask queue. */
async function turns(rounds: number): Promise<void> {
  for (let turn = 0; turn < rounds; turn += 1) {
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 0);
    });
  }
}

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
    this.served = [this.dialer.serve(), this.acceptor.serve()];
    this.served.forEach((task) => {
      void task.catch(() => undefined);
    });
  }

  async settle(rounds = 12): Promise<void> {
    await turns(rounds);
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

/**
 * One peer, fed frames from the wire by hand - the mirror of `conftest.py`'s `Lone`.
 *
 * A live pair cannot test a receiver's reaction to a frame no correct sender would produce: the
 * counterpart peer sees the answer, cannot account for it, and kills the connection - correctly, and
 * entirely beside the point. Injecting into a peer with no counterpart is how the conformance runner
 * does it, and it is what these tests need too.
 */
class Lone {
  private served: Promise<void> | null = null;

  private stopped = false;

  constructor(
    readonly peer: Peer,
    readonly socket: MemorySocket,
    readonly codec: Codec,
  ) {}

  start(): void {
    this.served = this.peer.serve();
    void this.served.catch(() => undefined);
  }

  inject(message: Frame | string | ArrayBuffer): void {
    if (typeof message === 'string' || message instanceof ArrayBuffer) {
      this.socket.inject(message);
      return;
    }
    this.socket.inject(this.codec.encode(message));
  }

  async settle(rounds = 12): Promise<void> {
    await turns(rounds);
  }

  sent(): Frame[] {
    return this.socket.sent.map((message) => WIRE.decode(message));
  }

  framesOfType(frameType: string): Frame[] {
    return this.sent().filter((frame) => frame.type === frameType);
  }

  async stop(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    this.socket.drop();
    await this.settle();
    if (this.served !== null) await this.served.catch(() => undefined);
  }
}

/** Every peer option these tests vary. Mirrors the keyword arguments of the Python fixtures. */
interface Caps {
  maxFrameBytes?: number;
  maxPayloadBytes?: number;
  maxConcurrentStreams?: number;
}

const pairs: Pair[] = [];
const lones: Lone[] = [];

/** Build a peer pair without starting it, so a test can register handlers first. */
function makePair(options: Caps = {}): Pair {
  const [left, right] = memoryPair();
  const codec = new JsonCodec();
  const pair = new Pair(
    new Peer(left, { codec, isDialer: true, ...options }),
    new Peer(right, { codec, isDialer: false, ...options }),
    left,
    right,
    codec,
  );
  pairs.push(pair);
  return pair;
}

/** An acceptor with no counterpart, so injected frames cannot confuse a real peer. */
function makeLone(options: Caps = {}): Lone {
  const [, socket] = memoryPair();
  const codec = new JsonCodec();
  const lone = new Lone(new Peer(socket, { codec, isDialer: false, ...options }), socket, codec);
  lones.push(lone);
  return lone;
}

/**
 * The peer's private bookkeeping. Python reads `_max_payload_bytes` and `_remote_stream_count()`;
 * both are statements about what the peer holds locally, which no wire observation can witness.
 */
interface PeerInternals {
  maxPayloadBytes: number;
  remoteStreamCount(): number;
}

function internals(peer: Peer): PeerInternals {
  return peer as unknown as PeerInternals;
}

/** What the `muxws.frames` shim in `ts/observability.ts` emitted, per level. Python reads caplog. */
const LEVELS = ['debug', 'info', 'warn', 'error'] as const;

let logged: Record<(typeof LEVELS)[number], string[]>;

beforeEach(() => {
  logged = { debug: [], info: [], warn: [], error: [] };
  LEVELS.forEach((level) => {
    vi.spyOn(console, level).mockImplementation((...args: unknown[]) => {
      logged[level].push(args.map((arg) => String(arg)).join(' '));
    });
  });
});

afterEach(async () => {
  await Promise.all(pairs.splice(0, pairs.length).map((pair) => pair.stop()));
  await Promise.all(lones.splice(0, lones.length).map((lone) => lone.stop()));
  vi.restoreAllMocks();
});

/** Every line the shim wrote, at every level. */
function allLogLines(): string[] {
  return LEVELS.flatMap((level) => logged[level]);
}

/** Python's `_hold`: keep the stream open until somebody else closes it. */
async function hold(payload: unknown, stream: Stream): Promise<void> {
  await stream.closed;
}

/** Python's `_reply_now`. */
async function replyNow(payload: unknown, stream: Stream): Promise<void> {
  await stream.reply({ ok: true });
}

/** `pytest.raises`, as a value: the rejection reason, or a failure if there was none. */
async function rejection(work: PromiseLike<unknown>): Promise<unknown> {
  try {
    await work;
  } catch (error) {
    return error;
  }
  throw new Error('expected a rejection, got a resolution');
}

// --------------------------------------------------------------------------- frame size

describe('the frame-size cap', () => {
  it('accepts a message at the constant - WSM-FRG-004/031', async () => {
    // A receiver MUST NOT reject a message at or below `MAX_FRAME_BYTES`.
    const pair = makePair();
    pair.acceptor.onStream(replyNow);
    pair.start();

    const { codec } = pair;
    // Sized so the whole encoded message lands just under the constant.
    const payload = { body: 'x'.repeat(MAX_FRAME_BYTES - 200) };
    expect(encodedLength(codec.encode({ type: 'open', stream: 1, payload }))).toBeLessThanOrEqual(MAX_FRAME_BYTES);

    expect(await pair.dialer.request(payload)).toEqual({ ok: true });
    expect(pair.acceptor.isOpen).toBe(true);
  });

  it('resets that stream only for an over-cap message - WSM-FRG-031', async () => {
    // The **whole encoded message** is measured, and the connection survives.
    const lone = makeLone({ maxFrameBytes: 512 });
    lone.peer.onStream(replyNow);
    lone.start();

    const oversize = lone.codec.encode({ type: 'open', stream: 1, payload: { body: 'x'.repeat(4000) } });
    expect(encodedLength(oversize)).toBeGreaterThan(512);
    lone.inject(oversize);
    await lone.settle();

    const resets = lone.framesOfType('reset');
    expect(resets.length).toBeGreaterThan(0);
    expect(resets[resets.length - 1].code).toBe(ResetCode.PAYLOAD_TOO_LARGE);
    expect(lone.peer.isOpen, 'a size violation is stream-level, not connection-level').toBe(true);
  });

  it('does not measure the fragment field alone - WSM-FRG-031', async () => {
    // WSM-FRG-031 says the entire encoded message, envelope included - never the field.
    const lone = makeLone({ maxFrameBytes: 512 });
    lone.peer.onStream(hold);
    lone.start();

    const { codec } = lone;
    // A fragment field comfortably under the cap, in an envelope that pushes the message over it.
    const fragment = 'y'.repeat(400);
    const frame: Frame = { type: 'open', stream: 1, fragment, more: true, headers: { h: 'z'.repeat(300) } };
    expect(encodedLength(fragment)).toBeLessThan(512);
    expect(encodedLength(codec.encode(frame))).toBeGreaterThan(512);

    lone.inject(frame);
    await lone.settle();
    expect(lone.framesOfType('reset').length).toBeGreaterThan(0);
  });

  it('rejects a cap below the envelope floor at construction - WSM-FRG-034', () => {
    // Found here, not later as an infinite split loop.
    const [, socket] = memoryPair();
    expect(() => new Peer(socket, { codec: new JsonCodec(), isDialer: false, maxFrameBytes: 16 })).toThrow(
      /WSM-FRG-034/,
    );
    expect(() => new Peer(socket, { codec: new JsonCodec(), isDialer: false, maxFrameBytes: 16 })).toThrow(
      ProtocolError,
    );
  });
});

// --------------------------------------------------------------------------- maxPayloadBytes

describe('maxPayloadBytes', () => {
  it('resets on the crossing fragment - WSM-FRG-032/WSM-INV-017 (spec)', async () => {
    // The reset goes out **before the final fragment arrives**. A receiver that assembled the payload
    // in order to measure it has already spent everything the limit existed to protect. The partial
    // buffer is dropped in the same step, for the same reason.
    const lone = makeLone({ maxFrameBytes: 512, maxPayloadBytes: 2000 });
    lone.peer.onStream(hold);
    lone.start();

    const parts = splitFrame(
      { type: 'open', stream: 1, payload: { body: 'x'.repeat(9000) }, end: true },
      512,
      lone.codec,
    );
    expect(parts.length).toBeGreaterThan(6);

    let crossing: number | null = null;
    for (let index = 0; index < parts.length; index += 1) {
      lone.inject(parts[index]);
      await lone.settle();
      if (lone.framesOfType('reset').length > 0) {
        crossing = index;
        break;
      }
    }

    expect(crossing, 'the limit was never enforced').not.toBeNull();
    expect(crossing as number, 'the reset must not wait for the last fragment').toBeLessThan(parts.length - 1);
    const resets = lone.framesOfType('reset');
    expect(resets[resets.length - 1].code).toBe(ResetCode.PAYLOAD_TOO_LARGE);
    expect(lone.peer.isOpen).toBe(true);
    expect(lone.peer.streams.size, 'the partial buffer must be released with the stream').toBe(0);
  });

  it('defaults to 64 MiB and is never announced - WSM-FRG-035', async () => {
    // A local receiver setting, and a sender may learn it only from a reset.
    const pair = makePair();
    pair.acceptor.onStream(replyNow);
    pair.start();

    expect(internals(pair.acceptor).maxPayloadBytes).toBe(67_108_864);
    await pair.dialer.request({ q: 1 });
    await pair.settle();
    (['dialer', 'acceptor'] as Who[]).forEach((who) => {
      pair.sentBy(who).forEach((frame) => {
        expect(JSON.stringify(frame)).not.toContain('67108864');
      });
    });
  });
});

// --------------------------------------------------------------------------- concurrency

describe('the concurrency limit', () => {
  it('refuses an open beyond the receiver limit and the opener raises nothing locally - WSM-STM-036 (spec)', async () => {
    // Refused without invoking the handler, and `open()` never fails for it.
    const pair = makePair({ maxConcurrentStreams: 3 });
    const handled: unknown[] = [];

    pair.acceptor.onStream(async (payload: unknown, stream: Stream) => {
      handled.push(payload);
      await stream.closed;
    });
    pair.start();

    for (let index = 0; index < 3; index += 1) pair.dialer.open({ n: index });
    await pair.settle();
    expect(handled).toHaveLength(3);

    // One more. `open()` must not throw - the limit is the receiver's alone (WSM-API-004).
    const extra = pair.dialer.open({ n: 3 });
    expect(await rejection(extra)).toBeInstanceOf(StreamRefused);

    expect(handled, 'the handler must not run for a refused open').toHaveLength(3);
    const refusals = pair.framesOfType('acceptor', 'reset').filter((frame) => frame.code === ResetCode.REFUSED);
    expect(refusals.length).toBeGreaterThan(0);
    expect(pair.acceptor.isOpen).toBe(true);
  });

  it('does not count own opens against the limit - WSM-STM-037', async () => {
    // The limit bounds work the *remote* can impose.
    const pair = makePair({ maxConcurrentStreams: 3 });
    pair.acceptor.onStream(hold);
    pair.dialer.onStream(hold);
    pair.start();

    for (let index = 0; index < 5; index += 1) pair.acceptor.open({ mine: index });
    await pair.settle();

    for (let index = 0; index < 3; index += 1) pair.dialer.open({ theirs: index });
    await pair.settle();
    expect(internals(pair.acceptor).remoteStreamCount()).toBe(3);
    expect(pair.framesOfType('acceptor', 'reset').filter((frame) => frame.code === ResetCode.REFUSED)).toHaveLength(0);
  });

  it('frees a slot when a stream closes', async () => {
    const pair = makePair({ maxConcurrentStreams: 2 });
    pair.acceptor.onStream(hold);
    pair.start();

    const first = pair.dialer.open({ n: 0 });
    pair.dialer.open({ n: 1 });
    await pair.settle();

    const refused = pair.dialer.open({ n: 2 });
    expect(await rejection(refused)).toBeInstanceOf(StreamReset);
    await first.reset(ResetCode.NO_ERROR);
    await pair.settle();

    const accepted = pair.dialer.open({ n: 3 });
    await pair.settle();
    expect(pair.acceptor.streams.has(accepted.id)).toBe(true);
  });

  it('is never announced and never checked by the sender - WSM-STM-036/WSM-INV-007', async () => {
    // A sender that counted its own opens has rebuilt the announced quota. Python reads
    // `inspect.getsource(Peer.open)`; `Function.prototype.toString` is the same witness here, and the
    // spec build is not minified, so the identifier survives into what it returns.
    expect(Peer.prototype.open.toString(), 'open() must not consult the concurrency limit').not.toContain(
      'maxConcurrent',
    );

    const pair = makePair({ maxConcurrentStreams: 1 });
    pair.acceptor.onStream(hold);
    pair.start();

    for (let index = 0; index < 5; index += 1) pair.dialer.open({ n: index }); // none of these may throw
    await pair.settle();
    (['dialer', 'acceptor'] as Who[]).forEach((who) => {
      pair.sentBy(who).forEach((frame) => {
        const rendered = JSON.stringify(frame);
        expect(rendered).not.toContain('maxConcurrentStreams');
        expect(rendered).not.toContain('max_concurrent_streams');
      });
    });
  });
});

// --------------------------------------------------------------------------- observability

describe('observability', () => {
  it('fires onFrame before encode and after decode with the byte length - WSM-OBS-003', async () => {
    // The handler always sees the logical frame, and the encoded length.
    const pair = makePair();
    pair.acceptor.onStream(replyNow);
    const seen: [string, Frame, number][] = [];
    pair.dialer.onFrame((direction, frame, byteLength) => {
      seen.push([direction, frame, byteLength]);
    });
    pair.start();

    await pair.dialer.request({ q: 1 });
    await pair.settle();

    const { codec } = pair;
    expect(seen[0][0]).toBe('tx');
    expect(seen.some((entry) => entry[0] === 'rx')).toBe(true);
    seen.forEach(([direction, frame, length]) => {
      // The hook sees the logical frame, not bytes: it can be re-encoded, which bytes could not be.
      expect(typeof frame.type).toBe('string');
      expect(length, `${direction} length was not the encoded one`).toBe(encodedLength(codec.encode(frame)));
    });
  });

  it('writes one line per frame at debug - WSM-OBS-001', async () => {
    // One line per frame, carrying `conn=` so two lines can be tied together.
    const original = logger.level;
    const pair = makePair();
    pair.acceptor.onStream(replyNow);
    pair.start();
    try {
      logger.level = 'debug';
      await pair.dialer.request({ q: 1 });
      await pair.settle();
    } finally {
      logger.level = original;
    }

    const lines = allLogLines().filter((line) => line.startsWith('muxws conn='));
    expect(lines.length).toBeGreaterThan(0);
    expect(lines.some((line) => line.includes(`conn=${pair.dialer.id}`))).toBe(true);
    expect(lines.some((line) => line.includes('dir=tx'))).toBe(true);
    expect(lines.some((line) => line.includes('dir=rx'))).toBe(true);
    expect(lines.every((line) => line.includes('bytes='))).toBe(true);
  });

  it('never puts payload contents in any log record - WSM-OBS-002', async () => {
    // Application data routinely contains secrets.
    const original = logger.level;
    const pair = makePair();
    pair.acceptor.onStream(replyNow);
    pair.start();
    try {
      logger.level = 'debug';
      await pair.dialer.request({ password: 'hunter2-sentinel' });
      await pair.settle();
    } finally {
      logger.level = original;
    }

    const lines = allLogLines();
    expect(lines.length, 'the level was raised, so something must have been logged').toBeGreaterThan(0);
    lines.forEach((line) => {
      expect(line).not.toContain('hunter2-sentinel');
    });
  });

  it('never sends window_update - WSM-BPR-001', async () => {
    // Reserved in v1, and the only flow control is the two local defences.
    const pair = makePair({ maxConcurrentStreams: 2, maxPayloadBytes: 4000 });
    pair.acceptor.onStream(replyNow);
    pair.start();

    for (let index = 0; index < 6; index += 1) pair.dialer.open({ q: 1 });
    await pair.settle();
    (['dialer', 'acceptor'] as Who[]).forEach((who) => {
      expect(pair.sentBy(who).filter((frame) => frame.type === 'window_update')).toHaveLength(0);
    });
  });
});
