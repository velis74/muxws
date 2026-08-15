/**
 * Connection liveness and orderly shutdown (§6).
 *
 * A mirror of `muxws/lifecycle_test.py`, test for test. Where an assertion is spelled differently it
 * is because JavaScript offers a different witness of the same fact - `stream.signal.aborted` where
 * Python reads `stream.closed.is_set()`, a rejected promise where Python raises out of an `await` -
 * and each of those places says so. The one substantive difference is the unit: `peer.ping()` returns
 * **milliseconds** here and seconds in Python (WSM-CON-012).
 */

// `describe`/`it`/`expect` are configured as globals; `vi` is imported because the shared eslint
// config does not know it as one.
import { vi } from 'vitest';

import { type Codec, JsonCodec } from './codec';
import { ConnectionClosed, ConnectionGoingAway, ConnectionLost, ResetCode, StreamRefused } from './errors';
import type { Frame } from './frames';
import { GoawayState, MAX_STREAM_ID, newNonce, PingRegistry } from './lifecycle';
import { Peer } from './peer';
import { type Stream, StreamState } from './stream';
import { MemorySocket, memoryPair } from './transports/memory';

// --------------------------------------------------------------------------- the harness

type Who = 'dialer' | 'acceptor';

/** The decoder the test itself reads the wire with, independent of the pair's own codec. */
const WIRE = new JsonCodec();

function sleep(ms: number): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
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

  /**
   * Let both read loops and both writers reach quiescence.
   *
   * Python spins the event loop with `asyncio.sleep(0)`; a macrotask turn is the JavaScript
   * equivalent that also drains the microtask queue, which is where every peer-internal step runs.
   */
  async settle(rounds = 12): Promise<void> {
    for (let turn = 0; turn < rounds; turn += 1) {
      await sleep(0);
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

  /** Deliver a frame straight into `who`'s inbox, bypassing the peer that would have sent it. */
  inject(who: Who, frame: Frame): void {
    this.socketOf(who).inject(this.codec.encode(frame));
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
function makePair(): Pair {
  const [left, right] = memoryPair();
  const codec = new JsonCodec();
  const pair = new Pair(
    new Peer(left, { codec, isDialer: true }),
    new Peer(right, { codec, isDialer: false }),
    left,
    right,
    codec,
  );
  built.push(pair);
  return pair;
}

/**
 * The peer's private bookkeeping, which the Python suite reads as `_next_id` and `_send_goaway`.
 *
 * Reaching for it is deliberate and mirrors what the Python tests do: driving the allocator to
 * exhaustion and sending a `goaway` without a counterpart are not things the public API offers.
 */
interface PeerInternals {
  nextId: number;
  dispatch(frame: Frame): Promise<boolean>;
  sendGoaway(code: ResetCode, reason: string | null): void;
}

function internals(peer: Peer): PeerInternals {
  return peer as unknown as PeerInternals;
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

const LEVELS = ['debug', 'info', 'warn', 'error'] as const;

beforeEach(() => {
  LEVELS.forEach((level) => {
    vi.spyOn(console, level).mockImplementation(() => undefined);
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

// --------------------------------------------------------------------------- establishment

describe('establishment', () => {
  it('requires no frame before any other', async () => {
    // WSM-CON-030: no handshake phase. A peer may open a stream on its first frame.
    const pair = makePair();
    pair.acceptor.onStream(replyNow);
    pair.start();

    await pair.dialer.request({ q: 1 });
    await pair.settle();

    expect(pair.sentBy('dialer')[0].type).toBe('open');
    expect(pair.sentBy('acceptor')[0].type).toBe('data');
  });

  it('never emits a settings frame', async () => {
    // WSM-CON-031: no limit, version or capability appears on the wire in any form.
    const forbidden = [
      'settings',
      'ack',
      'protocol_version',
      'extensions',
      'max_frame_bytes',
      'max_concurrent_streams',
      'max_payload_bytes',
    ];
    const pair = makePair();
    pair.acceptor.onStream(replyNow);
    pair.start();

    await pair.dialer.request({ q: 1 });
    await pair.dialer.ping();
    await pair.dialer.close();
    await pair.settle();

    (['dialer', 'acceptor'] as Who[]).forEach((who) => {
      pair.sentBy(who).forEach((frame) => {
        expect(frame.type).not.toBe('settings');
        forbidden.forEach((word) => {
          expect((frame as unknown as Record<string, unknown>)[word] ?? null).toBeNull();
        });
      });
    });
  });

  it('omits stream on every connection-level frame', async () => {
    // WSM-FRM-015: `ping`, `pong` and `goaway` are connection-level.
    const pair = makePair();
    pair.acceptor.onStream(replyNow);
    pair.start();

    await pair.dialer.ping();
    await pair.dialer.close();
    await pair.settle();

    (['dialer', 'acceptor'] as Who[]).forEach((who) => {
      pair
        .sentBy(who)
        .filter((frame) => ['ping', 'pong', 'goaway'].includes(frame.type))
        .forEach((frame) => {
          expect(frame.stream ?? 0).toBe(0);
        });
    });
  });
});

// --------------------------------------------------------------------------- ping / pong

describe('ping and pong', () => {
  it('echoes the ping nonce verbatim', async () => {
    // WSM-CON-010: the exact nonce string, promptly.
    const pair = makePair();
    pair.acceptor.onStream(replyNow);
    pair.start();

    await pair.dialer.ping();
    await pair.settle();

    const sent = pair.framesOfType('dialer', 'ping').map((frame) => frame.nonce);
    const echoed = pair.framesOfType('acceptor', 'pong').map((frame) => frame.nonce);
    expect(sent.length).toBeGreaterThan(0);
    expect(echoed).toEqual(sent);
  });

  it('needs no application involvement to answer a ping', async () => {
    // WSM-CON-010: no `onStream` handler anywhere, and the ping still round-trips.
    const pair = makePair();
    pair.start();

    const elapsed = await pair.dialer.ping();
    expect(elapsed).toBeGreaterThanOrEqual(0);
  });

  it('returns the round trip time in milliseconds', async () => {
    // WSM-CON-012: milliseconds in TypeScript; seconds as a float in Python.
    const pair = makePair();
    pair.start();

    const quick = await pair.dialer.ping();
    expect(typeof quick).toBe('number');
    // An in-memory round trip is fast, so this alone cannot tell the two units apart.
    expect(quick).toBeGreaterThanOrEqual(0);
    expect(quick).toBeLessThan(1000);

    // What does tell them apart: hold the pong back for a known delay. Sixty milliseconds reads as
    // ~60 here and would read as ~0.06 if this returned seconds.
    const held = 60;
    const acceptor = internals(pair.acceptor);
    const original = acceptor.dispatch.bind(pair.acceptor);
    acceptor.dispatch = async (frame: Frame): Promise<boolean> => {
      if (frame.type === 'ping') await sleep(held);
      return original(frame);
    };

    const measured = await pair.dialer.ping();
    expect(measured).toBeGreaterThan(held / 2);
  });

  it('raises on a ping timeout without killing the connection', async () => {
    // The call fails; the connection is untouched, which is what lets liveness detection be built on
    // top of it.
    const pair = makePair();
    pair.acceptor.onStream(replyNow);
    pair.start();

    // Swallow every pong on the way back in.
    const dialer = internals(pair.dialer);
    const original = dialer.dispatch.bind(pair.dialer);
    dialer.dispatch = async (frame: Frame): Promise<boolean> => (frame.type === 'pong' ? true : original(frame));

    const error = await rejection(pair.dialer.ping(50));
    expect(error).toBeInstanceOf(ConnectionClosed);
    expect((error as Error).message).toMatch(/no pong/);

    expect(pair.dialer.isOpen).toBe(true);
    dialer.dispatch = original;
    expect(await pair.dialer.request({ q: 1 })).toEqual({ ok: true });
  });

  it('never uses native websocket ping frames', async () => {
    // WSM-CON-011, and TypeScript-only: browsers do not expose control frames to JavaScript, so
    // liveness has to be built out of ordinary messages. The witness is that `sendText`/`sendBytes`
    // are the only send paths taken and the port offers no other.
    const pair = makePair();
    pair.start();
    const socket = pair.dialerSocket;
    const sendText = vi.spyOn(socket, 'sendText');
    const sendBytes = vi.spyOn(socket, 'sendBytes');

    await pair.dialer.ping();
    await pair.settle();

    expect(sendText).toHaveBeenCalledTimes(1);
    expect(sendBytes).not.toHaveBeenCalled();
    expect(WIRE.decode(sendText.mock.calls[0][0]).type).toBe('ping');
    const port = socket as unknown as Record<string, unknown>;
    expect(port.ping).toBeUndefined();
    expect(port.pong).toBeUndefined();
  });

  it('ignores a pong for a nonce nobody waited for', async () => {
    // A late echo of a ping whose deadline already expired is not an error.
    const pair = makePair();
    pair.start();

    pair.inject('dialer', { type: 'pong', nonce: 'nobody-waited-for-this' });
    await pair.settle();

    expect(pair.dialer.isOpen).toBe(true);
  });

  it('keys pings by nonce and never by order', async () => {
    // Matching by position would credit a late echo to the next ping and invent an RTT.
    const registry = new PingRegistry();
    const first = registry.open('aaa');
    const second = registry.open('bbb');
    expect(registry.size).toBe(2);

    expect(registry.settle('bbb', 0.5)).toBe(true);
    expect(await second).toBe(0.5);

    // `first` is untouched. An already-resolved sentinel wins the race only against a promise that
    // has not settled, which is this language's reading of Python's `assert not first.done()`.
    const pending = Symbol('pending');
    expect(await Promise.race([first, Promise.resolve(pending)])).toBe(pending);

    expect(registry.settle('zzz', 0.1)).toBe(false);
    registry.giveUp('aaa');
    expect(registry.size).toBe(0);
  });

  it('produces a nonce that is not guessable', () => {
    // `crypto.getRandomValues`, not `Math.random`: the reconnect jitter is the opposite case.
    const nonces = new Set(Array.from({ length: 50 }, () => newNonce()));
    expect(nonces.size).toBe(50);
    [...nonces].forEach((nonce) => {
      expect(nonce).toMatch(/^[0-9a-f]{16}$/);
    });
  });

  it('fails a pending ping when the socket dies', async () => {
    // Nobody is going to answer, so nobody should keep waiting.
    const pair = makePair();
    pair.start();

    const dialer = internals(pair.dialer);
    const original = dialer.dispatch.bind(pair.dialer);
    dialer.dispatch = async (frame: Frame): Promise<boolean> => (frame.type === 'pong' ? true : original(frame));

    const pinging = pair.dialer.ping(5000);
    void pinging.catch(() => undefined);
    await pair.settle();
    pair.dialerSocket.drop();
    await pair.settle();

    const error = await rejection(pinging);
    expect(error instanceof ConnectionLost || error instanceof ConnectionClosed).toBe(true);
  });
});

// --------------------------------------------------------------------------- goaway

describe('goaway', () => {
  it('carries code, reason and the remote parity last_stream', async () => {
    // WSM-CON-020: `last_stream` is the **remote's** parity, not ours.
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    pair.dialer.open({ q: 1 });
    pair.dialer.open({ q: 2 });
    await pair.settle();

    // A short drain, because both held streams are still live and the default would sit out the
    // whole ten seconds; the frame under test is already on the wire before the drain begins.
    await pair.acceptor.close({ code: ResetCode.NO_ERROR, reason: 'shutting down', drainMs: 20 });
    await pair.settle();

    const goawayFrames = pair.framesOfType('acceptor', 'goaway');
    const goaway = goawayFrames[goawayFrames.length - 1];
    expect(goaway.code).toBe(ResetCode.NO_ERROR);
    expect(goaway.reason).toBe('shutting down');
    // The acceptor allocates even ids; the streams it promises to finish are the dialer's odd ones.
    expect(goaway.last_stream).toBe(3);
    expect((goaway.last_stream as number) % 2).toBe(1);
  });

  it('refuses incoming opens after sending goaway', async () => {
    // WSM-CON-021: nothing new runs here, and this peer opens nothing either.
    //
    // A lone acceptor with injected frames, rather than a live pair: sending a `goaway` and then
    // feeding it an `open` its counterpart never made would leave the two peers disagreeing about the
    // id space, and the dialer would kill the connection for a reset it cannot account for -
    // correctly, and entirely beside the point being tested here.
    const handled: unknown[] = [];
    const codec = new JsonCodec();
    const [, socket] = memoryPair();
    const acceptor = new Peer(socket, { codec, isDialer: false });
    acceptor.onStream(async (payload: unknown, stream: Stream) => {
      handled.push(payload);
      await stream.reply({ ok: true });
    });
    const serving = acceptor.serve();
    void serving.catch(() => undefined);

    try {
      internals(acceptor).sendGoaway(ResetCode.NO_ERROR, 'going');
      for (let turn = 0; turn < 12; turn += 1) await sleep(0);

      socket.inject(codec.encode({ type: 'open', stream: 1, payload: { q: 1 }, end: true }));
      for (let turn = 0; turn < 12; turn += 1) await sleep(0);

      expect(handled).toEqual([]);
      const resets = socket.sent.map((message) => codec.decode(message)).filter((frame) => frame.type === 'reset');
      expect(resets.length).toBeGreaterThan(0);
      // Nothing ran, so the opener may retry elsewhere.
      expect(resets[resets.length - 1].code).toBe(ResetCode.REFUSED);

      expect(() => acceptor.open({ q: 2 })).toThrow(ConnectionGoingAway);
    } finally {
      socket.drop();
      await serving.catch(() => undefined);
    }
  });

  it('throws ConnectionGoingAway synchronously from open after receiving goaway', async () => {
    // WSM-CON-022/WSM-API-004: out of the call, not out of an await.
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    pair.inject('dialer', { type: 'goaway', code: ResetCode.NO_ERROR, last_stream: 0 });
    await pair.settle();

    expect(() => pair.dialer.open({ q: 1 })).toThrow(ConnectionGoingAway);
    // `notify()` is `async` in both languages, so its throw arrives as a rejection here; the
    // synchronous half of the rule is the `open()` assertion above.
    expect(await rejection(pair.dialer.notify({ q: 1 }))).toBeInstanceOf(ConnectionGoingAway);
  });

  it('resets streams above last_stream locally with REFUSED', async () => {
    // WSM-CON-023: never processed, so safe to retry - and nothing goes out for them.
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    const below = pair.dialer.open({ q: 1 });
    const above = pair.dialer.open({ q: 2 });
    await pair.settle();
    const before = pair.sentBy('dialer').length;

    pair.inject('dialer', { type: 'goaway', code: ResetCode.NO_ERROR, last_stream: below.id });
    await pair.settle();

    expect(await rejection(above)).toBeInstanceOf(StreamRefused);
    // A stream at or below the cut-off keeps going.
    expect(below.state).not.toBe(StreamState.CLOSED);
    // Nothing is sent for a stream the remote never saw.
    expect(pair.sentBy('dialer').length).toBe(before);
  });

  it('lets streams at or below last_stream finish within the drain window', async () => {
    // WSM-CON-024: work the remote promised to complete is allowed to complete.
    const pair = makePair();
    pair.acceptor.onStream(async (payload: unknown, stream: Stream) => {
      await sleep(20);
      await stream.reply({ finished: true });
    });
    pair.start();

    const stream = pair.dialer.open({ q: 1 });
    await pair.settle();
    const closing = pair.acceptor.close({ drainMs: 500 });
    void closing.catch(() => undefined);

    expect(await stream).toEqual({ finished: true });
    await closing;
  });

  it('closes the socket at the drain deadline with streams still live', async () => {
    // WSM-CON-024: then the socket closes regardless, and those streams fail locally.
    const pair = makePair();
    pair.acceptor.onStream(hold);
    pair.start();

    const stream = pair.dialer.open({ q: 1 });
    await pair.settle();

    await pair.acceptor.close({ drainMs: 50 });
    await pair.settle();

    expect(pair.acceptor.isOpen).toBe(false);
    // `signal.aborted` is the TypeScript witness of Python's `stream.closed.is_set()`; the two move
    // together on every close path (WSM-API-023).
    expect(stream.signal.aborted).toBe(true);
  });

  it('sends goaway, then drains, then closes', async () => {
    // WSM-CON-025: assert the order, because the order is the whole rule.
    const pair = makePair();
    pair.acceptor.onStream(replyNow);
    pair.start();

    await pair.dialer.request({ q: 1 });
    await pair.dialer.close();
    await pair.settle();

    const types = pair.sentBy('dialer').map((frame) => frame.type);
    expect(types).toContain('goaway');
    expect(types.indexOf('goaway')).toBe(types.length - 1);
    expect(pair.dialerSocket.isClosed).toBe(true);
    expect(pair.dialer.isOpen).toBe(false);
  });

  it('is idempotent on close', async () => {
    const pair = makePair();
    pair.start();

    await pair.dialer.close();
    await pair.dialer.close();

    expect(pair.framesOfType('dialer', 'goaway').length).toBe(1);
  });

  it('sends goaway, drains and closes when ids run out - WSM-SID-007', async () => {
    // The rule asks for four things, not one: refusing to open again, and then the goaway, the drain
    // and the close, all of which a peer that only refused would skip.
    expect(MAX_STREAM_ID).toBe(2 ** 31 - 1);

    const pair = makePair();
    pair.acceptor.onStream(replyNow);
    pair.start();
    try {
      pair.dialer.nextId = MAX_STREAM_ID;
      const last = pair.dialer.open({ q: 1 }, { end: true });
      expect(last.id).toBe(MAX_STREAM_ID);

      // Still synchronous: the shutdown was started, not awaited (WSM-API-001).
      expect(pair.dialer.exhaustionShutdown).not.toBeNull();
      expect(() => pair.dialer.open({ q: 2 })).toThrow(ConnectionGoingAway);

      await pair.dialer.exhaustionShutdown;
      await pair.settle();

      const goaway = pair.framesOfType('dialer', 'goaway');
      expect(goaway.length, 'the exhausting peer must say goodbye rather than just refusing').toBeGreaterThan(0);
      expect(goaway[goaway.length - 1].code).toBe(ResetCode.NO_ERROR);
      expect(String(goaway[goaway.length - 1].reason)).toContain('exhausted');
      expect(pair.dialer.isOpen, 'and must then close').toBe(false);
    } finally {
      await pair.stop();
    }
  });
});

// --------------------------------------------------------------------------- the state object

describe('GoawayState', () => {
  it('tracks the two directions separately', () => {
    // Having sent one and having received one mean different things.
    const state = new GoawayState();
    expect(state.isGoingAway).toBe(false);

    state.sent = true;
    expect(state.isGoingAway).toBe(true);
    // With nothing received, no cut-off is known and every stream gets its drain window.
    expect(state.survivesDrain(999)).toBe(true);

    state.remoteLastStream = 5;
    expect(state.survivesDrain(5)).toBe(true);
    expect(state.survivesDrain(7)).toBe(false);
  });
});
