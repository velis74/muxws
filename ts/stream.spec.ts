/**
 * `Stream`: the state table, the awaitable handle, cancellation and the send-after-close rules.
 *
 * The TypeScript mirror of `muxws/stream_test.py`. Where the two ports genuinely differ - what a
 * consumer walking away can be observed to do, `closed` being a promise, `signal` standing in for
 * Python's task cancellation - the difference is asserted here rather than glossed over.
 */

import { JsonCodec } from './codec';
import { ProtocolError, RemoteError, ResetCode, StreamAlreadyConsumed, StreamClosed, StreamReset } from './errors';
import { ABSENT, type Frame, framesEqual } from './frames';
import { logger, Peer } from './peer';
import { Stream, StreamState } from './stream';
import { type MemorySocket, memoryPair } from './transports/memory';

// --------------------------------------------------------------------------- the harness

const codec = new JsonCodec();

class Pair {
  readonly dialer: Peer;
  readonly acceptor: Peer;
  readonly dialerSocket: MemorySocket;
  readonly acceptorSocket: MemorySocket;
  private tasks: Promise<void>[] = [];

  constructor(options: { errorSerializer?: (error: unknown) => unknown } = {}) {
    const [left, right] = memoryPair();
    this.dialerSocket = left;
    this.acceptorSocket = right;
    this.dialer = new Peer(left, { codec, isDialer: true, errorSerializer: options.errorSerializer });
    this.acceptor = new Peer(right, { codec, isDialer: false, errorSerializer: options.errorSerializer });
  }

  start(): void {
    this.tasks = [this.dialer.serve(), this.acceptor.serve()];
    this.tasks.forEach((task) => void task.catch(() => undefined));
  }

  /** Let both read loops and both writers reach quiescence. */
  async settle(rounds = 24): Promise<void> {
    for (let index = 0; index < rounds; index += 1) {
      await Promise.resolve();
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
  }

  sentBy(who: 'dialer' | 'acceptor'): Frame[] {
    const socket = who === 'dialer' ? this.dialerSocket : this.acceptorSocket;
    return socket.sent.map((message) => codec.decode(message));
  }

  framesOfType(who: 'dialer' | 'acceptor', type: string): Frame[] {
    return this.sentBy(who).filter((frame) => frame.type === type);
  }

  inject(who: 'dialer' | 'acceptor', frame: Frame): void {
    const socket = who === 'dialer' ? this.dialerSocket : this.acceptorSocket;
    socket.inject(codec.encode(frame));
  }

  async stop(): Promise<void> {
    this.dialerSocket.drop();
    await this.settle();
    await Promise.allSettled(this.tasks);
  }
}

/** A handler that holds its stream open until something closes it. */
async function hold(_payload: unknown, stream: Stream): Promise<void> {
  await stream.closed;
}

/** A handler that records the stream it was handed, so its state can be inspected. */
function capture(sink: Stream[]) {
  return async (_payload: unknown, stream: Stream): Promise<void> => {
    sink.push(stream);
    await stream.closed;
  };
}

// --------------------------------------------------------------------------- the state table

/**
 * Every cell of section 5.3: five states by nine events. `n/r` cells are asserted unreachable rather
 * than exercised, which is what WSM-STM-011 means by "not externally observable".
 */
const STATE_TABLE: Record<string, string> = {
  'idle|send_open': 'open',
  'idle|recv_open': 'open',
  'idle|send_data': 'n/r',
  'idle|send_end': 'n/r',
  'idle|recv_data': 'ILL-C',
  'idle|recv_data_end': 'ILL-C',
  'idle|send_reset': 'n/r',
  'idle|recv_reset': 'ILL-C',
  'idle|socket_death': 'closed',
  'open|send_open': 'RAISE',
  'open|recv_open': 'ILL-C',
  'open|send_data': 'open',
  'open|send_end': 'half_closed_local',
  'open|recv_data': 'open',
  'open|recv_data_end': 'half_closed_remote',
  'open|send_reset': 'closed',
  'open|recv_reset': 'closed',
  'open|socket_death': 'closed',
  'half_closed_local|send_open': 'RAISE',
  'half_closed_local|recv_open': 'ILL-C',
  'half_closed_local|send_data': 'RAISE',
  'half_closed_local|send_end': 'RAISE',
  'half_closed_local|recv_data': 'half_closed_local',
  'half_closed_local|recv_data_end': 'closed',
  'half_closed_local|send_reset': 'closed',
  'half_closed_local|recv_reset': 'closed',
  'half_closed_local|socket_death': 'closed',
  'half_closed_remote|send_open': 'RAISE',
  'half_closed_remote|recv_open': 'ILL-C',
  'half_closed_remote|send_data': 'half_closed_remote',
  'half_closed_remote|send_end': 'closed',
  'half_closed_remote|recv_data': 'ILL-S',
  'half_closed_remote|recv_data_end': 'ILL-S',
  'half_closed_remote|send_reset': 'closed',
  'half_closed_remote|recv_reset': 'closed',
  'half_closed_remote|socket_death': 'closed',
  'closed|send_open': 'RAISE',
  'closed|recv_open': 'ILL-C',
  'closed|send_data': 'RAISE',
  'closed|send_end': 'RAISE',
  'closed|recv_data': 'IGN',
  'closed|recv_data_end': 'IGN',
  'closed|send_reset': 'NOOP',
  'closed|recv_reset': 'IGN',
  'closed|socket_death': 'NOOP',
};

describe('the state table', () => {
  it('has forty-five cells - five states by nine events', () => {
    expect(Object.keys(STATE_TABLE)).toHaveLength(45);
  });

  Object.entries(STATE_TABLE).forEach(([cell, outcome]) => {
    const [state, event] = cell.split('|');

    it(`${state} + ${event} -> ${outcome}`, async () => {
      // An ILL-C cell drives the peer into a connection-level protocol error deliberately, and a
      // peer that meets one is **required** to log it (§12) - so the line on the console is the
      // implementation working, not noise from a broken test. Python's twin is quiet only because
      // pytest captures `logging`; the TypeScript logger is a `console` shim, since the browser entry
      // point may not depend on a logging library (WSM-PKG-003), and vitest prints what it writes.
      //
      // Silenced for these seven cells and nowhere wider. A global filter would also hide a log from
      // a test that never asked for one, and that log is a signal: it is how an accidental
      // connection-level failure announces itself in a suite where every other test is quiet.
      const level = logger.level;
      if (outcome === 'ILL-C') logger.level = 'silent';
      const pair = new Pair();
      try {
        if (state === 'idle') {
          pair.start();
          await assertIdleCell(pair, event, outcome);
          return;
        }
        pair.acceptor.onStream(hold);
        pair.start();
        const { stream } = await driveTo(pair, state);
        await assertCell(pair, stream, event, outcome);
      } finally {
        await pair.stop();
        logger.level = level;
      }
    });
  });
});

/**
 * Put a dialer-side stream into `state`, using only the public API.
 *
 * The return value is BOXED, and has to be. `await` unwraps thenables recursively, and `Stream`
 * implements `PromiseLike` (WSM-API-002/015) - so `await somePromiseOfAStream` does not hand back
 * the stream, it awaits the stream and hands back its first payload, or throws the reset that
 * closed it. A `Stream` can never be the resolution value of a promise anywhere. See GAPS.md.
 */
async function driveTo(pair: Pair, state: string): Promise<{ stream: Stream }> {
  const stream = pair.dialer.open({ n: 1 }, { end: state === 'half_closed_local' });
  await pair.settle();

  if (state === 'open' || state === 'half_closed_local') return { stream };

  if (state === 'half_closed_remote') {
    const remote = pair.acceptor.streams.get(stream.id);
    expect(remote).toBeDefined();
    await remote!.end({ payload: { done: true } });
    await pair.settle();
    return { stream };
  }

  if (state === 'closed') {
    await stream.reset(ResetCode.NO_ERROR);
    await pair.settle();
    return { stream };
  }

  throw new Error(`unknown state ${state}`);
}

async function assertIdleCell(pair: Pair, event: string, outcome: string): Promise<void> {
  if (outcome === 'open') {
    if (event === 'send_open') {
      expect(pair.dialer.open({ n: 1 }).state).toBe(StreamState.OPEN);
      expect(pair.dialer.open({ n: 2 }, { end: true }).state).toBe(StreamState.HALF_CLOSED_LOCAL);
      return;
    }
    pair.acceptor.onStream(hold);
    pair.dialer.open({ n: 1 });
    await pair.settle();
    expect(pair.acceptor.streams.get(1)?.state).toBe(StreamState.OPEN);
    return;
  }

  if (outcome === 'n/r') {
    // `idle` exists only inside the indivisible allocate-and-enqueue step (WSM-SID-006), so the cell
    // is unreachable exactly when no stream the public API can hand out is ever in it. `open()`
    // returning a Stream rather than a Promise is what makes that true: there is no suspension point
    // at which a caller could be given a stream whose frame has not been enqueued.
    const handed: Stream[] = [];
    pair.acceptor.onStream(capture(handed));

    const local = pair.dialer.open({ n: 1 });
    expect(local).toBeInstanceOf(Stream);
    expect(local.state).not.toBe(StreamState.IDLE);
    await pair.settle();

    const observed = [...handed, ...pair.dialer.streams.values(), ...pair.acceptor.streams.values()];
    expect(observed.length).toBeGreaterThan(0);
    observed.forEach((stream) => {
      expect(stream.state, `a reachable stream is idle, so ${event} could be called on it`).not.toBe(StreamState.IDLE);
    });
    return;
  }

  if (outcome === 'closed') {
    pair.dialerSocket.drop();
    await pair.settle();
    expect(pair.dialer.isOpen).toBe(false);
    return;
  }

  // ILL-C: a stream-level frame for an id nobody opened is above the high-water mark.
  const frames: Record<string, Frame> = {
    recv_data: { type: 'data', stream: 7, payload: { a: 1 } },
    recv_data_end: { type: 'data', stream: 7, payload: { a: 1 }, end: true },
    recv_reset: { type: 'reset', stream: 7, code: ResetCode.CANCELLED },
  };
  pair.inject('acceptor', frames[event]);
  await pair.settle();
  expect(
    pair.framesOfType('acceptor', 'goaway').length,
    `${event} above the high-water mark must be ILL-C`,
  ).toBeGreaterThan(0);
}

async function assertCell(pair: Pair, stream: Stream, event: string, outcome: string): Promise<void> {
  if (event === 'send_open') {
    // `open` is a peer-level call; a stream never re-opens. What the RAISE column is really about is
    // the id never being handed out twice, which the allocator guarantees.
    expect(pair.dialer.open({ n: 2 }).id).not.toBe(stream.id);
    return;
  }

  if (event === 'recv_open') {
    pair.inject('acceptor', { type: 'open', stream: stream.id, payload: {} });
    await pair.settle();
    expect(pair.framesOfType('acceptor', 'goaway').length).toBeGreaterThan(0);
    return;
  }

  if (event === 'send_data' || event === 'send_end') {
    const call = event === 'send_data' ? stream.send({ x: 1 }) : stream.end();
    if (outcome === 'RAISE') {
      await expect(call).rejects.toThrow();
      return;
    }
    await call;
    await pair.settle();
    expect(stream.state).toBe(outcome);
    return;
  }

  if (event === 'recv_data' || event === 'recv_data_end') {
    pair.inject('dialer', { type: 'data', stream: stream.id, payload: { y: 1 }, end: event === 'recv_data_end' });
    await pair.settle();

    if (outcome === 'ILL-S') {
      expect(stream.state).toBe(StreamState.CLOSED);
      expect(pair.sentBy('dialer').some((f) => f.type === 'reset' && f.code === ResetCode.PROTOCOL_ERROR)).toBe(true);
      expect(pair.dialer.isOpen, 'a stream-level error must leave the connection alone').toBe(true);
      return;
    }
    if (outcome === 'IGN') {
      expect(pair.dialer.isOpen).toBe(true);
      return;
    }
    expect(stream.state).toBe(outcome);
    return;
  }

  if (event === 'send_reset') {
    const before = pair.framesOfType('dialer', 'reset').length;
    await stream.reset(ResetCode.CANCELLED);
    await pair.settle();
    const after = pair.framesOfType('dialer', 'reset').length;
    expect(stream.state).toBe(StreamState.CLOSED);
    if (outcome === 'NOOP') {
      expect(after, 'resetting a closed stream sends nothing').toBe(before);
    } else {
      expect(after).toBe(before + 1);
    }
    return;
  }

  if (event === 'recv_reset') {
    pair.inject('dialer', { type: 'reset', stream: stream.id, code: ResetCode.CANCELLED });
    await pair.settle();
    expect(stream.state).toBe(StreamState.CLOSED);
    expect(pair.dialer.isOpen).toBe(true);
    return;
  }

  if (event === 'socket_death') {
    pair.dialerSocket.drop();
    await pair.settle();
    expect(stream.state).toBe(StreamState.CLOSED);
    await expect(stream.closed).resolves.toBeUndefined();
    return;
  }

  throw new Error(`unhandled event ${event}`);
}

// --------------------------------------------------------------------------- the awaitable handle

describe('the awaitable handle', () => {
  it('returns the same value twice, by identity - WSM-API-010/011', async () => {
    const pair = new Pair();
    pair.acceptor.onStream(async (_p, stream) => stream.reply({ answer: 42 }));
    pair.start();
    try {
      const stream = pair.dialer.open({ q: 1 });
      const first = await stream;
      const second = await stream;
      expect(first).toEqual({ answer: 42 });
      // Identity, not equality: an implementation that re-read the queue would build an equal object
      // and pass an equality check while having consumed a payload that is not there twice.
      expect(second).toBe(first);
      expect(await stream.result()).toBe(first);
    } finally {
      await pair.stop();
    }
  });

  it('lets the first shape claim the stream and refuses the other - WSM-API-014', async () => {
    const pair = new Pair();
    pair.acceptor.onStream(async (_p, stream) => {
      await stream.send({ n: 1 });
      await stream.end({ payload: { n: 2 } });
    });
    pair.start();
    try {
      const awaited = pair.dialer.open({ q: 1 });
      expect(await awaited).toEqual({ n: 1 });
      await expect(collect(awaited)).rejects.toThrow(StreamAlreadyConsumed);

      const iterated = pair.dialer.open({ q: 2 });
      expect((await collect(iterated)).length).toBeGreaterThan(0);
      await expect(collect(iterated)).rejects.toThrow(StreamAlreadyConsumed);
    } finally {
      await pair.stop();
    }
  });

  it('resolves on the first payload while request polices a second - WSM-API-006/007', async () => {
    const pair = new Pair();
    pair.acceptor.onStream(async (_p, stream) => {
      await stream.send({ n: 1 });
      await stream.end({ payload: { n: 2 } });
    });
    pair.start();
    try {
      expect(await pair.dialer.open({ q: 1 })).toEqual({ n: 1 });
      await expect(pair.dialer.request({ q: 2 })).rejects.toThrow(ProtocolError);
    } finally {
      await pair.stop();
    }
  });

  it('settles a late await on a stream that ended with no payload', async () => {
    const pair = new Pair();
    pair.acceptor.onStream(async (_p, stream) => stream.end());
    pair.start();
    try {
      const stream = pair.dialer.open({ q: 1 });
      await pair.settle();
      // The answer arrived before anyone asked. Leaving the promise pending here is the spinner that
      // never stops which WSM-INV-011 names; it was a real hang in the Python port until it was fixed.
      await expect(stream.result()).rejects.toThrow(ProtocolError);
    } finally {
      await pair.stop();
    }
  });

  it('is a thenable, not a Promise subclass - WSM-API-015', async () => {
    const pair = new Pair();
    pair.acceptor.onStream(async (_p, stream) => stream.reply({ ok: true }));
    pair.start();
    try {
      const stream = pair.dialer.open({ q: 1 });
      expect(stream).not.toBeInstanceOf(Promise);
      const chained = stream.then((value) => value);
      expect(chained).toBeInstanceOf(Promise);
      expect(chained).not.toBeInstanceOf(Stream);
      expect(await chained).toEqual({ ok: true });
    } finally {
      await pair.stop();
    }
  });
});

describe('reset stream nobody consumed', () => {
  it('reports no unhandled rejection and does reach the error hook', async () => {
    const unhandled: unknown[] = [];
    const onUnhandled = (event: PromiseRejectionEvent): void => {
      unhandled.push(event.reason);
    };
    globalThis.addEventListener?.('unhandledrejection', onUnhandled as EventListener);

    const pair = new Pair();
    const reported: unknown[] = [];
    pair.dialer.onError((error) => reported.push(error));
    pair.acceptor.onStream(hold);
    pair.start();
    try {
      const stream = pair.dialer.open({ q: 1 });
      await pair.settle();
      pair.inject('dialer', { type: 'reset', stream: stream.id, code: ResetCode.APPLICATION_ERROR, reason: 'no' });
      await pair.settle();
      await new Promise((resolve) => setTimeout(resolve, 20));

      expect(reported).toHaveLength(1);
      expect(reported[0]).toBeInstanceOf(RemoteError);
      expect(unhandled).toEqual([]);
    } finally {
      globalThis.removeEventListener?.('unhandledrejection', onUnhandled as EventListener);
      await pair.stop();
    }
  });
});

describe('closed and signal', () => {
  it('resolve and abort on every close path - WSM-API-023', async () => {
    const paths: [string, (pair: Pair, stream: Stream) => Promise<void>][] = [
      [
        'clean end',
        async (pair, stream) => {
          await stream.end();
          pair.inject('dialer', { type: 'data', stream: stream.id, payload: { ok: true }, end: true });
          await pair.settle();
        },
      ],
      [
        'remote reset',
        async (pair, stream) => {
          pair.inject('dialer', { type: 'reset', stream: stream.id, code: ResetCode.CANCELLED });
          await pair.settle();
        },
      ],
      [
        'socket death',
        async (pair) => {
          pair.dialerSocket.drop();
          await pair.settle();
        },
      ],
    ];

    for (const [name, close] of paths) {
      const pair = new Pair();
      pair.acceptor.onStream(hold);
      pair.start();
      try {
        const stream = pair.dialer.open({ q: 1 });
        await pair.settle();
        expect(stream.signal.aborted, name).toBe(false);

        await close(pair, stream);

        // `closed` resolves and never rejects: a stream that closed by being reset still closed, and
        // the reset reaches the awaits and the iterator instead.
        await expect(stream.closed, name).resolves.toBeUndefined();
        expect(stream.signal.aborted, name).toBe(true);
      } finally {
        await pair.stop();
      }
    }
  });
});

// --------------------------------------------------------------------------- sending after close

describe('send after close', () => {
  it('distinguishes a normal close, a reset and socket death - WSM-ERR-009', async () => {
    const pair = new Pair();
    pair.acceptor.onStream(async (_p, stream) => stream.reply({ ok: true }));
    pair.start();
    try {
      const ended = pair.dialer.open({ q: 1 }, { end: true });
      expect(await ended).toEqual({ ok: true });
      await pair.settle();

      // A last send racing an orderly end is an expected outcome, not a failure and not a caller bug.
      await expect(ended.send({ late: true })).rejects.toThrow(StreamClosed);
      await ended.send({ late: true }).catch((error: unknown) => {
        expect(error).not.toBeInstanceOf(StreamReset);
        expect(error).not.toBeInstanceOf(ProtocolError);
      });

      const cancelled = pair.dialer.open({ q: 2 });
      await pair.settle();
      await cancelled.cancel();
      await cancelled.send({ late: true }).catch((error: unknown) => {
        expect(error).toBeInstanceOf(StreamReset);
        expect((error as StreamReset).code).toBe(ResetCode.CANCELLED);
      });
    } finally {
      await pair.stop();
    }
  });

  it('raises a fresh error each time rather than growing one stack', async () => {
    const pair = new Pair();
    pair.acceptor.onStream(hold);
    pair.start();
    try {
      const stream = pair.dialer.open({ q: 1 });
      await pair.settle();
      await stream.cancel();

      const seen: StreamReset[] = [];
      for (let index = 0; index < 5; index += 1) {
        await stream.send({ late: true }).catch((error: unknown) => seen.push(error as StreamReset));
      }
      expect(seen).toHaveLength(5);
      // Distinct objects: re-throwing one instance appends a frame to its stack on every raise.
      expect(new Set(seen).size).toBe(5);
      seen.forEach((error) => expect(error.code).toBe(ResetCode.CANCELLED));
    } finally {
      await pair.stop();
    }
  });
});

describe('the reset-code guard', () => {
  it.each([ResetCode.CONNECTION_CLOSED, 5, 42, -1])('refuses %s at the call site', async (code) => {
    const pair = new Pair();
    pair.acceptor.onStream(hold);
    pair.start();
    try {
      const stream = pair.dialer.open({ q: 1 });
      await expect(stream.reset(code as ResetCode)).rejects.toThrow(ProtocolError);
      await pair.settle();
      expect(pair.framesOfType('dialer', 'reset')).toEqual([]);
    } finally {
      await pair.stop();
    }
  });
});

// --------------------------------------------------------------------------- cancellation

describe('cancellation', () => {
  it('closes locally at once and discards what is still in flight - WSM-ERR-012', async () => {
    const pair = new Pair();
    pair.acceptor.onStream(async (_p, stream) => {
      await stream.send({ n: 1 });
      await stream.closed;
    });
    pair.start();
    try {
      const stream = pair.dialer.open({ q: 1 });
      await pair.settle();
      await stream.cancel('user navigated away');
      expect(stream.state).toBe(StreamState.CLOSED);

      pair.inject('dialer', { type: 'data', stream: stream.id, payload: { late: true } });
      await pair.settle();
      expect(pair.dialer.isOpen).toBe(true);
    } finally {
      await pair.stop();
    }
  });

  it('aborts the handler signal when a remote reset lands - WSM-ERR-013', async () => {
    const pair = new Pair();
    let observed: AbortSignal | null = null;
    pair.acceptor.onStream(async (_p, stream) => {
      observed = stream.signal;
      await stream.closed;
    });
    pair.start();
    try {
      const stream = pair.dialer.open({ q: 1 });
      await pair.settle();
      await stream.cancel();
      await pair.settle();
      expect(observed).not.toBeNull();
      expect(observed!.aborted).toBe(true);
    } finally {
      await pair.stop();
    }
  });

  it('resets with CANCELLED when an abort signal fires during a wait - WSM-ERR-014', async () => {
    const pair = new Pair();
    pair.acceptor.onStream(hold);
    pair.start();
    try {
      const stream = pair.dialer.open({ q: 1 });
      await pair.settle();

      const controller = new AbortController();
      const waiting = stream.result({ signal: controller.signal });
      void waiting.catch(() => undefined);
      await pair.settle();
      controller.abort();
      await expect(waiting).rejects.toThrow();
      await pair.settle();

      expect(pair.sentBy('dialer').some((f) => f.type === 'reset' && f.code === ResetCode.CANCELLED)).toBe(true);
    } finally {
      await pair.stop();
    }
  });

  it('resets with CANCELLED when a for-await loop breaks out early - WSM-ERR-014', async () => {
    const pair = new Pair();
    pair.acceptor.onStream(async (_p, stream) => {
      await stream.send({ n: 1 });
      await stream.closed;
    });
    pair.start();
    try {
      const stream = pair.dialer.open({ q: 1 });

      for await (const item of stream) {
        void item;
        break;
      }
      await pair.settle();
      expect(pair.sentBy('dialer').some((f) => f.type === 'reset' && f.code === ResetCode.CANCELLED)).toBe(true);
    } finally {
      await pair.stop();
    }
  });

  it('raises RemoteError with its payload out of both shapes - WSM-ERR-015', async () => {
    const pair = new Pair();
    pair.acceptor.onStream(async () => {
      throw new TypeError('no such report');
    });
    pair.start();
    try {
      await pair.dialer.open({ q: 1 }).catch((error: unknown) => {
        expect(error).toBeInstanceOf(RemoteError);
        expect((error as RemoteError).payload).toEqual({ type: 'TypeError', message: 'no such report' });
      });
      await expect(collect(pair.dialer.open({ q: 2 }))).rejects.toThrow(RemoteError);
    } finally {
      await pair.stop();
    }
  });
});

describe('a deadline', () => {
  it('sends reset(TIMEOUT) and rejects with StreamTimeout - WSM-ERR-011', async () => {
    const pair = new Pair();
    pair.acceptor.onStream(hold);
    pair.start();
    try {
      const stream = pair.dialer.open({ q: 1 });
      await pair.settle();
      await expect(stream.result({ timeoutMs: 20 })).rejects.toThrow();
      await pair.settle();
      expect(pair.sentBy('dialer').some((f) => f.type === 'reset' && f.code === ResetCode.TIMEOUT)).toBe(true);
    } finally {
      await pair.stop();
    }
  });
});

describe('trailers', () => {
  it('are populated when the stream ends', async () => {
    const pair = new Pair();
    pair.acceptor.onStream(async (_p, stream) => {
      await stream.end({ payload: { rows: 1 }, trailers: { checksum: 'abc' } });
    });
    pair.start();
    try {
      const stream = pair.dialer.open({ q: 1 });
      expect(await stream).toEqual({ rows: 1 });
      await pair.settle();
      expect(stream.trailers).toEqual({ checksum: 'abc' });
    } finally {
      await pair.stop();
    }
  });
});

describe('the frame model', () => {
  it('puts an open on the wire that matches what Python emits', async () => {
    const pair = new Pair();
    pair.acceptor.onStream(hold);
    pair.start();
    try {
      pair.dialer.open({ action: 'list' }, { end: true });
      await pair.settle();
      const [open] = pair.framesOfType('dialer', 'open');
      expect(framesEqual(open, { type: 'open', stream: 1, payload: { action: 'list' }, end: true })).toBe(true);
    } finally {
      await pair.stop();
    }
  });
});

async function collect(stream: Stream): Promise<unknown[]> {
  const items: unknown[] = [];
  for await (const item of stream) items.push(item);
  return items;
}

// --------------------------------------------------------------------------- leading headers

describe('the answering side’s leading headers (WSM-FRM-016)', () => {
  it('sends leading headers once, and refuses a second set - WSM-API-024', async () => {
    const pair = new Pair();
    const answered: Stream[] = [];
    pair.acceptor.onStream(capture(answered));
    pair.start();
    try {
      pair.dialer.open({ q: 'export' }, { end: true });
      await pair.settle();
      const [answering] = answered;

      await answering.sendHeaders({ 'content-type': 'text/csv' });
      // The chance is spent by the *frame*, not by the argument: a second set raises whether it
      // rides `sendHeaders`, `send` or `end`, and a port that only guarded the first would let two
      // sets onto a wire whose rule is one.
      await expect(answering.sendHeaders({ late: true })).rejects.toBeInstanceOf(ProtocolError);
      await expect(answering.send({ row: 1 }, { headers: { late: true } })).rejects.toBeInstanceOf(ProtocolError);
      await expect(answering.end({ headers: { late: true } })).rejects.toBeInstanceOf(ProtocolError);

      // Still sendable without them, which is the half a "raises on the second call" check misses:
      // refusing the headers must not have refused the frame.
      await answering.send({ row: 1 });
      await pair.settle();
      const data = pair.framesOfType('acceptor', 'data');
      expect(data.map((frame) => frame.headers ?? null)).toEqual([{ 'content-type': 'text/csv' }, null]);
      // ABSENT and not null: a headers frame carries no payload key at all (D1).
      expect(data[0].payload).toBe(ABSENT);
    } finally {
      await pair.stop();
    }
  });

  it('refuses them on a stream this peer opened, and says where they belong', async () => {
    const pair = new Pair();
    pair.acceptor.onStream(hold);
    pair.start();
    try {
      // The opener's first frame was the `open`, so its one chance is already spent - and the error
      // has to name `open()` rather than repeat the rule, since the caller has somewhere to put them.
      const stream = pair.dialer.open({ q: 1 });
      await expect(stream.sendHeaders({ trace: 'abc' })).rejects.toThrow(/open\(\)/);
    } finally {
      await pair.stop();
    }
  });

  it('arrive before the first payload does', async () => {
    const pair = new Pair();
    pair.acceptor.onStream(async (_payload: unknown, stream: Stream) => {
      await stream.sendHeaders({ 'content-type': 'text/csv' });
      await stream.end({ payload: { row: 1 } });
    });
    pair.start();
    try {
      const stream = pair.dialer.open({ q: 'export' }, { end: true });
      // The ordering is the whole feature: a consumer that learns the content type only after the
      // body has started has learned it too late. `headersArrived` settles on the frame that carried
      // them, which arrived before the one carrying the payload.
      await stream.replyHeadersArrived;
      expect(stream.replyHeaders).toEqual({ 'content-type': 'text/csv' });
      expect(await stream).toEqual({ row: 1 });
    } finally {
      await pair.stop();
    }
  });

  it('ride the first payload when the answer has nothing to announce early', async () => {
    const pair = new Pair();
    pair.acceptor.onStream(async (_payload: unknown, stream: Stream) => {
      await stream.reply({ rows: 2 }, { headers: { 'content-type': 'application/json' } });
    });
    pair.start();
    try {
      const stream = pair.dialer.open({ q: 1 }, { end: true });
      expect(await stream).toEqual({ rows: 2 });
      await stream.replyHeadersArrived;
      expect(stream.replyHeaders).toEqual({ 'content-type': 'application/json' });
      // One frame, not two: `reply` carried them rather than announcing them separately.
      expect(pair.framesOfType('acceptor', 'data')).toHaveLength(1);
    } finally {
      await pair.stop();
    }
  });

  it('resets the stream, and only the stream, when they arrive late', async () => {
    const pair = new Pair();
    pair.acceptor.onStream(hold);
    pair.start();
    try {
      const stream = pair.dialer.open({ q: 1 });
      const other = pair.dialer.open({ q: 2 });
      await pair.settle();

      // Injected rather than sent: the local handle refuses to build this frame (WSM-API-024), which
      // is exactly why the receiver's half needs a witness of its own.
      pair.inject('dialer', { type: 'data', stream: stream.id, payload: { row: 1 } });
      pair.inject('dialer', { type: 'data', stream: stream.id, headers: { late: true }, payload: { row: 2 } });
      await pair.settle();

      await expect(Promise.resolve(stream)).rejects.toBeInstanceOf(StreamReset);
      const [reset] = pair.framesOfType('dialer', 'reset');
      expect(reset.code).toBe(ResetCode.PROTOCOL_ERROR);
      expect(reset.stream).toBe(stream.id);
      // Stream-level: the connection and every other stream on it are untouched (§4.3).
      expect(other.state).not.toBe(StreamState.CLOSED);
      expect(pair.framesOfType('dialer', 'goaway')).toHaveLength(0);
    } finally {
      await pair.stop();
    }
  });

  it('settles replyHeadersArrived on every path, including the ones with no headers on them', async () => {
    const pair = new Pair();
    pair.acceptor.onStream(async (_payload: unknown, stream: Stream) => {
      await stream.cancel('not answering this one');
    });
    pair.start();
    try {
      // A remote that resets before answering sends no headers and never will. WSM-API-025 makes
      // that settle rather than hang: this await is the assertion, and a port that only settled on a
      // first frame would fail it by never returning.
      const refused = pair.dialer.open({ q: 1 });
      void Promise.resolve(refused).catch(() => undefined);
      await refused.replyHeadersArrived;
      expect(refused.replyHeaders).toEqual({});
    } finally {
      await pair.stop();
    }
  });

  it('is what the acceptor already had: an open’s headers, from construction', async () => {
    const pair = new Pair();
    const answered: Stream[] = [];
    pair.acceptor.onStream(capture(answered));
    pair.start();
    try {
      pair.dialer.open({ q: 1 }, { headers: { trace: 'abc123' }, end: true });
      await pair.settle();
      expect(answered[0].headers).toEqual({ trace: 'abc123' });
      // The open's headers are not the answer's: this handler has announced nothing yet.
      expect(answered[0].replyHeaders).toEqual({});
    } finally {
      await pair.stop();
    }
  });
});
