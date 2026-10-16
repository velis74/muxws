/**
 * The round-robin writer and the one-unsent-fragment rule (§4.2).
 *
 * Mirrors `muxws/writer_test.py` test for test; both suites make the same assertions.
 */

import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { JsonCodec } from './codec';
import { encodedLength, splitFrame } from './fragment';
import type { Frame } from './frames';
import { Peer } from './peer';
import { memoryPair } from './transports/memory';
import { CONNECTION_LANE, LaneEncodingError, StreamQueue, Writer } from './writer';

const codec = new JsonCodec();

function makeWriter(): Writer {
  return new Writer(codec, { maxFrameBytes: 256 });
}

function big(size = 4000): Record<string, unknown> {
  return { body: 'x'.repeat(size) };
}

/**
 * The two private members `writer_test.py` accesses through Python's underscore convention.
 *
 * `rotate` performs one step of the rotation. The peer drives the writer through `nextFrame`; tests
 * that check the order step it directly. `queues` lets the retirement test check that a finished
 * lane is removed.
 */
interface WriterInternals {
  rotate(): Frame | null;
  queues: Map<number, StreamQueue>;
}

function internals(writer: Writer): WriterInternals {
  return writer as unknown as WriterInternals;
}

/** Take frames until the writer has nothing left, recording the order they came out in. */
function drain(writer: Writer, limit = 500): Frame[] {
  const out: Frame[] = [];
  for (let taken = 0; taken < limit; taken += 1) {
    const frame = internals(writer).rotate();
    if (frame === null) break;
    out.push(frame);
    writer.advance(frame.stream ?? CONNECTION_LANE);
  }
  return out;
}

/** One turn of the event loop - the closest equivalent of `await asyncio.sleep(0)`. */
function yieldToEventLoop(): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, 0);
  });
}

describe('the writer', () => {
  it('holds at most one unsent fragment per stream - WSM-FRG-018', () => {
    const writer = makeWriter();
    writer.enqueue({ type: 'data', stream: 1, payload: big() });

    const depths: number[] = [];
    for (let turn = 0; turn < 60; turn += 1) {
      const frame = internals(writer).rotate();
      if (frame === null) break;
      depths.push(writer.preparedDepthOf(1));
      writer.advance(1);
    }

    expect(depths.length).toBeGreaterThan(5); // the payload must actually fragment for this to mean anything
    expect(Math.max(...depths)).toBeLessThanOrEqual(1);
  });

  it('selects across streams by round-robin, not FIFO - WSM-FRG-019', () => {
    const writer = makeWriter();
    [1, 3, 5].forEach((streamId) => {
      [0, 1, 2].forEach((index) => {
        writer.enqueue({ type: 'data', stream: streamId, payload: { n: index } });
      });
    });

    const order = drain(writer).map((frame) => frame.stream);
    expect(order.slice(0, 9)).toEqual([1, 3, 5, 1, 3, 5, 1, 3, 5]);
  });

  /**
   * WSM-INV-004, the invariant the round-robin writer provides.
   *
   * A 200-byte update on one stream must not wait for a megabyte on another. Without the rotation it
   * is sent last; with it, it is sent on the second turn.
   */
  it('sends a small frame before a fragmented payload completes - WSM-INV-004', () => {
    const writer = makeWriter();
    writer.enqueue({ type: 'data', stream: 1, payload: big(40_000) });
    writer.enqueue({ type: 'data', stream: 3, payload: { progress: 42 } });

    const order = drain(writer, 40).map((frame) => frame.stream);
    expect(order.indexOf(3)).toBeLessThanOrEqual(2);
    // Fragments of the large payload remain after the small frame is sent.
    expect(order.filter((streamId) => streamId === 1).length).toBeGreaterThan(10);
  });

  it('keeps the fragments of one payload contiguous on their stream - WSM-FRG-017', () => {
    const writer = makeWriter();
    writer.enqueue({ type: 'data', stream: 1, payload: big() });
    writer.enqueue({ type: 'data', stream: 3, payload: big() });

    const frames = drain(writer);
    [1, 3].forEach((streamId) => {
      const mine = frames.filter((frame) => frame.stream === streamId);
      expect(mine.map((frame) => frame.more === true)).toEqual([...mine.slice(0, -1).map(() => true), false]);
      const rejoined = mine.map((frame) => String(frame.fragment)).join('');
      expect(codec.decodePayload(rejoined)).toEqual(big());
    });
  });

  it('puts nothing over the cap on the wire - WSM-FRG-001/031', () => {
    const writer = makeWriter();
    writer.enqueue({ type: 'data', stream: 1, payload: big(20_000) });
    drain(writer).forEach((frame) => {
      expect(encodedLength(codec.encode(frame))).toBeLessThanOrEqual(256);
    });
  });

  it('cuts where the splitter cuts - one boundary computation, used two ways', () => {
    const writer = makeWriter();
    const source: Frame = { type: 'data', stream: 1, payload: big(3000), end: true };
    writer.enqueue(source);

    expect(drain(writer).map((frame) => frame.fragment)).toEqual(
      splitFrame(source, 256, codec).map((frame) => frame.fragment),
    );
  });

  it('includes connection-level frames in the rotation', () => {
    const writer = makeWriter();
    writer.enqueue({ type: 'data', stream: 1, payload: big(20_000) });
    writer.enqueue({ type: 'ping', nonce: 'abc' });

    const order = drain(writer, 20).map((frame) => frame.type);
    expect(order.indexOf('ping')).toBeLessThanOrEqual(2);
  });

  /** WSM-FRG-019 as a source check: the ordering decision is not made at enqueue time. */
  it('has no FIFO of frames anywhere in the send path - WSM-FRG-019', () => {
    const source = readFileSync(join(process.cwd(), 'ts', 'writer.ts'), 'utf8');
    // The rule forbids a single array of frames spanning streams. The per-stream `waiting` array is
    // allowed and required for contiguity (WSM-FRG-017).
    const marker = 'Frame[]';
    expect(source).toContain(marker);
    expect(source.split(marker).length - 1).toBe(1);
  });

  it('leaves nothing for a next socket after discardAll - WSM-RCN-042/WSM-INV-010', () => {
    const writer = makeWriter();
    writer.enqueue({ type: 'data', stream: 1, payload: big() });
    writer.enqueue({ type: 'data', stream: 3, payload: { n: 1 } });
    internals(writer).rotate();

    writer.discardAll();
    expect(writer.depth).toBe(0);
    expect(writer.lanes).toBe(0);
    expect(internals(writer).rotate()).toBeNull();
  });

  it('leaves the other streams alone when one is discarded', () => {
    const writer = makeWriter();
    writer.enqueue({ type: 'data', stream: 1, payload: big() });
    writer.enqueue({ type: 'data', stream: 3, payload: { n: 1 } });

    writer.discard(1);
    const order = drain(writer).map((frame) => frame.stream);
    expect(new Set(order)).toEqual(new Set([3]));
  });

  it('drops the remainder of a started payload when a stream is discarded - WSM-FRM-014', () => {
    const writer = makeWriter();
    writer.enqueue({ type: 'data', stream: 1, payload: big() });
    writer.enqueue({ type: 'data', stream: 1, payload: { n: 2 }, end: true });
    const first = internals(writer).rotate();
    expect(first?.more).toBe(true);
    writer.advance(1);

    writer.discard(1);
    writer.enqueue({ type: 'reset', stream: 1, code: 1 });
    writer.advance(1);
    expect(drain(writer).map((frame) => frame.type)).toEqual(['reset']);
  });

  it('keeps an open none of which was sent when a stream is discarded - WSM-STM-003', () => {
    const writer = makeWriter();
    writer.enqueue({ type: 'open', stream: 1, payload: { q: 1 } });
    writer.enqueue({ type: 'data', stream: 1, payload: big() });

    writer.discard(1);
    writer.enqueue({ type: 'reset', stream: 1, code: 1 });
    expect(drain(writer).map((frame) => frame.type)).toEqual(['open', 'reset']);
  });

  it('retires a spent lane - WSM-STM-001', () => {
    const writer = makeWriter();
    writer.enqueue({ type: 'data', stream: 1, payload: { n: 1 } });
    drain(writer);

    expect(writer.depthOf(1)).toBe(0);
    expect(internals(writer).queues.has(1)).toBe(false);
  });

  it('discards a lane whose next frame fails to encode when advancing, and throws LaneEncodingError', () => {
    const writer = makeWriter();
    writer.enqueue({ type: 'data', stream: 1, payload: { n: 1 } });
    // Bytes are not a payload type under JSON; the codec refuses them.
    writer.enqueue({ type: 'data', stream: 1, payload: { blob: new Uint8Array([0, 255]).buffer } });
    writer.enqueue({ type: 'data', stream: 1, payload: { n: 3 } });
    writer.enqueue({ type: 'data', stream: 5, payload: { n: 5 } });

    const first = internals(writer).rotate();
    expect(first?.stream).toBe(1);
    let thrown: unknown = null;
    try {
      writer.advance(1);
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeInstanceOf(LaneEncodingError);
    expect((thrown as LaneEncodingError).lane).toBe(1);
    expect(writer.depthOf(1)).toBe(0);
    expect(internals(writer).queues.has(1)).toBe(false);
    expect(drain(writer).map((frame) => frame.stream)).toEqual([5]);
  });

  it('waits when there is nothing to send, and wakes on the next enqueue', async () => {
    const writer = makeWriter();
    let settled = false;
    const waiting = writer.nextFrame().then((frame) => {
      settled = true;
      return frame;
    });

    await yieldToEventLoop();
    expect(settled).toBe(false);

    writer.enqueue({ type: 'data', stream: 1, payload: { n: 1 } });
    const frame = await waiting;
    expect(frame).not.toBeNull();
    expect(frame?.stream).toBe(1);
  });
});

describe('a stream queue', () => {
  it('holds one prepared fragment, tested without a writer', () => {
    const queue = new StreamQueue(1);
    queue.put({ type: 'data', stream: 1, payload: big() });

    queue.prepare(256, codec);
    expect(queue.preparedDepth).toBe(1);
    queue.prepare(256, codec);
    expect(queue.preparedDepth).toBe(1); // prepare() must be idempotent, not cumulative

    expect(queue.take()).not.toBeNull();
    expect(queue.preparedDepth).toBe(0);
    expect(queue.hasWork).toBe(true); // the tail is still there even with nothing prepared
  });
});

describe('the peer actually uses the writer', () => {
  /**
   * The rotation must be on the peer's send path, not only implemented in `ts/writer.ts`: a peer
   * that queued its own frames FIFO would pass every test above and still serialise the streams.
   * Only an end-to-end run shows which order reaches the socket.
   */
  it('interleaves a small frame with a fragmenting one, end to end - WSM-INV-004', async () => {
    const codec = new JsonCodec();
    const [left, right] = memoryPair();
    const dialer = new Peer(left, { codec, isDialer: true, maxFrameBytes: 512 });
    const acceptor = new Peer(right, { codec, isDialer: false, maxFrameBytes: 512 });
    acceptor.onStream(async (_payload: unknown, stream) => {
      await stream.closed;
    });

    const served = [dialer.serve(), acceptor.serve()];
    served.forEach((task) => void task.catch(() => undefined));

    try {
      // A megabyte-ish payload on one stream, then a 200-byte update on another.
      dialer.open({ body: 'x'.repeat(40_000) });
      dialer.open({ progress: 42 });

      for (let turn = 0; turn < 60; turn += 1) {
        await new Promise((resolve) => setTimeout(resolve, 0));
      }

      const streams = left.sent.map((message) => codec.decode(message).stream);
      const small = streams.indexOf(3);
      expect(small, 'the small frame never went out at all').toBeGreaterThan(-1);
      expect(small, `the small frame waited ${small} frames behind the big one`).toBeLessThan(4);
      expect(streams.filter((id) => id === 1).length, 'the big payload must still be fragmenting').toBeGreaterThan(10);
    } finally {
      left.drop();
      for (let turn = 0; turn < 12; turn += 1) await new Promise((resolve) => setTimeout(resolve, 0));
      await Promise.allSettled(served);
    }
  });
});

describe('WSM-INV-004 on a fast socket', () => {
  it('yields to the event loop per frame, so other streams can enqueue during a large payload', async () => {
    // The round-robin requires that another stream can enqueue a frame while a large payload is
    // being sent. `await` on an already resolved promise drains the microtask queue but does not run
    // timers, and a memory socket resolves immediately. A send loop that only awaits the socket would
    // therefore give a producer driven by setTimeout (as in a periodic backend) no turn until the
    // export is finished.
    const [left, right] = memoryPair();
    const dialer = new Peer(left, { codec, isDialer: true });
    const acceptor = new Peer(right, { codec, isDialer: false });
    acceptor.onStream(() => undefined);
    const order: (number | null | undefined)[] = [];
    dialer.onFrame((direction, frame) => {
      if (direction === 'tx') order.push(frame.stream);
    });
    const served = [dialer.serve(), acceptor.serve()];
    served.forEach((task) => void task.catch(() => undefined));

    let ticking = true;
    const tick = (): void => {
      if (!ticking) return;
      try {
        dialer.open({ tick: 1 }, { end: true });
      } catch {
        return;
      }
      setTimeout(tick, 0);
    };
    setTimeout(tick, 0);

    const exported = dialer.open({ export: 'x'.repeat(400_000) }, { end: true });
    await new Promise((resolve) => setTimeout(resolve, 300));
    ticking = false;

    const positions = order.map((stream, index) => (stream === exported.id ? index : -1)).filter((i) => i >= 0);
    expect(positions.length, 'the export must actually fragment for this to mean anything').toBeGreaterThan(3);
    const between = order
      .slice(positions[0], positions[positions.length - 1])
      .filter((stream) => stream !== exported.id);
    expect(
      between.length,
      `${positions.length} export fragments reached the wire with nothing else between them: the export ` +
        'monopolised the socket and WSM-INV-004 does not hold on this link',
    ).toBeGreaterThan(0);

    await dialer.close();
    await acceptor.close();
  });
});
