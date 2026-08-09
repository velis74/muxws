/**
 * The round-robin writer and the one-unsent-fragment rule (§4.2).
 *
 * A mirror of `muxws/writer_test.py`, test for test. The two suites assert the same thirteen things
 * because the two writers must make the same thirteen promises.
 */

import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { JsonCodec } from './codec';
import { encodedLength, splitFrame } from './fragment';
import type { Frame } from './frames';
import { Peer } from './peer';
import { memoryPair } from './transports/memory';
import { CONNECTION_LANE, StreamQueue, Writer } from './writer';

const codec = new JsonCodec();

function makeWriter(): Writer {
  return new Writer(codec, { maxFrameBytes: 256 });
}

function big(size = 4000): Record<string, unknown> {
  return { body: 'x'.repeat(size) };
}

/**
 * The two members `writer_test.py` reaches for through Python's underscore convention.
 *
 * `rotate` is one turn of the rotation by hand - the peer drives the writer through `nextFrame`, but
 * a test that wants to watch the *order* has to step it. `queues` is how the retirement test sees
 * that a spent lane left nothing behind.
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
   * WSM-INV-004, the reason all of this exists.
   *
   * A 200-byte update on one stream must not wait for a megabyte on another. Without the rotation it
   * would go out last; with it, it goes out on the second turn.
   */
  it('lets a small frame overtake a fragmented payload - WSM-INV-004', () => {
    const writer = makeWriter();
    writer.enqueue({ type: 'data', stream: 1, payload: big(40_000) });
    writer.enqueue({ type: 'data', stream: 3, payload: { progress: 42 } });

    const order = drain(writer, 40).map((frame) => frame.stream);
    expect(order.indexOf(3)).toBeLessThanOrEqual(2);
    // The big payload must still be fragmenting when the small one lands.
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

  it('makes connection-level frames take their turn', () => {
    const writer = makeWriter();
    writer.enqueue({ type: 'data', stream: 1, payload: big(20_000) });
    writer.enqueue({ type: 'ping', nonce: 'abc' });

    const order = drain(writer, 20).map((frame) => frame.type);
    expect(order.indexOf('ping')).toBeLessThanOrEqual(2);
  });

  /** WSM-FRG-019 spelled as a source check: the ordering decision is not made at enqueue time. */
  it('has no FIFO of frames anywhere in the send path - WSM-FRG-019', () => {
    const source = readFileSync(join(process.cwd(), 'ts', 'writer.ts'), 'utf8');
    // A single array of *frames* spanning streams is exactly what the rule forbids. The per-stream
    // `waiting` array is fine and necessary - contiguity (WSM-FRG-017) requires it.
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

  it('retires a spent lane - WSM-STM-001', () => {
    const writer = makeWriter();
    writer.enqueue({ type: 'data', stream: 1, payload: { n: 1 } });
    drain(writer);

    expect(writer.depthOf(1)).toBe(0);
    expect(internals(writer).queues.has(1)).toBe(false);
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
  it('holds one prepared fragment - the rule as a unit, without a writer around it', () => {
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
   * The gap this closes: ts/writer.ts landed with 13 passing tests while ts/peer.ts still sent
   * through a FIFO of its own, so the rotation - the entire point of M5a - was not on the send path
   * at all. Testing the writer in isolation cannot notice that.
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
  it('yields per frame, so the rotation has something to rotate between', async () => {
    // The round-robin is only worth having if another stream can get a frame into the writer while a
    // large payload is going out. `await` on a promise that is already resolved drains the microtask
    // queue but never lets a timer run, and a memory socket resolves immediately - so a producer
    // driven by setTimeout, which is what a real ticking backend looks like, gets no turn at all
    // until the export is finished. The Python port had exactly this defect and was measured at
    // seven fragments with zero other frames between them.
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
