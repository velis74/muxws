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
