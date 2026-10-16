/**
 * The send path: per-stream queues and a round-robin writer (§4.2).
 *
 * Mirrors `muxws/writer.py` line for line. This module implements WSM-FRG-017, WSM-FRG-018 and
 * WSM-FRG-019 together:
 *
 *   a stream holds at most one unsent fragment (WSM-FRG-018),
 *   fragments of one payload are contiguous on their own stream (WSM-FRG-017),
 *   the next frame to send is chosen round-robin across streams (WSM-FRG-019).
 *
 * As a result, a small frame on one stream is not delayed behind all fragments of a large payload on
 * another stream (WSM-INV-004); it waits for at most one fragment per other active stream. A single
 * FIFO of frames in this path would fix the order at enqueue time and prevent interleaving.
 */

import type { Codec } from './codec';
import { MAX_FRAME_BYTES, iterFragments } from './fragment';
import type { Frame } from './frames';

/**
 * The lane for connection-level frames (`ping`, `pong`, `goaway`), which have no stream. It takes
 * turns in the rotation like a stream lane.
 */
export const CONNECTION_LANE = 0;

/**
 * A frame on one lane could not be encoded.
 *
 * Encoding happens inside the writer. A codec error (for example an `ArrayBuffer` under JSON) is
 * wrapped in this class, with the lane, so the peer fails only that stream and the write loop keeps
 * running; an unwrapped error would stop the write loop while the peer still reports itself open.
 */
export class LaneEncodingError extends Error {
  readonly lane: number;

  /** The codec's original error, which the peer reports to the affected stream. */
  readonly cause: unknown;

  constructor(lane: number, cause: unknown) {
    super(`could not encode a frame on lane ${lane}: ${describeCause(cause)}`);
    this.name = 'LaneEncodingError';
    this.lane = lane;
    this.cause = cause;
  }
}

function describeCause(cause: unknown): string {
  return cause instanceof Error ? cause.message : String(cause);
}

/**
 * The TypeScript equivalent of `asyncio.Event`.
 *
 * Latching, as in Python: after `set()`, `wait()` resolves immediately until `clear()`, so an
 * `enqueue` between two iterations of the write loop is not missed.
 */
class Gate {
  private signalled = false;

  private waiters: (() => void)[] = [];

  set(): void {
    this.signalled = true;
    const pending = this.waiters;
    this.waiters = [];
    pending.forEach((resume) => {
      resume();
    });
  }

  clear(): void {
    this.signalled = false;
  }

  wait(): Promise<void> {
    if (this.signalled) return Promise.resolve();
    return new Promise<void>((resume) => {
      this.waiters.push(resume);
    });
  }
}

/**
 * One stream's outbound frames: whole frames waiting, and at most one prepared fragment.
 *
 * Frames within a stream are sent in FIFO order, so fragments of one payload are contiguous on their
 * stream (WSM-FRG-017). The choice between streams is made by `Writer`, round-robin.
 */
export class StreamQueue {
  readonly streamId: number;

  private waiting: Frame[] = [];

  /** The single fragment already sliced and not yet passed to the socket (WSM-FRG-018). */
  private prepared: Frame | null = null;

  /** The lazy remainder of the payload in flight, if this stream is mid-fragmentation. */
  private fragments: Iterator<Frame> | null = null;

  constructor(streamId: number) {
    this.streamId = streamId;
  }

  /** The number of queued, unsent frames, of which at most one is prepared (WSM-FRG-018). */
  get depth(): number {
    return this.waiting.length + (this.prepared === null ? 0 : 1);
  }

  get hasWork(): boolean {
    return this.prepared !== null || this.waiting.length > 0 || this.fragments !== null;
  }

  /** 0 or 1: whether a prepared fragment is held (WSM-FRG-018). Read by tests. */
  get preparedDepth(): number {
    return this.prepared === null ? 0 : 1;
  }

  put(frame: Frame): void {
    this.waiting.push(frame);
  }

  /**
   * Slice the next fragment only. No-op while a prepared fragment is held.
   *
   * Called after the previous fragment has been passed to the socket. Slicing further fragments
   * ahead would violate WSM-FRG-018 (at most one unsent fragment per stream).
   */
  prepare(cap: number, codec: Codec): void {
    if (this.prepared !== null) return;

    if (this.fragments !== null) {
      const continued = this.fragments.next();
      if (continued.done !== true) {
        this.prepared = continued.value;
        return;
      }
      this.fragments = null;
    }

    const next = this.waiting.shift();
    if (next === undefined) return;
    this.fragments = iterFragments(next, cap, codec);
    const started = this.fragments.next();
    this.prepared = started.done === true ? null : started.value;
  }

  /** Return and clear the prepared fragment. The next one is sliced by the next `prepare` call. */
  take(): Frame | null {
    const frame = this.prepared;
    this.prepared = null;
    return frame;
  }

  discard(): void {
    this.waiting = [];
    this.prepared = null;
    this.fragments = null;
  }
}

/** Chooses the next frame across streams round-robin, not by arrival order. */
export class Writer {
  private readonly codec: Codec;

  private readonly cap: number;

  private readonly queues = new Map<number, StreamQueue>();

  /**
   * The rotation order: lane ids are taken from the front and pushed to the back. A position index
   * into a list would skip or repeat lanes when lanes are added or removed.
   */
  private readonly order: number[] = [];

  private readonly wake = new Gate();

  /** Set while the write loop has sent every frame passed to `enqueue`; cleared by `enqueue`. */
  private readonly idle = new Gate();

  private stopped = false;

  constructor(codec: Codec, options: { maxFrameBytes?: number } = {}) {
    this.codec = codec;
    this.cap = options.maxFrameBytes ?? MAX_FRAME_BYTES;
    this.idle.set();
  }

  /** Every unsent frame across every lane. */
  get depth(): number {
    return [...this.queues.values()].reduce((total, queue) => total + queue.depth, 0);
  }

  get lanes(): number {
    return this.queues.size;
  }

  depthOf(streamId: number): number {
    return this.queues.get(streamId)?.depth ?? 0;
  }

  preparedDepthOf(streamId: number): number {
    return this.queues.get(streamId)?.preparedDepth ?? 0;
  }

  /** Synchronous, so `open()` does not suspend between allocating a stream id and enqueuing. */
  enqueue(frame: Frame): void {
    const lane = frame.stream ?? CONNECTION_LANE;
    let queue = this.queues.get(lane);
    if (queue === undefined) {
      queue = new StreamQueue(lane);
      this.queues.set(lane, queue);
      this.order.push(lane);
    }
    queue.put(frame);
    this.idle.clear();
    this.wake.set();
  }

  /**
   * The next frame to put on the wire, waiting if there is nothing to send.
   *
   * Resolves `null` once `stop()` has been called, including while waiting.
   */
  async nextFrame(): Promise<Frame | null> {
    for (;;) {
      if (this.stopped) return null;
      const frame = this.rotate();
      if (frame !== null) return frame;
      this.wake.clear();
      if (![...this.queues.values()].some((queue) => queue.hasWork)) {
        // The caller asks for the next frame only after the previous one was passed to the socket,
        // so no work here means every enqueued frame has been sent.
        this.idle.set();
        await this.wake.wait();
      }
      if (this.stopped) return null;
    }
  }

  /**
   * Resolves once every enqueued frame, including every fragment of a fragmented one, has been sent.
   *
   * Also resolves after `stop()` and `discardAll()`, which leave nothing to send.
   */
  waitIdle(): Promise<void> {
    return this.idle.wait();
  }

  /**
   * One full turn of the rotation, at most.
   *
   * Every lane is asked once before any lane is asked twice, so a stream sending a large payload
   * sends one fragment per turn.
   *
   * @internal - the peer uses `nextFrame`.
   */
  private rotate(): Frame | null {
    const turns = this.order.length;
    for (let turn = 0; turn < turns; turn += 1) {
      const lane = this.order.shift() as number;
      this.order.push(lane);
      const queue = this.queues.get(lane);
      if (queue !== undefined) {
        const frame = this.turnOne(lane, queue);
        if (frame !== null) return frame;
      }
    }
    return null;
  }

  /** Ask one lane for its next frame; a codec error discards the lane and throws `LaneEncodingError`. */
  private turnOne(lane: number, queue: StreamQueue): Frame | null {
    try {
      queue.prepare(this.cap, this.codec);
    } catch (failure) {
      if (failure instanceof LaneEncodingError) throw failure;
      queue.discard();
      this.retire(lane);
      throw new LaneEncodingError(lane, failure);
    }

    const frame = queue.take();
    if (frame !== null) return frame;
    if (!queue.hasWork) this.retire(lane);
    return null;
  }

  /** Remove an empty stream lane. No state is retained per closed stream (WSM-STM-001). */
  private retire(lane: number): void {
    if (lane === CONNECTION_LANE) return;
    this.queues.delete(lane);
    const at = this.order.indexOf(lane);
    if (at >= 0) this.order.splice(at, 1);
  }

  /** Slice the stream's next fragment after its previous one has been passed to the socket. */
  advance(streamId: number | null | undefined): void {
    const queue = this.queues.get(streamId ?? CONNECTION_LANE);
    if (queue === undefined) return;
    queue.prepare(this.cap, this.codec);
    if (queue.hasWork) this.wake.set();
  }

  /** Drop all queued frames of one stream. Called when the stream is reset. */
  discard(streamId: number): void {
    const queue = this.queues.get(streamId);
    if (queue === undefined) return;
    this.queues.delete(streamId);
    queue.discard();
    const at = this.order.indexOf(streamId);
    if (at >= 0) this.order.splice(at, 1);
  }

  /** Stop the writer. Every subsequent and pending `nextFrame` resolves `null`. */
  stop(): void {
    this.stopped = true;
    this.idle.set();
    this.wake.set();
  }

  /** Drop every lane on socket loss; nothing is kept for a later socket (WSM-RCN-042). */
  discardAll(): void {
    this.queues.forEach((queue) => {
      queue.discard();
    });
    this.queues.clear();
    this.order.length = 0;
    this.idle.set();
    this.wake.set();
  }
}
