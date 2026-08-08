/**
 * The send path: per-stream queues and a round-robin writer (§4.2).
 *
 * A line-for-line mirror of `muxws/writer.py`. This module is the whole of WSM-FRG-017, WSM-FRG-018
 * and WSM-FRG-019, and it is one object on purpose. The three rules are really one requirement seen
 * from three sides:
 *
 *   a stream holds at most **one** unsent fragment (WSM-FRG-018),
 *   fragments of one payload stay contiguous **on their own stream** (WSM-FRG-017),
 *   and the next frame to go out is chosen by **round-robin across streams** (WSM-FRG-019).
 *
 * Together they are what stops a 1 MB export adding a full second of latency to a 200-byte progress
 * update on another stream (WSM-INV-004). A single FIFO of frames anywhere in this path breaks all
 * three at once: the ordering decision is then made at enqueue time, and interleaving stops being
 * possible no matter what the loop does afterwards.
 */

import type { Codec } from './codec';
import { MAX_FRAME_BYTES, iterFragments } from './fragment';
import type { Frame } from './frames';

/**
 * Connection-level frames - `ping`, `pong`, `goaway` - have no stream of their own. They get one lane
 * keyed here, so they take their turn in the rotation rather than jumping it or waiting on it.
 */
export const CONNECTION_LANE = 0;

/**
 * A frame on one lane could not be encoded.
 *
 * Named rather than allowed to propagate: the encode now happens inside the writer, so a codec that
 * refuses a payload - an `ArrayBuffer` under JSON, say - would otherwise take the write loop down
 * with it, and a peer whose writer is dead while it still reports itself open is the worst possible
 * state. The lane is carried so the caller can fail exactly that stream and leave the connection
 * working.
 */
export class LaneEncodingError extends Error {
  readonly lane: number;

  /** The codec's own failure, kept whole so the peer can report it to the stream it belongs to. */
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
 * An `asyncio.Event` in the two lines TypeScript needs for it.
 *
 * Latching, exactly as Python's is: `set()` stays set until `clear()`, so an `enqueue` that lands
 * between two turns of the write loop cannot be missed.
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
 * One stream's outbound work: whole frames waiting, and **at most one** prepared fragment.
 *
 * The per-stream order is a queue and has to be - fragments of one payload are contiguous on their
 * stream (WSM-FRG-017). What must never be a queue is the choice *between* streams.
 */
export class StreamQueue {
  readonly streamId: number;

  private waiting: Frame[] = [];

  /** The one fragment already sliced and not yet handed to the socket. WSM-FRG-018 in a field. */
  private prepared: Frame | null = null;

  /** The lazy remainder of the payload in flight, if this stream is mid-fragmentation. */
  private fragments: Iterator<Frame> | null = null;

  constructor(streamId: number) {
    this.streamId = streamId;
  }

  /** How many frames are queued but unsent. Never more than one **prepared** (WSM-FRG-018). */
  get depth(): number {
    return this.waiting.length + (this.prepared === null ? 0 : 1);
  }

  get hasWork(): boolean {
    return this.prepared !== null || this.waiting.length > 0 || this.fragments !== null;
  }

  /** Instrumentation for the rule: this must never exceed 1. */
  get preparedDepth(): number {
    return this.prepared === null ? 0 : 1;
  }

  put(frame: Frame): void {
    this.waiting.push(frame);
  }

  /**
   * Slice the next fragment - and only the next one.
   *
   * Called *after* the previous fragment has reached the socket, never before. Slicing ahead is the
   * bug WSM-FRG-018 forbids: it is not wrong on the wire, it is wrong in the queue, because it
   * decides an order the writer has not been asked to commit to yet.
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

  /** Hand over the prepared fragment. The next one is not sliced until `prepare` is called. */
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

/** Chooses the next frame across streams by round-robin, and never by arrival order. */
export class Writer {
  private readonly codec: Codec;

  private readonly cap: number;

  private readonly queues = new Map<number, StreamQueue>();

  /**
   * Where the rotation resumes. Lane ids taken from the front and pushed to the back, never an index
   * into a list: streams come and go, and an index would silently start favouring whoever happened
   * to land in the vacated slot.
   */
  private readonly order: number[] = [];

  private readonly wake = new Gate();

  private stopped = false;

  constructor(codec: Codec, options: { maxFrameBytes?: number } = {}) {
    this.codec = codec;
    this.cap = options.maxFrameBytes ?? MAX_FRAME_BYTES;
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

  /** Synchronous by contract: `open()` must not suspend between allocating and enqueuing. */
  enqueue(frame: Frame): void {
    const lane = frame.stream ?? CONNECTION_LANE;
    let queue = this.queues.get(lane);
    if (queue === undefined) {
      queue = new StreamQueue(lane);
      this.queues.set(lane, queue);
      this.order.push(lane);
    }
    queue.put(frame);
    this.wake.set();
  }

  /**
   * The next frame to put on the wire, waiting if there is nothing to send.
   *
   * Resolves `null` when `stop()` retired the writer while we were waiting - the caller checks for
   * that rather than being handed a frame that no longer means anything.
   */
  async nextFrame(): Promise<Frame | null> {
    for (;;) {
      if (this.stopped) return null;
      const frame = this.rotate();
      if (frame !== null) return frame;
      this.wake.clear();
      if (![...this.queues.values()].some((queue) => queue.hasWork)) await this.wake.wait();
      if (this.stopped) return null;
    }
  }

  /**
   * One full turn of the rotation, at most.
   *
   * Every lane gets asked once before any lane is asked twice. That is the entire mechanism: a
   * stream mid-way through a megabyte holds the wire for exactly one fragment at a time.
   *
   * @internal - the peer drives the writer through `nextFrame`; only the tests turn it by hand.
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

  /** Ask one lane for its next frame, converting a codec refusal into a lane-scoped failure. */
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

  /** Forget a lane with nothing left. Nothing is retained per closed stream (WSM-STM-001). */
  private retire(lane: number): void {
    if (lane === CONNECTION_LANE) return;
    this.queues.delete(lane);
    const at = this.order.indexOf(lane);
    if (at >= 0) this.order.splice(at, 1);
  }

  /** Slice that stream's next fragment, now that its previous one has reached the socket. */
  advance(streamId: number | null | undefined): void {
    const queue = this.queues.get(streamId ?? CONNECTION_LANE);
    if (queue === undefined) return;
    queue.prepare(this.cap, this.codec);
    if (queue.hasWork) this.wake.set();
  }

  /** Drop one stream's queued work - it was reset, and none of it means anything now. */
  discard(streamId: number): void {
    const queue = this.queues.get(streamId);
    if (queue === undefined) return;
    this.queues.delete(streamId);
    queue.discard();
    const at = this.order.indexOf(streamId);
    if (at >= 0) this.order.splice(at, 1);
  }

  /** Retire the writer. `nextFrame` resolves `null` from here on, once and for good. */
  stop(): void {
    this.stopped = true;
    this.wake.set();
  }

  /** Socket death. M5b calls exactly this, and nothing is held for a next socket (WSM-RCN-042). */
  discardAll(): void {
    this.queues.forEach((queue) => {
      queue.discard();
    });
    this.queues.clear();
    this.order.length = 0;
    this.wake.set();
  }
}
