/**
 * Replays `conformance/sequences/*.json` against a live peer pair (WSM-TST-002).
 *
 * The twin of `muxws/sequences_test.py`, reading the same files. The invalid corpus injects frames no
 * correct implementation would send and asserts what comes back; this one is the opposite - **both**
 * peers are real, nothing is injected, and the fixture is a script of ordinary API calls with
 * assertions interleaved. What it proves is that the two ports agree about *sequences* - which frame
 * goes out when, which stream survives which event - and not merely about how one frame is spelled.
 *
 * Two properties of the schema do the work, and both exist to keep one corpus honest in two
 * languages:
 *
 * - **Streams are named by `stream_ref`, an ordinal, never by a raw id.** Ordinal 1 is the first
 *   stream the script opens. The runner resolves it to whatever id that peer actually allocated, so
 *   the same fixture replays unchanged when the roles are swapped; a raw id would bake one side's
 *   parity into the corpus. `last_stream_ref` is that resolution applied to `goaway.last_stream`,
 *   which carries the *other* peer's parity (WSM-CON-020).
 * - **`expect_frame` is a subset match, not equality.** The listed keys must hold and everything else
 *   is ignored, so a later revision that adds an optional field does not invalidate the corpus -
 *   which is what WSM-FRM-001 asks of a receiver, applied to the test suite.
 *
 * If one runner needs a field the other does not read, the fixture is wrong.
 */

import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

import { JsonCodec } from './codec';
import { ConnectionLost, RemoteError, ResetCode, StreamRefused, StreamReset, StreamTimeout } from './errors';
import type { Frame } from './frames';
import { Peer } from './peer';
import type { Stream } from './stream';
import { MemorySocket, memoryPair } from './transports/memory';

// --------------------------------------------------------------------------- the corpus

/** vitest runs from the repository root, which is where `conformance/` sits. */
const SEQUENCES_DIR = join(process.cwd(), 'conformance', 'sequences');

const FIXTURE_FILES = readdirSync(SEQUENCES_DIR)
  .filter((name) => name.endsWith('.json'))
  .sort();

/**
 * Both runners carry this number and both assert it. M6 raises it as it completes the corpus; until
 * then a fixture that stopped being collected would be a suite that passes by testing nothing.
 */
const EXPECTED_FIXTURES = 1;

/** Wall-clock ceiling on any one waiting step, so a stalled fixture fails by name rather than hangs. */
const STEP_TIMEOUT_MS = 2000;

/** How many turns of the event loop a bounded poll is given before it gives up. */
const POLL_TURNS = 400;

type Who = 'dialer' | 'acceptor';

type Step = Record<string, any>;

interface Fixture {
  name: string;
  description?: string;
  max_frame_bytes?: number;
  steps: Step[];
}

/**
 * `expect_error` names a class rather than a code, because the class is the part WSM-ERR-004 fixes
 * across the two languages. A predicate per name rather than a map of constructors: the subclasses
 * do not share one construct signature, and `instanceof` is the whole of what this needs.
 */
const ERROR_CLASSES: Record<string, (error: unknown) => boolean> = {
  ConnectionLost: (error) => error instanceof ConnectionLost,
  RemoteError: (error) => error instanceof RemoteError,
  StreamRefused: (error) => error instanceof StreamRefused,
  StreamReset: (error) => error instanceof StreamReset,
  StreamTimeout: (error) => error instanceof StreamTimeout,
};

/** Distinguishes "the call did not throw" from "the call threw undefined". */
const NOTHING_THROWN: unique symbol = Symbol('NOTHING_THROWN');

/** The marker a step deadline rejects with; never an `Error`, so it cannot be confused with one. */
const STEP_EXPIRED: unique symbol = Symbol('STEP_EXPIRED');

/** One turn of the event loop; a macrotask, so the microtask queue drains with it. */
function turn(): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, 0);
  });
}

/** Python reaches for `asyncio.wait_for`; this is the same guard with the same purpose. */
async function withTimeout<T>(work: Promise<T>, what: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(STEP_EXPIRED), STEP_TIMEOUT_MS);
  });
  try {
    return await Promise.race([work, deadline]);
  } catch (error) {
    if (error === STEP_EXPIRED) throw new Error(`${what} did not settle within ${STEP_TIMEOUT_MS}ms`);
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

// --------------------------------------------------------------------------- the runner

/** One fixture, one live pair, one pass over `steps`. */
class Replay {
  private readonly codec = new JsonCodec();

  private readonly sockets: Record<Who, MemorySocket>;

  private readonly peers: Record<Who, Peer>;

  /** Ordinal -> the id that stream actually got. Filled by every step that opens one. */
  private readonly ordinals: number[] = [];

  /** Both peers' view of every stream, by id: the opener's handle and the receiver's. */
  private readonly byId: Record<Who, Map<number, Stream>> = { dialer: new Map(), acceptor: new Map() };

  /** `as` labels, for `expect_result` / `expect_error`. */
  private readonly refs = new Map<string, Stream>();

  /** In-flight `close()` calls, awaited by an `await_close` step. */
  private readonly closing = new Map<Who, Promise<void>>();

  /** How far `expect_frame` has consumed each peer's wire; frames match forwards, in order. */
  private readonly cursor: Record<Who, number> = { dialer: 0, acceptor: 0 };

  private served: Promise<void>[] = [];

  constructor(private readonly fixture: Fixture) {
    const [dialerSocket, acceptorSocket] = memoryPair();
    this.sockets = { dialer: dialerSocket, acceptor: acceptorSocket };
    // `max_frame_bytes` is an instruction to the runner, never a wire value (WSM-TST-002/WSM-FRG-005).
    const maxFrameBytes = fixture.max_frame_bytes;
    this.peers = {
      dialer: new Peer(dialerSocket, { codec: this.codec, isDialer: true, maxFrameBytes }),
      acceptor: new Peer(acceptorSocket, { codec: this.codec, isDialer: false, maxFrameBytes }),
    };
  }

  // ------------------------------------------------------------------ lifecycle

  start(): void {
    (['dialer', 'acceptor'] as Who[]).forEach((who) => {
      this.peers[who].onStream(async (_payload: unknown, stream: Stream) => {
        // Record the stream, then hold it open. WSM-STM-035 ends a stream the moment its handler
        // returns, so a handler that returned here would close every inbound stream before the
        // script could `reply` on it.
        this.byId[who].set(stream.id, stream);
        await stream.closed;
      });
    });
    this.served = [this.peers.dialer.serve(), this.peers.acceptor.serve()];
    this.served.forEach((task) => {
      void task.catch(() => undefined);
    });
  }

  async stop(): Promise<void> {
    this.sockets.dialer.drop();
    await this.settle();
    // Nothing to cancel: a `serve()` whose socket is gone returns, and a `close()` left in flight
    // settles on the same event. Their rejections were absorbed when they were started.
    await Promise.race([Promise.all(this.served), turn()]);
  }

  async settle(rounds = 12): Promise<void> {
    for (let round = 0; round < rounds; round += 1) {
      await turn();
    }
  }

  // ------------------------------------------------------------------ the script

  async run(): Promise<void> {
    for (const [index, step] of this.fixture.steps.entries()) {
      try {
        await this.step(step);
      } catch (error) {
        const detail = error instanceof Error ? error.message : String(error);
        throw new Error(`${this.fixture.name} step ${index}: ${detail}`);
      }
    }
  }

  private async step(step: Step): Promise<void> {
    if ('settle' in step) await this.settle(Number(step.settle));
    else if ('call' in step) await this.doCall(step);
    else if ('expect_frame' in step) this.expectFrame(step);
    else if ('expect_no_frame' in step) this.expectNoFrame(step);
    else if ('expect_result' in step) await this.expectResult(step.expect_result);
    else if ('expect_error' in step) await this.expectError(step.expect_error);
    else if ('expect_closed' in step) await this.expectClosed(step.expect_closed);
    else throw new Error(`no step kind in ${JSON.stringify(step)}`);
  }

  /** The calls this milestone's corpus needs. M6 adds the rest, deliberately and in both runners. */
  private async doCall(step: Step): Promise<void> {
    const who = step.peer as Who;
    const peer = this.peers[who];

    if (step.call === 'open') {
      const stream = peer.open(step.payload ?? null, { headers: step.headers, end: step.end === true });
      this.ordinals.push(stream.id);
      this.byId[who].set(stream.id, stream);
      if (typeof step.as === 'string') this.refs.set(step.as, stream);
      return;
    }
    if (step.call === 'reply') {
      await this.streamFor(who, Number(step.stream_ref)).reply(step.payload ?? null);
      return;
    }
    if (step.call === 'close') {
      // `drain_ms` is milliseconds here and in the corpus; the Python runner is the one that
      // converts, because Python's durations are seconds (WSM-CON-012).
      const closing = peer.close({
        code: (step.code ?? ResetCode.NO_ERROR) as ResetCode,
        reason: step.reason as string | undefined,
        drainMs: step.drain_ms as number | undefined,
      });
      void closing.catch(() => undefined);
      // Started rather than awaited: `close()` sends `goaway`, *then* drains, and the steps after
      // this one are what the drain window is there to let happen (WSM-CON-025).
      this.closing.set(who, closing);
      return;
    }
    if (step.call === 'await_close') {
      const closing = this.closing.get(who);
      if (closing === undefined) throw new Error(`await_close: ${who} has no close() in flight`);
      await withTimeout(closing, `${who}.close()`);
      return;
    }
    throw new Error(`the runner does not implement the call '${String(step.call)}'`);
  }

  // ------------------------------------------------------------------ resolution

  /** `stream_ref: n` -> the id the n-th stream in the script actually got (WSM-TST-002). */
  private idOf(ordinal: number): number {
    if (!Number.isInteger(ordinal) || ordinal < 1 || ordinal > this.ordinals.length) {
      throw new Error(`stream_ref ${ordinal} names a stream this script has not opened`);
    }
    return this.ordinals[ordinal - 1];
  }

  private streamFor(who: Who, ordinal: number): Stream {
    const streamId = this.idOf(ordinal);
    const stream = this.byId[who].get(streamId);
    if (stream === undefined) throw new Error(`${who} has no stream for ordinal ${ordinal} (id ${streamId})`);
    return stream;
  }

  /** Turn the two ordinal-bearing keys into the ids this run allocated. */
  private resolve(wanted: Record<string, unknown>): Record<string, unknown> {
    const resolved: Record<string, unknown> = {};
    Object.entries(wanted).forEach(([key, value]) => {
      if (key === 'stream_ref') resolved.stream = this.idOf(Number(value));
      else if (key === 'last_stream_ref') resolved.last_stream = this.idOf(Number(value));
      else resolved[key] = value;
    });
    return resolved;
  }

  private framesSentBy(who: Who): Frame[] {
    return this.sockets[who].sent.map((message) => this.codec.decode(message));
  }

  /** A **subset** match: the listed keys, and nothing about the rest (WSM-TST-002). */
  private static matches(frame: Frame, wanted: Record<string, unknown>): boolean {
    const envelope = frame as unknown as Record<string, unknown>;
    return Object.entries(wanted).every(([key, value]) => JSON.stringify(envelope[key]) === JSON.stringify(value));
  }

  // ------------------------------------------------------------------ assertions

  private expectFrame(step: Step): void {
    const who = step.peer as Who;
    const wanted = this.resolve(step.expect_frame as Record<string, unknown>);
    const frames = this.framesSentBy(who);
    for (let index = this.cursor[who]; index < frames.length; index += 1) {
      if (Replay.matches(frames[index], wanted)) {
        this.cursor[who] = index + 1;
        return;
      }
    }
    throw new Error(
      `${who} sent no frame matching ${JSON.stringify(wanted)}; ` +
        `it sent ${JSON.stringify(frames.slice(this.cursor[who]))}`,
    );
  }

  /** Not "not yet", but "not at all": the whole of that peer's wire is searched. */
  private expectNoFrame(step: Step): void {
    const who = step.peer as Who;
    const wanted = this.resolve(step.expect_no_frame as Record<string, unknown>);
    const offending = this.framesSentBy(who).filter((frame) => Replay.matches(frame, wanted));
    if (offending.length > 0) {
      throw new Error(
        `${who} sent ${JSON.stringify(offending)}, and this fixture says it must send nothing ` +
          `matching ${JSON.stringify(wanted)}`,
      );
    }
  }

  private async expectResult(spec: Step): Promise<void> {
    const stream = this.streamNamed(spec.ref as string);
    const value = await withTimeout(stream.result(), `${String(spec.ref)}.result()`);
    expect(value).toEqual(spec.value);
  }

  private async expectError(spec: Step): Promise<void> {
    const stream = this.streamNamed(spec.ref as string);
    const isExpected = ERROR_CLASSES[spec.error as string];
    if (isExpected === undefined) throw new Error(`expect_error names an unknown class '${String(spec.error)}'`);

    let caught: unknown = NOTHING_THROWN;
    try {
      await withTimeout(stream.result(), `${String(spec.ref)}.result()`);
    } catch (error) {
      caught = error;
    }
    if (caught === NOTHING_THROWN) throw new Error(`${String(spec.ref)} was expected to fail and did not`);
    if (!isExpected(caught)) {
      throw new Error(`${String(spec.ref)} failed with ${String(caught)}, expected ${String(spec.error)}`);
    }
    if (spec.code !== undefined) expect((caught as StreamReset).code).toBe(spec.code);
  }

  /** A bounded poll, not a single read: the other end learns of a close one turn later. */
  private async expectClosed(spec: Step): Promise<void> {
    const who = spec.peer as Who;
    const wantSocket = spec.socket === true;
    for (let round = 0; round < POLL_TURNS; round += 1) {
      if (!this.peers[who].isOpen && (!wantSocket || this.sockets[who].isClosed)) return;
      await turn();
    }
    throw new Error(
      `${who} is still open (peer.isOpen=${this.peers[who].isOpen}, ` +
        `socket.isClosed=${this.sockets[who].isClosed})`,
    );
  }

  private streamNamed(ref: string): Stream {
    const stream = this.refs.get(ref);
    if (stream === undefined) throw new Error(`no step labelled '${ref}' with "as"`);
    return stream;
  }
}

// --------------------------------------------------------------------------- the suite

describe('conformance/sequences', () => {
  FIXTURE_FILES.forEach((fileName) => {
    it(`replays ${fileName}`, async () => {
      const fixture = JSON.parse(readFileSync(join(SEQUENCES_DIR, fileName), 'utf-8')) as Fixture;
      expect(fixture.name).toBe(fileName.replace(/\.json$/, ''));

      const replay = new Replay(fixture);
      replay.start();
      try {
        await replay.run();
      } finally {
        await replay.stop();
      }
    });
  });

  it('collects a corpus that is not empty', () => {
    // An empty `conformance/sequences/` is a suite that passes by testing nothing.
    expect(FIXTURE_FILES.length).toBeGreaterThan(0);
    // `muxws/sequences_test.py` pins the same number and must be raised with this one.
    expect(FIXTURE_FILES.length).toBe(EXPECTED_FIXTURES);
  });
});
