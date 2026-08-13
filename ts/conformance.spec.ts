/**
 * The TypeScript conformance runner: `conformance/sequences/` and `conformance/invalid/` (M6 §3).
 *
 * The twin of `muxws/conformance_test.py`, reading the same files under the same schema
 * (`conformance/README.md`). If one runner needs a field the other does not read, the fixture is
 * wrong.
 *
 * Two corpora, two shapes of proof:
 *
 * - **`sequences/`** - both peers are real, nothing is injected, and the fixture is a script of
 *   ordinary API calls with assertions interleaved. What it proves is that the ports agree about
 *   *sequences* - which frame goes out when, which stream survives which event - and not merely
 *   about how one frame is spelled. Every fixture is replayed **twice, with the roles swapped**,
 *   which is what `stream_ref`-as-an-ordinal buys and the whole point of the corpus: a raw id would
 *   bake one side's parity in, and the second pass is what proves it did not.
 * - **`invalid/`** - one real peer, fed messages no correct implementation would send, asserting
 *   both which frame goes out and whether the connection survives. The survival column is the point:
 *   it is the difference between "the peers can still agree about every other stream" and "they
 *   cannot" (WSM-STM-024).
 *
 * Nothing here may change peer behaviour. A failing fixture means a rule was implemented wrongly in
 * M1-M5b; the fix belongs to the milestone that owns the rule, not to a fixture edit.
 */

import { createHash } from 'node:crypto';
import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

import { JsonCodec } from './codec';
import type { Codec } from './codec';
import { ConnectionLost, RemoteError, ResetCode, StreamRefused, StreamReset, StreamTimeout } from './errors';
import { ABSENT, deepEqual, fromMapping } from './frames';
import type { Frame } from './frames';
import { MsgpackCodec } from './msgpack';
import { Peer } from './peer';
import type { Stream } from './stream';
import { MemorySocket, memoryPair } from './transports/memory';

// --------------------------------------------------------------------------- the corpus

/** vitest runs from the repository root, which is where `conformance/` sits. */
const CONFORMANCE = join(process.cwd(), 'conformance');
const SEQUENCES_DIR = join(CONFORMANCE, 'sequences');
const INVALID_DIR = join(CONFORMANCE, 'invalid');
const FRAMES_DIR = join(CONFORMANCE, 'frames');

function fixtureFiles(directory: string): string[] {
  return readdirSync(directory)
    .filter((name) => name.endsWith('.json'))
    .sort();
}

const SEQUENCE_FILES = fixtureFiles(SEQUENCES_DIR);
const INVALID_FILES = fixtureFiles(INVALID_DIR);

/**
 * Both runners carry these two numbers and both assert them, and the Python runner reads *these two
 * lines* to prove the counts still agree. A fixture that stopped being collected would otherwise be
 * a suite that passes by testing nothing, so keep the declarations on one line each.
 */
const EXPECTED_SEQUENCE_FIXTURES = 14;
const EXPECTED_INVALID_FIXTURES = 9;

/**
 * The nine cases WSM-TST-003 enumerates, **by name**. Enumerated rather than counted: a count alone
 * passes when one case is deleted and another duplicated under a new name.
 */
const REQUIRED_INVALID_CASES = [
  'data-above-high-water-mark',
  'data-after-end',
  'data-for-closed-id',
  'fragment-interrupted-by-non-fragment',
  'frame-over-max-frame-bytes',
  'headers-on-a-later-frame',
  'open-id-not-monotonic',
  'open-wrong-parity',
  'undecodable-message',
];

/**
 * WSM-CON-031, WSM-CON-009, WSM-PKG-005: no limit and no version, in any frame, in any form. Checked
 * against the **envelope keys on the wire** rather than against a decoded `Frame`, because
 * `fromMapping` drops unknown keys - decoding first would hide exactly the key this is looking for.
 * `muxws/conformance_test.py` carries the same two lists against the same corpus; a rule only one
 * port checks is a rule that drifts, which is the whole reason the corpus exists.
 */
const FORBIDDEN_ENVELOPE_KEYS = [
  'ack',
  'protocol_version',
  'extensions',
  'max_frame_bytes',
  'max_concurrent_streams',
  'max_payload_bytes',
];

/** Reserved in v1 and never sent (WSM-BPR-001). There is no `settings` frame at all (WSM-CON-031). */
const FORBIDDEN_FRAME_TYPES = ['settings', 'window_update'];

/** Wall-clock ceiling on any one waiting step, so a stalled fixture fails by name rather than hangs. */
const STEP_TIMEOUT_MS = 2000;

/** How many turns of the event loop a bounded poll is given before it gives up. */
const POLL_TURNS = 400;

const CODEC_NAME = new JsonCodec().name;

/** The second shipped codec, which WSM-CDC-007 requires to replay this same corpus. */
const BINARY_CODEC_NAME = new MsgpackCodec().name;

type Who = 'dialer' | 'acceptor';

type Step = Record<string, any>;

interface Fixture {
  name: string;
  description?: string;
  max_frame_bytes?: number;
  max_concurrent_streams?: number;
  requires_codec?: string;
  steps: Step[];
}

interface InvalidFixture {
  name: string;
  description: string;
  max_frame_bytes?: number;
  inbound: Record<string, any>[];
  expect_out: Record<string, any>[];
  connection_survives: boolean;
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

/**
 * The envelope defaults, mirroring the module-private `FIELD_DEFAULTS` of `frames.ts`.
 *
 * A matcher must read a field the frame left out as that field's default, exactly as Python's
 * dataclass does. Reading it as `undefined` instead would make `{"end": false}` fail here and pass
 * there - one corpus, quietly meaning two different things.
 */
const FIELD_DEFAULTS: Record<string, unknown> = {
  stream: null,
  payload: ABSENT,
  fragment: null,
  more: false,
  headers: null,
  end: false,
  trailers: null,
  code: null,
  reason: null,
  nonce: null,
  last_stream: null,
};

function fieldOf(frame: Frame, key: string): unknown {
  const raw = (frame as unknown as Record<string, unknown>)[key];
  return raw === undefined ? FIELD_DEFAULTS[key] : raw;
}

/** Render a value for a failure message, spelling out bytes that `JSON.stringify` renders as `{}`. */
function render(value: unknown): string {
  if (value instanceof ArrayBuffer) return `bytes[${[...new Uint8Array(value)].join(', ')}]`;
  if (Array.isArray(value)) return `[${value.map((item) => render(item)).join(', ')}]`;
  if (typeof value === 'object' && value !== null) {
    return `{${Object.entries(value)
      .map(([key, item]) => `${key}: ${render(item)}`)
      .join(', ')}}`;
  }
  return JSON.stringify(value) ?? String(value);
}

/**
 * Resolve every `{"$bytes": [...]}` placeholder into the `ArrayBuffer` those integers spell.
 *
 * JSON has no byte type, so a fixture that needs one under a binary codec (WSM-CDC-008) has to spell
 * it. This is fixture *notation*, resolved before the first step runs; it is never a payload shape
 * and never reaches the wire. A runner that passed the mapping through unresolved would send
 * `{"$bytes": [...]}` as an ordinary object and pass the fixture while testing nothing, which is
 * what `resolves the $bytes placeholder before a step runs` exists to prevent.
 *
 * Only an object whose **sole** key is `$bytes` is a placeholder; `$bytes` alongside anything else
 * is an ordinary payload key, so an application object can still carry one.
 */
function substituteBytes(value: unknown): unknown {
  if (Array.isArray(value)) return value.map((item) => substituteBytes(item));
  if (typeof value !== 'object' || value === null) return value;
  const keys = Object.keys(value as object);
  if (keys.length === 1 && keys[0] === '$bytes') {
    return new Uint8Array((value as { $bytes: number[] }).$bytes).buffer;
  }
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>).map(([key, item]) => [key, substituteBytes(item)]),
  );
}

/** A **subset** match: the listed keys, and nothing about the rest (WSM-TST-002). */
function matches(frame: Frame, wanted: Record<string, unknown>): boolean {
  return Object.entries(wanted).every(([key, value]) => deepEqual(fieldOf(frame, key), value));
}

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

async function settle(rounds = 12): Promise<void> {
  for (let round = 0; round < rounds; round += 1) {
    await turn();
  }
}

/** Python reaches for `asyncio.wait_for`; this is the same guard with the same purpose. */
async function withTimeout<T>(work: Promise<T>, what: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(STEP_EXPIRED), STEP_TIMEOUT_MS);
  });
  try {
    // Racing rather than cancelling, which is also what the Python runner's `shield` buys: a step
    // that gives up must not tear down the call underneath it, or the failure reported would be a
    // reset this fixture never asked for.
    return await Promise.race([work, deadline]);
  } catch (error) {
    if (error === STEP_EXPIRED) throw new Error(`${what} did not settle within ${STEP_TIMEOUT_MS}ms`);
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

// --------------------------------------------------------------------------- the sequence runner

/** One fixture, one live pair, one pass over `steps` - in one of the two role assignments. */
class Replay {
  private readonly codec: Codec;

  private readonly sockets: Record<Who, MemorySocket>;

  readonly peers: Record<Who, Peer>;

  /** Ordinal -> the id that stream actually got. Filled by every step that opens one. */
  private readonly ordinals: number[] = [];

  /** Both peers' view of every stream, by id: the opener's handle and the receiver's. */
  private readonly byId: Record<Who, Map<number, Stream>> = { dialer: new Map(), acceptor: new Map() };

  /** `as` labels bound to a stream, for a later `expect_result` / `expect_error`. */
  private readonly refs = new Map<string, Stream>();

  /**
   * `as` labels bound to work already in flight - a `request`, an `iterate` - which is a different
   * thing from a stream: the value those steps assert is the call's return, and the call was started
   * rather than awaited so that the steps after it could run.
   */
  private readonly tasks = new Map<string, Promise<unknown>>();

  /** In-flight `close()` calls, awaited by an `await_close` step. */
  private readonly closing = new Map<Who, Promise<void>>();

  /** How far `expect_frame` has consumed each peer's wire; frames match forwards, in order. */
  private readonly cursor: Record<Who, number> = { dialer: 0, acceptor: 0 };

  private served: Promise<void>[] = [];

  constructor(
    private readonly fixture: Fixture,
    private readonly swapped: boolean,
    codec?: Codec,
  ) {
    // The codec is a parameter, not a constant, because WSM-CDC-007 requires every shipped codec to
    // run this corpus with a real peer at each end - a corpus only the default codec ever replays
    // proves nothing about the seam it is supposed to prove.
    this.codec = codec ?? new JsonCodec();
    const [dialerSocket, acceptorSocket] = memoryPair();
    this.sockets = { dialer: dialerSocket, acceptor: acceptorSocket };
    // Both are instructions to the runner, never wire values (WSM-TST-002, WSM-FRG-005,
    // WSM-CON-031).
    const options = {
      codec: this.codec,
      maxFrameBytes: fixture.max_frame_bytes,
      maxConcurrentStreams: fixture.max_concurrent_streams,
    };
    // The two roles keep their names; which of them holds the odd parity is what swaps. That is the
    // whole of a role swap, and it is why a fixture may never write a raw stream id: every id in the
    // second pass is the other one (WSM-TST-002).
    this.peers = {
      dialer: new Peer(dialerSocket, { ...options, isDialer: !swapped }),
      acceptor: new Peer(acceptorSocket, { ...options, isDialer: swapped }),
    };
  }

  // ------------------------------------------------------------------ lifecycle

  start(): void {
    (['dialer', 'acceptor'] as Who[]).forEach((who) => {
      this.peers[who].onStream(async (payload: unknown, stream: Stream) => {
        // Record the stream, then hold it open. WSM-STM-035 ends a stream the moment its handler
        // returns, so a handler that returned here would close every inbound stream before the
        // script could `reply` on it.
        this.byId[who].set(stream.id, stream);
        // The `{"handler": "raise"}` payload is the only way a script of ordinary API calls can
        // reach the handler-failure path of WSM-STM-034 - `stream.reset(APPLICATION_ERROR)` sends
        // the same code but carries no serialized error object, which is the half a fixture pins.
        // `Error` and not a subclass: the serializer sends the class name, and Python raises a class
        // spelled `Error` for the same reason, so that the pinned payload is one value not two.
        const asked = payload as { handler?: string; message?: string } | null;
        if (asked !== null && typeof asked === 'object' && asked.handler === 'raise') {
          throw new Error(asked.message ?? 'the handler raised');
        }
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
    await settle();
    // Nothing to cancel: a `serve()` whose socket is gone returns, a `close()` left in flight
    // settles on the same event, and a collector still iterating a stream ends when the stream
    // fails. Their rejections were absorbed when they were started.
    await Promise.race([Promise.all(this.served), turn()]);
  }

  // ------------------------------------------------------------------ the script

  async run(): Promise<void> {
    for (const [index, step] of this.fixture.steps.entries()) {
      try {
        await this.step(step);
      } catch (error) {
        const detail = error instanceof Error ? error.message : String(error);
        throw new Error(
          `${this.fixture.name} [${this.swapped ? 'roles swapped' : 'dialer opens'}] step ${index}: ${detail}`,
        );
      }
    }
  }

  private async step(step: Step): Promise<void> {
    if ('settle' in step) await settle(Number(step.settle));
    else if ('call' in step) await this.doCall(step);
    else if ('inject' in step) this.inject(step);
    else if ('expect_frame' in step) this.expectFrame(step);
    else if ('expect_no_frame' in step) this.expectNoFrame(step);
    else if ('expect_result' in step) await this.expectResult(step.expect_result);
    else if ('expect_headers' in step) await this.expectHeaders(step.expect_headers);
    else if ('expect_error' in step) await this.expectError(step.expect_error);
    else if ('expect_closed' in step) await this.expectClosed(step.expect_closed);
    else throw new Error(`no step kind in ${JSON.stringify(step)}`);
  }

  /** Dispatch one `call` step. An unimplemented call is a hard failure, never a skip. */
  private async doCall(step: Step): Promise<void> {
    const calls: Record<string, (step: Step) => Promise<void>> = {
      open: (s) => this.callOpen(s),
      request: (s) => this.callRequest(s),
      notify: (s) => this.callNotify(s),
      send: (s) => this.callSend(s),
      send_headers: (s) => this.callSendHeaders(s),
      end: (s) => this.callEnd(s),
      reply: (s) => this.callReply(s),
      cancel: (s) => this.callCancel(s),
      iterate: (s) => this.callIterate(s),
      close: (s) => this.callClose(s),
      await_close: (s) => this.callAwaitClose(s),
    };
    const handler = calls[String(step.call)];
    if (handler === undefined) throw new Error(`the runner does not implement the call '${String(step.call)}'`);

    const expected = step.raises as string | undefined;
    if (expected === undefined) {
      await handler(step);
      return;
    }
    // `raises` is how a fixture states that the *call* fails - a producer whose stream the consumer
    // cancelled, say. The alternative is asserting on frames, which cannot see that the local handle
    // refuses to send at all (WSM-ERR-009).
    let caught: unknown = NOTHING_THROWN;
    try {
      await handler(step);
    } catch (error) {
      caught = error;
    }
    if (caught === NOTHING_THROWN) throw new Error(`${String(step.call)} was expected to throw ${expected}`);
    if (!ERROR_CLASSES[expected](caught))
      throw new Error(`${String(step.call)} threw ${String(caught)}, not ${expected}`);
  }

  private async callOpen(step: Step): Promise<void> {
    const peer = this.peers[step.peer as Who];
    const stream = peer.open(step.payload ?? null, { headers: step.headers, end: step.end === true });
    this.adopt(step, stream);
  }

  /**
   * `peer.request(...)`, **started and not awaited** (WSM-API-006).
   *
   * The steps after a `request` are the assertions about the frames it produced, so awaiting it here
   * would deadlock every fixture that uses it. The stream it allocated is then discovered through
   * the public `peer.streams` map rather than returned, because `request()` deliberately hands back
   * a value and not a handle.
   */
  private async callRequest(step: Step): Promise<void> {
    const peer = this.peers[step.peer as Who];
    const before = new Set(peer.streams.keys());
    const task = peer.request(step.payload ?? null, {
      headers: step.headers,
      // Milliseconds here and in the corpus; the Python runner is the one that converts, because
      // Python's durations are seconds (WSM-CON-012).
      timeoutMs: step.timeout_ms as number | undefined,
    });
    void task.catch(() => undefined);
    this.adopt(step, (await this.discover(peer, before)).stream, task);
  }

  private async callNotify(step: Step): Promise<void> {
    const peer = this.peers[step.peer as Who];
    const before = new Set(peer.streams.keys());
    await peer.notify(step.payload ?? null, { headers: step.headers });
    this.adopt(step, (await this.discover(peer, before)).stream);
  }

  private async callSend(step: Step): Promise<void> {
    await this.streamFor(step.peer as Who, Number(step.stream_ref)).send(step.payload ?? null, {
      end: step.end === true,
      headers: step.headers as Record<string, unknown> | undefined,
    });
  }

  /** The answering side's leading headers, on a frame with no payload at all (WSM-API-024). */
  private async callSendHeaders(step: Step): Promise<void> {
    await this.streamFor(step.peer as Who, Number(step.stream_ref)).sendHeaders(
      step.headers as Record<string, unknown>,
    );
  }

  private async callEnd(step: Step): Promise<void> {
    // An absent `payload` key is `ABSENT`, not `null`: they are different frames (D1).
    await this.streamFor(step.peer as Who, Number(step.stream_ref)).end({
      payload: 'payload' in step ? step.payload : undefined,
      trailers: step.trailers,
      headers: step.headers as Record<string, unknown> | undefined,
    });
  }

  private async callReply(step: Step): Promise<void> {
    await this.streamFor(step.peer as Who, Number(step.stream_ref)).reply(step.payload ?? null, {
      trailers: step.trailers,
      headers: step.headers as Record<string, unknown> | undefined,
    });
  }

  private async callCancel(step: Step): Promise<void> {
    await this.streamFor(step.peer as Who, Number(step.stream_ref)).cancel(step.reason as string | undefined);
  }

  /**
   * Start consuming a stream as an async iterator; the collected list is the ref's value.
   *
   * Started rather than awaited, and bound to a label, so that iteration and the frames that feed it
   * can be asserted in the same script: `expect_result` on the label compares the whole list.
   */
  private async callIterate(step: Step): Promise<void> {
    const stream = this.streamFor(step.peer as Who, Number(step.stream_ref));
    const collect = async (): Promise<unknown[]> => {
      const items: unknown[] = [];
      for await (const item of stream) items.push(item);
      return items;
    };
    const task = collect();
    void task.catch(() => undefined);
    this.tasks.set(String(step.as), task);
  }

  private async callClose(step: Step): Promise<void> {
    const who = step.peer as Who;
    const closing = this.peers[who].close({
      code: (step.code ?? ResetCode.NO_ERROR) as ResetCode,
      reason: step.reason as string | undefined,
      drainMs: step.drain_ms as number | undefined,
    });
    void closing.catch(() => undefined);
    // Started rather than awaited: `close()` sends `goaway`, *then* drains, and the steps after this
    // one are what the drain window is there to let happen (WSM-CON-025).
    this.closing.set(who, closing);
  }

  private async callAwaitClose(step: Step): Promise<void> {
    const closing = this.closing.get(step.peer as Who);
    if (closing === undefined) throw new Error(`await_close: ${String(step.peer)} has no close() in flight`);
    await withTimeout(closing, `${String(step.peer)}.close()`);
  }

  /** Give the stream this step opened its ordinal, and bind `as` if the step carries one. */
  private adopt(step: Step, stream: Stream, task?: Promise<unknown>): void {
    this.ordinals.push(stream.id);
    this.byId[step.peer as Who].set(stream.id, stream);
    if (typeof step.as !== 'string') return;
    if (task === undefined) this.refs.set(step.as, stream);
    else this.tasks.set(step.as, task);
  }

  /**
   * The stream a call opened without handing it back, found through the public map.
   *
   * A bounded poll rather than a single read: `request()` reaches its `open()` synchronously here,
   * but in Python it is a coroutine that does not run until the task is first scheduled - exactly
   * the kind of difference a shared corpus must not be able to see.
   *
   * The stream comes back **boxed**, and that is not decoration. `Stream` is a thenable, so a
   * promise resolved with one adopts it: `await discover(...)` would await the stream itself,
   * claiming it for `await` and making every later `iterate` on it fail as a second consumer
   * (WSM-API-014). A box is not thenable and cannot be adopted.
   */
  private async discover(peer: Peer, before: Set<number>): Promise<{ stream: Stream }> {
    for (let round = 0; round < POLL_TURNS; round += 1) {
      for (const [streamId, stream] of peer.streams) {
        if (!before.has(streamId)) return { stream };
      }
      await turn();
    }
    throw new Error('the call opened no stream');
  }

  /**
   * Deliver one message **to** the named peer, as if its remote had sent it.
   *
   * The envelope is encoded as it is written, without passing through `fromMapping`, which is the
   * whole point for the two extension-point fixtures: `fromMapping` drops unknown keys
   * (WSM-FRM-001), so a frame built through it could never carry the unknown field whose toleration
   * is being asserted.
   */
  private inject(step: Step): void {
    const who = step.peer as Who;
    this.sockets[who].inject(this.codec.encodePayload(this.resolve(step.inject as Record<string, unknown>)));
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

  framesSentBy(who: Who): Frame[] {
    return this.sockets[who].sent.map((message) => this.codec.decode(message));
  }

  /**
   * Every message both peers actually put on the wire, undecoded.
   *
   * Undecoded on purpose: `fromMapping` drops unknown keys (WSM-FRM-001), so reading a decoded
   * `Frame` would discard exactly the key `puts no limit and no version on the wire` looks for.
   */
  wire(): string[] {
    return (['dialer', 'acceptor'] as Who[]).flatMap((who) => this.sockets[who].sent.map((message) => String(message)));
  }

  // ------------------------------------------------------------------ assertions

  private expectFrame(step: Step): void {
    const who = step.peer as Who;
    const wanted = this.resolve(step.expect_frame as Record<string, unknown>);
    const frames = this.framesSentBy(who);
    for (let index = this.cursor[who]; index < frames.length; index += 1) {
      if (matches(frames[index], wanted)) {
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
    const offending = this.framesSentBy(who).filter((frame) => matches(frame, wanted));
    if (offending.length > 0) {
      throw new Error(
        `${who} sent ${JSON.stringify(offending)}, and this fixture says it must send nothing ` +
          `matching ${JSON.stringify(wanted)}`,
      );
    }
  }

  /** Await whatever the label was bound to: a call in flight, or a stream's own result. */
  private valueOf(ref: string): Promise<unknown> {
    const task = this.tasks.get(ref);
    if (task !== undefined) return withTimeout(task, `${ref}`);
    const stream = this.refs.get(ref);
    if (stream === undefined) throw new Error(`no step labelled '${ref}' with "as"`);
    return withTimeout(stream.result(), `${ref}.result()`);
  }

  private async expectResult(spec: Step): Promise<void> {
    // `deepEqual` rather than `toEqual`, because vitest's `toEqual` compares two `ArrayBuffer`s by
    // byte *length* and calls buffers of equal length equal whatever they contain. Under a binary
    // codec a payload is an `ArrayBuffer` (WSM-CDC-008), so `toEqual` would pass this assertion for
    // a peer that delivered the right number of entirely wrong bytes.
    const value = await this.valueOf(spec.ref as string);
    expect(deepEqual(value, spec.value), `${String(spec.ref)} produced ${render(value)}`).toBe(true);
  }

  /**
   * What a peer ended up **holding**, which no assertion on frames can see (WSM-API-025).
   *
   * `expect_frame` proves the headers went out; this proves they were surfaced, on the attribute
   * that is theirs. `of` is required rather than defaulted, because the two attributes are the whole
   * distinction the step exists to check and a fixture that omitted it would be asserting whichever
   * one this runner happened to prefer. For `reply` the wait is on `replyHeadersArrived` rather than
   * on a settle count: that event fires the moment the value stops changing, and a fixture that
   * slept instead would pass against a port that never fired it at all.
   */
  private async expectHeaders(spec: Step): Promise<void> {
    const stream = this.streamFor(spec.peer as Who, Number(spec.stream_ref));
    const of = String(spec.of);
    if (of !== 'open' && of !== 'reply') throw new Error(`expect_headers needs "of": "open" or "reply", not ${of}`);
    if (of === 'reply') {
      await withTimeout(
        stream.replyHeadersArrived,
        `${String(spec.peer)} stream ${String(spec.stream_ref)} reply headers`,
      );
    }
    const held = of === 'open' ? stream.headers : stream.replyHeaders;
    expect(deepEqual(held, spec.value), `${of} headers were ${render(held)}`).toBe(true);
  }

  private async expectError(spec: Step): Promise<void> {
    const isExpected = ERROR_CLASSES[spec.error as string];
    if (isExpected === undefined) throw new Error(`expect_error names an unknown class '${String(spec.error)}'`);

    let caught: unknown = NOTHING_THROWN;
    try {
      await this.valueOf(spec.ref as string);
    } catch (error) {
      caught = error;
    }
    if (caught === NOTHING_THROWN) throw new Error(`${String(spec.ref)} was expected to fail and did not`);
    if (!isExpected(caught)) {
      throw new Error(`${String(spec.ref)} failed with ${String(caught)}, expected ${String(spec.error)}`);
    }
    if (spec.code !== undefined) expect((caught as StreamReset).code).toBe(spec.code);
    // `deepEqual` for the same reason `expect_result` uses it: a serialized error payload may carry
    // bytes under a binary codec, and `toEqual` cannot tell two `ArrayBuffer`s of one length apart.
    if (spec.payload !== undefined) {
      const carried = (caught as RemoteError).payload;
      expect(deepEqual(carried, spec.payload), `${String(spec.ref)} carried ${render(carried)}`).toBe(true);
    }
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
}

function loadSequence(fileName: string): Fixture {
  return substituteBytes(JSON.parse(readFileSync(join(SEQUENCES_DIR, fileName), 'utf-8'))) as Fixture;
}

/** Run one sequence fixture end to end and hand back the finished `Replay` for inspection. */
async function replay(fileName: string, swapped: boolean, codec?: Codec): Promise<Replay> {
  const fixture = loadSequence(fileName);
  expect(fixture.name).toBe(fileName.replace(/\.json$/, ''));

  const replaying = new Replay(fixture, swapped, codec);
  replaying.start();
  try {
    await replaying.run();
  } finally {
    await replaying.stop();
  }
  return replaying;
}

/**
 * A fixture written for another codec is **skipped with a reason**, never quietly passed.
 *
 * `bytes-payload-under-binary-codec` is the one that needs it: bytes are a payload type only under a
 * binary codec (WSM-CDC-008), and there is nothing a JSON-configured pass can assert about it. A skip
 * nobody can see is the same as a missing test, so the reason is in the test's own name and the set
 * of skipped fixtures is itself pinned by a test below - and the msgpack suite skips nothing, which
 * is what stops this from being a fixture every configuration skips (WSM-CDC-007).
 */
function skipReason(fixture: Fixture, codecName: string = CODEC_NAME): string | null {
  const required = fixture.requires_codec;
  if (required === undefined || required === codecName) return null;
  return `requires the ${required} codec; this runner is configured with ${codecName}`;
}

// --------------------------------------------------------------------------- the suites

describe('conformance/sequences', () => {
  SEQUENCE_FILES.forEach((fileName) => {
    const reason = skipReason(loadSequence(fileName));
    [false, true].forEach((swapped) => {
      const assignment = swapped ? 'roles swapped' : 'dialer opens';
      if (reason !== null) {
        // The second pass is not decoration. Every stream id in it is the other parity, so a fixture
        // that wrote a raw id - or a runner that resolved an ordinal to a constant - fails there and
        // only there (WSM-TST-002).
        it.skip(`skips ${fileName} [${assignment}]: it ${reason}`, () => undefined);
        return;
      }
      it(`replays ${fileName} [${assignment}]`, async () => {
        await replay(fileName, swapped);
      });
    });
  });

  it('collects the corpus, and the same number of fixtures as the Python runner', () => {
    // An empty `conformance/sequences/` is a suite that passes by testing nothing.
    expect(SEQUENCE_FILES.length).toBeGreaterThan(0);
    expect(SEQUENCE_FILES.length).toBe(EXPECTED_SEQUENCE_FIXTURES);
    // `muxws/conformance_test.py` pins the same two numbers and reads them out of this file.
  });

  it('runs every fixture under some configured codec, skipping none everywhere', () => {
    // WSM-CDC-007. Asserted about the corpus rather than trusted to the two suites, because the
    // failure it guards against is silent: a `requires_codec` naming a codec no suite is configured
    // with produces a fixture every configuration skips, which reads in the report as covered while
    // being reachable by no line of the library.
    const configured = [CODEC_NAME, BINARY_CODEC_NAME];
    SEQUENCE_FILES.forEach((fileName) => {
      const required = loadSequence(fileName).requires_codec;
      expect(
        required === undefined || configured.includes(required),
        `${fileName} requires the ${String(required)} codec and no suite is configured with it, ` +
          `so nothing runs it; configure a suite or drop the fixture (WSM-CDC-007)`,
      ).toBe(true);
    });
  });

  it('resolves the $bytes placeholder before a step runs', () => {
    // `{"$bytes": [...]}` is notation, never a payload shape (WSM-CDC-008). A runner that left it as
    // an object would send an ordinary object, both codecs would carry it happily, and the fixture
    // would pass while asserting nothing about bytes. So assert both halves: the corpus really does
    // carry the placeholder, and loading really does resolve it.
    const fileName = 'bytes-payload-under-binary-codec.json';
    expect(readFileSync(join(SEQUENCES_DIR, fileName), 'utf-8')).toContain('"$bytes"');

    const found: ArrayBuffer[] = [];
    const walk = (value: unknown): void => {
      if (value instanceof ArrayBuffer) {
        found.push(value);
        return;
      }
      if (Array.isArray(value)) {
        value.forEach((item) => walk(item));
        return;
      }
      if (typeof value !== 'object' || value === null) return;
      const keys = Object.keys(value as object);
      expect(keys.length === 1 && keys[0] === '$bytes', 'a $bytes placeholder survived loading').toBe(false);
      Object.values(value as Record<string, unknown>).forEach((item) => walk(item));
    };
    walk(loadSequence(fileName));

    expect(found.length).toBe(3);
    found.forEach((buffer) => expect([...new Uint8Array(buffer)]).toEqual([0, 255, 16]));
  });
});

/**
 * WSM-CDC-007: the second shipped codec runs the whole corpus with a real peer at each end.
 *
 * Not a duplicate of the suite above. A codec that ships without this is a codec whose only proof is
 * `decode(encode(frame)) === frame` over a bag of frames - which cannot see anything a *sequence*
 * exposes: the send path picking `sendBytes` from `codec.binary` (WSM-CDC-002), fragment sizes
 * measured as buffer length rather than character count (WSM-FRG-003), or a payload type JSON does
 * not have surviving both directions (WSM-CDC-008). It is also the suite that runs
 * `bytes-payload-under-binary-codec`, which every JSON-configured suite skips.
 */
describe('conformance/sequences under msgpack', () => {
  SEQUENCE_FILES.forEach((fileName) => {
    const reason = skipReason(loadSequence(fileName), BINARY_CODEC_NAME);
    [false, true].forEach((swapped) => {
      const assignment = swapped ? 'roles swapped' : 'dialer opens';
      if (reason !== null) {
        it.skip(`skips ${fileName} [${assignment}]: it ${reason}`, () => undefined);
        return;
      }
      it(`replays ${fileName} [${assignment}]`, async () => {
        await replay(fileName, swapped, new MsgpackCodec());
      });
    });
  });
});

describe('conformance/sequences', () => {
  /** Every message the whole corpus put on the wire, tagged with the fixture that produced it. */
  async function wireOfTheWholeCorpus(): Promise<[string, string][]> {
    const wire: [string, string][] = [];
    for (const fileName of SEQUENCE_FILES) {
      if (skipReason(loadSequence(fileName)) !== null) continue;
      const replaying = await replay(fileName, false);
      wire.push(...replaying.wire().map((message): [string, string] => [fileName, message]));
    }
    expect(wire.length, 'the corpus produced no frames at all').toBeGreaterThan(0);
    return wire;
  }

  it('puts no limit and no version on the wire', async () => {
    // WSM-CON-031, WSM-CON-009, WSM-PKG-005: replay everything and look at what actually went out.
    // There is no `settings` frame and no announced limit of any kind; the `muxws.v1.` subprotocol
    // prefix is the only version anywhere. This replaces the retired extension-advertisement test
    // (WSM-FRM-003). `muxws/conformance_test.py` asserts the same thing over the same corpus - an
    // assertion one port makes and the other does not is the divergence the corpus exists to catch.
    for (const [fileName, message] of await wireOfTheWholeCorpus()) {
      const envelope = JSON.parse(message) as Record<string, unknown>;
      expect(FORBIDDEN_FRAME_TYPES, `${fileName} put a ${String(envelope.type)} frame on the wire`).not.toContain(
        envelope.type,
      );
      const carried = FORBIDDEN_ENVELOPE_KEYS.filter((key) => Object.hasOwn(envelope, key));
      expect(carried, `${fileName} put ${carried.join(', ')} on the wire in a ${String(envelope.type)} frame`).toEqual(
        [],
      );
    }
  }, 20_000);

  it('never sends window_update', async () => {
    // WSM-BPR-001: separate from the test above, though it replays the same corpus, because it is a
    // separate promise. Flow control is *reserved*, not merely unannounced, and the day someone
    // implements it this is the test that must be deleted on purpose.
    for (const [fileName, message] of await wireOfTheWholeCorpus()) {
      expect((JSON.parse(message) as Record<string, unknown>).type, `${fileName} sent a window_update`).not.toBe(
        'window_update',
      );
    }
  }, 20_000);

  it('skips exactly the fixtures written for another codec, and says why', () => {
    // Pinned, because a skip that spread to a second fixture would be a corpus quietly shrinking.
    const skipped = SEQUENCE_FILES.map((file) => [file, skipReason(loadSequence(file))]).filter(
      ([, reason]) => reason !== null,
    );
    expect(skipped).toEqual([
      ['bytes-payload-under-binary-codec.json', `requires the msgpack codec; this runner is configured with json`],
    ]);
  });
});

// --------------------------------------------------------------------------- invalid

/**
 * `expect_out` is an ordered subset match: the listed keys must appear, in order, on some frame.
 *
 * Asserting equality instead would make every fixture invalid the moment an optional field is added,
 * which is the opposite of what WSM-FRM-001 asks of a receiver.
 */
function assertExpectedFrames(emitted: Frame[], fixture: InvalidFixture): void {
  if (fixture.expect_out.length === 0) {
    // Not "no assertion": `data-for-closed-id` needs to say that a late frame below the high-water
    // mark provokes **nothing at all** (WSM-STM-002), and this is the only way to.
    expect(emitted, `${fixture.name}: nothing should have gone out`).toEqual([]);
    return;
  }

  let remaining = emitted;
  for (const wanted of fixture.expect_out) {
    const index = remaining.findIndex((frame) => matches(frame, wanted));
    if (index === -1) {
      throw new Error(`${fixture.name}: no frame matching ${JSON.stringify(wanted)} in ${JSON.stringify(emitted)}`);
    }
    remaining = remaining.slice(index + 1);
  }
}

describe('conformance/invalid', () => {
  INVALID_FILES.forEach((fileName) => {
    it(`answers ${fileName} as declared, and survives or does not`, async () => {
      const fixture = JSON.parse(readFileSync(join(INVALID_DIR, fileName), 'utf-8')) as InvalidFixture;
      expect(fixture.name).toBe(fileName.replace(/\.json$/, ''));
      const codec = new JsonCodec();

      // The peer under test is an acceptor (even ids); the misbehaving remote is a dialer (odd ids).
      // Deliberately **not** role-swappable, unlike a sequence fixture: the whole point is
      // hand-written messages no API call on either side could have produced, and an ordinal cannot
      // name a stream that was never opened.
      const [, acceptorSocket] = memoryPair();
      const peer = new Peer(acceptorSocket, {
        codec,
        isDialer: false,
        maxFrameBytes: fixture.max_frame_bytes ?? 65_536,
      });
      peer.onStream(async (_payload: unknown, stream: Stream) => {
        await stream.closed;
      });
      const served = peer.serve();
      void served.catch(() => undefined);

      try {
        for (const entry of fixture.inbound) {
          const message = 'raw' in entry ? (entry.raw as string) : codec.encode(fromMapping(entry));
          acceptorSocket.inject(message);
          await settle();
        }
        await settle();

        assertExpectedFrames(
          acceptorSocket.sent.map((message) => codec.decode(message)),
          fixture,
        );
        expect(peer.isOpen, fixture.description).toBe(fixture.connection_survives);
      } finally {
        acceptorSocket.close();
        await settle(2);
      }
    });
  });

  it('has a fixture for every one of the nine cases of WSM-TST-003, by name', () => {
    // Without this, a deleted fixture is a silently passing suite.
    expect(INVALID_FILES.map((file) => file.replace(/\.json$/, ''))).toEqual(REQUIRED_INVALID_CASES);
    expect(INVALID_FILES.length).toBe(EXPECTED_INVALID_FIXTURES);
  });
});

// --------------------------------------------------------------------------- the freeze

describe('conformance/frames', () => {
  it('hashes to the one digest checked in to the Python runner', () => {
    // sha256 over `conformance/frames/`, sorted by file name, each file contributing `name`, a NUL,
    // its bytes, a NUL. There is exactly one checked-in digest and both ports compute it from the
    // bytes they themselves read, so a port that reads a different corpus fails here. Changing the
    // JSON wire after 1.0 means editing that literal, deliberately, in the same commit.
    const digest = createHash('sha256');
    fixtureFiles(FRAMES_DIR).forEach((fileName) => {
      digest.update(fileName, 'utf-8');
      digest.update(Buffer.from([0]));
      digest.update(readFileSync(join(FRAMES_DIR, fileName)));
      digest.update(Buffer.from([0]));
    });

    const runner = readFileSync(join(process.cwd(), 'muxws', 'conformance_test.py'), 'utf-8');
    const pinned = /^JSON_WIRE_DIGEST = "([0-9a-f]{64})"$/m.exec(runner);
    expect(pinned, 'muxws/conformance_test.py declares no JSON_WIRE_DIGEST').not.toBeNull();
    expect(digest.digest('hex')).toBe(pinned?.[1]);
  });
});
