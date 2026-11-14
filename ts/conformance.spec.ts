/**
 * The TypeScript conformance runner: `conformance/sequences/` and `conformance/invalid/`.
 *
 * Counterpart of `muxws/conformance_test.py`; both read the same files under the same schema
 * (`conformance/README.md`). A field that only one runner reads indicates a defective fixture.
 *
 * The two corpora test different things:
 *
 * - **`sequences/`** - both peers are real, nothing is injected, and the fixture is a script of ordinary API calls
 * with interleaved assertions. It checks that the ports agree about *sequences* - which frame is sent when, which
 * stream survives which event - and not only about the encoding of single frames. Every fixture is replayed
 * **twice, with the roles swapped**. `stream_ref` is an ordinal so that this is possible: a raw id would fix one
 * side's parity, and the second pass detects that. - **`invalid/`** - one real peer, fed messages no correct
 * implementation sends, asserting which frame goes out and whether the connection survives. The survival column
 * distinguishes "the peers can still agree about every other stream" from "they cannot" (WSM-STM-024).
 *
 * Nothing here changes peer behaviour. A failing fixture means a rule is implemented incorrectly; the fix belongs
 * in the code that implements the rule, not in the fixture.
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
import { logger, Peer } from './peer';
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
 * Both runners declare and assert these two numbers, and the Python runner reads *these two lines* to check that
 * the counts agree. Without this, a fixture that is no longer collected leaves a suite that passes without testing
 * it. Keep each declaration on one line.
 */
const EXPECTED_SEQUENCE_FIXTURES = 17;
const EXPECTED_INVALID_FIXTURES = 9;

/**
 * The nine cases WSM-TST-003 enumerates, **by name**. A count alone passes when one case is deleted and another
 * duplicated under a new name.
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
 * WSM-CON-031, WSM-CON-009, WSM-PKG-005: no limit and no version, in any frame, in any form. Checked against the
 * **envelope keys on the wire** rather than a decoded `Frame`, because `fromMapping` drops unknown keys and
 * decoding first would hide the key this looks for. `muxws/conformance_test.py` checks the same two lists against
 * the same corpus, so both ports enforce the rule.
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
 * `expect_error` names a class rather than a code, because WSM-ERR-004 makes the class identical across the two
 * languages. One predicate per name rather than a map of constructors: the subclasses do not share one construct
 * signature, and only `instanceof` is needed.
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
 * A matcher reads a field the frame omits as that field's default, as Python's dataclass does. Reading it as
 * `undefined` would make `{"end": false}` fail here and pass in Python, so one corpus would have two meanings.
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
 * Resolve every `{"$bytes": [...]}` placeholder into the `ArrayBuffer` those integers encode.
 *
 * JSON has no byte type, so a fixture that needs one under a binary codec (WSM-CDC-008) uses this notation. It is
 * resolved before the first step runs; it is never a payload shape and never reaches the wire. A runner that passed
 * the mapping through unresolved would send `{"$bytes": [...]}` as an ordinary object and pass the fixture without
 * testing bytes; `resolves the $bytes placeholder before a step runs` checks for that.
 *
 * Only an object whose **sole** key is `$bytes` is a placeholder; `$bytes` alongside other keys is an ordinary
 * payload key, so an application object can still carry one.
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

/** Equivalent of Python's `asyncio.wait_for`, with the same purpose. */
async function withTimeout<T>(work: Promise<T>, what: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(STEP_EXPIRED), STEP_TIMEOUT_MS);
  });
  try {
    // Racing rather than cancelling, as the Python runner's `shield` does: a step that times out must not abort the
    // call underneath it, otherwise the reported failure would be a reset the fixture did not request.
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
   * `as` labels bound to work already in flight - a `request`, an `iterate`. These differ from stream labels: the
   * asserted value is the call's return, and the call was started without awaiting so that the following steps
   * could run.
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
    // The codec is a parameter because WSM-CDC-007 requires every shipped codec to run this corpus with a real peer
    // at each end; a corpus replayed only under the default codec does not test the codec interface.
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
    // The two roles keep their names; only the assignment of odd parity swaps. Because every id in the second pass
    // has the other parity, a fixture may never contain a raw stream id (WSM-TST-002).
    this.peers = {
      dialer: new Peer(dialerSocket, { ...options, isDialer: !swapped }),
      acceptor: new Peer(acceptorSocket, { ...options, isDialer: swapped }),
    };
  }

  // ------------------------------------------------------------------ lifecycle

  start(): void {
    (['dialer', 'acceptor'] as Who[]).forEach((who) => {
      this.peers[who].onStream(async (payload: unknown, stream: Stream) => {
        // Record the stream, then hold it open. WSM-STM-035 ends a stream when its handler returns, so a handler
        // that returned here would close every inbound stream before the script could `reply` on it.
        this.byId[who].set(stream.id, stream);
        // The `{"handler": "raise"}` payload is the only way a script of ordinary API calls reaches the
        // handler-failure path of WSM-STM-034. `stream.reset(APPLICATION_ERROR)` sends the same code but no
        // serialized error object, which is what the fixture asserts. `Error` and not a subclass: the serializer
        // sends the class name, and Python raises a class named `Error` too, so the asserted payload is the same
        // value in both ports.
        const asked = payload as { handler?: string; message?: string } | null;
        if (asked !== null && typeof asked === 'object' && asked.handler === 'raise') {
          throw new Error(asked.message ?? 'the handler raised');
        }
        // `{"handler": "return"}` returns at once, which ends a oneway stream (WSM-STM-040).
        if (asked !== null && typeof asked === 'object' && asked.handler === 'return') return;
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
    // Nothing to cancel: a `serve()` whose socket is gone returns, a `close()` still in flight settles on the same
    // event, and a collector still iterating a stream ends when the stream fails. Their rejections were handled
    // when they were started.
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
    // `raises` states that the *call* fails, for example a producer whose stream the consumer cancelled. Assertions
    // on frames cannot show that the local handle refuses to send (WSM-ERR-009).
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
   * The steps after a `request` assert the frames it produced, so awaiting it here would deadlock every fixture
   * that uses it. The stream it allocated is found through the public `peer.streams` map, because `request()`
   * returns a value and not a handle.
   */
  private async callRequest(step: Step): Promise<void> {
    const peer = this.peers[step.peer as Who];
    const before = new Set(peer.streams.keys());
    const task = peer.request(step.payload ?? null, {
      headers: step.headers,
      // Milliseconds here and in the corpus; the Python runner converts, because Python's durations are seconds
      // (WSM-CON-012).
      timeoutMs: step.timeout_ms as number | undefined,
    });
    void task.catch(() => undefined);
    this.adopt(step, (await this.discover(peer, before)).stream, task);
  }

  /**
   * `peer.notify(...)`, awaited. The stream is closed at once and is not in `peer.streams` (WSM-STM-041), so its
   * ordinal is the id of the `open` it put on the wire.
   */
  private async callNotify(step: Step): Promise<void> {
    const who = step.peer as Who;
    const opens = (): number[] =>
      this.framesSentBy(who)
        .filter((frame) => frame.type === 'open')
        .map((frame) => Number(frame.stream));
    const before = new Set(opens());
    await this.peers[who].notify(step.payload ?? null, { headers: step.headers });
    for (let round = 0; round < POLL_TURNS; round += 1) {
      const fresh = opens().find((id) => !before.has(id));
      if (fresh !== undefined) {
        this.ordinals.push(fresh);
        return;
      }
      await turn();
    }
    throw new Error('the notify put no open on the wire');
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
    // An absent `payload` key is `ABSENT`, not `null`: they are different frames.
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
   * Started without awaiting and bound to a label, so that iteration and the frames that feed it can be asserted in
   * the same script: `expect_result` on the label compares the whole list.
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
    // Started without awaiting: `close()` sends `goaway`, *then* drains, and the steps after this one run inside
    // the drain window (WSM-CON-025).
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
   * The stream a call opened without returning it, found through the public map.
   *
   * A bounded poll rather than a single read: `request()` reaches its `open()` synchronously here, but in Python it
   * is a coroutine that does not run until the task is first scheduled, and the shared corpus must not depend on
   * that difference.
   *
   * The stream is returned **boxed**. `Stream` is a thenable, so a promise resolved with one adopts it: `await
   * discover(...)` would await the stream itself, claiming it for `await` and making every later `iterate` on it
   * fail as a second consumer (WSM-API-014). A box is not thenable and is not adopted.
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
   * The envelope is encoded as written, without passing through `fromMapping`. The two extension-point fixtures
   * depend on this: `fromMapping` drops unknown keys (WSM-FRM-001), so a frame built through it cannot carry the
   * unknown field whose tolerance is being asserted.
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
   * Every message both peers put on the wire, undecoded.
   *
   * Undecoded because `fromMapping` drops unknown keys (WSM-FRM-001), so a decoded `Frame` would not contain the
   * key `puts no limit and no version on the wire` looks for.
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

  /** Asserts absence from the whole of that peer's wire, not only from the frames seen so far. */
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
    // `deepEqual` rather than `toEqual`, because vitest's `toEqual` compares two `ArrayBuffer`s by byte *length*
    // and treats buffers of equal length as equal regardless of content. Under a binary codec a payload is an
    // `ArrayBuffer` (WSM-CDC-008), so `toEqual` would pass for a peer that delivered the right number of wrong
    // bytes.
    const value = await this.valueOf(spec.ref as string);
    expect(deepEqual(value, spec.value), `${String(spec.ref)} produced ${render(value)}`).toBe(true);
  }

  /**
   * What a peer **holds**, which assertions on frames cannot see (WSM-API-025).
   *
   * `expect_frame` checks that the headers were sent; this checks that they were exposed on the correct attribute.
   * `of` is required rather than defaulted, because the step exists to distinguish the two attributes, and a
   * fixture that omitted it would assert whichever one this runner defaulted to. For `reply` the wait is on
   * `replyHeadersArrived` rather than on a settle count: that event fires when the value stops changing, and a
   * fixture that slept instead would pass against a port that never fires it.
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
    // `deepEqual` for the same reason as in `expect_result`: a serialized error payload may contain bytes under a
    // binary codec, and `toEqual` does not distinguish two `ArrayBuffer`s of the same length.
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
 * A fixture written for another codec is **skipped with a reason**, never passed silently.
 *
 * `bytes-payload-under-binary-codec` is that fixture: bytes are a payload type only under a binary codec
 * (WSM-CDC-008), and a JSON-configured pass has nothing to assert about it. The reason is in the test's name, the
 * set of skipped fixtures is asserted by a test below, and the msgpack suite skips nothing, so no fixture is
 * skipped by every configuration (WSM-CDC-007).
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
        // Every stream id in the second pass has the other parity, so a fixture that contains a raw id, or a runner
        // that resolves an ordinal to a constant, fails in that pass only (WSM-TST-002).
        it.skip(`skips ${fileName} [${assignment}]: it ${reason}`, () => undefined);
        return;
      }
      it(`replays ${fileName} [${assignment}]`, async () => {
        await replay(fileName, swapped);
      });
    });
  });

  it('collects the corpus, and the same number of fixtures as the Python runner', () => {
    // An empty `conformance/sequences/` would pass without testing anything.
    expect(SEQUENCE_FILES.length).toBeGreaterThan(0);
    expect(SEQUENCE_FILES.length).toBe(EXPECTED_SEQUENCE_FIXTURES);
    // `muxws/conformance_test.py` asserts the same two numbers and reads them from this file.
  });

  it('runs every fixture under at least one configured codec', () => {
    // WSM-CDC-007. Asserted over the corpus rather than left to the two suites, because the failure is silent: a
    // `requires_codec` naming a codec no suite is configured with produces a fixture that every configuration
    // skips, which the report shows as covered although no library code runs it.
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
    // `{"$bytes": [...]}` is notation, never a payload shape (WSM-CDC-008). A runner that left it as an object
    // would send an ordinary object, both codecs would carry it, and the fixture would pass without testing bytes.
    // Both parts are asserted: the corpus contains the placeholder, and loading resolves it.
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
 * This suite covers what the codec's round-trip tests (`decode(encode(frame)) === frame` over a set of frames)
 * cannot, because only a *sequence* exercises it: the send path choosing `sendBytes` from `codec.binary`
 * (WSM-CDC-002), fragment sizes measured as buffer length rather than character count (WSM-FRG-003), and a payload
 * type JSON does not have surviving both directions (WSM-CDC-008). It also runs `bytes-payload-under-binary-codec`,
 * which every JSON-configured suite skips.
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
    // WSM-CON-031, WSM-CON-009, WSM-PKG-005: replay everything and inspect what was sent. There is no `settings`
    // frame and no announced limit of any kind; the `muxws.v1.` subprotocol prefix is the only version. There is no
    // extension advertisement either, because there is no `settings` frame to advertise in.
    // `muxws/conformance_test.py` asserts the same over the same corpus.
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
    // WSM-BPR-001: a separate test from the one above, although it replays the same corpus, because it is a
    // separate rule. Flow control is *reserved*, not only unannounced; implementing it requires removing this test.
    for (const [fileName, message] of await wireOfTheWholeCorpus()) {
      expect((JSON.parse(message) as Record<string, unknown>).type, `${fileName} sent a window_update`).not.toBe(
        'window_update',
      );
    }
  }, 20_000);

  it('skips exactly the fixtures written for another codec, and says why', () => {
    // Asserted, so that a skip applying to a second fixture is detected.
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
 * An equality match would invalidate every fixture whenever an optional field is added, contrary to what
 * WSM-FRM-001 requires of a receiver.
 */
function assertExpectedFrames(emitted: Frame[], fixture: InvalidFixture): void {
  if (fixture.expect_out.length === 0) {
    // An empty list is an assertion: `data-for-closed-id` states that a late frame below the high-water mark causes
    // **nothing** (WSM-STM-002), and an empty `expect_out` is how a fixture says so.
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
    it(`answers ${fileName} as declared, and keeps or drops the connection as declared`, async () => {
      // Four of these fixtures send messages that kill the connection, and a peer that receives one must log it
      // (§12), so the console line is expected. It is silenced for this describe only, as for the ILL-C cells in
      // `ts/stream.spec.ts`: a log from any other test indicates an unexpected failure.
      const level = logger.level;
      logger.level = 'silent';
      const fixture = JSON.parse(readFileSync(join(INVALID_DIR, fileName), 'utf-8')) as InvalidFixture;
      expect(fixture.name).toBe(fileName.replace(/\.json$/, ''));
      const codec = new JsonCodec();

      // The peer under test is an acceptor (even ids); the misbehaving remote is a dialer (odd ids). **Not**
      // role-swappable, unlike a sequence fixture: the fixtures are hand-written messages no API call on either
      // side can produce, and an ordinal cannot name a stream that was never opened.
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
        logger.level = level;
      }
    });
  });

  it('has a fixture for every one of the nine cases of WSM-TST-003, by name', () => {
    // Without this, a deleted fixture leaves a suite that passes.
    expect(INVALID_FILES.map((file) => file.replace(/\.json$/, ''))).toEqual(REQUIRED_INVALID_CASES);
    expect(INVALID_FILES.length).toBe(EXPECTED_INVALID_FIXTURES);
  });
});

// --------------------------------------------------------------------------- the freeze

describe('conformance/frames', () => {
  it('hashes to the one digest checked in to the Python runner', () => {
    // sha256 over `conformance/frames/`, sorted by file name, each file contributing `name`, a NUL, its bytes, a
    // NUL. There is one checked-in digest and both ports compute it from the bytes they read, so a port that reads
    // a different corpus fails here. A change to the JSON wire format requires changing that literal in
    // the same commit.
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
