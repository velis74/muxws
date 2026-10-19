/**
 * The TypeScript half of the live cross-language interop matrix.
 *
 * Run as an acceptor:         node --import tsx interop/runner.ts accept <port>
 * Accept on a socket file:    node --import tsx interop/runner.ts accept-unix <path>
 * Run the WSM-TST-004 script: node --import tsx interop/runner.ts dial ws://127.0.0.1:<port>
 *      ... over a socket file: node --import tsx interop/runner.ts dial ws+unix://<path>:/ws
 * Run the WSM-TST-005 script: node --import tsx interop/runner.ts reconnect-dial ws://127.0.0.1:<port>
 * Serve the corpus:           node --import tsx interop/runner.ts corpus-accept <control-port>
 * Conduct the corpus:         node --import tsx interop/runner.ts corpus-dial 127.0.0.1:<control-port>
 *
 * The same entry points exist in `interop/runner.py`, and `interop/drive.sh` pairs them in both
 * role assignments, so a rule the two ports implement differently is reported as a named failure,
 * not a hang.
 *
 * `dial` takes only a URL, so the Unix pairing needs no separate dialling mode: the same script runs
 * over a socket file with the URL `ws+unix:///path/to.sock:/route`. Both ports must split that URL
 * identically - pathname and search up to the **first** colon are the filesystem path, the rest is
 * the HTTP request target - and only a cross-language run tests that the two parsers agree.
 */

import { readdirSync, readFileSync } from 'node:fs';
import { createServer as createHttpServer, type IncomingMessage } from 'node:http';
import { createServer, connect as connectTcp, type Socket as TcpSocket } from 'node:net';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { WebSocket, WebSocketServer } from 'ws';

// Imported through the public entry points, as an application imports them. `ts/index.ts` registers
// the JSON codec (WSM-CDC-004), because a codec module must not register itself (WSM-CDC-014);
// importing `ts/codec` directly gives an empty registry. The dialer comes from `ts/node`, so the
// reconnect driver under test is the one a node application uses.
// `offer` builds the subprotocol list and is internal, so it is not exported from the package root
// (a consumer needs `PREFIX` only). The interop driver is in-repo and imports the module directly.
// `deepEqual`, not `assert.deepStrictEqual` or a custom comparison: a payload under a binary codec
// is an `ArrayBuffer` (WSM-CDC-008), and `conformance/README.md` names this comparison for that
// case. A comparison by byte *length* would treat buffers of equal length as equal regardless of
// content.
import { deepEqual } from '../ts/frames';
import {
  ABSENT,
  ConnectionGoingAway,
  ConnectionLost,
  type Codec,
  type Frame,
  getCodec,
  Peer,
  PREFIX,
  registerCodec,
  RemoteError,
  ResetCode,
  settings,
  StreamClosed,
  StreamRefused,
  StreamReset,
  StreamTimeout,
  type Stream,
  type StreamHandler,
  unjitteredDelay,
} from '../ts/index';
import { accept, connect, handleProtocols, refuseMismatchedUpgrade, Reconnect, WsSocket } from '../ts/node';
import type { ConnectionLoop } from '../ts/reconnect';
import { offer } from '../ts/subprotocol';

/** How many streams the acceptor pushes back when asked (WSM-TST-004's "server push"). */
const PUSH_COUNT = 3;
/** Rows an `export` produces, the last one carrying `end` and trailers. */
const EXPORT_ROWS = 5;
/**
 * A pause between export rows. Without it an acceptor could enqueue all five rows before any other
 * handler is scheduled, and the interleaving assertion below would depend on scheduling order.
 */
const EXPORT_GAP_MS = 10;
/** The cancelled-mid-flight producer's cadence. */
const DRIP_GAP_MS = 20;
/** Rows the `slow` stream produces; this stream must complete during the goaway drain. */
const SLOW_ROWS = 3;
const SLOW_GAP_MS = 50;

/**
 * The hello replayed on every connection the reconnect scenario's dialer makes (WSM-RCN-020/027).
 * Nested, so a replay that rebuilds the payload instead of replaying the captured bytes can produce
 * different bytes. Byte-for-byte the same value as in `interop/runner.py`, so both languages'
 * dialers produce the same hello.
 */
const HELLO_PAYLOAD = { who: 'interop', caps: ['a', 'b'], nested: { n: 1 } };
const HELLO_HEADERS: Record<string, unknown> = { session: 'interop-session' };

/**
 * Short delays, so the jittered retries of WSM-TST-005 complete within a CI job without a 30 s wait.
 * The jitter fraction keeps its default, because the scenario asserts that the delays are dispersed.
 */
const RECONNECT = new Reconnect({ initialDelayMs: 50, maxDelayMs: 500 });

function check(condition: boolean, what: string): void {
  if (!condition) {
    console.error(`interop FAILED: ${what}`);
    process.exit(1);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** One JSON object per line on stdout - what `interop/drive.sh` greps. */
function emit(fields: Record<string, unknown>): void {
  console.log(JSON.stringify(fields));
}

/**
 * Select and register the configured codec. This is the application's responsibility, not the
 * library's.
 *
 * WSM-CDC-013 forbids the library from dynamically importing or probing for a codec; an application
 * chooses which codec module to import. An unknown name fails at startup and never falls back to
 * JSON (WSM-INV-015).
 *
 * `settings.codec` is assigned here: `ts/conf.ts` reads `import.meta.env`, which Vite replaces at
 * build time and which does not exist under `node`. Without the assignment the msgpack job would run
 * on JSON (WSM-INV-015).
 */
async function configureCodec(): Promise<void> {
  const name = process.env.VITE_MUXWS_CODEC ?? process.env.MUXWS_CODEC ?? 'json';
  settings.codec = name;
  if (name === 'json') return; // `ts/index.ts` already registered it (WSM-CDC-004).
  if (name === 'msgpack') {
    const { MsgpackCodec } = await import('../ts/msgpack');
    registerCodec('msgpack', new MsgpackCodec());
    return;
  }
  console.error(`interop FAILED: the interop runner knows no codec named '${name}'`);
  process.exit(1);
}

function codec(): Codec {
  return getCodec(settings.codec);
}

/**
 * The bytes this frame goes out as, as hex.
 *
 * Hex, because a binary codec produces an ArrayBuffer and JSON produces text, and WSM-RCN-027
 * compares bytes.
 */
function encodingOf(frame: Frame): string {
  const encoded = codec().encode(frame);
  const bytes = typeof encoded === 'string' ? Buffer.from(encoded, 'utf8') : Buffer.from(encoded);
  return bytes.toString('hex');
}

// --------------------------------------------------------------------------- the acceptor

interface ConnectionState {
  dripSent: number;
  dripStopped: boolean;
  shutdown: Promise<void> | null;
}

/**
 * Every shape the two scenarios exercise, chosen by the opening payload.
 *
 * Built per connection, so it can use `peer` (a server push is an ordinary `open()` from the
 * acceptor) and so `state` is not shared between connections; the reconnect scenario makes two
 * connections to this process.
 */
function makeHandler(peer: Peer, state: ConnectionState): StreamHandler {
  return async (payload: any, stream: Stream) => {
    const action = (payload ?? {}).action as string | undefined;

    if (action === undefined) {
      // The hello. It has no marker on the wire (WSM-RCN-021); it is acknowledged by this handler
      // returning, which ends the stream (WSM-RCN-022/WSM-STM-035). Requiring an `action` here would
      // reset the hello stream and the reconnect scenario would fail on its first connection.
      if (JSON.stringify(payload) !== JSON.stringify(HELLO_PAYLOAD)) {
        throw new Error(`opened with neither an action nor the hello: ${JSON.stringify(payload)}`);
      }
    } else if (action === 'echo') {
      await stream.reply({ echo: payload.value });
    } else if (action === 'export') {
      for (let index = 0; index < EXPORT_ROWS - 1; index += 1) {
        await stream.send({ row: index });
        await sleep(EXPORT_GAP_MS);
      }
      await stream.end({ payload: { row: EXPORT_ROWS - 1 }, trailers: { rows: String(EXPORT_ROWS) } });
    } else if (action === 'push') {
      const count = Number(payload.count ?? PUSH_COUNT);
      for (let index = 0; index < count; index += 1) {
        await peer.notify({ event: 'tick', index });
      }
      await stream.reply({ pushed: count });
    } else if (action === 'drip') {
      await drip(stream, state);
    } else if (action === 'drip-report') {
      await stream.reply({ stopped: state.dripStopped, sent: state.dripSent });
    } else if (action === 'slow') {
      for (let index = 0; index < SLOW_ROWS - 1; index += 1) {
        await stream.send({ row: index });
        await sleep(SLOW_GAP_MS);
      }
      await stream.end({ payload: { row: SLOW_ROWS - 1 } });
    } else if (action === 'raise') {
      throw new Error('interop handler said no');
    } else if (action === 'forever') {
      await stream.closed;
    } else if (action === 'big') {
      // Larger than MAX_FRAME_BYTES once encoded, so the reply is fragmented while other streams are
      // in flight (WSM-FRG-019 round robin).
      await stream.reply({ blob: 'š'.repeat(40_000) });
    } else if (action === 'goaway') {
      // Started, not awaited: `peer.close()` drains the streams in flight, including this handler's
      // own stream, so awaiting it here would deadlock. The promise is stored in `state` so the drain
      // continues after this handler returns.
      state.shutdown = peer.close({ reason: 'interop goaway' });
      void state.shutdown.catch(() => undefined);
    } else {
      // Thrown, not exited: an unknown action fails only this stream, and the dialer receives it as
      // an application error (WSM-ERR-006) instead of a lost socket.
      throw new Error(`unknown action ${String(action)}`);
    }
  };
}

/**
 * Produce until the consumer cancels the stream; the remote half of the cancel-mid-flight assertion.
 *
 * Both exits are recorded. TypeScript cannot interrupt a running function, so WSM-ERR-013 gives the
 * handler `stream.signal`; a reset that arrives between two sends surfaces as
 * `StreamClosed`/`StreamReset` from the send. Which exit is taken depends on when the reset arrives.
 */
async function drip(stream: Stream, state: ConnectionState): Promise<void> {
  try {
    while (!stream.signal.aborted) {
      await stream.send({ row: state.dripSent });
      state.dripSent += 1;
      await sleep(DRIP_GAP_MS);
    }
    state.dripStopped = true;
  } catch (error: unknown) {
    if (!(error instanceof StreamClosed) && !(error instanceof StreamReset)) throw error;
    state.dripStopped = true;
  }
}

/**
 * One accepted connection, over a TCP socket or a socket file.
 *
 * `acceptForever` and `acceptForeverOnSocketFile` both register this listener, so the two acceptors
 * differ only in how they bind and the Unix pairing tests the same transport-agnostic path
 * (WSM-API-021).
 */
function serveConnection(socket: WebSocket, request: IncomingMessage): void {
  void (async () => {
    // The HTTP request target of this connection. The socket file is the address and neither
    // acceptor routes on the target, so a dialer that sent `/` or dropped the query string would
    // pass every assertion in the script. The driver compares this line with the route it put in the
    // `ws+unix://<path>:/route` URL.
    emit({ event: 'accepted', target: request.url ?? null });
    const peer = await accept(socket);
    const state: ConnectionState = { dripSent: 0, dripStopped: false, shutdown: null };
    let seenOpen = false;

    // Report the first inbound `open` of this connection, re-encoded. The driver compares this line
    // across the two acceptor processes, which checks WSM-TST-005's byte-identical hello on the
    // accepting port. The hello is the first open because nothing may precede it (WSM-RCN-023).
    peer.onFrame((direction, frame) => {
      if (direction === 'rx' && frame.type === 'open' && !seenOpen) {
        seenOpen = true;
        emit({ event: 'first-open', encoding: encodingOf(frame) });
      }
    });
    peer.onStream(makeHandler(peer, state));
    // A dialer that disconnects makes `serve()` reject with `ConnectionLost`; that ends this
    // connection, not the acceptor process.
    await peer.serve().catch((error: unknown) => {
      if (!(error instanceof ConnectionLost)) throw error;
    });
  })();
}

async function acceptForever(port: number): Promise<void> {
  // Both hooks, because `handleProtocols` alone answers 101 to a codec this acceptor does not
  // support (WSM-CDC-022). The refusal must match the documented acceptor setup, because this is
  // where a dialer of the other language is refused.
  const server = refuseMismatchedUpgrade(new WebSocketServer({ port, host: '127.0.0.1', handleProtocols }));

  server.on('connection', serveConnection);

  await new Promise<void>((resolve) => server.on('listening', () => resolve()));
  const address = server.address();
  const bound = typeof address === 'object' && address !== null ? address.port : port;
  emit({ role: 'ts-acceptor', port: bound, codec: settings.codec });
  await new Promise(() => undefined);
}

/**
 * The same acceptor on a socket **file**, for the Unix half of WSM-TST-004.
 *
 * `WebSocketServer({ port })` creates its own HTTP server and cannot listen on a path, so an HTTP
 * server is created here and attached with `{ server }`. `refuseMismatchedUpgrade` wraps
 * `shouldHandle`, which the attached form also calls, so a dialer offering an unsupported codec gets
 * the same HTTP 400 as over TCP and must raise `CodecMismatch` (WSM-CDC-022/024). The driver runs the
 * unmodified WSM-TST-004 script over this transport.
 *
 * The driver's readiness signal is the `path=` line below, not a `port=` line. A socket file exists
 * between `bind` and `listen`, so waiting for the file would race the listen.
 */
async function acceptForeverOnSocketFile(path: string): Promise<void> {
  // Checked by `platform` before binding: on Windows `listen(path)` opens a named pipe for some path
  // forms and fails with an unspecific errno for others.
  if (process.platform === 'win32') {
    throw new Error('this platform has no AF_UNIX, so the unix scenario cannot run here');
  }
  const httpServer = createHttpServer();
  const server = refuseMismatchedUpgrade(new WebSocketServer({ server: httpServer, handleProtocols }));

  server.on('connection', serveConnection);

  // Waits on the HTTP server, which binds the path; a `WebSocketServer` attached to a server has no
  // `listen()` of its own.
  await new Promise<void>((resolve) => httpServer.listen(path, () => resolve()));
  emit({ role: 'ts-acceptor', path, codec: settings.codec });
  await new Promise(() => undefined);
}

// --------------------------------------------------------------------------- WSM-TST-004

type JournalEntry = { direction: 'tx' | 'rx'; type: string; stream: number | null };

/**
 * Every frame the peer saw, in order.
 *
 * Interleaving is asserted on the wire order, because two calls that overlap in JavaScript can still
 * be serialised on the socket.
 */
class Journal {
  readonly entries: JournalEntry[] = [];

  goawayFrame: Frame | null = null;

  private readonly gate: { promise: Promise<void>; resolve: () => void };

  constructor() {
    let resolve!: () => void;
    const promise = new Promise<void>((settle) => {
      resolve = settle;
    });
    this.gate = { promise, resolve };
  }

  record = (direction: 'tx' | 'rx', frame: Frame): void => {
    this.entries.push({ direction, type: frame.type, stream: frame.stream ?? null });
    if (direction === 'rx' && frame.type === 'goaway' && this.goawayFrame === null) {
      this.goawayFrame = frame;
      this.gate.resolve();
    }
  };

  goawayArrived(timeoutMs: number): Promise<void> {
    return withDeadline(this.gate.promise, timeoutMs, 'no goaway arrived');
  }
}

function withDeadline<T>(promise: Promise<T>, timeoutMs: number, what: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`interop timed out after ${timeoutMs}ms: ${what}`)), timeoutMs);
  });
  return Promise.race([promise, deadline]).finally(() => clearTimeout(timer)) as Promise<T>;
}

/**
 * Concurrent unary requests interleaved with a streaming export and a server push, one stream
 * cancelled mid-flight, then a `goaway` shutdown (WSM-TST-004).
 */
async function runTst004(peer: Peer, journal: Journal, pushes: any[], label: string): Promise<void> {
  const exportRows: unknown[] = [];
  let exportId = 0;

  const consumeExport = async (): Promise<void> => {
    const stream = peer.open({ action: 'export' });
    exportId = stream.id;
    for await (const row of stream) exportRows.push(row);
  };

  // Six streams open at once through `Promise.all`. The export and the fragmented `big` reply make
  // the concurrency visible on the wire.
  const [echoed1, echoed2, echoed3, big, , pushed] = await Promise.all([
    peer.request({ action: 'echo', value: 1 }),
    peer.request({ action: 'echo', value: 2 }),
    peer.request({ action: 'echo', value: 3 }),
    peer.request<{ blob: string }>({ action: 'big' }),
    consumeExport(),
    peer.request({ action: 'push', count: PUSH_COUNT }),
  ]);

  const echoes = JSON.stringify([echoed1, echoed2, echoed3]);
  check(
    echoes === JSON.stringify([{ echo: 1 }, { echo: 2 }, { echo: 3 }]),
    `${label}: unary answers crossed: ${echoes}`,
  );
  check(big.blob.length === 40_000, `${label}: fragmented payload came back as ${big.blob.length} chars`);
  check(big.blob[0] === 'š', `${label}: fragmented payload lost its non-ASCII content`);
  const wantedRows = JSON.stringify([0, 1, 2, 3, 4].map((row) => ({ row })));
  check(JSON.stringify(exportRows) === wantedRows, `${label}: export gave ${JSON.stringify(exportRows)}`);
  check(JSON.stringify(pushed) === JSON.stringify({ pushed: PUSH_COUNT }), `${label}: push answered ${String(pushed)}`);
  // Compared as a set: v1 does not order delivery across streams (WSM-STM-001), so the arrival order
  // of the three pushed streams is unspecified.
  const indices = pushes.map((push) => push.index as number).sort((left, right) => left - right);
  const events = new Set(pushes.map((push) => push.event as string));
  check(
    JSON.stringify(indices) === JSON.stringify([0, 1, 2]) && events.size === 1 && events.has('tick'),
    `${label}: the server pushed ${JSON.stringify(pushes)}`,
  );

  // Interleaving, asserted on the wire: between the export's first and last **inbound** frame there
  // must be an inbound frame of another stream. Outbound frames are not counted, because the six
  // opens are sent together regardless of how the acceptor schedules its replies.
  const inbound = journal.entries.filter((entry) => entry.direction === 'rx');
  const positions = inbound.flatMap((entry, index) => (entry.stream === exportId ? [index] : []));
  check(positions.length > 0, `${label}: no inbound frame for the export stream ${exportId}`);
  const foreign = inbound
    .slice(positions[0], positions[positions.length - 1])
    .filter((entry) => entry.stream !== exportId);
  check(foreign.length > 0, `${label}: the export was answered without a single other stream interleaved`);

  await runApplicationError(peer, label);
  await runCancelMidFlight(peer, label);
  await runGoawayShutdown(peer, journal, label);

  emit({ role: label, ok: true });
}

/** A handler that throws becomes `reset(APPLICATION_ERROR)` carrying the serialized payload. */
async function runApplicationError(peer: Peer, label: string): Promise<void> {
  let raised: RemoteError | null = null;
  await peer.request({ action: 'raise' }).catch((error: unknown) => {
    raised = error as RemoteError;
  });
  check(raised !== null, `${label}: a throwing handler did not produce an error`);

  const payload = raised!.payload as { type?: unknown; message?: unknown } | null;
  // Only `message` is compared. WSM-ERR-006's default serializer reports the remote's exception
  // class name in `type`, which is 'ValueError' from a Python acceptor and 'Error' from a
  // TypeScript one.
  check(payload?.message === 'interop handler said no', `${label}: application error ${JSON.stringify(payload)}`);
  check(
    typeof payload?.type === 'string' && payload.type.length > 0,
    `${label}: the application error carried no type name: ${JSON.stringify(payload)}`,
  );
}

/** One stream cancelled mid-flight; the remote producer must stop (WSM-TST-004). */
async function runCancelMidFlight(peer: Peer, label: string): Promise<void> {
  const dripStream = peer.open({ action: 'drip' });
  let seen = 0;
  for await (const _row of dripStream) {
    seen += 1;
    if (seen === 3) break;
  }
  check(seen === 3, `${label}: the drip stream produced only ${seen} rows before the cancel`);

  await dripStream.cancel('interop cancel mid-flight');
  check(dripStream.signal.aborted, `${label}: cancel did not close the stream locally`);

  // Polled until a deadline instead of a fixed sleep. A producer that does not see the reset never
  // reports `stopped`, and this fails at the deadline.
  const deadline = Date.now() + 5000;
  let report: { stopped?: boolean; sent?: number } = {};
  while (Date.now() < deadline) {
    report = await peer.request({ action: 'drip-report' });
    if (report.stopped === true) break;
    await sleep(DRIP_GAP_MS);
  }
  check(report.stopped === true, `${label}: reset(CANCELLED) did not stop the producer: ${JSON.stringify(report)}`);
  check(
    (report.sent ?? 0) >= seen,
    `${label}: the producer reports fewer rows than arrived: ${JSON.stringify(report)}`,
  );
}

/** A `goaway` shutdown: streams at or below `last_stream` drain, later opens are refused. */
async function runGoawayShutdown(peer: Peer, journal: Journal, label: string): Promise<void> {
  const slow = peer.open({ action: 'slow' });
  const rows: unknown[] = [];
  const draining = (async () => {
    for await (const row of slow) rows.push(row);
  })();

  await peer.notify({ action: 'goaway' });
  // Waits for the goaway frame instead of sleeping: the assertions below are about behaviour after
  // the goaway arrives.
  await journal.goawayArrived(5000);
  const frame = journal.goawayFrame!;
  check(
    frame.last_stream !== null && frame.last_stream !== undefined && frame.last_stream >= slow.id,
    `${label}: goaway last_stream ${String(frame.last_stream)} excludes the in-flight stream ${slow.id}`,
  );

  let refused: unknown = null;
  try {
    peer.open({ action: 'echo', value: 99 });
  } catch (error: unknown) {
    refused = error;
  }
  check(
    refused instanceof ConnectionGoingAway,
    `${label}: an open after goaway was not refused locally (WSM-CON-021): ${String(refused)}`,
  );

  await withDeadline(draining, 10_000, 'the drained stream never ended');
  check(
    JSON.stringify(rows) === JSON.stringify([0, 1, 2].map((row) => ({ row }))),
    `${label}: the stream inside last_stream did not drain to completion: ${JSON.stringify(rows)}`,
  );
}

// --------------------------------------------------------------------------- WSM-TST-005

/** Kill the acceptor process with streams open, restart it, and check the peer against WSM-TST-005. */
async function runTst005(url: string, label: string): Promise<void> {
  const hellos: string[] = [];
  const reconnects: [number, number][] = [];
  const closes: unknown[] = [];

  const peer = await connect(url, {
    hello: HELLO_PAYLOAD,
    helloHeaders: HELLO_HEADERS,
    reconnect: RECONNECT,
    onClose: (reason) => closes.push(reason),
    // The second number is the ordering assertion: when `onReconnect` fires, the replayed hello must
    // already be sent and acknowledged (WSM-RCN-023/030). A peer that fired `onReconnect` first would
    // record zero here.
    onReconnect: (attempt) => reconnects.push([attempt, hellos.length]),
  });

  // Capture the encoding of every hello this peer sends after the first connection. Stream 1 is
  // always the hello: the id space restarts at 1 on every socket, and WSM-RCN-023 forbids anything
  // preceding it. The first connection's hello is sent inside `connect()`, before application code
  // can register a handler, so the acceptor records the first-connection encoding instead (see
  // `serveConnection`).
  peer.onFrame((direction, frame) => {
    if (direction === 'tx' && frame.type === 'open' && frame.stream === 1) hellos.push(encodingOf(frame));
  });

  const held = [peer.open({ action: 'forever' }), peer.open({ action: 'forever' })];
  const outcomes = held.map(async (stream) => stream.then(() => null).catch((error: unknown) => error));
  // The driver kills the acceptor when it reads this line, so the streams above are opened first.
  emit({ role: label, event: 'streams-open', streams: held.map((stream) => stream.id) });

  // Awaited with a deadline. WSM-RCN-041 forbids leaving these streams pending after the socket is
  // lost; with the deadline that defect is reported as a failure instead of a hung job.
  const raised = await withDeadline(Promise.all(outcomes), 60_000, 'the streams open at the kill never settled');
  raised.forEach((outcome, index) => {
    check(
      outcome instanceof ConnectionLost,
      `${label}: stream ${held[index].id} was open when the acceptor died and raised ` +
        `${String(outcome)}, not ConnectionLost (WSM-TST-005)`,
    );
  });

  const loop = peer.connectionLoop as unknown as ConnectionLoop;
  const deadline = Date.now() + 60_000;
  while (reconnects.length === 0 && Date.now() < deadline) await sleep(50);
  // `delays`, not `attempts`: the attempt counter resets when a connection is established
  // (WSM-RCN-004), so it reads zero after a successful reconnect.
  check(
    reconnects.length > 0,
    `${label}: the dialer never announced a reconnect; it waited ${JSON.stringify(loop.delays)}`,
  );

  check(
    JSON.stringify(reconnects) === JSON.stringify([[1, 1]]),
    `${label}: onReconnect fired ${JSON.stringify(reconnects)}; WSM-TST-005 wants exactly one, ` +
      'after the replayed hello was acknowledged',
  );
  const expected = encodingOf({ type: 'open', stream: 1, payload: HELLO_PAYLOAD, headers: HELLO_HEADERS, end: true });
  check(
    JSON.stringify(hellos) === JSON.stringify([expected]),
    `${label}: the replayed hello was not the captured one, byte for byte (WSM-RCN-027): ${JSON.stringify(hellos)}`,
  );
  checkJitterDispersed(loop.delays, label);

  // A request on the new connection checks that the acceptor accepted the replayed hello and kept
  // the connection open.
  const echoed = await peer.request({ action: 'echo', value: 7 });
  check(JSON.stringify(echoed) === JSON.stringify({ echo: 7 }), `${label}: reconnected peer said ${String(echoed)}`);
  check(closes.length >= 1, `${label}: losing the socket fired no onClose`);

  const attempts = loop.delays.length;
  // Closed explicitly. The peer holds a heartbeat timer and a supervisor task that keep the event
  // loop alive, and `close()` is the only way to stop the reconnect helper (WSM-RCN-040); this peer
  // retries without limit.
  await peer.close({ reason: 'interop reconnect scenario complete' });
  emit({ role: label, ok: true, attempts, reconnections: loop.reconnections });
}

/**
 * WSM-RCN-002: jitter is applied to every delay, including the capped ones.
 *
 * Asserted as dispersion, not as a value. The check looks at the delays whose *unjittered* schedule
 * has reached `maxDelayMs`: without jitter they would all equal `maxDelayMs`.
 */
function checkJitterDispersed(delays: readonly number[], label: string): void {
  const capped = delays.filter((_delay, attempt) => unjitteredDelay(attempt, RECONNECT) === RECONNECT.maxDelayMs);
  check(
    capped.length >= 2,
    `${label}: only ${capped.length} capped delays in ${JSON.stringify(delays)}; the acceptor was ` +
      'not held down long enough for the schedule to reach maxDelayMs twice',
  );
  check(
    new Set(capped).size > 1,
    `${label}: every capped delay was ${capped[0]}; the cap was applied without jitter (WSM-RCN-002)`,
  );
  const lowest = RECONNECT.maxDelayMs * (1 - RECONNECT.jitter);
  const highest = RECONNECT.maxDelayMs * (1 + RECONNECT.jitter);
  const outside = capped.filter((delay) => delay < lowest || delay > highest);
  check(
    outside.length === 0,
    `${label}: capped delays ${JSON.stringify(outside)} fall outside +/-${RECONNECT.jitter} of the cap`,
  );
}

// --------------------------------------------------------------------------- WSM-CDC-007: the corpus
//
// The cross-language part of WSM-CDC-007: "a Python peer and a TypeScript peer, both configured with
// that codec, running the sequence corpus". `ts/conformance.spec.ts` and `muxws/conformance_test.py`
// replay `conformance/sequences/` with both peers in one process and one language.
//
// A sequence fixture scripts **both** peers as one ordered list of steps; here the peers are two
// processes in two languages. The dialer process conducts: it reads the script, executes the steps
// whose `peer` is its own role, and sends every other step over a control channel to the acceptor
// process, which executes it through the same `Side` class and replies. The script stays one totally
// ordered sequence, as in the in-process runners, and the whole corpus runs.
//
// The control channel carries only a step **index** and the ordinal -> stream id table. Both
// processes load the fixture from disk, so a `$bytes` placeholder (`conformance/README.md`) is
// resolved from the same file in each process and never needs a JSON encoding for bytes.

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');

/** Read from disk by both processes. Not sent over the control channel; see above. */
const SEQUENCES_DIR = join(ROOT, 'conformance', 'sequences');

/**
 * The file that declares the expected fixture count. `ts/conformance.spec.ts` declares it and
 * `muxws/conformance_test.py` checks against that file; this driver reads the same literal.
 */
const FIXTURE_COUNT_SOURCE = join(ROOT, 'ts', 'conformance.spec.ts');

/**
 * Mirrors `runs every fixture under some configured codec, skipping none everywhere`. The corpus's
 * `requires_codec` declarations must equal this list, so a fixture that gains a declaration fails the
 * run instead of being skipped silently under the other codec.
 */
const CODEC_SPECIFIC_FIXTURES: Record<string, string> = { 'bytes-payload-under-binary-codec': 'msgpack' };

/**
 * Every step kind and every `call` of `conformance/README.md`. A fixture that uses anything else is
 * reported as **unsupported by name** and fails the run; it is not skipped.
 */
const STEP_KINDS = [
  'settle',
  'call',
  'inject',
  'expect_frame',
  'expect_no_frame',
  'expect_result',
  'expect_headers',
  'expect_error',
  'expect_closed',
];
const IMPLEMENTED_CALLS = [
  'open',
  'request',
  'notify',
  'send',
  'send_headers',
  'end',
  'reply',
  'cancel',
  'iterate',
  'close',
  'await_close',
];
/** The three calls that append an ordinal, in step order (`conformance/README.md`). */
const ALLOCATING_CALLS = ['open', 'request', 'notify'];

/** `expect_error` names a class, because the class is the part WSM-ERR-004 fixes across the ports. */
const CORPUS_ERROR_CLASSES: Record<string, (error: unknown) => boolean> = {
  ConnectionLost: (error) => error instanceof ConnectionLost,
  RemoteError: (error) => error instanceof RemoteError,
  StreamRefused: (error) => error instanceof StreamRefused,
  StreamReset: (error) => error instanceof StreamReset,
  StreamTimeout: (error) => error instanceof StreamTimeout,
};

/**
 * The envelope defaults, mirroring the module-private `FIELD_DEFAULTS` of `ts/frames.ts`.
 *
 * A matcher reads a field the frame omits as that field's default, as Python's dataclass does.
 * Reading it as `undefined` would make `{"end": false}` fail here and pass in the Python runner.
 *
 * Every frame the TypeScript peer sends carries the keys the corpus matchers read, so the table does
 * not change any current outcome. It keeps the matcher equivalent to the Python side, where
 * `getattr(frame, key, None)` returns the dataclass default whether or not the peer sent the key.
 */
const CORPUS_FIELD_DEFAULTS: Record<string, unknown> = {
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

/**
 * Wall-clock limit on one waiting step. Larger than the in-process runner's 2 s because every step
 * here crosses a socket and a process boundary. A stalled fixture fails by name instead of hanging
 * the job.
 */
const CORPUS_STEP_TIMEOUT_MS = 5000;

/** One quiescence window, and how many of them a `settle` may take before it gives up. */
const QUIET_MS = 30;
const QUIET_ROUNDS = 60;

/** Ceiling on one control-channel round trip, so two processes cannot wait for each other forever. */
const CONTROL_TIMEOUT_MS = 60_000;

/** How long `streamFor` waits for an *inbound* stream to be delivered. Same value as in `interop/runner.py`. */
const STREAM_ARRIVAL_TURNS = 400;

type Who = 'dialer' | 'acceptor';
type Step = Record<string, any>;

interface Fixture {
  name: string;
  max_frame_bytes?: number;
  max_concurrent_streams?: number;
  requires_codec?: string;
  steps: Step[];
}

/** A step that did not hold. The fixture name and step index are attached where it is caught. */
class StepError extends Error {}

/** One turn of the event loop; a macrotask, so the microtask queue drains with it. */
function turn(): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, 0);
  });
}

/**
 * Resolve every `{"$bytes": [...]}` placeholder into the `ArrayBuffer` those integers spell.
 *
 * The same function as in `ts/conformance.spec.ts`; it runs in **both** processes. JSON has no byte
 * type, so an unresolved placeholder would be sent as an ordinary object, which msgpack also accepts,
 * and `bytes-payload-under-binary-codec` would not test bytes (WSM-CDC-008).
 */
function substituteBytes(value: unknown): unknown {
  if (Array.isArray(value)) return value.map((item) => substituteBytes(item));
  if (typeof value !== 'object' || value === null) return value;
  const keys = Object.keys(value as object);
  if (keys.length === 1 && keys[0] === '$bytes') return new Uint8Array((value as { $bytes: number[] }).$bytes).buffer;
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>).map(([key, item]) => [key, substituteBytes(item)]),
  );
}

function loadFixture(name: string): Fixture {
  return substituteBytes(JSON.parse(readFileSync(join(SEQUENCES_DIR, `${name}.json`), 'utf-8'))) as Fixture;
}

/**
 * `max_frame_bytes` / `max_concurrent_streams` are instructions to the runner (WSM-TST-002).
 *
 * Both peers are constructed with them, and neither is encoded into any frame (WSM-CON-031).
 */
function peerOptions(fixture: Fixture): { maxFrameBytes?: number; maxConcurrentStreams?: number } {
  return { maxFrameBytes: fixture.max_frame_bytes, maxConcurrentStreams: fixture.max_concurrent_streams };
}

/** Why this driver cannot replay the fixture, or null. An unsupported fixture is reported, not skipped. */
function unsupportedReason(fixture: Fixture): string | null {
  for (const [index, step] of fixture.steps.entries()) {
    const kind = STEP_KINDS.find((key) => key in step);
    if (kind === undefined) return `step ${index} has no step kind this driver recognises`;
    if (kind === 'call' && !IMPLEMENTED_CALLS.includes(String(step.call))) {
      return `step ${index} calls '${String(step.call)}', which this driver does not implement`;
    }
  }
  return null;
}

/**
 * One process's half of a cross-language replay: one peer, one journal, one role in the script.
 *
 * Both processes run this class, so a step has the same meaning on either end of the socket, as
 * `conformance/README.md` requires. The conductor runs the steps whose `peer` is its own role and
 * sends the rest to the other process over the control channel.
 */
class Side {
  /**
   * This peer's own wire, in order. Taken from `onFrame`, which reports **wire** frames on the way
   * out - after fragmentation - so `small-frame-overtakes-a-fragmented-payload` can assert on
   * individual fragments (WSM-OBS-003, WSM-FRG-019).
   */
  readonly sent: Frame[] = [];

  /** Every frame in either direction. Only `settleAcross` reads it, as a quiescence signal. */
  frames = 0;

  cursor = 0;

  socketClosed = false;

  /**
   * Set from the conductor's table on every step; this side keeps no ordinal list of its own, so
   * there is one ordinal table per run.
   */
  private ordinals: number[] = [];

  private readonly byId = new Map<number, Stream>();

  private readonly refs = new Map<string, Stream>();

  private readonly tasks = new Map<string, Promise<unknown>>();

  private closing: Promise<void> | null = null;

  constructor(
    readonly role: Who,
    readonly peer: Peer,
    readonly socket: WsSocket,
    readonly codec: Codec,
  ) {
    peer.onFrame((direction, frame) => {
      this.frames += 1;
      if (direction === 'tx') this.sent.push(frame);
    });
    peer.onStream(async (payload: any, stream: Stream) => {
      // Record the inbound stream, then hold it open. WSM-STM-035 ends a stream when its handler
      // returns, so returning here would close every inbound stream before the script can `reply`.
      this.byId.set(stream.id, stream);
      const asked = payload as { handler?: string; message?: string } | null;
      // `Error`, not a subclass: WSM-ERR-006's serializer sends the class name, and the Python runner
      // raises a class named `Error`, so `handler-raises-produces-application-error` sees the same
      // payload from both languages.
      if (asked !== null && typeof asked === 'object' && asked.handler === 'raise') {
        throw new Error(asked.message ?? 'the handler raised');
      }
      await stream.closed;
    });
  }

  // ------------------------------------------------------------------ the script

  /**
   * Run one step **as this side**, and return the stream id it allocated, if any.
   *
   * Routing has already happened by the time a step arrives here, so `step.peer` is not read: the
   * conductor decides who runs what, and for `inject` that decision is inverted (see `ownerOf`).
   */
  async execute(step: Step, ordinals: number[]): Promise<number | null> {
    this.ordinals = [...ordinals];
    if ('call' in step) return this.doCall(step);
    if ('inject' in step) await this.writeRaw(step.inject as Record<string, unknown>);
    else if ('expect_frame' in step) this.expectFrame(step.expect_frame as Record<string, unknown>);
    else if ('expect_no_frame' in step) this.expectNoFrame(step.expect_no_frame as Record<string, unknown>);
    else if ('expect_result' in step) await this.expectResult(step.expect_result as Step);
    else if ('expect_headers' in step) await this.expectHeaders(step.expect_headers as Step);
    else if ('expect_error' in step) await this.expectError(step.expect_error as Step);
    else if ('expect_closed' in step) await this.expectClosed(step.expect_closed as Step);
    else throw new StepError(`no step kind in ${JSON.stringify(step)}`);
    return null;
  }

  private async doCall(step: Step): Promise<number | null> {
    const calls: Record<string, (s: Step) => Promise<number | null>> = {
      open: (s) => this.corpusOpen(s),
      request: (s) => this.corpusRequest(s),
      notify: (s) => this.corpusNotify(s),
      send: (s) => this.corpusSend(s),
      send_headers: (s) => this.corpusSendHeaders(s),
      end: (s) => this.corpusEnd(s),
      reply: (s) => this.corpusReply(s),
      cancel: (s) => this.corpusCancel(s),
      iterate: (s) => this.corpusIterate(s),
      close: (s) => this.corpusClose(s),
      await_close: (s) => this.corpusAwaitClose(s),
    };
    const handler = calls[String(step.call)];
    if (handler === undefined) throw new StepError(`the interop driver does not implement '${String(step.call)}'`);

    const expected = step.raises as string | undefined;
    if (expected === undefined) return handler(step);
    if (!(expected in CORPUS_ERROR_CLASSES)) throw new StepError(`'raises' names an unknown class '${expected}'`);
    // `raises` states that the *call itself* fails, for example a send on a stream the consumer
    // cancelled. A frame matcher cannot express this: a peer that reset the stream and kept producing
    // sends the same frames (WSM-ERR-009).
    let caught: unknown = NOTHING_RAISED;
    try {
      await handler(step);
    } catch (error) {
      caught = error;
    }
    if (caught === NOTHING_RAISED) throw new StepError(`${String(step.call)} was expected to throw ${expected}`);
    if (!CORPUS_ERROR_CLASSES[expected](caught)) {
      throw new StepError(`${String(step.call)} threw ${String(caught)}, not ${expected}`);
    }
    return null;
  }

  private async corpusOpen(step: Step): Promise<number> {
    const stream = this.peer.open(step.payload ?? null, { headers: step.headers, end: step.end === true });
    return this.adopt(step, stream);
  }

  /**
   * `peer.request(...)`, started and not awaited (WSM-API-006) - the steps after it are the
   * assertions about the frames it produced.
   */
  private async corpusRequest(step: Step): Promise<number> {
    const before = new Set(this.peer.streams.keys());
    const task = this.peer.request(step.payload ?? null, {
      headers: step.headers,
      // Milliseconds here and in the corpus; the Python runner converts to seconds (WSM-CON-012).
      timeoutMs: step.timeout_ms as number | undefined,
    });
    void task.catch(() => undefined);
    return this.adopt(step, (await this.discover(before)).stream, task);
  }

  private async corpusNotify(step: Step): Promise<number> {
    const before = new Set(this.peer.streams.keys());
    await this.peer.notify(step.payload ?? null, { headers: step.headers });
    return this.adopt(step, (await this.discover(before)).stream);
  }

  private async corpusSend(step: Step): Promise<null> {
    const { stream } = await this.streamFor(Number(step.stream_ref));
    await stream.send(step.payload ?? null, {
      end: step.end === true,
      headers: step.headers as Record<string, unknown> | undefined,
    });
    return null;
  }

  /** The answering side's leading headers, on a frame with no payload at all (WSM-API-024). */
  private async corpusSendHeaders(step: Step): Promise<null> {
    const { stream } = await this.streamFor(Number(step.stream_ref));
    await stream.sendHeaders(step.headers as Record<string, unknown>);
    return null;
  }

  private async corpusEnd(step: Step): Promise<null> {
    const { stream } = await this.streamFor(Number(step.stream_ref));
    // An absent `payload` key is `ABSENT`, not `null`: they are different frames.
    await stream.end({
      payload: 'payload' in step ? step.payload : undefined,
      trailers: step.trailers,
      headers: step.headers as Record<string, unknown> | undefined,
    });
    return null;
  }

  private async corpusReply(step: Step): Promise<null> {
    const { stream } = await this.streamFor(Number(step.stream_ref));
    await stream.reply(step.payload ?? null, {
      trailers: step.trailers,
      headers: step.headers as Record<string, unknown> | undefined,
    });
    return null;
  }

  private async corpusCancel(step: Step): Promise<null> {
    const { stream } = await this.streamFor(Number(step.stream_ref));
    await stream.cancel(step.reason as string | undefined);
    return null;
  }

  private async corpusIterate(step: Step): Promise<null> {
    const { stream } = await this.streamFor(Number(step.stream_ref));
    const collect = async (): Promise<unknown[]> => {
      const items: unknown[] = [];
      for await (const item of stream) items.push(item);
      return items;
    };
    const task = collect();
    void task.catch(() => undefined);
    this.tasks.set(String(step.as), task);
    return null;
  }

  private async corpusClose(step: Step): Promise<null> {
    // Started, not awaited: `close()` sends `goaway` and then drains, and the steps after this one
    // run during the drain window (WSM-CON-025).
    const closing = this.peer.close({
      code: (step.code ?? ResetCode.NO_ERROR) as ResetCode,
      reason: step.reason as string | undefined,
      drainMs: step.drain_ms as number | undefined,
    });
    void closing.catch(() => undefined);
    this.closing = closing;
    return null;
  }

  private async corpusAwaitClose(_step: Step): Promise<null> {
    if (this.closing === null) throw new StepError(`await_close: ${this.role} has no close() in flight`);
    await withDeadline(this.closing, CORPUS_STEP_TIMEOUT_MS, `${this.role}.close()`);
    return null;
  }

  private adopt(step: Step, stream: Stream, task?: Promise<unknown>): number {
    this.byId.set(stream.id, stream);
    if (typeof step.as === 'string') {
      if (task === undefined) this.refs.set(step.as, stream);
      else this.tasks.set(step.as, task);
    }
    return stream.id;
  }

  /**
   * The stream a call opened without returning it, found through the public map.
   *
   * The stream is returned **boxed**. `Stream` is a thenable, so a promise resolved with one adopts
   * it: `await discover(...)` would await the stream itself, claiming it for `await`, and every later
   * `iterate` on it would fail as a second consumer (WSM-API-014).
   */
  private async discover(before: Set<number>): Promise<{ stream: Stream }> {
    for (let round = 0; round < STREAM_ARRIVAL_TURNS; round += 1) {
      for (const [streamId, stream] of this.peer.streams) {
        if (!before.has(streamId)) return { stream };
      }
      await turn();
    }
    throw new StepError('the call opened no stream');
  }

  /**
   * Put one hand-written envelope on this side's socket, bypassing the writer.
   *
   * This is `inject` executed by the sending side. In-process, a runner passes the message directly
   * to the receiving peer's socket; across two processes only the remote can deliver a message *to*
   * a peer, so this side sends it. The envelope does not pass through `fromMapping`, which drops
   * unknown keys (WSM-FRM-001); the fixtures assert that an unknown field and an unknown frame type
   * are tolerated.
   *
   * The writer is bypassed because the public API has no way to send an exact set of keys. Nothing
   * else is in flight in the two fixtures that use this, and a WebSocket message is atomic, so it
   * cannot interleave with a fragment.
   */
  private async writeRaw(envelope: Record<string, unknown>): Promise<void> {
    const message = this.codec.encodePayload(this.resolve(envelope));
    if (this.codec.binary) await this.socket.sendBytes(message as ArrayBuffer);
    else await this.socket.sendText(message as string);
  }

  // ------------------------------------------------------------------ resolution

  private idOf(ordinal: number): number {
    if (!Number.isInteger(ordinal) || ordinal < 1 || ordinal > this.ordinals.length) {
      throw new StepError(`stream_ref ${ordinal} names a stream this script has not opened`);
    }
    return this.ordinals[ordinal - 1];
  }

  /**
   * Boxed for the same reason as `discover`: `Stream` is a thenable, so a promise resolved with one
   * **adopts** it. Unboxed, every `await streamFor(...)` would await the stream itself, claim it for
   * `await`, and a later `iterate` on it would fail as a second consumer (WSM-API-014).
   */
  private async streamFor(ordinal: number): Promise<{ stream: Stream }> {
    const streamId = this.idOf(ordinal);
    for (let round = 0; round < STREAM_ARRIVAL_TURNS; round += 1) {
      const stream = this.byId.get(streamId);
      if (stream !== undefined) return { stream };
      await turn();
    }
    throw new StepError(`${this.role} has no stream for ordinal ${ordinal} (id ${streamId})`);
  }

  /** Turn the two ordinal-bearing keys into the ids this run allocated (WSM-TST-002). */
  private resolve(wanted: Record<string, unknown>): Record<string, unknown> {
    const resolved: Record<string, unknown> = {};
    Object.entries(wanted).forEach(([key, value]) => {
      if (key === 'stream_ref') resolved.stream = this.idOf(Number(value));
      else if (key === 'last_stream_ref') resolved.last_stream = this.idOf(Number(value));
      else resolved[key] = value;
    });
    return resolved;
  }

  /** A **subset** match: the listed keys, and nothing about the rest (WSM-TST-002). */
  private static matches(frame: Frame, wanted: Record<string, unknown>): boolean {
    return Object.entries(wanted).every(([key, value]) => {
      const raw = (frame as unknown as Record<string, unknown>)[key];
      return deepEqual(raw === undefined ? CORPUS_FIELD_DEFAULTS[key] : raw, value);
    });
  }

  // ------------------------------------------------------------------ assertions

  private expectFrame(wantedRaw: Record<string, unknown>): void {
    const wanted = this.resolve(wantedRaw);
    for (let index = this.cursor; index < this.sent.length; index += 1) {
      if (Side.matches(this.sent[index], wanted)) {
        this.cursor = index + 1;
        return;
      }
    }
    throw new StepError(
      `${this.role} sent no frame matching ${JSON.stringify(wanted)}; ` +
        `it sent ${JSON.stringify(this.sent.slice(this.cursor))}`,
    );
  }

  /** Searches every frame this peer has sent, not only frames sent since the last step. */
  private expectNoFrame(wantedRaw: Record<string, unknown>): void {
    const wanted = this.resolve(wantedRaw);
    const offending = this.sent.filter((frame) => Side.matches(frame, wanted));
    if (offending.length > 0) {
      throw new StepError(
        `${this.role} sent ${JSON.stringify(offending)}, and this fixture says it must send ` +
          `nothing like ${JSON.stringify(wanted)}`,
      );
    }
  }

  private valueOf(ref: string): Promise<unknown> {
    const task = this.tasks.get(ref);
    if (task !== undefined) return withDeadline(task, CORPUS_STEP_TIMEOUT_MS, ref);
    const stream = this.refs.get(ref);
    if (stream === undefined) throw new StepError(`no step labelled '${ref}' with "as" on ${this.role}`);
    return withDeadline(stream.result(), CORPUS_STEP_TIMEOUT_MS, `${ref}.result()`);
  }

  private async expectResult(spec: Step): Promise<void> {
    const value = await this.valueOf(spec.ref as string);
    if (!deepEqual(value, spec.value)) {
      throw new StepError(`${String(spec.ref)} produced ${renderValue(value)}, expected ${renderValue(spec.value)}`);
    }
  }

  /** What this peer holds on the attribute `of` names, once it can no longer change (WSM-API-025). */
  private async expectHeaders(spec: Step): Promise<void> {
    const { stream } = await this.streamFor(Number(spec.stream_ref));
    const of = String(spec.of);
    if (of !== 'open' && of !== 'reply') {
      throw new StepError(`expect_headers needs "of": "open" or "reply", not ${of}`);
    }
    if (of === 'reply') {
      await withDeadline(
        stream.replyHeadersArrived,
        CORPUS_STEP_TIMEOUT_MS,
        `stream ${String(spec.stream_ref)} reply headers`,
      );
    }
    const held = of === 'open' ? stream.headers : stream.replyHeaders;
    if (!deepEqual(held, spec.value)) {
      throw new StepError(`${of} headers were ${renderValue(held)}, expected ${renderValue(spec.value)}`);
    }
  }

  private async expectError(spec: Step): Promise<void> {
    const isExpected = CORPUS_ERROR_CLASSES[spec.error as string];
    if (isExpected === undefined) throw new StepError(`expect_error names an unknown class '${String(spec.error)}'`);

    let caught: unknown = NOTHING_RAISED;
    try {
      await this.valueOf(spec.ref as string);
    } catch (error) {
      caught = error;
    }
    if (caught === NOTHING_RAISED) throw new StepError(`${String(spec.ref)} was expected to fail and did not`);
    if (!isExpected(caught)) {
      throw new StepError(`${String(spec.ref)} failed with ${String(caught)}, expected ${String(spec.error)}`);
    }
    if (spec.code !== undefined && (caught as StreamReset).code !== spec.code) {
      throw new StepError(`${String(spec.ref)} failed with code ${String((caught as StreamReset).code)}`);
    }
    if (spec.payload !== undefined && !deepEqual((caught as RemoteError).payload, spec.payload)) {
      throw new StepError(`${String(spec.ref)} carried ${renderValue((caught as RemoteError).payload)}`);
    }
  }

  /** A bounded poll, not a single read: the other end observes a close one turn later. */
  private async expectClosed(spec: Step): Promise<void> {
    const wantSocket = spec.socket === true;
    for (let round = 0; round < QUIET_ROUNDS; round += 1) {
      if (!this.peer.isOpen && (!wantSocket || this.socketClosed)) return;
      await sleep(QUIET_MS);
    }
    throw new StepError(
      `${this.role} is still open (peer.isOpen=${this.peer.isOpen}, socketClosed=${this.socketClosed})`,
    );
  }

  // ------------------------------------------------------------------ teardown

  /**
   * Drop the socket and let the work the script left running settle.
   *
   * The socket is dropped, not closed with a `goaway`: most fixtures end with streams still open,
   * and a `goaway` close would wait out its drain window for each of them. Nothing is cancelled: a
   * collector still iterating a stream ends when the stream fails, and every promise this class
   * started has a rejection handler attached where it was started.
   */
  async teardown(): Promise<void> {
    try {
      await this.socket.close(1000, 'fixture complete');
    } catch {
      // A socket that cannot be closed is already closed.
    }
    await turn();
  }
}

/** Distinguishes "the call did not throw" from "the call threw undefined". */
const NOTHING_RAISED: unique symbol = Symbol('NOTHING_RAISED');

/** Render a value for a failure message, spelling out bytes that `JSON.stringify` renders as `{}`. */
function renderValue(value: unknown): string {
  if (value instanceof ArrayBuffer) return `bytes[${[...new Uint8Array(value)].join(', ')}]`;
  return JSON.stringify(value) ?? String(value);
}

/**
 * `settle: n` means n turns of one event loop, which has no meaning across two processes.
 *
 * The corpus defines `settle` as "a lower bound on progress, never a duration"
 * (`conformance/README.md`), for a transport that delivers a message within the event loop that sent
 * it. Over a socket between two processes the count bounds nothing, so this driver implements
 * `settle` as **wait until both peers are quiet**: both sides watch their own frame counter across
 * one window, and the step returns only when neither saw a frame during it.
 *
 * This condition is at least as strong as the in-process one. A fixture whose next step needs a
 * reset to have arrived (`cancel-mid-stream-stops-the-producer`) waits here until it has.
 */
async function settleAcross(side: Side, control: Control): Promise<void> {
  for (let round = 0; round < QUIET_ROUNDS; round += 1) {
    const before = side.frames;
    const remote = await control.rpc({ cmd: 'settle' });
    await sleep(QUIET_MS);
    if (remote.quiet === true && side.frames === before) return;
  }
  throw new StepError('the two peers never went quiet');
}

// --------------------------------------------------------------------------- the control channel

/**
 * The conductor's end of the line-delimited JSON control channel.
 *
 * A plain TCP socket next to the WebSocket under test. Control messages on the WebSocket itself
 * would appear among the frames the fixtures assert on, including every `expect_no_frame`.
 */
class Control {
  private buffer = '';

  private readonly waiting: { resolve: (line: string) => void; reject: (error: Error) => void }[] = [];

  /**
   * A command sent by `dispatch` whose acknowledgement has not been read yet. Collected before the
   * next command, so replies stay paired with their commands.
   */
  private pending: Promise<Record<string, any>> | null = null;

  constructor(private readonly socket: TcpSocket) {
    socket.setEncoding('utf8');
    socket.on('data', (chunk: string) => {
      this.buffer += chunk;
      let newline = this.buffer.indexOf('\n');
      while (newline >= 0) {
        const line = this.buffer.slice(0, newline);
        this.buffer = this.buffer.slice(newline + 1);
        this.waiting.shift()?.resolve(line);
        newline = this.buffer.indexOf('\n');
      }
    });
    // Rejected immediately instead of at the deadline, so a dead acceptor is reported as such and not
    // as a timeout.
    socket.on('close', () => {
      while (this.waiting.length > 0) {
        this.waiting.shift()?.reject(new StepError("the acceptor's control channel closed mid-run"));
      }
    });
  }

  async rpc(message: Record<string, unknown>): Promise<Record<string, any>> {
    await this.collect();
    return this.exchange(message);
  }

  /**
   * Send a command and go straight on to the next step, collecting the answer later.
   *
   * Used only for `close`, which the corpus defines as "**started and not awaited** - `close()`
   * sends `goaway` and *then* drains, and the steps after it are what the drain window exists to let
   * happen" (`conformance/README.md`). `goaway-drains-then-closes` then opens a stream "a moment too
   * late", and that stream must be sent **before** its opener receives the goaway
   * (WSM-CON-021/023).
   *
   * In one process turn ordering guarantees this. Across two processes, waiting for the
   * acknowledgement would race: the goaway is written before the ack, so either can reach the dialer
   * first. Not waiting lets the local `open()` go out a full round trip earlier, and the acceptor
   * receives the control message one event-loop turn before the open it must exclude from
   * `last_stream`.
   */
  async dispatch(message: Record<string, unknown>): Promise<void> {
    await this.collect();
    this.pending = this.exchange(message);
    // Caught here and re-thrown by `collect`: an unhandled rejection would terminate the process
    // before the failing command is reported.
    void this.pending.catch(() => undefined);
  }

  private async collect(): Promise<void> {
    const pending = this.pending;
    this.pending = null;
    if (pending !== null) await pending;
  }

  private async exchange(message: Record<string, unknown>): Promise<Record<string, any>> {
    this.socket.write(`${JSON.stringify(message)}\n`);
    const answer = new Promise<string>((resolve, reject) => {
      this.waiting.push({ resolve, reject });
    });
    const line = await withDeadline(answer, CONTROL_TIMEOUT_MS, `the acceptor's answer to '${String(message.cmd)}'`);
    const reply = JSON.parse(line) as Record<string, any>;
    if (reply.ok !== true) {
      throw new StepError(`the acceptor refused '${String(message.cmd)}': ${String(reply.error)}`);
    }
    return reply;
  }

  async close(): Promise<void> {
    await this.collect();
    this.socket.end();
  }
}

/** A one-shot gate; `reset()` arms it for the next fixture's connection. */
function newGate(): { promise: Promise<void>; open: () => void } {
  let open!: () => void;
  const promise = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { promise, open };
}

/** The acceptor process: one WebSocket server, one control channel, one fixture at a time. */
class CorpusAcceptor {
  url = '';

  side: Side | null = null;

  private fixture: Fixture | null = null;

  private options: { maxFrameBytes?: number; maxConcurrentStreams?: number } = {};

  private gate = newGate();

  constructor(private readonly wireCodec: Codec) {}

  handleConnection(socket: WebSocket): void {
    // Checked in addition to the handshake hook: the dialer must have offered `muxws.v1.<codec>`
    // first (WSM-CDC-020), so the negotiated subprotocol is that one.
    if (socket.protocol !== `${PREFIX}${this.wireCodec.name}`) {
      check(false, `the dialer negotiated '${socket.protocol}', not '${PREFIX}${this.wireCodec.name}'`);
      return;
    }
    const adapter = new WsSocket(socket);
    const peer = new Peer(adapter, { codec: this.wireCodec, isDialer: false, ...this.options });
    const side = new Side('acceptor', peer, adapter, this.wireCodec);
    this.side = side;
    this.gate.open();
    void peer
      .serve()
      .catch(() => undefined)
      .finally(() => {
        side.socketClosed = true;
      });
  }

  async dispatch(command: Record<string, any>): Promise<Record<string, unknown>> {
    const name = String(command.cmd);
    if (name === 'begin') {
      this.fixture = loadFixture(String(command.fixture));
      this.options = peerOptions(this.fixture);
      this.side = null;
      this.gate = newGate();
      return { ok: true, url: this.url };
    }
    if (name === 'ready') {
      await withDeadline(this.gate.promise, CONTROL_TIMEOUT_MS, "the fixture's connection");
      return { ok: true };
    }
    if (name === 'step') {
      if (this.side === null || this.fixture === null) {
        throw new StepError("a step arrived before the fixture's connection did");
      }
      const step = this.fixture.steps[Number(command.index)];
      return { ok: true, stream: await this.side.execute(step, command.ordinals as number[]) };
    }
    if (name === 'settle') {
      if (this.side === null) throw new StepError("a settle arrived before the fixture's connection did");
      const before = this.side.frames;
      await sleep(QUIET_MS);
      return { ok: true, quiet: this.side.frames === before };
    }
    if (name === 'end') {
      if (this.side !== null) await this.side.teardown();
      this.side = null;
      return { ok: true };
    }
    throw new StepError(`unknown control command '${name}'`);
  }
}

async function corpusAccept(port: number): Promise<void> {
  const wireCodec = codec();
  const acceptor = new CorpusAcceptor(wireCodec);

  // Both hooks, as in `acceptForever`: `handleProtocols` selects a subprotocol but does not refuse a
  // mismatched upgrade (WSM-CDC-022).
  const websockets = refuseMismatchedUpgrade(new WebSocketServer({ port: 0, host: '127.0.0.1', handleProtocols }));
  websockets.on('connection', (socket: WebSocket) => acceptor.handleConnection(socket));
  await new Promise<void>((resolve) => websockets.on('listening', () => resolve()));
  const wsAddress = websockets.address();
  acceptor.url = `ws://127.0.0.1:${typeof wsAddress === 'object' && wsAddress !== null ? wsAddress.port : 0}`;

  const done = newGate();
  const control = createServer((socket: TcpSocket) => {
    socket.setEncoding('utf8');
    let buffer = '';
    // One command at a time, chained, so steps run in script order.
    let queue: Promise<void> = Promise.resolve();
    socket.on('data', (chunk: string) => {
      buffer += chunk;
      let newline = buffer.indexOf('\n');
      while (newline >= 0) {
        const line = buffer.slice(0, newline);
        buffer = buffer.slice(newline + 1);
        queue = queue.then(async () => {
          let reply: Record<string, unknown>;
          try {
            reply = await acceptor.dispatch(JSON.parse(line) as Record<string, any>);
          } catch (error) {
            // Failures are reported by the conductor, so they are sent back over the channel instead of
            // ending this process.
            reply = { ok: false, error: error instanceof Error ? error.message : String(error) };
          }
          socket.write(`${JSON.stringify(reply)}\n`);
        });
        newline = buffer.indexOf('\n');
      }
    });
    socket.on('close', () => done.open());
  });

  await new Promise<void>((resolve) => control.listen(port, '127.0.0.1', () => resolve()));
  const controlAddress = control.address();
  // The port `interop/drive.sh` reads and passes to the conductor is the **control** port. The
  // conductor learns the WebSocket port from `begin`.
  emit({
    role: 'ts-corpus-acceptor',
    port: typeof controlAddress === 'object' && controlAddress !== null ? controlAddress.port : port,
    codec: settings.codec,
  });

  await done.promise;
  websockets.close();
  control.close();
  await finish();
}

// --------------------------------------------------------------------------- the conductor

/**
 * The fixture count declared in `ts/conformance.spec.ts`, read from that file.
 */
function pinnedFixtureCount(): number {
  const source = readFileSync(FIXTURE_COUNT_SOURCE, 'utf-8');
  const found = /^const EXPECTED_SEQUENCE_FIXTURES = (\d+);$/m.exec(source);
  if (found === null) {
    console.error(`interop FAILED: ${FIXTURE_COUNT_SOURCE} declares no EXPECTED_SEQUENCE_FIXTURES`);
    process.exit(1);
  }
  return Number(found[1]);
}

/**
 * Which side executes this step.
 *
 * `inject` is **inverted**: the step means "deliver this message *to* peer X", and in a
 * cross-process run only X's remote can deliver a message to X. Reading the key as "X runs this"
 * would have each peer send the unknown frame to itself, and no frame would cross the socket.
 */
function ownerOf(step: Step, labels: Map<string, Who>): Who {
  if ('inject' in step) return step.peer === 'dialer' ? 'acceptor' : 'dialer';
  const named = ['call', 'expect_frame', 'expect_no_frame'].find((key) => key in step);
  if (named !== undefined) return step.peer as Who;
  if ('expect_closed' in step) return (step.expect_closed as Step).peer as Who;
  if ('expect_headers' in step) return (step.expect_headers as Step).peer as Who;
  const labelled = ['expect_result', 'expect_error'].find((key) => key in step);
  if (labelled !== undefined) {
    const ref = String((step[labelled] as Step).ref);
    const owner = labels.get(ref);
    if (owner === undefined) throw new StepError(`${labelled} names '${ref}', which no step bound with "as"`);
    return owner;
  }
  throw new StepError(`no step kind in ${JSON.stringify(step)}`);
}

/** One fixture: a fresh connection, the whole script, then the connection dropped. */
async function runFixture(control: Control, name: string, wireCodec: Codec): Promise<void> {
  const fixture = loadFixture(name);
  const begun = await control.rpc({ cmd: 'begin', fixture: name });

  const socket = new WebSocket(String(begun.url), offer(wireCodec.name));
  await new Promise<void>((resolve, reject) => {
    socket.once('open', () => resolve());
    socket.once('error', reject);
  });
  if (socket.protocol !== `${PREFIX}${wireCodec.name}`) {
    check(false, `the acceptor negotiated '${socket.protocol}', not '${PREFIX}${wireCodec.name}' (WSM-CDC-020)`);
  }
  const adapter = new WsSocket(socket);
  const peer = new Peer(adapter, { codec: wireCodec, isDialer: true, ...peerOptions(fixture) });
  const side = new Side('dialer', peer, adapter, wireCodec);
  void peer.serve().catch(() => undefined);
  await control.rpc({ cmd: 'ready' });

  const ordinals: number[] = [];
  const labels = new Map<string, Who>();
  try {
    for (const [index, step] of fixture.steps.entries()) {
      try {
        await runStep(control, side, step, index, ordinals, labels);
      } catch (error) {
        const detail = error instanceof Error ? error.message : String(error);
        check(false, `${name} step ${index}: ${detail}`);
      }
    }
  } finally {
    await side.teardown();
    await control.rpc({ cmd: 'end' }).catch(() => undefined);
  }
}

async function runStep(
  control: Control,
  side: Side,
  step: Step,
  index: number,
  ordinals: number[],
  labels: Map<string, Who>,
): Promise<void> {
  if ('settle' in step) {
    await settleAcross(side, control);
    return;
  }

  const who = ownerOf(step, labels);
  if ('call' in step && typeof step.as === 'string') {
    // Which side holds the awaitable a later `expect_result` reads. Labels are separate from
    // ordinals: an ordinal identifies a stream on the wire, a label identifies a result the script
    // awaits, and only the side that started the call holds it.
    labels.set(step.as, who);
  }

  const command = { cmd: 'step', index, ordinals };
  let allocated: number | null;
  if (who === side.role) {
    allocated = await side.execute(step, ordinals);
  } else if (step.call === 'close') {
    // The only call the corpus declares started and not awaited; see `Control.dispatch`.
    await control.dispatch(command);
    allocated = null;
  } else {
    allocated = ((await control.rpc(command)).stream ?? null) as number | null;
  }

  if (ALLOCATING_CALLS.includes(String(step.call))) {
    if (allocated === null) throw new StepError(`${String(step.call)} allocated no ordinal`);
    // Appended only here, so there is one ordinal table; the other process receives a copy of it
    // (WSM-TST-002).
    ordinals.push(allocated);
  }
}

/** Conduct the whole sequence corpus against the other language's acceptor (WSM-CDC-007). */
async function corpusDial(endpoint: string, label: string): Promise<void> {
  const wireCodec = codec();
  const separator = endpoint.lastIndexOf(':');
  const socket = connectTcp(Number(endpoint.slice(separator + 1)), endpoint.slice(0, separator));
  await new Promise<void>((resolve, reject) => {
    socket.once('connect', () => resolve());
    socket.once('error', reject);
  });
  const control = new Control(socket);

  const names = readdirSync(SEQUENCES_DIR)
    .filter((file) => file.endsWith('.json'))
    .map((file) => file.replace(/\.json$/, ''))
    .sort();
  const expected = pinnedFixtureCount();
  check(names.length === expected, `${label}: ${names.length} fixtures on disk, ${expected} pinned by the runner`);

  const declared: Record<string, string> = {};
  names.forEach((name) => {
    const raw = JSON.parse(readFileSync(join(SEQUENCES_DIR, `${name}.json`), 'utf-8')) as Fixture;
    if (raw.requires_codec !== undefined) declared[name] = raw.requires_codec;
  });
  check(
    JSON.stringify(declared) === JSON.stringify(CODEC_SPECIFIC_FIXTURES),
    `${label}: the corpus declares ${JSON.stringify(declared)} codec-specific; this driver's skip ` +
      `list is ${JSON.stringify(CODEC_SPECIFIC_FIXTURES)}, and a fixture that declares a codec ` +
      'without being listed is skipped by every pass not configured with that codec (WSM-CDC-007)',
  );

  const ran: string[] = [];
  const skipped: { fixture: string; reason: string }[] = [];
  const unsupported: { fixture: string; reason: string }[] = [];
  for (const name of names) {
    const fixture = loadFixture(name);
    const required = fixture.requires_codec;
    if (required !== undefined && required !== wireCodec.name) {
      const reason = `requires the ${required} codec`;
      skipped.push({ fixture: name, reason });
      emit({ role: label, event: 'fixture', fixture: name, outcome: 'skipped', reason });
      continue;
    }
    const reason = unsupportedReason(fixture);
    if (reason !== null) {
      unsupported.push({ fixture: name, reason });
      emit({ role: label, event: 'fixture', fixture: name, outcome: 'unsupported', reason });
      continue;
    }
    await runFixture(control, name, wireCodec);
    ran.push(name);
    emit({ role: label, event: 'fixture', fixture: name, outcome: 'ran' });
  }

  await control.close();

  // Three checks: no fixture is unsupported, at least one fixture ran, and every fixture on disk
  // either ran or was skipped for requiring another codec (WSM-CDC-007).
  check(unsupported.length === 0, `${label}: this driver could not replay ${JSON.stringify(unsupported)}`);
  check(ran.length > 0, `${label}: the corpus run exercised no fixture at all`);
  check(
    ran.length + skipped.length === names.length,
    `${label}: ${ran.length} ran and ${skipped.length} were skipped, which is not the ${names.length} on disk`,
  );
  emit({
    role: label,
    event: 'corpus',
    codec: wireCodec.name,
    total: names.length,
    ran: ran.length,
    fixtures: ran,
    skipped,
    unsupported,
  });
  emit({ role: label, ok: true, ran: ran.length });
}

// --------------------------------------------------------------------------- entry points

async function dial(url: string): Promise<void> {
  const pushes: any[] = [];
  const journal = new Journal();

  // `maxAttempts: 0`: this scenario ends with the acceptor going away, and a dialer that reconnected
  // would keep dialling a process the driver has not yet killed.
  const peer = await connect(url, {
    reconnect: new Reconnect({ maxAttempts: 0 }),
    onStream: (payload: any) => {
      pushes.push(payload);
    },
  });
  peer.onFrame(journal.record);
  await runTst004(peer, journal, pushes, 'ts-dialer');
}

/**
 * Exit when the scenario ends instead of waiting for the event loop to drain.
 *
 * The heartbeat's next tick can stay pending for up to a ping interval after `close()`, and node
 * stays alive until it fires; waiting would add up to twenty seconds to every CI job. stdout is
 * flushed first: under a CI runner it is a pipe, where writes are asynchronous and `process.exit`
 * would truncate the line the driver reads.
 */
async function finish(): Promise<void> {
  await new Promise<void>((resolve) => process.stdout.write('', () => resolve()));
  process.exit(0);
}

async function main(): Promise<void> {
  const [mode, argument] = process.argv.slice(2);
  await configureCodec();
  if (mode === 'accept') {
    await acceptForever(Number(argument));
  } else if (mode === 'accept-unix') {
    await acceptForeverOnSocketFile(argument);
  } else if (mode === 'dial') {
    await dial(argument);
    await finish();
  } else if (mode === 'reconnect-dial') {
    await runTst005(argument, 'ts-dialer');
    await finish();
  } else if (mode === 'corpus-accept') {
    await corpusAccept(Number(argument));
  } else if (mode === 'corpus-dial') {
    await corpusDial(argument, 'ts-corpus-dialer');
    await finish();
  } else {
    console.error(
      'usage: runner.ts accept <port> | accept-unix <path> | dial <url> | reconnect-dial <url> | ' +
        'corpus-accept <port> | corpus-dial <host:port>',
    );
    process.exit(1);
  }
}

void main().catch((error: unknown) => {
  console.error(`interop FAILED: ${String(error)}`);
  process.exit(1);
});
