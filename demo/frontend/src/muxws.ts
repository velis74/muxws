/**
 * The demo's whole store, and the only place a `Peer` is built (D4).
 *
 * There is no state management library and no router here on purpose: every line a reader has to
 * understand before they see muxws is a line taxed against the demo's purpose. What is left is one
 * `reactive` object, one `connect()`, and the four call shapes:
 *
 *   * the board  - twenty streams the *backend* opened, arriving at this dialer's own `onStream`
 *                  handler. There is no push API because there does not need to be one
 *                  (WSM-INV-002).
 *   * `quote`    - `peer.request()`: one payload out, exactly one back (WSM-API-006).
 *   * `history`  - `for await (... of peer.open(...))`: a response that fills in progressively, and
 *                  one that stops the *producer* when it is cancelled (WSM-ERR-012/013).
 *   * `depth` /
 *     `export`   - one payload far over `MAX_FRAME_BYTES`, fragmented on the way out and reassembled
 *                  on the way in with this file doing nothing at all (WSM-FRG-010).
 *
 * Nothing in here holds seed data. Every symbol, price and name the screen shows arrived over the
 * socket, because a board that could draw itself without a connection would prove nothing about the
 * transport (D1).
 */

import { reactive } from 'vue';

import {
  connect,
  ConnectionLost,
  type CloseReason,
  type Frame,
  type Peer,
  Reconnect,
  ResetCode,
  type Stream,
  StreamReset,
} from 'muxws';

// --------------------------------------------------------------------------- what the wire carries

/** One board row, exactly as `market.Instrument.row()` sends it. */
export interface BoardRow {
  symbol: string;
  name: string;
  price: number;
  open: number;
  change: number;
  change_pct: number;
  volume: number;
  ticks: number;
}

export interface Quote {
  symbol: string;
  name: string;
  last: number;
  bid: number;
  ask: number;
  spread: number;
  open: number;
  volume: number;
}

export interface HistoryPoint {
  t: number;
  px: number;
  volume: number;
}

interface DepthPayload {
  symbol: string;
  levels: number;
  bids: unknown[];
  asks: unknown[];
}

interface ExportPayload {
  symbol: string;
  count: number;
  trades: unknown[];
}

/** `MarketService._stats`: the counters plus what the backend knows about itself. */
export interface Stats {
  history_started: number;
  history_points_sent: number;
  history_cancelled: number;
  exports: number;
  depths: number;
  quotes: number;
  peers: number;
  streams: number;
  symbols: string[];
  tick_interval: number;
}

/** What one large payload cost, measured from `on_frame` rather than claimed. */
export interface FragmentReport {
  action: string;
  symbol: string;
  /** Bytes counted on the wire for this stream, envelopes included. */
  bytes: number;
  /** How many frames of it carried a `fragment` field (WSM-FRG-010). */
  fragments: number;
  /** Data frames on **other** streams that landed between the first and the last fragment. */
  interleaved: number;
  ms: number;
  /** The worst tick lateness observed while it was in flight, in milliseconds. */
  worstLatencyMs: number;
}

export type ConnectionState = 'connecting' | 'live' | 'reconnecting' | 'failed';

export type HistoryState = 'idle' | 'loading' | 'done' | 'cancelled' | 'lost';

// --------------------------------------------------------------------------- tuning

/** How often the throughput counters and the ping are refreshed. */
const DIAGNOSTICS_INTERVAL_MS = 1000;

/** One sample per nominal tick period, so a sample is one opportunity for a tick to have been late. */
const LATENCY_SAMPLE_MS = 250;

/** 80 samples at 250 ms is twenty seconds of sparkline - long enough to hold a whole export. */
const LATENCY_SAMPLES = 80;

/** The event log is a witness, not a journal; the oldest lines are the least interesting. */
const MAX_EVENTS = 40;

// --------------------------------------------------------------------------- the store

export const store = reactive({
  connection: 'connecting' as ConnectionState,
  connectError: null as string | null,
  /** Changes on every reconnection, on purpose: a reconnected peer reads as a new connection. */
  peerId: '',
  /** `on_reconnect`'s attempt number - the reconnect count the brief asks to be visible. */
  reconnects: 0,
  tickInterval: 0.25,

  /** Board streams open right now, and how many this *connection* has opened. */
  boardLive: 0,
  boardOpened: 0,
  /** Board streams that ended by raising rather than by ending, cumulative (WSM-RCN-030). */
  streamsLost: 0,
  /** What ended them, named by its error class. Twenty streams die together and say the same thing. */
  lastStreamEnd: null as string | null,
  /** True between a socket loss and the first tick of the next connection; the rows are stale. */
  stale: false,

  rows: {} as Record<string, BoardRow>,
  events: [] as string[],

  selected: null as string | null,
  quote: null as Quote | null,
  history: [] as HistoryPoint[],
  historyState: 'idle' as HistoryState,
  historyTrailers: null as Record<string, unknown> | null,

  depth: null as (FragmentReport & { levels: number }) | null,
  exportState: 'idle' as 'idle' | 'running' | 'done' | 'failed',
  exportReport: null as FragmentReport | null,

  stats: null as Stats | null,

  framesInPerSecond: 0,
  framesOutPerSecond: 0,
  bytesInPerSecond: 0,
  bytesOutPerSecond: 0,
  rttMs: null as number | null,
  liveStreams: 0,

  latencyNowMs: 0,
  latencyWorstMs: 0,
  latency: [] as number[],
});

// --------------------------------------------------------------------------- module state

let peer: Peer | null = null;

/** The outstanding history generation, held so switching symbols can stop it at the producer. */
let historyStream: Stream<HistoryPoint> | null = null;

/** When each symbol's stream last delivered a row, for the lateness readout. */
const lastTickAt = new Map<string, number>();

/** The worst lateness seen since the last sparkline sample. */
let windowLatencyMs = 0;

const throughput = { framesIn: 0, framesOut: 0, bytesIn: 0, bytesOut: 0 };

/**
 * The live measurement behind the headline, and the same assertion
 * `handlers_test.py::test_ticks_keep_flowing_during_an_export` makes: count the fragments of one
 * large payload, and count the data frames on *other* streams that arrived between the first and the
 * last of them. A FIFO writer scores zero. A round-robin writer cannot (WSM-INV-004).
 */
interface Probe {
  streamId: number;
  bytes: number;
  fragments: number;
  interleaved: number;
  worstLatencyMs: number;
}

let probe: Probe | null = null;

/** One line per socket loss rather than twenty; the count is carried by `store.streamsLost`. */
let lossReported = false;

function say(line: string): void {
  const at = new Date().toLocaleTimeString();
  store.events.unshift(`${at}  ${line}`);
  if (store.events.length > MAX_EVENTS) store.events.length = MAX_EVENTS;
}

function describe(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}

// --------------------------------------------------------------------------- connecting

/**
 * The dial. Same origin as the page, which is what makes Vite's `ws: true` proxy entry load-bearing.
 *
 * Where a credential would go: **not here and not in the hello**. It belongs on the handshake - a
 * ticket in the query string, or a cookie the browser already sends - checked before `accept()` ever
 * runs (WSM-AUT-001). The demo dials without one.
 */
function socketUrl(): string {
  const scheme = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${scheme}//${window.location.host}/ws`;
}

/**
 * A stable identity for this tab, in `sessionStorage` because that is exactly the lifetime of the
 * thing being identified: `localStorage` would make three tabs claim to be one client, and a module
 * variable would die on the reload that most often causes a reconnection.
 */
function tabId(): string {
  let tab = window.sessionStorage.getItem('muxws-demo-tab');
  if (tab === null) {
    tab = Math.random().toString(36).slice(2, 10);
    window.sessionStorage.setItem('muxws-demo-tab', tab);
  }
  return tab;
}

export async function start(): Promise<void> {
  store.connection = 'connecting';
  store.connectError = null;
  try {
    // The handler goes in `connect()` rather than on the peer it returns: the backend pushes the
    // board the instant it has read the hello, and a handler registered one `await` later would meet
    // those twenty streams with `reset(REFUSED)` (WSM-STM-033).
    peer = await connect(socketUrl(), {
      // Replayed verbatim on every socket this peer ever gets (WSM-RCN-020), which is what makes the
      // subscription survive a reconnection without a line of code on the accepting side (D3).
      hello: { action: 'hello', tags: { app: 'muxws-demo', tab: tabId() } },
      reconnect: new Reconnect(),
      onStream: onPushedStream,
      onClose: onConnectionClose,
      onReconnect: onReconnected,
    });
  } catch (error) {
    // WSM-RCN-006: the **first** attempt is never retried, whatever `reconnect` says. A typo in the
    // URL that retried forever would be indistinguishable from a server that is merely slow.
    store.connection = 'failed';
    store.connectError = describe(error);
    say(`connect() failed and did not retry (WSM-RCN-006): ${store.connectError}`);
    return;
  }

  peer.onFrame(recordFrame);
  store.peerId = peer.id;
  store.connection = 'live';
  say(`connected as ${peer.id}; hello sent, waiting for the backend to push the board`);

  window.setInterval(sampleDiagnostics, DIAGNOSTICS_INTERVAL_MS);
  window.setInterval(sampleLatency, LATENCY_SAMPLE_MS);
  void refreshStats();
}

function onConnectionClose(reason: CloseReason): void {
  // `willRetry` is the promise being kept or withdrawn: true while the helper still intends to dial,
  // false exactly once, when it has given up or `close()` was called deliberately.
  store.connection = reason.willRetry ? 'reconnecting' : 'failed';
  store.stale = true;
  store.rttMs = null;
  store.liveStreams = 0;
  say(
    `socket closed: code ${reason.code}, "${reason.reason || 'no reason given'}", clean=${reason.wasClean}, ` +
      `willRetry=${reason.willRetry}`,
  );
}

function onReconnected(attempt: number, reconnected: Peer): void {
  store.reconnects = attempt;
  store.peerId = reconnected.id;
  store.connection = 'live';
  // Reset, so the number that climbs back to twenty below is *this* connection's subscription and
  // not a total carried over from the last one. Nothing was resumed: WSM-RCN-030 restores a live
  // socket and an accepted identity and nothing else, so the board the reader is about to watch
  // re-appear is twenty brand-new streams the backend opened after replaying the hello.
  store.boardOpened = 0;
  lossReported = false;
  lastTickAt.clear();
  say(`reconnected on attempt ${attempt}: hello replayed, peer.id is now ${reconnected.id}, no stream survived`);
  void refreshStats();
}

// --------------------------------------------------------------------------- the board (push)

/**
 * This dialer's own `on_stream` handler, and the reason there is no push API.
 *
 * The backend calls `peer.open()` on its own initiative, twenty times, and each one arrives here on
 * the socket that was already there - one `Peer` type, one mechanism, no second correlation story
 * (WSM-INV-002).
 */
async function onPushedStream(payload: unknown, stream: Stream): Promise<void> {
  const opening = payload as { topic?: unknown; symbol?: unknown };
  if (opening?.topic !== 'ticks' || typeof opening.symbol !== 'string') {
    // REFUSED and not APPLICATION_ERROR: this end has not processed the stream and never will, which
    // is precisely the distinction the two codes carry (WSM-STM-034).
    await stream.reset(ResetCode.REFUSED, 'this demo subscribes to ticks and nothing else');
    return;
  }

  store.boardLive += 1;
  store.boardOpened += 1;
  try {
    for await (const row of stream as Stream<BoardRow>) {
      applyTick(row);
    }
  } catch (error) {
    // WSM-RCN-030: every stream live at the moment of a loss is already dead, and each one says so
    // by raising. The UI has to repeat it - a board that merely stopped moving is indistinguishable
    // from a quiet market. Twenty streams fail together, so this counts twenty times and speaks once.
    //
    // Which exception arrives is worth reading rather than assuming. `ConnectionLost` is the socket
    // dying underneath a live stream; a plain `StreamReset` is a producer that stopped first, which
    // is what this backend's kill switch deliberately does before it closes anything (its board
    // streams have no natural end, and `close()` would otherwise wait out its whole drain window on
    // them). The demo shows whichever actually happened.
    store.streamsLost += 1;
    if (!lossReported) {
      lossReported = true;
      store.lastStreamEnd = describe(error);
      say(
        error instanceof ConnectionLost
          ? `ConnectionLost reached the board streams: ${error.message}`
          : `the board streams were reset before the socket closed: ${describe(error)}`,
      );
    }
  } finally {
    store.boardLive -= 1;
  }
}

function applyTick(row: BoardRow): void {
  store.rows[row.symbol] = row;
  store.stale = false;
  const now = performance.now();
  const previous = lastTickAt.get(row.symbol);
  lastTickAt.set(row.symbol, now);
  // The first row of a stream is the snapshot, not a tick: there is no previous arrival to be late
  // relative to, and counting it would report the whole page load as latency.
  if (previous === undefined) return;
  noteLateness(now - previous - store.tickInterval * 1000);
}

function noteLateness(lateness: number): void {
  if (lateness <= windowLatencyMs) return;
  windowLatencyMs = lateness;
}

// --------------------------------------------------------------------------- diagnostics

function recordFrame(direction: 'tx' | 'rx', frame: Frame, byteLength: number): void {
  if (direction === 'rx') {
    throughput.framesIn += 1;
    throughput.bytesIn += byteLength;
  } else {
    throughput.framesOut += 1;
    throughput.bytesOut += byteLength;
  }

  if (probe === null || direction !== 'rx') return;
  if (frame.stream === probe.streamId) {
    probe.bytes += byteLength;
    if (frame.fragment !== null && frame.fragment !== undefined) probe.fragments += 1;
    return;
  }
  // Only once fragmentation has actually begun, so this counts frames that shared the wire with the
  // large payload rather than frames that merely arrived during the round trip.
  if (probe.fragments > 0 && frame.type === 'data') probe.interleaved += 1;
}

function sampleDiagnostics(): void {
  store.framesInPerSecond = throughput.framesIn;
  store.framesOutPerSecond = throughput.framesOut;
  store.bytesInPerSecond = throughput.bytesIn;
  store.bytesOutPerSecond = throughput.bytesOut;
  throughput.framesIn = 0;
  throughput.framesOut = 0;
  throughput.bytesIn = 0;
  throughput.bytesOut = 0;
  store.liveStreams = peer === null ? 0 : peer.streams.size;
  void samplePing();
  void refreshStats();
}

async function samplePing(): Promise<void> {
  if (peer === null || !peer.isOpen) {
    store.rttMs = null;
    return;
  }
  try {
    // A connection-level round trip on a `ping` frame, not a stream: it measures the socket, so it
    // stays honest while every stream on it is busy (WSM-CON-012).
    store.rttMs = Math.round((await peer.ping()) * 100) / 100;
  } catch {
    // A ping that could not be sent or was not answered is a diagnostic that failed, not an
    // application error; the connection state next to it already says what happened.
    store.rttMs = null;
  }
}

/**
 * The tick-latency readout, sampled rather than event-driven.
 *
 * Two things feed it, because either alone would lie. `windowLatencyMs` is how late a tick was when
 * it finally landed - which is only known *after* the stall. The staleness scan below is how late
 * every symbol is *right now*, so a stall in progress shows while it is happening rather than at its
 * end.
 */
function sampleLatency(): void {
  const now = performance.now();
  const nominal = store.tickInterval * 1000;
  let worst = windowLatencyMs;
  lastTickAt.forEach((at) => {
    const stale = now - at - nominal;
    if (stale > worst) worst = stale;
  });

  store.latencyNowMs = Math.max(0, Math.round(worst));
  store.latency.push(store.latencyNowMs);
  if (store.latency.length > LATENCY_SAMPLES) store.latency.shift();
  if (store.latencyNowMs > store.latencyWorstMs) store.latencyWorstMs = store.latencyNowMs;
  if (probe !== null && store.latencyNowMs > probe.worstLatencyMs) probe.worstLatencyMs = store.latencyNowMs;
  windowLatencyMs = 0;
}

export function resetLatencyWorst(): void {
  store.latencyWorstMs = 0;
}

/**
 * `history_cancelled` is why this exists.
 *
 * Nothing on a screen can distinguish a backend that stopped generating from a frontend that stopped
 * looking, so the number is asked of the backend, over the same socket as everything else, and the
 * generator itself is what increments it (WSM-ERR-012/013).
 */
export async function refreshStats(): Promise<void> {
  if (peer === null || !peer.isOpen) return;
  try {
    store.stats = await peer.request<Stats>({ action: 'stats' });
    store.tickInterval = store.stats.tick_interval;
  } catch (error) {
    if (!(error instanceof ConnectionLost)) say(`stats failed: ${describe(error)}`);
  }
}

// --------------------------------------------------------------------------- the four call shapes

/** A row click: cancel what is outstanding, then one unary call and one streaming one. */
export async function select(symbol: string): Promise<void> {
  // First, and before anything is awaited: until this reset is on the wire the backend is still
  // generating points for the symbol the reader has already left (WSM-ERR-012).
  await cancelHistory('the reader selected another symbol');

  store.selected = symbol;
  store.quote = null;
  store.history = [];
  store.historyTrailers = null;
  store.historyState = 'idle';
  store.depth = null;

  await loadQuote(symbol);
  void streamHistory(symbol);
}

async function cancelHistory(reason: string): Promise<void> {
  const outstanding = historyStream;
  if (outstanding === null) return;
  historyStream = null;
  // A no-op if the stream is already closed, including after socket death, where there is nothing to
  // send it on (WSM-RCN-041).
  await outstanding.cancel(reason);
  // Asked immediately rather than waited for on the poll, so the counter the reader is watching
  // moves while the click is still the last thing they did.
  void refreshStats();
}

/** The unary shape: one payload out, exactly one payload back (WSM-API-006). */
async function loadQuote(symbol: string): Promise<void> {
  if (peer === null) return;
  try {
    store.quote = await peer.request<Quote>({ action: 'quote', symbol });
  } catch (error) {
    store.quote = null;
    say(`quote for ${symbol} failed: ${describe(error)}`);
  }
}

/**
 * The streaming shape. `open()` is synchronous - it hands back the `Stream` in the same turn it
 * allocated the id (WSM-API-001) - and `end: true` says this side has nothing more to send.
 */
async function streamHistory(symbol: string): Promise<void> {
  if (peer === null) return;
  let stream: Stream<HistoryPoint>;
  try {
    stream = peer.open<HistoryPoint>({ action: 'history', symbol }, { end: true });
  } catch (error) {
    store.historyState = 'lost';
    say(`history for ${symbol} could not be opened: ${describe(error)}`);
    return;
  }

  historyStream = stream;
  store.historyState = 'loading';
  try {
    for await (const point of stream) {
      store.history.push(point);
    }
    // Trailers ride the `end` frame, so they are readable only once the body is finished - which is
    // what makes them the right place for a count of what was sent.
    store.historyTrailers = stream.trailers;
    store.historyState = 'done';
  } catch (error) {
    if (error instanceof ConnectionLost) {
      store.historyState = 'lost';
      say(`the ${symbol} history stream raised ConnectionLost; nothing is resumed (WSM-RCN-030)`);
    } else if (error instanceof StreamReset && error.code === ResetCode.CANCELLED) {
      store.historyState = 'cancelled';
    } else {
      store.historyState = 'lost';
      say(`history for ${symbol} failed: ${describe(error)}`);
    }
  } finally {
    if (historyStream === stream) historyStream = null;
  }
}

/**
 * One payload far over `MAX_FRAME_BYTES`, and the measurement of what it did to everything else.
 *
 * `open()` awaited rather than `request()` only because the probe needs the stream id, and `request()`
 * returns the payload without ever showing it. The shape on the wire is identical - `request()` *is*
 * `open(payload, {end: true})` awaited to the stream's end (WSM-API-006).
 */
async function fragmented<T>(action: string, symbol: string): Promise<{ payload: T; report: FragmentReport }> {
  if (peer === null) throw new ConnectionLost('there is no peer');
  const stream = peer.open<T>({ action, symbol }, { end: true });
  probe = { streamId: stream.id, bytes: 0, fragments: 0, interleaved: 0, worstLatencyMs: 0 };
  const startedAt = performance.now();
  try {
    // Nothing here calls a reassembler. The peer hands over one whole payload and the application
    // never learns it was cut, which is the entire point of WSM-FRG-010.
    const payload = await stream;
    return {
      payload,
      report: {
        action,
        symbol,
        bytes: probe.bytes,
        fragments: probe.fragments,
        interleaved: probe.interleaved,
        ms: Math.round(performance.now() - startedAt),
        worstLatencyMs: probe.worstLatencyMs,
      },
    };
  } finally {
    probe = null;
  }
}

export async function loadDepth(): Promise<void> {
  const symbol = store.selected;
  if (symbol === null) return;
  try {
    const { payload, report } = await fragmented<DepthPayload>('depth', symbol);
    store.depth = { ...report, levels: payload.levels };
  } catch (error) {
    store.depth = null;
    say(`full depth for ${symbol} failed: ${describe(error)}`);
  }
}

/**
 * The headline. One megabyte on one stream while twenty others keep ticking (WSM-INV-004).
 *
 * What makes it a demonstration rather than a claim is `report.interleaved`: tick frames that landed
 * between the export's first and last fragment. The writer takes the streams round-robin and a stream
 * holds at most one unsent fragment at a time, so the export cannot own the wire. Replace the writer
 * with a FIFO and that number is zero.
 */
export async function runExport(): Promise<void> {
  const symbol = store.selected ?? Object.keys(store.rows)[0];
  if (symbol === undefined || peer === null) return;
  store.exportState = 'running';
  store.exportReport = null;
  try {
    const { payload, report } = await fragmented<ExportPayload>('export', symbol);
    store.exportReport = report;
    store.exportState = 'done';
    say(
      `export of ${payload.count} rows: ${report.fragments} fragments, ${report.interleaved} tick frames ` +
        `interleaved, worst tick lateness ${report.worstLatencyMs} ms`,
    );
  } catch (error) {
    store.exportState = 'failed';
    say(`the export failed: ${describe(error)}`);
  }
}

/** Ask the backend to close every peer, so the reader can watch a reconnection they did not cause. */
export async function killBackend(): Promise<void> {
  if (peer === null) return;
  try {
    const answer = await peer.request<{ closing: number }>({ action: 'kill' });
    say(`the backend is closing ${answer.closing} peer(s); every open stream is about to fail`);
  } catch (error) {
    say(`the kill switch failed: ${describe(error)}`);
  }
}
