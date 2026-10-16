/**
 * The demo's store, and the only module that creates a `Peer`.
 *
 * The demo uses no state management library and no router. The store is one `reactive` object, the
 * connection is one `connect()`, and the four call shapes are:
 *
 *   * the board  - twenty streams opened by the backend, delivered to this dialer's `onStream`
 *                  handler; muxws has no separate push API (WSM-INV-002).
 *   * `quote`    - `peer.request()`: one payload out, exactly one back (WSM-API-006).
 *   * `history`  - `for await (... of peer.open(...))`: a progressive response; cancelling it stops the
 *                  backend producer (WSM-ERR-012/013).
 *   * `depth` /
 *     `export`   - one payload larger than `MAX_FRAME_BYTES`, fragmented by the sender and reassembled
 *                  by the receiver without code in this file (WSM-FRG-010).
 *
 * This module holds no symbol data. Every symbol, price and name on the screen is received over the
 * socket.
 */

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
import { reactive } from 'vue';

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

/** The `stats` reply: the backend's counters, peer and stream counts, symbols and tick interval. */
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

/** Measurements of one large payload, taken from the `onFrame` hook. */
export interface FragmentReport {
  action: string;
  symbol: string;
  /** Bytes counted on the wire for this stream, envelopes included. */
  bytes: number;
  /** How many frames of it carried a `fragment` field (WSM-FRG-010). */
  fragments: number;
  /** Data frames on other streams received between the first and the last fragment. */
  interleaved: number;
  ms: number;
  /** The largest tick lateness sampled while the payload was in transit, in milliseconds. */
  worstLatencyMs: number;
}

export type ConnectionState = 'connecting' | 'live' | 'reconnecting' | 'failed';

export type HistoryState = 'idle' | 'loading' | 'done' | 'cancelled' | 'lost';

// --------------------------------------------------------------------------- tuning

/** How often the throughput counters and the ping are refreshed. */
const DIAGNOSTICS_INTERVAL_MS = 1000;

/** Latency sampling period in milliseconds; equal to the default tick interval. */
const LATENCY_SAMPLE_MS = 250;

/** Samples kept for the sparkline: 80 samples at 250 ms cover twenty seconds. */
const LATENCY_SAMPLES = 80;

/** Maximum event log length; the oldest lines are dropped first. */
const MAX_EVENTS = 40;

// --------------------------------------------------------------------------- the store

export const store = reactive({
  connection: 'connecting' as ConnectionState,
  connectError: null as string | null,
  /** The current peer's id; it changes on every reconnection. */
  peerId: '',
  /** The attempt number passed to the latest `onReconnect` call. */
  reconnects: 0,
  tickInterval: 0.25,

  /** Board streams currently open, and board streams opened on the current connection. */
  boardLive: 0,
  boardOpened: 0,
  /** Cumulative count of board streams that ended with an error (WSM-RCN-030). */
  streamsLost: 0,
  /** The error that ended the board streams at the latest loss, as `Name: message`. */
  lastStreamEnd: null as string | null,
  /** True from a socket loss until the first board repaint on the next connection. */
  stale: false,
  /**
   * Ticks whose row was replaced by a newer tick before it was rendered. No frames are lost: every
   * tick is received, counted and timed. The count increases when the push rate exceeds the display's
   * animation frame rate.
   */
  coalesced: 0,

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

/** The open history stream, kept so selecting another symbol can cancel it. */
let historyStream: Stream<HistoryPoint> | null = null;

/** When each symbol's stream last delivered a row (`performance.now()`), for the lateness readout. */
const lastTickAt = new Map<string, number>();

/** The worst lateness seen since the last sparkline sample. */
let windowLatencyMs = 0;

const throughput = { framesIn: 0, framesOut: 0, bytesIn: 0, bytesOut: 0 };

/**
 * Measurement state for one large payload: the fragments of the payload, and the data frames on other
 * streams received between its first and last fragment. This is the quantity that
 * `handlers_test.py::test_ticks_keep_flowing_during_an_export` asserts on. With a FIFO writer
 * `interleaved` is zero; with the round-robin writer it is not (WSM-INV-004).
 */
interface Probe {
  streamId: number;
  bytes: number;
  fragments: number;
  interleaved: number;
  worstLatencyMs: number;
}

let probe: Probe | null = null;

/** Set after the first board stream failure of a loss is logged; `store.streamsLost` holds the count. */
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
 * The WebSocket URL: `/ws` on the page's origin, forwarded by Vite's `ws: true` proxy entry.
 *
 * A credential belongs in the handshake, not in the hello: a ticket in the query string, or a cookie
 * the browser sends, checked before `accept()` runs (WSM-AUT-001). The demo sends no credential.
 */
function socketUrl(): string {
  const scheme = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${scheme}//${window.location.host}/ws`;
}

/**
 * An id for this browser tab, stored in `sessionStorage`, which is per tab and survives a reload.
 * `localStorage` would give every tab the same id; a module variable would not survive a reload.
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
    // The handler is passed to `connect()`, not set on the returned peer: the backend pushes the board
    // as soon as it reads the hello, and a handler registered one `await` later would see those twenty
    // streams reset with `REFUSED` (WSM-STM-033).
    peer = await connect(socketUrl(), {
      // Replayed unchanged on every new socket (WSM-RCN-020), so the subscription is restored after a
      // reconnection without reconnection-specific code on the accepting side.
      hello: { action: 'hello', tags: { app: 'muxws-demo', tab: tabId() } },
      // The demo caps the backoff at 2 s. The library default cap is 30 s, which spreads out
      // reconnecting clients after a server restart. With that cap the delays grow to 0.25, 0.5, 1, 2,
      // 4, 8, 16, 30 s, so after a backend that was stopped for half a minute restarts, the client can
      // wait up to another 30 s before its next attempt.
      reconnect: new Reconnect({ maxDelayMs: 2_000 }),
      onStream: onPushedStream,
      onClose: onConnectionClose,
      onReconnect: onReconnected,
    });
  } catch (error) {
    // WSM-RCN-006: the first connection attempt is not retried, whatever `reconnect` says, so a wrong
    // URL fails immediately instead of retrying indefinitely.
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
  // `willRetry` is true while the reconnect helper will dial again, and false once, when it has given
  // up or `close()` was called.
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
  // Reset, so `boardOpened` counts the streams of this connection only. No stream is resumed
  // (WSM-RCN-030): the board consists of twenty new streams the backend opens after the replayed
  // hello.
  store.boardOpened = 0;
  lossReported = false;
  lastTickAt.clear();
  say(`reconnected on attempt ${attempt}: hello replayed, peer.id is now ${reconnected.id}, no stream survived`);
  void refreshStats();
}

// --------------------------------------------------------------------------- the board (push)

/**
 * This dialer's `onStream` handler.
 *
 * The backend calls `peer.open()` twenty times, and each stream arrives here on the existing socket,
 * through the same `Peer` API used for requests (WSM-INV-002).
 */
async function onPushedStream(payload: unknown, stream: Stream): Promise<void> {
  const opening = payload as { topic?: unknown; symbol?: unknown };
  if (opening?.topic !== 'ticks' || typeof opening.symbol !== 'string') {
    // REFUSED, not APPLICATION_ERROR: this end has not processed the stream (WSM-STM-034).
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
    // WSM-RCN-030: every stream open at a connection loss fails with an error. The UI reports it,
    // because a board that stops updating looks the same as a market without price changes. Twenty
    // streams fail together, so each is counted and one line is logged.
    //
    // `ConnectionLost` means the socket closed under a live stream. A plain `StreamReset` means the
    // producer cancelled the stream first; the backend's kill switch does this before closing, because
    // board streams do not end by themselves and `close()` would wait for its full drain window. The
    // log line names which of the two occurred.
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

/**
 * Received rows not yet rendered, newest per symbol. Not reactive.
 *
 * Writing every tick directly into `store.rows` blocks the page at a 10 ms interval: twenty symbols at
 * a hundred ticks a second are two thousand reactive writes a second, each scheduling a re-render of
 * the grid, while the display repaints at most about sixty times a second. The main thread then cannot
 * process input, including the control that lowers the rate.
 *
 * No frame is dropped: every frame is received and counted and its lateness measured. Only rendering
 * is coalesced.
 */
const pending = new Map<string, BoardRow>();

/** Ticks that replaced a pending, not yet rendered row for the same symbol. */
let coalesced = 0;

let flushScheduled = false;

function applyTick(row: BoardRow): void {
  // Lateness is measured on arrival; measuring after rendering would include render time.
  const now = performance.now();
  const previous = lastTickAt.get(row.symbol);
  lastTickAt.set(row.symbol, now);
  if (previous !== undefined) noteLateness(now - previous - store.tickInterval * 1000);

  if (pending.has(row.symbol)) coalesced += 1;
  pending.set(row.symbol, row);
  scheduleFlush();
}

/**
 * Schedules one render per animation frame, regardless of how many ticks arrived in between.
 *
 * `requestAnimationFrame` runs at the display's refresh rate and does not run while the tab is hidden.
 * `pending` holds only the newest row per symbol, so a hidden tab accumulates no backlog.
 */
function scheduleFlush(): void {
  if (flushScheduled) return;
  flushScheduled = true;
  requestAnimationFrame(() => {
    flushScheduled = false;
    if (pending.size === 0) return;
    for (const [symbol, row] of pending) store.rows[symbol] = row;
    pending.clear();
    store.stale = false;
    store.coalesced = coalesced;
  });
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
  // Counted only after the first fragment has arrived, so frames received before the payload started
  // are excluded.
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
    // A connection-level round trip on a `ping` frame, not on a stream, in milliseconds
    // (WSM-CON-012).
    store.rttMs = Math.round((await peer.ping()) * 100) / 100;
  } catch {
    // A failed or unanswered ping clears the RTT readout and is not reported as an error; the
    // connection state is shown separately.
    store.rttMs = null;
  }
}

/**
 * Samples the tick-latency readout every `LATENCY_SAMPLE_MS`.
 *
 * The sample is the larger of two values. `windowLatencyMs` is the lateness of ticks that arrived
 * since the last sample, known only after a delayed tick arrives. The scan below computes how late
 * each symbol's next tick is at sampling time, so a delay is shown while it is in progress.
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
 * Requests the backend's `stats` over the same socket and stores the reply.
 *
 * `history_cancelled` is incremented by the backend's history generator when a cancellation stops it,
 * so it shows that the backend stopped generating (WSM-ERR-012/013).
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

/**
 * Sends a `rate` request that sets the backend's tick interval per symbol, in milliseconds.
 *
 * The default of four ticks a second per symbol is a `sleep` in the backend's generator, not a
 * transport limit.
 */
export async function setTickInterval(intervalMs: number): Promise<void> {
  if (peer === null || !peer.isOpen) return;
  try {
    await peer.request({ action: 'rate', interval_ms: intervalMs });
    store.tickInterval = intervalMs / 1000;
    say(`the backend now pushes every ${intervalMs} ms per symbol`);
  } catch (error) {
    if (!(error instanceof ConnectionLost)) say(`rate failed: ${describe(error)}`);
  }
}

// --------------------------------------------------------------------------- the four call shapes

/** Selects a symbol: cancels the open history stream, then sends a quote request and opens a history stream. */
export async function select(symbol: string): Promise<void> {
  // Cancelled first: until the reset is sent, the backend keeps generating points for the previously
  // selected symbol (WSM-ERR-012).
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
  // A no-op if the stream is already closed, including after a connection loss (WSM-RCN-041).
  await outstanding.cancel(reason);
  // Requested immediately instead of on the next poll, so the counter updates right after the click.
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
 * The streaming shape. `open()` is synchronous and returns the `Stream` in the same turn it allocates
 * the id (WSM-API-001); `end: true` half-closes the sending side.
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
    // Trailers are carried on the `end` frame and are readable after the body is complete.
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
 * Requests one payload larger than `MAX_FRAME_BYTES` and measures its fragments and the frames
 * interleaved with them.
 *
 * Uses an awaited `open()` instead of `request()` because the probe needs the stream id, which
 * `request()` does not expose. The frames are the same: `request()` is `open(payload, {end: true})`
 * awaited to the stream's end (WSM-API-006).
 */
async function fragmented<T>(action: string, symbol: string): Promise<{ payload: T; report: FragmentReport }> {
  if (peer === null) throw new ConnectionLost('there is no peer');
  const stream = peer.open<T>({ action, symbol }, { end: true });
  probe = { streamId: stream.id, bytes: 0, fragments: 0, interleaved: 0, worstLatencyMs: 0 };
  const startedAt = performance.now();
  try {
    // The peer reassembles the fragments and resolves with the whole payload (WSM-FRG-010).
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
 * Requests the 1 MB export on one stream while the twenty tick streams continue (WSM-INV-004).
 *
 * `report.interleaved` counts tick frames received between the export's first and last fragment. The
 * writer serves streams round-robin and a stream holds at most one unsent fragment at a time, so the
 * export's fragments alternate with tick frames. With a FIFO writer the count would be zero.
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

/** Sends a `kill` request; the backend closes every peer, and the reconnect helper reconnects. */
export async function killBackend(): Promise<void> {
  if (peer === null) return;
  try {
    const answer = await peer.request<{ closing: number }>({ action: 'kill' });
    say(`the backend is closing ${answer.closing} peer(s); every open stream is about to fail`);
  } catch (error) {
    say(`the kill switch failed: ${describe(error)}`);
  }
}
