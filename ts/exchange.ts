/**
 * `Exchange`: the record of one finished stream that `peer.onExchange` handlers receive.
 *
 * The same record as Python's `muxws.Exchange`. Payloads are the logical values (reassembled and decoded), never
 * fragments. Durations are milliseconds.
 */

import { ConnectionLost, type StreamReset } from './errors';

/** How a stream ended: both sides ended, a reset, or a lost socket. */
export type ExchangeOutcome = 'ok' | 'reset' | 'lost';

/** This peer or the remote. */
export type ExchangeParty = 'local' | 'remote';

/** What one side of a stream sent. */
export interface ExchangeSide {
  /** The side's leading headers; `null` when it sent none. */
  readonly headers: Record<string, unknown> | null;
  /** The side's payloads in order, cut to the handler's `maxPayloads`. */
  readonly payloads: readonly unknown[];
  /** How many payloads the side sent in total, including the ones not in `payloads`. */
  readonly payloadCount: number;
  /** The trailers of the side's end frame; `null` when it sent none. */
  readonly trailers: Record<string, unknown> | null;
  /** Whether the side sent `end`. */
  readonly ended: boolean;
}

/** `onExchange()`'s options object. */
export interface ExchangeOptions {
  /** How many payloads per side the handler receives; `null` for all. Defaults to 100. */
  maxPayloads?: number | null;
}

/** `asDict()`'s options object. */
export interface ExchangeDictOptions {
  /** The UTF-8 byte length above which a payload's JSON text is trimmed; `null` (default) for no trimming. */
  trimPayloadSize?: number | null;
}

/** One side of `asDict()`'s result. */
export type ExchangeSideDict = {
  headers: Record<string, unknown> | null;
  payloads: unknown[];
  payload_count: number;
  trailers: Record<string, unknown> | null;
  ended: boolean;
};

/**
 * `asDict()`'s result: the same keys, in the same order, as Python's `Exchange.as_dict()`.
 *
 * A type alias, not an interface, so it is assignable to `Record<string, unknown>`.
 */
export type ExchangeDict = {
  ts: string;
  conn: string;
  stream: number;
  opened_by: ExchangeParty;
  oneway: boolean;
  request: ExchangeSideDict;
  response: ExchangeSideDict;
  outcome: ExchangeOutcome;
  reset_by: ExchangeParty | null;
  code: number | null;
  reason: string | null;
  first_response_ms: number | null;
  duration_ms: number;
};

/** The fields of an `Exchange`. */
export interface ExchangeInit {
  conn: string;
  stream: number;
  openedBy: ExchangeParty;
  oneway: boolean;
  request: ExchangeSide;
  response: ExchangeSide;
  outcome: ExchangeOutcome;
  resetBy: ExchangeParty | null;
  code: number | null;
  reason: string | null;
  startedAt: Date;
  firstResponseMs: number | null;
  durationMs: number;
}

const ENCODER = new TextEncoder();
const DECODER = new TextDecoder();

/** One finished stream: what each side sent, how it ended and how long it took. Immutable. */
export class Exchange {
  /** `peer.id` of the connection the stream lived on. */
  readonly conn: string;
  readonly stream: number;
  readonly openedBy: ExchangeParty;
  readonly oneway: boolean;
  /** What the side that opened the stream sent. */
  readonly request: ExchangeSide;
  /** What the other side sent. */
  readonly response: ExchangeSide;
  readonly outcome: ExchangeOutcome;
  /** Who sent or caused the reset; `null` unless `outcome` is `'reset'`. */
  readonly resetBy: ExchangeParty | null;
  /** The reset code; `null` unless `outcome` is `'reset'`. */
  readonly code: number | null;
  /** The reset reason; `null` unless `outcome` is `'reset'`. */
  readonly reason: string | null;
  /** Wall-clock time at which the stream was created. */
  readonly startedAt: Date;
  /**
   * From the stream's creation to the answering side's first `data` frame (a reset does not count);
   * `null` when it sent none.
   */
  readonly firstResponseMs: number | null;
  /** From the stream's creation to the moment the exchange fired. */
  readonly durationMs: number;

  constructor(init: ExchangeInit) {
    this.conn = init.conn;
    this.stream = init.stream;
    this.openedBy = init.openedBy;
    this.oneway = init.oneway;
    this.request = Object.freeze({ ...init.request, payloads: Object.freeze([...init.request.payloads]) });
    this.response = Object.freeze({ ...init.response, payloads: Object.freeze([...init.response.payloads]) });
    this.outcome = init.outcome;
    this.resetBy = init.resetBy;
    this.code = init.code;
    this.reason = init.reason;
    this.startedAt = new Date(init.startedAt.getTime());
    this.firstResponseMs = init.firstResponseMs;
    this.durationMs = init.durationMs;
    Object.freeze(this);
  }

  /**
   * A fresh JSON-ready object. Binary values become base64 strings; with `trimPayloadSize`, a payload
   * whose JSON text is longer than that many UTF-8 bytes is replaced by its trimmed text.
   *
   * Throws `RangeError` for a negative or non-integer `trimPayloadSize`.
   */
  asDict(options: ExchangeDictOptions = {}): ExchangeDict {
    const limit = options.trimPayloadSize ?? null;
    if (limit !== null && !(Number.isInteger(limit) && limit >= 0)) {
      throw new RangeError(`trimPayloadSize must be a non-negative integer or null, not ${String(limit)}`);
    }
    return {
      ts: this.startedAt.toISOString(),
      conn: this.conn,
      stream: this.stream,
      opened_by: this.openedBy,
      oneway: this.oneway,
      request: sideDict(this.request, limit),
      response: sideDict(this.response, limit),
      outcome: this.outcome,
      reset_by: this.resetBy,
      code: this.code,
      reason: this.reason,
      first_response_ms: this.firstResponseMs === null ? null : roundMs(this.firstResponseMs),
      duration_ms: roundMs(this.durationMs),
    };
  }
}

function roundMs(value: number): number {
  return Math.round(value * 1000) / 1000;
}

function sideDict(side: ExchangeSide, limit: number | null): ExchangeSideDict {
  return {
    headers: side.headers === null ? null : (jsonReady(side.headers) as Record<string, unknown>),
    payloads: side.payloads.map((payload) => {
      const ready = jsonReady(payload);
      return limit === null ? ready : trimmed(ready, limit);
    }),
    payload_count: side.payloadCount,
    trailers: side.trailers === null ? null : (jsonReady(side.trailers) as Record<string, unknown>),
    ended: side.ended,
  };
}

/** A copy of `value` with arrays and plain objects copied and binary values as base64 strings. */
function jsonReady(value: unknown): unknown {
  if (value instanceof ArrayBuffer) return base64(new Uint8Array(value));
  if (ArrayBuffer.isView(value)) return base64(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
  if (Array.isArray(value)) return value.map(jsonReady);
  if (isPlainObject(value)) {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, jsonReady(item)]));
  }
  return value;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== 'object') return false;
  const prototype = Object.getPrototypeOf(value) as unknown;
  return prototype === Object.prototype || prototype === null;
}

function base64(bytes: Uint8Array): string {
  let binary = '';
  for (let start = 0; start < bytes.length; start += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(start, start + 0x8000));
  }
  return globalThis.btoa(binary);
}

/**
 * `value` itself when its compact JSON text is at most `limit` UTF-8 bytes; otherwise the first
 * `limit` bytes of that text, cut back to a character boundary, followed by `…(trimmed N bytes)…`.
 */
function trimmed(value: unknown, limit: number): unknown {
  const bytes = ENCODER.encode(jsonText(value));
  if (bytes.length <= limit) return value;
  let cut = limit;
  // A UTF-8 continuation byte (10xxxxxx) at `cut` means a character starts before it.
  while (cut > 0 && ((bytes[cut] as number) & 0xc0) === 0x80) cut -= 1;
  return `${DECODER.decode(bytes.subarray(0, cut))}…(trimmed ${bytes.length - cut} bytes)…`;
}

/**
 * `JSON.stringify(value)` with each `bigint` written as the string `String(value)`, as Python's
 * `default=str` writes a value json cannot serialise; `String(value)` for a circular reference or a
 * value `JSON.stringify` writes as nothing.
 */
function jsonText(value: unknown): string {
  try {
    const text = JSON.stringify(value, bigintAsString) as string | undefined;
    if (text !== undefined) return text;
  } catch {
    // A circular reference: falls through to `String(value)`, as Python falls back to `str(payload)`.
  }
  return String(value);
}

function bigintAsString(_key: string, item: unknown): unknown {
  return typeof item === 'bigint' ? item.toString() : item;
}

/** @internal `exchange` with each side's payloads cut to the first `maxPayloads`; `null` keeps all. */
export function limitExchange(exchange: Exchange, maxPayloads: number | null): Exchange {
  if (
    maxPayloads === null ||
    (exchange.request.payloads.length <= maxPayloads && exchange.response.payloads.length <= maxPayloads)
  ) {
    return exchange;
  }
  const cut = (side: ExchangeSide): ExchangeSide => ({ ...side, payloads: side.payloads.slice(0, maxPayloads) });
  return new Exchange({ ...exchange, request: cut(exchange.request), response: cut(exchange.response) });
}

/** @internal Throw `RangeError` unless `maxPayloads` is a non-negative integer or `null`. */
export function checkMaxPayloads(maxPayloads: number | null): void {
  if (maxPayloads !== null && !(Number.isInteger(maxPayloads) && maxPayloads >= 0)) {
    throw new RangeError(`maxPayloads must be a non-negative integer or null, not ${String(maxPayloads)}`);
  }
}

/** @internal What one side has sent so far. */
export class SideRecord {
  headers: Record<string, unknown> | null = null;
  readonly payloads: unknown[] = [];
  payloadCount = 0;
  trailers: Record<string, unknown> | null = null;
  ended = false;

  constructor(private readonly limit: number | null) {}

  payload(value: unknown): void {
    this.payloadCount += 1;
    if (this.limit === null || this.payloads.length < this.limit) this.payloads.push(value);
  }

  end(trailers: Record<string, unknown> | null | undefined): void {
    this.ended = true;
    if (trailers !== null && trailers !== undefined) this.trailers = trailers;
  }
}

/**
 * @internal Records one stream while it is live: `local` is what this peer sent, `remote` what it
 * received. Created by the peer with the stream, and only while an `onExchange` handler is registered.
 */
export class ExchangeRecorder {
  readonly local: SideRecord;
  readonly remote: SideRecord;
  private readonly startedAt = new Date();
  private readonly startMs = globalThis.performance.now();
  private firstResponseMs: number | null = null;
  private cause: 'normal' | StreamReset = 'normal';
  private resetBy: ExchangeParty = 'local';

  constructor(
    private readonly conn: string,
    limit: number | null,
  ) {
    this.local = new SideRecord(limit);
    this.remote = new SideRecord(limit);
  }

  /** Stamp the answering side's first `data` frame; later calls are ignored. */
  answered(): void {
    if (this.firstResponseMs === null) this.firstResponseMs = globalThis.performance.now() - this.startMs;
  }

  /** How the stream closed: `'normal'`, or the failure and which side reset it. */
  settle(cause: 'normal' | StreamReset, resetBy: ExchangeParty = 'local'): void {
    this.cause = cause;
    this.resetBy = resetBy;
  }

  /** The `Exchange` of the closed stream, with every recorded payload. */
  finish(stream: { readonly id: number; readonly local: boolean; readonly oneway: boolean }): Exchange {
    const durationMs = globalThis.performance.now() - this.startMs;
    const opener = stream.local ? this.local : this.remote;
    const answerer = stream.local ? this.remote : this.local;
    let outcome: ExchangeOutcome = 'ok';
    if (this.cause !== 'normal') {
      outcome = this.resetBy === 'local' && this.cause instanceof ConnectionLost ? 'lost' : 'reset';
    }
    const reset = outcome === 'reset' && this.cause !== 'normal' ? this.cause : null;
    return new Exchange({
      conn: this.conn,
      stream: stream.id,
      openedBy: stream.local ? 'local' : 'remote',
      oneway: stream.oneway,
      request: sideOf(opener),
      response: sideOf(answerer),
      outcome,
      resetBy: reset === null ? null : this.resetBy,
      code: reset === null ? null : reset.code,
      reason: reset === null ? null : reset.reason,
      startedAt: this.startedAt,
      firstResponseMs: this.firstResponseMs,
      durationMs,
    });
  }
}

function sideOf(record: SideRecord): ExchangeSide {
  return {
    headers: record.headers,
    payloads: record.payloads,
    payloadCount: record.payloadCount,
    trailers: record.trailers,
    ended: record.ended,
  };
}
