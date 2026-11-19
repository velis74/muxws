/**
 * Exchange log for Node: one JSON line per stream, with the request and the response paired.
 *
 * `attach(peer)` registers `onFrame` and `onClose`. A stream's line is written when both sides have
 * ended, when either side resets it (`"reset"`), or when the connection closes (`"lost"`). A oneway
 * stream is written once its opener has ended, with an empty response.
 *
 * Payloads may contain secrets: strip them with `redact`, and keep the log off in production unless it
 * is needed. The handlers run on the peer's read and write loops, so `write`, `redact` and `include`
 * must return quickly.
 */

import { performance } from 'node:perf_hooks';

import { ABSENT, Assembler, type Codec, type Frame, type Peer, logger } from 'muxws';

export type ExchangeSide = 'request' | 'response';

export interface ExchangeLogOptions {
  /** The codec the connection uses; it decodes payloads reassembled from fragments. */
  codec: Codec;
  /** Receives one finished JSON line, without a newline. */
  write: (line: string) => void;
  /** Longest logged payload in UTF-8 bytes of its JSON text; `null` never trims. Default 4096. */
  trimPayloadSize?: number | null;
  /** Payloads kept per side of a stream; further ones are counted in `dropped`. Default 100. */
  maxMessages?: number | null;
  /** Replaces a payload before it is serialised. */
  redact?: (payload: unknown, side: ExchangeSide) => unknown;
  /**
   * Decides from the opener's headers and first payload (reassembled when fragmented; `null` when the
   * open carries none or the stream ends before it is complete) whether the stream is logged.
   */
  include?: (headers: Record<string, unknown> | null, payload: unknown) => boolean;
  /** Monotonic time in milliseconds. Default `performance.now`. */
  clock?: () => number;
}

/** The part of `Peer` the log uses. */
export type ExchangePeer = Pick<Peer, 'id' | 'onFrame' | 'onClose'>;

interface SideState {
  headers: Record<string, unknown> | null;
  /** JSON text of each kept payload. */
  payloads: string[];
  trailers: Record<string, unknown> | null;
  dropped: number;
  bytes: number;
  ended: boolean;
  assembler: Assembler;
}

interface Exchange {
  ts: string;
  conn: string;
  stream: number;
  openedBy: 'local' | 'remote';
  oneway: boolean;
  started: number;
  firstResponse: number | null;
  /** The `include` verdict; `null` until the opening payload is complete. */
  included: boolean | null;
  request: SideState;
  response: SideState;
}

const ENCODER = new TextEncoder();
const DECODER = new TextDecoder();

function newSide(): SideState {
  return {
    headers: null,
    payloads: [],
    trailers: null,
    dropped: 0,
    bytes: 0,
    ended: false,
    assembler: new Assembler(),
  };
}

/** `JSON.stringify` replacer: bytes as base64, `bigint` as its decimal string. */
function jsonValue(this: unknown, key: string, value: unknown): unknown {
  const original = (this as Record<string, unknown>)[key];
  if (original instanceof ArrayBuffer) return Buffer.from(original).toString('base64');
  if (ArrayBuffer.isView(original)) {
    return Buffer.from(original.buffer, original.byteOffset, original.byteLength).toString('base64');
  }
  if (typeof value === 'bigint') return value.toString();
  return value;
}

/**
 * `text` cut to at most `limit` UTF-8 bytes on a character boundary, followed by
 * `…(trimmed N bytes)…`; `null` when `text` fits.
 */
export function trimText(text: string, limit: number): string | null {
  const bytes = ENCODER.encode(text);
  if (bytes.length <= limit) return null;
  let cut = limit;
  while (cut > 0 && (bytes[cut] & 0xc0) === 0x80) cut -= 1;
  return `${DECODER.decode(bytes.subarray(0, cut))}…(trimmed ${bytes.length - cut} bytes)…`;
}

function round(ms: number): number {
  return Math.round(ms * 1000) / 1000;
}

function hasPayload(frame: Frame): boolean {
  return frame.payload !== undefined && frame.payload !== ABSENT;
}

export class ExchangeLog {
  private readonly codec: Codec;

  private readonly write: (line: string) => void;

  private readonly trimPayloadSize: number | null;

  private readonly maxMessages: number | null;

  private readonly redact?: (payload: unknown, side: ExchangeSide) => unknown;

  private readonly include?: (headers: Record<string, unknown> | null, payload: unknown) => boolean;

  private readonly clock: () => number;

  constructor(options: ExchangeLogOptions) {
    this.codec = options.codec;
    this.write = options.write;
    this.trimPayloadSize = options.trimPayloadSize === undefined ? 4096 : options.trimPayloadSize;
    this.maxMessages = options.maxMessages === undefined ? 100 : options.maxMessages;
    this.redact = options.redact;
    this.include = options.include;
    this.clock = options.clock ?? (() => performance.now());
  }

  /** Log every stream of `peer`, on this connection and on every reconnected one. */
  attach(peer: ExchangePeer): void {
    const exchanges = new Map<number, Exchange>();
    peer.onFrame((direction, frame, byteLength) => {
      const id = frame.stream;
      if (id === null || id === undefined) return;
      try {
        this.onFrame(peer, exchanges, direction, frame, byteLength);
      } catch (error) {
        exchanges.delete(id);
        logger.warn(`exchange log dropped stream ${id} of conn=${peer.id}: ${String(error)}`);
      }
    });
    peer.onClose(() => {
      const lost = [...exchanges.values()];
      exchanges.clear();
      lost.forEach((exchange) => {
        try {
          this.finish(exchange, 'lost', null, null, this.clock());
        } catch (error) {
          logger.warn(`exchange log dropped stream ${exchange.stream} of conn=${exchange.conn}: ${String(error)}`);
        }
      });
    });
  }

  private onFrame(
    peer: ExchangePeer,
    exchanges: Map<number, Exchange>,
    direction: 'tx' | 'rx',
    frame: Frame,
    byteLength: number,
  ): void {
    const id = frame.stream as number;
    const now = this.clock();
    let exchange = exchanges.get(id);
    if (exchange === undefined) {
      if (frame.type !== 'open') return;
      exchange = {
        ts: new Date().toISOString(),
        conn: peer.id,
        stream: id,
        openedBy: direction === 'tx' ? 'local' : 'remote',
        oneway: frame.oneway === true,
        started: now,
        firstResponse: null,
        included: this.include === undefined ? true : null,
        request: newSide(),
        response: newSide(),
      };
      exchanges.set(id, exchange);
    }

    const isRequest = (direction === 'tx') === (exchange.openedBy === 'local');
    const sideName: ExchangeSide = isRequest ? 'request' : 'response';
    const side = isRequest ? exchange.request : exchange.response;
    side.bytes += byteLength;
    if (!isRequest && exchange.firstResponse === null) exchange.firstResponse = now;
    if (frame.headers && side.headers === null) side.headers = { ...frame.headers };

    if (frame.type === 'reset') {
      exchanges.delete(id);
      this.finish(exchange, 'reset', frame.code ?? null, frame.reason ?? null, now);
      return;
    }
    if (frame.type !== 'open' && frame.type !== 'data') return;

    const undecided = exchange.included === null && isRequest;
    const hasRoom = this.maxMessages === null || side.payloads.length < this.maxMessages;
    const fragmented = frame.fragment !== null && frame.fragment !== undefined;
    let payload: unknown = ABSENT;
    let complete = !fragmented && hasPayload(frame);
    if (!fragmented) {
      if (complete) payload = frame.payload;
    } else if ((exchange.included !== false && hasRoom) || undecided) {
      payload = side.assembler.feed(frame, this.codec);
      complete = payload !== ABSENT;
    } else {
      // A payload neither kept nor needed is not reassembled; it is counted when its last fragment arrives.
      complete = frame.more !== true;
    }

    if (undecided && frame.more !== true) {
      exchange.included = this.include?.(exchange.request.headers, payload === ABSENT ? null : payload) ?? true;
    }
    if (complete && exchange.included !== false) {
      if (hasRoom) side.payloads.push(this.render(payload, sideName));
      else side.dropped += 1;
    }

    if (frame.trailers) side.trailers = { ...frame.trailers };
    if (frame.end === true) side.ended = true;
    const done = exchange.oneway ? isRequest && frame.more !== true : exchange.request.ended && exchange.response.ended;
    if (done) {
      exchanges.delete(id);
      this.finish(exchange, 'ok', null, null, now);
    }
  }

  /** JSON text of `payload` after `redact`, trimmed to `trimPayloadSize`. */
  private render(payload: unknown, sideName: ExchangeSide): string {
    const value = this.redact ? this.redact(payload, sideName) : payload;
    const text = JSON.stringify(value, jsonValue) ?? 'null';
    if (this.trimPayloadSize === null) return text;
    const trimmed = trimText(text, this.trimPayloadSize);
    return trimmed === null ? text : JSON.stringify(trimmed);
  }

  private finish(
    exchange: Exchange,
    outcome: 'ok' | 'reset' | 'lost',
    code: number | null,
    reason: string | null,
    now: number,
  ): void {
    if (exchange.included === null) {
      // The stream ended before its opening payload was complete.
      exchange.included = this.include?.(exchange.request.headers, null) ?? true;
    }
    if (exchange.included === false) return;
    const firstResponse = exchange.firstResponse === null ? null : round(exchange.firstResponse - exchange.started);
    const fields: [string, string][] = [
      ['ts', JSON.stringify(exchange.ts)],
      ['conn', JSON.stringify(exchange.conn)],
      ['stream', JSON.stringify(exchange.stream)],
      ['opened_by', JSON.stringify(exchange.openedBy)],
      ['oneway', JSON.stringify(exchange.oneway)],
      ['request', sideJson(exchange.request)],
      ['response', sideJson(exchange.response)],
      ['outcome', JSON.stringify(outcome)],
      ['code', JSON.stringify(code)],
      ['reason', JSON.stringify(reason)],
      ['first_response_ms', JSON.stringify(firstResponse)],
      ['duration_ms', JSON.stringify(round(now - exchange.started))],
    ];
    this.write(`{${fields.map(([key, value]) => `"${key}":${value}`).join(',')}}`);
  }
}

function sideJson(side: SideState): string {
  return (
    `{"headers":${JSON.stringify(side.headers, jsonValue)},"payloads":[${side.payloads.join(',')}],` +
    `"trailers":${JSON.stringify(side.trailers, jsonValue)},"dropped":${side.dropped},"bytes":${side.bytes}}`
  );
}
