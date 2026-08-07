/**
 * The codec seam: the port, the explicit registry, and `JsonCodec` (§2.1, WSM-CDC-001..016).
 *
 * This module does **not** register anything at import time (WSM-CDC-014). `ts/index.ts` registers
 * JSON, because the library is required to ship it registered (WSM-CDC-004) while a side-effecting
 * import can never be tree-shaken out.
 */

import { CodecNotRegistered, ProtocolError } from './errors';
import { type Frame, fromMapping, toMapping } from './frames';

/**
 * Turns a logical frame into a WebSocket message and back.
 *
 * `binary` is **declared, not inferred** (WSM-CDC-002): the peer reads it to choose the socket's
 * text or binary send method and the inbound message type it expects. A peer never sniffs a message
 * to decide which branch to take.
 *
 * `encodePayload` / `decodePayload` are the payload-level half of the port. WSM-CDC-001 names only
 * the frame-level pair, but fragmentation is defined in terms of the encoded form of a *logical
 * payload* (WSM-FRG-011) which the receiver hands back to the codec once reassembled (WSM-FRG-030),
 * and neither operation can be expressed through `encode`/`decode` alone. See GAPS.md.
 */
export interface Codec {
  readonly name: string;
  readonly binary: boolean;
  encode(frame: Frame): string | ArrayBuffer;
  decode(message: string | ArrayBuffer): Frame;
  encodePayload(payload: unknown): string | ArrayBuffer;
  decodePayload(data: string | ArrayBuffer): unknown;
}

const REGISTRY = new Map<string, Codec>();

/**
 * Register `codec` under `name`.
 *
 * Registration is explicit and eager (WSM-CDC-013): there is no dynamic import, no lazy
 * auto-registration, no entry-point scan, and no probing of whether a module happens to be
 * installed.
 */
export function registerCodec(name: string, codec: Codec): void {
  REGISTRY.set(name, codec);
}

/**
 * Return the codec registered under `name`.
 *
 * Throws `CodecNotRegistered` naming the environment variable, the value found and the registered
 * set (WSM-CDC-016). There is no fallback to JSON, ever - a deployment that believes it is running
 * msgpack and silently is not may never find out (WSM-INV-015).
 */
export function getCodec(name: string): Codec {
  const codec = REGISTRY.get(name);
  if (codec === undefined) {
    const available = registeredCodecs();
    throw new CodecNotRegistered(
      `codec '${name}' is not registered (VITE_MUXWS_CODEC='${name}'); registered codecs are ` +
        `[${available.map((entry) => `'${entry}'`).join(', ')}]. Call registerCodec('${name}', ...) ` +
        'during bootstrap, before connecting.',
      { configured: name, available },
    );
  }
  return codec;
}

/** Every registered codec name, sorted. */
export function registeredCodecs(): string[] {
  return [...REGISTRY.keys()].sort();
}

/** Test seam: forget every registration. Never called by the library itself. */
export function clearCodecs(): void {
  REGISTRY.clear();
}

/**
 * Refuse rather than invent an encoding.
 *
 * Bytes are a first-class payload type under a binary codec and are *not* one under JSON, and muxws
 * MUST NOT base64-encode them on the application's behalf (WSM-CDC-008). `JSON.stringify` would
 * quietly turn an `ArrayBuffer` into `{}`, which is worse than base64 - it loses the data in silence.
 */
function assertJsonEncodable(value: unknown, seen: Set<object> = new Set()): void {
  if (value === null || value === undefined) return;

  if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) {
    throw new TypeError(
      `the json codec cannot carry ${value.constructor.name}: bytes are a payload type only under a ` +
        'binary codec, and muxws does not base64-encode them for you (WSM-CDC-008). Either encode ' +
        'them in the application or configure the msgpack codec.',
    );
  }

  if (typeof value === 'number' && !Number.isFinite(value)) {
    throw new TypeError(`the json codec cannot encode ${value}: JSON has no representation for it`);
  }

  if (typeof value === 'bigint' || typeof value === 'function' || typeof value === 'symbol') {
    throw new TypeError(`the json codec cannot encode ${typeof value}`);
  }

  if (typeof value !== 'object') return;
  if (seen.has(value)) throw new TypeError('the json codec cannot encode a circular structure');
  seen.add(value);

  if (value instanceof Map || value instanceof Set) {
    throw new TypeError(`the json codec cannot encode ${value.constructor.name}`);
  }
  if (Array.isArray(value)) {
    value.forEach((item) => assertJsonEncodable(item, seen));
  } else {
    Object.values(value as Record<string, unknown>).forEach((item) => assertJsonEncodable(item, seen));
  }
  seen.delete(value);
}

/**
 * `Codec` implementation over `JSON`.
 *
 * `JSON.stringify` emits no spaces and does not escape non-ASCII, which is exactly what Python's
 * `json.dumps(..., separators=(",", ":"), ensure_ascii=False)` emits. WSM-FRG-016 requires both
 * ports to cut fragments at identical boundaries, and boundaries are computed over the encoded form
 * - so byte-level agreement here is a protocol requirement, not a formatting preference.
 */
export class JsonCodec implements Codec {
  readonly name = 'json';
  readonly binary = false;

  encode(frame: Frame): string {
    const mapping = toMapping(frame);
    assertJsonEncodable(mapping);
    return JSON.stringify(mapping);
  }

  decode(message: string | ArrayBuffer): Frame {
    const text = typeof message === 'string' ? message : decodeText(message);
    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch (error) {
      throw new ProtocolError(`the json codec could not decode the message: ${(error as Error).message} (WSM-FRM-005)`);
    }
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new ProtocolError(`a frame must decode to an object, got ${describe(parsed)} (WSM-FRM-005)`);
    }
    return fromMapping(parsed as Record<string, unknown>);
  }

  encodePayload(payload: unknown): string {
    assertJsonEncodable(payload);
    return JSON.stringify(payload) ?? 'null';
  }

  decodePayload(data: string | ArrayBuffer): unknown {
    const text = typeof data === 'string' ? data : decodeText(data);
    try {
      return JSON.parse(text);
    } catch (error) {
      throw new ProtocolError(`the json codec could not decode a reassembled payload: ${(error as Error).message}`);
    }
  }
}

function decodeText(buffer: ArrayBuffer): string {
  return new TextDecoder('utf-8', { fatal: false }).decode(buffer);
}

function describe(value: unknown): string {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'array';
  return typeof value;
}
