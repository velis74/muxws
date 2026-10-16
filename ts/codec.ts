/**
 * The codec interface, the explicit codec registry, and `JsonCodec` (§2.1, WSM-CDC-001..016).
 *
 * This module registers nothing at import time (WSM-CDC-014), so importing it has no side effects
 * and it can be tree-shaken. `ts/index.ts` registers JSON, which the library ships registered
 * (WSM-CDC-004).
 */

import { CodecNotRegistered, ProtocolError } from './errors';
import { type Frame, fromMapping, toMapping } from './frames';

/**
 * Turns a logical frame into a WebSocket message and back.
 *
 * `binary` is declared by the codec (WSM-CDC-002). The peer reads it to choose between the socket's
 * text and binary send methods and to know which inbound message type to expect; it does not
 * inspect message contents to decide.
 *
 * `encodePayload` / `decodePayload` encode and decode a payload on its own. WSM-CDC-001 names only
 * `encode` / `decode`, but fragmentation splits the encoded form of a logical payload (WSM-FRG-011)
 * and the receiver decodes the reassembled bytes (WSM-FRG-030); `encode` / `decode` operate on whole
 * frames and cannot do either.
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
 * Registration is explicit and eager (WSM-CDC-013). The library performs no dynamic imports, lazy
 * auto-registration, entry-point scans or checks for installed modules.
 */
export function registerCodec(name: string, codec: Codec): void {
  REGISTRY.set(name, codec);
}

/**
 * Return the codec registered under `name`.
 *
 * Throws `CodecNotRegistered` naming the environment variable, the value found and the registered
 * set (WSM-CDC-016). It never falls back to JSON (WSM-INV-015).
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

/** Remove every registration. For tests; the library never calls it. */
export function clearCodecs(): void {
  REGISTRY.clear();
}

/**
 * Throw `TypeError` for bytes, non-finite numbers, bigints, functions, symbols, `Map`, `Set` and
 * circular structures anywhere in `value`.
 *
 * Bytes are a payload type under a binary codec but not under JSON, and muxws does not base64-encode
 * them (WSM-CDC-008). Without this check `JSON.stringify` would encode an `ArrayBuffer` as `{}` and
 * drop its contents.
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
 * `JSON.stringify` emits no spaces and does not escape non-ASCII. For strings, containers, booleans,
 * null and integers up to 2**53 this is identical to Python's
 * `json.dumps(..., separators=(",", ":"), ensure_ascii=False)`. Floats and larger integers differ:
 * JavaScript writes `1`, `0`, `10000000000000000`, `1e-7` where Python writes `1.0`, `-0.0`,
 * `1e+16`, `1e-07`. Fragment boundaries are computed over this encoded form, so the two
 * implementations split a payload containing such numbers at different offsets. Interoperability is
 * unaffected (the sender chooses the boundaries and the receiver concatenates), but the shared
 * boundary corpus cannot contain such payloads.
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
