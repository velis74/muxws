/**
 * Subpath export `muxws/msgpack`: the msgpack binary codec (WSM-CDC-006/007/008).
 *
 * `@msgpack/msgpack` is an optional peer dependency, reachable only through this subpath
 * (WSM-PKG-003). The browser entry point does not import this file, so a bundle that does not import
 * `muxws/msgpack` does not contain msgpack. This module has no side effects and does not call
 * `registerCodec` (WSM-CDC-014); a module with import-time side effects cannot be tree-shaken
 * regardless of `"sideEffects": false`. The application calls
 * `registerCodec('msgpack', new MsgpackCodec())` during bootstrap. There is no dynamic import and no
 * check for whether the package is installed (WSM-CDC-013).
 *
 * Encoded bytes are not pinned in fixtures (WSM-CDC-006): this library and Python's `msgpack` choose
 * integer widths and map formats differently, and both encodings are valid. Tests assert round-trips
 * only.
 */

import { decode as msgpackDecode, encode as msgpackEncode, type DecoderOptions } from '@msgpack/msgpack';

import type { Codec } from './codec';
import { ProtocolError } from './errors';
import { type Frame, fromMapping, toMapping } from './frames';

export { VERSION } from './version';

/**
 * - `rawStrings: false` decodes a msgpack string to a `string`, not a `Uint8Array`; it corresponds to
 *   Python's `raw=False`. With `true`, envelope keys and `type` values would decode as bytes,
 *   `fromMapping` would find no `type`, and every inbound frame would be a protocol error.
 * - `useBigInt64: false` decodes int64 to a `number`, as the JSON codec does for a large integer.
 *   With `true` the result would be a `bigint`, which compares unequal to the `number` Python
 *   produces and which `JSON.stringify` cannot serialize. Integers beyond ±(2^53 − 1) are excluded
 *   from the corpus, as for JSON; see `conformance/README.md`.
 *
 * `mapKeyConverter` keeps its default, which accepts string and number keys; Python's
 * `strict_map_key=False` accepts the same. The decoded results differ: a JavaScript object key is a
 * string, so `{1: 'a'}` decodes as `{'1': 'a'}` here and as `{1: "a"}` in Python. The corpus contains
 * no such payload.
 */
const DECODE_OPTIONS: DecoderOptions = { rawStrings: false, useBigInt64: false };

/**
 * Copy the encoder's output out of its scratch buffer.
 *
 * `encode()` returns `encodeSharedRef()`, a subarray of the encoder's reused internal buffer (initially
 * 2048 bytes). Sending `bytes.buffer` directly would send the whole internal buffer, which the remote
 * rejects as trailing data, and would alias memory that the next `encode()` overwrites.
 */
function toBuffer(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
}

/**
 * Re-type every `ArrayBuffer` as a view before encoding.
 *
 * `Encoder.encodeObject` writes `bin` only when `ArrayBuffer.isView(object)`; a bare `ArrayBuffer` is
 * passed to `encodeMap` and written as an empty map, losing its contents (WSM-CDC-008). Under a
 * binary codec every `fragment` is an `ArrayBuffer`, so every fragment of a bytes payload would be
 * affected.
 *
 * Subtrees containing no bytes are returned unchanged, not copied. WSM-FRG-014's binary search
 * encodes the same frame O(log n) times per fragment boundary, and copying the payload on every
 * encode would add that many copies.
 */
function asViews(value: unknown): unknown {
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) return value;

  if (Array.isArray(value)) {
    const mapped = value.map((item) => asViews(item));
    return mapped.some((item, index) => item !== value[index]) ? mapped : value;
  }

  if (value !== null && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>);
    const mapped = entries.map(([key, item]) => [key, asViews(item)] as const);
    if (mapped.every(([, item], index) => item === entries[index][1])) return value;
    return Object.fromEntries(mapped);
  }

  return value;
}

/**
 * Re-type every decoded `bin` as an `ArrayBuffer`.
 *
 * `Codec.encode` returns `string | ArrayBuffer`, `Frame.fragment` is `string | ArrayBuffer | null`,
 * and `Assembler` concatenates `ArrayBuffer`s: this implementation represents bytes as
 * `ArrayBuffer`, while `@msgpack/msgpack` decodes `bin` to `Uint8Array`. `framesEqual` compares
 * `ArrayBuffer`s, not views, so without the conversion a round-tripped bytes payload would compare
 * unequal to the original (WSM-CDC-008).
 */
function asArrayBuffers(value: unknown): unknown {
  if (value instanceof Uint8Array) return toBuffer(value);
  if (Array.isArray(value)) return value.map((item) => asArrayBuffers(item));
  if (value !== null && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    Object.entries(value as Record<string, unknown>).forEach(([key, item]) => {
      out[key] = asArrayBuffers(item);
    });
    return out;
  }
  return value;
}

/**
 * Decode one msgpack value, turning every library failure into a `ProtocolError`.
 *
 * A conforming peer never sends a text message on a binary-codec connection: it chooses `sendBytes`
 * from the declared `binary` flag (WSM-CDC-002). A `string` here means the remote sent a text
 * message; the original bytes cannot be recovered from it, so it is a `ProtocolError`
 * (WSM-FRM-005).
 */
function unpack(data: string | ArrayBuffer, what: string): unknown {
  if (typeof data === 'string') {
    throw new ProtocolError(
      `the msgpack codec was handed a text ${what}: msgpack is a binary codec and a text WebSocket ` +
        'message cannot carry it (WSM-CDC-002, WSM-FRM-005)',
    );
  }
  try {
    return asArrayBuffers(msgpackDecode(data, DECODE_OPTIONS));
  } catch (error) {
    throw new ProtocolError(
      `the msgpack codec could not decode the ${what}: ${(error as Error).message} (WSM-FRM-005)`,
    );
  }
}

/** `Codec` implementation over `@msgpack/msgpack`. */
export class MsgpackCodec implements Codec {
  readonly name = 'msgpack';
  /**
   * Declared by the codec (WSM-CDC-002). The peer reads this to choose `sendBytes`; it does not
   * inspect the value `encode` returns.
   */
  readonly binary = true;

  encode(frame: Frame): ArrayBuffer {
    return toBuffer(msgpackEncode(asViews(toMapping(frame))));
  }

  decode(message: string | ArrayBuffer): Frame {
    const mapping = unpack(message, 'message');
    if (mapping === null || typeof mapping !== 'object' || Array.isArray(mapping) || mapping instanceof ArrayBuffer) {
      throw new ProtocolError(`a frame must decode to a map, got ${describe(mapping)} (WSM-FRM-005)`);
    }
    return fromMapping(mapping as Record<string, unknown>);
  }

  encodePayload(payload: unknown): ArrayBuffer {
    return toBuffer(msgpackEncode(asViews(payload)));
  }

  decodePayload(data: string | ArrayBuffer): unknown {
    return unpack(data, 'reassembled payload');
  }
}

function describe(value: unknown): string {
  if (value === null) return 'null';
  if (value instanceof ArrayBuffer) return 'bytes';
  if (Array.isArray(value)) return 'array';
  return typeof value;
}
