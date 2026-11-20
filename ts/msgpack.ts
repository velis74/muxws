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
 * `rawStrings: false` decodes a msgpack string to a `string` (Python's `raw=False`); `useBigInt64:
 * false` decodes int64 to a `number`, as the JSON codec does. Map keys decode as strings: `{1: 'a'}`
 * becomes `{'1': 'a'}`, unlike in Python.
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
 * Re-type every `ArrayBuffer` as a `Uint8Array` before encoding: the encoder writes a bare
 * `ArrayBuffer` as an empty map (WSM-CDC-008). Subtrees without bytes are returned as they are, not
 * copied.
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
 * Re-type every decoded `bin` (a `Uint8Array`) as an `ArrayBuffer`, this implementation's
 * representation of bytes (WSM-CDC-008).
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
