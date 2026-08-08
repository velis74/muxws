/**
 * Subpath export `muxws/msgpack`: the second codec, and the honest proof that the codec seam built
 * in M1 is a seam (WSM-CDC-006/007/008).
 *
 * `@msgpack/msgpack` is an **optional peer dependency** reachable only through this subpath
 * (WSM-PKG-003), which is the whole of the selection: the browser entry point never imports this
 * file, so a bundle that does not ask for msgpack never carries it. That only holds while this
 * module stays free of side effects - it does **not** call `registerCodec` (WSM-CDC-014), because a
 * side-effecting import can never be tree-shaken out no matter what `"sideEffects": false` claims.
 * The application calls `registerCodec('msgpack', new MsgpackCodec())` during bootstrap. There is no
 * dynamic import and no "is it installed" probe (WSM-CDC-013).
 *
 * Nothing here may be pinned as bytes in a fixture (WSM-CDC-006): this library and Python's
 * `msgpack` make different but equally valid choices about integer width and map format, and a
 * pinned-bytes fixture would make a legal encoder fail. Round-trip is the only assertion.
 */

import { decode as msgpackDecode, encode as msgpackEncode, type DecoderOptions } from '@msgpack/msgpack';

import type { Codec } from './codec';
import { ProtocolError } from './errors';
import { type Frame, fromMapping, toMapping } from './frames';

export { VERSION } from './version';

/**
 * - `rawStrings: false` decodes a msgpack string to a `string` rather than to a `Uint8Array`. It is
 *   the twin of Python's `raw=False`, and it is stated for the same reason: with it inverted every
 *   envelope key and every `type` value would arrive as bytes, `fromMapping` would find no `type`,
 *   and every inbound frame would be a protocol error.
 * - `useBigInt64: false` decodes int64 to a `number`, matching what the JSON codec already does with
 *   a large integer. `true` would hand back a `bigint`, which compares unequal to the `number` the
 *   Python port decodes and which `JSON.stringify` cannot render - a payload that survived msgpack
 *   would then be unloggable. Integers beyond ±(2^53 − 1) are outside the corpus for exactly the
 *   same reason they are outside the JSON corpus; see `conformance/README.md`.
 *
 * `mapKeyConverter` is left at its default, which accepts string and number keys. Python's
 * `strict_map_key=False` is what makes that symmetric. The two ports do *not* agree on the result -
 * a JavaScript object key is a string, so `{1: 'a'}` comes back as `{'1': 'a'}` here and as
 * `{1: "a"}` there - which is reported in `GAPS.md` and is why no such payload is in the corpus.
 */
const DECODE_OPTIONS: DecoderOptions = { rawStrings: false, useBigInt64: false };

/**
 * Copy the encoder's output out of its scratch buffer.
 *
 * `encode()` returns `encodeSharedRef()`: a **subarray of the encoder's internal buffer**, which is
 * 2048 bytes to begin with and is reused. Handing `bytes.buffer` to the socket would put the whole
 * scratch buffer on the wire for a twenty-byte frame - and the remote would reject it as trailing
 * data - while also aliasing memory the next `encode()` overwrites. The copy is the correctness.
 */
function toBuffer(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
}

/**
 * Re-type every `ArrayBuffer` as a view before encoding.
 *
 * `Encoder.encodeObject` writes `bin` only for `ArrayBuffer.isView(object)`; a **bare
 * `ArrayBuffer`** falls through to `encodeMap` and is written as an empty map. That is silent data
 * loss of exactly the kind WSM-CDC-008 exists to prevent, and it would hit every fragment of a bytes
 * payload, because `fragment` is an `ArrayBuffer` under a binary codec.
 *
 * Subtrees containing no bytes are returned unchanged rather than copied. WSM-FRG-014's binary
 * search encodes the same frame O(log n) times to find one cut, so a walk that rebuilt the payload
 * on every probe would copy it log n times per fragment.
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
 * and `Assembler` concatenates `ArrayBuffer`s - `ArrayBuffer` is the port's spelling of "bytes", so
 * a decoder that handed back the library's `Uint8Array` would leak its own vocabulary into every
 * one of those. `framesEqual` compares `ArrayBuffer`s and not views, so it would also make a
 * round-tripped bytes payload compare unequal to the one that went in (WSM-CDC-008).
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
 * A `string` never reaches a binary codec from a correct peer: `binary` is declared, and a peer
 * picks `sendBytes` from it rather than sniffing the encoded value (WSM-CDC-002). A text message
 * arriving here therefore means the remote sent one on a binary connection, and no recoding could
 * recover the bytes - the transport has already lost them. Saying so is the only honest answer
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
   * Declared, never inferred (WSM-CDC-002). The peer reads this to choose `sendBytes`; it does not
   * look at what `encode` returned.
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
