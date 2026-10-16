/**
 * Fragmentation: the splitter and the assembler, as pure functions (§4).
 *
 * Mirrors `muxws/fragment.py` line for line. Both implementations must split at the same boundaries
 * (WSM-FRG-016), so the reservation, the codepoint boundary and the binary search are computed the
 * same way and in the same order as in Python.
 *
 * This module performs no I/O.
 */

import type { Codec } from './codec';
import { ProtocolError } from './errors';
import { ABSENT, type Frame } from './frames';

/**
 * The largest encoded message a sender may emit, in bytes. A protocol constant: it is not negotiated,
 * announced or configurable (WSM-FRG-004). A receiver accepts messages up to this size and may accept
 * larger ones; a sender always fragments at this size.
 */
export const MAX_FRAME_BYTES = 65_536;

const TEXT_ENCODER = new TextEncoder();
/**
 * Decodes slices of an encoded payload. `ignoreBOM: true` keeps a leading U+FEFF; the default strips
 * it, so a fragment starting with that codepoint would lose a character and the two implementations
 * would carry different payloads (WSM-FRG-016). `fatal: true` makes a cut inside a multi-byte
 * sequence throw instead of yielding U+FFFD, as `bytes.decode("utf-8")` does in Python.
 */
const TEXT_DECODER = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });

/**
 * Byte length of an encoded message: UTF-8 for text, buffer length for bytes.
 *
 * `String.prototype.length` counts UTF-16 code units, which differs from the UTF-8 byte count for
 * any non-ASCII character; WSM-FRG-002 therefore requires `TextEncoder`. Measuring with `.length`
 * would let non-ASCII text exceed the cap on the wire.
 */
export function encodedLength(message: string | ArrayBuffer): number {
  if (typeof message === 'string') return TEXT_ENCODER.encode(message).length;
  return message.byteLength;
}

/** The envelope budget the sender sets aside before slicing (WSM-FRG-013). */
function reservation(cap: number): number {
  return Math.min(512, Math.floor(cap / 2));
}

/**
 * The encoded payload as bytes, plus whether cuts must fall on codepoint boundaries.
 *
 * Text and binary codecs both slice a byte array; only the indivisible unit differs (WSM-FRG-012).
 * Text is UTF-8-encoded once, and every offset is a byte offset into that encoding, the index space
 * both implementations use (WSM-FRG-016). Indexing a JavaScript string would use UTF-16 code units,
 * split surrogate pairs, and match neither the wire bytes nor the Python implementation.
 *
 * A text codec's encoded payload must be well-formed Unicode. `TextEncoder` replaces an unpaired
 * surrogate with U+FFFD, so the fragments carry the replacement character; Python cannot encode such
 * a payload. `JsonCodec` escapes lone surrogates as ASCII, so its output is always well-formed.
 */
export interface Encoded {
  bytes: Uint8Array;
  text: boolean;
}

function encodedForm(encoded: string | ArrayBuffer): Encoded {
  if (typeof encoded === 'string') return { bytes: TEXT_ENCODER.encode(encoded), text: true };
  return { bytes: new Uint8Array(encoded), text: false };
}

/** Rebuild a slice of `units` in the codec's own representation. */
function sliceUnits(units: Encoded, start: number, count: number): string | ArrayBuffer {
  if (units.text) return TEXT_DECODER.decode(units.bytes.subarray(start, start + count));
  return units.bytes.slice(start, start + count).buffer as ArrayBuffer;
}

/** A UTF-8 continuation byte: the second, third or fourth byte of a multi-byte sequence. */
function isContinuation(byte: number): boolean {
  return (byte & 0xc0) === 0x80;
}

/**
 * The largest cut at or before `start + limit` bytes that the codec can represent.
 *
 * Under a text codec the slice is a string value in the envelope (a JSON string), which cannot hold
 * part of a codepoint. WebSocket continuation frames could carry a split codepoint, but muxws does not
 * use them: a fragmented WebSocket message occupies the socket until it ends, which causes
 * head-of-line blocking (WSM-INV-004). A cut inside a multi-byte sequence therefore moves backwards to
 * the start of that sequence; UTF-8 is self-synchronising, so this takes at most three steps.
 *
 * Returns 0 when nothing fits: `limit` is narrower than the codepoint at `start`, or `start` is the
 * end of the encoding.
 *
 * Exported for the spec tests, which assert the boundary rule on this function directly, as the
 * Python tests do with `_floor_boundary`.
 */
export function boundaryWithin(units: Encoded, start: number, limit: number): number {
  let cut = Math.min(start + limit, units.bytes.length);
  if (units.text) {
    while (cut > start && cut < units.bytes.length && isContinuation(units.bytes[cut])) cut -= 1;
  }
  return cut - start;
}

/** The width in bytes of the indivisible unit at `start`: a byte, or a whole codepoint. */
function firstUnitWidth(units: Encoded, start: number): number {
  let cut = start + 1;
  if (units.text) {
    while (cut < units.bytes.length && isContinuation(units.bytes[cut])) cut += 1;
  }
  return cut - start;
}

/** How many bytes at `start` fit into `budget`, always at least one indivisible unit. */
function unitsWithinBudget(units: Encoded, start: number, budget: number): number {
  return boundaryWithin(units, start, budget) || firstUnitWidth(units, start);
}

/**
 * Build one fragment frame.
 *
 * `headers` are carried only on the first fragment and are never split (WSM-FRG-021); `end` and
 * `trailers` only on the last (WSM-FRG-020); `more` is true on every fragment except the last.
 */
function fragmentFrame(source: Frame, chunk: string | ArrayBuffer, options: { first: boolean; last: boolean }): Frame {
  return {
    type: source.type,
    stream: source.stream ?? null,
    payload: ABSENT,
    fragment: chunk,
    more: !options.last,
    headers: options.first ? (source.headers ?? null) : null,
    end: options.last ? (source.end ?? false) : false,
    trailers: options.last ? (source.trailers ?? null) : null,
    code: source.code ?? null,
    reason: source.reason ?? null,
    nonce: source.nonce ?? null,
    last_stream: source.last_stream ?? null,
  };
}

/**
 * The error for a closing fragment that exceeds the cap even with an empty payload slice.
 *
 * `end` and `trailers` are carried on the final fragment and cannot be fragmented (WSM-FRG-020), as
 * `headers` cannot (WSM-FRG-021). If they do not fit, no valid split exists; without this error the
 * splitter would not terminate.
 */
function closingFloorError(cap: number): ProtocolError {
  return new ProtocolError(
    `a frame cap of ${cap} bytes cannot hold this frame's closing envelope; \`trailers\` ride the ` +
      'final fragment whole and are never fragmented (WSM-FRG-020/034)',
  );
}

function floorError(cap: number): ProtocolError {
  return new ProtocolError(
    `a frame cap of ${cap} bytes cannot hold this frame's envelope plus one indivisible unit of ` +
      'payload; raise the cap (WSM-FRG-034)',
  );
}

/**
 * Largest slice at `position` whose non-final fragment frame still fits under `cap`, in bytes.
 *
 * The verify-and-re-split step of WSM-FRG-014, as a binary search. It performs at most
 * log2(ceiling) encodes and is deterministic, so both implementations cut at the same boundary
 * (WSM-FRG-016). The search bisects byte limits and floors each probe to a representable boundary,
 * so the slice width is monotonic in the byte limit.
 *
 * Returns 0 when not even one unit fits: probes narrower than the first unit floor to an empty slice,
 * which is not recorded.
 */
function largestFittingCount(
  source: Frame,
  units: Encoded,
  position: number,
  ceiling: number,
  options: { first: boolean; cap: number; codec: Codec },
): number {
  let low = 1;
  let high = ceiling;
  let best = 0;
  while (low <= high) {
    const middle = Math.floor((low + high) / 2);
    const count = boundaryWithin(units, position, middle);
    const probe = fragmentFrame(source, sliceUnits(units, position, count), { first: options.first, last: false });
    if (encodedLength(options.codec.encode(probe)) <= options.cap) {
      best = count;
      low = middle + 1;
    } else {
      high = middle - 1;
    }
  }
  return best;
}

/**
 * All fragments as an array: `iterFragments` collected eagerly. Used by the pure-function tests and
 * the conformance corpus.
 */
export function splitFrame(frame: Frame, cap: number = MAX_FRAME_BYTES, codec?: Codec): Frame[] {
  return [...iterFragments(frame, cap, codec)];
}

/**
 * Yield `frame` if it fits under `cap`, otherwise the fragment frames that replace it, lazily.
 *
 * The writer consumes one fragment at a time, because a stream holds at most one unsent fragment
 * (WSM-FRG-018): fragment n+1 is sliced only after fragment n has been passed to the socket. Slicing
 * all fragments up front would fix the wire order and prevent interleaving (WSM-FRG-019).
 *
 * `splitFrame` and the writer both use this generator, so they split at the same boundaries.
 *
 * A pure function of `(frame, cap, codec)` (WSM-FRG-015): it reads no other state and mutates neither
 * argument. `cap` defaults to the protocol constant; only tests and the conformance runner pass a
 * smaller value (WSM-FRG-005), and it is never read from the wire.
 *
 * The payload is encoded with the codec, the encoded form is sliced, and each slice is placed in a
 * frame that the codec encodes again (WSM-FRG-011). Each slice is sized with an envelope reservation
 * (WSM-FRG-013) and then verified and re-split if it exceeds the cap (WSM-FRG-014).
 */
export function* iterFragments(frame: Frame, cap: number = MAX_FRAME_BYTES, codec?: Codec): Generator<Frame> {
  if (codec === undefined) {
    throw new ProtocolError('splitFrame requires a codec: fragment boundaries are defined over its output');
  }
  if (cap < 2) {
    throw new ProtocolError(`a frame cap of ${cap} bytes is too small to hold any frame (WSM-FRG-034)`);
  }

  if (encodedLength(codec.encode(frame)) <= cap) {
    yield frame;
    return;
  }

  if (frame.payload === ABSENT || frame.payload === undefined) {
    throw new ProtocolError(
      `frame '${frame.type}' exceeds the ${cap}-byte cap but carries no payload to fragment; ` +
        'headers are never fragmented (WSM-FRG-021)',
    );
  }

  const units = encodedForm(codec.encodePayload(frame.payload));
  const total = units.bytes.length;
  const budget = Math.max(1, cap - reservation(cap));

  let position = 0;
  let emitted = 0;

  for (;;) {
    // First, try the whole remainder as the closing fragment. It carries `end` and `trailers`, so it
    // is measured separately from a middle fragment. This runs on every pass, including when the
    // payload is fully consumed: the sequence ends with a `more: false` fragment, which may carry no
    // bytes.
    const first = emitted === 0;
    // The encoded closing frame is never shorter than the remainder, so the encode is skipped when
    // the remainder alone exceeds the cap. Encoding it on every pass would be quadratic in payload
    // size.
    if (total - position <= cap) {
      const tail = fragmentFrame(frame, sliceUnits(units, position, total - position), { first, last: true });
      if (encodedLength(codec.encode(tail)) <= cap) {
        yield tail;
        return;
      }
    }

    if (position >= total) throw closingFloorError(cap);

    // Otherwise a middle fragment, sized with the envelope reservation (WSM-FRG-013),
    let count = unitsWithinBudget(units, position, budget);
    let candidate = fragmentFrame(frame, sliceUnits(units, position, count), { first, last: false });

    // then verified and re-split if the encoded frame exceeds the cap (WSM-FRG-014).
    if (encodedLength(codec.encode(candidate)) > cap) {
      count = largestFittingCount(frame, units, position, count, { first, cap, codec });
      if (count === 0) throw floorError(cap);
      candidate = fragmentFrame(frame, sliceUnits(units, position, count), { first, last: false });
    }

    yield candidate;
    emitted += 1;
    position += count;
  }
}

/**
 * Receive side: concatenates `fragment` values and decodes the payload after the last one.
 *
 * `feed()` returns `ABSENT` while `more` is true, and the decoded payload on the frame that closes
 * the sequence (WSM-FRG-030).
 */
export class Assembler {
  private parts: (string | ArrayBuffer)[] = [];
  private bytes = 0;

  /** True from the first fragment until the fragment without `more`. */
  get inProgress(): boolean {
    return this.parts.length > 0;
  }

  /**
   * Bytes accumulated so far.
   *
   * `maxPayloadBytes` is enforced against this value as fragments arrive, not after reassembly
   * (WSM-FRG-032), so an oversized payload is rejected before it is fully buffered.
   */
  get byteLength(): number {
    return this.bytes;
  }

  /** Drop the partial buffer. Called when the stream is reset. */
  reset(): void {
    this.parts = [];
    this.bytes = 0;
  }

  /**
   * Add one fragment frame; return `ABSENT` while more are expected, else the decoded payload.
   *
   * Throws `ProtocolError` when the fragments mix text and binary values, or when the codec cannot
   * decode the joined payload. The partial buffer is dropped in both cases.
   */
  feed(frame: Frame, codec: Codec): unknown {
    const { fragment } = frame;
    if (fragment === null || fragment === undefined) {
      throw new ProtocolError('Assembler.feed was given a frame carrying no fragment');
    }
    if (this.parts.length > 0 && typeof fragment !== typeof this.parts[0]) {
      this.reset();
      throw new ProtocolError('the fragments of one payload mix text and binary values');
    }
    this.parts.push(fragment);
    this.bytes += encodedLength(fragment);
    if (frame.more === true) return ABSENT;

    const { parts } = this;
    const joined = typeof parts[0] === 'string' ? parts.join('') : concatBuffers(parts as ArrayBuffer[]);
    this.reset();
    return codec.decodePayload(joined);
  }
}

function concatBuffers(buffers: ArrayBuffer[]): ArrayBuffer {
  const total = buffers.reduce((sum, buffer) => sum + buffer.byteLength, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  buffers.forEach((buffer) => {
    out.set(new Uint8Array(buffer), offset);
    offset += buffer.byteLength;
  });
  return out.buffer;
}
