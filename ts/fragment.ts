/**
 * Fragmentation: the splitter and the assembler, as pure functions (§4).
 *
 * A line-for-line mirror of `muxws/fragment.py`. The two ports must cut at the *same* boundaries
 * (WSM-FRG-016), which is why every decision here - the reservation, the codepoint boundary, the
 * binary search - is made the same way and in the same order as it is there.
 *
 * Nothing here touches a socket.
 */

import type { Codec } from './codec';
import { ProtocolError } from './errors';
import { ABSENT, type Frame } from './frames';

/**
 * The largest encoded message a sender may emit. A **protocol constant**, not a setting: it is never
 * negotiated, never announced, and never read from configuration (WSM-FRG-004). A receiver accepts
 * anything up to it and may accept more; a sender always fragments at it regardless of what the
 * remote appears willing to accept.
 */
export const MAX_FRAME_BYTES = 65_536;

const TEXT_ENCODER = new TextEncoder();
/**
 * Both options are load-bearing for a decoder that reads a *slice of a payload* rather than a
 * document. `ignoreBOM: true` keeps a leading U+FEFF: the default strips one, so a fragment
 * beginning with that codepoint would arrive one character short and the two ports would carry
 * different payloads (WSM-FRG-016). `fatal: true` makes a cut inside a multi-byte sequence throw
 * instead of yielding U+FFFD, which is what `bytes.decode("utf-8")` does in the Python port.
 */
const TEXT_DECODER = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });

/**
 * Byte length of an encoded message: UTF-8 for text, buffer length for bytes.
 *
 * `String.prototype.length` counts UTF-16 code units and disagrees with Python on every non-BMP
 * character, which is why WSM-FRG-002 names `TextEncoder` explicitly. Using `.length` here would
 * make a 1 MB emoji payload silently over-cap on the wire.
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
 * The encoded payload as bytes, plus whether cuts into it have to respect codepoint boundaries.
 *
 * Both codec families slice the same array; only the indivisible unit differs (WSM-FRG-012). Text is
 * encoded once, here, and every offset from then on is a byte offset into that encoding, which is
 * the index space the two ports agree on (WSM-FRG-016). Indexing a JavaScript string directly would
 * index UTF-16 code units instead, which cuts surrogate pairs in half and matches neither the bytes
 * on the wire nor the other port.
 *
 * A text codec's encoded payload has to be well-formed Unicode. `TextEncoder` writes U+FFFD where an
 * unpaired surrogate stands, so the fragments carry the replacement; the Python port cannot encode
 * such a payload at all. `JsonCodec` escapes lone surrogates to ASCII and never reaches this.
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
 * Under a text codec the slice sits in the envelope as a value of the codec's own type system - a
 * JSON string - and half a codepoint has no representation in one. WebSocket continuation frames
 * would carry a split codepoint, but muxws does not use them: a continuation sequence holds the
 * socket until the message ends, which is the head-of-line blocking the library exists to prevent
 * (WSM-INV-004). So a cut landing inside a multi-byte sequence walks backwards until it does not.
 * UTF-8 is self-synchronising, which bounds that walk at three steps.
 *
 * Returns 0 when nothing fits: `limit` is narrower than the codepoint at `start`, or `start` is
 * already the end of the encoding.
 *
 * Exported for the spec, which asserts the boundary rule on the primitive the splitter itself calls;
 * the Python port's tests reach `_floor_boundary` the same way.
 */
export function boundaryWithin(units: Encoded, start: number, limit: number): number {
  let cut = Math.min(start + limit, units.bytes.length);
  if (units.text) {
    while (cut > start && cut < units.bytes.length && isContinuation(units.bytes[cut])) cut -= 1;
  }
  return cut - start;
}

/** The width in bytes of the one indivisible unit at `start`: a byte, or a whole codepoint. */
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
 * `headers` ride only the first fragment and are never split (WSM-FRG-021); `end` and `trailers`
 * ride only the last (WSM-FRG-020); `more` is true on every fragment but the last.
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
 * The closing fragment does not fit even carrying no payload at all.
 *
 * `end` and `trailers` ride the final fragment and cannot themselves be fragmented (WSM-FRG-020),
 * exactly as `headers` cannot (WSM-FRG-021). If they do not fit, no valid split exists and saying so
 * is the only honest answer - the alternative is a sequence that never terminates.
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
 * The verify-and-re-split half of WSM-FRG-014, as a **binary search**: it terminates in
 * log2(ceiling) encodes, and - more importantly - it is exactly reproducible, so this port and the
 * Python one cut at the same boundary (WSM-FRG-016). The search bisects byte limits and floors each
 * probe onto a boundary the codec can represent, so what it counts and what it cuts are the same
 * quantity: a wider byte limit never yields a narrower slice, and a slice that does not fit is never
 * a prefix of one that does.
 *
 * Returns 0 when not even one unit fits - the probes below the first unit's width all floor back to
 * an empty slice, which fits and records nothing.
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
 * Every fragment at once. `iterFragments` is the same computation, one slice at a time.
 *
 * Kept because the pure-function tests and the conformance corpus want the whole list, and because a
 * caller with a small payload should not have to think about generators.
 */
export function splitFrame(frame: Frame, cap: number = MAX_FRAME_BYTES, codec?: Codec): Frame[] {
  return [...iterFragments(frame, cap, codec)];
}

/**
 * Yield `frame` when it already fits, else the fragment frames that replace it, **lazily**.
 *
 * The writer consumes this one slice at a time, because WSM-FRG-018 says a stream holds at most one
 * unsent fragment: fragment *n+1* is sliced only once fragment *n* has been handed to the socket.
 * Computing them all up front would commit the wire order in advance, and interleaving - the whole
 * point of WSM-FRG-019 - becomes impossible once the order is already decided.
 *
 * Every boundary decision lives here, so `splitFrame` and the writer cut in exactly the same places
 * by construction rather than by two implementations agreeing.
 *
 * A pure function of `(frame, cap, codec)` (WSM-FRG-015): it reads nothing else and mutates neither
 * argument. `cap` defaults to the protocol constant; a smaller value comes only from a test or the
 * conformance runner (WSM-FRG-005) and is never a value read off the wire.
 *
 * The sender encodes the logical payload with the codec, slices *that* encoded form, and puts each
 * slice into a frame the codec then encodes again (WSM-FRG-011). Slicing budgets for the envelope
 * (WSM-FRG-013) and then **verifies and re-splits** (WSM-FRG-014): the reservation is a per-codec
 * hint, the loop is the guarantee.
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
    // Everything left, as the closing fragment? `end` and `trailers` ride only this one, so it is a
    // different size from a middle fragment and has to be measured as itself. This is checked first
    // on every pass, including the one where the payload is already spent: the sequence MUST end
    // with a fragment carrying `more: false`, even if that fragment carries no bytes.
    const first = emitted === 0;
    // Skipped when the answer is already known: the encoded frame carries the remaining payload plus
    // an envelope plus whatever the codec's escaping adds, so it is never *shorter* than the
    // remainder itself. If the remainder alone is over the cap, encoding it only to be told so
    // renders the whole rest of the payload for nothing - and does it again on every pass, which is
    // quadratic in payload size.
    if (total - position <= cap) {
      const tail = fragmentFrame(frame, sliceUnits(units, position, total - position), { first, last: true });
      if (encodedLength(codec.encode(tail)) <= cap) {
        yield tail;
        return;
      }
    }

    if (position >= total) throw closingFloorError(cap);

    // Otherwise a middle fragment: budget for the envelope first (WSM-FRG-013)...
    let count = unitsWithinBudget(units, position, budget);
    let candidate = fragmentFrame(frame, sliceUnits(units, position, count), { first, last: false });

    // ...then verify, and re-split if the reservation guessed low (WSM-FRG-014). The reservation is
    // a per-codec hint; this is the guarantee.
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
 * Receive side: concatenates `fragment` values and decodes once the last one lands.
 *
 * `feed()` returns `ABSENT` while `more` is true, and the decoded payload on the frame that closes
 * the sequence (WSM-FRG-030).
 */
export class Assembler {
  private parts: (string | ArrayBuffer)[] = [];
  private bytes = 0;

  /** True between the first fragment and the one that arrives without `more`. */
  get inProgress(): boolean {
    return this.parts.length > 0;
  }

  /**
   * Bytes accumulated so far.
   *
   * `maxPayloadBytes` is enforced against this **as fragments arrive** rather than after
   * reassembly (WSM-FRG-032): a receiver that assembles a payload in order to measure it has
   * already spent what the limit was protecting.
   */
  get byteLength(): number {
    return this.bytes;
  }

  /** Drop the partial buffer. Called when the stream is reset, releasing the bytes with it. */
  reset(): void {
    this.parts = [];
    this.bytes = 0;
  }

  /** Absorb one fragment frame; return `ABSENT` while more are expected, else the payload. */
  feed(frame: Frame, codec: Codec): unknown {
    const { fragment } = frame;
    if (fragment === null || fragment === undefined) {
      throw new ProtocolError('Assembler.feed was given a frame carrying no fragment');
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
