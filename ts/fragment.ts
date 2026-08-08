/**
 * Fragmentation: the splitter and the assembler, as pure functions (§4).
 *
 * A line-for-line mirror of `muxws/fragment.py`. The two ports must cut at the *same* boundaries
 * (WSM-FRG-016), which is why every decision here - the reservation, the codepoint cursor, the
 * binary search - is made the same way and in the same order as it is there.
 *
 * Nothing here touches a socket. The send path wires the splitter in at M5a.
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
 * The encoded payload as a list of indivisible units: whole Unicode codepoints for text, bytes for
 * a binary codec (WSM-FRG-012).
 *
 * Splitting a JavaScript string by UTF-16 index would cut surrogate pairs in half and disagree with
 * Python, whose strings are already sequences of codepoints. Spreading the string is what makes the
 * two ports index the same units.
 */
function unitsOf(encoded: string | ArrayBuffer): string[] | Uint8Array {
  if (typeof encoded === 'string') return [...encoded];
  return new Uint8Array(encoded);
}

/** Rebuild a slice of `units` in the codec's own representation. */
function sliceUnits(units: string[] | Uint8Array, start: number, count: number): string | ArrayBuffer {
  if (Array.isArray(units)) return units.slice(start, start + count).join('');
  const slice = units.slice(start, start + count);
  return slice.buffer.slice(slice.byteOffset, slice.byteOffset + slice.byteLength) as ArrayBuffer;
}

/** How many units at `start` fit into `budget` **bytes**, always at least one. */
function unitsWithinBudget(units: string[] | Uint8Array, start: number, budget: number): number {
  if (!Array.isArray(units)) return Math.max(1, Math.min(budget, units.length - start));

  let taken = 0;
  let count = 0;
  for (let index = start; index < units.length; index += 1) {
    const width = TEXT_ENCODER.encode(units[index]).length;
    if (taken + width > budget) break;
    taken += width;
    count += 1;
  }
  return Math.max(1, count);
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
 * Largest number of units at `position` whose non-final fragment frame still fits under `cap`.
 *
 * The verify-and-re-split half of WSM-FRG-014, as a **binary search**: it terminates in
 * log2(ceiling) encodes, and - more importantly - it is exactly reproducible, so this port and the
 * Python one cut at the same boundary (WSM-FRG-016). Returns 0 when not even one unit fits.
 */
function largestFittingCount(
  source: Frame,
  units: string[] | Uint8Array,
  position: number,
  ceiling: number,
  options: { first: boolean; cap: number; codec: Codec },
): number {
  let low = 1;
  let high = ceiling;
  let best = 0;
  while (low <= high) {
    const middle = Math.floor((low + high) / 2);
    const probe = fragmentFrame(source, sliceUnits(units, position, middle), { first: options.first, last: false });
    if (encodedLength(options.codec.encode(probe)) <= options.cap) {
      best = middle;
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

  const encoded = codec.encodePayload(frame.payload);
  const units = unitsOf(encoded);
  const total = units.length;
  const budget = Math.max(1, cap - reservation(cap));

  let position = 0;
  let emitted = 0;

  for (;;) {
    // Everything left, as the closing fragment? `end` and `trailers` ride only this one, so it is a
    // different size from a middle fragment and has to be measured as itself. This is checked first
    // on every pass, including the one where the payload is already spent: the sequence MUST end
    // with a fragment carrying `more: false`, even if that fragment carries no bytes.
    const first = emitted === 0;
    const tail = fragmentFrame(frame, sliceUnits(units, position, total - position), { first, last: true });
    if (encodedLength(codec.encode(tail)) <= cap) {
      yield tail;
      return;
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
   * M5a enforces `maxPayloadBytes` against this **as fragments arrive** rather than after
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
