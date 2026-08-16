import boundaries from '../conformance/frames/v1-fragment-boundaries.json';
import corpus from '../conformance/frames/v1-frames.json';

import { type Codec, JsonCodec } from './codec';
import { ProtocolError } from './errors';
import { Assembler, MAX_FRAME_BYTES, boundaryWithin, encodedLength, splitFrame } from './fragment';
import { ABSENT, type Frame, fromMapping, toMapping } from './frames';

const codec = new JsonCodec();

/**
 * The same payloads the Python suite sweeps, spelled identically, so that a boundary disagreement
 * between the ports shows up as a failing test here rather than as corruption on a cross-language
 * connection.
 */
const PAYLOADS: Record<string, unknown> = {
  ascii: { body: 'abcdefghij'.repeat(400) },
  'three-byte-codepoints': { body: 'č š ž — ъ ђ '.repeat(300) },
  'four-byte-codepoints': { body: '🛰️𝕄𝕦𝕩🌍'.repeat(300) },
  'control-characters': {
    body: Array.from({ length: 31 }, (_, index) => String.fromCharCode(index + 1))
      .join('')
      .repeat(200),
  },
  mixed: { rows: Array.from({ length: 200 }, (_, index) => ({ id: index, name: `vrstica ${index} 🛰️` })) },
  'bare-string': 'x'.repeat(5000),
  'bare-list': Array.from({ length: 2000 }, (_, index) => index),
};

function reassemble(parts: Frame[], target: Codec): unknown {
  if (parts.length === 1 && (parts[0].fragment === null || parts[0].fragment === undefined)) {
    return parts[0].payload;
  }
  const assembler = new Assembler();
  let result: unknown = ABSENT;
  parts.forEach((part) => {
    result = assembler.feed(part, target);
  });
  return result;
}

describe('the frame constant', () => {
  it('is 64 KiB and is never configuration - WSM-FRG-004', () => {
    expect(MAX_FRAME_BYTES).toBe(65_536);
  });
});

describe('encodedLength', () => {
  it('measures bytes, not UTF-16 units - WSM-FRG-002', () => {
    expect(encodedLength('abc')).toBe(3);
    expect(encodedLength('č')).toBe(2);
    expect(encodedLength('𝕄')).toBe(4);
    // The failure this rule exists to prevent: a non-BMP character is 2 in `.length` and 4 on the wire.
    expect('𝕄'.length).toBe(2);
    expect(encodedLength('𝕄')).not.toBe('𝕄'.length);
    expect(encodedLength(new Uint8Array([0, 255]).buffer)).toBe(2);
  });
});

/**
 * The take invariant, asserted as itself rather than against the implementation.
 *
 * For a string S, a byte offset i on a codepoint boundary and a budget b, the take T satisfies:
 *   1. T is a prefix of S from i onwards
 *   2. the UTF-8 length of T is at most b
 *   3. maximality: either i + the length of T is the end of S, or the next codepoint would exceed b
 *   4. T is empty exactly when the codepoint at i does not fit in b
 *
 * Both ports compute this; neither has to compute it the same way, and it is what makes them cut at
 * the same boundaries by construction (WSM-FRG-012/016). Pinning the current arithmetic instead
 * would pin its mistakes with it, so the expectations below re-derive every quantity - codepoint
 * widths come from the lead byte, not from the walk the implementation does.
 */
describe('takeWithinBudget', () => {
  const encoder = new TextEncoder();
  // The splitter's own decoder options: a slice is a piece of a payload and not a document, so a
  // leading U+FEFF is payload, and a cut inside a sequence must throw rather than yield U+FFFD.
  const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });

  /**
   * The splitter's reservation cut, over a plain string: the longest prefix of the UTF-8 from byte
   * `start` that fits `budget` bytes and ends on a codepoint boundary.
   *
   * Built on the library's own `boundaryWithin`, so the invariant below is asserted on the rule the
   * splitter cuts by rather than on a second implementation of it.
   */
  function takeWithinBudget(text: string, start: number, budget: number): string {
    const units = { bytes: encoder.encode(text), text: true };
    return decoder.decode(units.bytes.subarray(start, start + boundaryWithin(units, start, budget)));
  }

  /** Width in bytes of the codepoint whose lead byte sits at `index`, read off the lead byte alone. */
  function codepointWidth(bytes: Uint8Array, index: number): number {
    const lead = bytes[index];
    if (lead < 0x80) return 1;
    if ((lead & 0xe0) === 0xc0) return 2;
    if ((lead & 0xf0) === 0xe0) return 3;
    return 4;
  }

  /** Every byte offset in `bytes` that starts a codepoint, plus the offset one past the end. */
  function boundaries(bytes: Uint8Array): number[] {
    const offsets: number[] = [];
    for (let index = 0; index < bytes.length; index += codepointWidth(bytes, index)) offsets.push(index);
    offsets.push(bytes.length);
    return offsets;
  }

  function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
    if (left.length !== right.length) return false;
    return left.every((byte, index) => byte === right[index]);
  }

  function assertInvariant(text: string, start: number, budget: number, context: string): void {
    const bytes = encoder.encode(text);
    const take = takeWithinBudget(text, start, budget);
    const taken = encoder.encode(take);

    expect(bytesEqual(taken, bytes.subarray(start, start + taken.length)), `prefix - ${context}`).toBe(true);
    expect(taken.length, `budget - ${context}`).toBeLessThanOrEqual(budget);

    const end = start + taken.length;
    if (end < bytes.length) {
      expect(taken.length + codepointWidth(bytes, end), `maximality - ${context}`).toBeGreaterThan(budget);
    }

    const fits = start < bytes.length && codepointWidth(bytes, start) <= budget;
    expect(take === '', `emptiness - ${context}`).toBe(!fits);
  }

  /** Deterministic 32-bit PRNG, so a failing case is reproduced by re-running the file. */
  function seeded(seed: number): () => number {
    let state = seed >>> 0;
    return () => {
      state = (state + 0x6d2b79f5) >>> 0;
      let value = Math.imul(state ^ (state >>> 15), state | 1);
      value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
      return ((value ^ (value >>> 14)) >>> 0) / 4_294_967_296;
    };
  }

  /** One character of each UTF-8 width, so a generated string exercises all four. */
  const ALPHABET = [...'ax9 ', ...'čšžð', ...'—ъ€中', ...'🛰𝕄𝕦🌍'];

  it('holds for every offset and budget over a generated corpus', () => {
    const random = seeded(0x5eed_1234);
    for (let round = 0; round < 60; round += 1) {
      const length = 1 + Math.floor(random() * 40);
      const text = Array.from({ length }, () => ALPHABET[Math.floor(random() * ALPHABET.length)]).join('');
      const offsets = boundaries(encoder.encode(text));
      offsets.forEach((start) => {
        for (let budget = 0; budget <= 12; budget += 1) {
          assertInvariant(text, start, budget, `round ${round} start ${start} budget ${budget}`);
        }
      });
    }
  });

  it('holds for every offset and budget in a string that is entirely non-ASCII', () => {
    const text = 'čšž—ъ€中🛰𝕄🌍'.repeat(4);
    boundaries(encoder.encode(text)).forEach((start) => {
      for (let budget = 0; budget <= 9; budget += 1) {
        assertInvariant(text, start, budget, `start ${start} budget ${budget}`);
      }
    });
  });

  it('takes nothing when the budget is smaller than the first codepoint', () => {
    expect(takeWithinBudget('𝕄x', 0, 3)).toBe('');
    expect(takeWithinBudget('č', 0, 1)).toBe('');
    assertInvariant('𝕄x', 0, 3, 'four-byte codepoint under a three-byte budget');
  });

  it('takes nothing on a budget of zero', () => {
    expect(takeWithinBudget('abc', 0, 0)).toBe('');
    assertInvariant('abc', 0, 0, 'zero budget');
  });

  it('takes nothing at an offset past the last codepoint', () => {
    const text = 'č𝕄';
    expect(encodedLength(text)).toBe(6);
    expect(takeWithinBudget(text, 6, 100)).toBe('');
    assertInvariant(text, 6, 100, 'offset at the end');
  });

  it('takes the whole run when the budget lands exactly on a boundary', () => {
    // 'čš' is four bytes and '𝕄' is four more, so 4 and 8 are the two exact landings.
    expect(takeWithinBudget('čš𝕄', 0, 4)).toBe('čš');
    expect(takeWithinBudget('čš𝕄', 0, 8)).toBe('čš𝕄');
    // One byte short of the landing takes one codepoint less, never a partial one.
    expect(takeWithinBudget('čš𝕄', 0, 7)).toBe('čš');
  });

  it('never cuts a four-byte codepoint, whichever of its bytes the budget lands on', () => {
    const text = '🛰𝕄𝕦🌍';
    [4, 5, 6, 7].forEach((budget) => expect(takeWithinBudget(text, 0, budget)).toBe('🛰'));
    // A surrogate pair is one codepoint and two UTF-16 units; the take must carry both or neither.
    [...takeWithinBudget(text, 0, 15)].forEach((char) => {
      const code = char.charCodeAt(0);
      if (code >= 0xd800 && code <= 0xdfff) expect(char.length).toBe(2);
    });
  });

  it('leaves the caller to report a cap that cannot hold one codepoint - WSM-FRG-034', () => {
    // The empty take at the top of a fragment is not a stall: the splitter raises instead of looping.
    const payload = { body: '𝕄'.repeat(200) };
    expect(() => splitFrame({ type: 'data', stream: 1, payload }, 5, codec)).toThrow(/indivisible unit/);
  });

  it('carries a leading U+FEFF, which a decoder reading a document would swallow', () => {
    // The slice is a piece of a payload, not a file: a byte-order mark inside it is payload too, and
    // a fragment that starts on one must arrive with it (WSM-FRG-016).
    expect(takeWithinBudget('﻿abc', 0, 6)).toBe('﻿abc');
    expect(takeWithinBudget('﻿﻿', 0, 3)).toBe('﻿');
    assertInvariant('﻿a﻿', 0, 4, 'leading byte-order mark');
  });
});

describe('a payload of byte-order marks', () => {
  it('survives the split and the reassembly whole', () => {
    // Every fragment after the first begins on a U+FEFF, so a decoder that strips one loses a
    // character per fragment - silently, and only on this port.
    const payload = '﻿'.repeat(100);
    [64, 96, 128, 256].forEach((cap) => {
      const parts = splitFrame({ type: 'data', stream: 1, payload, end: true }, cap, codec);
      expect(parts.length).toBeGreaterThan(1);
      const joined = parts.map((part) => part.fragment as string).join('');
      expect(joined).toBe(codec.encodePayload(payload));
      expect(codec.decodePayload(joined)).toBe(payload);
    });
  });
});

describe('splitFrame', () => {
  [64, 96, 128, 256, 512, 1024, 4096].forEach((cap) => {
    Object.keys(PAYLOADS).forEach((name) => {
      it(`never exceeds the cap - ${name} at ${cap}`, () => {
        const frame: Frame = { type: 'data', stream: 1, payload: PAYLOADS[name], end: true };
        splitFrame(frame, cap, codec).forEach((part) => {
          expect(encodedLength(codec.encode(part))).toBeLessThanOrEqual(cap);
        });
      });
    });
  });

  ['three-byte-codepoints', 'four-byte-codepoints'].forEach((name) => {
    it(`cuts only at codepoint boundaries - ${name}`, () => {
      const parts = splitFrame({ type: 'data', stream: 1, payload: PAYLOADS[name], end: true }, 96, codec);
      expect(parts.length).toBeGreaterThan(1);
      parts.forEach((part) => {
        const fragment = part.fragment as string;
        expect(typeof fragment).toBe('string');
        // Re-encoding and decoding in isolation only round-trips if the slice holds whole codepoints.
        expect(new TextDecoder().decode(new TextEncoder().encode(fragment))).toBe(fragment);
        // `[...fragment]` iterates codepoints, so an astral character arrives as a two-unit string.
        // A *lone* surrogate is the one-unit case, and that is what must never appear.
        expect(
          [...fragment].every(
            (char) => char.length === 2 || !(char.charCodeAt(0) >= 0xd800 && char.charCodeAt(0) <= 0xdfff),
          ),
        ).toBe(true);
      });
    });
  });

  it('never splits a surrogate pair - WSM-FRG-012', () => {
    const parts = splitFrame({ type: 'data', stream: 1, payload: { body: '𝕄𝕦𝕩'.repeat(400) } }, 96, codec);
    parts.forEach((part) => {
      const fragment = part.fragment as string;
      for (let index = 0; index < fragment.length; index += 1) {
        const code = fragment.charCodeAt(index);
        if (code >= 0xd800 && code <= 0xdbff) {
          const low = fragment.charCodeAt(index + 1);
          expect(low >= 0xdc00 && low <= 0xdfff).toBe(true);
        }
      }
    });
  });

  it('re-splits a control-character payload rather than emitting it over cap - WSM-FRG-014', () => {
    const parts = splitFrame(
      { type: 'data', stream: 1, payload: PAYLOADS['control-characters'], end: true },
      128,
      codec,
    );
    expect(parts.length).toBeGreaterThan(1);
    parts.forEach((part) => expect(encodedLength(codec.encode(part))).toBeLessThanOrEqual(128));
    expect(reassemble(parts, codec)).toEqual(PAYLOADS['control-characters']);
  });

  it('puts end and trailers only on the last fragment, headers only on the first - WSM-FRG-020/021', () => {
    const frame: Frame = {
      type: 'open',
      stream: 1,
      payload: PAYLOADS.ascii,
      headers: { trace: 'abc' },
      end: true,
      trailers: { checksum: 'd' },
    };
    const parts = splitFrame(frame, 256, codec);
    expect(parts.length).toBeGreaterThan(2);

    parts.forEach((part, index) => {
      const last = index === parts.length - 1;
      expect(part.more).toBe(!last);
      expect(part.end).toBe(last);
      expect(part.trailers).toEqual(last ? { checksum: 'd' } : null);
      expect(part.headers).toEqual(index === 0 ? { trace: 'abc' } : null);
      expect(part.payload).toBe(ABSENT);
    });
  });

  it('never fragments headers - WSM-FRG-021', () => {
    const headers = { trace: 't'.repeat(300) };
    const parts = splitFrame({ type: 'open', stream: 1, payload: PAYLOADS.ascii, headers }, 1024, codec);
    expect(parts[0].headers).toEqual(headers);
  });

  it('cannot split a frame that is oversized on its envelope alone', () => {
    expect(() => splitFrame({ type: 'open', stream: 1, headers: { trace: 't'.repeat(5000) } }, 256, codec)).toThrow(
      /headers are never fragmented/,
    );
  });

  it('returns a fitting frame untouched', () => {
    const frame: Frame = { type: 'data', stream: 1, payload: { a: 1 } };
    expect(splitFrame(frame, MAX_FRAME_BYTES, codec)).toEqual([frame]);
  });

  it('raises rather than looping when the cap cannot hold the envelope - WSM-FRG-034', () => {
    expect(() => splitFrame({ type: 'data', stream: 1, payload: PAYLOADS.ascii }, 32, codec)).toThrow(/envelope/);
    expect(() => splitFrame({ type: 'data', stream: 1, payload: PAYLOADS.ascii }, 1, codec)).toThrow(/too small/);
  });

  it('requires a codec, because boundaries are defined over its output', () => {
    expect(() => splitFrame({ type: 'data', stream: 1, payload: { a: 1 } }, 256)).toThrow(ProtocolError);
  });

  it('is pure - WSM-FRG-015', () => {
    const payload = JSON.parse(JSON.stringify(PAYLOADS.mixed)) as unknown;
    const before = JSON.parse(JSON.stringify(payload)) as unknown;
    const frame: Frame = { type: 'data', stream: 1, payload, headers: { trace: 'x' }, end: true };

    const first = splitFrame(frame, 512, codec);
    const second = splitFrame(frame, 512, codec);

    expect(first).toEqual(second);
    expect(payload).toEqual(before);
    expect(frame.payload).toEqual(before);
  });

  it('keeps the fragments of one payload contiguous - WSM-FRG-017', () => {
    const parts = splitFrame({ type: 'data', stream: 1, payload: PAYLOADS.mixed }, 512, codec);
    const rejoined = parts.map((part) => part.fragment as string).join('');
    expect(codec.decodePayload(rejoined)).toEqual(PAYLOADS.mixed);
  });
});

describe('Assembler', () => {
  Object.keys(PAYLOADS).forEach((name) => {
    it(`round-trips every payload - ${name}`, () => {
      const parts = splitFrame({ type: 'data', stream: 1, payload: PAYLOADS[name], end: true }, 256, codec);
      const assembler = new Assembler();
      expect(assembler.inProgress).toBe(false);
      parts.slice(0, -1).forEach((part) => {
        expect(assembler.feed(part, codec)).toBe(ABSENT);
        expect(assembler.inProgress).toBe(true);
      });
      expect(assembler.feed(parts[parts.length - 1], codec)).toEqual(PAYLOADS[name]);
      expect(assembler.inProgress).toBe(false);
    });
  });

  it('tracks accumulated bytes before reassembly - WSM-FRG-032', () => {
    const parts = splitFrame({ type: 'data', stream: 1, payload: PAYLOADS.ascii }, 256, codec);
    const assembler = new Assembler();
    let seen = 0;
    parts.slice(0, -1).forEach((part) => {
      assembler.feed(part, codec);
      expect(assembler.byteLength).toBeGreaterThan(seen);
      seen = assembler.byteLength;
    });
    assembler.reset();
    expect(assembler.byteLength).toBe(0);
    expect(assembler.inProgress).toBe(false);
  });

  it('rejects a frame carrying no fragment', () => {
    expect(() => new Assembler().feed({ type: 'data', stream: 1, payload: { a: 1 } }, codec)).toThrow(/no fragment/);
  });
});

describe('the binary path', () => {
  /**
   * A toy binary codec, so the byte-boundary half of the splitter has something to exercise without
   * pulling msgpack in. `latin-1` round-trips any byte sequence one-to-one, which is all the envelope
   * needs.
   */
  class BinaryJsonCodec implements Codec {
    readonly name = 'binary-json';
    readonly binary = true;

    encode(frame: Frame): ArrayBuffer {
      const mapping = toMapping(frame);
      if (mapping.fragment instanceof ArrayBuffer) {
        mapping.fragment = Array.from(new Uint8Array(mapping.fragment))
          .map((byte) => String.fromCharCode(byte))
          .join('');
      }
      return new TextEncoder().encode(JSON.stringify(mapping)).buffer as ArrayBuffer;
    }

    decode(message: string | ArrayBuffer): Frame {
      const text = typeof message === 'string' ? message : new TextDecoder().decode(message);
      const mapping = JSON.parse(text) as Record<string, unknown>;
      if (typeof mapping.fragment === 'string') {
        mapping.fragment = Uint8Array.from([...mapping.fragment].map((char) => char.charCodeAt(0))).buffer;
      }
      return fromMapping(mapping);
    }

    encodePayload(payload: unknown): ArrayBuffer {
      return new TextEncoder().encode(JSON.stringify(payload)).buffer as ArrayBuffer;
    }

    decodePayload(data: string | ArrayBuffer): unknown {
      return JSON.parse(typeof data === 'string' ? data : new TextDecoder().decode(data));
    }
  }

  it('slices at byte boundaries and measures the buffer - WSM-FRG-003/012', () => {
    const binary = new BinaryJsonCodec();
    const payload = { body: 'abcdefghij'.repeat(200) };
    const parts = splitFrame({ type: 'data', stream: 1, payload, end: true }, 256, binary);

    expect(parts.length).toBeGreaterThan(1);
    parts.forEach((part) => {
      expect(part.fragment).toBeInstanceOf(ArrayBuffer);
      expect(encodedLength(binary.encode(part))).toBeLessThanOrEqual(256);
    });
    expect(reassemble(parts, binary)).toEqual(payload);
  });

  it('may be cut at every byte - WSM-FRG-003', () => {
    // Under a binary codec the unit is the byte, and 0x80..0xBF are ordinary payload rather than
    // continuation bytes: no cut walks backwards, and one byte is a whole unit.
    const units = { bytes: Uint8Array.from({ length: 256 }, (_, index) => index), text: false };
    for (let limit = 0; limit <= units.bytes.length; limit += 1) {
      expect(boundaryWithin(units, 0, limit), `limit ${limit}`).toBe(limit);
    }
    expect(boundaryWithin(units, 0x80, 1)).toBe(1);
  });
});

describe('the one-unit floor', () => {
  /**
   * A codec whose fragment frames are the fragment and nothing else, so that the reservation can be
   * narrower than one codepoint while a fragment carrying that codepoint still fits under the cap.
   * Under any real envelope the cap would have to be smaller than the envelope for that to happen,
   * and the splitter would raise instead (WSM-FRG-034), leaving the floor unobservable.
   */
  class BareCodec implements Codec {
    readonly name = 'bare';
    readonly binary = false;

    encode(frame: Frame): string {
      return typeof frame.fragment === 'string' ? frame.fragment : JSON.stringify(toMapping(frame));
    }

    decode(message: string | ArrayBuffer): Frame {
      return fromMapping(JSON.parse(typeof message === 'string' ? message : '') as Record<string, unknown>);
    }

    encodePayload(payload: unknown): string {
      return payload as string;
    }

    decodePayload(data: string | ArrayBuffer): unknown {
      return data;
    }
  }

  it('takes one whole codepoint when the reservation cannot hold it - WSM-FRG-013', () => {
    // A cap of 5 reserves 2, leaving a budget of 3 against a payload of four-byte codepoints.
    const payload = '𝕄'.repeat(5);
    const parts = splitFrame({ type: 'data', stream: 1, payload }, 5, new BareCodec());

    expect(parts.map((part) => part.fragment)).toEqual(['𝕄', '𝕄', '𝕄', '𝕄', '𝕄']);
    expect(parts.map((part) => part.fragment as string).join('')).toBe(payload);
  });
});

describe('the corpus', () => {
  corpus.forEach((entry) => {
    const testCase = entry as { name: string; frame: Record<string, unknown> };
    if (!('payload' in testCase.frame)) return;

    it(`fragments and reassembles at a hostile cap - ${testCase.name}`, () => {
      const parts = splitFrame({ type: 'data', stream: 1, payload: testCase.frame.payload }, 96, codec);
      parts.forEach((part) => expect(encodedLength(codec.encode(part))).toBeLessThanOrEqual(96));
      expect(reassemble(parts, codec)).toEqual(testCase.frame.payload);
    });
  });
});

describe('cross-language boundaries', () => {
  /**
   * WSM-FRG-016: the pinned fragments are the contract both ports are held to.
   *
   * A boundary disagreement between TypeScript and Python is invisible until two peers of different
   * languages try to reassemble each other's payloads, at which point it looks like corruption.
   * This fixture - generated once, read by both suites - turns it into a failing unit test in
   * whichever port drifted.
   */
  boundaries.forEach((entry) => {
    const testCase = entry as { name: string; cap: number; payload: unknown; fragments: string[] };

    it(`cuts exactly where the Python port cuts - ${testCase.name}`, () => {
      const parts = splitFrame({ type: 'data', stream: 1, payload: testCase.payload, end: true }, testCase.cap, codec);
      expect(parts.map((part) => part.fragment)).toEqual(testCase.fragments);
      expect(reassemble(parts, codec)).toEqual(testCase.payload);
    });
  });

  it('encodes payloads to the same bytes Python does', () => {
    // The boundaries can only agree if the encoded form does, so this is the assumption underneath.
    boundaries.forEach((entry) => {
      const testCase = entry as { payload: unknown; fragments: string[] };
      expect(codec.encodePayload(testCase.payload)).toBe(testCase.fragments.join(''));
    });
  });
});

describe('sequence termination', () => {
  /**
   * A splitter that sizes its slices against the middle envelope alone cannot terminate a sequence
   * whose trailers are larger than that: the closing frame is over cap at every slice point, so the
   * loop emits middle fragments until the payload runs out and then stops. The receiver's assembler
   * never fires, the payload never reaches the application, and nothing anywhere reports an error.
   */
  it('always ends with a fragment carrying more:false - WSM-FRG-020/030', () => {
    const trailers = { checksum: 'd'.repeat(130) };
    const frame: Frame = { type: 'data', stream: 1, payload: { body: 'x'.repeat(900) }, end: true, trailers };
    const parts = splitFrame(frame, 256, codec);

    expect(parts[parts.length - 1].more).toBe(false);
    expect(parts[parts.length - 1].end).toBe(true);
    expect(parts[parts.length - 1].trailers).toEqual(trailers);
    expect(parts.slice(0, -1).every((part) => part.more === true)).toBe(true);
    expect(reassemble(parts, codec)).toEqual(frame.payload);
  });

  it('raises when trailers alone cannot fit, rather than splitting forever', () => {
    const frame: Frame = { type: 'data', stream: 1, payload: { a: 1 }, end: true, trailers: { x: 'y'.repeat(500) } };
    expect(() => splitFrame(frame, 256, codec)).toThrow(/closing envelope/);
  });

  it('accepts a closing fragment that carries no payload bytes', () => {
    const trailers = { checksum: 'd'.repeat(130) };
    const frame: Frame = { type: 'data', stream: 1, payload: { body: 'x'.repeat(900) }, end: true, trailers };
    const parts = splitFrame(frame, 256, codec);
    const rejoined = parts.map((part) => part.fragment as string).join('');
    expect(codec.decodePayload(rejoined)).toEqual(frame.payload);
  });
});
