import boundaries from '../conformance/frames/v1-fragment-boundaries.json';
import corpus from '../conformance/frames/v1-frames.json';

import { type Codec, JsonCodec } from './codec';
import { ProtocolError } from './errors';
import { Assembler, MAX_FRAME_BYTES, encodedLength, splitFrame } from './fragment';
import { ABSENT, type Frame, fromMapping, toMapping } from './frames';

const codec = new JsonCodec();

/**
 * The same payloads the Python suite sweeps, spelled identically, so that a boundary disagreement
 * between the ports shows up as a failing test rather than as a cross-language surprise in M6.
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
   * A toy binary codec, so the byte-boundary half of the splitter has something to exercise. The
   * real one is msgpack and arrives in M6; `latin-1` round-trips any byte sequence one-to-one, which
   * is all the envelope needs.
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
   * Regression. Trailers larger than the reservation used to make the closing frame too big at
   * every slice point, so the loop emitted middle fragments until the payload ran out and then
   * stopped - leaving a sequence with no terminator. The receiver's assembler never fires, the
   * payload never reaches the application, and nothing anywhere reports an error.
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
