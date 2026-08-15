import corpus from '../conformance/frames/v1-frames.json';

import { JsonCodec } from './codec';
import { ProtocolError } from './errors';
import { ABSENT, type Frame, V1_FRAME_TYPES, framesEqual, fromMapping, toMapping } from './frames';

const codec = new JsonCodec();

/** Fixtures spell a frame as its envelope, so an absent payload key means ABSENT (D1). */
function frameFromFixture(mapping: Record<string, unknown>): Frame {
  return fromMapping(mapping);
}

describe('the frame corpus', () => {
  corpus.forEach((entry) => {
    const testCase = entry as { name: string; frame: Record<string, unknown>; json_wire: string };

    it(`decodes the pinned wire to the frame - ${testCase.name}`, () => {
      expect(framesEqual(codec.decode(testCase.json_wire), frameFromFixture(testCase.frame))).toBe(true);
    });

    it(`round-trips - ${testCase.name}`, () => {
      const frame = frameFromFixture(testCase.frame);
      expect(framesEqual(codec.decode(codec.encode(frame)), frame)).toBe(true);
    });

    it(`encodes to something that parses equal to the pinned wire - ${testCase.name}`, () => {
      const frame = frameFromFixture(testCase.frame);
      expect(JSON.parse(codec.encode(frame))).toEqual(JSON.parse(testCase.json_wire));
    });
  });

  it('is the same corpus the Python suite reads', () => {
    expect(corpus.length).toBeGreaterThan(10);
    expect(corpus.every((entry) => 'name' in entry && 'frame' in entry && 'json_wire' in entry)).toBe(true);
  });
});

describe('the v1 frame set', () => {
  /**
   * Section 2.3 verbatim, written out here so the assertion below is a **set equality** rather than
   * a membership check.
   *
   * A membership check cannot fail for a type that should not be there, and the two facts most worth
   * policing are absences: there is no `end` frame type (WSM-FRM-012) and no trailers frame type
   * (WSM-FRM-013). Both are flags on `data` - a `V1_FRAME_TYPES` that grew an `end` entry would
   * satisfy every membership assertion in this file and still be a different protocol.
   */
  const SECTION_2_3 = ['open', 'data', 'reset', 'ping', 'pong', 'goaway'];

  it('is exactly the six of section 2.3 - WSM-FRM-012/WSM-FRM-013', () => {
    expect([...V1_FRAME_TYPES].sort()).toEqual([...SECTION_2_3].sort());
    expect(V1_FRAME_TYPES.size).toBe(6);
  });

  it('has no end frame type and no trailers frame type - WSM-FRM-012/WSM-FRM-013', () => {
    // Named individually as well as by the equality above, so a failure says which absence broke.
    // `window_update` is reserved and unimplemented (WSM-BPR-001) and there is no `settings` frame
    // (WSM-CON-031); neither may appear in the set a v1 peer sends from.
    ['end', 'trailers', 'window_update', 'settings', 'headers', 'ack'].forEach((absent) => {
      expect(V1_FRAME_TYPES.has(absent), `${absent} must not be a v1 frame type`).toBe(false);
    });
  });

  it('spells end and trailers as flags on data - WSM-FRM-012/WSM-FRM-013', () => {
    // The positive half of the same pair of rules: what a peer with nothing left to say actually
    // sends is a `data` frame, and trailers ride on the frame carrying `end: true`.
    const closing: Frame = { type: 'data', stream: 7, end: true, trailers: { checksum: 'deadbeef' } };
    expect(V1_FRAME_TYPES.has(closing.type)).toBe(true);
    expect(codec.encode(closing)).toBe('{"type":"data","stream":7,"end":true,"trailers":{"checksum":"deadbeef"}}');
    expect(framesEqual(codec.decode(codec.encode(closing)), closing)).toBe(true);
    // And `end` carries no payload of its own: the key is absent, not null (D1).
    expect('payload' in toMapping(closing)).toBe(false);
  });

  it('is the only vocabulary the shared corpus uses - WSM-FRM-012/WSM-FRM-013', () => {
    // The corpus is the fixture both ports read, so this is the one place a divergence between the
    // two `V1_FRAME_TYPES` constants would show up as a wire fact rather than as a local opinion.
    // Every pinned frame must be one of the six, and the corpus must actually exercise the flags -
    // otherwise "no end frame type" would be trivially true of a corpus that never ends a stream.
    const types = new Set(corpus.map((entry) => (entry as { frame: { type: string } }).frame.type));
    types.forEach((type) => expect(V1_FRAME_TYPES.has(type), `${type} is pinned but is not a v1 type`).toBe(true));

    const flagged = corpus.filter((entry) => 'end' in (entry as { frame: Record<string, unknown> }).frame);
    expect(flagged.length, 'no pinned frame ends a stream, so the flag is untested').toBeGreaterThan(0);
    flagged.forEach((entry) => {
      expect(['open', 'data']).toContain((entry as { frame: { type: string } }).frame.type);
    });
  });
});

describe('the envelope', () => {
  it('orders keys type, stream, then alphabetically - WSM-CDC-005', () => {
    const frame: Frame = {
      type: 'data',
      stream: 7,
      payload: { rows: 1 },
      end: true,
      trailers: { checksum: 'x' },
      reason: 'because',
      code: 0,
    };
    expect(Object.keys(toMapping(frame))).toEqual(['type', 'stream', 'code', 'end', 'payload', 'reason', 'trailers']);
  });

  it('drops unknown fields rather than preserving them - WSM-FRM-001, D2', () => {
    const withExtra = codec.decode('{"type":"data","stream":1,"payload":{"a":1},"colour":"red"}');
    const without = codec.decode('{"type":"data","stream":1,"payload":{"a":1}}');
    expect(framesEqual(withExtra, without)).toBe(true);
    expect('colour' in withExtra).toBe(false);
  });

  it('lets an unknown frame type survive decoding - WSM-FRM-002, D4', () => {
    expect(codec.decode('{"type":"window_update","stream":1}').type).toBe('window_update');
  });

  it('rejects a missing type and payload+fragment together - WSM-FRM-004/005', () => {
    expect(() => fromMapping({ stream: 1 })).toThrow(ProtocolError);
    expect(() => fromMapping({ type: 'data', stream: 1, payload: 1, fragment: 'x' })).toThrow(ProtocolError);
  });

  it('distinguishes an absent payload from an explicit null - D1', () => {
    expect('payload' in toMapping({ type: 'data', stream: 1 })).toBe(false);
    expect(toMapping({ type: 'data', stream: 1, payload: null }).payload).toBeNull();
    expect(codec.decode('{"type":"data","stream":1}').payload).toBe(ABSENT);
    expect(codec.decode('{"type":"data","stream":1,"payload":null}').payload).toBeNull();
  });

  it('has no settings frame and no field that could carry one - WSM-CON-031', () => {
    const decoded = codec.decode('{"type":"data","stream":1,"settings":{"max_frame_bytes":1},"ack":true}');
    expect(framesEqual(decoded, { type: 'data', stream: 1 })).toBe(true);
    expect('settings' in decoded).toBe(false);
    expect('ack' in decoded).toBe(false);
  });

  it('treats undefined and the field default as the same thing', () => {
    expect(framesEqual({ type: 'data', stream: 1 }, { type: 'data', stream: 1, more: false, end: false })).toBe(true);
    expect(framesEqual({ type: 'data', stream: 1 }, { type: 'data', stream: 2 })).toBe(false);
  });

  it('compares payloads by value, including nested and array shapes - D3', () => {
    expect(
      framesEqual({ type: 'data', payload: { a: [1, { b: 2 }] } }, { type: 'data', payload: { a: [1, { b: 2 }] } }),
    ).toBe(true);
    expect(framesEqual({ type: 'data', payload: { a: [1] } }, { type: 'data', payload: { a: [2] } })).toBe(false);
    expect(framesEqual({ type: 'data', payload: { a: 1 } }, { type: 'data', payload: { a: 1, b: 2 } })).toBe(false);
    expect(framesEqual({ type: 'data', payload: [1, 2] }, { type: 'data', payload: [1] })).toBe(false);
    expect(framesEqual({ type: 'data', payload: [1] }, { type: 'data', payload: { 0: 1 } })).toBe(false);
  });

  it('compares byte payloads by content', () => {
    const left = new Uint8Array([1, 2, 3]).buffer;
    const right = new Uint8Array([1, 2, 3]).buffer;
    const other = new Uint8Array([1, 2, 4]).buffer;
    expect(framesEqual({ type: 'data', fragment: left }, { type: 'data', fragment: right })).toBe(true);
    expect(framesEqual({ type: 'data', fragment: left }, { type: 'data', fragment: other })).toBe(false);
    expect(framesEqual({ type: 'data', fragment: left }, { type: 'data', fragment: 'abc' })).toBe(false);
  });
});
