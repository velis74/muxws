import corpus from '../conformance/frames/v1-frames.json';

import { JsonCodec } from './codec';
import { ProtocolError } from './errors';
import { ABSENT, type Frame, framesEqual, fromMapping, toMapping } from './frames';

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
    expect(codec.encode({ type: 'data', stream: 1 })).toBe('{"type":"data","stream":1}');
    expect(codec.encode({ type: 'data', stream: 1, payload: null })).toBe('{"type":"data","stream":1,"payload":null}');
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
