import { JsonCodec, clearCodecs, getCodec, registerCodec, registeredCodecs } from './codec';
import { CodecNotRegistered, ProtocolError } from './errors';
import { type Frame, framesEqual } from './frames';

const codec = new JsonCodec();

class FakeCodec {
  readonly name = 'fake';
  readonly binary = true;

  encode(): ArrayBuffer {
    return new Uint8Array(0).buffer;
  }

  decode(): Frame {
    return { type: 'data' };
  }

  encodePayload(): ArrayBuffer {
    return new Uint8Array(0).buffer;
  }

  decodePayload(): unknown {
    return null;
  }
}

describe('the registry', () => {
  afterEach(() => {
    clearCodecs();
    registerCodec('json', new JsonCodec());
  });

  it('has json registered by the library, not by the codec module - WSM-CDC-004/014', async () => {
    await import('./index');
    expect(registeredCodecs()).toContain('json');
    expect(getCodec('json').name).toBe('json');
  });

  it('throws CodecNotRegistered naming the variable, the value and the set - WSM-CDC-016', () => {
    let caught: CodecNotRegistered | null = null;
    try {
      getCodec('msgpack');
    } catch (error) {
      caught = error as CodecNotRegistered;
    }
    expect(caught).toBeInstanceOf(CodecNotRegistered);
    expect(caught?.message).toContain('VITE_MUXWS_CODEC');
    expect(caught?.message).toContain('msgpack');
    expect(caught?.configured).toBe('msgpack');
    expect(caught?.available).toContain('json');
  });

  it('registers explicitly, with no probing or auto-discovery - WSM-CDC-013', () => {
    registerCodec('fake', new FakeCodec());
    expect(getCodec('fake').binary).toBe(true);
    expect(registeredCodecs()).toEqual([...registeredCodecs()].sort());
  });

  it('never falls back to json - WSM-INV-015', () => {
    expect(() => getCodec('msgpack')).toThrow(CodecNotRegistered);
    clearCodecs();
    expect(() => getCodec('json')).toThrow(CodecNotRegistered);
  });
});

describe('the msgpack subpath', () => {
  it('registers nothing at import time - WSM-CDC-014', async () => {
    clearCodecs();
    await import('./msgpack');
    expect(registeredCodecs()).toEqual([]);
    registerCodec('json', new JsonCodec());
  });
});

describe('JsonCodec', () => {
  it('declares binary rather than inferring it - WSM-CDC-001/002', () => {
    expect(codec.name).toBe('json');
    expect(codec.binary).toBe(false);
  });

  it('refuses bytes rather than base64-encoding them - WSM-CDC-008', () => {
    expect(() => codec.encode({ type: 'data', stream: 1, payload: new Uint8Array([0, 255]).buffer })).toThrow(
      /WSM-CDC-008/,
    );
    expect(() => codec.encodePayload({ blob: new Uint8Array([0, 255]) })).toThrow(/WSM-CDC-008/);
  });

  it('refuses everything else JSON cannot represent, rather than losing it silently', () => {
    expect(() => codec.encodePayload(Number.NaN)).toThrow(TypeError);
    expect(() => codec.encodePayload(Number.POSITIVE_INFINITY)).toThrow(TypeError);
    expect(() => codec.encodePayload({ big: 1n })).toThrow(TypeError);
    expect(() => codec.encodePayload({ fn: () => undefined })).toThrow(TypeError);
    expect(() => codec.encodePayload({ sym: Symbol('x') })).toThrow(TypeError);
    expect(() => codec.encodePayload({ tags: new Set([1]) })).toThrow(TypeError);
    expect(() => codec.encodePayload({ map: new Map() })).toThrow(TypeError);

    const circular: Record<string, unknown> = {};
    circular.self = circular;
    expect(() => codec.encodePayload(circular)).toThrow(/circular/);
  });

  it('treats an undecodable message as a protocol error - WSM-FRM-005', () => {
    expect(() => codec.decode('{this is not json')).toThrow(ProtocolError);
    expect(() => codec.decode('[1,2,3]')).toThrow(/must decode to an object/);
    expect(() => codec.decode('null')).toThrow(/must decode to an object/);
    expect(() => codec.decodePayload('{truncated')).toThrow(/reassembled/);
  });

  it('emits compact, un-escaped JSON so both ports agree byte for byte - WSM-FRG-016', () => {
    const encoded = codec.encode({ type: 'data', stream: 1, payload: { a: 1, b: 'č' } });
    expect(encoded).not.toContain(' ');
    expect(encoded).not.toContain('\\u');
    expect(encoded).toContain('č');
  });

  it('round-trips a payload independently of the envelope', () => {
    [{ a: [1, 2] }, 'plain', 17, null, [], { 'non-bmp': '𝕄' }].forEach((payload) => {
      expect(codec.decodePayload(codec.encodePayload(payload))).toEqual(payload);
    });
  });

  it('accepts an ArrayBuffer as well as a string when decoding', () => {
    const buffer = new TextEncoder().encode('{"type":"ping","nonce":"a"}').buffer;
    expect(framesEqual(codec.decode(buffer), { type: 'ping', nonce: 'a' })).toBe(true);
    expect(codec.decodePayload(new TextEncoder().encode('{"a":1}').buffer)).toEqual({ a: 1 });
  });

  it('encodes undefined as null rather than emitting nothing', () => {
    expect(codec.encodePayload(undefined)).toBe('null');
  });
});
