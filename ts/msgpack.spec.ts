/**
 * The msgpack codec: round-trip only, no pinned bytes, and no registration on import.
 *
 * Mirrors `muxws/codecs/msgpack__test.py` test for test. Every assertion compares
 * `decode(encode(frame))` with the original frame (WSM-CDC-006); none compares against a byte string,
 * because this library and Python's `msgpack` choose integer widths and map formats differently and
 * both choices are valid. The logical corpus is the same file the JSON tests read.
 */

import { readFile } from 'node:fs/promises';
import { join } from 'node:path';

import { vi } from 'vitest';

import corpus from '../conformance/frames/v1-frames.json';

import { JsonCodec, clearCodecs, getCodec, registerCodec } from './codec';
import { ProtocolError } from './errors';
import { Assembler, splitFrame } from './fragment';
import { ABSENT, type Frame, framesEqual, fromMapping } from './frames';
import { MsgpackCodec } from './msgpack';
import { Peer } from './peer';
import type { Stream } from './stream';
import type { SocketAdapter } from './transports/index';
import { MemorySocket, memoryPair } from './transports/memory';

const codec = new MsgpackCodec();

/** Both suites resolve the repository root from the process's working directory - see conformance/README.md. */
const SOURCE_DIR = join(process.cwd(), 'ts');

/** Fixtures spell a frame as its envelope, so an absent payload key means ABSENT. */
function frameFromFixture(mapping: Record<string, unknown>): Frame {
  return fromMapping(mapping);
}

function bytesOf(buffer: ArrayBuffer): number[] {
  return [...new Uint8Array(buffer)];
}

function settle(rounds = 12): Promise<void> {
  return [...new Array(rounds)].reduce<Promise<void>>(
    (chain) =>
      chain.then(
        () =>
          new Promise<void>((resolve) => {
            setTimeout(resolve, 0);
          }),
      ),
    Promise.resolve(),
  );
}

/**
 * A `SocketAdapter` that records which send method the peer calls.
 *
 * It delegates to a real `MemorySocket`, so the peer on the other end still answers and the same
 * test checks both the send path and a working connection.
 */
class SpySocket implements SocketAdapter {
  readonly textCalls: unknown[] = [];

  readonly byteCalls: unknown[] = [];

  constructor(private readonly inner: MemorySocket) {}

  sendText(text: string): void {
    this.textCalls.push(text);
    this.inner.sendText(text);
  }

  sendBytes(bytes: ArrayBuffer): void {
    this.byteCalls.push(bytes);
    this.inner.sendBytes(bytes);
  }

  receive(): Promise<string | ArrayBuffer> {
    return this.inner.receive();
  }

  close(code?: number, reason?: string): void {
    this.inner.close(code, reason);
  }
}

/**
 * Declares `binary = true` and returns a string anyway.
 *
 * Test-only. WSM-CDC-002 says the peer MUST NOT infer the encoding from the encoded value's type; a
 * codec whose declaration and output type differ shows which of the two the peer uses.
 */
class LyingCodec extends MsgpackCodec {
  encode(frame: Frame): ArrayBuffer {
    return String.fromCharCode(...new Uint8Array(super.encode(frame))) as unknown as ArrayBuffer;
  }
}

async function echo(payload: unknown, stream: Stream): Promise<void> {
  await stream.end({ payload });
}

// --------------------------------------------------------------------------- the port

describe('MsgpackCodec', () => {
  it('declares binary rather than inferring it - WSM-CDC-002', () => {
    expect(codec.name).toBe('msgpack');
    expect(codec.binary).toBe(true);
  });

  corpus.forEach((entry) => {
    const testCase = entry as { name: string; frame: Record<string, unknown>; json_wire: string };

    // `json_wire` is not read. It applies to the JSON codec only; asserting msgpack against it, or
    // against pinned msgpack bytes, would pin bytes a conforming encoder may produce differently
    // (WSM-CDC-006).
    it(`round-trips - ${testCase.name}`, () => {
      const frame = frameFromFixture(testCase.frame);
      const encoded = codec.encode(frame);
      expect(encoded).toBeInstanceOf(ArrayBuffer);
      expect(framesEqual(codec.decode(encoded), frame)).toBe(true);
    });
  });

  it('reads a corpus that covers every v1 frame type', () => {
    // Without this check, a reduced corpus would let the round-trip tests above pass while covering
    // fewer frame types.
    expect(corpus.length).toBeGreaterThan(10);
    expect(new Set(corpus.map((entry) => entry.frame.type)).size).toBe(6);
    expect(corpus.every((entry) => Object.keys(entry).length === 3 && 'json_wire' in entry)).toBe(true);
  });

  it('emits only the frame, not the encoder scratch buffer', () => {
    // `encode()` in @msgpack/msgpack returns `encodeSharedRef()`: a subarray of a 2048-byte internal
    // buffer that the next call overwrites. Passing `.buffer` to the socket would send the whole
    // buffer, and the remote would receive trailing data after the frame.
    const encoded = codec.encode({ type: 'ping', nonce: 'a' });
    expect(encoded.byteLength).toBeLessThan(32);
    expect(framesEqual(codec.decode(encoded), { type: 'ping', nonce: 'a' })).toBe(true);
  });

  it('decodes strings as text, not as bytes', () => {
    // `rawStrings: false`, the counterpart of Python's `raw=False`. With `true`, every envelope key
    // would decode as a Uint8Array, no frame would have a `type`, and every message would be an error.
    const frame = codec.decode(codec.encode({ type: 'data', stream: 1, payload: { label: 'č' } }));
    expect(frame.type).toBe('data');
    expect(frame.payload).toEqual({ label: 'č' });
  });

  it('round-trips an array as an array', () => {
    expect(codec.decodePayload(codec.encodePayload({ rows: [1, 2, [3, 4]] }))).toEqual({ rows: [1, 2, [3, 4]] });
  });
});

// --------------------------------------------------------------------------- bytes are a payload type

describe('bytes under a binary codec', () => {
  it('round-trips - WSM-CDC-008', () => {
    const payload = new Uint8Array([0, 255]).buffer;
    const frame: Frame = { type: 'data', stream: 1, payload, end: true };
    const decoded = codec.decode(codec.encode(frame));
    expect(framesEqual(decoded, frame)).toBe(true);
    expect(bytesOf(decoded.payload as ArrayBuffer)).toEqual([0, 255]);
  });

  it('round-trips nested inside a payload, and decodes as an ArrayBuffer', () => {
    // This port represents bytes as `ArrayBuffer`: `Codec.encode` returns one, `Frame.fragment` is
    // one, `Assembler` concatenates them. A decoder returning the library's `Uint8Array` would expose
    // that type, and `framesEqual` would report the round trip as unequal.
    const nested = { blob: new Uint8Array([1, 2]).buffer, name: 'x', parts: [new Uint8Array([3]).buffer] };
    const decoded = codec.decodePayload(codec.encodePayload(nested)) as typeof nested;
    expect(decoded.blob).toBeInstanceOf(ArrayBuffer);
    expect(bytesOf(decoded.blob)).toEqual([1, 2]);
    expect(bytesOf(decoded.parts[0])).toEqual([3]);
    expect(decoded.name).toBe('x');
  });

  it('is written as bin and never as an empty map', () => {
    // `Encoder.encodeObject` writes `bin` only for `ArrayBuffer.isView(object)`; a bare ArrayBuffer
    // falls through to `encodeMap`, which loses the data without an error (WSM-CDC-008) for every
    // fragment of a bytes payload.
    const encoded = codec.encodePayload(new Uint8Array([7, 8, 9]).buffer);
    expect([...new Uint8Array(encoded)]).toEqual([0xc4, 3, 7, 8, 9]);
  });

  it('is refused by the json codec rather than base64-encoded - WSM-CDC-008', () => {
    // `AP8=` is the base64 of the same bytes; asserting it is absent makes this test fail if the
    // codec base64-encodes bytes instead of refusing them.
    const json = new JsonCodec();
    expect(() => json.encode({ type: 'data', stream: 1, payload: new Uint8Array([0, 255]).buffer })).toThrow(
      /WSM-CDC-008/,
    );
    let message = '';
    try {
      json.encodePayload({ blob: new Uint8Array([0, 255]).buffer });
    } catch (error) {
      message = (error as Error).message;
    }
    expect(message).toMatch(/WSM-CDC-008/);
    expect(message).not.toContain('AP8=');
  });
});

// --------------------------------------------------------------------------- decode failures

describe('what the msgpack codec refuses', () => {
  it('turns an undecodable message into a ProtocolError - WSM-FRM-005', () => {
    expect(() => codec.decode(new Uint8Array([0xc1]).buffer)).toThrow(ProtocolError);
    expect(() => codec.decode(new ArrayBuffer(0))).toThrow(ProtocolError);
  });

  it('refuses a second value in the same message', () => {
    // One frame per WebSocket message (§3); a trailing value is not part of this frame.
    const one = new Uint8Array(codec.encode({ type: 'ping' }));
    const twice = new Uint8Array(one.length * 2);
    twice.set(one, 0);
    twice.set(one, one.length);
    expect(() => codec.decode(twice.buffer)).toThrow(ProtocolError);
  });

  it('refuses a message that is not a map', () => {
    expect(() => codec.decode(codec.encodePayload([1, 2, 3]))).toThrow(/map/);
    expect(() => codec.decode(codec.encodePayload(7))).toThrow(/map/);
    expect(() => codec.decode(codec.encodePayload(new Uint8Array([1]).buffer))).toThrow(/map/);
  });

  it('refuses text rather than recoding it - WSM-CDC-002', () => {
    // `binary` is declared, so text on this wire is an error of the remote. No recoding can recover
    // the original bytes, which the transport has already lost, so recoding would turn a detectable
    // violation into undetected corruption.
    expect(() => codec.decode('{"type":"ping"}')).toThrow(/text/);
    expect(() => codec.decodePayload('plain')).toThrow(/text/);
  });
});

// --------------------------------------------------------------------------- registration

describe('the msgpack subpath', () => {
  it('registers nothing at import time - WSM-CDC-013/014', async () => {
    // `vi.resetModules()` is required. This file imports `./msgpack` statically, so the module is
    // evaluated before any assertion, and a module-scope `registerCodec` would write into a registry
    // that a later `clearCodecs()` empties; the check would then pass for a module that violates the
    // rule. Resetting re-evaluates both `./codec` and `./msgpack`, so `fresh` is the registry the
    // re-imported module would register into.
    vi.resetModules();
    const fresh = await import('./codec');
    const module = await import('./msgpack');
    expect(fresh.registeredCodecs()).toEqual([]);

    fresh.registerCodec('msgpack', new module.MsgpackCodec());
    expect(fresh.getCodec('msgpack').name).toBe('msgpack');
  });

  it('contains no registration call at module scope - WSM-CDC-014', async () => {
    // Source check, independent of import timing: it reads the file. Counterpart of
    // `registry_test.py`'s AST check.
    const source = await readFile(join(SOURCE_DIR, 'msgpack.ts'), 'utf8');
    expect(source).not.toMatch(/^\s*registerCodec\(/m);
    // Comments are stripped first, because the module's doc comment names the call the application
    // must make. `registry_test.py` drops the docstring for the same reason. The remaining code must
    // not contain a registration call anywhere, including inside a function the module calls during
    // import, which has the same side effect.
    const code = source.replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/.*$/gm, '');
    expect(code).not.toContain('registerCodec(');
  });

  it('reaches @msgpack/msgpack by one static import and no probe - WSM-CDC-013', async () => {
    // Counterpart of `msgpack__test.py`'s AST check. A dynamic `import()`, a `require()`, or an import
    // nested under a `try` tests whether the package is installed instead of failing at import, which
    // violates WSM-INV-015: a deployment could be configured for msgpack without using it. Comments
    // are stripped first for the same reason as above: the module's doc comment names the package.
    const source = await readFile(join(SOURCE_DIR, 'msgpack.ts'), 'utf8');
    const code = source.replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/.*$/gm, '');
    const referring = code.split('\n').filter((line) => line.includes('@msgpack/msgpack'));
    expect(referring, 'exactly one line may name the package').toHaveLength(1);
    // Anchored at column zero: an import nested inside a `try` or a function is a probe.
    expect(referring[0]).toMatch(/^import\b.*from '@msgpack\/msgpack';$/);
    expect(code).not.toMatch(/\bimport\s*\(/);
    expect(code).not.toMatch(/\brequire\s*\(/);
  });

  it('reaches @msgpack/msgpack from nowhere but this subpath - WSM-PKG-003', async () => {
    // The optional peer dependency must not be reachable from the browser entry point, or the
    // bundler requires it. `ts/index.ts` is checked by following its imports; this test asserts that
    // `ts/msgpack.ts` is the only file that names the package.
    const files = ['index.ts', 'codec.ts', 'peer.ts', 'fragment.ts', 'frames.ts', 'node.ts'];
    const sources = await Promise.all(files.map((name) => readFile(join(SOURCE_DIR, name), 'utf8')));
    expect(sources.filter((source) => source.includes('@msgpack/msgpack'))).toEqual([]);
  });
});

// --------------------------------------------------------------------------- through the real peer

describe('a binary codec through the real Peer', () => {
  it('uses sendBytes and never sendText - WSM-CDC-002', async () => {
    // Builds the peers as an application does (`registerCodec`, then `getCodec`, then `Peer`) and
    // asserts on the SocketAdapter spy, because `MemorySocket` accepts a string on `sendBytes`
    // without an error and would not detect a peer that calls the wrong method.
    registerCodec('msgpack', new MsgpackCodec());
    const configured = getCodec('msgpack');
    const [left, right] = memoryPair();
    const spy = new SpySocket(left);
    const dialer = new Peer(spy, { codec: configured, isDialer: true });
    const acceptor = new Peer(right, { codec: configured, isDialer: false });
    acceptor.onStream(echo);
    const served = [dialer.serve(), acceptor.serve()];
    served.forEach((task) => void task.catch(() => undefined));

    try {
      const blob = new Uint8Array([0, 255]).buffer;
      const answer = (await dialer.request({ action: 'ping', blob })) as { action: string; blob: ArrayBuffer };
      expect(answer.action).toBe('ping');
      expect(bytesOf(answer.blob)).toEqual([0, 255]);
      expect(spy.textCalls).toEqual([]);
      expect(spy.byteCalls.length).toBeGreaterThan(0);
      expect(spy.byteCalls.every((message) => message instanceof ArrayBuffer)).toBe(true);
    } finally {
      left.drop();
      right.drop();
      await settle();
      await Promise.all(served.map((task) => task.catch(() => undefined)));
      clearCodecs();
      registerCodec('json', new JsonCodec());
    }
  });

  it('still uses sendBytes when the codec returns text - WSM-CDC-002', async () => {
    // The declaration decides, not the value. A peer that inferred it from the value would call
    // sendText here.
    const [left, right] = memoryPair();
    const spy = new SpySocket(left);
    const peer = new Peer(spy, { codec: new LyingCodec(), isDialer: true });
    const served = peer.serve();
    void served.catch(() => undefined);

    try {
      peer.open({ a: 1 }, { end: true });
      await settle();
      expect(spy.textCalls).toEqual([]);
      expect(spy.byteCalls.map((message) => typeof message)).toEqual(['string']);
    } finally {
      left.drop();
      right.drop();
      await settle();
      await served.catch(() => undefined);
    }
  });

  it('fragments a large bytes payload and reassembles it end to end - WSM-FRG-003', async () => {
    // Codec, splitter, writer, socket, assembler, codec again. A lowered cap is a test construction
    // argument (WSM-FRG-005), never a wire value.
    registerCodec('msgpack', new MsgpackCodec());
    const configured = getCodec('msgpack');
    const [left, right] = memoryPair();
    const dialer = new Peer(left, { codec: configured, isDialer: true, maxFrameBytes: 1024 });
    const acceptor = new Peer(right, { codec: configured, isDialer: false, maxFrameBytes: 1024 });
    acceptor.onStream(echo);
    const served = [dialer.serve(), acceptor.serve()];
    served.forEach((task) => void task.catch(() => undefined));

    try {
      const blob = new Uint8Array(10_240).map((_byte, index) => index % 256).buffer;
      const answer = (await dialer.request({ blob })) as { blob: ArrayBuffer };
      expect(bytesOf(answer.blob)).toEqual(bytesOf(blob));
      expect(left.sent.length).toBeGreaterThan(1);
      expect(left.sent.every((message) => message instanceof ArrayBuffer)).toBe(true);
      expect(Math.max(...left.sent.map((message) => (message as ArrayBuffer).byteLength))).toBeLessThanOrEqual(1024);
    } finally {
      left.drop();
      right.drop();
      await settle();
      await Promise.all(served.map((task) => task.catch(() => undefined)));
      clearCodecs();
      registerCodec('json', new JsonCodec());
    }
  });
});

// --------------------------------------------------------------------------- fragmentation

describe('fragment boundaries under a binary codec', () => {
  it('are byte boundaries measured on the produced buffer - WSM-FRG-003', () => {
    // Uses `byteLength` on the encoded buffer, not `encodedLength`, so the assertion measures the
    // quantity the rule names without depending on the helper under test. The two bounds detect the
    // two errors: under-counting puts a fragment over the cap; over-counting produces smaller legal
    // fragments, which fails the constant slice size check. The cap check alone detects neither.
    const cap = 512;
    const blob = new Uint8Array(3072).map((_byte, index) => index % 256).buffer;
    const parts = splitFrame({ type: 'data', stream: 1, payload: blob, end: true }, cap, codec);

    expect(parts.length).toBeGreaterThan(1);
    const sizes = parts.map((part) => codec.encode(part).byteLength);
    expect(Math.max(...sizes)).toBeLessThanOrEqual(cap);

    // The indivisible unit under a binary codec is one byte (WSM-FRG-012), so the splitter takes a
    // whole budget of them every time and only the last fragment is short.
    const carried = parts.map((part) => (part.fragment as ArrayBuffer).byteLength);
    expect(new Set(carried.slice(0, -1)).size).toBe(1);
    expect(carried[0]).toBeGreaterThanOrEqual(cap / 4);
    expect(carried.reduce((sum, length) => sum + length, 0)).toBe(codec.encodePayload(blob).byteLength);

    const assembler = new Assembler();
    let result: unknown = ABSENT;
    parts.forEach((part) => {
      result = assembler.feed(part, codec);
    });
    expect(bytesOf(result as ArrayBuffer)).toEqual(bytesOf(blob));
  });
});
