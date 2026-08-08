/**
 * The transport itself, so a failure in it is never misread as a peer bug.
 *
 * Mirrors `muxws/transports/memory_test.py` test for test.
 */

import { ConnectionClosed } from '../errors';

import { memoryPair } from './memory';

import type { SocketAdapter } from './index';

const BYTES = new Uint8Array([0x00, 0xff]).buffer;

describe('the in-memory pair', () => {
  it('delivers both ways, and drop kills both directions', async () => {
    const [left, right] = memoryPair();
    // The port is structural in TypeScript, so `isinstance(left, SocketAdapter)` becomes a binding
    // the compiler has to accept plus the four members the peer is allowed to reach for.
    const port: SocketAdapter = left;
    expect(typeof port.sendText).toBe('function');
    expect(typeof port.sendBytes).toBe('function');
    expect(typeof port.receive).toBe('function');
    expect(typeof port.close).toBe('function');

    left.sendText('hello');
    expect(await right.receive()).toBe('hello');
    right.sendBytes(BYTES);
    const received = await left.receive();
    expect(received).toBeInstanceOf(ArrayBuffer);
    expect(new Uint8Array(received as ArrayBuffer)).toEqual(new Uint8Array([0x00, 0xff]));

    expect(left.sent).toEqual(['hello']);
    expect(right.sent).toHaveLength(1);
    // Identity, not equality: the pair is a wire, and a wire does not copy what it carries.
    expect(right.sent[0]).toBe(BYTES);

    left.drop();
    expect(left.isClosed).toBe(true);
    expect(right.isClosed).toBe(true);
    await expect(right.receive()).rejects.toBeInstanceOf(ConnectionClosed);
    expect(() => {
      left.sendText('too late');
    }).toThrow(ConnectionClosed);
    // The other direction is dead too, or "both directions" would be a claim the test never made.
    expect(() => {
      right.sendText('too late');
    }).toThrow(ConnectionClosed);
    expect(right.sent).toHaveLength(1);
  });

  it('blocks in receive until something arrives', async () => {
    const [left, right] = memoryPair();
    let done = false;
    const pending = right.receive().then((message) => {
      done = true;
      return message;
    });

    // A macrotask turn: everything already queued has run, and nothing resolved the receive.
    await new Promise((resolve) => {
      setTimeout(resolve, 0);
    });
    expect(done).toBe(false);

    left.sendText('now');
    expect(await pending).toBe('now');
  });

  it('injects past the other peer', async () => {
    const [left, right] = memoryPair();
    right.inject('{"type":"widget"}');
    expect(await right.receive()).toBe('{"type":"widget"}');
    // How the conformance runner delivers frames no correct implementation would send: the other
    // end never put this on the wire, which is the whole point of the method.
    expect(left.sent).toEqual([]);
  });

  it('closes idempotently', () => {
    const [left] = memoryPair();
    left.close();
    left.close();
    expect(left.isClosed).toBe(true);
  });
});
