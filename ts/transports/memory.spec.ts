/**
 * Tests of the in-memory transport alone, so a transport failure is not reported as a peer bug.
 *
 * Mirrors `muxws/transports/memory_test.py` test for test.
 */

import { ConnectionClosed } from '../errors';

import { memoryPair } from './memory';

import type { SocketAdapter } from './index';

const BYTES = new Uint8Array([0x00, 0xff]).buffer;

describe('the in-memory pair', () => {
  it('delivers both ways, and drop closes both directions', async () => {
    const [left, right] = memoryPair();
    // `SocketAdapter` is a structural type, so `isinstance(left, SocketAdapter)` becomes a typed
    // binding plus a check of the four members the peer uses.
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
    // Identity, not equality: the pair passes messages by reference without copying.
    expect(right.sent[0]).toBe(BYTES);

    left.drop();
    expect(left.isClosed).toBe(true);
    expect(right.isClosed).toBe(true);
    await expect(right.receive()).rejects.toBeInstanceOf(ConnectionClosed);
    expect(() => {
      left.sendText('too late');
    }).toThrow(ConnectionClosed);
    // The other direction is closed as well.
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
    // The conformance runner uses `inject` to deliver frames a correct implementation never sends;
    // the injected message does not come from the other end.
    expect(left.sent).toEqual([]);
  });

  it('closes idempotently', () => {
    const [left] = memoryPair();
    left.close();
    left.close();
    expect(left.isClosed).toBe(true);
  });
});
