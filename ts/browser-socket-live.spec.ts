/**
 * `BrowserSocket` against a third-party WebSocket implementation.
 *
 * `BrowserSocket` is the browser transport adapter (WSM-API-021). The other tests of this adapter use
 * `MockWebSocket`, defined in the same file as the assertions, so they do not check real readyState
 * transitions, event ordering or the subprotocol a server echoes back.
 *
 * Node's global `WebSocket` is a WHATWG implementation with the same API as a browser:
 * `new WebSocket(url, protocols)`, `addEventListener`, `readyState`, `protocol`, `close(code, reason)`.
 * It is not a browser (see GAPS.md). In this file the WebSocket, the server, the handshake and the
 * HTTP 400 refusal are third-party code, so the tests fail if the mock's assumptions about event
 * ordering, `readyState` or subprotocol negotiation are wrong. These tests do not detect mutations of
 * this library's own source that the mock spec misses; they check the mock against a real
 * implementation.
 */

import type { AddressInfo } from 'node:net';

import { afterEach, describe, expect, it } from 'vitest';
import { WebSocketServer } from 'ws';

import { JsonCodec } from './codec';
import { CodecMismatch } from './errors';
import './index';
import { handleProtocols, refuseMismatchedUpgrade, serve } from './node';
import { logger, Peer } from './peer';
import { BrowserSocket } from './transports/browser-socket';

const servers: WebSocketServer[] = [];

afterEach(async () => {
  await Promise.all(servers.splice(0).map((server) => new Promise<void>((resolve) => server.close(() => resolve()))));
});

/** A real acceptor, speaking `json`, echoing whatever a stream sends it. */
async function acceptor(): Promise<string> {
  const server = refuseMismatchedUpgrade(new WebSocketServer({ port: 0, handleProtocols }));
  servers.push(server);
  server.on('connection', (socket) => {
    void serve(socket, {
      handler: async (payload, stream) => {
        await stream.reply({ echo: payload });
      },
    }).catch(() => undefined);
  });
  await new Promise<void>((resolve) => server.once('listening', resolve));
  return `ws://127.0.0.1:${(server.address() as AddressInfo).port}`;
}

describe('BrowserSocket against a real WebSocket', () => {
  it('negotiates, carries a request and closes against a real WebSocket', async () => {
    const url = await acceptor();
    const adapter = await BrowserSocket.connect(url, 'json');

    // The subprotocol is echoed by a real server through a real client. `MockWebSocket` echoes the
    // value the test configures (WSM-CDC-020/022).
    expect(adapter.socket.protocol).toBe('muxws.v1.json');

    const peer = new Peer(adapter, { codec: new JsonCodec(), isDialer: true });
    void peer.serve().catch(() => undefined);

    expect(await peer.request({ hello: 'from a real socket' })).toEqual({
      echo: { hello: 'from a real socket' },
    });

    await peer.close();
    expect(adapter.isClosed).toBe(true);
  });

  it('raises CodecMismatch when the server speaks another codec - WSM-CDC-024', async () => {
    const url = await acceptor();
    // The acceptor is configured for json and refuses this upgrade with HTTP 400 (WSM-CDC-022). A
    // browser cannot read that status, so the dialer composes the diagnostic from the codec it offered.
    //
    // The acceptor logs the refusal (WSM-CDC-029). The log level is lowered here, as the ILL-C cells
    // do in `ts/stream.spec.ts`, instead of spying on `console.error`, so this path contains no test
    // double.
    const level = logger.level;
    logger.level = 'silent';
    try {
      await expect(BrowserSocket.connect(url, 'msgpack')).rejects.toBeInstanceOf(CodecMismatch);
    } finally {
      logger.level = level;
    }
  });

  it('reports a closed socket as ConnectionClosed, the only connection-loss signal of the peer', async () => {
    const url = await acceptor();
    const adapter = await BrowserSocket.connect(url, 'json');
    const waiting = adapter.receive();

    adapter.socket.close(1000, 'done here');

    // `SocketAdapter.receive` must reject when the socket ends; a real close event checks that the
    // adapter listens for the correct event (WSM-INV-011).
    await expect(waiting).rejects.toThrow();
    expect(adapter.isClosed).toBe(true);
  });
});

describe('an unreachable acceptor is not diagnosed as a codec mismatch', () => {
  it('names both causes, and the reachable one first', async () => {
    // In a browser, a dev-server proxy pointing at an unserved port produces the same events as a
    // refused handshake: `error`, then `close` with 1006, no status, no body. The message therefore
    // names both causes and does not state that the acceptor refused the codec.
    //
    // The class is `CodecMismatch` in both cases: WSM-CDC-024 requires it for a refused handshake and
    // forbids a bare connection failure. Only the message states both possibilities.
    const dead = 'ws://127.0.0.1:9/ws'; // discard port: reachable stack, nothing accepting
    const failure = await BrowserSocket.connect(dead, 'json').then(
      () => null,
      (error: unknown) => error,
    );

    expect(failure).toBeInstanceOf(CodecMismatch);
    const message = (failure as Error).message;
    expect(message, 'the address a reader would check is not in the message').toContain('127.0.0.1:9');
    expect(message, 'the reachable cause is not named').toMatch(/nothing is listening/);
    expect(message, 'the codec cause is not named either - both must be').toMatch(/codec/);
    // The message must not state as fact that the acceptor refused the codec.
    expect(message, 'the message states the codec refusal as fact').not.toMatch(
      /^the acceptor refused the muxws handshake/,
    );
  });
});
