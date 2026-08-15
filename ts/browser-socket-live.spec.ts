/**
 * `BrowserSocket` against a WebSocket implementation that is not ours.
 *
 * `BrowserSocket` is the browser half of the transport seam (WSM-API-021). Every other test of this
 * adapter drives a `MockWebSocket` written in the same file as the assertions, and a double that
 * agrees with the code under test cannot witness the readyState transitions, the event ordering or
 * the subprotocol a server actually echoes back.
 *
 * Node's global `WebSocket` is a WHATWG implementation written by other people, and it is the same
 * API surface a browser exposes: `new WebSocket(url, protocols)`, `addEventListener`, `readyState`,
 * `protocol`, `close(code, reason)`. It is not Chrome - the browser gap stays recorded in GAPS.md -
 * but here the WebSocket, the server, the handshake and the HTTP 400 refusal are all somebody else's
 * code, so this file fails if the mock's assumptions about event ordering, `readyState` or
 * subprotocol negotiation are wrong. That is a categorical assurance rather than a sharper one: no
 * mutation of this library's own source distinguishes it from the mock spec, because the mock and the
 * library agree by construction.
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
  it('negotiates, carries a request and closes - none of it through a double', async () => {
    const url = await acceptor();
    const adapter = await BrowserSocket.connect(url, 'json');

    // The subprotocol came back from a real server through a real client, which is the one thing a
    // mock cannot witness: `MockWebSocket` echoes whatever the test told it to (WSM-CDC-020/022).
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
    // browser cannot read that status, which is exactly why the dialer composes the diagnostic from
    // the codec it offered rather than from anything the server said.
    //
    // The acceptor logs the refusal because WSM-CDC-029 requires it, and this is the one place that
    // provokes a real one - so the level is dropped here rather than in a setup file, as the ILL-C
    // cells do in `ts/stream.spec.ts`. Silencing it by spying on `console.error`, which every other
    // spec reaching this line does, would put a double in a path that deliberately has none.
    const level = logger.level;
    logger.level = 'silent';
    try {
      await expect(BrowserSocket.connect(url, 'msgpack')).rejects.toBeInstanceOf(CodecMismatch);
    } finally {
      logger.level = level;
    }
  });

  it('reports the socket dying as ConnectionClosed, which is the peer only death signal', async () => {
    const url = await acceptor();
    const adapter = await BrowserSocket.connect(url, 'json');
    const waiting = adapter.receive();

    adapter.socket.close(1000, 'done here');

    // `SocketAdapter.receive` must reject when the socket ends; a real close event is the only way to
    // find out whether this adapter listens for the right one (WSM-INV-011).
    await expect(waiting).rejects.toThrow();
    expect(adapter.isClosed).toBe(true);
  });
});

describe('an unreachable acceptor is not diagnosed as a codec mismatch', () => {
  it('names both causes, and the reachable one first', async () => {
    // A dev-server proxy pointing at a port nothing serves is, in a browser, indistinguishable from a
    // refused handshake: `error` then `close` with 1006, no status, no body. A message that asserted
    // the acceptor had refused the codec would send the reader to check configuration that is not
    // wrong.
    //
    // The class is `CodecMismatch` either way: WSM-CDC-024 requires it for a refused handshake and
    // forbids a bare connection failure. Only the message can carry the ambiguity.
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
    // The overclaim this forbids: a flat assertion that the acceptor refused the codec.
    expect(message, 'the message still diagnoses a cause it cannot know').not.toMatch(
      /^the acceptor refused the muxws handshake/,
    );
  });
});
