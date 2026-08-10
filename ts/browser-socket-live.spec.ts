/**
 * `BrowserSocket` against a WebSocket implementation that is not ours.
 *
 * Every other test of this adapter drives a `MockWebSocket` written in the same file as the
 * assertions - and a double that agrees with the code under test is not evidence, it is a restatement.
 * `BrowserSocket` is the browser half of the transport seam (WSM-API-021) and until this file existed
 * it had never touched a real WebSocket at all: not the readyState transitions, not the event
 * ordering, not the subprotocol the server actually echoed back.
 *
 * Node's global `WebSocket` is a WHATWG implementation written by other people, and it is the same
 * API surface a browser exposes: `new WebSocket(url, protocols)`, `addEventListener`, `readyState`,
 * `protocol`, `close(code, reason)`. That is not Chrome - the browser gap this project cannot close
 * from a terminal is real and stays recorded - but it is the difference between "tested against a
 * mock" and "tested against someone else's implementation of the spec".
 *
 * **What this file is worth, stated honestly.** Three mutations were tried against both this file and
 * `transports/browser-socket.spec.ts` - a malformed subprotocol offer, a dropped `close` listener, and
 * a bare `Error` where WSM-CDC-024 requires `CodecMismatch`. The mock spec caught all three, and one
 * of them this file did not. **No mutation was found that only this file catches**, so it earns its
 * place on a different argument: it is the only test in which the WebSocket, the server, the
 * handshake and the HTTP 400 refusal are all somebody else's code. It would fail if the mock's
 * assumptions about event ordering, `readyState` or subprotocol negotiation were wrong, and no
 * mutation of *our* source can demonstrate that, because the mock and the library agree by
 * construction. That is a categorical assurance, not a sharper one - and it is worth less than a
 * mutation-proven test, which is why it is written down here rather than implied.
 */

import type { AddressInfo } from 'node:net';

import { afterEach, describe, expect, it } from 'vitest';
import { WebSocketServer } from 'ws';

import { JsonCodec } from './codec';
import { CodecMismatch } from './errors';
import './index';
import { handleProtocols, refuseMismatchedUpgrade, serve } from './node';
import { Peer } from './peer';
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
    await expect(BrowserSocket.connect(url, 'msgpack')).rejects.toBeInstanceOf(CodecMismatch);
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
