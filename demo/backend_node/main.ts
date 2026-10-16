/**
 * The `ws` server and its single WebSocket endpoint; the Node counterpart of
 * `demo/backend_python/main.py`.
 *
 * After `accept()` the code is transport-independent. `accept()` verifies the subprotocol the handshake
 * selected, because the `muxws.v1.<codec>` name carries the codec (WSM-CDC-026/028).
 *
 * `python demo.py --browser node` starts this backend on :8020 and the Vite dev server on :5173; the
 * Python backend uses the same port. The frontend dials `/ws` through Vite's proxy and is the same for
 * both backends.
 *
 * This server serves no page. The Vite dev server serves the frontend and proxies `/ws` to this port
 * (`demo/frontend/vite.config.ts`). The launcher requires only a WebSocket endpoint on that port.
 */

import { WebSocketServer, type WebSocket as NodeWebSocket } from 'ws';

// A consumer imports from 'muxws' and 'muxws/node'. Inside this repository the bare specifiers do not
// resolve, because npm links only workspace members into `node_modules` and this repository is the
// package itself. This file uses relative paths; `interop/runner.ts` does the same and
// `demo/frontend/vite.config.ts` uses an alias.
import { CodecMismatch, ConnectionClosed, ConnectionLost, logger, type Peer, PeerRegistry } from '../../ts/index';
import { accept, handleProtocols, refuseMismatchedUpgrade } from '../../ts/node';

import { MarketService } from './handlers';
import { Market } from './market';

export const HOST = '127.0.0.1';

/** Same port and environment variable (`MUXWS_DEMO_PORT`, default 8020) as the Python backend. */
export const PORT = Number(process.env.MUXWS_DEMO_PORT ?? '8020');

/** The path the Vite proxy forwards; the Python backend serves the same path (`@app.websocket("/ws")`). */
export const PATH = '/ws';

/**
 * Per-process registry. muxws provides no cross-process backplane (WSM-REG-018); a deployment with
 * several processes publishes on its own message bus and each process fans out to its own sockets.
 */
export const registry = new PeerRegistry();
export const service = new MarketService(new Market(), registry);

/**
 * Accepts one socket as a peer, installs the stream handler and serves the peer until the connection
 * ends.
 *
 * Authentication belongs before `accept()` and uses the handshake: a header, a cookie or a ticket in
 * the query string (WSM-AUT-001). It does not belong in the hello, which is an application stream that
 * is already accepted when it is read (WSM-RCN-025). The demo performs no authentication.
 */
async function serveConnection(socket: NodeWebSocket): Promise<void> {
  let peer: Peer;
  try {
    peer = await accept(socket);
  } catch (error) {
    // The refusal is already sent on the wire and logged with both codec names (WSM-CDC-022/029).
    // Rethrowing it would produce an unhandled rejection from the `ws` connection handler, which
    // terminates the process. `serve()` in `ts/node.ts` handles it the same way.
    if (error instanceof CodecMismatch) return;
    throw error;
  }
  // Registered before `serve()` starts reading, so a stream opened in the first frame reaches the
  // handler instead of being reset with `REFUSED, "no on_stream handler"` (WSM-STM-033).
  peer.onStream(service.handlerFor(peer));
  try {
    await peer.serve();
  } catch (error) {
    // A closed or lost connection ends this connection only; it is not a process error.
    if (!(error instanceof ConnectionLost) && !(error instanceof ConnectionClosed)) throw error;
  } finally {
    // For a peer that sent a hello, the `onClose` hooks installed by `startBoard` and
    // `PeerRegistry.register` (WSM-REG-016) already clean up. This call covers a peer that closed
    // before sending a hello. `forget` is idempotent.
    service.forget(peer);
  }
}

/**
 * Creates the `ws` server with both handshake hooks installed (WSM-CDC-022).
 *
 * `handleProtocols` only selects the subprotocol: `ws` answers 101 whatever it returns, including
 * `false`, so a mismatch would be detected only on an open socket. WSM-CDC-022 requires refusing the
 * handshake where the transport allows it. `refuseMismatchedUpgrade` answers HTTP 400 through
 * `shouldHandle`, the only `ws` 8 hook that can abort an upgrade with a status. Without it the server
 * completes handshakes it must refuse.
 *
 * `refuseMismatchedUpgrade` chains the inherited `shouldHandle`, so the `path` check still applies: the
 * server answers on `/ws` and refuses other paths, as the FastAPI route does.
 */
export function startServer(port: number = PORT): WebSocketServer {
  const server = refuseMismatchedUpgrade(new WebSocketServer({ port, host: HOST, path: PATH, handleProtocols }));
  server.on('connection', (socket: NodeWebSocket) => {
    void serveConnection(socket).catch((error: unknown) => {
      logger.error('muxws demo: a connection ended badly', error);
    });
  });
  // A failed `listen` is emitted as an `error` event. Unhandled, it terminates the process with a stack
  // trace that names neither the port nor the cause. EADDRINUSE usually means another demo backend is
  // still running; this handler logs one line naming the port, as uvicorn does.
  //
  // The handler sets `process.exitCode` instead of calling `process.exit()`: stderr is a pipe under the
  // launcher, and an immediate exit can truncate the logged line. With no listening socket the event
  // loop has no work left, so the process exits with that code.
  server.on('error', (error: Error) => {
    // `ws` types this as a plain `Error`; Node adds a `code` string at runtime. The cast reads it
    // without `NodeJS.ErrnoException`, a type-only global the lint config does not allow.
    const cause =
      (error as { code?: string }).code === 'EADDRINUSE'
        ? `port ${port} is already in use - another demo backend is probably still running`
        : error.message;
    logger.error(`muxws demo: the node backend could not start: ${cause}`);
    process.exitCode = 1;
  });
  return server;
}

// The server starts on import, without a `require.main === module` guard. The launcher spawns this
// file and nothing imports it; the tests import `handlers.ts` and `market.ts` only. A guard would make
// the backend listen on nothing when the launcher's path does not match `process.argv[1]`.
const server = startServer();
server.on('listening', () => {
  console.log(`muxws demo backend (node): ws://${HOST}:${PORT}${PATH}`);
});
