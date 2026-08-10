/**
 * The `ws` server and its one WebSocket endpoint - the twin of `demo/backend_python/main.py`.
 *
 * One endpoint, and nothing transport-specific after `accept()`. `accept()` verifies the subprotocol
 * the handshake selected, because the codec is what the `muxws.v1.<codec>` name carries
 * (WSM-CDC-026/028).
 *
 * `python demo.py --backend node` starts this on :8020 and the Vite dev server on :5173, exactly as it
 * starts the Python backend on the same port. The frontend dials `/ws` through Vite's proxy and does
 * not change by a single line - which is the whole reason this file exists.
 *
 * There is no page to serve here. uvicorn does not serve one either: the dev server does, and it
 * proxies `/ws` to this port (`demo/frontend/vite.config.ts`), so a WebSocket endpoint on the port the
 * Python backend uses is the entire contract with the launcher.
 */

import { WebSocketServer, type WebSocket as NodeWebSocket } from 'ws';

// The published spelling is `import { ... } from 'muxws'` and `from 'muxws/node'`. This repository is
// the package, and npm links only workspace *members* into `node_modules`, so the bare specifier has
// nothing to resolve to here - `interop/runner.ts` and `demo/frontend/vite.config.ts` each work around
// the same gap, one with a relative path and one with an alias. A consumer outside this repository
// writes the two bare specifiers and changes nothing else.
import { CodecMismatch, ConnectionClosed, ConnectionLost, logger, type Peer, PeerRegistry } from '../../ts/index';
import { accept, handleProtocols, refuseMismatchedUpgrade } from '../../ts/node';

import { MarketService } from './handlers';
import { Market } from './market';

export const HOST = '127.0.0.1';

/** The same port and the same environment variable as the Python backend; the launcher knows one. */
export const PORT = Number(process.env.MUXWS_DEMO_PORT ?? '8020');

/** The path the Vite proxy forwards, and the path `@app.websocket("/ws")` answers on the other side. */
export const PATH = '/ws';

/**
 * Per process, and that is the whole of its scope. muxws ships no cross-process backplane
 * (WSM-REG-018): a deployment that ran two of these would publish on whatever bus it already has and
 * let each process fan out to the sockets it holds.
 */
export const registry = new PeerRegistry();
export const service = new MarketService(new Market(), registry);

/**
 * One socket, one peer, one handler.
 *
 * Where authentication would go: **here**, before `accept()`, out of the handshake - a header, a cookie
 * or a ticket in the query string (WSM-AUT-001). It does not go in the hello, which is an ordinary
 * application stream that has already been accepted by the time it is read (WSM-RCN-025). The demo
 * dials without a credential and this comment is the whole of its auth story.
 */
async function serveConnection(socket: NodeWebSocket): Promise<void> {
  let peer: Peer;
  try {
    peer = await accept(socket);
  } catch (error) {
    // A refusal that has already been answered on the wire and already logged with both codec names
    // (WSM-CDC-022/029); re-reporting it here would only surface it as an unhandled rejection out of a
    // `ws` connection handler, which takes the process down. `serve()` in `ts/node.ts` swallows it for
    // exactly this reason.
    if (error instanceof CodecMismatch) return;
    throw error;
  }
  // Registered before `serve()` starts reading, so a stream that arrives in the first frame off the
  // socket meets a handler rather than `reset(REFUSED, "no on_stream handler")` (WSM-STM-033).
  peer.onStream(service.handlerFor(peer));
  try {
    await peer.serve();
  } catch (error) {
    // A browser that walked away is this connection ending, not this process failing.
    if (!(error instanceof ConnectionLost) && !(error instanceof ConnectionClosed)) throw error;
  } finally {
    // `onClose` already does this - `startBoard` installs a hook, and `PeerRegistry.register` installs
    // its own (WSM-REG-016). This is here for the peer that never got as far as a hello: it has no
    // board and no index entry, and `forget` is idempotent, so the cost of being sure is one map
    // lookup.
    service.forget(peer);
  }
}

/**
 * **Both** handshake hooks, and neither is redundant (WSM-CDC-022).
 *
 * `handleProtocols` only *selects*: whatever it returns - including `false` - `ws` still answers 101,
 * and a mismatch would then have to be found on an open socket, which is the "complete the handshake
 * and close afterwards" the rule forbids wherever the transport gives a choice. `refuseMismatchedUpgrade`
 * is what answers HTTP 400, through `shouldHandle`, which is the only hook `ws` 8 leaves that can abort
 * an upgrade with a status. A server that installs the first alone completes handshakes it must refuse.
 *
 * `path` rides on the same hook - `refuseMismatchedUpgrade` chains the inherited `shouldHandle` rather
 * than replacing it - so this server answers on `/ws` and refuses everything else, exactly as the
 * FastAPI route does.
 */
export function startServer(port: number = PORT): WebSocketServer {
  const server = refuseMismatchedUpgrade(new WebSocketServer({ port, host: HOST, path: PATH, handleProtocols }));
  server.on('connection', (socket: NodeWebSocket) => {
    void serveConnection(socket).catch((error: unknown) => {
      logger.error('muxws demo: a connection ended badly', error);
    });
  });
  // A failed `listen` arrives as an `error` event, and an *unhandled* one takes the process down with
  // a stack trace whose top frame is `node:net:2324` - which names neither the port nor the cause.
  // The reader who hits this is nearly always the one with a demo still running in another terminal,
  // and `demo.py` records that exact failure for vite on 5173; uvicorn answers the same mistake with
  // one readable line, so the Node backend says it too rather than failing worse than its twin.
  //
  // `exitCode` rather than `process.exit()`: stderr is a pipe under the launcher and an immediate exit
  // can truncate the very line this exists to print. With no listening socket there is nothing left to
  // keep the loop alive, so setting the code is enough to end the process with it.
  server.on('error', (error: Error) => {
    // `ws` types this as a plain `Error` and Node hangs an `errno` string off it at runtime, so the
    // code is read through a narrowing cast rather than through `NodeJS.ErrnoException` - that
    // namespace is a type-only global the lint config would have to be widened to accept.
    const cause =
      (error as { code?: string }).code === 'EADDRINUSE'
        ? `port ${port} is already in use - another demo backend is probably still running`
        : error.message;
    logger.error(`muxws demo: the node backend could not start: ${cause}`);
    process.exitCode = 1;
  });
  return server;
}

// Started on import, with no `require.main === module` guard around it.
//
// `main.py` has one because uvicorn *imports* that module to find `app`; nothing imports this one. The
// launcher spawns it, and the guard's only possible effect here would be a backend that silently
// listens on nothing when a launcher reaches the file by a path `process.argv[1]` does not match. The
// tests import `handlers.ts` and `market.ts`, never this file, which is what keeps that harmless.
const server = startServer();
server.on('listening', () => {
  console.log(`muxws demo backend (node): ws://${HOST}:${PORT}${PATH}`);
});
